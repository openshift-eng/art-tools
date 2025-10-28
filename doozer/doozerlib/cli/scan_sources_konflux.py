import asyncio
import base64
import json
import logging
import os
import random
import tempfile
from datetime import datetime, timedelta, timezone
from functools import wraps
from json import JSONDecodeError
from typing import Dict, List, Optional, cast

import aiohttp
import artcommonlib.util
import click
import dateutil.parser
import pycares
import yaml
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch
from artcommonlib.constants import KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS
from artcommonlib.exectools import cmd_gather_async
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.package_rpm_finder import PackageRpmFinder
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from artcommonlib.release_util import isolate_timestamp_in_release
from artcommonlib.rhcos import get_primary_container_name
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import deep_merge, fetch_slsa_attestation
from async_lru import alru_cache
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import rhcos, util
from doozerlib.build_info import KonfluxBuildRecordInspector
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.cli import release_gen_payload as rgp
from doozerlib.constants import KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.metadata import Metadata, RebuildHint, RebuildHintCode
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolver
from doozerlib.util import oc_image_info_for_arch_async__caching

DEFAULT_THRESHOLD_HOURS = 6
TASK_BUNDLE_AGE_THRESHOLD_DAYS = 10


class ConfigScanSources:
    def __init__(
        self,
        runtime: Runtime,
        ci_kubeconfig: str,
        session: aiohttp.ClientSession,
        as_yaml: bool,
        rebase_priv: bool = False,
        dry_run: bool = False,
    ):
        if runtime.konflux_db is None:
            raise DoozerFatalError('Cannot run scan-sources without a valid Konflux DB connection')
        runtime.konflux_db.bind(KonfluxBuildRecord)

        self.github_token = os.getenv('GITHUB_TOKEN')
        if not self.github_token:
            raise DoozerFatalError("GITHUB_TOKEN environment variable must be set")

        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.session = session
        self.ci_kubeconfig = ci_kubeconfig
        self.as_yaml = as_yaml
        self.rebase_priv = rebase_priv
        self.dry_run = dry_run

        self.all_rpm_metas = set(runtime.rpm_metas())
        self.all_image_metas = set(
            filter(
                lambda meta: meta.enabled or (meta.mode == 'disabled' and self.runtime.load_disabled),
                runtime.image_metas(),
            )
        )
        self.all_metas = self.all_rpm_metas.union(self.all_image_metas)

        self.changing_image_names = set()
        self.assessment_reason = dict()  # maps metadata qualified_key => message describing change
        self.issues = list()  # tracks issues that arose during the scan, which did not interrupt the job

        self.package_rpm_finder = PackageRpmFinder(runtime)
        self.latest_image_build_records_map: Dict[str, KonfluxBuildRecord] = {}
        self.latest_rpm_build_records_map: Dict[str, Dict[str, KonfluxBuildRecord]] = {}
        self.image_tree = {}
        self.changing_rpm_names = set()
        self.rhcos_status = []
        self.registry_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
        self.current_task_bundles: Dict[str, str] = {}

    async def run(self):
        # Try to rebase into openshift-priv to reduce upstream merge -> downstream build time
        if self.rebase_priv:
            major, minor = self.runtime.get_major_minor_fields()
            version = f'{major}.{minor}'
            if version not in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS:
                self.logger.warning(
                    'ocp4-scan for Konflux is not allowed to rebase into openshfit-priv version %s', version
                )
            else:
                self.rebase_into_priv()

        # Gather latest builds for ART-managed RPMs
        await self.find_latest_rpms_builds()

        # Find RPMs built by ART that need to be rebuilt
        await self.check_changing_rpms()

        # Get current task bundle SHAs from GitHub
        self.current_task_bundles = await self.get_current_task_bundle_shas()

        # Build an image dependency tree to scan across levels of inheritance. This should save us some time,
        # as when an image is found in need for a rebuild, we can also mark its children or operators without checking
        self.image_tree = self.generate_dependency_tree(self.runtime.image_tree)
        for level in sorted(self.image_tree.keys()):
            await self.scan_images(self.image_tree[level])

        # Check RHCOS status if the kubeconfig is provided and group is openshift-*
        if self.ci_kubeconfig and self.runtime.group.startswith("openshift-"):
            await self.detect_rhcos_status()

        # Print the output report
        self.generate_report()

    def _try_reconciliation(self, metadata: Metadata, repo_name: str, pub_branch_name: str, priv_branch_name: str):
        reconciled = False

        # Attempt a fast-forward merge
        rc, _, _ = exectools.cmd_gather(cmd=['git', 'pull', '--ff-only', 'public_upstream', pub_branch_name])
        if not rc:
            # fast-forward succeeded, will push to openshift-priv
            self.logger.info('Fast-forwarded %s from public_upstream/%s', metadata.name, pub_branch_name)
            reconciled = True

        else:
            # fast-forward failed, trying a merge commit
            rc, _, _ = exectools.cmd_gather(
                cmd=[
                    'git',
                    'merge',
                    f'public_upstream/{pub_branch_name}',
                    '-m',
                    f'Reconciled {repo_name} with public upstream',
                ],
                log_stderr=True,
                log_stdout=True,
            )
            if not rc:
                # merge succeeded, will push to openshift-priv
                reconciled = True
                self.logger.info('Merged public_upstream/%s into %s', priv_branch_name, metadata.name)

        if not reconciled:
            # Could not rebase from public upstream: need manual reconciliation. Log a warning and return
            self.logger.warning(
                'failed rebasing %s from public upstream: will need manual reconciliation', metadata.name
            )
            self.issues.append(
                {'name': metadata.distgit_key, 'issue': 'Could not rebase into -priv as it needs manual reconciliation'}
            )
            return

        if self.dry_run:
            self.logger.info('Would have tried reconciliation for %s/%s', repo_name, priv_branch_name)
            return

        # Try to push to openshift-priv
        try:
            exectools.cmd_assert(cmd=['git', 'push', 'origin', priv_branch_name], retries=3)
            self.logger.info('Successfully reconciled %s with public upstream', metadata.name)

        except ChildProcessError:
            # Failed pushing to openshift-priv
            self.logger.warning('failed pushing to openshift-priv for %s', metadata.name)
            self.issues.append({'name': metadata.distgit_key, 'issue': 'Failed pushing to openshift-priv'})

    def _do_shas_match(self, public_url, pub_branch_name, priv_url, priv_branch_name) -> bool:
        """
        Use GitHub API to check commit SHAs on private and public upstream for a given branch.
        Return True if they match, False otherwise
        """

        try:
            # Check public commit ID
            out, _ = exectools.cmd_assert(
                ['git', 'ls-remote', public_url, pub_branch_name], retries=5, on_retry='sleep 5'
            )
            pub_commit = out.strip().split()[0]

            # Check private commit ID
            out, _ = exectools.cmd_assert(
                ['git', 'ls-remote', priv_url, priv_branch_name], retries=5, on_retry='sleep 5'
            )
            priv_commit = out.strip().split()[0]

        except ChildProcessError:
            self.logger.warning('Could not fetch latest commit SHAs from %s: skipping rebase', public_url)
            return True

        if pub_commit == priv_commit:
            self.logger.info('Latest commits match on public and priv upstreams for %s', public_url)
            return True

        self.logger.info(
            'Latest commits do not match on public and priv upstreams for %s: public SHA = %s, private SHA = %s',
            public_url,
            pub_commit,
            priv_commit,
        )
        return False

    def _is_pub_ancestor_of_priv(self, path: str, pub_branch_name: str, priv_branch_name: str, repo_name: str) -> bool:
        """
        If a reconciliation already happened, private upstream might have a merge commit thus be a descendant
        of the public upstream. In this case, we don't need to rebase public into priv

        Use merge-base --is-ancestor to determine if public upstream is an ancestor of the private one
        """

        with Dir(path):
            # Check if the first <commit> is an ancestor of the second <commit>,
            # and exit with status 0 if true, or with status 1 if not.
            # Errors are signaled by a non-zero status that is not 1.
            rc, _, _ = exectools.cmd_gather(
                [
                    'git',
                    'merge-base',
                    '--is-ancestor',
                    f'public_upstream/{pub_branch_name}',
                    f'origin/{priv_branch_name}',
                ]
            )
        if rc == 1:
            self.logger.info('Public upstream is ahead of private for %s: will need to rebase', repo_name)
            return False
        if rc == 0:
            self.logger.info('Private upstream is ahead of public for %s: no need to rebase', repo_name)
            return True
        raise IOError(f'Could not determine ancestry between public and private upstreams for {repo_name}')

    def rebase_into_priv(self):
        if self.dry_run:
            self.logger.info('Would have rebased into openshift-priv')
            return

        self.logger.info('Rebasing public upstream contents into openshift-priv')
        upstream_mappings = exectools.parallel_exec(
            lambda meta, _: (
                meta,
                SourceResolver.get_public_upstream(
                    meta.config.content.source.git.url, self.runtime.group_config.public_upstreams
                ),
            ),
            self.all_metas,
            n_threads=20,
        ).get()

        for metadata, public_upstream in upstream_mappings:
            # Skip rebase for disabled images
            if not metadata.enabled:
                self.logger.warning('%s is disabled: skipping rebase', metadata.name)
                continue

            if metadata.config.content is Missing:
                self.logger.warning(
                    '%s %s is a distgit-only component: skipping openshift-priv rebase',
                    metadata.meta_type,
                    metadata.name,
                )
                continue

            public_url, public_branch_name, has_public_upstream = public_upstream

            # If no public upstream exists, skip the rebase
            if not has_public_upstream:
                self.logger.warning(
                    '%s %s does not have a public upstream: skipping openshift-priv rebase',
                    metadata.meta_type,
                    metadata.name,
                )
                continue

            priv_url = artcommonlib.util.convert_remote_git_to_https(metadata.config.content.source.git.url)
            priv_branch_name = metadata.config.content.source.git.branch.target

            # If a git commit hash was declared as the upstream source, skip the rebase
            try:
                _ = int(priv_branch_name, 16)
                # target branch is a sha: skip rebase for this component
                self.logger.warning('Target branch for %s is a SHA: skipping rebase', metadata.name)
                continue

            except ValueError:
                # target branch is a normal branch name
                pass

            # If no public_upstreams field exists, public_branch_name will be None
            public_branch_name = public_branch_name or priv_branch_name

            if priv_url == public_url:
                # Upstream repo does not have a public counterpart: no need to rebase
                self.logger.warning(
                    '%s %s does not have a public upstream: skipping openshift-priv rebase',
                    metadata.meta_type,
                    metadata.name,
                )
                continue

            # First, quick check: if SHAs match across remotes, repo is synced and we can avoid cloning it
            _, public_org, public_repo_name = artcommonlib.util.split_git_url(public_url)
            _, priv_org, priv_repo_name = artcommonlib.util.split_git_url(priv_url)

            if self._do_shas_match(
                public_url, public_branch_name, metadata.config.content.source.git.url, priv_branch_name
            ):
                # If they match, do nothing
                continue

            # If they don't, clone source repo
            path = self.runtime.source_resolver.resolve_source(metadata).source_path

            # SHAs might differ because of previous rebase; let's check the actual content across upstreams
            if self._is_pub_ancestor_of_priv(path, public_branch_name, priv_branch_name, priv_repo_name):
                # Private upstream is ahead of public: no need to rebase
                continue

            with Dir(path):
                self._try_reconciliation(metadata, priv_repo_name, public_branch_name, priv_branch_name)

    def generate_dependency_tree(self, tree, level=1, levels_dict=None):
        if not levels_dict:
            levels_dict = {}

        for key, value in tree.items():
            if level not in levels_dict:
                levels_dict[level] = []
            levels_dict[level].append(key)

            self.generate_dependency_tree(value, level + 1, levels_dict)

        return levels_dict

    async def find_latest_rpms_builds(self):
        """
        The RPM build map stores latest builds for all RPM targets:
        {
            'openshift-clients': {
                'el8': <KonfluxBuildRecord>,
                'el9': <KonfluxBuildRecord>
            },
            'microshift': {
                'el9': <KonfluxBuildRecord>
            }
        }
        """

        self.logger.info('Gathering latest RPM build records information...')

        async def _find_target_build(rpm_meta, el_target):
            rpm_name = rpm_meta.rpm_name
            build_record = await rpm_meta.get_latest_build(el_target=el_target, engine=Engine.BREW.value)
            if not self.latest_rpm_build_records_map.get(rpm_name):
                self.latest_rpm_build_records_map[rpm_name] = {}
            self.latest_rpm_build_records_map[rpm_name][el_target] = build_record

        tasks = []
        for rpm in self.runtime.rpm_metas():
            tasks.extend([_find_target_build(rpm, f'el{target}') for target in rpm.determine_rhel_targets()])
        await asyncio.gather(*tasks)

    async def find_latest_image_builds(self, image_names: List[str]):
        self.logger.info('Gathering latest image build records information...')
        latest_image_builds = await asyncio.gather(
            *[self.runtime.image_map[name].get_latest_build(engine=Engine.KONFLUX.value) for name in image_names]
        )
        self.latest_image_build_records_map.update((zip(image_names, latest_image_builds)))

    async def scan_images(self, image_names: List[str]):
        # Do not scan images that have been disabled for Konflux operations
        image_names = filter(lambda name: self.runtime.image_map[name].config.konflux.mode != 'disabled', image_names)

        # Do not scan images that have already been requested for rebuild
        image_names = list(filter(lambda name: name not in self.changing_image_names, image_names))

        # Store latest build records in a map, to reduce DB queries and execution time
        await self.find_latest_image_builds(image_names)

        # Scan images for changes
        scanning_image_metas = [self.runtime.image_map[image_name] for image_name in image_names]
        await asyncio.gather(*[self.scan_image(image_meta) for image_meta in scanning_image_metas])

    @staticmethod
    def skip_check_if_changing(coro):
        """
        Do not scan images that have already been marked for rebuild
        """

        @wraps(coro)
        async def inner(self, image_meta: ImageMetadata, *args, **kwargs):
            if image_meta.distgit_key not in self.changing_image_names:
                return await coro(self, image_meta, *args, **kwargs)
            else:
                self.logger.info('%s already marked as changed, skipping %s()', image_meta.distgit_key, coro.__name__)

        return inner

    @skip_check_if_changing
    async def scan_image(self, image_meta: ImageMetadata):
        self.logger.info(f'Scanning {image_meta.distgit_key} for changes')
        if image_meta.config.konflux is not Missing:
            image_meta.config = Model(deep_merge(image_meta.config.primitive(), image_meta.config.konflux.primitive()))

        # Check if the component has ever been built
        latest_build_record = self.latest_image_build_records_map.get(image_meta.distgit_key, None)
        if not latest_build_record:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(
                    code=RebuildHintCode.NO_LATEST_BUILD,
                    reason=f'Component {image_meta.distgit_key} has no latest build '
                    f'for assembly {self.runtime.assembly}',
                ),
            )
            return

        # Check for changes in image arches
        await self.scan_arch_changes(image_meta)

        # Check for changes in the network mode
        await self.scan_network_mode_changes(image_meta)

        # Check if there's already a build from upstream latest commit
        await self.scan_for_upstream_changes(image_meta)

        # Check if there has been a config change since last build
        await self.scan_for_config_changes(image_meta)

        # Check for dependency changes
        await self.scan_dependency_changes(image_meta)

        # Check for changes in builders
        await self.scan_builders_changes(image_meta)

        # Check for RPM changes
        await self.scan_rpm_changes(image_meta)

        # Check for changes in extra packages
        await self.scan_extra_packages(image_meta)

        # Check for outdated task bundles
        await self.scan_task_bundle_changes(image_meta)

    def find_upstream_commit_hash(self, meta: Metadata):
        """
        Get the upstream latest commit hash using git ls-remote
        """
        use_source_fallback_branch = cast(str, self.runtime.group_config.use_source_fallback_branch or "yes")
        _, upstream_commit_hash = SourceResolver.detect_remote_source_branch(
            meta.config.content.source.git, self.runtime.stage, use_source_fallback_branch
        )
        return upstream_commit_hash

    @skip_check_if_changing
    async def scan_arch_changes(self, image_meta: ImageMetadata):
        """
        Check if all arches the image should be built for are present in latest build record
        """
        target_arches = set(image_meta.get_arches())
        build_record = self.latest_image_build_records_map[image_meta.distgit_key]
        build_arches = set(build_record.arches)

        if target_arches != build_arches:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(
                    RebuildHintCode.ARCHES_CHANGE,
                    f'Arches of {build_record.nvr}: ({build_arches}) does not match target arches {target_arches}',
                ),
            )

    @skip_check_if_changing
    async def scan_network_mode_changes(self, image_meta: ImageMetadata):
        """
        Check if image conforms to the network mode derived from config

        Note that Konflux only cares about hermetic. We have an additional network mode 'internal-only' which
        will be deprecated in the future once we completely more to hermetic.
        """
        network_mode = image_meta.get_konflux_network_mode()
        self.logger.debug(f"Network mode of {image_meta.name} in config is {network_mode}")
        build_record = self.latest_image_build_records_map[image_meta.distgit_key]

        # Fetch the SLSA attestation for the latest build
        attestation = await fetch_slsa_attestation(
            build_record.image_pullspec, build_record.name, self.registry_auth_file
        )
        if not attestation:
            self.logger.warning('Skipping network mode check for %s', image_meta.distgit_key)
            return

        # Inspect the SLSA attestation to see if the build is hermetic
        is_hermetic = attestation["predicate"]["invocation"]["parameters"]["hermetic"]
        is_hermetic = True if is_hermetic.lower() == "true" else False

        self.logger.debug(f"Hermetic mode for {build_record.image_pullspec} is set to: {is_hermetic}")
        # Rebuild if there is a mismatch
        if (network_mode == "hermetic") != is_hermetic:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(
                    code=RebuildHintCode.NETWORK_MODE_CHANGE,
                    reason=f"Latest build {build_record.image_pullspec} network mode was {'hermetic' if is_hermetic else 'open'} but {network_mode} is required",
                ),
            )

    @skip_check_if_changing
    async def scan_for_upstream_changes(self, image_meta: ImageMetadata):
        """
        Determine if the current upstream source commit hash
        has a downstream build associated with it.
        """

        # We have no more "alias" source anywhere in ocp-build-data, and there's no such a thing as a distgit-only
        # component in Konflux; hence, assume that git is the only possible source for a component
        # TODO runtime.stage seems to be never different from False, maybe it can be pruned?
        # TODO use_source_fallback_branch isn't defined anywhere in ocp-build-data, maybe it can be pruned?

        # Scan for any build in this assembly which includes the git commit.
        upstream_commit_hash = self.find_upstream_commit_hash(image_meta)
        upstream_commit_build_record = await image_meta.get_latest_build(
            engine=Engine.KONFLUX.value, extra_patterns={'commitish': upstream_commit_hash}
        )

        # No build from latest upstream commit: handle accordingly
        if not upstream_commit_build_record:
            await self.handle_missing_upstream_commit_build(image_meta, upstream_commit_hash)
            return

        # Does most recent build match the one from the latest upstream commit?
        # If it doesn't, mark for rebuild
        latest_build_record = self.latest_image_build_records_map[image_meta.distgit_key]
        if latest_build_record.commitish != upstream_commit_hash:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(
                    code=RebuildHintCode.UPSTREAM_COMMIT_MISMATCH,
                    reason=f'Latest build {latest_build_record.nvr} does not match upstream commit build '
                    f'{upstream_commit_build_record.nvr}; commit reverted?',
                ),
            )

    @skip_check_if_changing
    async def handle_missing_upstream_commit_build(self, image_meta: ImageMetadata, upstream_commit_hash: str):
        """
        There is no build for this upstream commit. Two options to assess:
        1. This is a new commit and needs to be built
        2. Previous attempts at building this commit have failed
        """

        # If a build fails, how long will we wait before trying again
        rebuild_interval = self.runtime.group_config.scan_freshness.threshold_hours or DEFAULT_THRESHOLD_HOURS
        now = datetime.now(timezone.utc)

        # Check whether a build attempt with this commit has failed before.
        failed_commit_build_record = await image_meta.get_latest_build(
            extra_patterns={'commitish': upstream_commit_hash}, outcome=KonfluxBuildOutcome.FAILURE
        )

        # If not, this is a net-new upstream commit. Build it.
        if not failed_commit_build_record:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(
                    code=RebuildHintCode.NEW_UPSTREAM_COMMIT,
                    reason='A new upstream commit exists and needs to be built',
                ),
            )
            return

        # Otherwise, there was a failed attempt at this upstream commit on record.
        # Make sure provide at least rebuild_interval hours between such attempts
        last_attempt_time = failed_commit_build_record.start_time

        # Latest failed attempt is older than the threshold: rebuild
        # Otherwise, delay next build attempt
        if last_attempt_time + timedelta(hours=rebuild_interval) < now:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(
                    code=RebuildHintCode.LAST_BUILD_FAILED,
                    reason=f'It has been {rebuild_interval} hours since last failed build attempt',
                ),
            )

    async def fetch_config_digest(self, build_record: KonfluxBuildRecord):
        """
        Given a Konflux build record, fetches the configuration digest associated with the rebase commit
        associated with the build.
        """

        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            config_digest = temp_file.name

            # Download the config digest to the temporary file
            await artcommonlib.util.download_file_from_github(
                repository=build_record.rebase_repo_url,
                branch=build_record.rebase_commitish,
                path='.oit/config_digest',
                token=self.github_token,
                destination=config_digest,
                session=self.session,
            )

            # Read and return the content of the temporary file
            with open(config_digest) as f:
                return f.read()

    @skip_check_if_changing
    async def scan_for_config_changes(self, image_meta: ImageMetadata):
        try:
            latest_build_record = self.latest_image_build_records_map[image_meta.distgit_key]

            # Look at the digest that created THIS build. What is in head does not matter.
            prev_digest = await self.fetch_config_digest(latest_build_record)

            # Compute the latest config digest
            current_digest = image_meta.calculate_config_digest(self.runtime.group_config, self.runtime.streams)

            if current_digest.strip() != prev_digest.strip():
                self.logger.info(
                    '%s config_digest %s is differing from %s', image_meta.distgit_key, prev_digest, current_digest
                )
                # fetch latest commit message on branch for the image metadata file
                with Dir(self.runtime.data_dir):
                    path = f'images/{image_meta.config_filename}'
                    rc, commit_message, _ = exectools.cmd_gather(f'git log -1 --format=%s -- {path}', strip=True)
                    if rc != 0:
                        raise IOError(f'Unable to retrieve commit message from {self.runtime.data_dir} for {path}')

                if 'scan-sources-konflux:noop' in commit_message.lower():
                    self.logger.info('Ignoring digest change since commit message indicates noop')
                else:
                    self.logger.warning(
                        'Would have rebuild %s because of metadata config change', image_meta.distgit_key
                    )
                    self.add_image_meta_change(
                        image_meta, RebuildHint(RebuildHintCode.CONFIG_CHANGE, 'Metadata configuration change')
                    )

        except IOError:
            # IOError is raised by fetch_cgit_file() when config_digest could not be found
            self.logger.warning('config_digest not found for %s: skipping config check', image_meta.distgit_key)
            return

        except pycares.AresError as e:
            self.logger.error(e)
            raise

        except Exception as e:
            # Something else went wrong: request a build
            self.logger.info('%s config_digest cannot be retrieved: %s', image_meta.distgit_key, e)
            self.add_image_meta_change(
                image_meta, RebuildHint(RebuildHintCode.CONFIG_CHANGE, 'Unable to retrieve config_digest')
            )

    @skip_check_if_changing
    async def scan_dependency_changes(self, image_meta: ImageMetadata):
        # Get rebase time from image latest build record
        build_record = self.latest_image_build_records_map[image_meta.distgit_key]
        rebase_time = isolate_timestamp_in_release(build_record.release)
        if not rebase_time:  # no timestamp string in NVR?
            self.logger.warning('No rebase timestamp string in %s, skipping dependency check', build_record.nvr)
            return
        rebase_time = datetime.strptime(rebase_time, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

        # Dependencies are parent images, builders of type member, and operands.
        dependencies = image_meta.dependencies.copy()
        base_image = image_meta.config['from'].member
        if base_image:
            dependencies.add(base_image)
        for builder in image_meta.config['from'].builder:
            if builder.member:
                dependencies.add(builder.member)
        self.logger.info('Checking dependencies of %s: %s', image_meta.distgit_key, ','.join(dependencies))

        for dep_key in dependencies:
            # Is the image dependency included in doozer --images list?
            if not self.runtime.image_map.get(dep_key, None):
                self.logger.warning(
                    "Image %s has unknown dependency %s. Is it excluded?", image_meta.distgit_key, dep_key
                )
                continue

            # Is the dependency ever been built?
            dependency_build_record = self.latest_image_build_records_map.get(dep_key, None)
            if not dependency_build_record:
                self.logger.warning('Dependency %s of image %s has never been built', dep_key, image_meta.distgit_key)
                continue

            # Is the dependency build newer than the dependent's one?
            dep_rebase_time = isolate_timestamp_in_release(dependency_build_record.release)
            if not dep_rebase_time:  # no timestamp string in NVR?
                self.logger.warning(
                    'Could not determine dependency rebase time from release %s', dependency_build_record.release
                )
                continue

            dep_rebase_time = datetime.strptime(dep_rebase_time, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            if dep_rebase_time > rebase_time:
                self.add_image_meta_change(
                    image_meta, RebuildHint(RebuildHintCode.DEPENDENCY_NEWER, 'Dependency has a newer build')
                )
                break

    @alru_cache
    async def get_builder_build_nvr(self, builder_image_name: str):
        """
        Given a builder stream definition,
        """

        if "." in builder_image_name.split('/', 2)[0]:
            # looks like full pullspec with domain name; e.g. "registry.redhat.io/ubi8/nodejs-12:1-45"
            builder_image_url = builder_image_name
        else:
            # Assume this is a org/repo name relative to brew; e.g. "openshift/ose-base:ubi8"
            builder_image_url = self.runtime.resolve_brew_image_url(builder_image_name)

        # Find and map the builder image NVR
        # Use KONFLUX_OPERATOR_INDEX_AUTH_FILE for non-openshift groups (like OADP), otherwise use default
        # Since OADP et. al. uses other streams like https://github.com/openshift-eng/ocp-build-data/blob/oadp-1.5/streams.yml#L13
        builder_auth_file = (
            (os.getenv("KONFLUX_OPERATOR_INDEX_AUTH_FILE") or self.registry_auth_file)
            if not self.runtime.group.startswith("openshift-")
            else self.registry_auth_file
        )
        latest_builder_image_info = Model(
            await oc_image_info_for_arch_async__caching(builder_image_url, registry_config=builder_auth_file)
        )
        builder_info_labels = latest_builder_image_info.config.config.Labels
        builder_nvr_list = [
            builder_info_labels['com.redhat.component'],
            builder_info_labels['version'],
            builder_info_labels['release'],
        ]

        if not all(builder_nvr_list):
            raise IOError(f'Unable to find nvr in {builder_info_labels}')

        builder_image_nvr = '-'.join(builder_nvr_list)

        return builder_image_nvr

    @alru_cache
    async def get_builder_build_start_time(self, builder_build_nvr: str) -> Optional[datetime]:
        """
        Given a builder pullspec, determine the build start time.
        First check if the build is tracked inside the Konflux DB.
        If it's not, query Brew API to get the Brew build result. Querying Brew API will eventually go away.
        """

        # Look for the build record in Konflux DB. BigQuery is partitioned by start_time, so we need a reasonable
        # time interval to look at. In most cases, we can infer the builder build date from its NVR, and use that
        # as the search window lower boundary. In all other cases (e.g. nodejs builder, which has a NVR like
        # nodejs-18-container-1-98), we can only use a default, broad search window. This is an expensive query,
        # so an option might be to store this information in Redis
        nvr_timestamp = isolate_timestamp_in_release(builder_build_nvr)
        if nvr_timestamp:
            start_search = datetime.strptime(nvr_timestamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        else:
            # Default search window: last 365 days
            self.logger.warning('Could not extract timestamp from NVR %s', builder_build_nvr)
            start_search = datetime.now(tz=timezone.utc) - timedelta(days=365)

        build = await anext(
            self.runtime.konflux_db.search_builds_by_fields(
                start_search=start_search,
                where={
                    'nvr': builder_build_nvr,
                    'group': self.runtime.group,
                    'assembly': self.runtime.assembly,
                },
                limit=1,
            ),
            None,
        )
        if build:
            return build.start_time

        # Builder build isn't tracked inside Konflux DB: look at Brew
        with self.runtime.pooled_koji_client_session() as koji_api:
            builder_brew_build = koji_api.getBuild(builder_build_nvr)
            if builder_brew_build:
                return dateutil.parser.parse(builder_brew_build['creation_time']).replace(tzinfo=timezone.utc)

            # No builder build info?
            self.logger.warning('Could not fetch build info for %s', builder_build_nvr)

    @skip_check_if_changing
    async def scan_builders_changes(self, image_meta: ImageMetadata):
        """
        Check whether non-member builder images have changed
        """

        build_record = self.latest_image_build_records_map[image_meta.distgit_key]
        builders = list(image_meta.config['from'].builder) or []
        builders.append(image_meta.config['from'])

        for builder in builders:
            if builder.member:
                # Member builder changes are already being propagated to descendants: skip
                continue

            # Resolve builder build NVR
            if builder.image:
                builder_image_name = builder.image
            elif builder.stream:
                builder_image_name = self.runtime.resolve_stream(builder.stream).image
            else:
                raise IOError(f'Unable to determine builder or parent image pullspec from {builder}')
            builder_build_nvr = await self.get_builder_build_nvr(builder_image_name)

            # Get the builder build start time
            builder_build_start_time = await self.get_builder_build_start_time(builder_build_nvr)
            if not builder_build_start_time:
                continue

            # If the builder build is newer, mark the image as changing
            if build_record.start_time < builder_build_start_time:
                self.logger.info(
                    '%s will be rebuilt because a builder or parent image has a newer build: %s',
                    image_meta.distgit_key,
                    builder_build_nvr,
                )
                self.add_image_meta_change(
                    image_meta,
                    RebuildHint(
                        RebuildHintCode.BUILDER_CHANGING,
                        f'A builder or parent image build {builder_build_nvr} is newer than latest '
                        f'{image_meta.distgit_key} build',
                    ),
                )
                return

    @skip_check_if_changing
    async def scan_rpm_changes(self, image_meta: ImageMetadata):
        # Check if the build used any of the ART built rpms that are changing
        build_record = self.latest_image_build_records_map[image_meta.distgit_key]
        for rpm in self.changing_rpm_names:
            if rpm in {parse_nvr(package)['name']: package for package in build_record.installed_packages}:
                self.add_image_meta_change(
                    image_meta,
                    RebuildHint(RebuildHintCode.PACKAGE_CHANGE, f'Image includes {rpm} which is also about to change'),
                )
                return

        # Check for changes in non-ART RPMs
        build_record_inspector = KonfluxBuildRecordInspector(self.runtime, build_record)
        non_latest_rpms = await build_record_inspector.find_non_latest_rpms(self.package_rpm_finder)
        rebuild_hints = [
            f"Outdated RPM {installed_rpm} installed in {build_record.nvr} ({arch}) when {latest_rpm} was available in repo {repo}"
            for arch, non_latest in non_latest_rpms.items()
            for installed_rpm, latest_rpm, repo in non_latest
        ]
        if rebuild_hints:
            self.add_image_meta_change(
                image_meta, RebuildHint(RebuildHintCode.PACKAGE_CHANGE, ";\n".join(rebuild_hints))
            )
        else:
            self.logger.info('No package changes detected for %s', build_record.nvr)

    @skip_check_if_changing
    async def scan_extra_packages(self, image_meta: ImageMetadata):
        """
        Very rarely, an image might need to pull a package that is not actually installed in the
        builder image or in the final image.
        e.g. https://github.com/openshift/ironic-ipa-downloader/blob/999c80f17472d5dbbd4775d901e1be026b239652/Dockerfile.ocp#L11-L14
        This is programmatically undetectable through koji queries. So we allow extra scan-sources hints to
        be placed in the image metadata.
        """

        extra_packages = self.runtime.group_config.config.scan_sources.extra_packages
        if extra_packages is Missing:
            return

        with self.runtime.pooled_koji_client_session() as koji_api:
            for package_details in extra_packages:
                extra_package_name = package_details.name
                extra_package_brew_tag = package_details.tag

                # Example of queryHistory: https://gist.github.com/jupierce/943b845c07defe784522fd9fd76f4ab0
                extra_latest_tagging_infos = koji_api.queryHistory(
                    table='tag_listing', tag=extra_package_brew_tag, package=extra_package_name, active=True
                )['tag_listing']

                if not extra_latest_tagging_infos:
                    self.logger.warning(
                        f'{image_meta.distgit_key} unable to find tagging event for for extra_packages '
                        f'{extra_package_name} in tag {extra_package_brew_tag} ; Possible metadata error.'
                    )
                    continue

                extra_latest_tagging_infos.sort(key=lambda event: event['create_event'])

                # We have information about the most recent time this package was tagged into the
                # relevant tag. Why the tagging event and not the build time? Well, the build could have been
                # made long ago, but only tagged into the relevant tag recently.
                extra_latest_tagging_event = extra_latest_tagging_infos[-1]['create_event']

                # Convert the Brew event to a timestamp
                result = koji_api.getEvent(extra_latest_tagging_event)
                extra_latest_tagging_timestamp = datetime.fromtimestamp(result['ts'], tz=timezone.utc)

                # Compare that with the Konflux build time
                build_record = self.latest_image_build_records_map[image_meta.distgit_key]
                if extra_latest_tagging_timestamp > build_record.start_time:
                    return self, RebuildHint(
                        RebuildHintCode.PACKAGE_CHANGE,
                        f'Image {image_meta.distgit_key} is sensitive to extra_packages {extra_package_name} '
                        f'which changed at event {extra_latest_tagging_event}',
                    )

    def _extract_task_bundles_from_attestation(self, attestation: Dict) -> Dict[str, str]:
        """
        Extract task bundles from SLSA attestation materials.
        Returns a dict mapping task names to their SHA256 digests.
        """
        try:
            # Parse attestation to get materials (task bundles)
            # For example
            # $ cosign download attestation <build pullspec> | jq -r ' .payload | @base64d | fromjson | .predicate.materials'
            # [{
            #     "digest": {
            #       "sha256": "4a601aeec58a1dd89c271e728fd8f0d84777825b46940c3aec27f15bab3edacf"
            #     },
            #     "uri": "quay.io/konflux-ci/tekton-catalog/task-git-clone-oci-ta"
            #   },
            # ...]
            materials = attestation["predicate"]["materials"]

        except Exception as e:
            self.logger.warning("Failed to parse SLSA attestation for task bundle check: %s", e)
            return {}

        # Extract tekton-catalog task bundles
        self.logger.info(f'Extracting task bundles from {len(materials)} materials in SLSA attestation')
        task_bundles = {}
        for material in materials:
            uri = material.get("uri", "")
            if "quay.io/konflux-ci/tekton-catalog/" in uri:
                task_name = uri.split("/")[-1]
                task_sha = material.get("digest", {}).get("sha256", "")
                if task_sha:
                    task_bundles[task_name] = task_sha

        return task_bundles

    async def check_task_bundle_age(self, task_name: str, used_sha: str, current_sha: str):
        """
        Check if a single task bundle is old enough to warrant a rebuild.
        Returns a tuple with task info and age if old enough, None otherwise.
        """
        self.logger.info(
            f'Task bundle {task_name} version differs: used={used_sha[:12]}... vs current={current_sha[:12]}...'
        )

        task_age_days = await self.get_task_bundle_age_days(task_name, used_sha)
        if not task_age_days:
            return None

        if task_age_days >= TASK_BUNDLE_AGE_THRESHOLD_DAYS:
            return task_name, used_sha, current_sha, task_age_days
        else:
            self.logger.info(
                f'Task bundle {task_name} is only {task_age_days} days old (< {TASK_BUNDLE_AGE_THRESHOLD_DAYS} days), skipping rebuild'
            )
            return None

    @skip_check_if_changing
    async def scan_task_bundle_changes(self, image_meta: ImageMetadata):
        """
        Check if task bundles used in the build are outdated compared to current versions
        and if old task bundles are more than {TASK_BUNDLE_AGE_THRESHOLD_DAYS} days old, trigger a rebuild.
        """
        # Skip if image is not being released
        for_release = image_meta.config.for_release
        if for_release is False:
            self.logger.info(f"Skipping scanning task bundle for {image_meta.distgit_key} since its unreleased")
            return

        self.logger.info(f'Scanning task bundle changes for {image_meta.distgit_key}')
        build_record = self.latest_image_build_records_map[image_meta.distgit_key]

        # Fetch SLSA attestation
        attestation = await fetch_slsa_attestation(
            build_record.image_pullspec, build_record.name, self.registry_auth_file
        )
        if not attestation:
            self.logger.warning('Skipping task bundle check for %s', image_meta.distgit_key)
            return

        # Extract task bundles from attestation
        task_bundles = self._extract_task_bundles_from_attestation(attestation)
        if not task_bundles:
            self.logger.info(f'No tekton-catalog task bundles found in {build_record.image_pullspec}')
            return

        self.logger.info(f'Found {len(task_bundles)} task bundles: {list(task_bundles.keys())}')

        # Get current task bundle SHAs from GitHub
        current_task_bundles = self.current_task_bundles
        if not current_task_bundles:
            self.logger.warning('Current task bundle SHAs not available, skipping task bundle check')
            return

        self.logger.info(f'Retrieved {len(current_task_bundles)} current task bundles from GitHub')

        # Check each task bundle for outdated versions
        self.logger.info(f'Comparing task bundle versions for {image_meta.distgit_key}')

        # Filter out task bundles that are up-to-date or not found in current template
        outdated_task_bundles = []
        for task_name, used_sha in task_bundles.items():
            current_sha = current_task_bundles.get(task_name)
            if not current_sha:
                self.logger.info(f'Task {task_name} not found in current template')
                continue

            if used_sha == current_sha:
                self.logger.info(f'Task bundle {task_name} is up to date (SHA: {used_sha[:12]}...)')
                continue

            outdated_task_bundles.append((task_name, used_sha, current_sha))

        if not outdated_task_bundles:
            return

        # Check if any outdated task bundles are old enough and apply staggered rebuild logic once
        # Execute all age checks in parallel
        age_check_results = await asyncio.gather(
            *[
                self.check_task_bundle_age(task_name, used_sha, current_sha)
                for task_name, used_sha, current_sha in outdated_task_bundles
            ]
        )

        # Filter out None results to get only old enough task bundles
        old_outdated_tasks = [result for result in age_check_results if result is not None]

        if not old_outdated_tasks:
            return

        # Apply staggered rebuild logic once for all old outdated tasks
        # Use the oldest task for probability calculation
        oldest_task = max(old_outdated_tasks, key=lambda x: x[3])
        task_name, used_sha, current_sha, task_age_days = oldest_task

        self.logger.info(
            f'Found {len(old_outdated_tasks)} outdated task bundles >= {TASK_BUNDLE_AGE_THRESHOLD_DAYS} days old, '
            f'applying staggered rebuild logic based on oldest task {task_name} ({task_age_days} days old)'
        )

        # Staggered rebuild logic: probability increases as age increases
        #   - At 10 days: 1 in 21 chance (~5%)
        #   - At 15 days: 1 in 16 chance (~6%)
        #   - At 20 days: 1 in 11 chance (~9%)
        #   - At 25 days: 1 in 6 chance (~17%)
        #   - At 29 days: 1 in 2 chance (50%)
        #   - At 30+ days: Always rebuild (100%)
        rebuild_probability_denominator = max(30 - task_age_days, 1)
        probability_percentage = (1.0 / rebuild_probability_denominator) * 100
        random_number = random.randint(1, rebuild_probability_denominator)
        should_rebuild = random_number == 1

        self.logger.info(
            f'Staggered rebuild probability: {probability_percentage:.1f}% (1/{rebuild_probability_denominator}), '
            f'generated random number: {random_number}, rebuild decision: {should_rebuild}'
        )

        if not should_rebuild:
            self.logger.info(
                f'Staggered rebuild logic decided not to rebuild despite {len(old_outdated_tasks)} outdated task bundles '
                f'(probability was {probability_percentage:.1f}% based on oldest task {task_name})'
            )
            return

        # Trigger rebuild using the oldest task as the reason
        self.logger.info(
            f'Triggering rebuild for {image_meta.distgit_key} due to outdated task bundles '
            f'(oldest: {task_name}, {task_age_days} days old)'
        )
        rebuild_hint = RebuildHint(
            RebuildHintCode.TASK_BUNDLE_OUTDATED,
            f'Task bundle {task_name} is {task_age_days} days old (>={TASK_BUNDLE_AGE_THRESHOLD_DAYS} days) '
            f'and newer version is available (staggered rebuild)',
        )
        self.add_image_meta_change(image_meta, rebuild_hint)

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(5))
    async def get_current_task_bundle_shas(self) -> Dict[str, str]:
        """
        Fetch current task bundle SHAs from the art-konflux-template GitHub repository
        """
        self.logger.info(f'Fetching task bundle template from {KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL}')
        try:
            async with self.session.get(
                KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL, headers={'Authorization': f'Bearer {self.github_token}'}
            ) as response:
                response.raise_for_status()
                yaml_content = await response.text()

            # Parse YAML to extract task bundle references
            self.logger.info('Parsing YAML content to extract task bundle references')
            yaml_data = yaml.safe_load(yaml_content)
            task_bundles = {}

            # Look for task references in the YAML
            self._extract_task_refs(yaml_data, task_bundles)
            self.logger.info(f'Successfully extracted {len(task_bundles)} task bundle references from GitHub template')
            return task_bundles

        except Exception as e:
            self.logger.error(f'Failed to fetch current task bundle SHAs: {e}')
            return {}

    def _extract_task_refs(self, obj, task_bundles: Dict[str, str]):
        """
        Recursively extract task bundle references from YAML data.
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == 'taskRef' and isinstance(value, dict):
                    resolver = value.get('resolver')
                    if resolver == 'bundles':
                        params = value.get('params', [])
                        name_param = next((p for p in params if p.get('name') == 'name'), None)
                        bundle_param = next((p for p in params if p.get('name') == 'bundle'), None)
                        if name_param and bundle_param:
                            bundle_url = bundle_param.get('value', '')
                            if 'quay.io/konflux-ci/tekton-catalog/' in bundle_url and '@sha256:' in bundle_url:
                                # Extract task name from bundle URL like "quay.io/konflux-ci/tekton-catalog/task-init:0.2@sha256:..."
                                # The task name is between the last '/' and the first ':' or '@'
                                url_parts = bundle_url.split('/')
                                if len(url_parts) >= 4:
                                    task_part = url_parts[-1]  # e.g., "task-init:0.2@sha256:..."
                                    actual_task_name = (
                                        task_part.split(':')[0] if ':' in task_part else task_part.split('@')[0]
                                    )
                                    sha = bundle_url.split('@sha256:')[1]
                                    task_bundles[actual_task_name] = sha
                else:
                    self._extract_task_refs(value, task_bundles)
        elif isinstance(obj, list):
            for item in obj:
                self._extract_task_refs(item, task_bundles)

    async def get_task_bundle_age_days(self, task_name: str, sha: str) -> Optional[int]:
        """
        Get the age of a task bundle in days using oc image info
        """
        pullspec = f"quay.io/konflux-ci/tekton-catalog/{task_name}@sha256:{sha}"
        self.logger.info(f'Getting age for task bundle {task_name} using pullspec {pullspec}')
        try:
            cmd = f"oc image info {pullspec} -o json"
            _, out, _ = await cmd_gather_async(cmd)

            image_info = json.loads(out)
            created_str = image_info.get('config', {}).get('created', '')
            if not created_str:
                return None

            # Parse creation time and calculate age
            created_time = datetime.fromisoformat(created_str.rstrip('Z')).replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            age_days = (now - created_time).days

            self.logger.info(
                f'Task bundle {task_name} was created on {created_time.strftime("%Y-%m-%d")}, age: {age_days} days'
            )
            return age_days

        except Exception as e:
            self.logger.warning(f'Failed to get age for task bundle {task_name}@{sha}: {e}')
            return None

    async def check_changing_rpms(self):
        """
        For each RPM built by ART, determine if the current upstream source commit hash
        has a successful build associated with it. As of 12/2024, RPMs are still being built in Brew
        but ART is tracking the build records in the Konflux DB
        """

        async def find_rpm_commit_hash(rpm: RPMMetadata):
            with Dir(rpm.distgit_repo().source_path()):
                try:
                    _, out, _ = await cmd_gather_async(['git', 'rev-parse', 'HEAD'], cwd=Dir.getcwd())
                    return out.strip()

                except Exception as e:
                    raise DoozerFatalError('Could not determine commitish for rpm %s: %s', rpm.rpm_name, e)

        async def check_rpm_target(rpm_meta: RPMMetadata, el_target):
            rpm_name = rpm_meta.name
            self.logger.info('Checking %s changes in target %s', rpm_name, el_target)
            latest_build_record = self.latest_rpm_build_records_map.get(rpm_name, {}).get(el_target, None)

            # RPM has never been built
            if not latest_build_record:
                self.logger.warning('No build found for RPM %s in %s', rpm_name, self.runtime.group)
                self.add_rpm_meta_change(
                    rpm_meta,
                    RebuildHint(
                        code=RebuildHintCode.NO_LATEST_BUILD,
                        reason=f'Component {rpm_name} has no latest build for assembly {self.runtime.assembly}',
                    ),
                )
                return

            # Check if most recent build failed
            latest_failed_build_record = await rpm_meta.get_latest_build(
                el_target=el_target, engine=Engine.BREW.value, outcome=KonfluxBuildOutcome.FAILURE
            )
            rebuild_interval = self.runtime.group_config.scan_freshness.threshold_hours or 6
            now = datetime.now(timezone.utc)

            if latest_failed_build_record and latest_failed_build_record.start_time > latest_build_record.start_time:
                # There is a failed build more recent than the latest successful one
                self.logger.warning('Latest build for RPM %s in %s has failed', rpm_name, self.runtime.group)

                if latest_failed_build_record.start_time + timedelta(hours=rebuild_interval) > now:
                    # Latest failed build is too recent: delay next attempt
                    self.add_rpm_meta_change(
                        rpm_meta,
                        RebuildHint(
                            code=RebuildHintCode.DELAYING_NEXT_ATTEMPT,
                            reason=f'Waiting at least {rebuild_interval} hours after last failed build',
                        ),
                    )

                else:
                    # It's been long enough since the last failed build: try again
                    self.add_rpm_meta_change(
                        rpm_meta,
                        RebuildHint(
                            code=RebuildHintCode.LAST_BUILD_FAILED,
                            reason=f'Latest build {latest_failed_build_record.nvr} for {rpm_name} has failed',
                        ),
                    )
                return

            # Scan for any build in this assembly which includes the git commit.
            upstream_commit_hash = await find_rpm_commit_hash(rpm_meta)
            upstream_commit_build_record = await rpm_meta.get_latest_build(
                el_target=el_target, extra_patterns={'commitish': upstream_commit_hash}, engine=Engine.BREW.value
            )

            if not upstream_commit_build_record:
                self.logger.warning('No build for RPM %s from upstream commit %s', rpm_name, upstream_commit_hash)
                self.add_rpm_meta_change(
                    rpm_meta,
                    RebuildHint(
                        code=RebuildHintCode.NEW_UPSTREAM_COMMIT,
                        reason=f'New upstream commit {upstream_commit_hash} for {rpm_name} needs to be built',
                    ),
                )
                return

            # Does most recent build match the one from the latest upstream commit?
            # If it doesn't, mark for rebuild
            if latest_build_record.commitish != upstream_commit_build_record.commitish:
                self.logger.warning(
                    'Latest build for RPM %s does not match upstream commit %s', rpm_name, upstream_commit_hash
                )
                self.add_rpm_meta_change(
                    rpm_meta,
                    RebuildHint(
                        code=RebuildHintCode.UPSTREAM_COMMIT_MISMATCH,
                        reason=f'Latest build {latest_build_record.nvr} does not match upstream commit build '
                        f'{upstream_commit_build_record.nvr}; commit reverted?',
                    ),
                )
                return

        tasks = []
        for rpm_meta in self.runtime.rpm_metas():
            if rpm_meta.config.targets:
                tasks.extend(
                    [check_rpm_target(rpm_meta, f'el{target}') for target in rpm_meta.determine_rhel_targets()]
                )
            else:
                tasks.extend(
                    [
                        check_rpm_target(rpm_meta, f'el{artcommonlib.util.isolate_el_version_in_brew_tag(target)}')
                        for target in self.runtime.group_config.build_profiles.rpm.default.targets
                    ]
                )
        await asyncio.gather(*tasks)

    def add_assessment_reason(self, meta, rebuild_hint: RebuildHint):
        # qualify by whether this is a True or False for change so that we can store both in the map.
        key = f'{meta.qualified_key}+{rebuild_hint.rebuild}'
        # If the key is already there, don't replace the message as it is likely more interesting
        # than subsequent reasons (e.g. changing because of ancestry)
        if key not in self.assessment_reason:
            self.assessment_reason[key] = rebuild_hint.reason

    def add_image_meta_change(self, meta: ImageMetadata, rebuild_hint: RebuildHint):
        # If the rebuild hint does not require a rebuild, do nothing
        if not rebuild_hint.rebuild:
            return

        self.changing_image_names.add(meta.distgit_key)
        self.add_assessment_reason(meta, rebuild_hint)

        # Mark all descendants for rebuild, so to prevent redundant scans
        for descendant_meta in meta.get_descendants():
            self.changing_image_names.add(descendant_meta.distgit_key)
            self.add_assessment_reason(
                descendant_meta,
                RebuildHint(RebuildHintCode.ANCESTOR_CHANGING, f'Ancestor {meta.distgit_key} is changing'),
            )

    def add_rpm_meta_change(self, meta: RPMMetadata, rebuild_hint: RebuildHint):
        # If the rebuild hint does not require a rebuild, do nothing
        if not rebuild_hint.rebuild:
            return

        self.changing_rpm_names.add(meta.distgit_key)
        self.add_assessment_reason(meta, rebuild_hint)

    def is_image_enabled(self, image_name: str) -> bool:
        image_meta = self.runtime.image_map[image_name]
        mode = image_meta.config.konflux.mode
        enabled = mode != 'disabled' and mode != 'wip'
        if not enabled:
            self.logger.warning('Excluding image %s from the report as it is not enabled in Konflux', image_name)
        return enabled

    async def detect_rhcos_status(self):
        """
        gather the existing RHCOS tags and compare them to latest rhcos builds. Also check outdated rpms in builds
        @return a list of status entries like:
            {
                'name': "4.2-x86_64-priv",
                'changed': False,
                'reason': "could not find an RHCOS build to sync",
            }
        """
        statuses = []

        version = self.runtime.get_minor_version()
        primary_container = get_primary_container_name(self.runtime)
        for arch in self.runtime.arches:
            brew_arch = brew_arch_for_go_arch(arch)
            for private in (False, True):
                status = dict(name=f"{version}-{brew_arch}{'-priv' if private else ''}")
                if self.runtime.group_config.rhcos.get("layered_rhcos", False):
                    tagged_rhcos_value = self.tagged_rhcos_node_digest(primary_container, version, brew_arch, private)
                    latest_rhcos_value = self.latest_rhcos_node_shasum(arch)
                else:
                    tagged_rhcos_value = self.tagged_rhcos_id(primary_container, version, brew_arch, private)
                    latest_rhcos_value = self.latest_rhcos_build_id(version, brew_arch, private)

                if latest_rhcos_value and tagged_rhcos_value != latest_rhcos_value:
                    status['updated'] = True
                    status['changed'] = True
                    status['reason'] = (
                        f"latest RHCOS build is {latest_rhcos_value} which differs from istag {tagged_rhcos_value}"
                    )
                    statuses.append(status)
                # check outdate rpms in rhcos
                pullspec_for_tag = dict()
                build_id = ""
                for container_conf in self.runtime.group_config.rhcos.payload_tags:
                    build_id, pullspec = rhcos.RHCOSBuildFinder(
                        self.runtime, version, brew_arch, private
                    ).latest_container(container_conf)
                    pullspec_for_tag[container_conf.name] = pullspec
                non_latest_rpms = await rhcos.RHCOSBuildInspector(
                    self.runtime, pullspec_for_tag, brew_arch, build_id
                ).find_non_latest_rpms(exclude_rhel=True)
                non_latest_rpms_filtered = []

                # exclude rpm if non_latest_rpms in rhel image rpm list
                exclude_rpms = self.runtime.group_config.rhcos.get("exempt_rpms", [])
                for installed_rpm, latest_rpm, repo in non_latest_rpms:
                    if any(excluded in installed_rpm for excluded in exclude_rpms):
                        self.logger.info(
                            f"[EXEMPT SKIPPED] Exclude {installed_rpm} because its in the exempt list when {latest_rpm} was available in repo {repo}"
                        )
                    else:
                        non_latest_rpms_filtered.append((installed_rpm, latest_rpm, repo))
                if non_latest_rpms_filtered:
                    status['outdated'] = True
                    status['changed'] = True
                    status['reason'] = ";\n".join(
                        f"Outdated RPM {installed_rpm} installed in RHCOS ({brew_arch}) when {latest_rpm} was available in repo {repo}"
                        for installed_rpm, latest_rpm, repo in non_latest_rpms_filtered
                    )
                    statuses.append(status)

        self.rhcos_status = statuses

    def tagged_rhcos_node_digest(self, container_name, version, arch, private) -> Optional[str]:
        """get latest coreos image diget from tagged RHCOS in given imagestream"""
        base_namespace = rgp.default_imagestream_namespace_base_name()
        base_name = rgp.default_imagestream_base_name(version, self.runtime)
        namespace, name = rgp.payload_imagestream_namespace_and_name(base_namespace, base_name, arch, private)
        stdout, _ = exectools.cmd_assert(
            f"oc --kubeconfig '{self.ci_kubeconfig}' --namespace '{namespace}' get istag '{name}:{container_name}' -o json",
            retries=3,
            pollrate=5,
            strip=True,
        )

        try:
            istagdata = json.loads(stdout)
            # shasum format is sha256:66d827b7f70729ca9dc6f7a2358df8fb37c82380cf36ca9653efff8605cf3a82
            shasum = istagdata['image']['metadata']['name']
        except KeyError:
            self.logger.error('Could not find .metadata.name in RHCOS imagestream:\n%s', stdout)
            raise

        return shasum

    def latest_rhcos_node_shasum(self, arch) -> Optional[str]:
        """get latest node image from quay.io/openshift-release-dev/ocp-v4.0-art-dev:4.x-9.x-node-image"""
        go_arch = go_arch_for_brew_arch(arch)
        rhcos_index = next(
            (tag.rhcos_index_tag for tag in self.runtime.group_config.rhcos.payload_tags if tag.primary), ""
        )
        rhcos_info = util.oc_image_info_for_arch(rhcos_index, go_arch)
        return rhcos_info['digest']

    def tagged_rhcos_id(self, container_name, version, arch, private) -> Optional[str]:
        """determine the most recently tagged RHCOS in given imagestream"""
        base_namespace = rgp.default_imagestream_namespace_base_name()
        base_name = rgp.default_imagestream_base_name(version, self.runtime)
        namespace, name = rgp.payload_imagestream_namespace_and_name(base_namespace, base_name, arch, private)
        stdout, _ = exectools.cmd_assert(
            f"oc --kubeconfig '{self.ci_kubeconfig}' --namespace '{namespace}' get istag '{name}:{container_name}' -o json",
            retries=3,
            pollrate=5,
            strip=True,
        )

        try:
            istagdata = json.loads(stdout)
            labels = istagdata['image']['dockerImageMetadata']['Config']['Labels']
        except KeyError:
            self.logger.error(
                'Could not find .image.dockerImageMetadata.Config.Labels in RHCOS imageMetadata:\n%s', stdout
            )
            raise

        build_id = None
        if not (build_id := labels.get('org.opencontainers.image.version', None)):
            build_id = labels.get('version', None)

        return build_id

    def latest_rhcos_build_id(self, version, arch, private) -> Optional[str]:
        """
        Wrapper to return None if anything goes wrong, which will be taken as no change
        """

        try:
            return rhcos.RHCOSBuildFinder(self.runtime, version, arch, private).latest_rhcos_build_id()

        except rhcos.RHCOSNotFound as ex:
            # don't let flakiness in rhcos lookups prevent us from scanning regular builds;
            # if anything else changed it will sync anyway.
            self.logger.warning(
                f"could not determine RHCOS build for {version}-{arch}{'-priv' if private else ''}: {ex}"
            )
            return None

    def generate_report(self):
        image_results = []

        # Filter out images that are disabled or wip at the konflux level
        changing_image_names = list(
            filter(lambda image_name: self.is_image_enabled(image_name), self.changing_image_names)
        )
        for image_meta in self.all_image_metas:
            dgk = image_meta.distgit_key
            is_changing = dgk in changing_image_names
            if is_changing:
                image_results.append(
                    {
                        'name': dgk,
                        'changed': is_changing,
                        'reason': self.assessment_reason.get(f'{image_meta.qualified_key}+{is_changing}'),
                    }
                )

        rpm_results = []
        for rpm_meta in self.all_rpm_metas:
            dgk = rpm_meta.distgit_key
            is_changing = dgk in self.changing_rpm_names
            if is_changing:
                rpm_results.append(
                    {
                        'name': dgk,
                        'changed': is_changing,
                        'reason': self.assessment_reason.get(f'{rpm_meta.qualified_key}+{is_changing}'),
                    }
                )

        results = dict(
            images=image_results,
            rpms=rpm_results,
            rhcos=self.rhcos_status,
        )

        self.logger.debug(f'scan-sources coordinate: results:\n{yaml.safe_dump(results, indent=4)}')

        if self.as_yaml:
            click.echo('---')
            results['issues'] = self.issues
            click.echo(yaml.safe_dump(results, indent=4))
        else:
            # Log change results
            for kind, items in results.items():
                if not items:
                    continue
                click.echo(kind.upper() + ":")
                for item in items:
                    click.echo(
                        '  {} is {} (reason: {})'.format(
                            item['name'], 'changed' if item['changed'] else 'the same', item['reason']
                        )
                    )
            # Log issues
            click.echo("ISSUES:")
            for item in self.issues:
                click.echo(f"   {item['name']}: {item['issue']}")


@cli.command("beta:config:konflux:scan-sources", short_help="Determine if any rpms / images need to be rebuilt.")
@click.option(
    "--ci-kubeconfig",
    metavar='KC_PATH',
    required=True,
    help="File containing kubeconfig for looking at release-controller imagestreams",
)
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@click.option("--rebase-priv", default=False, is_flag=True, help='Try to reconcile public upstream into openshift-priv')
@click.option('--dry-run', default=False, is_flag=True, help='Do not actually perform reconciliation, just log it')
@click_coroutine
@pass_runtime
async def config_scan_source_changes_konflux(runtime: Runtime, ci_kubeconfig, as_yaml, rebase_priv, dry_run):
    """
    Determine if any rpms / images need to be rebuilt.

    \b
    The method will report RPMs in this group if:
    - Their source git hash no longer matches their upstream source.
    - The buildroot used by the previous RPM build has changed.

    \b
    It will report images if the latest build:
    - Contains an RPM that is about to be rebuilt based on the RPM check above.
    - If the source git hash no longer matches the upstream source.
    - Contains any RPM (from anywhere in Red Hat) which has likely changed since the image was built.
        - This indirectly detects non-member parent image changes.
    - Was built with a buildroot that has now changed (probably not useful for images, but was cheap to add).
    - Used a builder image (from anywhere in Red Hat) that has changed.
    - Used a builder image from this group that is about to change.
    - If the associated member is a descendant of any image that needs change.

    \b
    It will report RHCOS updates available per imagestream.
    """

    # Initialize group config: we need this to determine the canonical builders behavior
    runtime.initialize(config_only=True)
    runtime.initialize(mode='both', clone_distgits=False)

    async with aiohttp.ClientSession() as session:
        await ConfigScanSources(
            runtime=runtime,
            ci_kubeconfig=ci_kubeconfig,
            as_yaml=as_yaml,
            rebase_priv=rebase_priv,
            dry_run=dry_run,
            session=session,
        ).run()
