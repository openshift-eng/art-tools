import asyncio
import logging
import os
import tempfile
import typing
from datetime import datetime, timezone, timedelta

import click
import yaml
from typing import cast

import artcommonlib.util
from artcommonlib import exectools
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord, Engine, KonfluxBuildOutcome
from artcommonlib.model import Missing
from artcommonlib.pushd import Dir
from doozerlib.cli import cli, pass_runtime, click_coroutine
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.metadata import RebuildHint, RebuildHintCode, Metadata
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolver

DEFAULT_THRESHOLD_HOURS = 6


class ConfigScanSources:
    def __init__(self, runtime: Runtime, ci_kubeconfig: str, as_yaml: bool,
                 rebase_priv: bool = False, dry_run: bool = False):
        if runtime.konflux_db is None:
            raise DoozerFatalError('Cannot run scan-sources without a valid Konflux DB connection')
        runtime.konflux_db.bind(KonfluxBuildRecord)

        self.github_token = os.getenv('GITHUB_TOKEN')
        if not self.github_token:
            raise DoozerFatalError("GITHUB_TOKEN environment variable must be set")

        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.ci_kubeconfig = ci_kubeconfig
        self.as_yaml = as_yaml
        self.rebase_priv = rebase_priv
        self.dry_run = dry_run

        self.all_rpm_metas = set(runtime.rpm_metas())
        self.all_image_metas = set(runtime.image_metas())
        self.all_metas = self.all_rpm_metas.union(self.all_image_metas)

        self.changing_image_names = set()
        self.assessment_reason = dict()  # maps metadata qualified_key => message describing change
        self.issues = list()  # tracks issues that arose during the scan, which did not interrupt the job

        # Common params for all queries
        self.base_search_params = {
            'group': self.runtime.group,
            'assembly': self.runtime.assembly,  # to let test ocp4-scan in non-stream assemblies, e.g. 'test'
        }

        self.latest_image_build_records_map: typing.Dict[str, KonfluxBuildRecord] = {}
        self.image_tree = {}

    async def run(self):
        # Try to rebase into openshift-priv to reduce upstream merge -> downstream build time
        if self.rebase_priv:
            # TODO: to be removed once this job is the only one we use for scanning
            raise DoozerFatalError('ocp4-scan for Konflux is not yet allowed to rebase into openshfit-priv!')
            self.rebase_into_priv()

        # Build an image dependency tree to scan across levels of inheritance. This should save us some time,
        # as when an image is found in need for a rebuild, we can also mark its children or operators without checking
        self.image_tree = self.generate_dependency_tree(self.runtime.image_tree)
        for level in sorted(self.image_tree.keys()):
            await self.scan_images(self.image_tree[level])

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
            rc, _, _ = exectools.cmd_gather(cmd=['git', 'merge', f'public_upstream/{pub_branch_name}',
                                                 '-m', f'Reconciled {repo_name} with public upstream'],
                                            log_stderr=True,
                                            log_stdout=True)
            if not rc:
                # merge succeeded, will push to openshift-priv
                reconciled = True
                self.logger.info('Merged public_upstream/%s into %s', priv_branch_name, metadata.name)

        if not reconciled:
            # Could not rebase from public upstream: need manual reconciliation. Log a warning and return
            self.logger.warning('failed rebasing %s from public upstream: will need manual reconciliation',
                                metadata.name)
            self.issues.append({'name': metadata.distgit_key,
                                'issue': 'Could not rebase into -priv as it needs manual reconciliation'})
            return

        if self.dry_run:
            self.logger.info('Would have tried reconciliation for %s/%s', repo_name, priv_branch_name)
            return

        # Try to push to openshift-priv
        try:
            exectools.cmd_assert(
                cmd=['git', 'push', 'origin', priv_branch_name],
                retries=3)
            self.logger.info('Successfully reconciled %s with public upstream', metadata.name)

        except ChildProcessError:
            # Failed pushing to openshift-priv
            self.logger.warning('failed pushing to openshift-priv for %s', metadata.name)
            self.issues.append({'name': metadata.distgit_key,
                                'issue': 'Failed pushing to openshift-priv'})

    def _do_shas_match(self, public_url, pub_branch_name,
                       priv_url, priv_branch_name) -> bool:
        """
        Use GitHub API to check commit SHAs on private and public upstream for a given branch.
        Return True if they match, False otherwise
        """

        try:
            # Check public commit ID
            out, _ = exectools.cmd_assert(['git', 'ls-remote', public_url, pub_branch_name],
                                          retries=5, on_retry='sleep 5')
            pub_commit = out.strip().split()[0]

            # Check private commit ID
            out, _ = exectools.cmd_assert(['git', 'ls-remote', priv_url, priv_branch_name],
                                          retries=5, on_retry='sleep 5')
            priv_commit = out.strip().split()[0]

        except ChildProcessError:
            self.logger.warning('Could not fetch latest commit SHAs from %s: skipping rebase', public_url)
            return True

        if pub_commit == priv_commit:
            self.logger.info('Latest commits match on public and priv upstreams for %s', public_url)
            return True

        self.logger.info('Latest commits do not match on public and priv upstreams for %s: '
                         'public SHA = %s, private SHA = %s', public_url, pub_commit, priv_commit)
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
            rc, _, _ = exectools.cmd_gather(['git', 'merge-base', '--is-ancestor',
                                             f'public_upstream/{pub_branch_name}', f'origin/{priv_branch_name}'])
        if rc == 1:
            self.logger.info('Public upstream is ahead of private for %s: will need to rebase', repo_name)
            return False
        if rc == 0:
            self.logger.info('Private upstream is ahead of public for %s: no need to rebase', repo_name)
            return True
        raise IOError(f'Could not determine ancestry between public and private upstreams for {repo_name}')

    def rebase_into_priv(self):
        self.logger.info('Rebasing public upstream contents into openshift-priv')
        upstream_mappings = exectools.parallel_exec(
            lambda meta, _: (meta, SourceResolver.get_public_upstream(meta.config.content.source.git.url,
                                                                      self.runtime.group_config.public_upstreams)),
            self.all_metas,
            n_threads=20,
        ).get()

        for metadata, public_upstream in upstream_mappings:
            # Skip rebase for disabled images
            if not metadata.enabled:
                self.logger.warning('%s is disabled: skipping rebase', metadata.name)
                continue

            if metadata.config.content is Missing:
                self.logger.warning('%s %s is a distgit-only component: skipping openshift-priv rebase',
                                    metadata.meta_type, metadata.name)
                continue

            public_url, public_branch_name, has_public_upstream = public_upstream

            # If no public upstream exists, skip the rebase
            if not has_public_upstream:
                self.logger.warning('%s %s does not have a public upstream: skipping openshift-priv rebase',
                                    metadata.meta_type, metadata.name)
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
                self.logger.warning('%s %s does not have a public upstream: skipping openshift-priv rebase',
                                    metadata.meta_type, metadata.name)
                continue

            # First, quick check: if SHAs match across remotes, repo is synced and we can avoid cloning it
            _, public_org, public_repo_name = artcommonlib.util.split_git_url(public_url)
            _, priv_org, priv_repo_name = artcommonlib.util.split_git_url(priv_url)

            if self._do_shas_match(public_url, public_branch_name,
                                   metadata.config.content.source.git.url, priv_branch_name):
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

    async def find_latest_image_builds(self, image_names: typing.List[str]):
        self.logger.info('Gathering latest image build records information...')
        latest_image_builds = await self.runtime.konflux_db.get_latest_builds(
            names=image_names,
            engine=Engine.KONFLUX,
            **self.base_search_params)
        self.latest_image_build_records_map.update((zip(
            image_names, latest_image_builds)))

    async def scan_images(self, image_names: typing.List[str]):
        # Do not scan images that have already been requested for rebuild
        image_names = list(filter(lambda name: name not in self.changing_image_names, image_names))

        # Store latest build records in a map, to reduce DB queries and execution time
        await self.find_latest_image_builds(image_names)

        await asyncio.gather(*[self.scan_image(image_name) for image_name in image_names])

    async def scan_image(self, image_name: str):
        self.logger.info(f'Scanning {image_name} for changes')
        image_meta = self.runtime.image_map[image_name]

        if not (image_meta.enabled or image_meta.mode == 'disabled' and self.runtime.load_disabled):
            # Ignore disabled configs unless explicitly indicated
            # An enabled image's dependents are always loaded.
            return

        # If the component has never been built, mark for rebuild
        latest_build_record = self.latest_image_build_records_map.get(image_name, None)
        if not latest_build_record:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(code=RebuildHintCode.NO_LATEST_BUILD,
                            reason=f'Component {image_name} has no latest build '
                                   f'for assembly {self.runtime.assembly}'))
            return

        # Check if there's already a build from upstream latest commit
        await self.scan_for_upstream_changes(image_meta)

        # Check if there has been a config change since last build
        await self.scan_for_config_changes(image_meta)

    async def scan_for_upstream_changes(self, image_meta: ImageMetadata):
        """
        Determine if the current upstream source commit hash
        has a downstream build associated with it.
        """

        latest_build_record = self.latest_image_build_records_map[image_meta.distgit_key]

        # We have no more "alias" source anywhere in ocp-build-data, and there's no such a thing as a distgit-only
        # component in Konflux; hence, assume that git is the only possible source for a component
        # TODO runtime.stage seems to be never different from False, maybe it can be pruned?
        # TODO use_source_fallback_branch isn't defined anywhere in ocp-build-data, maybe it can be pruned?

        # Check the upstream latest commit hash using git ls-remote
        use_source_fallback_branch = cast(str, self.runtime.group_config.use_source_fallback_branch or "yes")
        _, upstream_commit_hash = SourceResolver.detect_remote_source_branch(image_meta.config.content.source.git,
                                                                             self.runtime.stage,
                                                                             use_source_fallback_branch)

        # Scan for any build in this assembly which includes the git commit.
        upstream_commit_build_record = await self.runtime.konflux_db.get_latest_build(
            **self.base_search_params,
            name=image_meta.distgit_key,
            extra_patterns={'commitish': upstream_commit_hash}  # WHERE commitish LIKE '%{upstream_commit_hash}%'
        )

        # No build from latest upstream commit: handle accordingly
        if not upstream_commit_build_record:
            await self.handle_missing_upstream_commit_build(image_meta, upstream_commit_hash)
            return

        # Does most recent build match the one from the latest upstream commit?
        # If it doesn't, mark for rebuild
        if latest_build_record.nvr != upstream_commit_build_record.nvr:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(code=RebuildHintCode.UPSTREAM_COMMIT_MISMATCH,
                            reason=f'Latest build {latest_build_record.nvr} does not match upstream commit build '
                                   f'{upstream_commit_build_record.nvr}; commit reverted?'))

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
        failed_commit_build_record = await self.runtime.konflux_db.get_latest_build(
            **self.base_search_params,
            name=image_meta.distgit_key,
            extra_patterns={'commitish': upstream_commit_hash},
            outcome=KonfluxBuildOutcome.FAILURE
        )

        # If not, this is a net-new upstream commit. Build it.
        if not failed_commit_build_record:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(code=RebuildHintCode.NEW_UPSTREAM_COMMIT,
                            reason='A new upstream commit exists and needs to be built'))
            return

        # Otherwise, there was a failed attempt at this upstream commit on record.
        # Make sure provide at least rebuild_interval hours between such attempts
        last_attempt_time = failed_commit_build_record.start_time

        # Latest failed attempt is older than the threshold: rebuild
        # Otherwise, delay next build attempt
        if last_attempt_time + timedelta(hours=rebuild_interval) < now:
            self.add_image_meta_change(
                image_meta,
                RebuildHint(code=RebuildHintCode.LAST_BUILD_FAILED,
                            reason=f'It has been {rebuild_interval} hours since last failed build attempt'))

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
                destination=config_digest)

            # Read and return the content of the temporary file
            with open(config_digest) as f:
                return f.read()

    async def scan_for_config_changes(self, image_meta: ImageMetadata):
        try:
            latest_build_record = self.latest_image_build_records_map[image_meta.distgit_key]

            # Look at the digest that created THIS build. What is in head does not matter.
            prev_digest = await self.fetch_config_digest(latest_build_record)

            # Compute the latest config digest
            current_digest = image_meta.calculate_config_digest(self.runtime.group_config, self.runtime.streams)

            if current_digest.strip() != prev_digest.strip():
                self.logger.info('%s config_digest %s is differing from %s',
                                 image_meta.distgit_key, prev_digest, current_digest)
                # fetch latest commit message on branch for the image metadata file
                with Dir(self.runtime.data_dir):
                    path = f'images/{image_meta.config_filename}'
                    rc, commit_message, _ = exectools.cmd_gather(f'git log -1 --format=%s -- {path}', strip=True)
                    if rc != 0:
                        raise IOError(f'Unable to retrieve commit message from {self.runtime.data_dir} for {path}')

                if commit_message.lower().startswith('scan-sources:noop'):
                    self.logger.info('Ignoring digest change since commit message indicates noop')
                else:
                    self.add_image_meta_change(image_meta,
                                               RebuildHint(RebuildHintCode.CONFIG_CHANGE,
                                                           'Metadata configuration change'))

        except IOError:
            # IOError is raised by fetch_cgit_file() when config_digest could not be found
            self.logger.warning('config_digest not found for %s: skipping config check', image_meta.distgit_key)
            return

        except Exception as e:
            # Something else went wrong: request a build
            self.logger.info('%s config_digest cannot be retrieved: %s', image_meta.distgit_key, e)
            self.add_image_meta_change(image_meta,
                                       RebuildHint(RebuildHintCode.CONFIG_CHANGE,
                                                   'Unable to retrieve config_digest'))

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
            self.add_assessment_reason(descendant_meta, RebuildHint(RebuildHintCode.ANCESTOR_CHANGING,
                                                                    f'Ancestor {meta.distgit_key} is changing'))

    def generate_report(self):
        image_results = []
        changing_image_names = [name for name in self.changing_image_names]
        for image_meta in self.all_image_metas:
            dgk = image_meta.distgit_key
            is_changing = dgk in changing_image_names
            if is_changing:
                image_results.append({
                    'name': dgk,
                    'changed': is_changing,
                    'reason': self.assessment_reason.get(f'{image_meta.qualified_key}+{is_changing}')
                })

        results = dict(
            images=image_results
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
                    click.echo('  {} is {} (reason: {})'.format(item['name'],
                                                                'changed' if item['changed'] else 'the same',
                                                                item['reason']))
            # Log issues
            click.echo("ISSUES:")
            for item in self.issues:
                click.echo(f"   {item['name']}: {item['issue']}")


@cli.command("beta:config:konflux:scan-sources", short_help="Determine if any rpms / images need to be rebuilt.")
@click.option("--ci-kubeconfig", metavar='KC_PATH', required=False,
              help="File containing kubeconfig for looking at release-controller imagestreams")
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@click.option("--rebase-priv", default=False, is_flag=True,
              help='Try to reconcile public upstream into openshift-priv')
@click.option('--dry-run', default=False, is_flag=True, help='Do not actually perform reconciliation, just log it')
@click_coroutine
@pass_runtime
async def config_scan_source_changes(runtime: Runtime, ci_kubeconfig, as_yaml, rebase_priv, dry_run):
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

    if runtime.group_config.canonical_builders_from_upstream:
        runtime.initialize(mode="both", clone_distgits=True)
    else:
        runtime.initialize(mode='both', clone_distgits=False)

    await ConfigScanSources(runtime, ci_kubeconfig, as_yaml, rebase_priv, dry_run).run()
