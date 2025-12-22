import asyncio
import json
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import artcommonlib.util
import click
import yaml
from artcommonlib import exectools, release_util
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.constants import KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS
from artcommonlib.model import Missing
from artcommonlib.pushd import Dir
from artcommonlib.rhcos import get_primary_container_name

from doozerlib import brew, rhcos, util
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.cli import release_gen_payload as rgp
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.metadata import Metadata, RebuildHint, RebuildHintCode
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolver


class ConfigScanSources:
    def __init__(
        self, runtime: Runtime, ci_kubeconfig: str, as_yaml: bool, rebase_priv: bool = False, dry_run: bool = False
    ):
        self.runtime = runtime
        self.ci_kubeconfig = ci_kubeconfig
        self.as_yaml = as_yaml
        self.rebase_priv = rebase_priv
        self.dry_run = dry_run

        self.changing_rpm_metas = set()
        self.changing_image_metas = set()
        self.changing_rpm_packages = set()
        self.assessment_reason = dict()  # maps metadata qualified_key => message describing change
        self.issues = list()  # tracks issues that arose during the scan, which did not interrupt the job

        self.all_rpm_metas = set(runtime.rpm_metas())
        self.all_image_metas = set(runtime.image_metas())
        self.all_metas = self.all_rpm_metas.union(self.all_image_metas)

        self.oldest_image_event_ts = None
        self.newest_image_event_ts = 0

    async def run(self):
        with self.runtime.shared_koji_client_session() as koji_api:
            self.runtime.logger.info(
                f'scan-sources coordinate: brew_event: '
                f'{koji_api.getLastEvent(brew.KojiWrapperOpts(brew_event_aware=True))}'
            )
            self.runtime.logger.info(f'scan-sources coordinate: emulated_brew_event: {self.runtime.brew_event}')

            # First, try to rebase into openshift-priv to reduce upstream merge -> downstream build time
            if self.rebase_priv:
                major, minor = self.runtime.get_major_minor_fields()
                version = f'{major}.{minor}'
                if version in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS:
                    self.runtime.logger.warning(
                        'ocp4-scan for Brew is not allowed to rebase into openshfit-priv version %s', version
                    )
                else:
                    self.rebase_into_priv()

            # Then, scan for any upstream source code changes. If found, these are guaranteed rebuilds.
            self.scan_for_upstream_changes(koji_api)

            # Check for other reasons why images should be rebuilt (e.g. config changes, dependencies)
            await self.check_for_image_changes(koji_api)

        # Checks if an image needs to be rebuilt based on the packages it is dependent on
        await self.check_changing_rpms()

        # does_image_need_change() checks whether its non-member builder images have changed
        # but cannot determine whether member builder images have changed until anticipated
        # changes have been calculated. The following function call does this.
        self.check_builder_images()

        # We have our information. Now build and print the output report
        await self.generate_report()

    def _try_reconciliation(self, metadata: Metadata, repo_name: str, pub_branch_name: str, priv_branch_name: str):
        reconciled = False

        # Attempt a fast-forward merge
        rc, _, _ = exectools.cmd_gather(cmd=['git', 'pull', '--ff-only', 'public_upstream', pub_branch_name])
        if not rc:
            # fast-forward succeeded, will push to openshift-priv
            self.runtime.logger.info('Fast-forwarded %s from public_upstream/%s', metadata.name, pub_branch_name)
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
                self.runtime.logger.info('Merged public_upstream/%s into %s', priv_branch_name, metadata.name)

        if not reconciled:
            # Could not rebase from public upstream: need manual reconciliation. Log a warning and return
            self.runtime.logger.warning(
                'failed rebasing %s from public upstream: will need manual reconciliation', metadata.name
            )
            self.issues.append(
                {'name': metadata.distgit_key, 'issue': 'Could not rebase into -priv as it needs manual reconciliation'}
            )
            return

        if self.dry_run:
            self.runtime.logger.info('Would have tried reconciliation for %s/%s', repo_name, priv_branch_name)
            return

        # Try to push to openshift-priv
        try:
            exectools.cmd_assert(cmd=['git', 'push', 'origin', priv_branch_name], retries=3)
            self.runtime.logger.info('Successfully reconciled %s with public upstream', metadata.name)

        except ChildProcessError:
            # Failed pushing to openshift-priv
            self.runtime.logger.warning('failed pushing to openshift-priv for %s', metadata.name)
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
            self.runtime.logger.warning('Could not fetch latest commit SHAs from %s: skipping rebase', public_url)
            return True

        if pub_commit == priv_commit:
            self.runtime.logger.info('Latest commits match on public and priv upstreams for %s', public_url)
            return True

        self.runtime.logger.info(
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
            self.runtime.logger.info('Public upstream is ahead of private for %s: will need to rebase', repo_name)
            return False
        if rc == 0:
            self.runtime.logger.info('Private upstream is ahead of public for %s: no need to rebase', repo_name)
            return True
        raise IOError(f'Could not determine ancestry between public and private upstreams for {repo_name}')

    def rebase_into_priv(self):
        if self.dry_run:
            self.runtime.logger.info('Would have rebased into openshift-priv')
            return

        self.runtime.logger.info('Rebasing public upstream contents into openshift-priv')
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
                self.runtime.logger.warning('%s is disabled: skipping rebase', metadata.name)
                continue

            if metadata.config.content is Missing:
                self.runtime.logger.warning(
                    '%s %s is a distgit-only component: skipping openshift-priv rebase',
                    metadata.meta_type,
                    metadata.name,
                )
                continue

            public_url, public_branch_name, has_public_upstream = public_upstream

            # If no public upstream exists, skip the rebase
            if not has_public_upstream:
                self.runtime.logger.warning(
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
                self.runtime.logger.warning('Target branch for %s is a SHA: skipping rebase', metadata.name)
                continue

            except ValueError:
                # target branch is a normal branch name
                pass

            # If no public_upstreams field exists, public_branch_name will be None
            public_branch_name = public_branch_name or priv_branch_name

            if priv_url == public_url:
                # Upstream repo does not have a public counterpart: no need to rebase
                self.runtime.logger.warning(
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

    def scan_for_upstream_changes(self, koji_api):
        # Determine if the current upstream source commit hash has a downstream build associated with it.
        # Result is a list of tuples, where each tuple contains an rpm or image metadata
        # and a change tuple (changed: bool, message: str).
        upstream_changes: List[Tuple[Metadata, RebuildHint]] = exectools.parallel_exec(
            lambda image_meta, _: (image_meta, image_meta.needs_rebuild()),
            self.all_metas,
            n_threads=20,
        ).get()

        for meta, rebuild_hint in upstream_changes:
            dgk = meta.distgit_key
            if not (meta.enabled or meta.mode == "disabled" and self.runtime.load_disabled):
                # An enabled image's dependents are always loaded.
                # Ignore disabled configs unless explicitly indicated
                continue

            if meta.meta_type == 'rpm':
                package_name = meta.get_package_name()

                # If no change has been detected, check buildroots to see if it has changed
                if not rebuild_hint.rebuild:
                    # A package may contain multiple RPMs; find the oldest one in the latest package build.
                    eldest_rpm_build = None
                    tag = meta.branch() + '-candidate'
                    latest_rpms = koji_api.getLatestRPMS(tag=tag, package=package_name)[1]

                    for latest_rpm_build in latest_rpms:
                        # pylint: disable=unsubscriptable-object
                        if (
                            not eldest_rpm_build
                            or latest_rpm_build['creation_event_id'] < eldest_rpm_build['creation_event_id']
                        ):
                            eldest_rpm_build = latest_rpm_build

                    self.runtime.logger.info(
                        f'Determining build root changes for package {package_name} in tag {tag} and its eldest rpm {eldest_rpm_build}'
                    )
                    # Detect if our buildroot changed since the oldest rpm of the package latest build of was built.
                    build_root_change = brew.has_tag_changed_since_build(
                        self.runtime, koji_api, eldest_rpm_build, meta.build_root_tag(), inherit=True
                    )

                    if build_root_change:
                        rebuild_hint = RebuildHint(
                            RebuildHintCode.BUILD_ROOT_CHANGING, 'Oldest package rpm build was before buildroot change'
                        )
                        self.runtime.logger.info(
                            f'{dgk} ({eldest_rpm_build}) in {package_name} '
                            f'is older than more recent buildroot change: {build_root_change}'
                        )

                if rebuild_hint.rebuild:
                    self.add_assessment_reason(meta, rebuild_hint)
                    self.changing_rpm_metas.add(meta)
                    self.changing_rpm_packages.add(package_name)

            elif meta.meta_type == 'image':
                if rebuild_hint.rebuild:
                    self.add_image_meta_change(meta, rebuild_hint)

            else:
                raise IOError(f'Unsupported meta type: {meta.meta_type}')

    async def check_for_image_changes(self, koji_api):
        async def _inner(image_meta):
            # If a rebuild is already requested, skip following checks
            if image_meta in self.changing_image_metas:
                return

            build_info = await image_meta.get_latest_build(default=None, exclude_large_columns=True)

            if build_info is None:
                return

            # To limit the size of the queries we are going to make, find the oldest and newest image
            self.find_oldest_newest(koji_api, build_info)

            # Request a rebuild if A is a dependent (operator or child image) of B
            # but the latest build of A is older than B.
            self.check_dependents(image_meta, build_info)

            # If no upstream change has been detected, check configurations
            # like image meta, repos, and streams to see if they have changed
            # We detect config changes by comparing their digest changes.
            # The config digest of the previous build is stored at .oit/config_digest on distgit repo.
            self.check_config_changes(image_meta, build_info)

        await asyncio.gather(*[_inner(image_meta) for image_meta in self.runtime.image_metas()])

        self.runtime.logger.debug(
            'Will be assessing tagging changes between newest_image_event_ts:%s and oldest_image_event_ts:%s',
            self.newest_image_event_ts,
            self.oldest_image_event_ts,
        )

    def find_oldest_newest(self, koji_api, build_info):
        create_event_ts = koji_api.getEvent(build_info['creation_event_id'])['ts']
        if self.oldest_image_event_ts is None or create_event_ts < self.oldest_image_event_ts:
            self.oldest_image_event_ts = create_event_ts
        if create_event_ts > self.newest_image_event_ts:
            self.newest_image_event_ts = create_event_ts

    def check_dependents(self, image_meta: ImageMetadata, build_info):
        rebase_time = release_util.isolate_timestamp_in_release(build_info["release"])
        if not rebase_time:  # no timestamp string in NVR?
            return

        rebase_time = datetime.strptime(rebase_time, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        dependencies = image_meta.dependencies.copy()
        base_image = image_meta.config["from"].member

        # Compute dependencies
        if base_image:
            dependencies.add(base_image)
        for builder in image_meta.config['from'].builder:
            if builder.member:
                dependencies.add(builder.member)

        def _check_dep(dep_key):
            dep = self.runtime.image_map.get(dep_key)
            if not dep:
                self.runtime.logger.warning(
                    "Image %s has unknown dependency %s. Is it excluded?", image_meta.distgit_key, dep_key
                )
                return

            dep_info = dep.get_latest_brew_build(default=None)
            if not dep_info:
                return

            dep_rebase_time = release_util.isolate_timestamp_in_release(dep_info["release"])
            if not dep_rebase_time:  # no timestamp string in NVR?
                return

            dep_rebase_time = datetime.strptime(dep_rebase_time, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            if dep_rebase_time > rebase_time:
                self.add_image_meta_change(
                    image_meta, RebuildHint(RebuildHintCode.DEPENDENCY_NEWER, 'Dependency has a newer build')
                )

        exectools.parallel_exec(
            f=lambda dep, _: _check_dep(dep),
            args=dependencies,
            n_threads=20,
        )

    def check_config_changes(self, image_meta: ImageMetadata, build_info):
        try:
            # git://pkgs.devel.redhat.com/containers/atomic-openshift-descheduler#6fc9c31e5d9437ac19e3c4b45231be8392cdacac
            source_url = build_info['source']
            source_commit = source_url.split('#')[1]  # isolate the commit hash
            # Look at the digest that created THIS build. What is in head does not matter.
            prev_digest = image_meta.fetch_cgit_file('.oit/config_digest', commit_hash=source_commit).decode('utf-8')

            current_digest = image_meta.calculate_config_digest(self.runtime.group_config, self.runtime.streams)
            if current_digest.strip() != prev_digest.strip():
                self.runtime.logger.info(
                    '%s config_digest %s is differing from %s', image_meta.distgit_key, prev_digest, current_digest
                )
                # fetch latest commit message on branch for the image metadata file
                with Dir(self.runtime.data_dir):
                    path = f'images/{image_meta.config_filename}'
                    rc, commit_message, _ = exectools.cmd_gather(f'git log -1 --format=%s -- {path}', strip=True)
                    if rc != 0:
                        raise IOError(f'Unable to retrieve commit message from {self.runtime.data_dir} for {path}')

                if 'scan-sources:noop' in commit_message.lower():
                    self.runtime.logger.info('Ignoring digest change since commit message indicates noop')
                else:
                    self.add_image_meta_change(
                        image_meta, RebuildHint(RebuildHintCode.CONFIG_CHANGE, 'Metadata configuration change')
                    )
        except exectools.RetryException:
            self.runtime.logger.info('%s config_digest cannot be retrieved; request a build', image_meta.distgit_key)
            self.add_image_meta_change(
                image_meta, RebuildHint(RebuildHintCode.CONFIG_CHANGE, 'Unable to retrieve config_digest')
            )

        except IOError:
            # IOError is raised by fetch_cgit_file() when config_digest could not be found
            self.runtime.logger.warning('config_digest not found for %s: skipping config check', image_meta.name)
            return

    async def check_changing_rpms(self):
        # Checks if an image needs to be rebuilt based on the packages (and therefore RPMs)
        # it is dependent on might have changed in tags relevant to the image.
        # A check is also made if the image depends on a package we know is changing
        # because we are about to rebuild it.

        async def _thread_does_image_need_change(image_meta):
            return await image_meta.does_image_need_change(
                changing_rpm_packages=self.changing_rpm_packages,
                buildroot_tag=image_meta.build_root_tag(),
                newest_image_event_ts=self.newest_image_event_ts,
                oldest_image_event_ts=self.oldest_image_event_ts,
            )

        change_results = await asyncio.gather(
            *[_thread_does_image_need_change(image_meta) for image_meta in self.runtime.image_metas()],
        )

        for change_result in filter(lambda r: r, change_results):
            meta, rebuild_hint = change_result
            if rebuild_hint.rebuild:
                self.add_image_meta_change(meta, rebuild_hint)

    def check_builder_images(self):
        # It uses while True because, technically, we could keep finding changes in intermediate builder images
        # and have to ensure we pull in images that rely on them in the next iteration.
        # fyi, changes in direct parent images should be detected by RPM changes, which
        # does does_image_name_change will detect.
        while True:
            changing_image_dgks = [meta.distgit_key for meta in self.changing_image_metas]
            for image_meta in self.all_image_metas:
                dgk = image_meta.distgit_key
                if dgk in changing_image_dgks:  # Already in? Don't look any further
                    continue

                for builder in image_meta.config['from'].builder:
                    if builder.member and builder.member in changing_image_dgks:
                        self.runtime.logger.info(f'{dgk} will be rebuilt due to change in builder member ')
                        self.add_image_meta_change(
                            image_meta,
                            RebuildHint(
                                RebuildHintCode.BUILDER_CHANGING, f'Builder group member has changed: {builder.member}'
                            ),
                        )

            if len(self.changing_image_metas) == len(changing_image_dgks):
                # The for loop didn't find anything new, we can exit
                return

    def add_assessment_reason(self, meta, rebuild_hint: RebuildHint):
        # qualify by whether this is a True or False for change so that we can store both in the map.
        key = f'{meta.qualified_key}+{rebuild_hint.rebuild}'
        # If the key is already there, don't replace the message as it is likely more interesting
        # than subsequent reasons (e.g. changing because of ancestry)
        if key not in self.assessment_reason:
            self.assessment_reason[key] = rebuild_hint.reason

    def add_image_meta_change(self, meta: ImageMetadata, rebuild_hint: RebuildHint):
        self.changing_image_metas.add(meta)
        self.add_assessment_reason(meta, rebuild_hint)
        for descendant_meta in meta.get_descendants():
            self.changing_image_metas.add(descendant_meta)
            self.add_assessment_reason(
                descendant_meta,
                RebuildHint(RebuildHintCode.ANCESTOR_CHANGING, f'Ancestor {meta.distgit_key} is changing'),
            )

    async def generate_report(self):
        image_results = []
        changing_image_dgks = [meta.distgit_key for meta in self.changing_image_metas]
        for image_meta in self.all_image_metas:
            dgk = image_meta.distgit_key
            is_changing = dgk in changing_image_dgks
            if is_changing:
                image_results.append(
                    {
                        'name': dgk,
                        'changed': is_changing,
                        'reason': self.assessment_reason.get(f'{image_meta.qualified_key}+{is_changing}'),
                    }
                )

        rpm_results = []
        changing_rpm_dgks = [meta.distgit_key for meta in self.changing_rpm_metas]
        for rpm_meta in self.all_rpm_metas:
            dgk = rpm_meta.distgit_key
            is_changing = dgk in changing_rpm_dgks
            if is_changing:
                rpm_results.append(
                    {
                        'name': dgk,
                        'changed': is_changing,
                        'reason': self.assessment_reason.get(f'{rpm_meta.qualified_key}+{is_changing}'),
                    }
                )

        results = dict(
            rpms=rpm_results,
            images=image_results,
        )

        self.runtime.logger.debug(f'scan-sources coordinate: results:\n{yaml.safe_dump(results, indent=4)}')

        if self.ci_kubeconfig:  # we can determine m-os-c needs updating if we can look at imagestreams
            results['rhcos'] = await self._detect_rhcos_status()

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

        self.runtime.logger.debug(f'KojiWrapper cache size: {int(brew.KojiWrapper.get_cache_size() / 1024)}KB')

    def _latest_rhcos_build_id(self, version, arch, private) -> Optional[str]:
        """
        Wrapper to return None if anything goes wrong, which will be taken as no change
        """

        try:
            return rhcos.RHCOSBuildFinder(self.runtime, version, arch, private).latest_rhcos_build_id()

        except rhcos.RHCOSNotFound as ex:
            # don't let flakiness in rhcos lookups prevent us from scanning regular builds;
            # if anything else changed it will sync anyway.
            self.runtime.logger.warning(
                f"could not determine RHCOS build for {version}-{arch}{'-priv' if private else ''}: {ex}"
            )
            return None

    async def _detect_rhcos_status(self) -> list:
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
        major, minor = self.runtime.get_major_minor_fields()
        primary_container = get_primary_container_name(self.runtime)
        for arch in self.runtime.arches:
            for private in (False, True):
                status = dict(name=f"{version}-{arch}{'-priv' if private else ''}")
                if self.runtime.group_config.rhcos.get("layered_rhcos", False):
                    tagged_rhcos_value = self._tagged_rhcos_node_digest(primary_container, version, arch, private)
                    latest_rhcos_value = self._latest_rhcos_node_shasum(arch)
                else:
                    tagged_rhcos_value = self._tagged_rhcos_id(primary_container, version, arch, private)
                    latest_rhcos_value = self._latest_rhcos_build_id(version, arch, private)

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
                    build_id, pullspec = rhcos.RHCOSBuildFinder(self.runtime, version, arch, private).latest_container(
                        container_conf
                    )
                    pullspec_for_tag[container_conf.name] = pullspec
                non_latest_rpms = await rhcos.RHCOSBuildInspector(
                    self.runtime, pullspec_for_tag, arch, build_id
                ).find_non_latest_rpms(exclude_rhel=True)
                non_latest_rpms_filtered = []

                # exclude rpm if non_latest_rpms in rhel image rpm list
                exclude_rpms = self.runtime.group_config.rhcos.get("exempt_rpms", [])
                for installed_rpm, latest_rpm, repo in non_latest_rpms:
                    if any(excluded in installed_rpm for excluded in exclude_rpms):
                        self.runtime.logger.info(
                            f"[EXEMPT SKIPPED] Exclude {installed_rpm} because its in the exempt list when {latest_rpm} was available in repo {repo}"
                        )
                    else:
                        non_latest_rpms_filtered.append((installed_rpm, latest_rpm, repo))
                if non_latest_rpms_filtered:
                    status['outdated'] = True
                    status['changed'] = True
                    status['reason'] = ";\n".join(
                        f"Outdated RPM {installed_rpm} installed in RHCOS ({arch}) when {latest_rpm} was available in repo {repo}"
                        for installed_rpm, latest_rpm, repo in non_latest_rpms_filtered
                    )
                    statuses.append(status)
        return statuses

    def _tagged_rhcos_id(self, container_name, version, arch, private) -> Optional[str]:
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
            self.runtime.logger.error(
                'Could not find .image.dockerImageMetadata.Config.Labels in RHCOS imageMetadata:\n%s', stdout
            )
            raise

        build_id = None
        if not (build_id := labels.get('org.opencontainers.image.version', None)):
            build_id = labels.get('version', None)

        return build_id

    def _tagged_rhcos_node_digest(self, container_name, version, arch, private) -> Optional[str]:
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
            self.runtime.logger.error('Could not find .metadata.name in RHCOS imagestream:\n%s', stdout)
            raise

        return shasum

    def _latest_rhcos_node_shasum(self, arch) -> Optional[str]:
        """get latest node image from quay.io/openshift-release-dev/ocp-v4.0-art-dev:4.x-9.x-node-image"""
        go_arch = go_arch_for_brew_arch(arch)
        rhcos_index = next(
            (tag.rhcos_index_tag for tag in self.runtime.group_config.rhcos.payload_tags if tag.primary), ""
        )
        rhcos_info = util.oc_image_info_for_arch(rhcos_index, go_arch)
        return rhcos_info['digest']


@cli.command("config:scan-sources", short_help="Determine if any rpms / images need to be rebuilt.")
@click.option(
    "--ci-kubeconfig",
    metavar='KC_PATH',
    required=False,
    help="File containing kubeconfig for looking at release-controller imagestreams",
)
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@click.option("--rebase-priv", default=False, is_flag=True, help='Try to reconcile public upstream into openshift-priv')
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

    if runtime.group_config.canonical_builders_from_upstream and runtime.build_system == 'brew':
        runtime.initialize(mode="both", clone_distgits=True)
    else:
        runtime.initialize(mode='both', clone_distgits=False)

    await ConfigScanSources(runtime, ci_kubeconfig, as_yaml, rebase_priv, dry_run).run()


cli.add_command(config_scan_source_changes)
