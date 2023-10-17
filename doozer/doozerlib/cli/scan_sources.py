import asyncio
from datetime import datetime, timezone

import click
import yaml
from typing import List, Tuple, Optional

from doozerlib import brew, exectools, rhcos, util
from doozerlib.cli import cli, pass_runtime, click_coroutine
from doozerlib.cli import release_gen_payload as rgp
from doozerlib.image import ImageMetadata
from doozerlib.metadata import RebuildHint, RebuildHintCode, Metadata
from doozerlib.runtime import Runtime
from doozerlib.pushd import Dir


class ConfigScanSources:
    def __init__(self, runtime: Runtime, ci_kubeconfig: str, as_yaml: bool):
        self.runtime = runtime
        self.ci_kubeconfig = ci_kubeconfig
        self.as_yaml = as_yaml

        self.changing_rpm_metas = set()
        self.changing_image_metas = set()
        self.changing_rpm_packages = set()
        self.assessment_reason = dict()  # maps metadata qualified_key => message describing change

        self.all_rpm_metas = set(runtime.rpm_metas())
        self.all_image_metas = set(runtime.image_metas())
        self.all_metas = self.all_rpm_metas.union(self.all_image_metas)

        self.oldest_image_event_ts = None
        self.newest_image_event_ts = 0

    def run(self):
        with self.runtime.shared_koji_client_session() as koji_api:
            self.runtime.logger.info(f'scan-sources coordinate: brew_event: '
                                     f'{koji_api.getLastEvent(brew.KojiWrapperOpts(brew_event_aware=True))}')
            self.runtime.logger.info(f'scan-sources coordinate: emulated_brew_event: {self.runtime.brew_event}')

            # First, scan for any upstream source code changes. If found, these are guaranteed rebuilds.
            self.scan_for_upstream_changes(koji_api)

            # Check for other reasons why images should be rebuilt (e.g. config changes, dependencies)
            self.check_for_image_changes(koji_api)

        # Checks if an image needs to be rebuilt based on the packages it is dependent on
        self.check_changing_rpms()

        # does_image_need_change() checks whether its non-member builder images have changed
        # but cannot determine whether member builder images have changed until anticipated
        # changes have been calculated. The following function call does this.
        self.check_builder_images()

        # We have our information. Now build and print the output report
        self.generate_report()

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
                    latest_rpms = koji_api.getLatestRPMS(tag=meta.branch() + '-candidate', package=package_name)[1]

                    for latest_rpm_build in latest_rpms:
                        if not eldest_rpm_build or latest_rpm_build['creation_event_id'] < \
                                eldest_rpm_build['creation_event_id']:
                            eldest_rpm_build = latest_rpm_build

                    # Detect if our buildroot changed since the oldest rpm of the package latest build of was built.
                    build_root_change = brew.has_tag_changed_since_build(self.runtime, koji_api, eldest_rpm_build,
                                                                         meta.build_root_tag(), inherit=True)

                    if build_root_change:
                        rebuild_hint = RebuildHint(RebuildHintCode.BUILD_ROOT_CHANGING,
                                                   'Oldest package rpm build was before buildroot change')
                        self.runtime.logger.info(f'{dgk} ({eldest_rpm_build}) in {package_name} '
                                                 f'is older than more recent buildroot change: {build_root_change}')

                if rebuild_hint.rebuild:
                    self.add_assessment_reason(meta, rebuild_hint)
                    self.changing_rpm_metas.add(meta)
                    self.changing_rpm_packages.add(package_name)

            elif meta.meta_type == 'image':
                if rebuild_hint.rebuild:
                    self.add_image_meta_change(meta, rebuild_hint)

            else:
                raise IOError(f'Unsupported meta type: {meta.meta_type}')

    def check_for_image_changes(self, koji_api):
        for image_meta in self.runtime.image_metas():
            build_info = image_meta.get_latest_build(default=None)

            if build_info is None:
                continue

            # To limit the size of the queries we are going to make, find the oldest and newest image
            self.find_oldest_newest(koji_api, build_info)

            # If a rebuild is already requested, skip following checks
            if image_meta in self.changing_image_metas:
                continue

            # Request a rebuild if A is a dependent (operator or child image) of B
            # but the latest build of A is older than B.
            self.check_dependents(image_meta, build_info)

            # If no upstream change has been detected, check configurations
            # like image meta, repos, and streams to see if they have changed
            # We detect config changes by comparing their digest changes.
            # The config digest of the previous build is stored at .oit/config_digest on distgit repo.
            self.check_config_changes(image_meta, build_info)

        self.runtime.logger.debug('Will be assessing tagging changes between '
                                  'newest_image_event_ts:%s and oldest_image_event_ts:%s',
                                  self.newest_image_event_ts, self.oldest_image_event_ts)

    def find_oldest_newest(self, koji_api, build_info):
        create_event_ts = koji_api.getEvent(build_info['creation_event_id'])['ts']
        if self.oldest_image_event_ts is None or create_event_ts < self.oldest_image_event_ts:
            self.oldest_image_event_ts = create_event_ts
        if create_event_ts > self.newest_image_event_ts:
            self.newest_image_event_ts = create_event_ts

    def check_dependents(self, image_meta, build_info):
        rebase_time = util.isolate_timestamp_in_release(build_info["release"])
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
                self.runtime.logger.warning("Image %s has unknown dependency %s. Is it excluded?",
                                            image_meta.distgit_key, dep_key)
                return

            dep_info = dep.get_latest_build(default=None)
            if not dep_info:
                return

            dep_rebase_time = util.isolate_timestamp_in_release(dep_info["release"])
            if not dep_rebase_time:  # no timestamp string in NVR?
                return

            dep_rebase_time = datetime.strptime(dep_rebase_time, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            if dep_rebase_time > rebase_time:
                self.add_image_meta_change(image_meta,
                                           RebuildHint(RebuildHintCode.DEPENDENCY_NEWER,
                                                       'Dependency has a newer build'))

        exectools.parallel_exec(
            f=lambda dep, _: _check_dep(dep),
            args=dependencies,
            n_threads=20
        )

    def check_config_changes(self, image_meta, build_info):
        try:
            # git://pkgs.devel.redhat.com/containers/atomic-openshift-descheduler#6fc9c31e5d9437ac19e3c4b45231be8392cdacac
            source_url = build_info['source']
            source_commit = source_url.split('#')[1]  # isolate the commit hash
            # Look at the digest that created THIS build. What is in head does not matter.
            prev_digest = image_meta.fetch_cgit_file('.oit/config_digest',
                                                     commit_hash=source_commit).decode('utf-8')
            current_digest = image_meta.calculate_config_digest(self.runtime.group_config, self.runtime.streams)
            if current_digest.strip() != prev_digest.strip():
                self.runtime.logger.info('%s config_digest %s is differing from %s',
                                         image_meta.distgit_key, prev_digest, current_digest)
                # fetch latest commit message on branch
                with Dir(self.runtime.data_path):
                    rc, commit_message, _ = exectools.cmd_gather('git log -1 --format=%s', strip=True)
                    if rc != 0:
                        raise IOError(f'Unable to retrieve commit message from {self.runtime.data_path}')

                if commit_message.lower().startswith('scan-sources:noop'):
                    self.runtime.logger.info('Ignoring digest change since commit message indicates noop')
                else:
                    self.add_image_meta_change(image_meta,
                                               RebuildHint(RebuildHintCode.CONFIG_CHANGE,
                                                           'Metadata configuration change'))
        except exectools.RetryException:
            self.runtime.logger.info('%s config_digest cannot be retrieved; request a build',
                                     image_meta.distgit_key)
            self.add_image_meta_change(image_meta,
                                       RebuildHint(RebuildHintCode.CONFIG_CHANGE,
                                                   'Unable to retrieve config_digest'))

    def check_changing_rpms(self):
        # Checks if an image needs to be rebuilt based on the packages (and therefore RPMs)
        # it is dependent on might have changed in tags relevant to the image.
        # A check is also made if the image depends on a package we know is changing
        # because we are about to rebuild it.

        def _thread_does_image_need_change(image_meta):
            return asyncio.run(image_meta.does_image_need_change(
                changing_rpm_packages=self.changing_rpm_packages,
                buildroot_tag=image_meta.build_root_tag(),
                newest_image_event_ts=self.newest_image_event_ts,
                oldest_image_event_ts=self.oldest_image_event_ts)
            )

        change_results = exectools.parallel_exec(
            f=lambda image_meta, terminate_event: _thread_does_image_need_change(image_meta),
            args=self.runtime.image_metas(),
            n_threads=20
        ).get()

        for change_result in change_results:
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
                        self.add_image_meta_change(image_meta,
                                                   RebuildHint(RebuildHintCode.BUILDER_CHANGING,
                                                               f'Builder group member has changed: {builder.member}'))

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
            self.add_assessment_reason(descendant_meta, RebuildHint(RebuildHintCode.ANCESTOR_CHANGING,
                                                                    f'Ancestor {meta.distgit_key} is changing'))

    def generate_report(self):
        image_results = []
        changing_image_dgks = [meta.distgit_key for meta in self.changing_image_metas]
        for image_meta in self.all_image_metas:
            dgk = image_meta.distgit_key
            is_changing = dgk in changing_image_dgks
            image_results.append({
                'name': dgk,
                'changed': is_changing,
                'reason': self.assessment_reason.get(f'{image_meta.qualified_key}+{is_changing}', 'No change detected'),
            })

        rpm_results = []
        changing_rpm_dgks = [meta.distgit_key for meta in self.changing_rpm_metas]
        for rpm_meta in self.all_rpm_metas:
            dgk = rpm_meta.distgit_key
            is_changing = dgk in changing_rpm_dgks
            rpm_results.append({
                'name': dgk,
                'changed': is_changing,
                'reason': self.assessment_reason.get(f'{rpm_meta.qualified_key}+{is_changing}', 'No change detected'),
            })

        results = dict(
            rpms=rpm_results,
            images=image_results
        )

        self.runtime.logger.debug(f'scan-sources coordinate: results:\n{yaml.safe_dump(results, indent=4)}')

        if self.ci_kubeconfig:  # we can determine m-os-c needs updating if we can look at imagestreams
            results['rhcos'] = self._detect_rhcos_status()

        if self.as_yaml:
            click.echo('---')
            click.echo(yaml.safe_dump(results, indent=4))
        else:
            for kind, items in results.items():
                if not items:
                    continue
                click.echo(kind.upper() + ":")
                for item in items:
                    click.echo('  {} is {} (reason: {})'.format(item['name'],
                                                                'changed' if item['changed'] else 'the same',
                                                                item['reason']))

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
            self.runtime.logger.warning(f"could not determine RHCOS build for "
                                        f"{version}-{arch}{'-priv' if private else ''}: {ex}")
            return None

    def _detect_rhcos_status(self) -> list:
        """
        gather the existing RHCOS tags and compare them to latest rhcos builds
        @return a list of status entries like:
            {
                'name': "4.2-x86_64-priv",
                'changed': False,
                'reason': "could not find an RHCOS build to sync",
            }
        """
        statuses = []

        version = self.runtime.get_minor_version()
        for arch in self.runtime.arches:
            for private in (False, True):
                name = f"{version}-{arch}{'-priv' if private else ''}"
                tagged_rhcos_id = self._tagged_rhcos_id(rhcos.get_primary_container_name(self.runtime),
                                                        version, arch, private)
                latest_rhcos_id = self._latest_rhcos_build_id(version, arch, private)
                status = dict(name=name)
                if not latest_rhcos_id:
                    status['changed'] = False
                    status['reason'] = "could not find an RHCOS build to sync"
                elif tagged_rhcos_id == latest_rhcos_id:
                    status['changed'] = False
                    status['reason'] = f"latest RHCOS build is still {latest_rhcos_id} -- no change from istag"
                else:
                    status['changed'] = True
                    status['reason'] = f"latest RHCOS build is {latest_rhcos_id} " \
                                       f"which differs from istag {tagged_rhcos_id}"
                statuses.append(status)

        return statuses

    def _tagged_rhcos_id(self, container_name, version, arch, private) -> str:
        """determine the most recently tagged RHCOS in given imagestream"""
        base_namespace = rgp.default_imagestream_namespace_base_name()
        base_name = rgp.default_imagestream_base_name(version)
        namespace, name = rgp.payload_imagestream_namespace_and_name(base_namespace, base_name, arch, private)
        stdout, _ = exectools.cmd_assert(
            f"oc --kubeconfig '{self.ci_kubeconfig}' --namespace '{namespace}' get istag '{name}:{container_name}'"
            " --template '{{.image.dockerImageMetadata.Config.Labels.version}}'",
            retries=3,
            pollrate=5,
            strip=True,
        )
        return stdout if stdout else None


@cli.command("config:scan-sources", short_help="Determine if any rpms / images need to be rebuilt.")
@click.option("--ci-kubeconfig", metavar='KC_PATH', required=False,
              help="File containing kubeconfig for looking at release-controller imagestreams")
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@pass_runtime
def config_scan_source_changes(runtime: Runtime, ci_kubeconfig, as_yaml):
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

    runtime.initialize(mode='both', clone_distgits=False, clone_source=False, prevent_cloning=True)
    ConfigScanSources(runtime, ci_kubeconfig, as_yaml).run()


cli.add_command(config_scan_source_changes)
