import asyncio
import json
import logging
import os
import shutil
import tempfile
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple

import click
import yaml
from artcommonlib import exectools, redis
from artcommonlib.build_visibility import is_release_embargoed
from artcommonlib.constants import (
    KONFLUX_DEFAULT_IMAGE_REPO,
    KONFLUX_DEFAULT_IMAGE_SHARE_REPO,
    REGISTRY_BREW,
    REGISTRY_CI_OPENSHIFT,
    REGISTRY_QUAY_OCP_RELEASE_DEV,
    REGISTRY_QUAY_OPENSHIFT,
    REGISTRY_REDHAT_IO,
)
from artcommonlib.registry_config import RegistryConfig, RegistryCredential
from artcommonlib.util import (
    new_roundtrip_yaml_handler,
    run_safe,
    sync_to_quay,
    uses_konflux_imagestream_override,
    validate_build_priority,
)

from pyartcd import constants, jenkins, locks, util
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.util import (
    build_history_link_url,
    get_group_images,
    get_group_rpms,
    increment_rebase_fail_counter,
    mass_rebuild_score,
    reset_rebase_fail_counter,
)

LOGGER = logging.getLogger(__name__)


class BuildStrategy(Enum):
    ALL = 'all'
    ONLY = 'only'
    EXCEPT = 'except'
    NONE = 'none'

    def __str__(self):
        return self.value


class BuildPlan:
    def __init__(self, image_build_strategy=BuildStrategy.ALL, rpm_build_strategy=BuildStrategy.NONE):
        self.image_build_strategy = image_build_strategy  # build all images or a subset
        self.images_included = []  # include list for images to build
        self.images_excluded = []  # exclude list for images to build
        self.active_image_count = 0  # number of images active in this version

        self.rpm_build_strategy = rpm_build_strategy  # build all RPMs or a subset
        self.rpms_included = []  # include list for RPMs to build
        self.rpms_excluded = []  # exclude list for RPMs to build
        self.active_rpm_count = 0  # number of RPMs active in this version

    def __str__(self):
        return json.dumps(self.__dict__, indent=4, cls=EnumEncoder)


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class KonfluxOcpPipeline:
    def __init__(
        self,
        runtime: Runtime = None,
        assembly: str = None,
        data_path: Optional[str] = None,
        image_build_strategy: Optional[str] = None,
        image_list: Optional[str] = None,
        rpm_build_strategy: Optional[str] = None,
        rpm_list: Optional[str] = None,
        version: str = None,
        data_gitref: Optional[str] = None,
        kubeconfig: Optional[str] = None,
        skip_rebase: bool = None,
        skip_bundle_build: bool = None,
        skip_build_sync_konflux: bool = False,
        arches: Tuple[str, ...] = None,
        plr_template: str = None,
        lock_identifier: str = None,
        skip_plashets: bool = False,
        build_priority: str = None,
        use_mass_rebuild_locks: bool = False,
        network_mode: Optional[str] = None,
    ):
        self.runtime = runtime
        self.assembly = assembly
        self.version = version
        self.release = util.default_release_suffix()
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.kubeconfig = kubeconfig
        self.arches = arches
        self.skip_rebase = skip_rebase
        self.skip_bundle_build = skip_bundle_build
        self.skip_build_sync_konflux = skip_build_sync_konflux
        self.plr_template = plr_template
        self.lock_identifier = lock_identifier
        self.skip_plashets = skip_plashets
        self.build_priority = build_priority
        self.use_mass_rebuild_locks = use_mass_rebuild_locks
        self.network_mode = network_mode

        # If build plan includes more than half or excludes less than half or rebuilds everything, it's a mass rebuild
        self.mass_rebuild = False

        group_param = f'--group=openshift-{version}'
        if data_gitref:
            group_param += f'@{data_gitref}'

        self._doozer_base_command = [
            'doozer',
            f'--assembly={assembly}',
            f'--working-dir={self.runtime.doozer_working}',
            f'--data-path={data_path}',
            '--build-system=konflux',
            group_param,
        ]

        self.build_plan = BuildPlan(BuildStrategy(image_build_strategy), BuildStrategy(rpm_build_strategy))
        self.image_list = [image.strip() for image in image_list.split(',')] if image_list else []
        self.rpm_list = [rpm.strip() for rpm in rpm_list.split(',')] if rpm_list else []

        self.group_images = []
        self.rebase_failures = []
        self.rebase_skipped_due_to_parent = []

        self.slack_client = runtime.new_slack_client()

        validate_build_priority(build_priority)

    def include_exclude_param(self, kind: str):
        """
        Returns the include/exclude parameters for the Doozer command based on the build strategy.
        """

        if kind not in ['rpms', 'images']:
            raise ValueError('Kind must be one in ["rpms", "images"]')

        if kind == 'images':
            build_strategy = self.build_plan.image_build_strategy
            includes = self.build_plan.images_included
            excludes = self.build_plan.images_excluded

        else:  # kind == 'rpms'
            build_strategy = self.build_plan.rpm_build_strategy
            includes = self.build_plan.rpms_included
            excludes = self.build_plan.rpms_excluded

        if build_strategy == BuildStrategy.ALL:
            return []

        elif build_strategy == BuildStrategy.ONLY:
            return [f'--{kind}={",".join(includes)}']

        elif build_strategy == BuildStrategy.EXCEPT:
            return [f'--{kind}=', f'--exclude={",".join(excludes)}']

    async def update_rebase_fail_counters(
        self,
        failed_images: list[str],
        skipped_due_to_parent: Optional[list[str]] = None,
    ):
        """
        Adjust Redis rebase-fail counters after a Konflux rebase run.

        - Images that rebased successfully: reset counter.
        - Images that failed rebase directly: increment counter.
        - Images skipped because a parent failed first: no change (rebase was not meaningfully attempted).
        """
        if self.assembly != 'stream':
            # Only update fail counters for stream assembly
            return

        failed_set = set(failed_images)
        skipped_set = set(skipped_due_to_parent or [])

        # Reset fail counters only for images that actually rebased successfully
        match self.build_plan.image_build_strategy:
            case BuildStrategy.ALL:
                successful_images = [
                    image for image in self.group_images if image not in failed_set and image not in skipped_set
                ]
            case BuildStrategy.EXCEPT:
                successful_images = [
                    image
                    for image in self.group_images
                    if image not in self.build_plan.images_excluded
                    and image not in failed_set
                    and image not in skipped_set
                ]
            case BuildStrategy.ONLY:
                successful_images = [
                    image for image in self.image_list if image not in failed_set and image not in skipped_set
                ]
            case _:
                raise ValueError(
                    f"Unknown build strategy: {self.build_plan.image_build_strategy}. Valid strategies: {[s.value for s in BuildStrategy]}"
                )
        await asyncio.gather(
            *[reset_rebase_fail_counter(image, self.version, 'konflux') for image in successful_images]
        )

        # Increment fail counters only for images that failed rebase directly
        job_url = os.getenv('BUILD_URL')
        await asyncio.gather(
            *[increment_rebase_fail_counter(image, self.version, 'konflux', job_url=job_url) for image in failed_images]
        )

    def building_images(self):
        """
        Returns True if images are being built, False otherwise.
        """

        # If the build strategy is NONE, no images will be built
        if self.build_plan.image_build_strategy == BuildStrategy.NONE:
            return False

        # If the build strategy is ONLY but no images are included, no images will be built
        if self.build_plan.image_build_strategy == BuildStrategy.ONLY and not self.build_plan.images_included:
            return False

        # If the build strategy is EXCEPT but no images are excluded,
        # or if the build strategy is ALL, all images will be built
        return True

    async def rebase_images(self, version: str, input_release: str):
        # Skip rebase if the flag is set
        if self.skip_rebase:
            LOGGER.warning("Skipping rebase step because --skip-rebase flag is set")
            return

        # If no images are being built, skip the rebase step
        if not self.building_images():
            LOGGER.warning('No images will be rebased')
            return

        LOGGER.info(f"Rebasing images for OCP {self.version} with release {self.release}")

        cmd = self._doozer_base_command.copy()
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))

        cmd.append('--latest-parent-version')
        cmd.extend(self.include_exclude_param(kind='images'))
        cmd.extend(
            [
                'beta:images:konflux:rebase',
                f'--version={version}',
                f'--release={input_release}',
                f"--message='Updating Dockerfile version and release {version}-{input_release}'",
            ]
        )
        if self.network_mode:
            cmd.extend(['--network-mode', self.network_mode])
        if not self.runtime.dry_run:
            cmd.append('--push')

        try:
            await exectools.cmd_assert_async(cmd)
            await self.update_rebase_fail_counters([])

        except ChildProcessError:
            with open(f'{self.runtime.doozer_working}/state.yaml') as state_yaml:
                state = yaml.safe_load(state_yaml)
            konflux_rebase = state.get('images:konflux:rebase', {})
            failed_images = konflux_rebase.get('failed-images', [])
            skipped_due_to_parent = konflux_rebase.get('skipped-due-to-parent-rebase-failure', [])
            excluded_from_build = list(dict.fromkeys(failed_images + skipped_due_to_parent))
            if not excluded_from_build:
                raise  # Something else went wrong

            # Some images failed to rebase: log them, and track direct failures in Redis (not dependents skipped
            # because a parent failed — those are listed separately).
            if failed_images:
                LOGGER.warning(f'Following images failed to rebase and won\'t be built: {",".join(failed_images)}')
            if skipped_due_to_parent:
                LOGGER.warning(
                    'Following images were skipped because a parent failed to rebase and won\'t be built: %s',
                    ','.join(skipped_due_to_parent),
                )
            await self.update_rebase_fail_counters(failed_images, skipped_due_to_parent)

            # Exclude images that failed or were skipped due to parent from the build step
            if self.build_plan.image_build_strategy == BuildStrategy.ALL:
                # Move from building all to excluding failed images
                self.build_plan.image_build_strategy = BuildStrategy.EXCEPT
                self.build_plan.images_excluded = excluded_from_build

            elif self.build_plan.image_build_strategy == BuildStrategy.ONLY:
                # Remove failed images from included ones
                self.build_plan.images_included = [
                    i for i in self.build_plan.images_included if i not in excluded_from_build
                ]

            else:  # strategy = EXCLUDE
                # Append failed images to excluded ones
                self.build_plan.images_excluded.extend(excluded_from_build)

            # Track rebase issues for later steps (direct failures match Redis counters)
            self.rebase_failures = failed_images
            self.rebase_skipped_due_to_parent = skipped_due_to_parent

    async def build_images(self):
        if not self.building_images():
            LOGGER.warning('No images will be built')
            return

        LOGGER.info(f"Building images for OCP {self.version} with release {self.release}")

        cmd = self._doozer_base_command.copy()
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))

        cmd.append('--latest-parent-version')
        cmd.extend(self.include_exclude_param(kind='images'))
        cmd.extend(
            [
                'beta:images:konflux:build',
                "--konflux-namespace=ocp-art-tenant",
            ]
        )
        if self.network_mode:
            cmd.extend(['--network-mode', self.network_mode])
        if self.kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.kubeconfig])
        if self.plr_template:
            plr_template_owner, plr_template_branch = (
                self.plr_template.split("@") if self.plr_template else ["openshift-priv", "main"]
            )
            plr_template_url = constants.KONFLUX_IMAGE_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
                owner=plr_template_owner, branch_name=plr_template_branch
            )
            cmd.extend(['--plr-template', plr_template_url])
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        # Add build priority. Can be a str between "1" (highest priority) - "10" or "auto"
        LOGGER.info(f"Using build priority: {self.build_priority}")
        cmd.extend(['--build-priority', self.build_priority])

        await exectools.cmd_assert_async(cmd)

        LOGGER.info("All builds completed successfully")

    async def sync_images(self):
        if not self.building_images():
            LOGGER.warning('No images will be synced')
            return

        if self.runtime.dry_run:
            LOGGER.info('Not syncing images in dry run mode')
            return

        LOGGER.info('Syncing images...')

        record_log = self.parse_record_log()
        if not record_log:
            LOGGER.error('record.log not found!')
            return

        built_images = [
            entry['name'] for entry in record_log.get('image_build_konflux', []) if not int(entry['status'])
        ]
        failed_images = [entry['name'] for entry in record_log.get('image_build_konflux', []) if int(entry['status'])]
        if 1 <= len(failed_images) <= 10:
            jenkins.update_description(f'Failed images: {", ".join(failed_images)}<br/>')
        elif len(failed_images) > 10:
            jenkins.update_description(f'{len(failed_images)} images failed. Check record.log for details<br/>')

        if not built_images:
            # Nothing to do, skipping build-sync
            LOGGER.info('All image builds failed, nothing to sync')
            return

        if self.assembly == 'test':
            self.runtime.logger.warning('Skipping build-sync job for test assembly')
            return

        if self.skip_build_sync_konflux:
            LOGGER.warning("Skipping build-sync-konflux step because --skip-build-sync-konflux flag is set")
            return

        # Determine arches to be excluded as the difference between the group configured arches,
        # and the arches selected from current build. If self.arches is empty, it means that we're building all arches
        # and no arches should be excluded
        if not self.arches:
            exclude_arches = []

        else:
            _, out, _ = await exectools.cmd_gather_async(
                [*self._doozer_base_command.copy(), 'config:read-group', 'arches', '--yaml']
            )
            exclude_arches = new_roundtrip_yaml_handler().load(out.strip())
            for arch in self.arches:
                exclude_arches.remove(arch)

        jenkins.start_build_sync(
            build_version=self.version,
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
            build_system='konflux',
            exclude_arches=exclude_arches,
            SKIP_MULTI_ARCH_PAYLOAD="auto",
        )

    async def mirror_streams_to_ci(self):
        # If the API server builds, we mirror out the streams to CI. If ART builds a bad golang builder image it will
        # break CI builds for most upstream components if we don't catch it before we push. So we use apiserver as
        # bellwether to make sure that the current builder image is good enough. We can still break CI (e.g. pushing a
        # bad ruby-25 image along with this push, but it will not be a catastrophic event like breaking the apiserver.
        record_log = self.parse_record_log()
        if not record_log:
            LOGGER.warning('record.log not found, skipping CI mirroring check')
            return

        built_images = {
            entry['name']: entry for entry in record_log.get('image_build_konflux', []) if not int(entry['status'])
        }
        if built_images.get('ose-openshift-apiserver', None):
            LOGGER.warning('apiserver rebuilt: mirroring streams to CI...')

            cmd = self._doozer_base_command.copy()
            cmd.extend(['images:streams', 'mirror', '--registry-auth', self._registry_auth_file])
            await exectools.cmd_assert_async(cmd)

    async def sweep_bugs(self):
        """
        Find MODIFIED bugs for the target-releases, and set them to ON_QA
        """

        if self.assembly != 'stream':
            self.runtime.logger.info('Not setting bugs to ON_QA since assembly is not stream')
            return

        cmd = [
            'elliott',
            f'--group=openshift-{self.version}',
            "find-bugs:qe",
        ]
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            if self.runtime.dry_run:
                return

            self.slack_client.bind_channel(f'openshift-{self.version}')
            await self.slack_client.say(f'Bug sweep failed for {self.version}. Please investigate')

    async def sweep_golang_bugs(self):
        # find-bugs:golang only modifies bug state after verifying
        # that the bug is fixed in the builds found in latest nightly / rpms in candidate tag
        # therefore we do not need to check which builds are successful to run this
        if self.assembly != 'stream':
            self.runtime.logger.info('Not setting golang bugs to ON_QA since assembly is not stream')
            return

        cmd = [
            'elliott',
            '--assembly',
            'stream',
            f'--group=openshift-{self.version}',
            f'--registry-config={self._registry_auth_file}',
            "find-bugs:golang",
            "--analyze",
            "--update-tracker",
        ]

        if self.runtime.dry_run:
            cmd.append('--dry-run')

        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            if self.runtime.dry_run:
                return
            self.slack_client.bind_channel(f'openshift-{self.version}')
            await self.slack_client.say(f'Golang bug sweep failed for {self.version}. Please investigate')

    async def sweep_second_fix_bugs(self):
        # find-bugs:second-fix closes CVE trackers that are not first-fix in pre-release branches
        # The command itself checks the software lifecycle phase and only runs for pre-release/signing phases
        cmd = [
            'elliott',
            f'--group=openshift-{self.version}',
            "find-bugs:second-fix",
            "--close",
        ]
        try:
            await exectools.cmd_assert_async(cmd)
        except ChildProcessError:
            self.slack_client.bind_channel(f'openshift-{self.version}')
            await self.slack_client.say(f'Second-fix bug sweep failed for {self.version}. Please investigate')

    async def init_build_plan(self):
        # Get number of images in current group
        shutil.rmtree(self.runtime.doozer_working, ignore_errors=True)
        self.group_images = await get_group_images(
            group=f'openshift-{self.version}',
            assembly=self.assembly,
            build_system='konflux',
            working_dir=Path(self.runtime.doozer_working),
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        self.build_plan.active_image_count = len(self.group_images)

        # Get the number of RPMs in the current group (microshift is excluded by default)
        group_rpms = await get_group_rpms(
            group=f'openshift-{self.version}',
            assembly=self.assembly,
            working_dir=Path(self.runtime.doozer_working),
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        self.build_plan.active_rpm_count = len(group_rpms)

        # Set the image and RPM lists based on the build strategies
        self.check_building_images()
        self.check_building_rpms()

        # Log the initial build plan
        self.runtime.logger.info('Initial build plan:\n%s', self.build_plan)

    def check_building_images(self):
        if self.build_plan.image_build_strategy == BuildStrategy.NONE:
            jenkins.update_title('[NO IMAGES]')

        elif self.build_plan.image_build_strategy == BuildStrategy.ALL:
            self.build_plan.images_included = []
            self.build_plan.images_excluded = []
            self.mass_rebuild = True
            jenkins.update_title('[mass rebuild]')
            jenkins.update_description(f'Images: building {self.build_plan.active_image_count} images.<br/>')

        elif self.build_plan.image_build_strategy == BuildStrategy.ONLY:
            self.build_plan.images_included = self.image_list
            self.build_plan.images_excluded = []

            n_images = len(self.build_plan.images_included)
            if n_images == 1:
                jenkins.update_title(f'[{self.build_plan.images_included[0]}]')
            else:
                jenkins.update_title(f'[{n_images} images]')

            if n_images <= 10:
                jenkins.update_description(f'Images: building {", ".join(self.build_plan.images_included)}.<br/>')
            else:
                jenkins.update_description(f'Images: building {n_images} images.<br/>')

            # It's a mass rebuild if we included more than half of all active images in the group
            if n_images > self.build_plan.active_image_count / 2:
                self.mass_rebuild = True

        else:  # build_plan.build_strategy == BuildStrategy.EXCEPT
            self.build_plan.images_included = []
            self.build_plan.images_excluded = self.image_list

            n_images = self.build_plan.active_image_count - len(self.build_plan.images_excluded)
            jenkins.update_title(f'[{n_images} images]')
            if len(self.build_plan.images_excluded) <= 10:
                jenkins.update_description(
                    f'Images: building all images except {",".join(self.build_plan.images_excluded)}.<br/>'
                )
            else:
                jenkins.update_description(f'Images: building {n_images} images.<br/>')

            # It's a mass rebuild if we excluded less than half of all active images in the group
            if n_images < self.build_plan.active_image_count / 2:
                self.mass_rebuild = True

        if self.mass_rebuild:
            jenkins.update_title(' [MASS REBUILD]')

    def check_building_rpms(self):
        # If the version is not in the override list, skip the RPM rebase and build
        # TODO this can be removed once all versions are handled by ocp4-konflux
        if not uses_konflux_imagestream_override(self.version):
            self.runtime.logger.info(
                'Skipping RPM rebase and build for %s since it is being handled by ocp4', {self.version}
            )
            self.build_plan.rpm_build_strategy = BuildStrategy.NONE

        if self.build_plan.rpm_build_strategy == BuildStrategy.NONE:
            jenkins.update_title('[NO RPMs]')

        elif self.build_plan.rpm_build_strategy == BuildStrategy.ALL:
            self.build_plan.rpms_included = []
            self.build_plan.rpms_excluded = []
            jenkins.update_description('Building all RPMs.<br/>')
            jenkins.update_title('[All RPMs]')

        elif self.build_plan.rpm_build_strategy == BuildStrategy.ONLY:
            self.build_plan.rpms_included = self.rpm_list
            self.build_plan.rpms_excluded = []

            n_rpms = len(self.build_plan.rpms_included)
            if n_rpms == 1:
                jenkins.update_title(f'[{self.build_plan.rpms_included[0]}]')
            else:
                jenkins.update_title(f'[{n_rpms} RPMs]')

            if n_rpms <= 10:
                jenkins.update_description(f'RPMs: building {", ".join(self.build_plan.rpms_included)}.<br/>')
            else:
                jenkins.update_description(f'RPMs: building {n_rpms} RPMs.<br/>')

        else:  # build_plan.rpm_build_strategy == BuildStrategy.EXCEPT
            self.build_plan.rpms_included = []
            self.build_plan.rpms_excluded = self.rpm_list

            n_rpms = self.build_plan.active_rpm_count - len(self.build_plan.rpms_excluded)
            jenkins.update_title(f'[{n_rpms} RPMs]')
            if len(self.build_plan.images_excluded) <= 10:
                jenkins.update_description(f'Building all RPMs except {",".join(self.build_plan.rpms_excluded)}.<br/>')
            else:
                jenkins.update_description(f'RPMs: building {n_rpms} RPMs.<br/>')

    async def initialize(self):
        jenkins.init_jenkins()
        jenkins.update_title(f' - {self.version} [{self.assembly}] ')
        await self.init_build_plan()

    async def clean_up(self):
        LOGGER.info('Cleaning up Doozer source dirs')
        await asyncio.gather(
            *[
                self.runtime.cleanup_sources('sources'),
                self.runtime.cleanup_sources('konflux_build_sources'),
            ]
        )

        # If any image failed to rebase or was skipped due to a parent failure, raise to make the pipeline unstable
        if self.rebase_failures or self.rebase_skipped_due_to_parent:
            parts = []
            if self.rebase_failures:
                parts.append(f'failed to rebase: {",".join(self.rebase_failures)}')
            if self.rebase_skipped_due_to_parent:
                parts.append(f'skipped (parent rebase failed): {",".join(self.rebase_skipped_due_to_parent)}')
            raise RuntimeError('Following images ' + '; '.join(parts))

    def trigger_bundle_build(self):
        if self.skip_bundle_build:
            LOGGER.warning("Skipping bundle build step because --skip-bundle-build flag is set")
            return

        record_log = self.parse_record_log()
        if not record_log:
            LOGGER.warning('record.log not found, skipping bundle build')
            return

        try:
            records = record_log.get('image_build_konflux', [])
            operator_nvrs = []
            for record in records:
                if record['has_olm_bundle'] == '1' and record['status'] == '0' and record.get('nvrs', None):
                    operator_nvrs.append(record['nvrs'].split(',')[0])
            if operator_nvrs:
                jenkins.start_olm_bundle_konflux(
                    build_version=self.version,
                    assembly=self.assembly,
                    operator_nvrs=operator_nvrs,
                    doozer_data_path=self.data_path or '',
                    doozer_data_gitref=self.data_gitref or '',
                )
        except Exception as e:
            LOGGER.exception(f"Failed to trigger bundle build: {e}")

    def parse_record_log(self) -> Optional[dict]:
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        if not record_log_path.exists():
            return None

        with record_log_path.open('r') as file:
            record_log: dict = record_util.parse_record_log(file)
            return record_log

    async def request_mass_rebuild(self):
        await self.slack_client.say(
            f':konflux: :loading-correct: Enqueuing mass rebuild for {self.version} :loading-correct:'
        )

        queue = locks.Keys.KONFLUX_MASS_REBUILD_QUEUE.value
        mapping = {self.version: mass_rebuild_score(self.version)}
        await redis.call('zadd', queue, mapping, nx=True)
        self.runtime.logger.info('Queued in for mass rebuild lock')

        return await locks.enqueue_for_lock(
            coro=self.rebase_and_build_images(),
            lock=Lock.KONFLUX_MASS_REBUILD,
            lock_name=Lock.KONFLUX_MASS_REBUILD.value,
            lock_id=self.lock_identifier,
            ocp_version=self.version,
            version_queue_name=queue,
        )

    async def rebase_and_build_images(self):
        if self.mass_rebuild:
            await self.slack_client.say(
                f':construction: Starting image builds for {self.version} mass rebuild :construction:'
            )

        await self.rebase_images(f"v{self.version}.0", self.release)

        # ART-14540: Notify component owners about missing branch protection
        record_log_path = Path(self.runtime.doozer_working) / "record.log"
        if record_log_path.exists():
            await util.notify_branch_protection_missing(
                version=self.version,
                doozer_working=self.runtime.doozer_working,
                mail_client=self.runtime.new_mail_client(),
            )

        await self.build_images()

        if self.mass_rebuild:
            await self.slack_client.say(f':done_it_is: Mass rebuild for {self.version} complete :done_it_is:')

    async def mirror_images(self):
        """
        Mirror non embargoed builds to quay.io/redhat-user-workloads/ocp-art-tenant/art-images-share
        """

        if not self.building_images():
            LOGGER.warning('No images will be mirrored')
            return

        if self.runtime.dry_run:
            LOGGER.info('Not mirroring images in dry run mode')
            return

        LOGGER.info(f'Mirroring images to {KONFLUX_DEFAULT_IMAGE_SHARE_REPO}...')

        record_log = self.parse_record_log()
        if not record_log:
            LOGGER.error('record.log not found!')
            return

        # Get the list of successful builds
        builds_to_mirror = [entry for entry in record_log.get('image_build_konflux', []) if not int(entry['status'])]

        async def sync_build(build):
            release = build["nvrs"].split("-")[-1]
            image_pullspec = build["image_pullspec"]

            if self.assembly != "stream":
                LOGGER.info(f"Not syncing {image_pullspec} because assembly {self.assembly} != stream")
                return

            if is_release_embargoed(release=release, build_system="konflux"):
                LOGGER.info(f"Not syncing {image_pullspec} because it is in an embargoed release")
                return

            image_tag = build["image_tag"]
            latest_tag = f'{build["name"]}-{self.version}'
            await sync_to_quay(image_pullspec, KONFLUX_DEFAULT_IMAGE_SHARE_REPO, [image_tag, latest_tag])

        await asyncio.gather(*[sync_build(build) for build in builds_to_mirror])

    async def run(self):
        # Unset XDG_RUNTIME_DIR to ensure we don't use default auth.json
        # All registry auth must come from explicit Jenkins credentials or oc login to temp files
        if 'XDG_RUNTIME_DIR' in os.environ:
            LOGGER.info('Unsetting XDG_RUNTIME_DIR to prevent use of default registry auth')
            del os.environ['XDG_RUNTIME_DIR']

        # Get Jenkins credentials
        quay_auth_file = os.getenv('QUAY_AUTH_FILE')
        redhat_registry_auth_file = os.getenv('KONFLUX_OPERATOR_INDEX_AUTH_FILE')

        if not quay_auth_file:
            raise ValueError(
                "QUAY_AUTH_FILE environment variable is required but not set. "
                "Ensure Jenkins credentials are properly bound."
            )

        # Build source files list
        source_files = [quay_auth_file]
        if redhat_registry_auth_file:
            source_files.append(redhat_registry_auth_file)

        # Build explicit credentials for QCI push (DPTP's CI registry)
        qci_user = os.environ.get('QCI_USER')
        qci_password = os.environ.get('QCI_PASSWORD')
        if not qci_user or not qci_password:
            raise ValueError(
                "QCI_USER and QCI_PASSWORD environment variables are required but not set. "
                "Ensure Jenkins credentials are properly bound."
            )
        credentials = [
            RegistryCredential(REGISTRY_QUAY_OPENSHIFT, qci_user, qci_password),
        ]

        # Global registry config for ALL pipeline operations
        # kubeconfig obtains CI registry credentials via `oc registry login`
        # This ensures all oc commands (including doozer's internal calls) use explicit credentials
        with RegistryConfig(
            kubeconfig=os.environ.get('KUBECONFIG'),
            source_files=source_files,
            registries=[
                REGISTRY_QUAY_OCP_RELEASE_DEV,  # For: ART release images
                KONFLUX_DEFAULT_IMAGE_REPO,  # For: Konflux builds (cosign attestations)
                KONFLUX_DEFAULT_IMAGE_SHARE_REPO,  # For: Konflux image sharing (sync_images)
                REGISTRY_REDHAT_IO,  # For: RHEL base images
                REGISTRY_BREW,  # For: parent images (uses registry.redhat.io creds via alias)
                REGISTRY_CI_OPENSHIFT,  # For: CI operations
            ],
            credentials=credentials,
        ) as global_auth_file:
            self._registry_auth_file = global_auth_file

            # cosign requires DOCKER_CONFIG pointing to a dir containing config.json
            docker_config_dir = tempfile.mkdtemp(prefix='docker_config_')
            docker_config_file = os.path.join(docker_config_dir, 'config.json')

            try:
                shutil.copy2(global_auth_file, docker_config_file)

                original_docker_config = os.environ.get('DOCKER_CONFIG')
                os.environ['DOCKER_CONFIG'] = docker_config_dir

                LOGGER.info(
                    f'Set registry auth file={global_auth_file} for pipeline operations '
                    f'(cherry-picked from {len(source_files)} source file(s))'
                )

                try:
                    await self._run_pipeline()
                finally:
                    if original_docker_config:
                        os.environ['DOCKER_CONFIG'] = original_docker_config
                    elif 'DOCKER_CONFIG' in os.environ:
                        del os.environ['DOCKER_CONFIG']
            finally:
                if os.path.exists(docker_config_dir):
                    shutil.rmtree(docker_config_dir)

    async def _run_pipeline(self):
        """Core pipeline logic wrapped by global registry auth config."""
        await self.initialize()

        # Rebase and build RPMs
        await self.rebase_and_build_rpms(self.release)

        # Build plashets if needed
        if not self.skip_plashets and uses_konflux_imagestream_override(self.version):
            group_param = f"openshift-{self.version}"
            jenkins.start_build_plashets(
                group=group_param,
                release=self.release,
                assembly=self.assembly,
                data_path=self.data_path,
                data_gitref=self.data_gitref,
                dry_run=self.runtime.dry_run,
                block_until_complete=True,
            )

        try:
            if self.use_mass_rebuild_locks and self.mass_rebuild:
                await self.request_mass_rebuild()
            else:
                await self.rebase_and_build_images()

        finally:
            # Track critical failures while ensuring problematic operations attempt to run
            critical_failures = []

            await self.sync_images()

            if uses_konflux_imagestream_override(self.version):
                await self.sweep_bugs()
                await self.sweep_golang_bugs()
                if not self.runtime.dry_run:
                    await self.sweep_second_fix_bugs()
                else:
                    LOGGER.info('Skipping second-fix bug sweep in dry run mode')
            else:
                LOGGER.info(
                    f'Skipping bug sweep for {self.version} since it is not in the override list and is handled by ocp4'
                )

            self.trigger_bundle_build()

            # Wrap problematic operations to prevent them from blocking each other or clean_up
            await run_safe(self.mirror_images, critical_failures)
            await run_safe(self.mirror_streams_to_ci, critical_failures)

            try:
                await self.clean_up()
            finally:
                # Add link to art-build-history at the end of the job
                job_url = os.getenv('BUILD_URL', '')
                build_history_url = build_history_link_url(
                    group=f'openshift-{self.version}', assembly=self.assembly, days=2, job_url=job_url
                )
                jenkins.update_description(f'<a href="{build_history_url}">View build history</a><br/>')

            # Re-raise to mark build as UNSTABLE if any critical operations failed
            if critical_failures:
                failed_ops = ", ".join(f"{name}: {err}" for name, err in critical_failures)
                raise RuntimeError(f"Critical operations failed in finally block: {failed_ops}")

    async def rebase_and_build_rpms(self, input_release: str):
        if (
            self.build_plan.rpm_build_strategy == BuildStrategy.ONLY and not self.build_plan.rpms_included
        ) or self.build_plan.rpm_build_strategy == BuildStrategy.NONE:
            LOGGER.warning('No RPMs will be built')
            return

        cmd = self._doozer_base_command.copy()
        cmd.extend(self.include_exclude_param('rpms'))
        cmd.extend(
            [
                'rpms:rebase-and-build',
                f'--version={self.version}',
                f'--release={input_release}',
            ]
        )

        if self.runtime.dry_run:
            cmd.append('--dry-run')

        # Rebase and build RPMs
        self.runtime.logger.info('Building RPMs')
        try:
            await exectools.cmd_assert_async(cmd)

        except ChildProcessError:
            self.handle_rpm_build_failure()

    def handle_rpm_build_failure(self):
        """
        Handle the failure of RPM builds by parsing the record log and updating the Jenkins description.
        """

        record_log = self.parse_record_log()
        if not record_log:
            raise RuntimeError('record.log not found!')

        failed_map = record_util.get_failed_rpms(record_log)
        if not failed_map:
            # failed so badly we don't know what failed; give up
            raise RuntimeError('No failed RPMs found in record.log!')

        failed_rpms = list(failed_map.keys())
        self.runtime.logger.warning('Following rpms failed to rebase or build: %s', ', '.join(failed_rpms))
        jenkins.update_description(f'Failed rpms: {", ".join(failed_rpms)}<br/>')


@cli.command("beta:ocp4-konflux", help="A pipeline to build images with Konflux for OCP 4")
@click.option(
    '--image-build-strategy',
    required=True,
    type=click.Choice(['all', 'none', 'only', 'except'], case_sensitive=False),
    help='Which images are candidates for building? "only/except" refer to the --image-list param',
)
@click.option(
    '--image-list',
    required=True,
    help='Comma/space-separated list to include/exclude per --image-build-strategy (e.g. logging-kibana5,openshift-jenkins-2)',
)
@click.option(
    '--rpm-build-strategy',
    required=True,
    type=click.Choice(['all', 'none', 'only', 'except'], case_sensitive=False),
    help='Which RPMs are candidates for building? "only/except" refer to the --rpm-list param',
)
@click.option(
    '--rpm-list',
    required=True,
    help='Comma/space-separated list to include/exclude per --rpm-build-strategy (e.g. openshift-ansible,openshift-clients)',
)
@click.option('--assembly', required=True, help='The name of an assembly to rebase & build for')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--version', required=True, help='OCP version to scan, e.g. 4.14')
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option("--kubeconfig", required=False, help="Path to kubeconfig file to use for Konflux cluster connections")
@click.option(
    '--ignore-locks',
    is_flag=True,
    default=False,
    help='Do not wait for other builds in this version to complete (use only if you know they will not conflict)',
)
@click.option("--skip-rebase", is_flag=True, help="(For testing) Skip the rebase step")
@click.option("--skip-bundle-build", is_flag=True, help="(For testing) Skip the bundle build step")
@click.option("--skip-build-sync-konflux", is_flag=True, help="(For testing) Skip the build-sync-konflux step")
@click.option(
    "--arch", "arches", metavar="TAG", multiple=True, help="(Optional) [MULTIPLE] Limit included arches to this list"
)
@click.option(
    '--plr-template',
    required=False,
    default='',
    help='Override the Pipeline Run template commit from openshift-priv/art-konflux-template; format: <owner>@<branch>',
)
@click.option(
    '--skip-plashets',
    is_flag=True,
    default=False,
    help='Do not build plashets (for example to save time when running multiple builds against test assembly)',
)
@click.option(
    '--build-priority',
    type=str,
    metavar='PRIORITY',
    default='auto',
    required=True,
    help='Kueue build priority. Use "auto" for automatic resolution from image/group config, or specify a number 1-10 (where 1 is highest priority). Takes precedence over group and image config settings.',
)
@click.option(
    '--use-mass-rebuild-locks',
    is_flag=True,
    default=False,
    help='Use legacy mass rebuild locks instead of Kueue priorities (for fallback/revert scenarios).',
)
@click.option(
    '--network-mode',
    type=click.Choice(['hermetic', 'internal-only', 'open']),
    help='Override network mode for Konflux builds. Takes precedence over image and group config settings.',
)
@pass_runtime
@click_coroutine
async def ocp4(
    runtime: Runtime,
    image_build_strategy: str,
    image_list: Optional[str],
    rpm_build_strategy: str,
    rpm_list: Optional[str],
    assembly: str,
    data_path: Optional[str],
    version: str,
    data_gitref: Optional[str],
    kubeconfig: Optional[str],
    ignore_locks: bool,
    skip_rebase: bool,
    skip_bundle_build: bool,
    skip_build_sync_konflux: bool,
    arches: Tuple[str, ...],
    plr_template: str,
    skip_plashets,
    build_priority: Optional[str],
    use_mass_rebuild_locks: bool,
    network_mode: Optional[str],
):
    if not kubeconfig:
        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    lock_identifier = jenkins.get_build_path_or_random()

    pipeline = KonfluxOcpPipeline(
        runtime=runtime,
        assembly=assembly,
        data_path=data_path,
        image_build_strategy=image_build_strategy,
        image_list=image_list,
        rpm_build_strategy=rpm_build_strategy,
        rpm_list=rpm_list,
        version=version,
        data_gitref=data_gitref,
        kubeconfig=kubeconfig,
        skip_rebase=skip_rebase,
        skip_bundle_build=skip_bundle_build,
        skip_build_sync_konflux=skip_build_sync_konflux,
        arches=arches,
        plr_template=plr_template,
        lock_identifier=lock_identifier,
        skip_plashets=skip_plashets,
        build_priority=build_priority,
        use_mass_rebuild_locks=use_mass_rebuild_locks,
        network_mode=network_mode,
    )

    if ignore_locks:
        await pipeline.run()
    else:
        await locks.run_with_lock(
            coro=pipeline.run(),
            lock=Lock.BUILD_KONFLUX,
            lock_name=Lock.BUILD_KONFLUX.value.format(version=version, assembly=assembly),
            lock_id=lock_identifier,
        )
