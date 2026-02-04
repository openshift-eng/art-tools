import logging
import os

import click
import yaml
from artcommonlib import exectools
from doozerlib.metadata import RebuildHintCode

from pyartcd import constants, jenkins, locks, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.util import has_layered_rhcos


class Ocp4ScanPipeline:
    def __init__(self, runtime, version, data_path, assembly, data_gitref, image_list):
        self.runtime = runtime
        self.version = version
        self.data_path = data_path
        self.assembly = assembly
        self.data_gitref = data_gitref
        self.image_list = image_list

        self.logger = logging.getLogger(__name__)
        self._doozer_working = self.runtime.working_dir / "doozer_working"
        self.changes = {}
        self.report = {}

        self.rhcos_updated = False
        self.rhcos_outdated = False
        self.rhcos_inconsistent = False
        self.inconsistent_rhcos_rpms = None

        self.skipped = True  # True by default; if not locked, run() will set it to False

        group_param = f'openshift-{self.version}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'
        self.doozer_base_command = [
            'doozer',
            f'--working-dir={self._doozer_working}',
            f'--data-path={self.data_path}',
            f'--group={group_param}',
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            '--load-okd-only',
        ]

    async def run(self):
        # If we get here, lock could be acquired
        self.skipped = False
        scan_info = f'Scanning version {self.version}, assembly {self.assembly}, data path {self.data_path}'

        if self.data_gitref:
            scan_info += f'@{self.data_gitref}'
        self.logger.info(scan_info)

        self.check_params()

        # Scan for changes and RHCOS inconsistencies
        await self.get_changes()
        await self.get_rhcos_inconsistencies()

        # Handle image source changes
        self.handle_source_changes()

        # Handle RHCOS changes or inconsistencies
        await self.handle_rhcos_changes()

    def check_params(self):
        """
        Make sure non-stream assemblies, custom forks and branches are only used with dry run mode
        """

        if not self.runtime.dry_run:
            if self.assembly != 'stream':
                raise ValueError('non-stream assemblies are only allowed in dry-dun mode')
            if self.data_path != constants.OCP_BUILD_DATA_URL or self.data_gitref:
                raise ValueError('Custom data paths can only be used in dry-run mode')

    async def get_changes(self):
        """
        Check for changes by calling doozer config:scan-sources
        Changed rpms, images or rhcos are recorded in self.changes
        self.rhcos_changed is also updated accordingly
        """

        cmd = self.doozer_base_command.copy()
        if self.image_list:
            cmd.append(f'--images={self.image_list}')
        cmd.extend(
            [
                'beta:config:konflux:scan-sources',
                '--yaml',
                f'--ci-kubeconfig={os.environ["KUBECONFIG"]}',
                '--rebase-priv',
            ]
        )
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        self.logger.info('scan-sources output for openshift-%s:\n%s', self.version, out)

        self.report = yaml.safe_load(out)
        self.changes = util.get_changes(self.report)
        if self.changes:
            self.logger.info('Detected source changes:\n%s', yaml.safe_dump(self.changes))
        else:
            self.logger.info('No changes detected in RPMs, images or RHCOS')

        # Check for RHCOS changes
        if self.changes.get('rhcos', None):
            for rhcos_change in self.changes['rhcos']:
                if rhcos_change['reason'].get('updated', None):
                    self.rhcos_updated = True
                if rhcos_change['reason'].get('outdated', None):
                    self.rhcos_outdated = True

    async def get_rhcos_inconsistencies(self):
        """
        Check for RHCOS inconsistencies by calling doozer inspect:stream INCONSISTENT_RHCOS_RPMS
        """

        cmd = self.doozer_base_command + [
            'inspect:stream',
            'INCONSISTENT_RHCOS_RPMS',
            '--strict',
        ]

        try:
            _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
            self.logger.info(out)
            self.rhcos_inconsistent = False

        except ChildProcessError as e:
            self.rhcos_inconsistent = True
            self.inconsistent_rhcos_rpms = e

    def handle_source_changes(self):
        if not self.changes:
            return

        self.logger.info('Detected source changes')

        # Determine major version to call the appropriate job
        major_version = int(self.version.split('.')[0])

        # Trigger jobs based on major version
        # Note: OCP5 uses the same ocp4-konflux job as OCP4
        match major_version:
            case 5 | 4:
                self.trigger_ocp4()
                if major_version == 4:
                    self.trigger_okd4()
            case _:
                raise ValueError(f'Unsupported OCP major version: {major_version}')

    def trigger_ocp4(self):
        changed_rpm = self.changes.get('rpms', [])

        # Filter out okd-only images (mode: disabled, okd.mode: enabled)
        # These will be built by trigger_okd4() only
        changed_ocp_images = [
            image['name']
            for image in self.report.get('images', [])
            if image.get('changed') and not image.get('okd_only')
        ]

        if not changed_ocp_images and not changed_rpm:
            self.logger.info('No OCP images/RPMs to build')
            return

        # Update Jenkins title and description
        jenkins.update_title(' [SOURCE CHANGES]')
        if changed_rpm:
            jenkins.update_description(f'Changed {len(changed_rpm)} RPMs<br/>')
        if changed_ocp_images:
            jenkins.update_description(f'Changed {len(changed_ocp_images)} OCP images<br/>')

        if self.runtime.dry_run:
            self.logger.info('Would have triggered a %s ocp4 build for %s', self.version, ','.join(changed_ocp_images))
            return

        # Trigger ocp4-konflux
        self.logger.info('Triggering a %s ocp4-konflux build with %d images', self.version, len(changed_ocp_images))
        jenkins.start_ocp4_konflux(
            build_version=self.version,
            assembly='stream',
            image_list=changed_ocp_images,
            rpm_list=changed_rpm,
        )

    def trigger_okd4(self):
        # Only trigger OKD builds for enabled versions
        if self.version not in constants.OKD_ENABLED_VERSIONS:
            self.logger.info(
                'Skipping OKD4 build for version %s (enabled versions: %s)',
                self.version,
                constants.OKD_ENABLED_VERSIONS,
            )
            return

        # Valid reasons to rebuild an image in OKD
        rebuild_reasons = [
            RebuildHintCode.NO_LATEST_BUILD,
            RebuildHintCode.LAST_BUILD_FAILED,
            RebuildHintCode.NEW_UPSTREAM_COMMIT,
            RebuildHintCode.UPSTREAM_COMMIT_MISMATCH,
            RebuildHintCode.ANCESTOR_CHANGING,
            RebuildHintCode.CONFIG_CHANGE,
            RebuildHintCode.BUILDER_CHANGING,
            RebuildHintCode.DEPENDENCY_NEWER,
        ]

        # Filter images to only those with valid rebuild hint codes
        changed_okd_images = []
        for image in self.report.get('images', []):
            if not image.get('changed'):
                continue
            code_str = image.get('code')
            if not code_str:
                continue

            try:
                # Access enum member by name using bracket notation
                code = RebuildHintCode[code_str]
                if code in rebuild_reasons:
                    changed_okd_images.append(image['name'])

            except (KeyError, TypeError):
                # Skip images with invalid or missing codes
                self.logger.warning(f"Invalid rebuild hint code '{code_str}' for image {image.get('name', 'unknown')}")
                continue

        if not changed_okd_images:
            self.logger.info('No images found with valid rebuild reasons for OKD4')
            return

        # Update Jenkins title and description
        jenkins.update_title(' [SOURCE CHANGES]')
        jenkins.update_description(f'Changed {len(changed_okd_images)} images for OKD4<br/>')

        if self.runtime.dry_run:
            self.logger.info(
                'Would have triggered a %s okd4 build with images %s', self.version, ','.join(changed_okd_images)
            )
            return

        # Trigger okd4 build
        self.logger.info('Triggering a %s okd4 build with %d images', self.version, len(changed_okd_images))
        jenkins.start_okd4(
            build_version=self.version,
            assembly='stream',
            image_list=changed_okd_images,
        )

    async def handle_rhcos_changes(self):
        rhcos_changes = False

        if self.rhcos_inconsistent or self.rhcos_outdated:
            rhcos_changes = True
            # Update Jenkins title and description
            jenkins.update_title(' [RHCOS CHANGES]')

            if self.rhcos_inconsistent:
                self.logger.info('Detected inconsistent RHCOS RPMs:\n%s', self.inconsistent_rhcos_rpms)
                jenkins.update_description('RHCOS inconsistent<br/>')
            if self.rhcos_outdated:
                self.logger.info('Detected outdated RHCOS RPMs:\n%s', self.changes.get('rhcos', None))
                jenkins.update_description('RHCOS outdated<br/>')

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s RHCOS build', self.version)
                return

            # Inconsistency probably means partial failure and we would like to retry.
            # but don't kick off more if already in progress.
            self.logger.info('Triggering a %s RHCOS build for consistency', self.version)
            layered_rhcos = await has_layered_rhcos(self.doozer_base_command)
            job_name = 'build-node-image' if layered_rhcos else 'build'
            jenkins.start_rhcos(build_version=self.version, new_build=False, job_name=job_name)

        elif self.rhcos_updated:
            rhcos_changes = True
            self.logger.info('Detected at least one updated RHCOS')

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s build-sync build', self.version)
                return

            self.logger.info('Triggering a %s build-sync to pick up latest RHCOS', self.version)
            jenkins.start_build_sync(
                build_version=self.version,
                assembly="stream",
                build_system="konflux",
            )

        if rhcos_changes:
            jenkins.update_title(' [RHCOS CHANGES]')


@cli.command('beta:konflux:ocp4-scan')
@click.option('--version', required=True, help='OCP version to scan')
@click.option('--assembly', required=False, default='stream', help='Assembly to scan for')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option('--image-list', required=False, help='Comma/space-separated list to of images to scan, empty to scan all')
@pass_runtime
@click_coroutine
async def ocp4_scan(runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref, image_list: str):
    # KUBECONFIG env var must be defined in order to scan sources
    if not os.getenv('KUBECONFIG'):
        raise RuntimeError('Environment variable KUBECONFIG must be defined')

    jenkins.init_jenkins()

    pipeline = Ocp4ScanPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        image_list=image_list,
    )

    if runtime.dry_run:
        await pipeline.run()

    else:
        lock = Lock.SCAN_KONFLUX
        lock_name = lock.value.format(version=version)
        lock_identifier = jenkins.get_build_path()
        if not lock_identifier:
            runtime.logger.warning(
                'Env var BUILD_URL has not been defined: a random identifier will be used for the locks'
            )

        # Scheduled builds are already being skipped if the lock is already acquired.
        # For manual builds, we need to check if the build and scan locks are already acquired,
        # and skip the current build if that's the case.
        # Should that happen, signal it by appending a [SKIPPED][LOCKED] to the build title
        async def run_with_build_lock():
            build_lock = Lock.BUILD_KONFLUX
            build_lock_name = build_lock.value.format(version=version)
            await locks.run_with_lock(
                coro=pipeline.run(),
                lock=build_lock,
                lock_name=build_lock_name,
                lock_id=lock_identifier,
                skip_if_locked=True,
            )

        await locks.run_with_lock(
            coro=run_with_build_lock(),
            lock=lock,
            lock_name=lock_name,
            lock_id=lock_identifier,
            skip_if_locked=True,
        )

    if pipeline.skipped:
        jenkins.update_title(' [SKIPPED][LOCKED]')
