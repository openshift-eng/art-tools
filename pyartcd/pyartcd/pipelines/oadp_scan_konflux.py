import asyncio
import logging
import os

import click
import yaml
from artcommonlib import exectools

from pyartcd import constants, jenkins, locks, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime


class OadpScanPipeline:
    def __init__(self, runtime, group, data_path, assembly, data_gitref, image_list):
        self.runtime = runtime
        self.group = group
        self.data_path = data_path
        self.assembly = assembly
        self.data_gitref = data_gitref
        self.image_list = image_list

        self.logger = logging.getLogger(__name__)
        self._doozer_working = self.runtime.working_dir / "doozer_working"
        self.changes = {}

        self.skipped = True  # True by default; if not locked, run() will set it to False

        group_param = self.group
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'
        self.doozer_base_command = [
            'doozer',
            f'--working-dir={self._doozer_working}',
            f'--data-path={self.data_path}',
            f'--group={group_param}',
            f'--assembly={self.assembly}',
            '--build-system=konflux',
        ]

    async def run(self):
        # If we get here, lock could be acquired
        self.skipped = False
        scan_info = f'Scanning OADP group {self.group}, assembly {self.assembly}, data path {self.data_path}'

        if self.data_gitref:
            scan_info += f'@{self.data_gitref}'
        self.logger.info(scan_info)

        self.check_params()

        # Scan for changes (OADP only has images/RPMs, no RHCOS)
        await self.get_changes()

        # Handle image source changes
        self.handle_source_changes()

    def check_params(self):
        """
        Make sure non-stream assemblies, custom forks and branches are only used with dry run mode
        """

        if not self.runtime.dry_run:
            # TODO: Uncomment after logging can support stream builds
            # if self.assembly != 'stream':
            #     raise ValueError('non-stream assemblies are only allowed in dry-run mode')
            if self.data_path != constants.OCP_BUILD_DATA_URL or self.data_gitref:
                raise ValueError('Custom data paths can only be used in dry-run mode')

    async def get_changes(self):
        """
        Check for changes by calling doozer config:scan-sources
        Changed rpms and images are recorded in self.changes
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
        self.logger.info('scan-sources output for %s:\n%s', self.group, out)

        yaml_data = yaml.safe_load(out)
        self.changes = util.get_changes(yaml_data)
        if self.changes:
            self.logger.info('Detected source changes:\n%s', yaml.safe_dump(self.changes))
        else:
            self.logger.info('No changes detected in OADP RPMs or images')

    def handle_source_changes(self):
        if not self.changes:
            return

        jenkins.update_title(' [SOURCE CHANGES]')
        self.logger.info('Detected at least one updated OADP image')

        image_list = self.changes.get('images', [])

        # Do NOT trigger konflux builds in dry-run mode
        if self.runtime.dry_run:
            self.logger.info('Would have triggered an OADP %s build', self.group)
            return

        # Update build description
        jenkins.update_description(f'Changed {len(image_list)} OADP images<br/>')

        # Trigger OADP build
        self.logger.info('Triggering an OADP %s build', self.group)
        jenkins.start_oadp(
            group=self.group,
            assembly=self.assembly,
            image_list=image_list,
        )


@cli.command('beta:konflux:oadp-scan')
@click.option('--group', required=True, help='OADP group to scan (e.g., oadp-1.5)')
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
async def oadp_scan(runtime: Runtime, group: str, assembly: str, data_path: str, data_gitref, image_list: str):
    # KUBECONFIG env var must be defined in order to scan sources
    if not os.getenv('KUBECONFIG'):
        raise RuntimeError('Environment variable KUBECONFIG must be defined')

    jenkins.init_jenkins()

    pipeline = OadpScanPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        image_list=image_list,
    )

    if runtime.dry_run:
        await pipeline.run()

    else:
        lock = Lock.OADP_SCAN
        lock_name = lock.value.format(group=group)
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
            build_lock = Lock.OADP_BUILD
            build_lock_name = build_lock.value.format(group=group)
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
