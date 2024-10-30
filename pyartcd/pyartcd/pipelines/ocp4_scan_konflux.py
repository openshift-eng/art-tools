import logging

import click
import yaml

from pyartcd import util, jenkins
from pyartcd import constants, exectools
from pyartcd.cli import cli, pass_runtime, click_coroutine
from pyartcd.runtime import Runtime


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

    async def run(self):
        scan_info = f'Scanning version {self.version}, assembly {self.assembly}, data path {self.data_path}'
        if self.data_gitref:
            scan_info += f'@{self.data_gitref}'
        self.logger.info(scan_info)

        self.check_params()
        await self.get_changes()

        # Handle image source changes
        if self.changes.get('images', None):
            self.logger.info('Detected at least one updated image')
            image_list = self.changes.get('images', [])

            # Do NOT trigger konflux builds in dry-run mode
            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s ocp4 build', self.version)
                return

            # Update build description
            jenkins.update_description(f'Changed {len(image_list)} images<br/>')

            # Trigger ocp4-konflux
            self.logger.info('Triggering a %s ocp4-konflux build', self.version)
            jenkins.start_ocp4_konflux(
                build_version=self.version,
                assembly='stream',
                image_list=image_list,
            )

        else:
            self.logger.info('*** No changes detected')
            jenkins.update_title(' [NO CHANGES]')

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

        group_param = f'openshift-{self.version}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'

        # Run doozer konflux:scan-sources. --rebase-priv is always disabled for now, as it might conflict
        # with rebase operations triggered by regular ocp4-scan. In the future, we'll have to add the --rebase-priv
        # options to the Doozer invocation
        cmd = [
            'doozer',
            f'--working-dir={self._doozer_working}',
            f'--data-path={self.data_path}',
            f'--group={group_param}',
            f'--assembly={self.assembly}'
        ]
        if self.image_list:
            cmd.append(f'--images={self.image_list}')
        cmd.extend([
            'beta:config:konflux:scan-sources',
            '--yaml'
        ])
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        self.logger.info('scan-sources output for openshift-%s:\n%s', self.version, out)

        yaml_data = yaml.safe_load(out)
        self.changes = util.get_changes(yaml_data)
        if self.changes:
            self.logger.info('Detected source changes:\n%s', yaml.safe_dump(self.changes))
        else:
            self.logger.info('No changes detected in RPMs, images or RHCOS')


@cli.command('beta:konflux:ocp4-scan')
@click.option('--version', required=True, help='OCP version to scan')
@click.option('--assembly', required=False, default='stream', help='Assembly to scan for')
@click.option('--data-path', required=False, default=constants.OCP_BUILD_DATA_URL,
              help='ocp-build-data fork to use (e.g. assembly definition in your own fork)')
@click.option('--data-gitref', required=False, default='',
              help='Doozer data path git [branch / tag / sha] to use')
@click.option('--image-list', required=False,
              help='Comma/space-separated list to of images to scan, empty to scan all')
@click.option('--ignore-locks', is_flag=True, default=False,
              help='Do not wait for other builds in this version to complete (only allowed in dry-run mode)')
@pass_runtime
@click_coroutine
async def ocp4_scan(runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref,
                    image_list: str, ignore_locks: bool):
    jenkins.init_jenkins()

    # TODO handle locks
    if ignore_locks and not runtime.dry_run:
        raise RuntimeError('--ignore-locks can only by used with --dry-run')

    await Ocp4ScanPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        image_list=image_list,
    ).run()
