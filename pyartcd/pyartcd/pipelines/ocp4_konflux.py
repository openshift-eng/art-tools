import logging
import os
from typing import Optional, Tuple

import click
from artcommonlib import exectools

from pyartcd import constants, util, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)


class KonfluxOcp4Pipeline:
    def __init__(self, runtime: Runtime, assembly: str, data_path: Optional[str],
                 image_list: Optional[str], version: str, data_gitref: Optional[str],
                 kubeconfig: Optional[str], skip_rebase: bool, arches: Tuple[str, ...]):
        self.runtime = runtime
        self.image_list = image_list
        self._doozer_working = os.path.abspath(f'{self.runtime.working_dir / "doozer_working"}')
        self.version = version
        self.kubeconfig = kubeconfig
        self.arches = arches
        self.skip_rebase = skip_rebase

        group_param = f'--group=openshift-{version}'
        if data_gitref:
            group_param += f'@{data_gitref}'

        self._doozer_base_command = [
            'doozer',
            f'--assembly={assembly}',
            f'--working-dir={self._doozer_working}',
            f'--data-path={data_path}',
            group_param
        ]

    async def rebase(self, version: str, input_release: str):
        cmd = self._doozer_base_command.copy()
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))

        image_list = self.image_list or ''
        cmd.extend([
            '--latest-parent-version',
            f'--images={image_list}',
            'beta:images:konflux:rebase',
            f'--version={version}',
            f'--release={input_release}',
            f"--message='Updating Dockerfile version and release {version}-{input_release}'",
        ])
        if not self.runtime.dry_run:
            cmd.append('--push')
        await exectools.cmd_assert_async(cmd)

    async def build(self):
        cmd = self._doozer_base_command.copy()
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))

        image_list = self.image_list or ''
        cmd.extend([
            '--latest-parent-version',
            f'--images={image_list}',
            'beta:images:konflux:build',
            "--konflux-namespace=ocp-art-tenant",
        ])
        if self.kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.kubeconfig])
        if self.runtime.dry_run:
            cmd.append('--dry-run')
        await exectools.cmd_assert_async(cmd)

    async def run(self):
        version = f"v{self.version}.0"
        input_release = util.default_release_suffix()

        if self.skip_rebase:
            LOGGER.warning("Skipping rebase step because --skip-rebase flag is set")
        else:
            LOGGER.info(f"Rebasing images for OCP {self.version} with release {input_release}")
            await self.rebase(version, input_release)

        LOGGER.info(f"Building images for OCP {self.version} with release {input_release}")
        await self.build()

        LOGGER.info("All builds completed successfully")


@cli.command("beta:ocp4-konflux", help="A pipeline to build images with Konflux for OCP 4")
@click.option('--image-list', required=True,
              help='Comma/space-separated list to include/exclude per BUILD_IMAGES '
                   '(e.g. logging-kibana5,openshift-jenkins-2)')
@click.option('--assembly', required=True, help='The name of an assembly to rebase & build for')
@click.option('--data-path', required=False, default=constants.OCP_BUILD_DATA_URL,
              help='ocp-build-data fork to use (e.g. assembly definition in your own fork)')
@click.option('--version', required=True, help='OCP version to scan, e.g. 4.14')
@click.option('--data-gitref', required=False, default='',
              help='Doozer data path git [branch / tag / sha] to use')
@click.option("--kubeconfig", required=False, help="Path to kubeconfig file to use for Konflux cluster connections")
@click.option("--skip-rebase", is_flag=True, help="(For testing) Skip the rebase step")
@click.option("--arch", "arches", metavar="TAG", multiple=True,
              help="(Optional) [MULTIPLE] Limit included arches to this list")
@pass_runtime
@click_coroutine
async def ocp4(runtime: Runtime, assembly: str, data_path: Optional[str], image_list: Optional[str],
               version: str, data_gitref: Optional[str], kubeconfig: Optional[str], skip_rebase: bool, arches: Tuple[str, ...]):
    if not kubeconfig:
        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    pipeline = KonfluxOcp4Pipeline(
        runtime=runtime,
        assembly=assembly,
        data_path=data_path,
        image_list=image_list,
        version=version,
        data_gitref=data_gitref,
        kubeconfig=kubeconfig,
        skip_rebase=skip_rebase,
        arches=arches)

    await locks.run_with_lock(
        coro=pipeline.run(),
        lock=Lock.BUILD_KONFLUX,
        lock_name=Lock.BUILD_KONFLUX.value.format(version=version),
        lock_id=lock_identifier
    )
