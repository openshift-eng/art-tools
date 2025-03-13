import click

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from artcommonlib.constants import KONFLUX_RELEASE_DATA_URL
from artcommonlib import exectools


class KonfluxReleasePipeline:
    """ Create Konflux Release """

    def __init__(self,
                 runtime: Runtime,
                 group: str,
                 assembly: str,
                 konflux_release_path: str,
                 config_filename: str,
                 release_env: str,
                 force: bool):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.force = force
        self.konflux_release_path = konflux_release_path
        self.config_filename = config_filename
        self.release_env = release_env

        self._working_dir = self.runtime.working_dir.absolute()

        if not self.konflux_release_path:
            self.konflux_release_path = KONFLUX_RELEASE_DATA_URL

        self.dry_run = self.runtime.dry_run
        self.elliott_working_dir = self._working_dir / "elliott-working"

        self._elliott_base_command = [
            'elliott',
            f'--group={self.group}',
            f'--working-dir={self.elliott_working_dir}',
            f'--konflux-release-path={self.konflux_release_path}',
        ]
        if self.assembly:
            self._elliott_base_command.append(f'--assembly={self.assembly}')

    async def run(self):
        await self.create_release()

    async def create_release(self) -> int:
        cmd = self._elliott_base_command + [
            "release",
            "new",
            f"--env={self.release_env}",
        ]
        if self.config_filename:
            cmd.append(f"--filename={self.config_filename}")
        if self.force:
            cmd.append("--force")
        if not self.dry_run:
            cmd.append("--apply")
        await exectools.cmd_assert_async(cmd)


@cli.command("konflux-release")
@click.option("--konflux-release-path", metavar='KONFLUX_RELEASE_PATH', default=None,
              help="Git repo or directory containing group metadata for konflux release e.g."
                   f" {KONFLUX_RELEASE_DATA_URL}. Defaults to the default branch of the repo - to point to a "
                   f"branch/commit use repo@commitish")
@click.option("-g", "--group", metavar='NAME', required=True,
              help="The group to operate on. e.g. openshift-4.18")
@click.option("--assembly", metavar="ASSEMBLY_NAME",
              help="(Optional) The name of the associated assembly e.g. 4.18.1")
@click.option("--config-filename", metavar="CONFIG_FILENAME",
              help="(Optional) Release config filename to use. Defaults to assembly name")
@click.option("--env", metavar="RELEASE_ENV", required=True,
              help="The release env to operate on in the config file e.g. stage, prod")
@click.option('--force', is_flag=True, help='Proceed even if an associated release/advisory detected')
@pass_runtime
@click_coroutine
async def konflux_release(runtime: Runtime, konflux_release_path: str, group: str, assembly: str, config_filename: str,
                          env: str, force: bool):
    pipeline = KonfluxReleasePipeline(runtime=runtime, konflux_release_path=konflux_release_path, group=group,
                                      assembly=assembly, config_filename=config_filename,
                                      release_env=env, force=force)
    await pipeline.run()
