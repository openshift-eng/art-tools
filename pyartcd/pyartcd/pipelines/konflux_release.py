import click
from ruamel.yaml import YAML

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from artcommonlib.constants import SHIPMENT_DATA_URL
from artcommonlib import exectools, logutil

yaml = YAML()
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)

LOGGER = logutil.get_logger(__name__)


class KonfluxReleasePipeline:
    """ Create Konflux Release """

    def __init__(self,
                 runtime: Runtime,
                 group: str,
                 assembly: str,
                 shipment_path: str,
                 config: str,
                 release_env: str,
                 force: bool):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.force = force
        self.shipment_path = shipment_path
        self.config = config
        self.release_env = release_env

        self._working_dir = self.runtime.working_dir.absolute()
        self.dry_run = self.runtime.dry_run
        self.elliott_working_dir = self._working_dir / "elliott-working"

        self._elliott_base_command = [
            'elliott',
            f'--group={self.group}',
            f'--assembly={self.assembly}',
            f'--working-dir={self.elliott_working_dir}',
        ]
        if self.shipment_path:
            self._elliott_base_command.append(f'--shipment-path={self.shipment_path}')

    async def run(self):
        release_name = await self.create_release()
        await self.watch_release(release_name)

    async def create_release(self) -> int:
        cmd = self._elliott_base_command + [
            "release",
            "new",
            f"--env={self.release_env}",
            f"--config={self.config}"
        ]
        if self.force:
            cmd.append("--force")
        if not self.dry_run:
            cmd.append("--apply")
        _, out, err = await exectools.cmd_gather_async(cmd)
        LOGGER.info(err)
        LOGGER.info(out)
        release_obj = yaml.load(out)
        return release_obj.get('metadata', {}).get('name')

    async def watch_release(self, release_name, timeout=5) -> int:
        cmd = self._elliott_base_command + [
            "release",
            "watch",
            release_name,
            f"--timeout={timeout}",
        ]
        if self.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd)


@cli.command("konflux-release")
@click.option("--shipment-path", metavar='SHIPMENT_PATH', default=None,
              help="Git repo or directory containing group metadata for shipping a konflux release e.g."
                   f" {SHIPMENT_DATA_URL.format('ocp')}. Defaults to `main` branch for a repo - to point to a "
                   "different branch/commit use repo@commitish")
@click.option("-g", "--group", metavar='GROUP', required=True,
              help="The group to operate on. e.g. openshift-4.18")
@click.option("--assembly", metavar="ASSEMBLY", required=True,
              help="The name of the associated assembly e.g. 4.18.1")
@click.option("--config", metavar="CONFIG_PATH", required=True,
              help="Path of the shipment config file to use for creating release. The path should be from the root of the  "
                   "given shipment-data repo.")
@click.option('--env', metavar='RELEASE_ENV', required=True, type=click.Choice(["stage", "prod"]),
              help='Release environment to create the release for')
@click.option('--force', is_flag=True,
              help='Proceed even if an associated release/advisory detected')
@pass_runtime
@click_coroutine
async def konflux_release(runtime: Runtime, shipment_path: str, group: str, assembly: str, config: str,
                          env: str, force: bool):
    pipeline = KonfluxReleasePipeline(runtime=runtime,
                                      shipment_path=shipment_path,
                                      group=group,
                                      assembly=assembly,
                                      config=config,
                                      release_env=env,
                                      force=force)
    await pipeline.run()
