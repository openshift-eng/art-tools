import click
import sys

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from doozerlib import constants
from artcommonlib import logutil
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, Engine, ArtifactType, KonfluxBuildRecord

LOGGER = logutil.get_logger(__name__)


class CreateSnapshotCli:
    def __init__(self, runtime: Runtime, konflux_kubeconfig, konflux_context, konflux_namespace, builds, dry_run):
        self.runtime = runtime
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.builds = builds
        self.dry_run = dry_run

    async def run(self):
        self.runtime.initialize(mode='images')
        if self.runtime.konflux_db is None:
            raise RuntimeError('Must run Elliott with Konflux DB initialized')
        self.runtime.konflux_db.bind(KonfluxBuildRecord)

    async def fetch_builds(self):
        if bool(self.builds):
            names = self.builds
        else:
            metas = self.runtime.image_metas()
            names = [meta.distgit_key for meta in metas]
        args = {
            'names': names,
            'group': self.runtime.group,
            'assembly': self.runtime.assembly,
            'outcome': KonfluxBuildOutcome.SUCCESS
        }
        builds = await self.runtime.konflux_db.get_latest_builds(**args)
        missing_builds = [name for name in names if name not in [b.name for b in builds]]
        if missing_builds:
            LOGGER.warning('Builds have not been found for these components: %s', ', '.join(missing_builds))
        return builds


@cli.group("snapshot", short_help="Commands for managing Konflux Snapshots")
def release_snapshot_cli():
    pass


@release_snapshot_cli.command("new", short_help="Create a new Konflux Snapshot in the given namespace for the given "
                                                "builds")
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=constants.KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.argument('builds', metavar='<NVR>', nargs=-1, required=False, default=None)
@click.option(
    "--builds-file", "-f", "builds_file",
    help="File to read builds from, `-` to read from STDIN.",
    type=click.File("rt"),
)
@click.option('--dry-run', is_flag=True, help='Do not actually create the snapshot, just print what would be done.')
@click.pass_obj
@click_coroutine
async def new_snapshot(runtime: Runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                       builds_file, builds, dry_run):
    """
    Create a new Konflux Snapshot in the given namespace for the given builds

    \b
    $ elliott snapshot new --builds-file builds.txt
    """
    if bool(builds) and bool(builds_file):
        raise click.BadParameter("Use only one of --build or --builds-file")

    if builds_file:
        if builds_file == "-":
            builds_file = sys.stdin
        builds = [line.strip() for line in builds_file.readlines()]

    pipeline = CreateSnapshotCli(runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                                 builds, dry_run)
    await pipeline.run()
