import click

from artcommonlib import logutil
from elliottlib import Runtime
from elliottlib.cli.common import cli, pass_runtime

LOGGER = logutil.get_logger(__name__)


@cli.command('find-k-builds', short_help='Find Konflux builds')
@click.option(
    '--kind', '-k', metavar='KIND', required=True,
    type=click.Choice(['rpm', 'image']),
    help='Find builds of the given KIND [rpm, image]')
@pass_runtime
def find_k_builds_cli(runtime: Runtime, kind):
    runtime.initialize(mode='images' if kind == 'image' else 'rpms')
    if runtime.konflux_db is None:
        raise RuntimeError('Must run Elliott with Konflux DB initialized')

    LOGGER.info('searching for builds of kind %s in group %s', kind, runtime.group)
    builds = runtime.konflux_db.search_builds_by_fields(
        names=['group', 'artifact_type'],
        values=[runtime.group, kind]
    )
    for build in builds:
        LOGGER.info('Found build: %s', build)
