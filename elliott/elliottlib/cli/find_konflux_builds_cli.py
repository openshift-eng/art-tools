import click

from artcommonlib import logutil
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, Engine, ArtifactType, KonfluxBuildRecord
from elliottlib.runtime import Runtime
from elliottlib.cli.common import cli, pass_runtime, click_coroutine

LOGGER = logutil.get_logger(__name__)


@cli.command('find-k-builds', short_help='Find Konflux builds')
@click.option(
    '--kind', '-k', metavar='KIND', required=True,
    type=click.Choice([e.value for e in ArtifactType]),
    help=f'Find builds of the given KIND {[e.value for e in ArtifactType]}')
@click.option(
    '--engine', '-e', metavar='ENGINE',
    type=click.Choice([e.value for e in Engine]),
    help=f'Limit builds to ones built in the given ENGINE {[e.value for e in Engine]}')
@click.option('--outcome', type=click.Choice([e.value for e in KonfluxBuildOutcome]),
              help=f'Limit builds to given OUTCOME {[e.value for e in KonfluxBuildOutcome]}')
@click.option('--output', '-o', type=click.Choice(['json']),
              help='Output in the given format')
@pass_runtime
@click_coroutine
async def find_k_builds_cli(runtime: Runtime, kind, engine, outcome, output):
    runtime.initialize(mode='images' if kind == 'image' else 'rpms')
    if runtime.konflux_db is None:
        raise RuntimeError('Must run Elliott with Konflux DB initialized')
    runtime.konflux_db.bind(KonfluxBuildRecord)

    engine_fmt = f"{engine} " if engine else ""
    outcome = KonfluxBuildOutcome(outcome) if outcome else KonfluxBuildOutcome.SUCCESS
    LOGGER.info(f'searching for {engine_fmt}builds of kind={kind} in group={runtime.group} with outcome={outcome.value}')
    metas = runtime.image_metas() if kind == 'image' else runtime.rpm_metas()
    names = [meta.distgit_key for meta in metas]

    args = {
        'names': names,
        'group': runtime.group,
        'assembly': runtime.assembly,
        'outcome': outcome
    }
    if engine:
        args.update({'engine': Engine(engine)})
    builds = await runtime.konflux_db.get_latest_builds(**args)

    missing_builds = [name for name in names if name not in [b.name for b in builds]]
    if missing_builds:
        LOGGER.warning('Builds have not been found for these components: %s', ', '.join(missing_builds))

    LOGGER.info('Found %s builds', len(builds))

    if output == 'json' and builds:
        click.echo(str(builds[0]))
    else:
        for build in builds:
            click.echo(build.nvr)
