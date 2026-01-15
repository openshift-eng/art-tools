import json
from typing import Dict

import click
from artcommonlib import logutil
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord

from elliottlib.cli.common import cli, click_coroutine, pass_runtime
from elliottlib.runtime import Runtime


class GetNetworkModeCli:
    def __init__(self, runtime, as_json: bool, only_hermetic: bool, only_open: bool):
        self.runtime = runtime
        self.as_json = as_json
        self.only_hermetic = only_hermetic
        self.only_open = only_open

        self.konflux_db = runtime.konflux_db
        self.logger = logutil.get_logger(__name__)

    async def get_results(self):
        # Use provided query_group to override the group for querying builds, or use runtime group
        # This allows querying builds stored with different group names (e.g., okd-4.21)
        assembly_to_use = self.runtime.assembly or 'stream'

        self.konflux_db.bind(KonfluxBuildRecord)
        image_metas = self.runtime.image_metas()

        if not image_metas:
            self.logger.warning('No images found matching the specified criteria')
            if self.as_json:
                click.echo('[]')
            return

        self.logger.info(
            f'Querying network mode for {len(image_metas)} images in group={self.runtime.group}, '
            f'assembly={assembly_to_use}'
        )

        # Get latest builds for all images
        builds = await self.konflux_db.get_latest_builds(
            names=[meta.distgit_key for meta in image_metas],
            group=self.runtime.group,
            assembly=assembly_to_use,
            outcome=KonfluxBuildOutcome.SUCCESS,
            artifact_type=ArtifactType.IMAGE,
            exclude_large_columns=True,  # Only need name, nvr, hermetic - not installed_rpms/packages
        )

        # Filter out non-found builds
        builds = [build for build in builds if build]

        # Filter by network mode if provided
        if self.only_hermetic:
            builds = [build for build in builds if build.hermetic]
        elif self.only_open:
            builds = [build for build in builds if not build.hermetic]

        # Create a map of name to build record
        results = [
            {
                'name': build.name,
                'nvr': build.nvr,
                'network_mode': 'hermetic' if build.hermetic else 'open',
                'pullspec': build.image_pullspec,
            }
            for build in builds
        ]
        return results

    async def run(self):
        results = await self.get_results()

        # Output results
        if self.as_json:
            click.echo(json.dumps(results, indent=2))

        else:
            # Table output (default)
            click.echo(f"{'Image':<30} {'Network Mode':<15} {'NVR':<100}")
            click.echo('-' * 155)
            for result in results:
                network_mode_str = result['network_mode'] or 'N/A'
                nvr_str = result['nvr'] or 'N/A'
                click.echo(f"{result['name']:<30} {network_mode_str:<15} {nvr_str:<100} ")


@cli.command('get-network-mode', short_help='Get network mode of latest builds for image components')
@click.option(
    '--json',
    'as_json',
    is_flag=True,
    default=False,
    help='Output results in JSON format',
)
@click.option(
    '--disable-cache',
    is_flag=True,
    default=False,
    help='Disable Konflux DB cache',
)
@click.option(
    '--only-hermetic',
    is_flag=True,
    default=False,
    help='Only returns hermetic network mode builds',
)
@click.option(
    '--only-open',
    is_flag=True,
    default=False,
    help='Only returns open network mode builds',
)
@pass_runtime
@click_coroutine
async def get_network_mode_cli(
    runtime: Runtime,
    as_json: bool,
    disable_cache: bool,
    only_hermetic: bool,
    only_open: bool,
):
    """
    Get the network mode (hermetic, internal-only, or open) of the latest build
    for each image component in the group.

    The command respects the --images option to filter specific images,
    and uses --group and --assembly options to determine which builds to query.

    Examples:
        # Get network mode for all images in the group
        elliott --group openshift-4.21 get-network-mode

        # Get network mode for specific images
        elliott --group openshift-4.21 --images ironic,ironic-inspector get-network-mode

        # Output as JSON
        elliott --group openshift-4.21 get-network-mode --json

        # Only fetch components that were built in hermetic mode
        elliott --group openshift-4.21 get-network-mode --only-hermetic

        # Only fetch components that were built in open mode
        elliott --group openshift-4.21 get-network-mode --only-open

        # Disable Konflux DB cache
        elliott --group openshift-4.21 get-network-mode --disable-cache
    """

    if only_open and only_hermetic:
        raise click.BadParameter('Use only one of --only-open and --only-hermetic')

    runtime.initialize(mode='images', disable_konflux_db_cache=disable_cache)

    if runtime.konflux_db is None:
        raise RuntimeError('Must run Elliott with Konflux DB initialized')

    await GetNetworkModeCli(runtime, as_json, only_hermetic, only_open).run()
