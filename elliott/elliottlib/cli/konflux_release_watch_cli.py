import os
import sys
from datetime import timedelta

import click

from elliottlib.cli.konflux_release_cli import konflux_release_cli
from elliottlib.cli.common import click_coroutine
from elliottlib.runtime import Runtime
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_client import KonfluxClient
from artcommonlib import logutil
from artcommonlib import util as art_util

LOGGER = logutil.get_logger(__name__)


class WatchReleaseCli:
    def __init__(self, runtime: Runtime, release: str, konflux_config: dict, timeout: int, dry_run: bool):
        self.runtime = runtime
        self.release = release
        self.konflux_config = konflux_config
        self.timeout = timeout
        self.dry_run = dry_run
        self.konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_config['namespace'],
            config_file=self.konflux_config['kubeconfig'],
            context=self.konflux_config['context'],
            dry_run=self.dry_run,
        )
        self.konflux_client.verify_connection()

    async def run(self):
        self.runtime.initialize(no_group=True)
        release_obj = await self.konflux_client.wait_for_release(
            self.release, overall_timeout_timedelta=timedelta(hours=self.timeout)
        )

        # Assume that these will be available
        released_condition = art_util.KubeCondition.find_condition(release_obj, 'Released')
        if not released_condition:
            raise ValueError("Expected to find `Released` status in release_obj but couldn't")

        reason = released_condition.reason
        status = released_condition.status
        success = reason == "Succeeded" and status == "True"

        if success:
            return True

        message = released_condition.message
        if message == "Release processing failed on managed pipelineRun":
            managed_plr = release_obj['status'].get('managedProcessing', {}).get('pipelineRun', '')
            message += f" {managed_plr}"
        LOGGER.error(message)
        return False


@konflux_release_cli.command("watch", short_help="Watch and report on status of a given Konflux Release")
@click.argument("release", metavar='RELEASE_NAME', nargs=1)
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option(
    '--timeout',
    metavar='TIMEOUT_HOURS',
    type=click.INT,
    default=5,
    help='Time to wait, in hours. Set 0 to report and exit.',
)
@click.option('--dry-run', is_flag=True, help='Init and exit')
@click.pass_obj
@click_coroutine
async def watch_release_cli(
    runtime: Runtime,
    release: str,
    konflux_kubeconfig: str,
    konflux_context: str,
    konflux_namespace,
    timeout: int,
    dry_run: bool,
):
    """
    Watch the given Konflux Release and report on its status
    \b

    $ elliott release watch ose-4-18-stage-202503131819 --konflux-namespace ocp-art-tenant
    """
    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not konflux_kubeconfig:
        raise ValueError("Must pass kubeconfig using --konflux-kubeconfig or KONFLUX_SA_KUBECONFIG env var")

    konflux_config = {
        'kubeconfig': konflux_kubeconfig,
        'namespace': konflux_namespace,
        'context': konflux_context,
    }

    pipeline = WatchReleaseCli(
        runtime, release=release, konflux_config=konflux_config, timeout=timeout, dry_run=dry_run
    )
    release_status = await pipeline.run()
    if release_status is True:
        click.echo("Release successful!")
        sys.exit(0)
    else:
        click.echo("Release failed!")
        sys.exit(1)
