import os
import sys
from datetime import timedelta

import click
from artcommonlib import logutil
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.util import (
    KubeCondition,
    new_roundtrip_yaml_handler,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib.backend.konflux_client import KonfluxClient

from elliottlib.cli.common import click_coroutine
from elliottlib.cli.konflux_release_cli import konflux_release_cli
from elliottlib.runtime import Runtime

yaml = new_roundtrip_yaml_handler()

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

    async def run(self) -> tuple[bool, dict]:
        """
        Run the watch release pipeline.
        :return: A tuple of (success: bool, release_obj: dict)
        """

        # Initialize runtime if not already initialized (for direct class usage in tests)
        if not getattr(self.runtime, 'initialized', False):
            self.runtime.initialize(no_group=True, build_system='konflux')

        release_obj = await self.konflux_client.wait_for_release(
            self.release, overall_timeout_timedelta=timedelta(hours=self.timeout)
        )

        # Assume that these will be available
        released_condition = KubeCondition.find_condition(release_obj, 'Released')
        if not released_condition:
            raise ValueError("Expected to find `Released` status in release_obj but couldn't")

        reason = released_condition.reason
        status = released_condition.status
        success = reason == "Succeeded" and status == "True"

        if success:
            return True, release_obj

        message = released_condition.message
        if message == "Release processing failed on managed pipelineRun":
            managed_plr = release_obj['status'].get('managedProcessing', {}).get('pipelineRun', '')
            message += f" {managed_plr}"
        LOGGER.error(message)
        return False, release_obj


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
    help='The namespace to use for Konflux cluster connections. If not provided, will be auto-detected based on group (e.g., ocp-art-tenant for openshift- groups, art-oadp-tenant for oadp- groups).',
)
@click.option(
    '--timeout',
    metavar='TIMEOUT_HOURS',
    type=click.INT,
    default=5,
    help='Time to wait, in hours. Set 0 to report and exit.',
)
@click.option('--dry-run', is_flag=True, help='Init and exit')
@click.option('--dump', is_flag=True, help='Dump the release object to stdout in YAML format')
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
    dump: bool,
):
    """
    Watch the given Konflux Release and report on its status
    \b

    $ elliott release watch ose-4-18-stage-202503131819 --konflux-namespace ocp-art-tenant
    """
    # Initialize runtime to populate runtime.product before using resolver functions
    runtime.initialize(build_system='konflux')

    # Resolve kubeconfig and namespace using product-based utility functions
    resolved_kubeconfig = resolve_konflux_kubeconfig_by_product(runtime.product, konflux_kubeconfig)
    resolved_namespace = resolve_konflux_namespace_by_product(runtime.product, konflux_namespace)

    konflux_config = {
        'kubeconfig': resolved_kubeconfig,
        'namespace': resolved_namespace,
        'context': konflux_context,
    }

    pipeline = WatchReleaseCli(
        runtime, release=release, konflux_config=konflux_config, timeout=timeout, dry_run=dry_run
    )

    release_status, release_obj = await pipeline.run()
    message = f"Release {'successful' if release_status else 'failed'}!"
    rc = 0 if release_status else 1

    LOGGER.info(message)
    if dump:
        yaml.dump(release_obj.to_dict(), sys.stdout)
    sys.exit(rc)
