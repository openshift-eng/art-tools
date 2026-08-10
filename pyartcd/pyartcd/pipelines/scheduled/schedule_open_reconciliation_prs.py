import asyncio

import click
from artcommonlib.constants import ACTIVE_OCP_VERSIONS

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime):
    """
    Schedule open-reconciliation-prs for a single OCP version.

    Args:
        version: OCP version (e.g., "4.17")
        runtime: PyARTCD runtime instance
    """
    try:
        runtime.logger.info('[%s] Scheduling open-reconciliation-prs', version)
        jenkins.start_open_reconciliation_prs(version=version, block_until_complete=False)
        runtime.logger.info('[%s] Reconciliation job started', version)
    except Exception:
        runtime.logger.warning('[%s] Failed to schedule reconciliation job', version, exc_info=True)
        raise


@cli.command('schedule-open-reconciliation-prs')
@click.option(
    '--version',
    '-v',
    required=False,
    multiple=True,
    help='OCP version(s) to reconcile (e.g., "4.17"). If not specified, reconciles all active OCP versions.',
)
@pass_runtime
@click_coroutine
async def schedule_open_reconciliation_prs(runtime: Runtime, version: tuple):
    """
    Schedule open-reconciliation-prs jobs for one or more OCP versions.

    This scheduler fires and forgets — it triggers individual
    open-reconciliation-prs jobs per version without waiting for
    completion. Locking is handled by the triggered pipeline itself.

    Examples:
        # Reconcile all active OCP versions
        artcd schedule-open-reconciliation-prs

        # Reconcile specific versions
        artcd schedule-open-reconciliation-prs -v 4.17 -v 4.18
    """
    jenkins.init_jenkins()

    # Determine which versions to process
    versions_to_process = list(version) if version else ACTIVE_OCP_VERSIONS

    if not versions_to_process:
        runtime.logger.warning('No versions to process')
        return

    runtime.logger.info('Scheduling reconciliation for versions: %s', ', '.join(versions_to_process))

    results = await asyncio.gather(*[run_for(v, runtime) for v in versions_to_process], return_exceptions=True)

    # Log any exceptions that occurred
    failed_versions = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_versions.append(versions_to_process[i])
            runtime.logger.error(
                'Version %s failed: %s',
                versions_to_process[i],
                result,
                exc_info=result,
            )
    if failed_versions:
        raise RuntimeError(f"Failed to schedule open-reconciliation-prs for: {', '.join(failed_versions)}")
