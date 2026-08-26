import asyncio

import click

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime


async def run_for(group: str, runtime: Runtime):
    """
    Schedule open-reconciliation-prs-layered for a single layered product group.

    Args:
        group: Layered product group (e.g., "logging-6.5")
        runtime: PyARTCD runtime instance
    """
    try:
        runtime.logger.info('[%s] Scheduling open-reconciliation-prs-layered', group)
        jenkins.start_open_reconciliation_prs_layered(group=group, block_until_complete=False)
        runtime.logger.info('[%s] Reconciliation job started', group)
    except Exception:
        runtime.logger.warning('[%s] Failed to schedule reconciliation job', group, exc_info=True)
        raise


@cli.command('schedule-open-reconciliation-prs-layered')
@click.option(
    '--group',
    '-g',
    required=True,
    multiple=True,
    help='Layered product group(s) to reconcile (e.g., "logging-6.5"). At least one group is required.',
)
@pass_runtime
@click_coroutine
async def schedule_open_reconciliation_prs_layered(runtime: Runtime, group: tuple):
    """
    Schedule open-reconciliation-prs-layered jobs for one or more layered product groups.

    This scheduler fires and forgets — it triggers individual
    open-reconciliation-prs-layered jobs per group without waiting for
    completion. Locking is handled by the triggered pipeline itself.

    Groups are passed from the Jenkins scheduled job, which iterates
    commonlib.nonOCPGroups to build the --group flags.

    Examples:
        # Reconcile specific groups
        artcd schedule-open-reconciliation-prs-layered -g logging-6.5 -g oadp-1.5
    """
    jenkins.init_jenkins()

    groups_to_process = list(group)

    if not groups_to_process:
        runtime.logger.warning('No groups to process')
        return

    runtime.logger.info('Scheduling reconciliation for groups: %s', ', '.join(groups_to_process))

    results = await asyncio.gather(*[run_for(g, runtime) for g in groups_to_process], return_exceptions=True)

    # Log any exceptions that occurred
    failed_groups = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_groups.append(groups_to_process[i])
            runtime.logger.error(
                'Group %s failed: %s',
                groups_to_process[i],
                result,
                exc_info=result,
            )
    if failed_groups:
        raise RuntimeError(f"Failed to schedule open-reconciliation-prs-layered for: {', '.join(failed_groups)}")
