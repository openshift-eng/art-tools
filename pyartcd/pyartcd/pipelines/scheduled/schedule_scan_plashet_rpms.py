import asyncio

import click
from artcommonlib import redis

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(group: str, assembly: str, runtime: Runtime, lock_manager: LockManager):
    # Skip if locked on scan
    scan_lock_name = Lock.SCAN_PLASHET_RPMS.value.format(assembly=assembly, group=group)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info('[%s/%s] Locked on %s, skipping', group, assembly, scan_lock_name)
        return

    # Check if build is permitted (not frozen)
    if not await util.is_build_permitted(
        group=group, doozer_working=str(runtime.working_dir / f"doozer_working-{group}")
    ):
        runtime.logger.info('[%s/%s] Not permitted (frozen), skipping', group, assembly)
        return

    # Schedule scan
    runtime.logger.info('[%s/%s] Scheduling plashet-rpms scan', group, assembly)
    jenkins.start_scan_plashet_rpms(group=group, assembly=assembly, block_until_building=False)


@cli.command('schedule-scan-plashet-rpms')
@click.option('--group', '-g', required=True, help='OCP group to scan (e.g., openshift-4.17)', multiple=True)
@click.option('--assembly', '-a', multiple=True, default=['stream'], help='Assembly to scan (default: stream)')
@pass_runtime
@click_coroutine
async def schedule_scan_plashet_rpms(runtime: Runtime, group: tuple, assembly: tuple):
    """
    Schedule plashet-rpms scanning jobs for multiple OCP groups and assemblies.

    This command is typically called by a scheduled Jenkins job to trigger
    plashet RPM scanning across multiple OCP groups and assemblies.

    Example:
        artcd schedule-scan-plashet-rpms --group openshift-4.16 --group openshift-4.17
        artcd schedule-scan-plashet-rpms -g openshift-4.17 -g openshift-4.18 --assembly 4.18.1
        artcd schedule-scan-plashet-rpms -g openshift-4.17 -a stream -a 4.17.1
    """
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        # Create tasks for all group/assembly combinations
        tasks = [run_for(g, a, runtime, lock_manager) for g in group for a in assembly]
        await asyncio.gather(*tasks)
    finally:
        await lock_manager.destroy()
