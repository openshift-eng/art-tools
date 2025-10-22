import asyncio

import click
from artcommonlib import redis

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(group: str, runtime: Runtime, lock_manager: LockManager):
    # Skip if locked on OADP scan
    scan_lock_name = Lock.OADP_SCAN.value.format(group=group)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info(f'[{group}] Locked on {scan_lock_name}, skipping')
        return

    # Skip if locked on OADP build
    build_lock_name = Lock.OADP_BUILD.value.format(group=group)
    if await lock_manager.is_locked(build_lock_name):
        runtime.logger.info(f'[{group}] Locked on {build_lock_name}, skipping')
        return

    # Schedule OADP scan
    runtime.logger.info('[%s] Scheduling oadp-scan-konflux', group)

    # Until ashwin's fork is not being used for logging builds
    # https://github.com/openshift-eng/ocp-build-data/blob/logging-6.3/images/cluster-logging-operator.yml#L7
    assembly = 'test' if group.startswith("logging-") else 'stream'

    jenkins.start_oadp_scan_konflux(group=group, assembly=assembly, block_until_building=False)


@cli.command('schedule-oadp-scan')
@click.option('--group', '-g', required=True, help='OADP group to scan', multiple=True)
@pass_runtime
@click_coroutine
async def oadp_scan(runtime: Runtime, group: tuple):
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        await asyncio.gather(*[run_for(g, runtime, lock_manager) for g in group])
    finally:
        await lock_manager.destroy()
