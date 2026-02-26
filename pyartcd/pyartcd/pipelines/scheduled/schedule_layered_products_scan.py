import asyncio

import click
from artcommonlib import redis

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(group: str, runtime: Runtime, lock_manager: LockManager):
    # Skip if locked on layered products scan
    scan_lock_name = Lock.OADP_SCAN.value.format(group=group)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info(f'[{group}] Locked on {scan_lock_name}, skipping')
        return

    # Skip if locked on layered products build
    build_lock_name = Lock.OADP_BUILD.value.format(group=group)
    if await lock_manager.is_locked(build_lock_name):
        runtime.logger.info(f'[{group}] Locked on {build_lock_name}, skipping')
        return

    # Schedule layered products scan
    runtime.logger.info('[%s] Scheduling layered-products-scan-konflux', group)

    jenkins.start_layered_products_scan_konflux(group=group, block_until_building=False)


@cli.command('schedule-layered-products-scan')
@click.option('--group', '-g', required=True, help='Layered products group to scan', multiple=True)
@pass_runtime
@click_coroutine
async def layered_products_scan(runtime: Runtime, group: tuple):
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        await asyncio.gather(*[run_for(g, runtime, lock_manager) for g in group])
    finally:
        await lock_manager.destroy()
