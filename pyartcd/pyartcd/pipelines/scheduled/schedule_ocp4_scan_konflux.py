import asyncio

import click

from artcommonlib import redis
from pyartcd import util, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime, lock_manager: LockManager):
    # Skip if locked on scan
    scan_lock_name = Lock.SCAN_KONFLUX.value.format(version=version)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info(f'[{version}] Locked on {scan_lock_name}, skipping')
        return

    # Skip if locked on build
    build_lock_name = Lock.BUILD_KONFLUX.value.format(version=version)
    if await lock_manager.is_locked(build_lock_name):
        runtime.logger.info(f'[{version}] Locked on {build_lock_name}, skipping')
        return

    # Skip if frozen
    if not await util.is_build_permitted(version, doozer_working=str(runtime.working_dir / "doozer_working-" / version)):
        runtime.logger.info('[%s] Not permitted, skipping', version)
        return

    # Schedule scan
    runtime.logger.info('[%s] Scheduling ocp4-scan-konflux', version)
    jenkins.start_ocp4_scan_konflux(version=version, block_until_building=False)


@cli.command('schedule-ocp4-scan-konflux')
@click.option('--version', '-v', required=True, help='OCP version to scan', multiple=True)
@pass_runtime
@click_coroutine
async def ocp4_scan_konflux(runtime: Runtime, version: tuple):
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in version])
    finally:
        await lock_manager.destroy()
