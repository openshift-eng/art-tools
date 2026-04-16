import asyncio

import click
from artcommonlib import redis

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime, lock_manager: LockManager, serial: bool = False):
    # Skip if locked on scan
    scan_lock_name = Lock.SCAN_KONFLUX.value.format(version=version)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info(f'[{version}] Locked on {scan_lock_name}, skipping')
        return

    # Skip if locked on build
    build_lock_name = Lock.BUILD_KONFLUX.value.format(version=version, assembly='stream')
    if await lock_manager.is_locked(build_lock_name):
        runtime.logger.info(f'[{version}] Locked on {build_lock_name}, skipping')
        return

    # Skip if frozen
    if not await util.is_build_permitted(
        version, doozer_working=str(runtime.working_dir / "doozer_working-" / version)
    ):
        runtime.logger.info('[%s] Not permitted, skipping', version)
        return

    # Schedule scan
    if serial:
        runtime.logger.info('[%s] Scheduling ocp4-scan-konflux (serial: waiting for completion)', version)
        try:
            result = jenkins.start_ocp4_scan_konflux(
                version=version, block_until_building=True, block_until_complete=True
            )
            runtime.logger.info('[%s] Scan completed with result: %s', version, result)
        except Exception:
            runtime.logger.warning('[%s] Scan failed, continuing with remaining versions', version, exc_info=True)
    else:
        runtime.logger.info('[%s] Scheduling ocp4-scan-konflux', version)
        jenkins.start_ocp4_scan_konflux(version=version, block_until_building=False)


@cli.command('schedule-ocp4-scan-konflux')
@click.option('--version', '-v', required=True, help='OCP version to scan', multiple=True)
@click.option('--serial', is_flag=True, default=False, help='Run scans sequentially, waiting for each to complete')
@pass_runtime
@click_coroutine
async def ocp4_scan_konflux(runtime: Runtime, version: tuple, serial: bool):
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        if serial:
            runtime.logger.info('Running scans serially for versions: %s', ', '.join(version))
            for v in version:
                await run_for(v, runtime, lock_manager, serial=True)
        else:
            await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in version])
    finally:
        await lock_manager.destroy()
