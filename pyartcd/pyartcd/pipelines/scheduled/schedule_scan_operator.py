import asyncio

import click
from artcommonlib import redis

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime, lock_manager: LockManager):
    """Run scan-operator for one version if not locked and permitted."""

    # Skip if locked on scan
    scan_lock_name = Lock.SCAN_OPERATOR.value.format(version=version)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info(f'[{version}] Locked on {scan_lock_name}, skipping')
        return

    # Skip if locked on build
    build_lock_name = Lock.BUILD_KONFLUX.value.format(version=version)
    if await lock_manager.is_locked(build_lock_name):
        runtime.logger.info(f'[{version}] Locked on {build_lock_name}, skipping')
        return

    # Skip if frozen
    if not await util.is_build_permitted(version, doozer_working=str(runtime.working_dir / "doozer_working" / version)):
        runtime.logger.info(f'[{version}] Not permitted, skipping')
        return

    # Schedule scan
    runtime.logger.info(f'[{version}] Scheduling scan-operator')
    jenkins.start_scan_operator(version=version, block_until_building=False)


@cli.command('schedule-scan-operator')
@click.option('--version', '-v', required=True, help='OCP version to scan', multiple=True)
@pass_runtime
@click_coroutine
async def schedule_scan_operator(runtime: Runtime, version: tuple):
    """Schedule scan-operator jobs for specified versions."""
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in version])
    finally:
        await lock_manager.destroy()
