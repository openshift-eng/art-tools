import asyncio

import click
from artcommonlib import redis

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime, lock_manager: LockManager):
    # Skip if locked on build-sync-multi
    build_sync_multi_lock_name = Lock.BUILD_SYNC_MULTI.value.format(version=version)
    if await lock_manager.is_locked(build_sync_multi_lock_name):
        runtime.logger.info(f'[{version}] Locked on {build_sync_multi_lock_name}, skipping')
        return

    # Skip if locked on build-sync-konflux (regular Konflux sync in progress)
    build_sync_konflux_lock_name = Lock.BUILD_SYNC_KONFLUX.value.format(version=version)
    if await lock_manager.is_locked(build_sync_konflux_lock_name):
        runtime.logger.info(f'[{version}] Locked on {build_sync_konflux_lock_name}, skipping')
        return

    # Skip if frozen
    if not await util.is_build_permitted(
        version, doozer_working=str(runtime.working_dir / "doozer_working-" / version)
    ):
        runtime.logger.info('[%s] Not permitted, skipping', version)
        return

    # Schedule build-sync-multi (uses default multi_model from Jenkins job)
    runtime.logger.info('[%s] Scheduling build-sync-multi', version)
    jenkins.start_build_sync_multi(version=version, block_until_building=False)


@cli.command('schedule-build-sync-multi')
@click.option('--version', '-v', required=True, help='OCP version to sync', multiple=True)
@pass_runtime
@click_coroutine
async def schedule_build_sync_multi(runtime: Runtime, version: tuple):
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in version])
    finally:
        await lock_manager.destroy()
