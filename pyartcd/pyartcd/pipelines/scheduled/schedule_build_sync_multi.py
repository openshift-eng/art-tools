import click
from artcommonlib import redis

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime, lock_manager: LockManager) -> bool:
    """
    Check if build-sync-multi should run for a version and trigger it.

    Returns True if the job was triggered and completed, False if skipped.
    """
    # Skip if locked on build-sync-multi
    build_sync_multi_lock_name = Lock.BUILD_SYNC_MULTI.value.format(version=version)
    if await lock_manager.is_locked(build_sync_multi_lock_name):
        runtime.logger.info(f"[{version}] Locked on {build_sync_multi_lock_name}, skipping")
        return False

    # Skip if frozen
    if not await util.is_build_permitted(
        version, doozer_working=str(runtime.working_dir / "doozer_working-" / version)
    ):
        runtime.logger.info("[%s] Not permitted, skipping", version)
        return False

    # Trigger build-sync-multi and wait for it to complete before returning
    runtime.logger.info("[%s] Starting build-sync-multi and waiting for completion", version)
    result = jenkins.start_build_sync_multi(
        version=version,
        block_until_building=True,
        block_until_complete=True,
    )
    runtime.logger.info("[%s] build-sync-multi completed with result: %s", version, result)
    return True


@cli.command("schedule-build-sync-multi")
@click.option("--version", "-v", required=True, help="OCP version to sync", multiple=True)
@pass_runtime
@click_coroutine
async def schedule_build_sync_multi(runtime: Runtime, version: tuple):
    """
    Schedule build-sync-multi for each version sequentially.

    Each version is processed one at a time, waiting for the triggered job
    to complete before moving on to the next version.
    """
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        for v in version:
            await run_for(v, runtime, lock_manager)
    finally:
        await lock_manager.destroy()
