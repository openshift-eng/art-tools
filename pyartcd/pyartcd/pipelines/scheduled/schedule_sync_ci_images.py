import asyncio

import click
from artcommonlib import redis
from artcommonlib.constants import ACTIVE_OCP_VERSIONS

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime, lock_manager: LockManager, serial: bool = False):
    """
    Schedule sync-ci-images for a single OCP version.

    Args:
        version: OCP version (e.g., "4.17")
        runtime: PyARTCD runtime instance
        lock_manager: Redis lock manager
        serial: If True, wait for job to complete before returning
    """
    # Atomically acquire lock to prevent duplicate scheduling
    lock_name = Lock.SYNC_CI_IMAGES.value.format(version=version)
    lock = None
    try:
        # Acquire lock with 60s timeout to handle parallel scheduling of multiple versions
        # Each version has its own lock, but Redis can be slow under parallel load
        lock = await lock_manager.lock(lock_name, lock_timeout=60)
    except Exception as e:
        runtime.logger.info(
            f'[{version}] Failed to acquire lock {lock_name}, skipping (already running or scheduled): {e}'
        )
        return

    try:
        # Schedule sync-ci-images and wait until it's building to ensure worker acquires lock
        runtime.logger.info('[%s] Scheduling sync-ci-images%s', version, ' (serial mode)' if serial else '')
        jenkins.start_sync_ci_images(version=version, block_until_building=True, block_until_complete=serial)
        runtime.logger.info('[%s] Sync job started', version)
    except Exception:
        runtime.logger.warning('[%s] Failed to schedule sync job', version, exc_info=True)
        raise
    finally:
        # Release scheduler lock once worker has started (worker holds its own lock)
        if lock:
            try:
                await lock_manager.unlock(lock)
            except Exception as e:
                # Suppress errors during unlock - connection pool might be closing
                runtime.logger.debug('[%s] Error releasing lock (ignored): %s', version, e)


@cli.command('schedule-sync-ci-images')
@click.option(
    '--version',
    '-v',
    required=False,
    multiple=True,
    help='OCP version(s) to sync (e.g., "4.17"). If not specified, syncs all active OCP versions.',
)
@click.option('--serial', is_flag=True, default=False, help='Run syncs sequentially, waiting for each to complete')
@pass_runtime
@click_coroutine
async def schedule_sync_ci_images(runtime: Runtime, version: tuple, serial: bool):
    """
    Schedule sync-ci-images jobs for one or more OCP versions.

    This scheduler spawns individual sync-ci-images jobs per version,
    allowing per-version isolation, better traceability, and parallel execution.

    Examples:
        # Sync all active OCP versions in parallel
        artcd schedule-sync-ci-images

        # Sync specific versions in parallel
        artcd schedule-sync-ci-images -v 4.17 -v 4.18

        # Sync versions serially (wait for each to complete)
        artcd schedule-sync-ci-images -v 4.17 -v 4.18 --serial
    """
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])

    # Determine which versions to process
    versions_to_process = list(version) if version else ACTIVE_OCP_VERSIONS

    if not versions_to_process:
        runtime.logger.warning('No versions to process')
        return

    runtime.logger.info('Processing versions: %s', ', '.join(versions_to_process))

    try:
        if serial:
            runtime.logger.info('Running syncs serially for versions: %s', ', '.join(versions_to_process))
            for v in versions_to_process:
                await run_for(v, runtime, lock_manager, serial=True)
        else:
            runtime.logger.info('Running syncs in parallel for versions: %s', ', '.join(versions_to_process))
            # Gather with return_exceptions=True to handle failures gracefully
            results = await asyncio.gather(
                *[run_for(v, runtime, lock_manager) for v in versions_to_process], return_exceptions=True
            )
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
                raise RuntimeError(f"Failed to schedule sync-ci-images for: {', '.join(failed_versions)}")
    finally:
        await lock_manager.destroy()
