import asyncio

import click

from pyartcd import util, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime):
    # Skip if locked
    lock = Lock.SCAN
    lock_name = lock.value.format(version=version)
    lock_manager = LockManager.from_lock(lock)
    try:
        locked = await lock_manager.is_locked(lock_name)
    finally:
        await lock_manager.destroy()
    if locked:
        runtime.logger.info('[%s] Locked, skipping', version)
        return

    # Skip if frozen
    if not await util.is_build_permitted(version, doozer_working=str(runtime.working_dir / "doozer_working-" / version)):
        runtime.logger.info('[%s] Not permitted, skipping', version)
        return

    # Schedule scan
    runtime.logger.info('[%s] Scheduling ocp4-scan', version)
    jenkins.start_ocp4_scan(version=version, block_until_building=False)


@cli.command('schedule-ocp4-scan')
@click.option('--version', '-v', required=True, help='OCP version to scan', multiple=True)
@pass_runtime
@click_coroutine
async def ocp4_scan(runtime: Runtime, version: tuple):
    jenkins.init_jenkins()
    await asyncio.gather(*[run_for(v, runtime) for v in version])
