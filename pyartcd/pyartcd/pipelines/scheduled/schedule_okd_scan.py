#!/usr/bin/env python3

"""
Scheduled OKD scan pipeline.

This scheduled job triggers OKD scans for configured versions.
It checks locks and build permissions before scheduling.
"""

import asyncio

import click
from artcommonlib import redis

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def run_for(version: str, runtime: Runtime, lock_manager: LockManager):
    """
    Schedule OKD scan for a specific version if conditions are met.

    Arg(s):
        version (str): OCP/OKD version to scan
        runtime (Runtime): Pipeline runtime context
        lock_manager (LockManager): Lock manager instance
    """
    # Skip if locked on scan
    scan_lock_name = Lock.SCAN_OKD.value.format(version=version)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info(f'[{version}] Locked on {scan_lock_name}, skipping')
        return

    # Skip if locked on build
    build_lock_name = Lock.BUILD_OKD.value.format(version=version)
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
    runtime.logger.info('[%s] Scheduling okd-scan', version)
    jenkins.start_okd_scan_konflux(version=version, block_until_building=False)


@cli.command('schedule-okd-scan')
@click.option('--version', '-v', required=True, help='OCP/OKD version to scan', multiple=True)
@pass_runtime
@click_coroutine
async def okd_scan(runtime: Runtime, version: tuple):
    """
    Schedule OKD scans for specified versions.

    Example usage:
        artcd schedule-okd-scan --version 4.21 --version 4.22
    """
    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in version])
    finally:
        await lock_manager.destroy()
