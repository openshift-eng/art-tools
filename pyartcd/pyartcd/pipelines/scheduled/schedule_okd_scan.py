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
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime


async def _is_version_okd_enabled(runtime: Runtime, version: str) -> bool:
    doozer_cmd = util.okd_doozer_base_command(
        version=version,
        assembly='stream',
        data_path=OCP_BUILD_DATA_URL,
        data_gitref='',
        working_dir=runtime.working_dir / f'doozer_working-{version}',
    )
    return await util.is_okd_version_enabled(doozer_cmd)


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

    if not await _is_version_okd_enabled(runtime, version):
        runtime.logger.info(
            '[%s] OKD not enabled in build-data (okd.enabled); skipping okd-scan schedule',
            version,
        )
        return

    # Schedule scan
    runtime.logger.info('[%s] Scheduling okd-scan', version)
    jenkins.start_okd_scan_konflux(version=version, block_until_building=False)


async def resolve_schedule_versions(runtime: Runtime, version: tuple) -> list[str]:
    if version:
        enabled = []
        for candidate in version:
            if await _is_version_okd_enabled(runtime, candidate):
                enabled.append(candidate)
            else:
                runtime.logger.info(
                    'Version %s is not enabled for OKD (set okd.enabled: true in group.yml on openshift-%s). Skipping.',
                    candidate,
                    candidate,
                )
        return enabled
    versions = await util.get_okd_enabled_versions(
        working_dir=runtime.working_dir / 'doozer_working',
    )
    if not versions:
        runtime.logger.info('No OKD-enabled versions found in build-data; nothing to schedule')
    else:
        runtime.logger.info('OKD-enabled versions from build-data: %s', ', '.join(versions))
    return versions


@cli.command('schedule-okd-scan')
@click.option(
    '--version',
    '-v',
    required=False,
    help='OCP/OKD version to scan (omit to discover enabled versions from build-data)',
    multiple=True,
)
@pass_runtime
@click_coroutine
async def okd_scan(runtime: Runtime, version: tuple):
    """
    Schedule OKD scans for specified versions.

    When --version is omitted, probes ACTIVE_OCP_VERSIONS and schedules only versions
    with okd.enabled in group.yml (merged via doozer --variant=okd).

    Example usage:
        artcd schedule-okd-scan
        artcd schedule-okd-scan --version 4.21 --version 4.22
    """
    versions = await resolve_schedule_versions(runtime, version)
    if not versions:
        return

    jenkins.init_jenkins()
    lock_manager = LockManager([redis.redis_url()])
    try:
        await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in versions])
    finally:
        await lock_manager.destroy()
