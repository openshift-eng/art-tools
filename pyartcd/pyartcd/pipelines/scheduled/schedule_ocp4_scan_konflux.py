import asyncio

import click
from artcommonlib import redis
from artcommonlib.telemetry import start_as_current_span_async
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime

TRACER = trace.get_tracer(__name__)


@start_as_current_span_async(TRACER, "schedule-ocp4-scan-konflux.run-for")
async def run_for(version: str, runtime: Runtime, lock_manager: LockManager, serial: bool = False):
    span = trace.get_current_span()
    span.set_attribute("version", version)
    span.set_attribute("serial", serial)

    # Skip if locked on scan
    scan_lock_name = Lock.SCAN_KONFLUX.value.format(version=version)
    if await lock_manager.is_locked(scan_lock_name):
        runtime.logger.info(f'[{version}] Locked on {scan_lock_name}, skipping')
        span.set_attribute("skipped_reason", "scan_locked")
        return

    # Skip if locked on build
    build_lock_name = Lock.BUILD_KONFLUX.value.format(version=version, assembly='stream')
    if await lock_manager.is_locked(build_lock_name):
        runtime.logger.info(f'[{version}] Locked on {build_lock_name}, skipping')
        span.set_attribute("skipped_reason", "build_locked")
        return

    # Skip if frozen
    if not await util.is_build_permitted(
        version, doozer_working=str(runtime.working_dir / "doozer_working-" / version)
    ):
        runtime.logger.info('[%s] Not permitted, skipping', version)
        span.set_attribute("skipped_reason", "not_permitted")
        return

    # Schedule scan
    span.set_attribute("triggered", True)
    if serial:
        runtime.logger.info('[%s] Scheduling ocp4-scan-konflux (serial: waiting for completion)', version)
        try:
            result = jenkins.start_ocp4_scan_konflux(
                version=version, block_until_building=True, block_until_complete=True
            )
            runtime.logger.info('[%s] Scan completed with result: %s', version, result)
            span.set_attribute("scan_result", str(result))
        except Exception as e:
            runtime.logger.warning('[%s] Scan failed, continuing with remaining versions', version, exc_info=True)
            span.set_attribute("scan_failed", True)
            span.set_status(StatusCode.ERROR, str(e))
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
    with TRACER.start_as_current_span("schedule-ocp4-scan-konflux") as span:
        span.set_attribute("versions", list(version))
        span.set_attribute("serial", serial)
        span.set_attribute("version_count", len(version))
        try:
            if serial:
                runtime.logger.info('Running scans serially for versions: %s', ', '.join(version))
                for v in version:
                    await run_for(v, runtime, lock_manager, serial=True)
            else:
                await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in version])
        finally:
            await lock_manager.destroy()
