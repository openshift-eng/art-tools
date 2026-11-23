import asyncio
import hashlib
import os

import click
from artcommonlib import exectools, redis
from artcommonlib.telemetry import start_as_current_span_async
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from pyartcd import jenkins, tekton, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock, LockManager
from pyartcd.runtime import Runtime

TRACER = trace.get_tracer(__name__)


@start_as_current_span_async(TRACER, "schedule-ocp4-scan-konflux.run-for")
async def run_for(
    version: str,
    runtime: Runtime,
    lock_manager: LockManager,
    serial: bool = False,
    *,
    tekton_namespace: str | None = None,
):
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

    if tekton_namespace is not None:
        if runtime.dry_run:
            runtime.logger.info('[%s] Would start ocp4-scan-konflux in %s', version, tekton_namespace)
            return

        # Use the same name if Kubernetes runs the scheduler Job's pod again.
        job_name = os.getenv('K8S_JOB_NAME')
        job_namespace = os.getenv('POD_NAMESPACE')
        run_name = f'{job_name}-{version.replace(".", "-")}' if job_name else None
        if run_name and len(run_name) > 63:
            # Tekton uses the PipelineRun name as a label value on child resources.
            digest = hashlib.sha256(run_name.encode()).hexdigest()[:12]
            run_name = f'{run_name[:50].rstrip("-.")}-{digest}'
        await exectools.to_thread(
            tekton.start_pipeline_run,
            'ocp4-scan-konflux',
            {'version': version},
            namespace=tekton_namespace,
            run_name=run_name,
            scheduler_job=f'{job_namespace}/{job_name}' if job_namespace and job_name else None,
        )
        span.set_attribute("triggered", True)
        return

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
            span.set_status(StatusCode.ERROR, str(e)[:500])
    else:
        runtime.logger.info('[%s] Scheduling ocp4-scan-konflux', version)
        jenkins.start_ocp4_scan_konflux(version=version, block_until_building=False)


@cli.command('schedule-ocp4-scan-konflux')
@click.option('--version', '-v', required=True, help='OCP version to scan', multiple=True)
@click.option('--serial', is_flag=True, default=False, help='Run scans sequentially, waiting for each to complete')
@click.option('--tekton-namespace', help='Submit scans as Tekton PipelineRuns in this namespace')
@pass_runtime
@click_coroutine
async def ocp4_scan_konflux(runtime: Runtime, version: tuple, serial: bool, tekton_namespace: str | None = None):
    if tekton_namespace is not None:
        if not tekton_namespace.strip():
            raise click.UsageError('--tekton-namespace must be a non-empty namespace name')
        if serial:
            raise click.UsageError('--serial is only supported for Jenkins scans')
        version = tuple(dict.fromkeys(version))
    else:
        jenkins.init_jenkins()

    with TRACER.start_as_current_span("schedule-ocp4-scan-konflux") as span:
        span.set_attribute("versions", list(version))
        span.set_attribute("serial", serial)
        span.set_attribute("version_count", len(version))
        lock_manager = None
        try:
            lock_manager = LockManager([redis.redis_url()])
            if tekton_namespace is not None:
                results = await asyncio.gather(
                    *(run_for(v, runtime, lock_manager, tekton_namespace=tekton_namespace) for v in version),
                    return_exceptions=True,
                )
                failures = [
                    (scan_version, result)
                    for scan_version, result in zip(version, results, strict=True)
                    if isinstance(result, BaseException)
                ]
                for scan_version, error in failures:
                    runtime.logger.error('[%s] Failed to schedule ocp4-scan-konflux', scan_version, exc_info=error)
                if failures:
                    raise failures[0][1]
            elif serial:
                runtime.logger.info('Running scans serially for versions: %s', ', '.join(version))
                for v in version:
                    await run_for(v, runtime, lock_manager, serial=True)
            else:
                await asyncio.gather(*[run_for(v, runtime, lock_manager) for v in version])
        except Exception:
            if tekton_namespace is not None and not runtime.dry_run:
                try:
                    slack_client = runtime.new_slack_client()
                    slack_client.bind_channel('#art-release')
                    await slack_client.say('Error scheduling OCP Konflux scans in artc. Check the scheduler Job logs.')
                except Exception:
                    runtime.logger.exception('Failed to report the artc scheduler error to Slack')
            raise
        finally:
            if lock_manager is not None:
                await lock_manager.destroy()
