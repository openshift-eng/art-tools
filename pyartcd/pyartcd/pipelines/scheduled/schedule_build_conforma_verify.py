import asyncio
from datetime import datetime, timedelta, timezone

import click
from artcommonlib.release_util import SoftwareLifecyclePhase
from doozerlib.constants import (
    KONFLUX_RELEASE_EC_POLICY_CONFIGURATION,
    KONFLUX_RELEASE_FBC_EC_POLICY_CONFIGURATION,
    KONFLUX_RELEASE_PREGA_EC_POLICY_CONFIGURATION,
)

from pyartcd import jenkins, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

EFFECTIVE_TIME_OFFSET_DAYS = 21


async def run_for(version: str, runtime: Runtime, serial: bool = False):
    # Skip if frozen
    if not await util.is_build_permitted(
        version, doozer_working=str(runtime.working_dir / "doozer_working-" / version)
    ):
        runtime.logger.info('[%s] Not permitted, skipping', version)
        return

    # Load group config to determine GA vs pre-GA
    group = f'openshift-{version}'
    group_config = await util.load_group_config(group=group, assembly='stream')
    phase_name = group_config.get('software_lifecycle', {}).get('phase', '')

    try:
        phase = SoftwareLifecyclePhase.from_name(phase_name) if phase_name else None
    except ValueError:
        phase = None

    if phase == SoftwareLifecyclePhase.PRE_RELEASE:
        ec_policy = KONFLUX_RELEASE_PREGA_EC_POLICY_CONFIGURATION
    else:
        ec_policy = KONFLUX_RELEASE_EC_POLICY_CONFIGURATION
    fbc_ec_policy = KONFLUX_RELEASE_FBC_EC_POLICY_CONFIGURATION

    effective_time = (datetime.now(timezone.utc) + timedelta(days=EFFECTIVE_TIME_OFFSET_DAYS)).strftime(
        '%Y-%m-%dT%H:%M:%SZ'
    )

    runtime.logger.info(
        '[%s] Scheduling build-conforma-verify (phase=%s, ec_policy=%s, fbc_ec_policy=%s, effective_time=%s)',
        version,
        phase_name,
        ec_policy,
        fbc_ec_policy,
        effective_time,
    )

    if serial:
        result = jenkins.start_build_conforma_verify(
            build_version=version,
            assembly='stream',
            ec_policy=ec_policy,
            fbc_ec_policy=fbc_ec_policy,
            effective_time=effective_time,
            include_corresponding_bundles=True,
            include_corresponding_fbcs=True,
            report_to_slack=True,
            block_until_building=True,
            block_until_complete=True,
        )
        runtime.logger.info('[%s] Conforma verify completed with result: %s', version, result)
    else:
        jenkins.start_build_conforma_verify(
            build_version=version,
            assembly='stream',
            ec_policy=ec_policy,
            fbc_ec_policy=fbc_ec_policy,
            effective_time=effective_time,
            include_corresponding_bundles=True,
            include_corresponding_fbcs=True,
            report_to_slack=True,
            block_until_building=False,
        )


@cli.command('schedule-build-conforma-verify')
@click.option('--version', '-v', required=True, help='OCP version to verify', multiple=True)
@click.option('--serial', is_flag=True, default=False, help='Run verifications sequentially, waiting for each')
@pass_runtime
@click_coroutine
async def schedule_build_conforma_verify(runtime: Runtime, version: tuple, serial: bool):
    jenkins.init_jenkins()
    failed_versions: list[str] = []

    if serial:
        runtime.logger.info('Running conforma verify serially for versions: %s', ', '.join(version))
        for v in version:
            try:
                await run_for(v, runtime, serial=True)
            except Exception:
                runtime.logger.warning('[%s] Failed to spawn conforma verify job', v, exc_info=True)
                failed_versions.append(v)
    else:
        results = await asyncio.gather(*[run_for(v, runtime) for v in version], return_exceptions=True)
        for v, result in zip(version, results):
            if isinstance(result, Exception):
                runtime.logger.warning('[%s] Failed to spawn conforma verify job: %s', v, result)
                failed_versions.append(v)

    if failed_versions:
        raise RuntimeError(f"Failed to spawn conforma verify for: {', '.join(failed_versions)}")
