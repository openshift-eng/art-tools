import asyncio
from datetime import datetime, timedelta, timezone

import click
from artcommonlib.constants import LAYERED_PRODUCT_CONFORMA_STAGE_POLICY_MAP
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


async def run_for(group: str, runtime: Runtime, serial: bool = False):
    # Skip if frozen
    if not await util.is_build_permitted(
        group=group, doozer_working=str(runtime.working_dir / "doozer_working-" / group)
    ):
        runtime.logger.info('[%s] Not permitted, skipping', group)
        return

    group_config = await util.load_group_config(group=group, assembly='stream')

    if group.startswith('openshift-'):
        # OCP group: select EC policy based on software lifecycle phase
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
    else:
        product = group_config.get('product')
        if product not in LAYERED_PRODUCT_CONFORMA_STAGE_POLICY_MAP:
            raise ValueError(f"Unsupported layered-product group '{group}' product '{product}'")
        ec_policy, fbc_ec_policy = LAYERED_PRODUCT_CONFORMA_STAGE_POLICY_MAP[product]
        phase_name = 'n/a'

    effective_time = (datetime.now(timezone.utc) + timedelta(days=EFFECTIVE_TIME_OFFSET_DAYS)).strftime(
        '%Y-%m-%dT%H:%M:%SZ'
    )

    runtime.logger.info(
        '[%s] Scheduling build-conforma-verify (phase=%s, ec_policy=%s, fbc_ec_policy=%s, effective_time=%s)',
        group,
        phase_name,
        ec_policy,
        fbc_ec_policy,
        effective_time,
    )

    if serial:
        result = jenkins.start_build_conforma_verify(
            group=group,
            assembly='stream',
            ec_policy=ec_policy,
            fbc_ec_policy=fbc_ec_policy,
            effective_time=effective_time,
            include_corresponding_bundles=True,
            include_corresponding_fbcs=fbc_ec_policy is not None,
            report_to_slack=True,
            block_until_building=True,
            block_until_complete=True,
        )
        runtime.logger.info('[%s] Conforma verify completed with result: %s', group, result)
    else:
        jenkins.start_build_conforma_verify(
            group=group,
            assembly='stream',
            ec_policy=ec_policy,
            fbc_ec_policy=fbc_ec_policy,
            effective_time=effective_time,
            include_corresponding_bundles=True,
            include_corresponding_fbcs=fbc_ec_policy is not None,
            report_to_slack=True,
            block_until_building=False,
        )


@cli.command('schedule-build-conforma-verify')
@click.option('--group', '-g', required=True, help='Group to verify (e.g. openshift-4.18, logging-6.7)', multiple=True)
@click.option('--serial', is_flag=True, default=False, help='Run verifications sequentially, waiting for each')
@pass_runtime
@click_coroutine
async def schedule_build_conforma_verify(runtime: Runtime, group: tuple, serial: bool):
    jenkins.init_jenkins()
    failed_groups: list[str] = []

    if serial:
        runtime.logger.info('Running conforma verify serially for groups: %s', ', '.join(group))
        for g in group:
            try:
                await run_for(g, runtime, serial=True)
            except Exception:
                runtime.logger.warning('[%s] Failed to spawn conforma verify job', g, exc_info=True)
                failed_groups.append(g)
    else:
        results = await asyncio.gather(*[run_for(g, runtime) for g in group], return_exceptions=True)
        for g, result in zip(group, results):
            if isinstance(result, Exception):
                runtime.logger.warning('[%s] Failed to spawn conforma verify job: %s', g, result)
                failed_groups.append(g)

    if failed_groups:
        raise RuntimeError(f"Failed to spawn conforma verify for: {', '.join(failed_groups)}")
