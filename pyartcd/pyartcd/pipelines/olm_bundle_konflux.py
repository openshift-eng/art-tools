import asyncio
import sys
from pathlib import Path

import click
from artcommonlib import exectools
from artcommonlib.constants import PRODUCT_KUBECONFIG_MAP
from artcommonlib.util import resolve_konflux_kubeconfig_by_product, resolve_konflux_namespace_by_product

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config


async def _stage_release_related_images(
    runtime: Runtime,
    doozer_base_cmd: list,
    namespace: str,
    final_kubeconfig: str,
    operator_nvrs: list,
) -> list:
    """Run doozer beta:bundle:stage-release-related-images for the given operator NVRs.

    Called once per bundle job, before triggering FBC builds, so images are released to the advisory
    here rather than once per FBC build (which could be many for layered products). Doozer stage-releases
    each operator separately, so one operator's failure leaves the rest untouched.

    :return: the operator NVRs whose stage release failed. An empty list means everything succeeded.
    """
    cmd = doozer_base_cmd + [
        'beta:bundle:stage-release-related-images',
        '--konflux-namespace',
        namespace,
        '--konflux-kubeconfig',
        final_kubeconfig,
    ]
    if runtime.dry_run:
        cmd.append('--dry-run')
    cmd.append('--')
    cmd.extend(operator_nvrs)

    runtime.logger.info('Stage-releasing bundle related images')
    try:
        await exectools.cmd_assert_async(cmd)
        return []
    except ChildProcessError as e:
        failed_nvrs = _parse_failed_stage_releases(runtime)
        if not failed_nvrs:
            # Doozer failed without attributing the failure to any operator (bad kubeconfig, crash before
            # the first operator, ...). That is a job-wide failure, not a partial one.
            runtime.logger.error(f'Stage release of related images failed with no per-operator results: {e}')
            raise
        runtime.logger.error(f'Stage release of related images failed for: {", ".join(failed_nvrs)}')
        return failed_nvrs


def _parse_failed_stage_releases(runtime: Runtime) -> list:
    """Collect operator NVRs whose stage_release_related_images record.log entry reports a failure."""
    record_log_path = Path(runtime.doozer_working, 'record.log')
    if not record_log_path.exists():
        runtime.logger.error('record.log not found - cannot determine stage release results')
        return []
    with record_log_path.open() as file:
        record_log = parse_record_log(file)
    failed_nvrs = []
    for record in record_log.get('stage_release_related_images', []):
        if record.get('status') == '0':
            continue
        nvr = record.get('operator_nvr')
        if nvr:
            failed_nvrs.append(nvr)
            runtime.logger.error(f'Stage release failed for {nvr}: {record.get("message", "")}')
    return failed_nvrs


@cli.command('olm-bundle-konflux')
@click.option('--version', required=True, help='OCP version')
@click.option('--assembly', required=True, help='Assembly name')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=False,
    help="The group of components on which to operate. e.g. openshift-4.9 / oadp-1.5",
)
@click.option('--data-gitref', required=False, help='(Optional) Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--nvrs',
    required=False,
    help='(Optional) List **only** the operator NVRs you want to build bundles for, everything else '
    'gets ignored. The operators should not be mode:disabled/wip in ocp-build-data',
)
@click.option(
    '--only',
    required=False,
    help='(Optional) List **only** the operators you want to build, everything else gets ignored.\n'
    'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
    'Leave empty to build all (except EXCLUDE, if defined)',
)
@click.option(
    '--exclude',
    required=False,
    help='(Optional) List the operators you **don\'t** want to build, everything else gets built.\n'
    'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
    'Leave empty to build all (or ONLY, if defined)',
)
@click.option(
    '--force', is_flag=True, help='Rebuild bundle containers, even if they already exist for given operator NVRs'
)
@click.option(
    '--force-release', is_flag=True, help='Stage-release related images even if bundle containers were not rebuilt'
)
@click.option("--kubeconfig", required=False, help="Path to kubeconfig file to use for Konflux cluster connections")
@click.option(
    '--plr-template',
    required=False,
    default='',
    help='Override the Pipeline Run template commit from openshift-priv/art-konflux-template; format: <owner>@<branch>',
)
@pass_runtime
@click_coroutine
async def olm_bundle_konflux(
    runtime: Runtime,
    version: str,
    assembly: str,
    data_path: str,
    data_gitref: str,
    nvrs: str,
    only: bool,
    exclude: str,
    force: bool,
    force_release: bool,
    kubeconfig: str,
    plr_template: str,
    group: str,
):
    # If unspecified, assume it's for openshift
    if not group:
        group = f"openshift-{version}"

    # Shared doozer invocation prefix, reused by the stage-release call below
    doozer_base_cmd = [
        'doozer',
        '--build-system=konflux',
        f'--assembly={assembly}',
        f'--working-dir={runtime.doozer_working}',
        f'--group={group}@{data_gitref}' if data_gitref else f'--group={group}',
        f'--data-path={data_path}',
    ]

    # Create Doozer invocation
    cmd = doozer_base_cmd.copy()
    if only:
        cmd.append(f'--images={only}')
    if exclude:
        cmd.append(f'--exclude={exclude}')
    cmd.append('beta:images:konflux:bundle')
    if force:
        cmd.append('--force')

    # Load group config to get product information
    group_config = await load_group_config(
        group=group, assembly=assembly, doozer_data_path=data_path, doozer_data_gitref=data_gitref
    )
    product = group_config.get('product', 'ocp')

    # Set namespace based on product
    namespace = resolve_konflux_namespace_by_product(product)
    cmd.extend(['--konflux-namespace', namespace])

    # Use kubeconfig from CLI parameter or product-specific environment variable
    final_kubeconfig = resolve_konflux_kubeconfig_by_product(product, kubeconfig)
    if not final_kubeconfig:
        available_env_vars = list(PRODUCT_KUBECONFIG_MAP.values())
        raise ValueError(
            f"Kubeconfig required for Konflux builds. Provide --kubeconfig parameter or set one of: {', '.join(available_env_vars)}"
        )

    cmd.extend(['--konflux-kubeconfig', final_kubeconfig])
    if plr_template:
        plr_template_owner, plr_template_branch = (
            plr_template.split("@") if plr_template else ["openshift-priv", "main"]
        )
        plr_template_url = constants.KONFLUX_BUNDLE_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
            owner=plr_template_owner, branch_name=plr_template_branch
        )
        cmd.extend(['--plr-template', plr_template_url])
    if runtime.dry_run:
        cmd.append('--dry-run')
    if nvrs:
        cmd.append('--')
        cmd.extend(nvrs.split(','))

    lock = Lock.OLM_BUNDLE_KONFLUX
    lock_name = lock.value.format(version=version)
    lock_identifier = jenkins.get_build_path_or_random()

    # Track whether doozer command succeeded
    doozer_error = None

    try:
        # Build bundles
        await locks.run_with_lock(
            coro=exectools.cmd_assert_async(cmd),
            lock=lock,
            lock_name=lock_name,
            lock_id=lock_identifier,
        )
    except ChildProcessError as e:
        # Doozer command failed - but bundles may have partially succeeded
        doozer_error = e
        runtime.logger.warning(f'Doozer command failed: {e}')
        runtime.logger.info('Checking record.log for partial success...')

    # Explicitly-requested operator NVRs. Only used if record.log turns up empty, which means
    # every requested bundle already existed and doozer skipped it.
    fallback_nvrs = [n.strip() for n in nvrs.split(',') if n.strip()] if nvrs else []

    # Parse doozer record.log to determine actual build results
    # This runs regardless of whether doozer succeeded or failed
    operator_nvrs = []
    operators_with_failed_bundles = []
    successful_bundles = []

    record_log_path = Path(runtime.doozer_working, 'record.log')
    if record_log_path.exists():
        with record_log_path.open() as file:
            record_log = parse_record_log(file)
        records = record_log.get('build_olm_bundle_konflux', [])

        for record in records:
            if record['status'] == '0':
                operator_nvrs.append(record['operator_nvr'])
                successful_bundles.append(record['bundle_nvr'])
            else:
                operators_with_failed_bundles.append(record.get('operator_nvr', 'unknown'))

        if successful_bundles:
            runtime.logger.info(f'Successfully built {len(successful_bundles)} bundle(s): {successful_bundles}')
        if operators_with_failed_bundles:
            runtime.logger.error(f'Bundle builds of the following operators failed: {operators_with_failed_bundles}')
    else:
        runtime.logger.error('record.log not found - cannot determine build results')
        if doozer_error:
            # No record.log and doozer failed
            raise doozer_error

    total_bundles = len(successful_bundles) + len(operators_with_failed_bundles)
    bundles_were_built = total_bundles > 0
    if total_bundles == 0:
        if doozer_error:
            runtime.logger.error('No bundle builds were attempted')
            raise doozer_error
        if not fallback_nvrs:
            raise RuntimeError('No bundle builds found in record.log and no input NVRs provided')
        # Doozer succeeded but wrote no record.log — all bundles already existed and were skipped.
        # Use fallback_nvrs to still trigger FBC builds.
        runtime.logger.info(
            'No new bundles were built (all already exist); proceeding with %d pre-existing operator(s)',
            len(fallback_nvrs),
        )
        operator_nvrs = fallback_nvrs

    if operators_with_failed_bundles and not successful_bundles:
        # All builds failed - re-raise the error or raise a new one
        runtime.logger.error(f'All {len(operators_with_failed_bundles)} bundle build(s) failed')
        if doozer_error:
            raise doozer_error
        raise RuntimeError(f'All {len(operators_with_failed_bundles)} bundle build(s) failed')

    # Operators whose related-image stage release failed; they are excluded from FBC and mark the job UNSTABLE
    failed_stage_nvrs = []

    # Trigger FBC builds for successful bundles only
    if operator_nvrs:
        runtime.logger.info(f'Found operator NVRs: {operator_nvrs}')

        # Automatically propagate parameters if set in environment
        propagate_params = jenkins.get_propagatable_params()

        # Stage-release related images before triggering any FBC builds.
        # Skip when bundles were not rebuilt — related images were already released in the original build.
        if bundles_were_built or force_release:
            failed_stage_nvrs = await _stage_release_related_images(
                runtime=runtime,
                doozer_base_cmd=doozer_base_cmd,
                namespace=namespace,
                final_kubeconfig=final_kubeconfig,
                operator_nvrs=operator_nvrs,
            )
            if failed_stage_nvrs:
                operator_nvrs = [nvr for nvr in operator_nvrs if nvr not in failed_stage_nvrs]
                if not operator_nvrs:
                    raise RuntimeError(
                        f'Stage release of related images failed for every operator: {", ".join(failed_stage_nvrs)}'
                    )
                runtime.logger.warning(
                    f'Skipping FBC builds for {len(failed_stage_nvrs)} operator(s) whose stage release failed; '
                    f'continuing with: {operator_nvrs}'
                )
        else:
            runtime.logger.info(
                'Skipping stage release of related images — no new bundles were built (use --force-release to override)'
            )

        # Check if this is a non-openshift group and if OCP_TARGET_VERSIONS is configured
        if group and not group.startswith("openshift-"):
            runtime.logger.info(f'Group {group} is a non-openshift group, checking for OCP_TARGET_VERSIONS')
            # Load group config to check for OCP_TARGET_VERSIONS
            group_config = await load_group_config(
                group=group, assembly=assembly, doozer_data_path=data_path, doozer_data_gitref=data_gitref
            )

            # Check if OCP_TARGET_VERSIONS is defined in group config
            ocp_target_versions = group_config.get("OCP_TARGET_VERSIONS")
            runtime.logger.info(f'OCP_TARGET_VERSIONS from group config: {ocp_target_versions}')

            if ocp_target_versions:
                runtime.logger.info(f'Starting multiple FBC jobs for target versions: {ocp_target_versions}')
                # Generate multiple FBC jobs, one for each target version
                for target_version in ocp_target_versions:
                    runtime.logger.info(f'Starting FBC job for target version: {target_version}')
                    jenkins.start_build_fbc(
                        version=version,
                        group=group,
                        assembly=assembly,
                        operator_nvrs=operator_nvrs,
                        dry_run=runtime.dry_run,
                        ocp_target_version=target_version,
                        # Always force rebuild FBCs for OADP / MTA / MTC
                        force_build=True,
                        propagate_params=propagate_params,
                    )
                    await asyncio.sleep(10)
            else:
                runtime.logger.info(f'No OCP_TARGET_VERSIONS defined for group {group}, using original behavior')
                # No OCP_TARGET_VERSIONS defined, use original behavior
                jenkins.start_build_fbc(
                    version=version,
                    group=group,
                    assembly=assembly,
                    operator_nvrs=operator_nvrs,
                    dry_run=runtime.dry_run,
                    propagate_params=propagate_params,
                )
        else:
            runtime.logger.info(f'Group {group} does not match OADP/MTA/MTC pattern, using original behavior')
            # Not an OADP/MTA/MTC group, use original behavior
            jenkins.start_build_fbc(
                version=version,
                group=group if group else None,
                assembly=assembly,
                operator_nvrs=operator_nvrs,
                dry_run=runtime.dry_run,
                propagate_params=propagate_params,
            )

    if operators_with_failed_bundles or failed_stage_nvrs:
        # Partial failure - exit with code 2 to mark Jenkins job as UNSTABLE
        runtime.logger.warning(
            'Job completed with partial success - FBC triggered only for the operators that fully succeeded'
        )
        if successful_bundles:
            runtime.logger.warning(
                f'{len(successful_bundles)} bundle(s) built successfully: {", ".join(successful_bundles)}'
            )
        if operators_with_failed_bundles:
            runtime.logger.warning(f'{len(operators_with_failed_bundles)} bundle(s) failed - see errors above')
        if failed_stage_nvrs:
            runtime.logger.warning(
                f'{len(failed_stage_nvrs)} operator(s) failed to stage-release their related images: '
                f'{", ".join(failed_stage_nvrs)}'
            )
        sys.exit(2)

    # If we get here, all builds succeeded - job will be marked as SUCCESS
    runtime.logger.info('All bundle builds completed successfully')
