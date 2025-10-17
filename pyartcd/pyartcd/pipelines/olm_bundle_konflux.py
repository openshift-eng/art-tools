import asyncio
import os
from pathlib import Path

import click
from aioredlock import LockError
from artcommonlib import exectools
from artcommonlib.constants import GROUP_KUBECONFIG_MAP, GROUP_NAMESPACE_MAP

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config


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
    kubeconfig: str,
    plr_template: str,
    group: str,
):
    # If unspecified, assume it's for openshift
    if not group:
        group = f"openshift-{version}"

    # Create Doozer invocation
    cmd = [
        'doozer',
        '--build-system=konflux',
        f'--assembly={assembly}',
        f'--working-dir={runtime.doozer_working}',
        f'--group={group}@{data_gitref}' if data_gitref else f'--group={group}',
        f'--data-path={data_path}',
    ]
    if only:
        cmd.append(f'--images={only}')
    if exclude:
        cmd.append(f'--exclude={exclude}')
    cmd.append('beta:images:konflux:bundle')
    if force:
        cmd.append('--force')

    # Set namespace based on group prefix
    for prefix, namespace in GROUP_NAMESPACE_MAP.items():
        if group.startswith(prefix):
            cmd.extend(['--konflux-namespace', namespace])
            break

    # Use kubeconfig from CLI parameter or tenant-specific environment variable
    final_kubeconfig = kubeconfig
    if not final_kubeconfig:
        # Determine the appropriate environment variable based on group prefix
        kubeconfig_env_var = None
        for prefix, env_var in GROUP_KUBECONFIG_MAP.items():
            if group.startswith(prefix):
                kubeconfig_env_var = env_var
                break

        if kubeconfig_env_var:
            final_kubeconfig = os.environ.get(kubeconfig_env_var)

        if not final_kubeconfig:
            available_env_vars = list(GROUP_KUBECONFIG_MAP.values())
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
    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    try:
        # Build bundles
        await locks.run_with_lock(
            coro=exectools.cmd_assert_async(cmd),
            lock=lock,
            lock_name=lock_name,
            lock_id=lock_identifier,
        )

        # Parse doozer record.log
        bundle_nvrs = []
        operator_nvrs = []
        record_log_path = Path(runtime.doozer_working, 'record.log')
        if record_log_path.exists():
            with record_log_path.open() as file:
                record_log = parse_record_log(file)
            records = record_log.get('build_olm_bundle_konflux', [])
            for record in records:
                if record['status'] != '0':
                    raise RuntimeError(
                        'record.log includes unexpected build_olm_bundle_konflux '
                        f'record with error message: {record["message"]}'
                    )
                bundle_nvrs.append(record['bundle_nvr'])
                operator_nvrs.append(record['operator_nvr'])

        runtime.logger.info(f'Successfully built:\n{", ".join(bundle_nvrs)}')

        if operator_nvrs:
            runtime.logger.info(f'Found operator NVRs: {operator_nvrs}')
            # Check if this is an OADP, MTA, or MTC group and if OCP_TARGET_VERSIONS is configured
            if group and (group.startswith("oadp-") or group.startswith("mta-") or group.startswith("mtc-")):
                runtime.logger.info(f'Group {group} matches OADP/MTA/MTC pattern, checking for OCP_TARGET_VERSIONS')
                # Load group config to check for OCP_TARGET_VERSIONS
                group_config = await load_group_config(group=group, assembly=assembly, doozer_data_path=data_path)

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
                        )
                        await asyncio.sleep(5)
                else:
                    runtime.logger.info(f'No OCP_TARGET_VERSIONS defined for group {group}, using original behavior')
                    # No OCP_TARGET_VERSIONS defined, use original behavior
                    jenkins.start_build_fbc(
                        version=version,
                        group=group,
                        assembly=assembly,
                        operator_nvrs=operator_nvrs,
                        dry_run=runtime.dry_run,
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
                )

    except (ChildProcessError, RuntimeError) as e:
        runtime.logger.error('Encountered error: %s', e)
        # if not runtime.dry_run and assembly != 'test':
        #    slack_client = runtime.new_slack_client()
        #    slack_client.bind_channel(version)
        #    await slack_client.say('*:heavy_exclamation_mark: konflux_olm_bundle failed*\n'
        #                           f'buildvm job: {os.environ["BUILD_URL"]}')
        raise
