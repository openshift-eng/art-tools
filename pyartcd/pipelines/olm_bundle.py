import os

import click
from aioredlock import LockError

from pyartcd import constants, exectools, jenkins
from pyartcd.cli import cli, pass_runtime, click_coroutine
from pyartcd import locks
from pyartcd.locks import Lock
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime


@cli.command('olm-bundle')
@click.option('--version', required=True, help='OCP version')
@click.option('--assembly', required=True, help='Assembly name')
@click.option('--data-path', required=False, default=constants.OCP_BUILD_DATA_URL,
              help='ocp-build-data fork to use (e.g. assembly definition in your own fork)')
@click.option('--data-gitref', required=False,
              help='(Optional) Doozer data path git [branch / tag / sha] to use')
@click.option('--nvrs', required=False,
              help='(Optional) List **only** the operator NVRs you want to build bundles for, everything else '
                   'gets ignored. The operators should not be mode:disabled/wip in ocp-build-data')
@click.option('--only', required=False,
              help='(Optional) List **only** the operators you want to build, everything else gets ignored.\n'
                   'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
                   'Leave empty to build all (except EXCLUDE, if defined)')
@click.option('--exclude', required=False,
              help='(Optional) List the operators you **don\'t** want to build, everything else gets built.\n'
                   'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
                   'Leave empty to build all (or ONLY, if defined)')
@click.option('--force', is_flag=True,
              help='Rebuild bundle containers, even if they already exist for given operator NVRs')
@pass_runtime
@click_coroutine
async def olm_bundle(runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref: str,
                     nvrs: str, only: bool, exclude: str, force: bool):
    # Create Doozer invocation
    cmd = [
        'doozer',
        f'--assembly={assembly}',
        '--working-dir=doozer_working',
        f'--group=openshift-{version}@{data_gitref}' if data_gitref else f'--group=openshift-{version}',
        f'--data-path={data_path}'
    ]
    if only:
        cmd.append(f'--images={only}')
    if exclude:
        cmd.append(f'--exclude={exclude}')
    cmd.append('olm-bundle:rebase-and-build')
    if force:
        cmd.append('--force')
    if runtime.dry_run:
        cmd.append('--dry-run')
    if nvrs:
        cmd.append('--')
        cmd.extend(nvrs.split(','))

    lock = Lock.OLM_BUNDLE
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
            lock_id=lock_identifier
        )

        # Parse doozer record.log
        with open('doozer_working/record.log') as file:
            record_log = parse_record_log(file)
        records = record_log.get('build_olm_bundle', [])
        bundle_nvrs = []

        for record in records:
            if record['status'] != '0':
                raise RuntimeError('record.log includes unexpected build_olm_bundle '
                                   f'record with error message: {record["message"]}')
            bundle_nvrs.append(record['bundle_nvr'])

        runtime.logger.info(f'Successfully built:\n{", ".join(bundle_nvrs)}')

    except (ChildProcessError, RuntimeError) as e:
        runtime.logger.error('Encountered error: %s', e)
        if not runtime.dry_run:
            slack_client = runtime.new_slack_client()
            slack_client.bind_channel(version)
            await slack_client.say('*:heavy_exclamation_mark: olm_bundle failed*\n'
                                   f'buildvm job: {os.environ["BUILD_URL"]}')
            raise
