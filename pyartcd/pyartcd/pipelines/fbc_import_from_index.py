import logging
from datetime import datetime, timezone
from pathlib import Path

import click
from artcommonlib import exectools

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime


class FbcImportPipeline:
    def __init__(self, runtime: Runtime,
                 version: str, assembly: str, data_path: str, data_gitref: str,
                 only: str, exclude: str, from_operator_index: str, into_fbc_repo: str
                 ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.only = only
        self.exclude = exclude
        self.from_operator_index = from_operator_index
        self.into_fbc_repo = into_fbc_repo

        self._logger = logging.getLogger(__name__)
        self._slack_client = runtime.new_slack_client()
        self._slack_client.bind_channel(version)

    async def run(self):
        try:
            # Import FBC objects from given operator index
            self._logger.info('Importing FBC objects from operator index')
            await self._import_from_index()

            # Parse doozer record.log
            self._logger.info('Parsing doozer record.log')
            fbc_nvrs = await self._parse_record_log()

            self._logger.info(f'Successfully built:\n{", ".join(fbc_nvrs)}')
        except Exception as e:
            self._logger.error('Encountered error: %s', e)
            await self._slack_client.say('*:heavy_exclamation_mark: Error importing FBC objects from operator index*\n')
            raise

    async def _import_from_index(self):
        cmd = [
            'doozer',
            '--build-system=konflux',
            f'--working-dir={self.runtime.doozer_working}',
            f'--assembly={self.assembly}',
            f'--group=openshift-{self.version}{"@" + self.data_gitref if self.data_gitref else ""}',
        ]
        if self.data_path:
            cmd.append(f'--data-path={self.data_path}')
        if self.only:
            cmd.append(f'--images={self.only}')
        if self.exclude:
            cmd.append(f'--exclude={self.exclude}')
        cmd.append('beta:fbc:import')
        if self.from_operator_index:
            cmd.extend(['--from-index', self.from_operator_index])
        if self.into_fbc_repo:
            cmd.extend(['--fbc-repo', self.into_fbc_repo])
        if not self.runtime.dry_run:
            cmd.append('--push')
        datetime_str = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        cmd.extend(['--message', f'Import FBC objects for {self.version} on {datetime_str}'])
        await exectools.cmd_assert_async(cmd)

    async def _parse_record_log(self):
        fbc_nvrs = []
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        if record_log_path.exists():
            with record_log_path.open() as file:
                record_log = parse_record_log(file)
            records = record_log.get('build_fbc_konflux', [])
            for record in records:
                if record['status'] != '0':
                    raise RuntimeError('record.log includes unexpected build_fbc_konflux '
                                       f'record with error message: {record["message"]}')
                fbc_nvrs.append(record['fbc_nvr'])
        return fbc_nvrs


@cli.command('fbc-import-from-index')
@click.option('--version', required=True, help='OCP version')
@click.option('--assembly', required=True, help='Assembly name')
@click.option('--data-path', required=False, default=constants.OCP_BUILD_DATA_URL,
              help='ocp-build-data fork to use (e.g. assembly definition in your own fork)')
@click.option('--data-gitref', required=False,
              help='(Optional) Doozer data path git [branch / tag / sha] to use')
@click.option('--only', required=False,
              help='(Optional) List **only** the operators you want to build, everything else gets ignored.\n'
                   'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
                   'Leave empty to build all (except EXCLUDE, if defined)')
@click.option('--exclude', required=False,
              help='(Optional) List the operators you **don\'t** want to build, everything else gets built.\n'
                   'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
                   'Leave empty to build all (or ONLY, if defined)')
@click.option("--from-operator-index", required=False, help="Path to the operator index file to import from")
@click.option('--into-fbc-repo', required=False, default='',
              help='(Optional) Path to the FBC repository to import the operator bundles into')
@pass_runtime
@click_coroutine
async def fbc_import_from_index(
        runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref: str,
        only: str, exclude: str, from_operator_index: str, into_fbc_repo: str):
    pipeline = FbcImportPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        only=only,
        exclude=exclude,
        from_operator_index=from_operator_index,
        into_fbc_repo=into_fbc_repo,
    )
    await pipeline.run()
