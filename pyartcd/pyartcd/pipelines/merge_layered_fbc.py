"""Orchestrate a unified layered-product FBC build without creating a shipment."""

import json
import re
from pathlib import Path

import click
from artcommonlib import exectools
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxFbcBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import extract_ocp_version_from_fbc_nvr, oc_image_info_for_arch_async

from pyartcd import constants, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config


class MergeLayeredFbcPipeline:
    def __init__(
        self,
        runtime: Runtime,
        ocp_target_version: str,
        fbc_builds: tuple[str, ...],
        product: str | None = None,
        data_path: str = constants.OCP_BUILD_DATA_URL,
        fbc_repo: str | None = None,
        image_repo: str | None = None,
        kubeconfig: str | None = None,
        registry_auth: str | None = None,
        plr_template: str | None = None,
        skip_checks: bool = False,
        skip_tasks: tuple[str, ...] = (),
        output_file: str | None = None,
    ):
        self.runtime = runtime
        self.group = None
        self.product = product
        self.target = ocp_target_version
        self.builds = fbc_builds
        self.data_path = data_path
        self.output_file = Path(output_file or runtime.working_dir / 'merged-fbc.json').absolute()
        self.options = {
            '--fbc-repo': fbc_repo,
            '--image-repo': image_repo,
            '--konflux-kubeconfig': kubeconfig,
            '--registry-auth': registry_auth,
            '--plr-template': plr_template,
        }
        self.skip_checks = skip_checks
        self.skip_tasks = skip_tasks

    async def _resolve_groups(self) -> list[str]:
        db = KonfluxDb()
        db.bind(KonfluxFbcBuildRecord)
        groups = set()
        for build in self.builds:
            nvr = build
            if '/' in build:
                info = await oc_image_info_for_arch_async(build, registry_config=self.options['--registry-auth'])
                nvr = info['config']['config'].get('Labels', {}).get('com.redhat.art.nvr')
                if not nvr:
                    raise ValueError(f'Missing FBC NVR label on {build}')
            record = await anext(
                db.search_builds_by_fields(where={'nvr': nvr, 'outcome': str(KonfluxBuildOutcome.SUCCESS)}, limit=1),
                None,
            )
            if not record or extract_ocp_version_from_fbc_nvr(record.nvr) != self.target:
                raise ValueError(f'{build} is not a successful FBC for OCP {self.target}')
            if not record.group or not re.fullmatch(r'[\w.-]+', record.group):
                raise ValueError(f'Invalid source group for {build}')
            groups.add(record.group)
        return sorted(groups)

    async def run(self) -> dict:
        if not self.builds:
            raise ValueError('At least one FBC build is required')
        if not re.fullmatch(r'\d+\.\d+', self.target):
            raise ValueError('OCP target must have MAJOR.MINOR format')
        groups = await self._resolve_groups()
        product = self.product
        for group in groups:
            config = await load_group_config(group, 'stream', None, self.data_path)
            source_product = config.get('product')
            if not source_product or source_product == 'ocp':
                raise ValueError(f'{group} is not a layered-product group')
            if product and source_product != product:
                raise ValueError(f'{group} belongs to {source_product}, expected product {product}')
            product = source_product
            if self.target not in config.get('OCP_TARGET_VERSIONS', []):
                raise ValueError(f'{group} does not support OCP {self.target}')
        self.group = groups[0]
        self.runtime.logger.info('Inferred source groups %s; using %s for build configuration', groups, self.group)
        if self.runtime.dry_run:
            return await self._build()
        return await locks.run_with_lock(
            coro=self._build(),
            lock=Lock.FBC_BUILD,
            lock_name=f'lock:fbc-merge:{product}:ocp-{self.target}',
        )

    async def _build(self) -> dict:
        cmd = [
            'doozer',
            '--build-system=konflux',
            f'--working-dir={self.runtime.doozer_working}',
            f'--data-path={self.data_path}',
            f'--group={self.group}',
            '--assembly=stream',
            'beta:fbc:merge-layered',
            f'--major-minor={self.target}',
            f'--output-file={self.output_file}',
        ]
        for build in self.builds:
            cmd.extend(['--fbc-build', build])
        for option, value in self.options.items():
            if value:
                cmd.extend([option, value])
        if self.runtime.dry_run:
            cmd.append('--dry-run')
        if self.skip_checks:
            cmd.append('--skip-checks')
        for task in self.skip_tasks:
            cmd.extend(['--skip-task', task])
        self.runtime.logger.info('Building merged FBC for %s OCP %s', self.group, self.target)
        await exectools.cmd_assert_async(cmd)
        result = json.loads(self.output_file.read_text())
        if not self.runtime.dry_run and (not result.get('pullspec') or not result.get('build_url')):
            raise ValueError('Merged FBC build returned no immutable pullspec or build URL')
        self.runtime.logger.info('Merged FBC result: %s', json.dumps(result))
        return result


@cli.command('merge-layered-fbc', help='Build a unified layered-product FBC for one OCP target')
@click.option('--ocp-target-version', required=True, help='Target OCP MAJOR.MINOR version.')
@click.option('--fbc-build', 'fbc_builds', multiple=True, required=True, help='FBC NVR or pullspec; repeat per build.')
@click.option('--product', help='Require all source builds to belong to this product.')
@click.option('--data-path', default=constants.OCP_BUILD_DATA_URL)
@click.option('--fbc-repo')
@click.option('--image-repo')
@click.option('--kubeconfig', type=click.Path())
@click.option('--registry-auth', type=click.Path(exists=True), envvar='QUAY_AUTH_FILE')
@click.option('--plr-template', help='Full URL of the Konflux FBC PipelineRun template.')
@click.option('--skip-checks', is_flag=True)
@click.option('--skip-task', 'skip_tasks', multiple=True)
@click.option('--output-file', type=click.Path(dir_okay=False))
@pass_runtime
@click_coroutine
async def merge_layered_fbc(runtime: Runtime, **kwargs):
    result = await MergeLayeredFbcPipeline(runtime, **kwargs).run()
    click.echo(json.dumps(result, indent=2))
