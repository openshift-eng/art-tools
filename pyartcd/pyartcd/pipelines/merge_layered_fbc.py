"""Orchestrate a unified layered-product FBC build without creating a shipment."""

import asyncio
import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path

import click
from artcommonlib import exectools
from artcommonlib.git_helper import validate_git_branch, validate_git_group_name, validate_git_remote_url
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxFbcBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import (
    extract_ocp_version_from_fbc_nvr,
    oc_image_info_for_arch_async,
    split_git_url,
)
from yaml import safe_load

from pyartcd import constants, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.lp_fbc import recorded_assembly_fbcs
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config


@dataclass(frozen=True)
class FbcTargetPlan:
    target: str
    inputs: dict


class MergeLayeredFbcPipeline:
    def __init__(
        self,
        runtime: Runtime,
        ocp_target_version: str | None = None,
        fbc_builds: tuple[str, ...] = (),
        assemblies: tuple[str, ...] = (),
        product: str | None = None,
        data_path: str = constants.OCP_BUILD_DATA_URL,
        fbc_repo: str | None = None,
        fbc_branch: str | None = None,
        image_repo: str | None = None,
        kubeconfig: str | None = None,
        registry_auth: str | None = None,
        plr_template: str | None = None,
        skip_checks: bool = False,
        skip_rebase: bool = False,
        skip_tasks: tuple[str, ...] = (),
        output_file: str | None = None,
        max_concurrent_targets: int = 1,
    ):
        if max_concurrent_targets < 1:
            raise ValueError('max_concurrent_targets must be at least 1')
        self.max_concurrent_targets = max_concurrent_targets
        validate_git_branch(fbc_branch)
        validate_git_remote_url(data_path)
        if fbc_repo is not None:
            validate_git_remote_url(fbc_repo)
        self.runtime = runtime
        self.group = None
        self.product = product
        self.target = ocp_target_version or None
        self.builds = fbc_builds
        self.assemblies = assemblies
        self.assembly_inputs = {}
        self.data_path = data_path
        self.output_file = Path(output_file or runtime.working_dir / 'unified-fbc.json').absolute()
        self.options = {
            '--fbc-repo': fbc_repo,
            '--fbc-branch': fbc_branch,
            '--image-repo': image_repo,
            '--konflux-kubeconfig': kubeconfig,
            '--registry-auth': registry_auth,
            '--plr-template': plr_template,
        }
        self.skip_checks = skip_checks
        self.skip_rebase = skip_rebase or bool(fbc_branch)
        self.skip_tasks = skip_tasks

    async def _resolve_assemblies(self) -> list[FbcTargetPlan]:
        if self.product not in (None, 'quay'):
            raise ValueError('Assembly inputs currently support product quay only')
        if self.builds or self.options['--fbc-branch'] or self.skip_rebase:
            raise ValueError('--assembly cannot be combined with --fbc-build, --fbc-branch, or --skip-rebase')
        self.product = 'quay'
        selections = {}
        for assembly in self.assemblies:
            if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', assembly):
                raise ValueError(f'Assembly {assembly} must have MAJOR.MINOR.PATCH format')
            group = 'quay-' + '.'.join(assembly.split('.')[:2])
            if group in selections:
                raise ValueError(f'Multiple assemblies supplied for {group}')
            selections[group] = assembly
        host, owner, name = split_git_url(self.data_path)
        if host != 'github.com':
            raise ValueError('Assembly inputs require a GitHub build-data repository')

        def read_assemblies():
            repo = get_github_client_for_org(owner).get_repo(f'{owner}/{name}')
            snapshots = []
            for group, assembly in sorted(selections.items()):
                revision = repo.get_branch(group).commit.sha
                releases = safe_load(repo.get_contents('releases.yml', ref=revision).decoded_content)
                if not isinstance(releases, dict) or assembly not in releases.get('releases', {}):
                    raise ValueError(f'Assembly {assembly} is missing from {group} releases.yml')
                snapshots.append((group, assembly, revision, releases))
            return snapshots

        snapshots = await exectools.to_thread(read_assemblies)
        inputs_by_target = {}
        for group, assembly, revision, releases in snapshots:
            config = await load_group_config(
                group,
                assembly,
                doozer_data_path=self.data_path,
                doozer_data_gitref=revision,
            )
            if config.get('product') != self.product:
                raise ValueError(f'{group} does not belong to product {self.product}')
            targets = config.get('OCP_TARGET_VERSIONS')
            if (
                not isinstance(targets, list)
                or not targets
                or any(not isinstance(target, str) or not re.fullmatch(r'[0-9]+\.[0-9]+', target) for target in targets)
            ):
                raise ValueError(f'{group} has no valid OCP_TARGET_VERSIONS')
            for target in sorted(set(targets), key=lambda value: tuple(map(int, value.split('.')))):
                if self.target and target != self.target:
                    continue
                entries = recorded_assembly_fbcs(releases, assembly, target)
                inputs = inputs_by_target.setdefault(target, {})
                for operator, record in sorted(entries.items()):
                    if record['nvr'] in inputs:
                        raise ValueError(f'Duplicate FBC NVR selected for OCP {target}: {record["nvr"]}')
                    inputs[record['nvr']] = {
                        'assembly': assembly,
                        'group': group,
                        'group_revision': revision,
                        'operator': operator,
                        'pullspec': record['pullspec'],
                    }
        if not inputs_by_target:
            raise ValueError(f'No selected assembly supports OCP {self.target}')
        return [
            FbcTargetPlan(target, inputs_by_target[target])
            for target in sorted(inputs_by_target, key=lambda value: tuple(map(int, value.split('.'))))
        ]

    def _target_worker(self, plan: FbcTargetPlan):
        runtime = copy.copy(self.runtime)
        runtime.working_dir = Path(self.runtime.working_dir).absolute() / f'ocp-{plan.target}'
        runtime.doozer_working = str(runtime.working_dir / 'doozer')
        worker = MergeLayeredFbcPipeline(
            runtime,
            plan.target,
            product=self.product,
            data_path=self.data_path,
            fbc_builds=tuple(record['pullspec'] for record in plan.inputs.values()),
        )
        worker.options = dict(self.options)
        worker.skip_checks = self.skip_checks
        worker.skip_tasks = self.skip_tasks
        worker.assembly_inputs = plan.inputs
        worker.group = min(record['group'] for record in plan.inputs.values())
        return worker

    async def _run_assembly_targets(self) -> dict:
        # Remove an earlier aggregate result before preflight so it cannot masquerade as this run.
        self.output_file.unlink(missing_ok=True)
        plans = await self._resolve_assemblies()
        workers = [self._target_worker(plan) for plan in plans]
        # Check every target's selected build records before starting any build.
        for worker in workers:
            groups = await worker._resolve_groups()
            expected = sorted({source['group'] for source in worker.assembly_inputs.values()})
            if groups != expected:
                raise ValueError(f'OCP {worker.target} resolved unexpected source groups: {groups}')
        self.runtime.logger.info(
            'Building %d OCP targets with concurrency %d', len(workers), self.max_concurrent_targets
        )
        semaphore = asyncio.Semaphore(self.max_concurrent_targets)

        async def build_target(worker):
            assemblies = sorted({source['assembly'] for source in worker.assembly_inputs.values()})
            async with semaphore:
                self.runtime.logger.info('OCP %s: selected assemblies %s', worker.target, assemblies)
                try:
                    worker.runtime.working_dir.mkdir(parents=True, exist_ok=True)
                    result = await worker._run_build()
                    return {**result, 'status': 'Succeeded', 'assemblies': assemblies}
                except Exception as exc:
                    self.runtime.logger.exception('Unified FBC failed for OCP %s', worker.target)
                    return {'status': 'Failed', 'assemblies': assemblies, 'error': str(exc)}

        outcomes = await asyncio.gather(*(build_target(worker) for worker in workers))
        result = {
            'dry_run': self.runtime.dry_run,
            'results': {plan.target: outcome for plan, outcome in zip(plans, outcomes)},
            'failed_targets': [plan.target for plan, outcome in zip(plans, outcomes) if outcome['status'] == 'Failed'],
        }
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.output_file.write_text(json.dumps(result, indent=2) + '\n')
        self.runtime.logger.info('Unified FBC results: %s', json.dumps(result))
        if result['failed_targets']:
            raise RuntimeError(
                f'Unified FBC builds failed for OCP {result["failed_targets"]}; results: {self.output_file}'
            )
        return result

    async def _resolve_groups(self) -> list[str]:
        if not self.builds:
            return await self._resolve_branch_group()
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
            validate_git_group_name(record.group)
            if self.assembly_inputs:
                source = self.assembly_inputs.get(record.nvr)
                if not source or source['group'] != record.group or f"{source['operator']}-fbc" != record.name:
                    raise ValueError(f'{build} does not match its recorded assembly FBC identity')
            groups.add(record.group)
        return sorted(groups)

    async def _resolve_branch_group(self) -> list[str]:
        repository = self.options['--fbc-repo'] or 'https://github.com/openshift-priv/art-fbc.git'

        def read_provenance():
            host, owner, name = split_git_url(repository)
            if host != 'github.com':
                raise ValueError(f'Unsupported FBC source repository {repository}')
            repo = get_github_client_for_org(owner).get_repo(f'{owner}/{name}')
            return json.loads(
                repo.get_contents('unified-provenance.json', ref=self.options['--fbc-branch']).decoded_content
            )

        provenance = await exectools.to_thread(read_provenance)
        if not isinstance(provenance, dict):
            raise ValueError('FBC branch provenance must be a JSON object')
        group = provenance.get('execution_group')
        try:
            validate_git_group_name(group)
        except ValueError:
            raise ValueError('FBC branch provenance has no valid execution_group') from None
        if self.target is None:
            self.target = provenance.get('ocp_target_version')
        if not isinstance(self.target, str) or not re.fullmatch(r'[0-9]+\.[0-9]+', self.target):
            raise ValueError('FBC branch provenance has no valid OCP target')
        if provenance.get('ocp_target_version') != self.target:
            raise ValueError(f'FBC branch provenance does not match OCP {self.target}')
        product = provenance.get('product')
        if not isinstance(product, str) or not product or product == 'ocp':
            raise ValueError('FBC branch provenance has no layered-product identity')
        if self.product and product != self.product:
            raise ValueError(f'FBC branch belongs to {product}, expected product {self.product}')
        self.product = product
        return [group]

    async def run(self) -> dict:
        if self.target is not None and not re.fullmatch(r'[0-9]+\.[0-9]+', self.target):
            raise ValueError('OCP target must have MAJOR.MINOR format')
        if self.assemblies:
            return await self._run_assembly_targets()
        if not self.builds and not self.options['--fbc-branch']:
            raise ValueError('At least one assembly or FBC build is required unless --fbc-branch is set')
        if not self.target and not self.options['--fbc-branch']:
            raise ValueError('--ocp-target-version is required for explicit FBC build inputs')
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
        self.product = product
        return await self._run_build()

    async def _run_build(self) -> dict:
        if self.runtime.dry_run:
            return await self._build()
        return await locks.run_with_lock(
            coro=self._build(),
            lock=Lock.FBC_BUILD,
            lock_name=f'lock:fbc-merge:{self.product}:ocp-{self.target}',
        )

    async def _build(self) -> dict:
        group_ref = self.group
        if self.assembly_inputs:
            revision = next(
                source['group_revision'] for source in self.assembly_inputs.values() if source['group'] == self.group
            )
            group_ref += '@' + revision
        cmd = [
            'doozer',
            '--build-system=konflux',
            f'--working-dir={self.runtime.doozer_working}',
            f'--data-path={self.data_path}',
            f'--group={group_ref}',
            '--assembly=stream',
            'beta:fbc:merge-layered',
            f'--major-minor={self.target}',
            f'--output-file={self.output_file}',
        ]
        if self.assembly_inputs:
            inputs_file = Path(self.runtime.working_dir) / 'unified-fbc-assembly-inputs.json'
            inputs_file.write_text(json.dumps(self.assembly_inputs, indent=2) + '\n')
            cmd.extend(['--assembly-inputs-file', str(inputs_file)])
        for build in self.builds:
            cmd.extend(['--fbc-build', build])
        for option, value in self.options.items():
            if value:
                cmd.extend([option, value])
        if self.runtime.dry_run:
            cmd.append('--dry-run')
        if self.skip_checks:
            cmd.append('--skip-checks')
        if self.skip_rebase:
            cmd.append('--skip-rebase')
        for task in self.skip_tasks:
            cmd.extend(['--skip-task', task])
        self.runtime.logger.info('Building unified FBC for %s OCP %s', self.group, self.target)
        await exectools.cmd_assert_async(cmd)
        result = json.loads(self.output_file.read_text())
        if not self.runtime.dry_run and (not result.get('pullspec') or not result.get('build_url')):
            raise ValueError('Unified FBC build returned no immutable pullspec or build URL')
        self.runtime.logger.info('Unified FBC result: %s', json.dumps(result))
        return result


@cli.command('lp-unified-fbc', help='Build unified layered-product catalogs across selected assembly targets')
@click.option(
    '--ocp-target-version', help='Optional OCP MAJOR.MINOR filter; assembly inputs default to all supported targets.'
)
@click.option(
    '--fbc-build',
    'fbc_builds',
    multiple=True,
    help='FBC NVR or pullspec; repeat for each source version. Optional when --fbc-branch is set.',
)
@click.option('--assembly', 'assemblies', multiple=True, help='Full Quay assembly version; repeat for each release.')
@click.option(
    '--max-concurrent-targets',
    type=click.IntRange(min=1),
    default=1,
    show_default=True,
    help='Maximum assembly targets built at once; serial execution bounds catalog memory.',
)
@click.option('--product', help='Require all source builds to belong to this product.')
@click.option('--data-path', default=constants.OCP_BUILD_DATA_URL)
@click.option('--fbc-repo')
@click.option('--fbc-branch', help='Build this existing FBC source branch directly; implies --skip-rebase.')
@click.option('--image-repo')
@click.option('--kubeconfig', type=click.Path())
@click.option('--registry-auth', type=click.Path(exists=True), envvar='QUAY_AUTH_FILE')
@click.option('--plr-template', help='Full URL of the Konflux FBC PipelineRun template.')
@click.option('--skip-checks', is_flag=True)
@click.option('--skip-rebase', is_flag=True, help='Build the existing unified FBC source branch without rebasing.')
@click.option('--skip-task', 'skip_tasks', multiple=True)
@click.option('--output-file', type=click.Path(dir_okay=False))
@pass_runtime
@click_coroutine
async def merge_layered_fbc(runtime: Runtime, **kwargs):
    result = await MergeLayeredFbcPipeline(runtime, **kwargs).run()
    click.echo(json.dumps(result, indent=2))
