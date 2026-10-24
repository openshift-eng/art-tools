"""Prepare several Quay releases and ship their product-wide unified FBCs."""

import copy
import json
import os
import re

import click
from artcommonlib.assembly import assembly_image_pins
from artcommonlib.konflux.konflux_build_record import KonfluxFbcBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.model import Model
from artcommonlib.product_catalog import get_product_config

from pyartcd import locks
from pyartcd.fbc_util import extract_ocp_version_from_nvr, validate_layered_product_group_assembly
from pyartcd.git import GitRepository
from pyartcd.lp_shipment import (
    UNIFIED_MR_MARKER,
    add_superseded_mr_comment,
    close_superseded_shipment_mr,
    get_shipment_mr_url,
    reconcile_shipment_mr,
    validate_shipment_mr_for_operation,
)
from pyartcd.pipelines.merge_layered_fbc import MergeLayeredFbcPipeline
from pyartcd.pipelines.prepare_release_lp import PrepareReleaseLPPipeline, yaml


def latest_group_assembly(group: str, releases: dict) -> str:
    """Choose the highest numeric named assembly in this group's release train."""
    candidates = []
    for name in releases.get('releases', {}):
        name = str(name)
        try:
            validate_layered_product_group_assembly(group, name)
        except ValueError:
            continue
        candidates.append(name)
    if not candidates:
        raise click.ClickException(f'No numeric release assembly found for {group}')
    return max(candidates, key=lambda name: tuple(int(part) for part in name.split('.')))


class PrepareUnifiedReleaseLPPipeline(PrepareReleaseLPPipeline):
    def __init__(self, runtime, groups: str, **kwargs):
        super().__init__(runtime=runtime, group='', assembly='', **kwargs)
        self.groups = sorted({group.strip() for group in groups.split(',') if group.strip()})
        if not self.groups or any(not re.fullmatch(r'[\w][\w.-]*-\d+\.\d+', group) for group in self.groups):
            raise click.ClickException('Supply comma-separated build-data group names without assembly assignments')
        self.sources: list[PrepareReleaseLPPipeline] = []
        self.unified_results: list[dict] = []
        self.product = 'quay'

    @property
    def scopes(self) -> list[tuple[str, str, str]]:
        return [(self.product, source.group, source.assembly) for source in self.sources]

    async def _load_sources(self):
        for group in self.groups:
            source_runtime = copy.copy(self.runtime)
            source_runtime.working_dir = self._working_dir / 'sources' / group
            source_runtime.doozer_working = str(source_runtime.working_dir / 'doozer_working')
            source = PrepareReleaseLPPipeline(
                runtime=source_runtime,
                group=group,
                assembly='stream',
                build_data_repo_url=self.build_data_repo_url,
                create_mr=self.create_mr,
                jira_bugs=self.jira_bugs,
                target_release_date=self.target_release_date,
            )
            source._setup_working_dir()
            checkout = source._working_dir / 'ocp-build-data-read'
            repo = GitRepository(checkout, dry_run=self.dry_run)
            await repo.setup(self.build_data_repo_url)
            await repo.fetch_switch_branch(group)
            group_config = yaml.load((checkout / 'group.yml').read_text()) or {}
            if group_config.get('product') != self.product:
                raise click.ClickException(
                    f'Unified release preparation currently supports only Quay; invalid group {group}'
                )
            releases = yaml.load((checkout / 'releases.yml').read_text()) or {}
            assembly = latest_group_assembly(group, releases)
            source.assembly = assembly
            source._doozer_base_command = [
                arg.replace('--assembly=stream', f'--assembly={assembly}') for arg in source._doozer_base_command
            ]
            source._elliott_base_command = [
                arg.replace('--assembly=stream', f'--assembly={assembly}') for arg in source._elliott_base_command
            ]
            source._doozer_base_command.append(f'--working-dir={source_runtime.doozer_working}')
            source.product = self.product
            source._releases_config = releases
            source._configured_shipment_mr_url = get_shipment_mr_url(releases, assembly)
            source.shipment_data_repo = self.shipment_data_repo
            source.gitlab_token = self.gitlab_token
            source.job_url = self.job_url
            if not source._extract_operand_nvrs(releases['releases'][assembly]):
                raise ValueError(f'The latest assembly {group}/{assembly} has no pinned images')
            self.sources.append(source)
            self._logger.info('Selected %s/%s', group, assembly)
        self.group, self.assembly = self.sources[0].group, self.sources[0].assembly

    async def _verify_sources(self):
        """Check every source and pointer before performing any shipment writes."""
        for source in self.sources:
            checkout = source._working_dir / 'ocp-build-data-read'
            repo = GitRepository(checkout, dry_run=self.dry_run)
            await repo.setup(self.build_data_repo_url)
            await repo.fetch_switch_branch(source.group)
            current = yaml.load((checkout / 'releases.yml').read_text()) or {}
            expected_pins = assembly_image_pins(Model(source._releases_config), source.assembly)
            current_pins = assembly_image_pins(Model(current), source.assembly)
            if (
                current_pins != expected_pins
                or get_shipment_mr_url(current, source.assembly) != source._configured_shipment_mr_url
            ):
                raise RuntimeError(
                    f'Release inputs or shipment pointer changed concurrently for {source.group}/{source.assembly}'
                )
            if self.create_mr:
                await source._verify_assembly_shipment_url()

    async def _validate_candidate(self, url, source, *, reuse=False):
        return await validate_shipment_mr_for_operation(
            self._gitlab,
            self.shipment_data_repo,
            url,
            self.shipment_data_repo_pull_url,
            self.shipment_data_repo_push_url,
            self.product,
            source.group,
            source.assembly,
            allowed_states=('opened',) if reuse else ('opened', 'closed'),
            allow_active_stage=self.force and not reuse,
            scopes=self.scopes,
            require_all_scopes=reuse,
        )

    async def _preflight_mrs(self):
        pointers = {source._configured_shipment_mr_url for source in self.sources}
        if not self.force and pointers != {None}:
            if len(pointers) != 1 or None in pointers:
                raise click.ClickException('Use --force to consolidate existing source shipment MRs')
            url = next(iter(pointers))
            mr, _ = await self._validate_candidate(url, self.sources[0], reuse=True)
            if UNIFIED_MR_MARKER not in (mr.description or ''):
                raise click.ClickException('Use --force to replace an ordinary shipment MR with a unified shipment')
            return mr, {}
        predecessors = {}
        for source in self.sources:
            url = source._configured_shipment_mr_url
            if url and url not in predecessors:
                mr, _ = await self._validate_candidate(url, source)
                predecessors[url] = (mr, source)
        return None, predecessors

    async def _load_mr_approvers_from_group_config(self):
        approvers = {}
        for source in self.sources:
            for name, users in (await source._load_mr_approvers_from_group_config()).items():
                approvers.setdefault(name, set()).update(users)
        return {name: sorted(users) for name, users in approvers.items()}

    def _shipment_mr_text(self):
        releases = ', '.join(f'{source.group}/{source.assembly}' for source in self.sources)
        title = f'Draft: Unified shipment for {self.product}: {releases}'
        if self.target_release_date:
            title += f' (ship date: {self.target_release_date})'
        description = f'{UNIFIED_MR_MARKER}\n\nUnified shipment for {releases}.\n'
        if self.job_url:
            description += f'\nCreated by job: {self.job_url}\n'
        for result in self.unified_results:
            description += (
                f"\n- `{result['nvr']}`: `{result['pullspec']}`; "
                f"[build]({result['build_url']}), [provenance]({result['provenance_url']})"
            )
        return title, description

    async def _unified_shipment(self, target, sources, artifacts):
        builds = tuple(
            nvr
            for source in sources
            for nvr in artifacts[source.group][2]
            if extract_ocp_version_from_nvr(nvr) == target
        )
        merge_runtime = copy.copy(self.runtime)
        merge_runtime.working_dir = self._working_dir / 'unified' / target
        merge_runtime.working_dir.mkdir(parents=True, exist_ok=True)
        merge_runtime.doozer_working = str(merge_runtime.working_dir / 'doozer_working')
        result = await MergeLayeredFbcPipeline(
            runtime=merge_runtime,
            ocp_target_version=target,
            fbc_builds=builds,
            product=self.product,
            data_path=self.build_data_repo_url,
            kubeconfig=os.getenv('QUAY_KONFLUX_SA_KUBECONFIG'),
            registry_auth=os.getenv('QUAY_AUTH_FILE'),
        ).run()
        if not result.get('nvr') or extract_ocp_version_from_nvr(result['nvr']) != target:
            raise ValueError(f'Unified build returned no matching NVR for OCP {target}')
        if (
            not re.fullmatch(r'.+@sha256:[0-9a-f]{64}', result.get('pullspec') or '')
            or not result.get('build_url')
            or not result.get('provenance_url')
        ):
            raise ValueError(f'Unified build returned incomplete output for OCP {target}')
        provenance = result.get('provenance', {})
        inputs = provenance.get('inputs', [])
        if {item['nvr'] for item in inputs} != set(builds):
            raise ValueError(f'Unified provenance does not match prepared FBCs for OCP {target}')
        expected_bundles = {nvr for source in sources for nvr in artifacts[source.group][1]}
        if {item['bundle_nvr'] for item in inputs} != expected_bundles:
            raise ValueError(f'Unified provenance does not match prepared bundles for OCP {target}')
        execution = next((source for source in sources if source.group == provenance.get('execution_group')), None)
        if execution is None:
            raise ValueError('Unified build returned an unexpected execution group')
        db = KonfluxDb()
        db.bind(KonfluxFbcBuildRecord)
        records = await db.get_build_records_by_nvrs([result['nvr']], where={'group': execution.group}, strict=True)
        record = records[0]
        config = get_product_config(self.product)
        stage = config.unified_fbc_stage_release
        if (
            str(record.outcome) != 'success'
            or record.image_pullspec != result['pullspec']
            or set(record.bundle_nvrs) != expected_bundles
            or record.get_konflux_application_name() != stage.application
        ):
            raise ValueError('Unified build record does not match the prepared release')
        snapshots = await execution._create_snapshot([result['nvr']])
        if len(snapshots) != 1 or snapshots[0].spec.application != stage.application:
            raise ValueError('Expected one snapshot for the unified FBC application')
        components = snapshots[0].spec.components
        if (
            len(components) != 1
            or components[0].containerImage != result['pullspec']
            or components[0].name != f"{stage.application}-{target.replace('.', '-')}"
        ):
            raise ValueError('Unified snapshot does not match the built target and immutable image')
        shipment = execution._create_shipment_config('fbc', snapshots[0], unified_fbc=True)
        self.unified_results.append(result)
        return shipment

    async def _publish(self, shipments, existing_mr, predecessors):
        await self._verify_sources()
        if existing_mr:
            url = self.sources[0]._configured_shipment_mr_url
            existing_mr, _ = await self._validate_candidate(url, self.sources[0], reuse=True)
            await reconcile_shipment_mr(
                self.shipment_data_repo,
                existing_mr,
                shipments,
                include_fbc_ocp_version=True,
                dry_run=self.dry_run,
                scopes=self.scopes,
            )
            description = self._shipment_mr_text()[1]
            if existing_mr.description != description:
                existing_mr.description = description
                if not self.dry_run:
                    existing_mr.save()
            self.shipment_mr_url = url
            return
        for url, (_, source) in predecessors.items():
            await self._validate_candidate(url, source)
        url = await self._create_shipment_mr(shipments)
        if not url:
            raise RuntimeError('No unified shipment MR was created')
        completed = []
        try:
            for source in self.sources:
                await source._update_assembly_with_shipment_url(url)
                completed.append(source.group)
        except Exception as exc:
            pending = [source.group for source in self.sources if source.group not in completed]
            raise RuntimeError(
                f'Unified shipment {url} remains Draft. Updated groups: {completed}; pending: {pending}. '
                'No source MRs were closed. Rerun with --force after resolving the update failure.'
            ) from exc
        for old_url, (_, source) in predecessors.items():
            mr, _ = await self._validate_candidate(old_url, source)
            close_superseded_shipment_mr(mr, self.dry_run)
            add_superseded_mr_comment(
                self._gitlab,
                old_url,
                url,
                operation='prepare-release-lp-unified',
                run_url=self.job_url,
            )
        await self._set_shipment_mr_ready()

    async def _run_locked(self):
        await self._verify_sources()
        existing_mr, predecessors = await self._preflight_mrs() if self.create_mr else (None, {})
        targets = {}
        for source in self.sources:
            versions = await source._load_ocp_target_versions()
            if not versions or any(not re.fullmatch(r'\d+\.\d+', str(version)) for version in versions):
                raise ValueError(f'{source.group} must configure explicit OCP_TARGET_VERSIONS')
            for target in dict.fromkeys(versions):
                targets.setdefault(target, []).append(source)
        artifacts = {}
        for source in self.sources:
            artifacts[source.group] = await source._prepare_artifacts(
                source._releases_config['releases'][source.assembly]
            )
            if not self.dry_run and not artifacts[source.group][2]:
                raise ValueError(f'No operator FBCs were prepared for {source.group}')
        if self.dry_run:
            plan = {
                target: [{'group': source.group, 'assembly': source.assembly} for source in sources]
                for target, sources in targets.items()
            }
            self._logger.info('[DRY-RUN] Would merge these source releases per OCP target: %s', json.dumps(plan))
            (self._working_dir / 'unified-release-plan.json').write_text(json.dumps(plan, indent=2) + '\n')
            return
        shipments = {}
        for source in self.sources:
            operands, bundles, _, _ = artifacts[source.group]
            for shipment in (await source._create_image_shipments(operands, bundles)).values():
                shipments[f'image{len(shipments) + 1:02d}'] = shipment
        for counter, target in enumerate(sorted(targets, key=lambda value: tuple(map(int, value.split('.')))), 1):
            shipments[f'fbc{counter:02d}'] = await self._unified_shipment(target, targets[target], artifacts)
        if self.create_mr:
            await self._publish(shipments, existing_mr, predecessors)
        else:
            from datetime import datetime, timezone

            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            for kind, shipment in shipments.items():
                await self._write_shipment_file(kind, shipment, 'prod', timestamp)

    async def run(self):
        self._check_env_vars()
        self._setup_working_dir()
        config = get_product_config(self.product)
        if not config.unified_fbc_stage_release or not config.unified_fbc_prod_release:
            raise ValueError(f'No unified FBC release targets configured for {self.product}')
        await self._load_sources()
        if self.create_mr:
            await self._setup_shipment_repo()

        async def acquire(index):
            if index == len(self.sources):
                return await self._run_locked()
            source = self.sources[index]
            return await locks.run_with_lock(
                coro=acquire(index + 1),
                lock=locks.Lock.LAYERED_PRODUCT_SHIPMENT,
                lock_name=locks.Lock.LAYERED_PRODUCT_SHIPMENT.value.format(
                    group=source.group, assembly=source.assembly
                ),
            )

        if self.create_mr and not self.dry_run:
            await acquire(0)
        else:
            await self._run_locked()
