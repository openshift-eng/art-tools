import asyncio
import json
from functools import wraps
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxFbcBuildRecord
from click.testing import CliRunner
from elliottlib.shipment_model import Snapshot, SnapshotSpec
from pyartcd.lp_shipment import UNIFIED_MR_MARKER, ShipmentMRProductionError
from pyartcd.pipelines.prepare_release_lp import PrepareReleaseLPPipeline, prepare_release_lp, yaml
from pyartcd.pipelines.prepare_unified_release_lp import PrepareUnifiedReleaseLPPipeline, latest_group_assembly
from pyartcd.runtime import Runtime

MODULE = 'pyartcd.pipelines.prepare_unified_release_lp'


def run_async(test):
    @wraps(test)
    def run(*args, **kwargs):
        return asyncio.run(test(*args, **kwargs))

    return run


def release(version, mr=None):
    return {
        'assembly': {
            'type': 'standard',
            'group': {'shipment': {'mr': mr}},
            'members': {
                'images': [
                    {
                        'distgit_key': 'quay-operator',
                        'metadata': {'is': {'nvr': f'quay-operator-container-{version}-1'}},
                    },
                ]
            },
        }
    }


@pytest.fixture
def runtime(tmp_path):
    return Runtime(
        {'build_config': {'ocp_build_data_repo_push_url': 'https://github.com/test/build-data'}},
        tmp_path,
        False,
    )


def test_latest_assembly_uses_numeric_release_train():
    definitions = {'releases': {name: {} for name in ['3.18.9', '3.18.10', 'stream', '3.19.99', '3.18.10-rc1']}}
    assert latest_group_assembly('quay-3.18', definitions) == '3.18.10'
    with pytest.raises(Exception, match='No numeric release'):
        latest_group_assembly('quay-3.17', definitions)


@pytest.mark.parametrize(
    'args, message',
    [
        (['--create-unified-fbcs'], 'requires --unified-fbc-groups'),
        (['--create-unified-fbcs', '--unified-fbc-groups', 'quay-3.18', '--group', 'quay-3.18'], 'cannot use'),
        (['--create-unified-fbcs', '--unified-fbc-groups', 'quay-3.18', '--assembly', '3.18.3'], 'cannot use'),
        (['--create-unified-fbcs', '--unified-fbc-groups', 'quay-3.18', '--refresh-fbcs'], 'cannot use'),
        (['--unified-fbc-groups', 'quay-3.18'], 'requires --create-unified-fbcs'),
        ([], 'requires --group and --assembly'),
    ],
)
def test_cli_rejects_conflicting_modes(runtime, args, message):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = CliRunner().invoke(prepare_release_lp, args, obj=runtime)
    finally:
        loop.close()
    assert result.exit_code != 0
    assert message in result.output


def test_cli_dispatches_unified_without_single_group_lock(runtime):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        with patch(f'{MODULE}.PrepareUnifiedReleaseLPPipeline') as pipeline:
            pipeline.return_value.run = AsyncMock()
            result = CliRunner().invoke(
                prepare_release_lp,
                ['--create-unified-fbcs', '--unified-fbc-groups', 'quay-3.18,quay-3.17'],
                obj=runtime,
            )
    finally:
        loop.close()
    assert result.exit_code == 0, result.exception
    pipeline.return_value.run.assert_awaited_once()
    assert pipeline.call_args.kwargs['groups'] == 'quay-3.18,quay-3.17'


@run_async
async def test_source_loading_selects_latest_and_isolates_workspaces(runtime):
    pipeline = PrepareUnifiedReleaseLPPipeline(runtime, ' quay-3.18,quay-3.17,quay-3.18 ')

    async def fetch(repo, group):
        repo._directory.mkdir(parents=True, exist_ok=True)
        version = group.removeprefix('quay-')
        yaml.dump({'product': 'quay'}, repo._directory / 'group.yml')
        yaml.dump(
            {'releases': {f'{version}.9': release(f'{version}.9'), f'{version}.10': release(f'{version}.10')}},
            repo._directory / 'releases.yml',
        )

    with (
        patch(f'{MODULE}.GitRepository.setup', new=AsyncMock()),
        patch(
            f'{MODULE}.GitRepository.fetch_switch_branch',
            new=fetch,
        ),
    ):
        await pipeline._load_sources()
    assert [(source.group, source.assembly) for source in pipeline.sources] == [
        ('quay-3.17', '3.17.10'),
        ('quay-3.18', '3.18.10'),
    ]
    assert len({source._working_dir for source in pipeline.sources}) == 2
    assert all(
        f'--working-dir={source.runtime.doozer_working}' in source._doozer_base_command for source in pipeline.sources
    )
    assert all(f'--assembly={source.assembly}' in source._doozer_base_command for source in pipeline.sources)
    assert all(
        source._extract_operand_nvrs(source._releases_config['releases'][source.assembly])
        for source in pipeline.sources
    )


@run_async
@pytest.mark.parametrize('product, empty', [('logging', False), ('quay', True)])
async def test_invalid_source_fails_before_building(runtime, product, empty):
    pipeline = PrepareUnifiedReleaseLPPipeline(runtime, 'quay-3.18')

    async def fetch(repo, _group):
        repo._directory.mkdir(parents=True, exist_ok=True)
        yaml.dump({'product': product}, repo._directory / 'group.yml')
        definition = release('3.18.3')
        if empty:
            definition['assembly']['members']['images'] = []
        yaml.dump({'releases': {'3.18.3': definition}}, repo._directory / 'releases.yml')

    with (
        patch(f'{MODULE}.GitRepository.setup', new=AsyncMock()),
        patch(
            f'{MODULE}.GitRepository.fetch_switch_branch',
            new=fetch,
        ),
        pytest.raises(Exception, match='only Quay|no pinned images'),
    ):
        await pipeline._load_sources()


@pytest.fixture
def pipeline(runtime):
    result = PrepareUnifiedReleaseLPPipeline(runtime, 'quay-3.18,quay-3.17')
    result._gitlab = Mock()
    for group, version, targets in [('quay-3.17', '3.17.7', ['4.21']), ('quay-3.18', '3.18.3', ['4.21', '4.22'])]:
        source = PrepareReleaseLPPipeline(runtime, group, version)
        source.product = 'quay'
        source._releases_config = {'releases': {version: release(version)}}
        source._load_ocp_target_versions = AsyncMock(return_value=targets)
        source._load_release_notes_template = Mock(return_value=None)
        source._prepare_artifacts = AsyncMock(
            return_value=(
                [f'quay-operator-container-{version}-1'],
                [f'{operator}-bundle-container-{version}-1' for operator in ['quay-operator', 'quay-bridge-operator']],
                [
                    f'{operator}-fbc-{version}-1.ocp{target}'
                    for target in targets
                    for operator in ['quay-operator', 'quay-bridge-operator']
                ],
                [],
            )
        )
        source._update_assembly_with_shipment_url = AsyncMock()

        async def snapshot(builds, source=source):
            app = 'quay-fbc-unified' if builds[0].startswith('quay-fbc-unified-') else source.group.replace('.', '-')
            components = []
            if app == 'quay-fbc-unified':
                target = builds[0].rsplit('ocp', 1)[1]
                components = [
                    {
                        'name': 'quay-fbc-unified-' + target.replace('.', '-'),
                        'containerImage': 'quay.io/test/fbc@sha256:' + 'a' * 64,
                        'source': {'git': {'url': 'https://github.com/test/fbc', 'revision': 'b' * 40}},
                    }
                ]
            return [Snapshot(spec=SnapshotSpec(application=app, components=components), nvrs=builds)]

        source._create_snapshot = AsyncMock(side_effect=snapshot)
        result.sources.append(source)
    result.group, result.assembly = result.sources[0].group, result.sources[0].assembly
    result._verify_sources = AsyncMock()
    result._preflight_mrs = AsyncMock(return_value=(None, {}))
    result._publish = AsyncMock()
    return result


def fake_merge_result(pipeline, target, builds):
    inputs = []
    for source in pipeline.sources:
        for fbc in builds:
            if f'-{source.assembly}-' in fbc:
                operator = fbc.split('-fbc-')[0]
                inputs.append({'nvr': fbc, 'bundle_nvr': f'{operator}-bundle-container-{source.assembly}-1'})
    groups = [source.group for source in pipeline.sources if any(f'-{source.assembly}-' in fbc for fbc in builds)]
    return {
        'nvr': f'quay-fbc-unified-{target}-1.ocp{target}',
        'pullspec': 'quay.io/test/fbc@sha256:' + 'a' * 64,
        'build_url': f'https://konflux/ns/art-quay-tenant/applications/quay-fbc-unified/pipelineruns/fbc-{target}',
        'provenance_url': 'https://github.com/test/fbc/blob/' + 'b' * 40 + '/unified-provenance.json',
        'provenance': {'execution_group': sorted(groups)[0], 'inputs': inputs},
    }


@run_async
async def test_combines_all_operators_per_target_and_ships_only_unified_fbcs(pipeline):
    results = {}

    def merger(**kwargs):
        target = kwargs['ocp_target_version']
        results[target] = fake_merge_result(pipeline, target, kwargs['fbc_builds'])
        return SimpleNamespace(run=AsyncMock(return_value=results[target]))

    async def records(nvrs, **kwargs):
        target = nvrs[0].rsplit('ocp', 1)[1]
        result = results[target]
        return [
            KonfluxFbcBuildRecord(
                nvr=nvrs[0],
                group=kwargs['where']['group'],
                outcome=KonfluxBuildOutcome.SUCCESS,
                image_pullspec=result['pullspec'],
                build_pipeline_url=result['build_url'],
                bundle_nvrs=[item['bundle_nvr'] for item in result['provenance']['inputs']],
            )
        ]

    pipeline.create_mr = True
    with patch(f'{MODULE}.MergeLayeredFbcPipeline', side_effect=merger) as merge, patch(f'{MODULE}.KonfluxDb') as db:
        db.return_value.get_build_records_by_nvrs = AsyncMock(side_effect=records)
        await pipeline._run_locked()
    assert merge.call_count == 2
    assert len(results['4.21']['provenance']['inputs']) == 4
    assert len(results['4.22']['provenance']['inputs']) == 2
    shipments = pipeline._publish.await_args.args[0]
    assert len(shipments) == 4
    fbc_shipments = [config.shipment for config in shipments.values() if config.shipment.metadata.fbc]
    assert {shipment.snapshot.nvrs[0] for shipment in fbc_shipments} == {
        'quay-fbc-unified-4.21-1.ocp4.21',
        'quay-fbc-unified-4.22-1.ocp4.22',
    }
    assert all(shipment.environments.stage.releasePlan == 'quay-fbc-stage-unified' for shipment in fbc_shipments)
    assert all(shipment.environments.prod.releasePlan == 'quay-fbc-prod-unified' for shipment in fbc_shipments)
    assert {(shipment.metadata.group, shipment.metadata.assembly) for shipment in fbc_shipments} == {
        ('quay-3.17', '3.17.7'),
        ('quay-3.18', '3.18.3'),
    }
    for source in pipeline.sources:
        operands, bundles, _, _ = source._prepare_artifacts.return_value
        assert source._create_snapshot.await_args_list[0].args[0] == operands + bundles


@run_async
async def test_dry_run_reports_union_without_merging_or_publishing(pipeline):
    pipeline.dry_run = True
    with patch(f'{MODULE}.MergeLayeredFbcPipeline') as merge:
        await pipeline._run_locked()
    merge.assert_not_called()
    pipeline._publish.assert_not_awaited()
    plan = json.loads((pipeline._working_dir / 'unified-release-plan.json').read_text())
    assert len(plan['4.21']) == 2
    assert len(plan['4.22']) == 1
    for source in pipeline.sources:
        source._create_snapshot.assert_not_awaited()
        source._update_assembly_with_shipment_url.assert_not_awaited()


@run_async
async def test_source_build_failure_never_publishes(pipeline):
    pipeline.sources[-1]._prepare_artifacts.side_effect = RuntimeError('bundle failed')
    with patch(f'{MODULE}.MergeLayeredFbcPipeline') as merge, pytest.raises(RuntimeError, match='bundle failed'):
        await pipeline._run_locked()
    merge.assert_not_called()
    pipeline._publish.assert_not_awaited()


@run_async
@pytest.mark.parametrize('field', ['nvr', 'pullspec', 'provenance', 'provenance_url'])
async def test_incomplete_unified_build_is_rejected(pipeline, field):
    source = pipeline.sources[-1]
    artifacts = {source.group: source._prepare_artifacts.return_value}
    builds = tuple(nvr for nvr in artifacts[source.group][2] if nvr.endswith('ocp4.22'))
    result = fake_merge_result(pipeline, '4.22', builds)
    del result[field]
    with patch(f'{MODULE}.MergeLayeredFbcPipeline') as merge:
        merge.return_value.run = AsyncMock(return_value=result)
        with pytest.raises(ValueError):
            await pipeline._unified_shipment('4.22', [source], artifacts)
    pipeline._publish.assert_not_awaited()


@run_async
async def test_first_consolidation_requires_force(pipeline):
    pipeline._preflight_mrs = PrepareUnifiedReleaseLPPipeline._preflight_mrs.__get__(pipeline)
    for index, source in enumerate(pipeline.sources):
        source._configured_shipment_mr_url = f'https://gitlab/mr/{index}'
    with pytest.raises(Exception, match='--force'):
        await pipeline._preflight_mrs()


@run_async
async def test_shared_mr_is_reused_without_force(pipeline):
    pipeline._preflight_mrs = PrepareUnifiedReleaseLPPipeline._preflight_mrs.__get__(pipeline)
    for source in pipeline.sources:
        source._configured_shipment_mr_url = 'https://gitlab/mr/1'
    mr = SimpleNamespace(description=UNIFIED_MR_MARKER)
    pipeline._validate_candidate = AsyncMock(return_value=(mr, None))
    assert await pipeline._preflight_mrs() == (mr, {})
    assert pipeline._validate_candidate.await_args.kwargs == {'reuse': True}


@run_async
async def test_force_deduplicates_predecessors_and_rejects_production(pipeline):
    pipeline._preflight_mrs = PrepareUnifiedReleaseLPPipeline._preflight_mrs.__get__(pipeline)
    pipeline.force = True
    for source in pipeline.sources:
        source._configured_shipment_mr_url = 'https://gitlab/mr/1'
    mr = Mock()
    pipeline._validate_candidate = AsyncMock(return_value=(mr, None))
    _, predecessors = await pipeline._preflight_mrs()
    assert len(predecessors) == 1
    pipeline._validate_candidate.assert_awaited_once()
    pipeline._validate_candidate.side_effect = ShipmentMRProductionError('production attempt')
    with pytest.raises(ShipmentMRProductionError):
        await pipeline._preflight_mrs()


@run_async
async def test_partial_pointer_update_keeps_draft_and_all_predecessors_open(pipeline):
    pipeline._publish = PrepareUnifiedReleaseLPPipeline._publish.__get__(pipeline)
    pipeline._create_shipment_mr = AsyncMock(return_value='https://gitlab/mr/new')
    pipeline._set_shipment_mr_ready = AsyncMock()
    pipeline._validate_candidate = AsyncMock(return_value=(Mock(), None))
    pipeline.sources[-1]._update_assembly_with_shipment_url.side_effect = RuntimeError('concurrent pointer change')
    with patch(f'{MODULE}.close_superseded_shipment_mr') as close:
        with pytest.raises(RuntimeError, match='Updated groups:.*quay-3.17.*pending:.*quay-3.18'):
            await pipeline._publish({}, None, {'https://gitlab/mr/old': (Mock(), pipeline.sources[0])})
    close.assert_not_called()
    pipeline._set_shipment_mr_ready.assert_not_awaited()


@run_async
async def test_all_pointers_updated_before_closing_sources_and_ready(pipeline):
    pipeline._publish = PrepareUnifiedReleaseLPPipeline._publish.__get__(pipeline)
    events = []
    pipeline._create_shipment_mr = AsyncMock(return_value='https://gitlab/mr/new')
    pipeline._set_shipment_mr_ready = AsyncMock(side_effect=lambda: events.append('ready'))
    pipeline._validate_candidate = AsyncMock(return_value=(Mock(), None))
    for source in pipeline.sources:
        source._update_assembly_with_shipment_url.side_effect = lambda _url: events.append('pointer')
    with (
        patch(f'{MODULE}.close_superseded_shipment_mr', side_effect=lambda *_: events.append('close')),
        patch(
            f'{MODULE}.add_superseded_mr_comment',
        ),
    ):
        await pipeline._publish({}, None, {'https://gitlab/mr/old': (Mock(), pipeline.sources[0])})
    assert events == ['pointer', 'pointer', 'close', 'ready']


@run_async
async def test_unified_run_locks_all_sources_in_sorted_order(pipeline):
    pipeline._check_env_vars = Mock()
    pipeline._setup_working_dir = Mock()
    pipeline._load_sources = AsyncMock()
    pipeline._setup_shipment_repo = AsyncMock()
    pipeline._run_locked = AsyncMock()
    pipeline.create_mr = True
    locks = []

    async def locked(coro, **kwargs):
        locks.append(kwargs['lock_name'])
        return await coro

    with patch(f'{MODULE}.locks.run_with_lock', side_effect=locked):
        await pipeline.run()
    assert locks == ['lock:layered-product-shipment:quay-3.17:3.17.7', 'lock:layered-product-shipment:quay-3.18:3.18.3']
    pipeline._run_locked.assert_awaited_once()


@run_async
@pytest.mark.parametrize('change', ['pin', 'pointer'])
async def test_changed_source_inputs_abort_before_shipment_writes(pipeline, change):
    pipeline._verify_sources = PrepareUnifiedReleaseLPPipeline._verify_sources.__get__(pipeline)

    async def fetch(repo, group):
        repo._directory.mkdir(parents=True, exist_ok=True)
        version = next(source.assembly for source in pipeline.sources if source.group == group)
        definition = release(version)
        if change == 'pin':
            definition['assembly']['members']['images'][0]['metadata']['is']['nvr'] = 'quay-operator-container-3.17.7-2'
        else:
            definition['assembly']['group']['shipment']['mr'] = 'https://gitlab/mr/concurrent'
        yaml.dump({'releases': {version: definition}}, repo._directory / 'releases.yml')

    with (
        patch(f'{MODULE}.GitRepository.setup', new=AsyncMock()),
        patch(
            f'{MODULE}.GitRepository.fetch_switch_branch',
            new=fetch,
        ),
        pytest.raises(RuntimeError, match='changed concurrently'),
    ):
        await pipeline._verify_sources()
    pipeline._publish.assert_not_awaited()


@run_async
async def test_reused_mr_reconciles_all_scopes_and_updates_provenance(pipeline):
    pipeline._publish = PrepareUnifiedReleaseLPPipeline._publish.__get__(pipeline)
    url = 'https://gitlab/mr/shared'
    for source in pipeline.sources:
        source._configured_shipment_mr_url = url
    mr = Mock(description=UNIFIED_MR_MARKER)
    pipeline._validate_candidate = AsyncMock(return_value=(mr, None))
    pipeline._create_shipment_mr = AsyncMock()
    pipeline.unified_results = [fake_merge_result(pipeline, '4.22', ('quay-operator-fbc-3.18.3-1.ocp4.22',))]
    with patch(f'{MODULE}.reconcile_shipment_mr', new=AsyncMock()) as reconcile:
        await pipeline._publish({'fbc01': Mock()}, mr, {})
    assert reconcile.await_args.kwargs['scopes'] == pipeline.scopes
    assert pipeline.unified_results[0]['provenance_url'] in mr.description
    assert 'quay-3.17/3.17.7' in mr.description and 'quay-3.18/3.18.3' in mr.description
    pipeline._create_shipment_mr.assert_not_awaited()
    for source in pipeline.sources:
        source._update_assembly_with_shipment_url.assert_not_awaited()


@run_async
@pytest.mark.parametrize('mismatch', ['record', 'snapshot'])
async def test_unified_build_cannot_ship_a_different_record_or_snapshot(pipeline, mismatch):
    source = pipeline.sources[-1]
    artifacts = {source.group: source._prepare_artifacts.return_value}
    builds = tuple(nvr for nvr in artifacts[source.group][2] if nvr.endswith('ocp4.22'))
    result = fake_merge_result(pipeline, '4.22', builds)
    record = Mock(
        outcome=KonfluxBuildOutcome.SUCCESS,
        image_pullspec=result['pullspec'],
        bundle_nvrs=[item['bundle_nvr'] for item in result['provenance']['inputs']],
    )
    record.get_konflux_application_name.return_value = 'quay-fbc-unified'
    if mismatch == 'record':
        record.image_pullspec = 'quay.io/test/fbc@sha256:' + 'c' * 64
    else:
        source._create_snapshot.side_effect = None
        source._create_snapshot.return_value = [
            Snapshot(
                spec=SnapshotSpec(application='quay-fbc-unified', components=[]),
                nvrs=[result['nvr']],
            )
        ]
    with patch(f'{MODULE}.MergeLayeredFbcPipeline') as merge, patch(f'{MODULE}.KonfluxDb') as db:
        merge.return_value.run = AsyncMock(return_value=result)
        db.return_value.get_build_records_by_nvrs = AsyncMock(return_value=[record])
        with pytest.raises(ValueError, match='does not match'):
            await pipeline._unified_shipment('4.22', [source], artifacts)
    assert pipeline.unified_results == []
