"""Focused tests for fresh unified shipment generation and bounded collision checks."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pyartcd.pipelines.merge_layered_fbc import FbcTargetPlan, MergeLayeredFbcPipeline
from pyartcd.unified_shipment import UNIFIED_LABEL, check_overlap, create_unified_shipment


def test_open_overlap_stops_without_scanning_pipeline_history():
    mr = SimpleNamespace(iid=1, source_project_id=9, source_branch='unified-fbc-mta-123', web_url='MR1')
    client, project = Mock(), Mock(path_with_namespace='art/shipments')
    project.mergerequests.get.return_value = mr
    client.list_merge_requests.return_value = [mr]
    with patch('pyartcd.unified_shipment.candidate_targets', return_value={'4.18', '4.19'}):
        with pytest.raises(RuntimeError, match='MR1'):
            check_overlap(client, project, 'mta', {'4.18'})
    project.pipelines.list.assert_not_called()
    assert client.list_merge_requests.call_args.kwargs['labels'] == [UNIFIED_LABEL]


def test_disjoint_target_and_other_product_do_not_block():
    mr = SimpleNamespace(iid=1, source_project_id=9, source_branch='unified-fbc-mta-123', web_url='MR1')
    other = SimpleNamespace(iid=2, source_branch='unified-fbc-coo-123')
    client, project = Mock(), Mock(path_with_namespace='art/shipments')
    project.mergerequests.get.return_value = mr
    project.pipelines.list.return_value = []
    client.list_merge_requests.return_value = [mr, other]
    with patch('pyartcd.unified_shipment.candidate_targets', return_value={'4.19'}) as targets:
        check_overlap(client, project, 'mta', {'4.18'})
    targets.assert_called_once()
    assert all(
        call.kwargs['status'] in ('created', 'waiting_for_resource', 'preparing', 'pending', 'running', 'scheduled')
        for call in project.pipelines.list.call_args_list
    )


def test_active_delivery_for_closed_mr_blocks():
    client, project = Mock(), Mock(path_with_namespace='art/shipments')
    client.list_merge_requests.return_value = []
    mr = SimpleNamespace(
        iid=7,
        source_project_id=9,
        source_branch='unified-fbc-mta-123',
        web_url='closedMR',
        labels=[UNIFIED_LABEL],
        state='closed',
    )
    project.mergerequests.get.return_value = mr
    project.pipelines.list.side_effect = lambda **kw: (
        [SimpleNamespace(id=8, ref='refs/merge-requests/7/merge', web_url='activePipeline', sha='a' * 40)]
        if kw['status'] == 'running'
        else []
    )
    with patch('pyartcd.unified_shipment.candidate_targets', return_value={'4.18'}):
        with pytest.raises(RuntimeError, match='activePipeline'):
            check_overlap(client, project, 'mta', {'4.18'})


@pytest.mark.parametrize('kwargs', [{}, {'fbc_builds': ('a',)}, {'assemblies': ('8.2.3',), 'skip_rebase': True}])
def test_generation_rejects_nonassembly_or_existing_source_modes(tmp_path, kwargs):
    with pytest.raises(ValueError, match='requires named assemblies'):
        MergeLayeredFbcPipeline(Mock(working_dir=tmp_path), create_mr=True, **kwargs)


def test_target_failure_never_calls_generator(tmp_path):
    runtime = Mock(working_dir=tmp_path, dry_run=False)
    p = MergeLayeredFbcPipeline(runtime, assemblies=('8.2.3',), product='mta', create_mr=True)
    plans = [FbcTargetPlan(t, {'input': {'group': 'mta-8.2', 'assembly': '8.2.3'}}) for t in ('4.18', '4.19')]
    workers = []
    for i, plan in enumerate(plans):
        worker = Mock(target=plan.target, assembly_inputs=plan.inputs)
        worker.runtime.working_dir = tmp_path / plan.target
        worker._resolve_groups = AsyncMock(return_value=['mta-8.2'])
        worker._run_build = (
            AsyncMock(return_value={'nvr': 'good'}) if not i else AsyncMock(side_effect=ValueError('bad target'))
        )
        workers.append(worker)
    p._resolve_assemblies = AsyncMock(return_value=plans)
    p._target_worker = Mock(side_effect=workers)
    with patch('pyartcd.unified_shipment.create_unified_shipment', new_callable=AsyncMock) as generate:
        with pytest.raises(RuntimeError, match='4.19'):
            asyncio.run(p._run_assembly_targets())
        generate.assert_not_awaited()
    assert all(w._run_build.await_count == 1 for w in workers)


def test_successful_targets_are_all_passed_to_generator(tmp_path):
    runtime = Mock(working_dir=tmp_path, dry_run=False)
    p = MergeLayeredFbcPipeline(runtime, assemblies=('8.2.3',), product='mta', create_mr=True)
    plans = [FbcTargetPlan('4.18', {'input': {'group': 'mta-8.2', 'assembly': '8.2.3'}})]
    worker = Mock(target='4.18', assembly_inputs=plans[0].inputs)
    worker.runtime.working_dir = tmp_path / 'target'
    worker._resolve_groups = AsyncMock(return_value=['mta-8.2'])
    worker._run_build = AsyncMock(return_value={'nvr': 'good'})
    p._resolve_assemblies = AsyncMock(return_value=plans)
    p._target_worker = Mock(return_value=worker)
    with patch(
        'pyartcd.unified_shipment.create_unified_shipment', new_callable=AsyncMock, return_value='newMR'
    ) as generate:
        result = asyncio.run(p._run_assembly_targets())
    assert result['shipment_mr'] == 'newMR'
    assert generate.await_args.args[1] == plans
    assert generate.await_args.args[2][0]['status'] == 'Succeeded'


def test_missing_build_metadata_prevents_mr_creation(tmp_path):
    runtime = Mock(working_dir=tmp_path, config={}, dry_run=False)
    pipeline = SimpleNamespace(runtime=runtime, product='mta')
    repo = Mock(_directory=tmp_path)
    repo.setup = AsyncMock()
    repo.fetch_switch_branch = AsyncMock()
    with (
        patch('pyartcd.unified_shipment.GitLabClient'),
        patch('pyartcd.unified_shipment.GitRepository', return_value=repo),
    ):
        with pytest.raises(ValueError, match='incomplete successful'):
            asyncio.run(create_unified_shipment(pipeline, [SimpleNamespace(target='4.18')], [{'status': 'Succeeded'}]))
    repo.commit_push.assert_not_called()


@pytest.mark.parametrize('failure', [None, 'provenance', 'rpa', 'overlap'])
def test_complete_publication_is_validated_and_locked(tmp_path, failure):
    revision = 'b' * 40
    pullspec = 'quay.io/example/fbc@sha256:' + 'a' * 64
    provenance = {
        'product': 'mta',
        'ocp_target_version': '4.18',
        'inputs': [
            {
                'group': 'mta-8.2',
                'assembly': '8.2.3',
                'assembly_group_revision': revision,
                'pullspec': pullspec,
            }
        ],
    }
    outcome = dict(
        status='Succeeded',
        nvr='mta-fbc-unified-4.18-20261123.ocp4.18',
        pullspec=pullspec,
        application='mta-fbc-unified',
        component='mta-fbc-unified-4-18',
        source_repo='https://github.com/openshift-priv/art-fbc',
        source_commit='c' * 40,
        build_url='https://konflux.example/build',
        provenance=provenance,
    )
    plan = FbcTargetPlan(
        '4.18', {'input': dict(group='mta-8.2', assembly='8.2.3', group_revision=revision, pullspec=pullspec)}
    )
    runtime = Mock(working_dir=tmp_path, config={}, dry_run=False)
    pipeline = SimpleNamespace(
        runtime=runtime,
        product='mta',
        data_path='https://github.com/openshift-eng/ocp-build-data',
        options={'--registry-auth': None},
    )
    repo = Mock(_directory=tmp_path)
    for name in ('setup', 'fetch_switch_branch', 'create_branch', 'commit_push'):
        setattr(repo, name, AsyncMock())
    client = Mock()
    project = Mock(id=11)
    client.get_project.return_value = project
    build_data = Mock()
    build_data.get_contents.return_value.decoded_content = (
        b'releases:\n  8.2.3:\n    assembly:\n      group:\n        shipment:\n'
        b'          mr: https://gitlab.example/art/shipments/-/merge_requests/1\n'
    )
    trace = []

    async def inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def locked(coro, *args, **kwargs):
        trace.append('locked')
        try:
            return await coro
        finally:
            trace.append('unlocked')

    def overlap(*args):
        assert trace == ['locked']
        if failure == 'overlap':
            raise RuntimeError('existing MR')

    async def create(*args):
        assert trace == ['locked']
        return SimpleNamespace(web_url='newMR')

    with (
        patch('pyartcd.unified_shipment.GitRepository', return_value=repo),
        patch('pyartcd.unified_shipment.GitLabClient.from_url', return_value=client),
        patch('pyartcd.unified_shipment.resolve_shipment_release_plans', return_value=('stage-plan', 'prod-plan')),
        patch('artcommonlib.github_auth.get_github_client_for_org') as github,
        patch(
            'pyartcd.unified_shipment.validate_unified_release',
            new_callable=AsyncMock,
            return_value={} if failure == 'provenance' else provenance,
        ),
        patch(
            'pyartcd.unified_shipment.validate_snapshot_against_rpa',
            new_callable=AsyncMock,
            side_effect=ValueError('RPA failure') if failure == 'rpa' else None,
        ),
        patch('pyartcd.unified_shipment.check_overlap', side_effect=overlap),
        patch('pyartcd.unified_shipment.exectools.to_thread', side_effect=inline),
        patch('pyartcd.locks.run_with_lock', side_effect=locked),
        patch('pyartcd.unified_shipment.create_shipment_mr_with_retry', side_effect=create) as mr,
    ):
        github.return_value.get_repo.return_value = build_data
        if failure:
            with pytest.raises((ValueError, RuntimeError)):
                asyncio.run(create_unified_shipment(pipeline, [plan], [outcome]))
            mr.assert_not_called()
            repo.commit_push.assert_not_awaited()
        else:
            assert asyncio.run(create_unified_shipment(pipeline, [plan], [outcome])) == 'newMR'
            repo.commit_push.assert_awaited_once()
            assert mr.await_args.args[1]['labels'] == [UNIFIED_LABEL]
            assert mr.await_args.args[1]['title'].startswith('Draft:')
            assert len(list((tmp_path / 'shipment').rglob('*.yaml'))) == 1
            assert trace == ['locked', 'unlocked']
