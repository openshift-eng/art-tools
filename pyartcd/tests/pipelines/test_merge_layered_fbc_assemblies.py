import asyncio
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pyartcd.pipelines.merge_layered_fbc import MergeLayeredFbcPipeline


def pipeline(tmp_path, target=None, assemblies=('3.18.5', '3.17.9'), **kwargs):
    runtime = Mock(working_dir=tmp_path, doozer_working=tmp_path / 'doozer', dry_run=True)
    return MergeLayeredFbcPipeline(runtime, target, assemblies=assemblies, **kwargs)


@pytest.fixture
def sources(monkeypatch):
    targets = {'quay-3.18': ['4.22', '4.21'], 'quay-3.17': ['4.21', '4.20']}
    versions = {'quay-3.18': '3.18.5', 'quay-3.17': '3.17.9'}
    revisions = {group: hashlib.sha1(group.encode()).hexdigest() for group in targets}
    documents = {}
    for group, assembly in versions.items():
        pin = f'quay-operator-container-{assembly}-1'
        records = {
            target: {
                'quay-operator': {
                    'nvr': f'quay-operator-fbc-{assembly}-1.ocp{target}',
                    'pullspec': 'quay.io/test/fbc@sha256:'
                    + hashlib.sha256(f'{assembly}/{target}'.encode()).hexdigest(),
                    'operand_nvrs': [pin],
                }
            }
            for target in targets[group]
        }
        documents[group] = {
            'releases': {
                assembly: {
                    'assembly': {
                        'members': {'images': [{'distgit_key': 'quay-operator', 'metadata': {'is': {'nvr': pin}}}]},
                        'group': {'fbc_builds': records},
                    }
                }
            }
        }
    repo = Mock()
    repo.get_branch.side_effect = lambda group: SimpleNamespace(commit=SimpleNamespace(sha=revisions[group]))
    by_revision = {revision: group for group, revision in revisions.items()}
    repo.get_contents.side_effect = lambda path, ref: SimpleNamespace(
        decoded_content=json.dumps(documents[by_revision[ref]]).encode(),
    )
    client = Mock()
    client.get_repo.return_value = repo
    monkeypatch.setattr('pyartcd.pipelines.merge_layered_fbc.get_github_client_for_org', lambda owner: client)

    async def group_config(group, assembly, **kwargs):
        assert assembly == versions[group]
        assert kwargs['doozer_data_gitref'] == revisions[group]
        return {'product': 'quay', 'OCP_TARGET_VERSIONS': targets[group]}

    config = AsyncMock(side_effect=group_config)
    monkeypatch.setattr('pyartcd.pipelines.merge_layered_fbc.load_group_config', config)
    return SimpleNamespace(repo=repo, documents=documents, targets=targets, revisions=revisions)


def test_discovers_union_and_selects_only_compatible_assemblies(tmp_path, sources):
    p = pipeline(tmp_path)
    plans = asyncio.run(p._resolve_assemblies())
    assert [plan.target for plan in plans] == ['4.20', '4.21', '4.22']
    assert [{s['assembly'] for s in plan.inputs.values()} for plan in plans] == [
        {'3.17.9'},
        {'3.17.9', '3.18.5'},
        {'3.18.5'},
    ]
    assert sources.repo.get_branch.call_count == 2
    assert sources.repo.get_contents.call_count == 2
    for plan in plans:
        for record in plan.inputs.values():
            assert record['group_revision'] == sources.revisions[record['group']]


def test_optional_filter_selects_only_matching_groups(tmp_path, sources):
    plans = asyncio.run(pipeline(tmp_path, target='4.22')._resolve_assemblies())
    assert len(plans) == 1
    assert {s['assembly'] for s in plans[0].inputs.values()} == {'3.18.5'}


@pytest.mark.parametrize('ignore_patch_version', [False, True])
def test_assembly_consumer_honors_effective_group_patch_policy(tmp_path, sources, monkeypatch, ignore_patch_version):
    """Load cross-patch records only when the selected build-data revision enables that policy."""
    record = sources.documents['quay-3.18']['releases']['3.18.5']['assembly']['group']['fbc_builds']['4.22'][
        'quay-operator'
    ]
    record['nvr'] = record['nvr'].replace('3.18.5', '3.18.4')
    config = AsyncMock(
        return_value={'product': 'quay', 'OCP_TARGET_VERSIONS': ['4.22'], 'ignore_patch_version': ignore_patch_version}
    )
    monkeypatch.setattr('pyartcd.pipelines.merge_layered_fbc.load_group_config', config)
    consumer = pipeline(tmp_path, assemblies=('3.18.5',))
    if not ignore_patch_version:
        with pytest.raises(ValueError, match='do not match'):
            asyncio.run(consumer._resolve_assemblies())
    else:
        plans = asyncio.run(consumer._resolve_assemblies())
        assert list(plans[0].inputs) == [record['nvr']]
    assert config.await_args.kwargs['doozer_data_gitref'] == sources.revisions['quay-3.18']


def test_numeric_target_ordering(tmp_path, sources):
    sources.targets['quay-3.18'] = ['4.10', '4.9']
    records = sources.documents['quay-3.18']['releases']['3.18.5']['assembly']['group']['fbc_builds']
    example = records['4.22']['quay-operator']
    for target in ('4.10', '4.9'):
        records[target] = {'quay-operator': {**example, 'nvr': example['nvr'].replace('ocp4.22', 'ocp' + target)}}
    plans = asyncio.run(pipeline(tmp_path, assemblies=('3.18.5',))._resolve_assemblies())
    assert [p.target for p in plans] == ['4.9', '4.10']


@pytest.mark.parametrize('assemblies', [('quay-3.18',), ('3.18',), ('3.18.5', '3.18.6')])
def test_rejects_invalid_or_ambiguous_versions_before_remote_reads(tmp_path, sources, assemblies):
    with pytest.raises(ValueError, match='format|Multiple assemblies'):
        asyncio.run(pipeline(tmp_path, assemblies=assemblies)._resolve_assemblies())
    sources.repo.get_branch.assert_not_called()


@pytest.mark.parametrize('options', [{'fbc_branch': 'custom'}, {'skip_rebase': True}, {'fbc_builds': ('nvr',)}])
def test_rejects_assembly_inputs_with_existing_source_modes(tmp_path, options):
    with pytest.raises(ValueError, match='cannot be combined'):
        asyncio.run(pipeline(tmp_path, **options)._resolve_assemblies())


@pytest.mark.parametrize('failure', ['missing', 'stale', 'config', 'unknown_assembly'])
def test_preflight_failures_start_no_builds_and_remove_stale_result(tmp_path, sources, failure):
    definition = sources.documents['quay-3.18']['releases']['3.18.5']['assembly']
    if failure == 'missing':
        del definition['group']['fbc_builds']['4.21']
    elif failure == 'stale':
        definition['members']['images'][0]['metadata']['is']['nvr'] += '.changed'
    elif failure == 'config':
        sources.targets['quay-3.18'] = []
    else:
        sources.documents['quay-3.18']['releases'] = {}
    p = pipeline(tmp_path)
    p.output_file.write_text('{"old": true}')
    with patch.object(MergeLayeredFbcPipeline, '_build', new=AsyncMock()) as build:
        with pytest.raises(ValueError):
            asyncio.run(p.run())
    build.assert_not_called()
    assert not p.output_file.exists()


def test_successful_record_preflight_finishes_for_all_targets_before_building(tmp_path, sources):
    checks = []

    async def resolve(worker):
        checks.append(worker.target)
        if worker.target == '4.22':
            raise ValueError('No successful FBC record')
        return sorted({s['group'] for s in worker.assembly_inputs.values()})

    with (
        patch.object(MergeLayeredFbcPipeline, '_resolve_groups', resolve),
        patch.object(MergeLayeredFbcPipeline, '_build', new=AsyncMock()) as build,
    ):
        with pytest.raises(ValueError, match='successful FBC'):
            asyncio.run(pipeline(tmp_path).run())
    assert checks == ['4.20', '4.21', '4.22']
    build.assert_not_called()


def test_workers_have_isolated_files_pinned_configuration_and_per_target_locks(tmp_path, sources):
    observed = []

    async def resolve(worker):
        return sorted({s['group'] for s in worker.assembly_inputs.values()})

    async def command(args, **kwargs):
        output = next(a.split('=', 1)[1] for a in args if a.startswith('--output-file='))
        target = next(a.split('=', 1)[1] for a in args if a.startswith('--major-minor='))
        from pathlib import Path

        Path(output).write_text(
            json.dumps({'nvr': 'unified-' + target, 'pullspec': 'image@sha256:abc', 'build_url': 'url'})
        )
        observed.append((args, kwargs['env']))

    async def locked(**kwargs):
        return await kwargs['coro']

    p = pipeline(tmp_path)
    p.runtime.dry_run = False
    with (
        patch.object(MergeLayeredFbcPipeline, '_resolve_groups', resolve),
        patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock(side_effect=command)),
        patch('pyartcd.pipelines.merge_layered_fbc.locks.run_with_lock', new=AsyncMock(side_effect=locked)) as locks,
    ):
        result = asyncio.run(p.run())
    assert list(result['results']) == ['4.20', '4.21', '4.22']
    assert result['failed_targets'] == []
    assert {c.kwargs['lock_name'] for c in locks.call_args_list} == {
        f'lock:fbc-merge:quay:ocp-{target}' for target in result['results']
    }
    for prefix in ('--working-dir=', '--output-file='):
        assert len({next(a for a in args if a.startswith(prefix)) for args, _ in observed}) == 3
    assert len({env['DOOZER_CACHE_DIR'] for _, env in observed}) == 3
    input_files = [args[args.index('--assembly-inputs-file') + 1] for args, _ in observed]
    assert len(set(input_files)) == 3
    for (args, _), filename in zip(observed, input_files):
        from pathlib import Path

        selections = json.loads(Path(filename).read_text())
        assert next(a for a in args if a.startswith('--group=')).split('@')[1] in sources.revisions.values()
        assert all(record['group_revision'] == sources.revisions[record['group']] for record in selections.values())
    assert json.loads(p.output_file.read_text()) == result


@pytest.mark.parametrize('concurrency,expected_peak', [(None, 1), (2, 2)])
def test_failure_preserves_successes_waits_for_remaining_targets_and_limits_concurrency(
    tmp_path, sources, concurrency, expected_peak
):
    active = 0
    peak = 0
    completed = []

    async def resolve(worker):
        return sorted({s['group'] for s in worker.assembly_inputs.values()})

    async def build(worker):
        nonlocal active, peak
        active += 1
        peak = max(active, peak)
        await asyncio.sleep(0)
        active -= 1
        completed.append(worker.target)
        if worker.target == '4.21':
            raise RuntimeError('target failed')
        return {'nvr': 'unified-' + worker.target, 'dry_run': True}

    p = pipeline(tmp_path, **({} if concurrency is None else {'max_concurrent_targets': concurrency}))
    with (
        patch.object(MergeLayeredFbcPipeline, '_resolve_groups', resolve),
        patch.object(MergeLayeredFbcPipeline, '_build', build),
        patch('pyartcd.pipelines.merge_layered_fbc.locks.run_with_lock') as locks,
    ):
        with pytest.raises(RuntimeError, match='4.21'):
            asyncio.run(p.run())
    locks.assert_not_called()
    assert peak == expected_peak
    assert set(completed) == {'4.20', '4.21', '4.22'}
    result = json.loads(p.output_file.read_text())
    assert result['failed_targets'] == ['4.21']
    assert result['results']['4.20']['status'] == 'Succeeded'
    assert result['results']['4.22']['nvr'] == 'unified-4.22'
    assert result['results']['4.21']['error'] == 'target failed'


def test_unsupported_filter_starts_no_builds(tmp_path, sources):
    with pytest.raises(ValueError, match='No selected assembly supports'):
        asyncio.run(pipeline(tmp_path, target='5.0').run())


def test_rejects_record_resolved_to_another_group(tmp_path):
    p = pipeline(tmp_path, target='4.22')
    nvr = 'quay-operator-fbc-3.18.5-1.ocp4.22'
    p.builds = (nvr,)
    p.assembly_inputs = {nvr: {'group': 'quay-3.18', 'operator': 'quay-operator'}}

    async def records(**kwargs):
        yield SimpleNamespace(nvr=nvr, group='quay-3.17', name='quay-operator-fbc')

    with patch('pyartcd.pipelines.merge_layered_fbc.KonfluxDb') as db:
        db.return_value.search_builds_by_fields = records
        with pytest.raises(ValueError, match='recorded assembly FBC identity'):
            asyncio.run(p._resolve_groups())


@pytest.mark.parametrize('value', [0, -1])
def test_rejects_invalid_target_concurrency(tmp_path, value):
    with pytest.raises(ValueError, match='at least 1'):
        pipeline(tmp_path, max_concurrent_targets=value)


def test_cli_target_concurrency_defaults_to_serial_and_rejects_zero():
    from click import BadParameter
    from pyartcd.cli import cli

    command = cli.commands['lp-unified-fbc']
    with command.make_context('lp-unified-fbc', []) as context:
        assert context.params['max_concurrent_targets'] == 1
    with command.make_context('lp-unified-fbc', ['--max-concurrent-targets=2']) as context:
        assert context.params['max_concurrent_targets'] == 2
    with pytest.raises(BadParameter):
        command.make_context('lp-unified-fbc', ['--max-concurrent-targets=0'])
