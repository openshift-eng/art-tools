from copy import deepcopy

import pytest
from pyartcd.lp_recovery_repair import plan_lineage_repairs

PREFIX = 'art.openshift.io/'


def run(name, pipeline, inputs='', parent=None, *, time='10', **params):
    return {
        'metadata': {
            'name': name,
            'uid': 'uid-' + name,
            'namespace': 'art-test-tenant',
            'creationTimestamp': f'2026-11-13T{time}:00:00Z',
            'labels': {PREFIX + 'parent-pipelinerun': parent} if parent else {},
            'annotations': {},
        },
        'spec': {
            'pipelineRef': {'name': pipeline},
            'params': [
                {'name': key, 'value': value}
                for key, value in {'group': 'test-1.0', 'assembly': 'stream', 'operator-nvrs': inputs, **params}.items()
            ],
        },
        'status': {
            'completionTime': f'2026-11-13T{time}:01:00Z',
            'conditions': [{'type': 'Succeeded', 'status': 'False'}],
        },
    }


def history():
    parent = run('images', 'build-layered-products', time='08')
    source = run('bundle', 'olm-bundle-konflux', 'op-a-1-1', parent='images')
    scan = run('scan', 'layered-products-scan', time='11')
    retry = run('retry', 'olm-bundle-konflux', 'op-a-1-1,op-b-1-1', time='12')
    retry['metadata']['annotations'] = {PREFIX + 'recovery-lineage': 'unresolved', PREFIX + 'recovery-scan': 'scan'}
    return [parent, source, scan, retry]


def test_repair_requires_full_source_coverage_and_preserves_input():
    runs = history()
    before = deepcopy(runs)
    plans, unresolved = plan_lineage_repairs(runs)
    assert not unresolved
    assert plans[0]['annotations'][PREFIX + 'rebuilt-from'] == 'uid-bundle'
    assert plans[0]['unmatched_nvrs'] == ['op-b-1-1']
    assert runs == before
    runs[1]['spec']['params'].append({'name': 'operator-nvrs', 'value': 'op-a-1-1,op-c-1-1'})
    assert not plan_lineage_repairs(runs)[0]


def test_repair_records_multiple_sources_without_inventing_a_single_parent():
    runs = history()
    runs.append(run('other-images', 'build-layered-products', time='08'))
    runs.append(run('other-bundle', 'olm-bundle-konflux', 'op-b-1-1', parent='other-images'))
    plans, _ = plan_lineage_repairs(runs)
    assert plans[0]['sources'] == ['bundle', 'other-bundle']
    assert plans[0]['annotations'][PREFIX + 'rebuilt-from-uids'] == '["uid-bundle", "uid-other-bundle"]'
    assert 'labels' not in plans[0]
    assert plans[0]['unmatched_nvrs'] == []


@pytest.mark.parametrize('field', ['scan', 'parent', 'namespace', 'group', 'time', 'dry-run', 'target', 'active'])
def test_repair_rejects_missing_or_mismatched_evidence(field):
    runs = history()
    if field == 'scan':
        runs.pop(2)
    elif field == 'parent':
        runs.pop(0)
    elif field == 'namespace':
        runs[1]['metadata']['namespace'] = 'art-other-tenant'
    elif field in ('group', 'dry-run', 'target'):
        key = 'ocp-target-version' if field == 'target' else field
        runs[1]['spec']['params'].append({'name': key, 'value': 'true' if field == 'dry-run' else 'other'})
    elif field == 'time':
        runs[1]['status']['completionTime'] = '2026-11-13T11:30:00Z'
    else:
        runs[1]['status']['conditions'][0]['status'] = 'Unknown'
    assert not plan_lineage_repairs(runs)[0]


def test_repair_rejects_ambiguous_source_and_reused_parent_name():
    for index in (0, 1, 2):
        runs = history()
        duplicate = deepcopy(runs[index])
        duplicate['metadata']['uid'] = 'different-uid'
        runs.append(duplicate)
        assert not plan_lineage_repairs(runs)[0]


def test_repair_is_idempotent_and_uses_latest_completed_source():
    runs = history()
    runs.append(run('newer', 'olm-bundle-konflux', 'op-a-1-1', parent='images', time='10'))
    runs[1]['metadata']['creationTimestamp'] = '2026-11-13T09:00:00Z'
    plans, _ = plan_lineage_repairs(runs)
    assert plans[0]['sources'] == ['newer']
    runs[3]['metadata']['annotations'].update(plans[0]['annotations'])
    assert plan_lineage_repairs(runs) == ([], [])
