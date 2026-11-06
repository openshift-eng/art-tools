import base64
import json
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from pyartcd.lp_recovery import PipelineRunLookup, RecoveryPlanner

NAMESPACE = 'art-test-tenant'
CONSOLE = 'https://console.example.com'


def run(name, pipeline='olm-bundle-konflux', *, parent=None, nvrs='op-1-1', target='', status='False', **params):
    return {
        'metadata': {
            'name': name,
            'uid': f'uid-{name}',
            'namespace': NAMESPACE,
            'creationTimestamp': '2026-10-28T10:00:00Z',
            'labels': {'art.openshift.io/parent-pipelinerun': parent} if parent else {},
        },
        'spec': {
            'pipelineRef': {'name': pipeline},
            'params': [
                {'name': key, 'value': value}
                for key, value in {
                    'group': 'test-1.0',
                    'assembly': 'stream',
                    'operator-nvrs': nvrs,
                    'ocp-target-version': target,
                    **params,
                }.items()
            ],
        },
        'status': {'conditions': [{'type': 'Succeeded', 'status': status}]},
    }


@pytest.fixture
def planner(monkeypatch):
    monkeypatch.setenv('TASKRUN_NAMESPACE', NAMESPACE)
    planner = RecoveryPlanner('test-1.0', 'stream')
    planner.lookup = MagicMock()
    yield planner
    planner.close()


def test_failed_fbc_keeps_bundle_parent_and_full_original_batch(planner):
    parent = run('bundle', status='True')
    original = run('fbc', 'build-fbc', parent='bundle', target='4.20', nvrs='op-1-1,other-2-1')
    planner.lookup.from_url.return_value = parent
    planner.lookup.children.return_value = [original]

    plans = planner.plan('build-fbc', {'ocp-target-version': '4.20'}, {'op-1-1': SimpleNamespace(art_job_url='url')})

    assert len(plans) == 1
    assert plans[0]['parent_run'] == parent
    assert plans[0]['rebuilt_from'] == original
    assert plans[0]['params']['operator-nvrs'] == 'op-1-1,other-2-1'
    assert plans[0]['use_current_parent'] is False


def test_bundle_recovery_uses_operator_parent(planner):
    parent = run('images', 'build-layered-products', status='True')
    planner.lookup.from_url.return_value = parent
    planner.lookup.children.return_value = []
    plans = planner.plan(
        'olm-bundle-konflux', {'force-release': 'true'}, {'op-1-1': SimpleNamespace(art_job_url='url')}
    )
    planner.lookup.from_url.assert_called_once_with('url', 'build-layered-products', 'test-1.0', 'stream', None)
    assert plans[0]['parent_run'] == parent
    assert plans[0]['rebuilt_from'] is None
    assert plans[0]['params']['force-release'] == 'true'


def test_unrelated_parents_and_separate_attempts_are_not_batched(planner):
    first = run('images-a', 'build-layered-products')
    second = run('images-b', 'build-layered-products')
    planner.lookup.from_url.side_effect = [first, first, second]
    planner.lookup.children.side_effect = [
        [run('bundle-a', parent='images-a', nvrs='a-1-1'), run('bundle-b', parent='images-a', nvrs='b-1-1')],
        [run('bundle-a', parent='images-a', nvrs='a-1-1'), run('bundle-b', parent='images-a', nvrs='b-1-1')],
        [],
    ]
    plans = planner.plan('olm-bundle-konflux', {}, dict.fromkeys(['a-1-1', 'b-1-1', 'c-1-1']))
    assert len(plans) == 3
    assert {plan['params']['operator-nvrs'] for plan in plans} == {'a-1-1', 'b-1-1', 'c-1-1'}


@pytest.mark.parametrize(
    'field,value', [('ocp-target-version', '4.19'), ('group', 'other-1.0'), ('assembly', 'test'), ('dry-run', 'true')]
)
def test_retry_source_must_match_context(planner, field, value):
    planner.lookup.from_url.return_value = run('bundle')
    original = run('fbc', 'build-fbc', target='4.20', **{field: value})
    planner.lookup.children.return_value = [original]
    plans = planner.plan('build-fbc', {'ocp-target-version': '4.20'}, {'op-1-1': None})
    assert plans[0]['rebuilt_from'] is None


def test_newest_retry_is_used_and_active_retry_is_not_duplicated(planner):
    parent = run('bundle')
    original = run('fbc', 'build-fbc', target='4.20')
    latest = run('retry', 'build-fbc', target='4.20')
    latest['metadata']['creationTimestamp'] = '2026-10-29T10:00:00Z'
    planner.lookup.from_url.return_value = parent
    planner.lookup.children.return_value = [latest, original]
    plans = planner.plan('build-fbc', {'ocp-target-version': '4.20'}, {'op-1-1': None})
    assert plans[0]['rebuilt_from'] == latest
    latest['status']['conditions'][0]['status'] = 'Unknown'
    assert planner.plan('build-fbc', {'ocp-target-version': '4.20'}, {'op-1-1': None}) == []


def test_active_parent_is_allowed_to_trigger_its_own_children(planner):
    planner.lookup.from_url.return_value = run('bundle', status='Unknown')
    assert planner.plan('build-fbc', {}, {'op-1-1': None}) == []
    planner.lookup.children.assert_not_called()


def test_batch_with_source_rebuild_is_deferred(planner):
    planner.lookup.from_url.return_value = run('bundle')
    planner.lookup.children.return_value = [run('fbc', 'build-fbc', nvrs='op-1-1,other-2-1')]
    assert planner.plan('build-fbc', {}, {'op-1-1': None}, {'other'}) == []


def test_full_batch_does_not_duplicate_an_active_sibling(planner):
    planner.lookup.from_url.return_value = run('bundle')
    planner.lookup.children.return_value = [
        run('failed', 'build-fbc', nvrs='op-1-1, other-2-1'),
        run('running', 'build-fbc', nvrs='other-2-1', status='Unknown'),
    ]
    assert planner.plan('build-fbc', {}, {'op-1-1': None}) == []


@pytest.mark.parametrize('lookup_failed', [True, False])
def test_unresolved_lineage_does_not_claim_the_scan_as_parent(planner, lookup_failed, caplog):
    if lookup_failed:
        planner.lookup.from_url.side_effect = RuntimeError('Results unavailable')
    else:
        planner.lookup.from_url.return_value = None
    plans = planner.plan('build-fbc', {}, {'op-1-1': None})
    assert plans[0]['parent_run'] is None
    assert plans[0]['rebuilt_from'] is None
    assert plans[0]['use_current_parent'] is False
    assert 'no verified' in caplog.text


def test_verified_parent_is_retained_when_child_lookup_fails(planner):
    parent = run('bundle')
    planner.lookup.from_url.return_value = parent
    planner.lookup.children.side_effect = RuntimeError('PipelineRun listing unavailable')
    plans = planner.plan('build-fbc', {}, {'op-1-1': None})
    assert plans[0]['parent_run'] == parent
    assert plans[0]['rebuilt_from'] is None


@pytest.fixture
def lookup(monkeypatch):
    monkeypatch.setenv('TASKRUN_NAMESPACE', NAMESPACE)
    monkeypatch.setenv('TEKTON_CONSOLE_URL', CONSOLE)
    lookup = PipelineRunLookup()
    lookup._oc = MagicMock()
    yield lookup
    lookup.close()


def url(name='images'):
    return f'{CONSOLE}/k8s/ns/{NAMESPACE}/tekton.dev~v1~PipelineRun/{name}'


def test_lookup_live_parent_and_caches_it(lookup):
    parent = run('images', 'build-layered-products')
    lookup._oc.return_value = json.dumps(parent)
    assert lookup.from_url(url(), 'build-layered-products', 'test-1.0', 'stream') == parent
    assert lookup.from_url(url(), 'build-layered-products', 'test-1.0', 'stream') == parent
    lookup._oc.assert_called_once()
    assert lookup.from_url(url(), 'olm-bundle-konflux', 'test-1.0', 'stream') is None
    assert lookup.from_url(url(), 'build-layered-products', 'other', 'stream') is None


@pytest.mark.parametrize(
    'invalid',
    [
        '',
        'n/a',
        'https://jenkins/job/1',
        url().replace(CONSOLE, 'https://another-cluster'),
        url().replace(NAMESPACE, 'another-tenant'),
    ],
)
def test_invalid_or_cross_cluster_urls_are_not_followed(lookup, invalid):
    assert lookup.from_url(invalid, 'build-layered-products', 'test-1.0', 'stream') is None
    lookup._oc.assert_not_called()


def test_archive_lookup_follows_pages_and_rejects_reused_parent_names(lookup):
    parent = run('images', 'build-layered-products')
    lookup._oc.return_value = ''
    requests = []

    def respond(request):
        requests.append(request)
        encoded = base64.b64encode(json.dumps(parent).encode()).decode()
        if len(requests) == 1:
            return httpx.Response(200, json={'records': [], 'next_page_token': 'page2'})
        assert request.url.params['page_token'] == 'page2'
        return httpx.Response(200, json={'records': [{'data': {'value': encoded}}]})

    lookup.__dict__['results_client'] = httpx.Client(base_url='https://results', transport=httpx.MockTransport(respond))
    assert lookup.from_url(url(), 'build-layered-products', 'test-1.0', 'stream') == parent
    assert len(requests) == 2
    lookup._runs.clear()
    lookup._archived = MagicMock(return_value=[parent, deepcopy(parent)])
    assert lookup.from_url(url(), 'build-layered-products', 'test-1.0', 'stream') is None


def test_children_combine_archive_with_live_and_validate_parent_namespace(lookup):
    parent = run('images', 'build-layered-products')
    archived = run('bundle', parent='images')
    live = deepcopy(archived)
    live['status']['conditions'][0]['status'] = 'True'
    foreign = run('foreign', parent='images')
    foreign['metadata']['namespace'] = 'another-tenant'
    unrelated = run('unrelated', parent='other')
    lookup._oc.return_value = json.dumps({'items': [live, foreign, unrelated]})
    lookup._archived = MagicMock(return_value=[archived])
    assert lookup.children(parent) == [live]
    assert lookup.children(parent) == [live]
    lookup._oc.assert_called_once()


def test_live_attempts_keep_their_parent_when_archive_is_unavailable(lookup, caplog):
    parent = run('images', 'build-layered-products')
    child = run('bundle', parent='images')
    lookup._oc.return_value = json.dumps({'items': [child]})
    lookup._archived = MagicMock(side_effect=httpx.ConnectError('Unavailable'))
    assert lookup.children(parent) == [child]
    assert 'using live runs' in caplog.text


def test_parent_name_reused_after_build_is_not_followed(lookup):
    parent = run('images', 'build-layered-products')
    lookup._oc.return_value = json.dumps(parent)
    assert (
        lookup.from_url(
            url(), 'build-layered-products', 'test-1.0', 'stream', datetime(2026, 10, 27, tzinfo=timezone.utc)
        )
        is None
    )
