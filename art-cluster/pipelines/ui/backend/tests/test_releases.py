import base64
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import yaml
from pipeline_ui import app as api
from pipeline_ui import gitlab_read, releases
from pipeline_ui.cluster import UpstreamError
from pipeline_ui.gitlab_read import GitLabReader, GitLabReadError

NS = "art-acm-tenant"
OWNERS = {"acm-5.0": {NS}}
URL = 'https://art-pipelines-ui-art-pipelines-ui.apps.artc2023.pc3z.p1.openshiftapps.com'
FILE = 'shipment/rhacm2/acm-5.0/acm-5-0/prod/5.0.0.image.123.yaml'


def raw_run(name='prepare', uid='uid-1', status='True', **params):
    return {
        'metadata': {'name': name, 'namespace': NS, 'uid': uid, 'creationTimestamp': '2026-11-10T12:00:00Z'},
        'spec': {
            'pipelineRef': {'name': 'release-from-fbc'},
            'params': [
                {'name': key, 'value': value}
                for key, value in {'group': 'acm-5.0', 'assembly': '5.0.0', **params}.items()
            ],
        },
        'status': {'conditions': [{'type': 'Succeeded', 'status': status}]},
    }


def mr_fixture():
    return {
        'iid': 42,
        'project_id': 1,
        'source_project_id': 1,
        'sha': 'source',
        'target_branch': 'main',
        'web_url': 'https://gitlab.cee.redhat.com/art/shipments/-/merge_requests/42',
        'state': 'opened',
        'draft': False,
        'labels': [],
        'updated_at': '2026-11-10T12:00:00Z',
        'description': f'Created by job: {URL}/#/run/{NS}/prepare?tab=logs',
        'head_pipeline': {
            'id': 100,
            'project_id': 1,
            'sha': 'synthetic',
            'ref': 'refs/merge-requests/42/merge',
            'status': 'success',
        },
    }


def release_fixture(iid=42):
    return {
        'id': str(iid),
        'namespace': NS,
        'group': 'acm-5.0',
        'assembly': '5.0.0',
        'mr': {'iid': iid},
        'creator': {'namespace': NS, 'name': 'prepare', 'uid': None},
    }


async def async_items(items=(), error=None):
    if error:
        raise error
    for item in items:
        yield item


class PreparationTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_over_archive_and_archive_failure_keeps_live(self):
        gateway = MagicMock()
        gateway.list_records.side_effect = lambda *a, **kw: async_items([(None, raw_run(status='False'))])
        gateway.list_kube.side_effect = lambda *a, **kw: async_items([raw_run()])
        errors = []
        runs = await releases.preparation_runs(gateway, errors, NS)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]['status'], 'Succeeded')
        gateway.list_records.side_effect = lambda *a, **kw: async_items(error=httpx.ConnectError('internal'))
        self.assertEqual(len(await releases.preparation_runs(gateway, errors, NS)), 1)
        self.assertEqual(errors, [f'{NS}: preparation archive history is unavailable'])

    async def test_expired_cluster_token_is_not_hidden_as_partial_history(self):
        gateway = MagicMock()
        gateway.list_records.side_effect = lambda *a, **kw: async_items(error=UpstreamError(401, 'Expired'))
        with self.assertRaises(UpstreamError):
            await releases.preparation_runs(gateway, [], NS)

    def test_explicit_creator_link_and_retry_keep_original_failure(self):
        run = releases.preparation_run(raw_run(status='False'))
        retry = {
            **run,
            'uid': 'retry-uid',
            'name': 'retry',
            'rebuiltFrom': run['uid'],
            'created': '2026-11-10T13:00:00Z',
            'status': 'Succeeded',
        }
        result = releases.attach_preparation([release_fixture()], [retry, run])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['preparation']['status'], 'Succeeded')
        self.assertEqual([r['status'] for r in result[0]['preparation']['attempts']], ['Succeeded', 'Failed'])

    def test_matching_group_and_assembly_do_not_link_an_unrelated_attempt(self):
        run = releases.preparation_run(raw_run(name='other'))
        result = releases.attach_preparation([release_fixture()], [run])
        self.assertEqual(len(result), 2)
        self.assertEqual(next(r for r in result if r.get('mr'))['preparation']['status'], 'Unknown')

    def test_reused_names_duplicate_mr_claims_and_dry_runs_are_not_verified(self):
        run = releases.preparation_run(raw_run())
        reused = {**run, 'uid': 'other'}
        result = releases.attach_preparation([release_fixture()], [run, reused])
        self.assertEqual(next(r for r in result if r.get('mr'))['preparation']['status'], 'Unknown')
        result = releases.attach_preparation([release_fixture(), release_fixture(43)], [run])
        self.assertTrue(all(r['preparation']['status'] == 'Unknown' for r in result))
        result = releases.attach_preparation([release_fixture()], [{**run, 'dryRun': True}])
        self.assertEqual(next(r for r in result if r.get('mr'))['preparation']['status'], 'Unknown')

    def test_creator_urls_cannot_select_other_tenants_or_external_hosts(self):
        self.assertEqual(
            releases.creator_run(f'Created by job: {URL}/#/run/{NS}/prepare?uid=123'),
            {'namespace': NS, 'name': 'prepare', 'uid': '123'},
        )
        self.assertIsNone(
            releases.creator_run('Created by job: https://untrusted.example/#/run/art-acm-tenant/prepare')
        )
        self.assertEqual(
            releases.creator_run(f'Created by job: {URL}/#/run/art-other-tenant/prepare')['namespace'],
            'art-other-tenant',
        )

    def test_replacement_mr_keeps_its_explicit_creator_without_claiming_the_previous_mr(self):
        original = releases.preparation_run(raw_run())
        replacement = {
            **original,
            'uid': 'replacement-uid',
            'name': 'replacement',
            'rebuiltFrom': original['uid'],
            'created': '2026-11-10T13:00:00Z',
        }
        old_mr = release_fixture()
        new_mr = {**release_fixture(43), 'creator': {'namespace': NS, 'name': 'replacement', 'uid': None}}
        result = releases.attach_preparation([old_mr, new_mr], [replacement, original])
        self.assertEqual(len(result), 2)
        by_mr = {r['mr']['iid']: r for r in result}
        self.assertEqual([r['uid'] for r in by_mr[42]['preparation']['attempts']], ['uid-1'])
        self.assertEqual([r['uid'] for r in by_mr[43]['preparation']['attempts']], ['replacement-uid'])
        self.assertTrue(all(r['preparation']['status'] == 'Succeeded' for r in result))


class CatalogTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_capability_and_groups_are_dynamic_and_viewer_scoped(self):
        gateway = MagicMock()

        async def pipeline(namespace, name):
            if namespace == 'no-pipeline':
                raise UpstreamError(404, 'Not found')
            if namespace == 'denied':
                raise UpstreamError(403, 'Denied')
            return {'spec': {'params': [{'name': 'group', 'enum': ['future-1.0'], 'default': 'future-2.0'}]}}

        gateway.pipeline = AsyncMock(side_effect=pipeline)
        gateway.list_records.side_effect = lambda *a, **kw: async_items()
        gateway.list_kube.side_effect = lambda *a, **kw: async_items()
        errors = []
        catalog = await releases.release_catalog(gateway, ['future-tenant', 'no-pipeline', 'denied'], errors)
        self.assertEqual(errors, [])
        self.assertEqual(
            releases.group_owners(catalog), {'future-1.0': {'future-tenant'}, 'future-2.0': {'future-tenant'}}
        )
        self.assertEqual(gateway.list_records.call_args.args[0], 'future-tenant')
        self.assertEqual(gateway.list_records.call_count, 1)
        with self.assertRaises(UpstreamError):
            await releases.release_catalog(gateway, ['denied'], [], selected=True)

    async def test_run_parameters_cannot_expand_declared_group_ownership(self):
        gateway = MagicMock()
        gateway.pipeline = AsyncMock(return_value={'spec': {'params': [{'name': 'group', 'enum': ['acm-5.0']}]}})
        gateway.list_records.side_effect = lambda *a, **kw: async_items()
        gateway.list_kube.side_effect = lambda *a, **kw: async_items([raw_run(group='private-other-product-1.0')])
        catalog = await releases.release_catalog(gateway, [NS], [])
        self.assertEqual(releases.group_owners(catalog), {'acm-5.0': {NS}})
        self.assertEqual(len(catalog[0]['runs']), 1)

    def test_mixed_ambiguous_and_wrong_creator_tenants_are_withheld(self):
        records = [{'group': 'first'}, {'group': 'second'}]
        self.assertEqual(releases.scoped_shipments(records, {'first': {NS}}), [])
        self.assertEqual(releases.scoped_shipments(records[:1], {'first': {NS, 'other'}}), [])
        self.assertEqual(releases.scoped_shipments(records[:1], {'first': {NS}}, {'namespace': 'other'}), [])

    def test_preparation_does_not_cross_tenants_even_with_identical_names_and_versions(self):
        run = releases.preparation_run(raw_run())
        other = {**run, 'uid': 'other-uid', 'namespace': 'other-tenant'}
        result = releases.attach_preparation([release_fixture()], [other, run])
        linked = next(item for item in result if item.get('mr'))
        self.assertEqual([r['uid'] for r in linked['preparation']['attempts']], [run['uid']])
        self.assertEqual(next(item for item in result if not item.get('mr'))['namespace'], 'other-tenant')


class GitLabTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        gitlab_read._cache.clear()
        releases._shipment_cache.clear()
        self.enterContext(
            patch.dict(
                os.environ,
                {'RELEASES_GITLAB_URL': 'https://gitlab.cee.redhat.com', 'RELEASES_SHIPMENT_PROJECT': 'art/shipments'},
                clear=False,
            )
        )
        self.mr = mr_fixture()
        self.requests = []
        self.fail_child = False
        self.parents = ['target', 'source']

    def handler(self, request):
        self.requests.append(request)
        self.assertEqual(request.method, 'GET')
        path = request.url.path
        if path.endswith('/diffs'):
            data = [{'new_path': FILE, 'deleted_file': False}]
        elif '/repository/files/' in path:
            shipment = {
                'shipment': {
                    'metadata': {
                        'product': 'rhacm2',
                        'group': 'acm-5.0',
                        'assembly': '5.0.0',
                        'application': 'acm-5-0',
                    },
                    'environments': {},
                }
            }
            data = {'content': base64.b64encode(yaml.safe_dump(shipment).encode()).decode()}
        elif '/repository/commits/' in path:
            data = {'parent_ids': self.parents}
        elif path.endswith('/approval_state'):
            data = {'rules': [{'name': 'QE', 'approvals_required': 1, 'approved': False, 'approved_by': []}]}
        elif path.endswith('/merge_requests'):
            data = [self.mr]
        elif path.endswith('/merge_requests/42/pipelines'):
            data = [self.mr['head_pipeline']]
        elif path.endswith('/bridges'):
            data = [
                {
                    'id': 3,
                    'name': 'stage-job',
                    'status': 'success',
                    'downstream_pipeline': {'id': 101, 'project_id': 1},
                },
                {'id': 4, 'name': 'prod-job', 'status': 'manual'},
            ]
        elif path.endswith('/100/jobs'):
            data = [
                {'id': 1, 'name': 'validate-job', 'status': 'success'},
                {'id': 2, 'name': 'generate-config-job', 'status': 'success'},
            ]
        elif path.endswith('/101/jobs'):
            if self.fail_child:
                return httpx.Response(503)
            data = [
                {'id': 10, 'name': 'image-watch-stage', 'status': 'success'},
                {'id': 11, 'name': 'fbc-watch-stage', 'status': 'failed'},
            ]
        elif path.endswith('/merge_requests/42'):
            data = self.mr
        else:
            raise AssertionError(f'Unexpected GET {request.url}')
        return httpx.Response(200, json=data)

    def reader(self):
        return GitLabReader('service-token', transport=httpx.MockTransport(self.handler))

    async def test_discovery_verifies_files_and_synthetic_pipeline_source_parent(self):
        async with self.reader() as reader:
            errors = []
            result = await releases.discover_releases(reader, 30, errors, OWNERS)
        self.assertEqual(errors, [])
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0]['ci']['current'])
        self.assertEqual(result[0]['creator']['name'], 'prepare')

    async def test_child_fbc_failure_overrides_successful_bridge_and_approvals_are_real(self):
        async with self.reader() as reader:
            result = await releases.release_detail(reader, 42, 'source', OWNERS)
        self.assertTrue(result['current'])
        self.assertEqual(result['stages'][1]['status'], 'Failed')
        self.assertEqual(result['stages'][2]['status'], 'Manual action')
        self.assertEqual(result['approvals']['status'], 'Waiting')
        self.assertEqual(result['approvals']['rules'][0]['name'], 'QE')

    async def test_jobs_blocked_on_production_remain_pending_until_they_execute(self):
        def handler(request):
            if request.url.path.endswith('/100/jobs'):
                return httpx.Response(
                    200,
                    json=[
                        {'id': 1, 'name': 'validate-job', 'status': 'success'},
                        {'id': 2, 'name': 'generate-config-job', 'status': 'success'},
                        {'id': 66297737, 'name': 'etera-job', 'status': 'created', 'started_at': None},
                        {'id': 66297738, 'name': 'automerge-job', 'status': 'created', 'started_at': None},
                    ],
                )
            return self.handler(request)

        async with GitLabReader('pending-jobs-token', transport=httpx.MockTransport(handler)) as reader:
            result = await releases.release_detail(reader, 42, 'source', OWNERS)
        blocked = [job for job in result['jobs'] if job['name'] in ('etera-job', 'automerge-job')]
        self.assertEqual([job['status'] for job in blocked], ['Pending', 'Pending'])
        self.assertTrue(all(job['started'] is None for job in blocked))
        self.assertEqual(result['stages'][2]['status'], 'Manual action')

    def test_only_executing_gitlab_jobs_are_running_and_pending_is_not_skipped(self):
        for status in (
            'created',
            'pending',
            'waiting_for_resource',
            'waiting_for_callback',
            'scheduled',
            'preparing',
            'canceling',
        ):
            with self.subTest(status=status):
                self.assertNotEqual(releases.ci_status(status), 'Running')
        self.assertEqual(releases.ci_status('running'), 'Running')
        jobs = [{'id': 1, 'name': 'target', 'status': 'Pending'}]
        self.assertEqual(releases.aggregate_jobs(jobs), 'Pending')
        jobs.append({'id': 2, 'name': 'other-target', 'status': 'Running'})
        self.assertEqual(releases.aggregate_jobs(jobs), 'Running')

    async def test_old_success_is_historical_and_missing_child_is_not_green(self):
        self.parents = ['target', 'previous-source']
        self.fail_child = True
        async with self.reader() as reader:
            result = await releases.release_detail(reader, 42, 'source', OWNERS)
        self.assertFalse(result['current'])
        self.assertTrue(all(stage['status'] == 'Historical' for stage in result['stages']))
        self.assertTrue(result['issues'])

    async def test_pagination_and_error_redaction(self):
        def handler(request):
            page = request.url.params.get('page')
            return httpx.Response(200, json=[{'page': page}], headers={'x-next-page': '2' if page == '1' else ''})

        async with GitLabReader('pagination-token', transport=httpx.MockTransport(handler)) as reader:
            self.assertEqual(len(await reader.all('/projects/1/pipelines')), 2)
        async with GitLabReader(
            'private-secret', transport=httpx.MockTransport(lambda req: httpx.Response(403, text='private-secret'))
        ) as reader:
            with self.assertRaises(GitLabReadError) as error:
                await reader.object('/projects/1')
            self.assertNotIn('private-secret', str(error.exception))

    async def test_mr_change_during_read_bypasses_cache_and_invalidates_current_evidence(self):
        reads = 0

        def handler(request):
            nonlocal reads
            if request.url.path.endswith('/merge_requests/42'):
                reads += 1
                if reads > 1:
                    return httpx.Response(200, json={**self.mr, 'sha': 'new-source'})
            return self.handler(request)

        async with GitLabReader('race-token', transport=httpx.MockTransport(handler)) as reader:
            result = await releases.release_detail(reader, 42, 'source', OWNERS)
        self.assertEqual(reads, 2)
        self.assertEqual(result['approvals']['status'], 'Historical')
        self.assertEqual(result['approvals']['observedStatus'], 'Waiting')
        self.assertFalse(result['current'])
        self.assertTrue(all(stage['status'] == 'Historical' for stage in result['stages']))

    async def test_successful_bridge_without_child_evidence_does_not_establish_success(self):
        def handler(request):
            if request.url.path.endswith('/bridges'):
                return httpx.Response(200, json=[{'id': 3, 'name': 'stage-job', 'status': 'success'}])
            return self.handler(request)

        async with GitLabReader('missing-child-token', transport=httpx.MockTransport(handler)) as reader:
            result = await releases.release_detail(reader, 42, 'source', OWNERS)
        self.assertEqual(result['stages'][1]['status'], 'Unknown')
        self.assertTrue(any('downstream evidence' in message for message in result['issues']))

    async def test_unowned_shipment_is_rejected_before_approval_or_job_reads(self):
        async with GitLabReader('isolation-token', transport=httpx.MockTransport(self.handler)) as reader:
            with self.assertRaises(UpstreamError) as error:
                await releases.release_detail(reader, 42, 'source', {'logging-6.5': {'art-logging-tenant'}})
        self.assertEqual(error.exception.status, 404)
        self.assertFalse(any('/approval_state' in str(r.url) or '/jobs' in str(r.url) for r in self.requests))

    async def test_a_future_product_is_discovered_without_a_product_allowlist(self):
        namespace, group = 'art-future-tenant', 'future-product-1.0'
        filename = f'shipment/future-product/{group}/future-app/prod/1.0.0.image.123.yaml'

        def handler(request):
            if request.url.path.endswith('/diffs'):
                return httpx.Response(200, json=[{'new_path': filename}])
            if '/repository/files/' in request.url.path:
                data = {
                    'shipment': {
                        'metadata': {
                            'product': 'future-product',
                            'group': group,
                            'assembly': '1.0.0',
                            'application': 'future-app',
                        }
                    }
                }
                return httpx.Response(200, json={'content': base64.b64encode(yaml.safe_dump(data).encode()).decode()})
            if request.url.path.endswith('/merge_requests'):
                return httpx.Response(
                    200, json=[{**self.mr, 'description': f'Created by job: {URL}/#/run/{namespace}/prepare'}]
                )
            if request.url.path.endswith('/merge_requests/42'):
                return httpx.Response(
                    200, json={**self.mr, 'description': f'Created by job: {URL}/#/run/{namespace}/prepare'}
                )
            return self.handler(request)

        async with GitLabReader('future-token', transport=httpx.MockTransport(handler)) as reader:
            errors = []
            result = await releases.discover_releases(reader, 7, errors, {group: {namespace}})
        self.assertEqual(errors, [])
        self.assertEqual(result[0]['namespace'], namespace)
        self.assertEqual(result[0]['shipments'][0]['product'], 'future-product')

    def test_latest_job_attempt_recovers_failure_without_losing_other_targets(self):
        jobs = [
            {'id': 1, 'name': 'fbc-a', 'status': 'Failed'},
            {'id': 2, 'name': 'fbc-a', 'status': 'Succeeded'},
            {'id': 3, 'name': 'fbc-b', 'status': 'Failed'},
        ]
        self.assertEqual(releases.aggregate_jobs(jobs), 'Failed')
        jobs.append({'id': 4, 'name': 'fbc-b', 'status': 'Succeeded'})
        self.assertEqual(releases.aggregate_jobs(jobs), 'Succeeded')
        self.assertEqual(releases.aggregate_jobs([]), 'Unknown')


class ReleaseAccessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateway = MagicMock()
        self.gateway.__aenter__.return_value = self.gateway
        self.gateway.pipeline = AsyncMock(return_value={'spec': {'params': [{'name': 'group', 'enum': ['acm-5.0']}]}})
        self.gateway.list_records.side_effect = lambda *a, **kw: async_items()
        self.gateway.list_kube.side_effect = lambda *a, **kw: async_items([raw_run()])
        self.enterContext(patch.object(api, 'Gateway', return_value=self.gateway))
        self.enterContext(patch.object(api, 'NAMESPACES', (NS, 'art-other-tenant')))
        self.enterContext(patch.object(api, 'service_token', return_value='service-token'))
        self.reader = self.enterContext(patch.object(api, 'GitLabReader'))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api.app),
            base_url='https://ui.example',
            headers={'x-forwarded-access-token': 'viewer'},
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_denied_viewer_cannot_use_service_gitlab_access(self):
        self.gateway.pipeline.side_effect = UpstreamError(403, 'Denied')
        for path in (f'/api/releases?namespace={NS}', f'/api/releases/mrs/42?sha=source&namespace={NS}'):
            self.assertEqual((await self.client.get(path)).status_code, 403)
        self.reader.assert_not_called()
        response = await self.client.get('/api/releases')
        self.assertEqual(response.json()['items'], [])

    async def test_missing_authentication_and_read_only_routes(self):
        del self.client.headers['x-forwarded-access-token']
        self.assertEqual((await self.client.get('/api/releases')).status_code, 401)
        self.assertEqual((await self.client.post('/api/releases')).status_code, 405)
        self.reader.assert_not_called()

    async def test_unconfigured_gitlab_preserves_preparation_and_other_tenants_do_not_query_gitlab(self):
        with patch.object(api, 'service_token', return_value=''):
            response = await self.client.get(f'/api/releases?namespace={NS}')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['items'][0]['preparation']['status'], 'Succeeded')
        self.assertIn('not configured', response.json()['errors'][0])
        self.gateway.pipeline.side_effect = UpstreamError(404, 'Not found')
        response = await self.client.get('/api/releases?namespace=art-other-tenant')
        self.assertEqual(response.json()['items'], [])
        self.assertEqual(response.json()['supportedNamespaces'], [])
        self.reader.assert_not_called()


if __name__ == '__main__':
    unittest.main()
