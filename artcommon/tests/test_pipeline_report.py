import asyncio
import copy
import json
import os
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from artcommonlib.pipeline_report import (
    COMPONENT_PREFIX,
    REPORT_ANNOTATION,
    PipelineReporter,
    component_annotation,
    get_reporter,
)


class MemoryReporter(PipelineReporter):
    def __init__(self, run=None, uid=''):
        super().__init__('art-tenant', 'art-run', uid)
        self.run = run if run is not None else {'metadata': {'uid': 'art-uid', 'annotations': {}}}
        self.patches = []

    async def _command(self, args):
        if args[0] == 'get':
            return json.dumps(self.run)
        payload = json.loads(args[-1])
        assert payload['metadata']['uid'] == self.run['metadata']['uid']
        self.patches.append(payload)
        self.run['metadata']['annotations'].update(payload['metadata']['annotations'])
        return '{}'

    def entry(self, kind='image', name='operator'):
        return json.loads(self.run['metadata']['annotations'][component_annotation(kind, name)])


class PipelineReportTests(IsolatedAsyncioTestCase):
    async def test_disabled_outside_art_tekton(self):
        with patch.dict(os.environ, {}, clear=True):
            reporter = get_reporter()
        reporter._command = AsyncMock()
        await reporter.component('image', 'operator').stage('build', 'Running')
        reporter._command.assert_not_awaited()

    async def test_reporter_shared_in_one_invocation(self):
        with patch.dict(
            os.environ,
            {'TASKRUN_NAME': 'task', 'TASKRUN_NAMESPACE': 'art-tenant', 'TEKTON_PIPELINERUN_NAME': 'art-run'},
        ):
            self.assertIs(get_reporter(), get_reporter())

    async def test_parallel_components_use_separate_atomic_annotations(self):
        reporter = MemoryReporter()
        await asyncio.gather(*(reporter.component('image', str(i)).stage('build', 'Running') for i in range(10)))
        self.assertEqual(len([k for k in reporter._annotations if k.startswith(COMPONENT_PREFIX)]), 10)
        for payload in reporter.patches:
            self.assertEqual(len([k for k in payload['metadata']['annotations'] if k.startswith(COMPONENT_PREFIX)]), 1)

    async def test_phase_error_retains_created_link(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        with self.assertRaisesRegex(RuntimeError, 'build failed'):
            async with report.phase('preparation'):
                await report.resource(
                    {'metadata': {'name': 'build-1', 'namespace': 'konflux'}},
                    'https://konflux.example/build-1',
                    role='build',
                    status='Running',
                    attempt=1,
                )
                raise RuntimeError('build failed')
        entry = reporter.entry()
        self.assertEqual(entry['stages']['preparation']['status'], 'Failed')
        self.assertEqual(entry['resources'][0]['url'], 'https://konflux.example/build-1')

    async def test_second_process_preserves_build_and_attempts(self):
        reporter = MemoryReporter()
        report = reporter.component('bundle', 'operator')
        await report.stage('build', 'Succeeded', operatorNvr='operator-1-1')
        for attempt in (1, 2):
            await report.resource(
                {'metadata': {'name': f'build-{attempt}'}},
                'https://konflux.example/run',
                role='build',
                status='Succeeded' if attempt == 2 else 'Failed',
                attempt=attempt,
            )
        later = MemoryReporter(reporter.run)
        await later.component('bundle', 'operator').stage('stage-release', 'Failed', category='release')
        entry = later.entry('bundle')
        self.assertEqual(entry['stages']['build']['status'], 'Succeeded')
        self.assertEqual([r['attempt'] for r in entry['resources']], [1, 2])

    async def test_stage_all_refreshes_after_subprocess(self):
        reporter = MemoryReporter()
        await reporter.note('Waiting for bundles')
        child = MemoryReporter(reporter.run)
        await child.component('bundle', 'operator').stage('build', 'Succeeded', operatorNvr='operator-1-1')
        await reporter.stage_all(
            'bundle', 'stage-release', 'Skipped', category='release', operator_nvrs=['operator-1-1']
        )
        self.assertEqual(reporter.entry('bundle')['stages']['stage-release']['status'], 'Skipped')

    async def test_repeated_observation_does_not_patch(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.stage('build', 'Succeeded')
        await report.stage('build', 'Succeeded')
        self.assertEqual(len(reporter.patches), 1)

    async def test_reporting_errors_nonfatal(self):
        reporter = MemoryReporter()
        reporter._command = AsyncMock(side_effect=RuntimeError('forbidden'))
        await reporter.component('image', 'operator').stage('build', 'Running')
        self.assertTrue(reporter._partial)

    async def test_failed_patch_is_retried_with_same_observation(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.stage('build', 'Running')
        command = reporter._command
        reporter._command = AsyncMock(side_effect=RuntimeError('temporary outage'))
        await report.stage('build', 'Succeeded')
        reporter._command = command
        reporter._retry_after = 0
        await report.stage('build', 'Succeeded')
        self.assertEqual(reporter.entry()['stages']['build']['status'], 'Succeeded')
        self.assertTrue(json.loads(reporter.run['metadata']['annotations'][REPORT_ANNOTATION])['partial'])

    async def test_invalid_resource_does_not_escape_to_builder(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.resource(object(), 'https://konflux.example/run', role='build')
        self.assertTrue(reporter._partial)

    async def test_uid_guard_rejects_replaced_parent(self):
        reporter = MemoryReporter(uid='old-uid')
        await reporter.component('image', 'operator').stage('build', 'Running')
        self.assertEqual(reporter.patches, [])

    async def test_capacity_preserves_previously_published_links(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.resource({'metadata': {'name': 'build-1'}}, 'https://konflux.example/run', role='build')
        previous = copy.deepcopy(reporter.entry())
        with patch('artcommonlib.pipeline_report.REPORT_BUDGET', 1):
            await report.stage('build', 'Succeeded')
        self.assertEqual(reporter.entry(), previous)
        marker = json.loads(reporter.run['metadata']['annotations'][REPORT_ANNOTATION])
        self.assertTrue(marker['partial'])

    async def test_dry_run_has_no_resource_links(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator', dry_run=True)
        await report.stage('build', 'Succeeded')
        await report.resource({'metadata': {'name': 'fake'}}, 'https://dry-run.invalid', role='build')
        self.assertEqual(reporter.entry()['stages']['build']['status'], 'DryRun')
        self.assertEqual(reporter.entry()['resources'], [])

    async def test_release_includes_managed_retries_and_legacy_references(self):
        reporter = MemoryReporter()
        report = reporter.component('bundle', 'operator')

        class Client:
            @staticmethod
            def resource_url(data):
                meta = data['metadata']
                return f"https://konflux.example/ns/{meta.get('namespace', 'tenant')}/{data['kind'].lower()}s/{meta['name']}"

        release = {
            'kind': 'Release',
            'metadata': {'name': 'release-1', 'namespace': 'tenant'},
            'status': {
                'conditions': [{'type': 'Released', 'status': 'True', 'reason': 'Succeeded'}],
                'managedPipelineAttempts': [
                    {'pipelineRun': 'managed/attempt-1', 'status': 'Failed'},
                    {'pipelineRun': 'managed/attempt-2', 'status': 'Succeeded'},
                ],
                'managedProcessing': {'pipelineRun': 'managed/attempt-2', 'completionTime': 'now'},
                'tenantProcessing': {'pipelineRun': 'tenant/tenant-run', 'completionTime': 'now'},
                'finalProcessing': {'pipelineRun': 'managed/final-run'},
                'collectorsProcessing': {'managedCollectorsProcessing': {'pipelineRun': 'managed/collector-run'}},
            },
        }
        await report.release(release, Client())
        resources = {r['name']: r for r in reporter.entry('bundle')['resources']}
        self.assertEqual(resources['attempt-1']['status'], 'Failed')
        self.assertEqual(resources['attempt-2']['status'], 'Succeeded')
        self.assertEqual(resources['tenant-run']['status'], 'Unknown')
        self.assertIn('/ns/managed/', resources['attempt-1']['url'])
        self.assertIn('?releaseName=release-1', resources['attempt-1']['url'])
        self.assertEqual(resources['collector-run']['role'], 'managed-collector')

    async def test_integration_tests_keep_optional_failure(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        client = type('Client', (), {'resource_url': staticmethod(lambda data: 'https://konflux.example/resource')})()
        snapshot = {
            'kind': 'Snapshot',
            'metadata': {
                'name': 'snapshot',
                'namespace': 'tenant',
                'annotations': {
                    'test.appstudio.openshift.io/status': json.dumps(
                        [
                            {'scenario': 'optional', 'status': 'TestFail', 'testPipelineRunName': 'optional-run'},
                            {'scenario': 'blocking', 'status': 'TestPassed', 'testPipelineRunName': 'blocking-run'},
                        ]
                    )
                },
            },
        }
        await report.integration_tests(snapshot, client, {'blocking'})
        stages = reporter.entry()['stages']
        self.assertFalse(stages['ITS: optional']['required'])
        self.assertEqual(stages['ITS: optional']['status'], 'Failed')
        self.assertTrue(stages['ITS: blocking']['required'])
