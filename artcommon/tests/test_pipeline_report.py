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


class Client:
    @staticmethod
    def resource_url(data):
        return 'https://konflux.example/ns/tenant/pipelineruns/' + data['metadata']['name']


def build(name='build-1', status='Unknown'):
    return {
        'kind': 'PipelineRun',
        'metadata': {'name': name, 'namespace': 'tenant'},
        'status': {'conditions': [{'type': 'Succeeded', 'status': status, 'reason': 'BuildResult'}]},
    }


class PipelineReportTests(IsolatedAsyncioTestCase):
    async def test_disabled_outside_art_tekton(self):
        with patch.dict(os.environ, {}, clear=True):
            reporter = get_reporter()
        reporter._command = AsyncMock()
        await reporter.component('image', 'operator').set_status('Pending')
        reporter._command.assert_not_awaited()

    async def test_reporter_shared_in_one_invocation(self):
        with patch.dict(
            os.environ,
            {'TASKRUN_NAME': 'task', 'TASKRUN_NAMESPACE': 'art-tenant', 'TEKTON_PIPELINERUN_NAME': 'art-run'},
        ):
            self.assertIs(get_reporter(), get_reporter())

    async def test_parallel_components_use_separate_annotations(self):
        reporter = MemoryReporter()
        await asyncio.gather(*(reporter.component('image', str(i)).set_status('Pending') for i in range(10)))
        self.assertEqual(len([k for k in reporter._annotations if k.startswith(COMPONENT_PREFIX)]), 10)
        for payload in reporter.patches:
            self.assertEqual(len([k for k in payload['metadata']['annotations'] if k.startswith(COMPONENT_PREFIX)]), 1)

    async def test_preparation_failure_without_pipeline_run(self):
        reporter = MemoryReporter()
        with self.assertRaisesRegex(RuntimeError, 'rebase failed'):
            async with reporter.component('image', 'operator').preparing():
                raise RuntimeError('rebase failed')
        self.assertEqual(reporter.entry()['status'], 'Failed')
        self.assertEqual(reporter.entry()['pipelineRuns'], [])

    async def test_retry_retains_failed_and_successful_links(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.observe(build(status='False'), Client(), attempt=1)
        await report.observe(build('build-2', 'True'), Client(), attempt=2)
        entry = reporter.entry()
        self.assertEqual(entry['status'], 'Succeeded')
        self.assertEqual([r['status'] for r in entry['pipelineRuns']], ['Failed', 'Succeeded'])
        self.assertEqual(
            [r['url'] for r in entry['pipelineRuns']],
            [
                'https://konflux.example/ns/tenant/pipelineruns/build-1',
                'https://konflux.example/ns/tenant/pipelineruns/build-2',
            ],
        )

    async def test_post_build_failure_does_not_change_native_success(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.observe(build(status='True'), Client(), attempt=1)
        await report.failed('EC or ITS or release failed')
        self.assertEqual(reporter.entry()['status'], 'Succeeded')
        self.assertNotIn('stages', reporter.entry())
        self.assertNotIn('resources', reporter.entry())

    async def test_wait_error_retains_link_without_inventing_native_failure(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.observe(build(), Client(), attempt=1)
        await report.failed('wait interrupted')
        self.assertEqual(reporter.entry()['status'], 'Unknown')
        self.assertEqual(len(reporter.entry()['pipelineRuns']), 1)

    async def test_second_process_preserves_build_attempts(self):
        reporter = MemoryReporter()
        await reporter.component('bundle', 'operator').observe(build(status='True'), Client(), attempt=1)
        later = MemoryReporter(reporter.run)
        await later.note('Waiting for FBC builds')
        await later.component('fbc', 'operator').set_status('Reused')
        self.assertEqual(later.entry('bundle')['status'], 'Succeeded')
        self.assertEqual(len(later.entry('bundle')['pipelineRuns']), 1)

    async def test_repeated_observation_does_not_patch(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.observe(build(status='True'), Client(), attempt=1)
        await report.observe(build(status='True'), Client(), attempt=1)
        self.assertEqual(len(reporter.patches), 1)

    async def test_reporting_errors_nonfatal(self):
        reporter = MemoryReporter()
        reporter._command = AsyncMock(side_effect=RuntimeError('forbidden'))
        await reporter.component('image', 'operator').set_status('Pending')
        self.assertTrue(reporter._partial)

    async def test_failed_patch_retries_identical_observation(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.set_status('Pending')
        command = reporter._command
        reporter._command = AsyncMock(side_effect=RuntimeError('temporary outage'))
        await report.observe(build(status='True'), Client(), attempt=1)
        reporter._command = command
        reporter._retry_after = 0
        await report.observe(build(status='True'), Client(), attempt=1)
        self.assertEqual(reporter.entry()['status'], 'Succeeded')
        self.assertTrue(json.loads(reporter.run['metadata']['annotations'][REPORT_ANNOTATION])['partial'])

    async def test_invalid_resource_does_not_escape_to_builder(self):
        reporter = MemoryReporter()
        await reporter.component('image', 'operator').observe(object(), Client(), attempt=1)
        self.assertTrue(reporter._partial)

    async def test_uid_guard_rejects_replaced_parent(self):
        reporter = MemoryReporter(uid='old-uid')
        await reporter.component('image', 'operator').set_status('Pending')
        self.assertEqual(reporter.patches, [])

    async def test_capacity_preserves_published_links(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator')
        await report.observe(build(), Client(), attempt=1)
        previous = copy.deepcopy(reporter.entry())
        with patch('artcommonlib.pipeline_report.REPORT_BUDGET', 1):
            await report.observe(build(status='True'), Client(), attempt=1)
        self.assertEqual(reporter.entry(), previous)
        self.assertTrue(json.loads(reporter.run['metadata']['annotations'][REPORT_ANNOTATION])['partial'])

    async def test_dry_run_has_no_fabricated_links(self):
        reporter = MemoryReporter()
        report = reporter.component('image', 'operator', dry_run=True)
        await report.set_status('Pending')
        await report.observe(build(status='True'), Client(), attempt=1)
        self.assertEqual(reporter.entry()['status'], 'DryRun')
        self.assertEqual(reporter.entry()['pipelineRuns'], [])

    async def test_reused_build_has_no_link_for_this_run(self):
        reporter = MemoryReporter()
        await reporter.component('bundle', 'operator').set_status('Reused', reason='Existing successful bundle reused')
        self.assertEqual(reporter.entry('bundle')['status'], 'Reused')
        self.assertEqual(reporter.entry('bundle')['pipelineRuns'], [])
