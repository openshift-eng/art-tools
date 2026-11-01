import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.variants import BuildVariant
from doozerlib.cli.images_health import ConcernCode
from pyartcd.fbc_health import FbcHealthReport
from pyartcd.pipelines.images_health import ImagesHealthPipeline
from pyartcd.pipelines.layered_products_image_health import (
    LayeredProductHealthReport,
    LayeredProductsImageHealthPipeline,
)
from pyartcd.slack import SlackClient

RAW_URL = 'https://dummy-user:dummy-password@execution.invalid/job?token=dummy-secret'


class TestIncompleteHealthPrivacy(unittest.IsolatedAsyncioTestCase):
    def runtime(self, dry_run=False, build_url=RAW_URL):
        runtime = MagicMock()
        runtime.working_dir = Path('/unused')
        client = SlackClient(None, 'example-job', '1', build_url, dry_run=dry_run)
        client._client = MagicMock()
        client._client.chat_postMessage = AsyncMock(return_value=MagicMock(data={'ts': 'thread'}))
        runtime.new_slack_client.return_value = client
        return runtime, client

    def concern(self, version='4.18'):
        return dict(
            group=f'openshift-{version}',
            image_name='build-image',
            code=ConcernCode.LATEST_ATTEMPT_FAILED.value,
            latest_success_idx=3,
            latest_failed_nvr=RAW_URL,
            latest_failed_build_record_id='record',
            latest_failed_build_time='2026-01-01T00:00:00+00:00',
        )

    def failures(self):
        return {'counter-image': dict(failure_count=3, jenkins_url=RAW_URL, pipeline_url=RAW_URL)}

    def assert_private(self, payload):
        for marker in ('execution.invalid', 'dummy-secret', 'dummy-password'):
            self.assertNotIn(marker, repr(payload))

    async def test_layered_incomplete_final_payloads_and_dry_run_logs(self):
        for dry_run in (False, True):
            with self.subTest(dry_run=dry_run):
                runtime, client = self.runtime(dry_run)
                pipeline = LayeredProductsImageHealthPipeline(runtime, 'example-group', '', '', 'stream', '')
                report = LayeredProductHealthReport(
                    'example-group',
                    'oadp',
                    BuildVariant.OADP,
                    [self.concern()],
                    {},
                    self.failures(),
                    self.failures(),
                    self.failures(),
                    error='Image collection incomplete',
                    fbc_health=FbcHealthReport(error=True),
                )
                with patch('pyartcd.slack._LOGGER') as logger:
                    await pipeline._notify_slack([report])
                payloads = logger.warning.call_args_list if dry_run else client._client.chat_postMessage.call_args_list
                self.assertEqual(len(payloads), 2)
                self.assert_private(payloads)
                self.assertIn('incomplete', repr(payloads))
                self.assertIn('build-image', repr(payloads))
                self.assertIn('counter-image', repr(payloads))
                self.assertIn('Failed 3', repr(payloads))
                self.assertNotIn('Healthy', repr(payloads))

    async def test_ocp_real_run_mixed_versions_and_all_notification_sinks(self):
        runtime, client = self.runtime()
        pipeline = ImagesHealthPipeline(
            runtime, '4.18,4.19', True, '#example', '', '', '', 'stream', sync_jira=False, ping_chai_bot=True
        )
        pipeline.report = [self.concern(version) for version in pipeline.versions]
        for version in pipeline.versions:
            pipeline.ec_failures[version] = self.failures()
            pipeline.release_failures[version] = self.failures()
            pipeline.rebase_failures[version] = self.failures()

        async def collect(version):
            pipeline.scanned_versions.append(version)
            if version == '4.18':
                raise RuntimeError('query unavailable')

        pipeline.get_report = AsyncMock(side_effect=collect)
        for method in ('get_rebase_failures', 'get_ec_failures', 'get_release_failures', 'get_fbc_health'):
            setattr(pipeline, method, AsyncMock())
        with self.assertRaisesRegex(RuntimeError, 'Image health collection incomplete'):
            await pipeline.run()
        calls = client._client.chat_postMessage.call_args_list
        incomplete = [
            call.kwargs
            for call in calls
            if 'openshift-4.18' in call.kwargs['text'] or 'Image collection incomplete' in call.kwargs['text']
        ]
        self.assertGreaterEqual(len(incomplete), 3)
        for payload in incomplete:
            self.assert_private(payload)
            self.assertEqual(payload['attachments'], [])
        self.assertTrue(
            any('dummy-secret' in repr(call.kwargs) for call in calls if 'openshift-4.19' in call.kwargs['text'])
        )
        self.assertIn('counter-image', repr(calls))
        self.assertIn('Failed 3', repr(calls))
        runtime.new_jira_client.assert_not_called()

    async def test_complete_layered_links_remain_compatible(self):
        safe_url = 'https://build.example.invalid/job/1'
        runtime, client = self.runtime(build_url=safe_url)
        pipeline = LayeredProductsImageHealthPipeline(runtime, 'example-group', '', '', 'stream', '')
        report = LayeredProductHealthReport(
            'example-group',
            'oadp',
            BuildVariant.OADP,
            [],
            {},
            {'counter-image': {'failure_count': 2, 'pipeline_url': safe_url}},
            {},
            {},
        )
        await pipeline._notify_slack([report])
        thread = client._client.chat_postMessage.call_args_list[1].kwargs
        self.assertIn(safe_url, thread['text'])
        self.assertIn(safe_url, repr(thread['attachments']))

    async def test_ocp_incomplete_empty_fbc_and_unavailable_dry_run_sinks(self):
        reports = (
            FbcHealthReport(),
            FbcHealthReport(error=True),
            FbcHealthReport(
                {
                    'legacy': dict(
                        kind='operator',
                        entity='fbc-operator',
                        variant='ocp',
                        assembly='stream',
                        target='4.18',
                        version='1',
                        failure_count=2,
                        jenkins_url=RAW_URL,
                    )
                }
            ),
        )
        for report in reports:
            with self.subTest(report=report):
                runtime, client = self.runtime(dry_run=True)
                pipeline = ImagesHealthPipeline(
                    runtime, '4.18', True, '#example', '', '', '', 'stream', sync_jira=False
                )
                pipeline.collection_errors['4.18'] = True
                pipeline.fbc_health['4.18'] = report
                with patch('pyartcd.slack._LOGGER') as logger:
                    await pipeline.notify_release_channel('4.18')
                    await pipeline.notify_public_channel()
                self.assertEqual(logger.warning.call_count, 4)
                self.assert_private(logger.warning.call_args_list)
                messages = repr(logger.warning.call_args_list)
                self.assertIn('incomplete', messages)
                self.assertNotIn('Healthy', messages)
                self.assertNotIn('No tracked failures', messages)
                if report.summary():
                    self.assertIn(report.summary(), messages)
                client._client.chat_postMessage.assert_not_awaited()
