#!/usr/bin/env python3

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline


class TestUpdateBuildFailCounters(unittest.IsolatedAsyncioTestCase):
    """
    Tests for KonfluxOcpPipeline.update_build_fail_counters().

    Focus: the early-return path (all failures are infra/parent-dep) must still
    reset counters for successfully built images before returning.
    """

    def _make_pipeline(self, assembly='stream', version='4.21'):
        runtime = MagicMock()
        runtime.doozer_working = '/tmp/doozer-working'
        runtime.new_slack_client.return_value = MagicMock()
        with patch('pyartcd.pipelines.ocp4_konflux.util.default_release_suffix', return_value='202408190000'):
            pipeline = KonfluxOcpPipeline(
                runtime=runtime,
                assembly=assembly,
                version=version,
                image_build_strategy='all',
                rpm_build_strategy='none',
                build_priority='auto',
                data_path='https://github.com/openshift-eng/ocp-build-data',
            )
        return pipeline

    def _infra_failure_record_log(self, failed_image: str):
        """Record log where the sole failure has task_id=n/a (infra failure)."""
        return {
            'image_build_konflux': [
                {
                    'name': failed_image,
                    'status': '1',
                    'task_id': 'n/a',
                    'task_url': 'n/a',
                    'message': 'infrastructure failure',
                    'outcome': '',
                    'nvrs': '',
                    'build_pipeline_url': '',
                }
            ]
        }

    @patch.dict(os.environ, {'BUILD_URL': 'https://jenkins.example.com/job/1'})
    @patch('pyartcd.pipelines.ocp4_konflux.reset_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.increment_fail_counter', new_callable=AsyncMock)
    async def test_infra_failure_still_resets_built_image_counters(self, mock_increment, mock_reset):
        """
        When all failures are infra (task_id=n/a), the early return must NOT skip
        resetting counters for successfully built images.

        Regression test for ART-22800.
        """
        pipeline = self._make_pipeline()

        built_images = ['driver-toolkit', 'base-images']
        failed_images = ['enterprise-cluster-capacity']
        record_log = self._infra_failure_record_log('enterprise-cluster-capacity')

        await pipeline.update_build_fail_counters(built_images, failed_images, record_log)

        # reset_fail_counter must be called for each built image × 3 counter types
        self.assertEqual(mock_reset.call_count, len(built_images) * 3)
        reset_keys = {call.args[0] for call in mock_reset.call_args_list}
        expected_keys = {
            f'count:{ct}:konflux:openshift-4.21:{img}'
            for img in built_images
            for ct in ('build-failure', 'ec-failure', 'release-failure')
        }
        self.assertEqual(reset_keys, expected_keys)

        # increment_fail_counter must NOT be called (infra failure, no real build attempted)
        mock_increment.assert_not_called()

    @patch.dict(os.environ, {'BUILD_URL': 'https://jenkins.example.com/job/1'})
    @patch('pyartcd.pipelines.ocp4_konflux.reset_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.increment_fail_counter', new_callable=AsyncMock)
    async def test_non_stream_assembly_skips_all_counters(self, mock_increment, mock_reset):
        """Non-stream assemblies must return immediately without touching any counters."""
        pipeline = self._make_pipeline(assembly='4.21.3')

        await pipeline.update_build_fail_counters(['some-image'], [], {})

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()

    @patch.dict(os.environ, {'BUILD_URL': 'https://jenkins.example.com/job/1'})
    @patch('pyartcd.pipelines.ocp4_konflux.reset_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.increment_fail_counter', new_callable=AsyncMock)
    async def test_no_built_no_failed_images_noop(self, mock_increment, mock_reset):
        """Empty built and failed lists produce no counter operations."""
        pipeline = self._make_pipeline()

        await pipeline.update_build_fail_counters([], [], {})

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()


if __name__ == '__main__':
    unittest.main()
