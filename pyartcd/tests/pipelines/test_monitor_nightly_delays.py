from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.monitor_nightly_delays import MonitorNightlyDelaysPipeline

from pyartcd import util


class TestMonitorNightlyDelaysPipeline(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = MagicMock()
        self.mock_runtime.working_dir = MagicMock()
        self.mock_runtime.logger = MagicMock()
        self.mock_runtime.dry_run = True

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()

        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

    async def test_no_delay_no_alert(self):
        """Test that no alert is sent when nightly is recent (< 6 hours)"""
        # Given: A nightly from 3 hours ago (timestamp embedded in name)
        recent_time = datetime.now(timezone.utc) - timedelta(hours=3)
        # Format timestamp as YYYYMMDD-HHMMSS for nightly name
        timestamp_str = recent_time.strftime('%Y-%m-%d-%H%M%S')
        mock_nightly = {
            'name': f'4.22.0-0.nightly-{timestamp_str}',
            'phase': 'Accepted',
        }

        mock_group_config = {'software_lifecycle': {'phase': 'pre-release'}}

        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='#forum-ocp-release-oversight',
        )

        mock_slack_client = self.mock_runtime.new_slack_client.return_value

        # When: We check and alert
        with (
            patch.object(pipeline, '_get_latest_nightly', return_value=mock_nightly),
            patch.object(util, 'load_group_config', return_value=mock_group_config),
        ):
            await pipeline.run()

        # Then: No alert should be sent
        mock_slack_client.say.assert_not_called()

    async def test_6h_delay_alerts_release_artists(self):
        """Test that 6-hour delay sends alert to release-artists channel"""
        # Given: A nightly from 7 hours ago (could be any status)
        old_time = datetime.now(timezone.utc) - timedelta(hours=7)
        timestamp_str = old_time.strftime('%Y-%m-%d-%H%M%S')
        mock_nightly = {
            'name': f'4.22.0-0.nightly-{timestamp_str}',
            'phase': 'Rejected',  # Even rejected nightlies count
        }

        mock_group_config = {'software_lifecycle': {'phase': 'pre-release'}}

        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='#forum-ocp-release-oversight',
        )

        mock_slack_client = self.mock_runtime.new_slack_client.return_value

        # When: We check and alert
        with (
            patch.object(pipeline, '_get_latest_nightly', return_value=mock_nightly),
            patch.object(util, 'load_group_config', return_value=mock_group_config),
        ):
            await pipeline.run()

        # Then: Alert should be sent to version-specific release-artists channel
        mock_slack_client.bind_channel.assert_called_with('#art-release-4-22')
        mock_slack_client.say.assert_called_once()
        alert_message = mock_slack_client.say.call_args[0][0]
        self.assertIn('6 hours', alert_message)
        # once @release-artist was enabled again in monitor_nightly_delays the following assert can be enabled again
        self.assertIn('Status: Rejected', alert_message)
        self.assertIn('4.22', alert_message)

    async def test_12h_delay_alerts_trt(self):
        """Test that 12-hour delay sends alert to TRT channel for pre-release versions"""
        # Given: A nightly from 13 hours ago (could be any status)
        old_time = datetime.now(timezone.utc) - timedelta(hours=13)
        timestamp_str = old_time.strftime('%Y-%m-%d-%H%M%S')
        mock_nightly = {
            'name': f'4.22.0-0.nightly-{timestamp_str}',
            'phase': 'Ready',  # Even pending nightlies count
        }

        # Pre-release version should alert TRT
        mock_group_config = {'software_lifecycle': {'phase': 'pre-release'}}

        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='#forum-ocp-release-oversight',
        )

        mock_slack_client = self.mock_runtime.new_slack_client.return_value

        # When: We check and alert
        with (
            patch.object(pipeline, '_get_latest_nightly', return_value=mock_nightly),
            patch.object(util, 'load_group_config', return_value=mock_group_config),
        ):
            await pipeline.run()

        # Then: Alert should be sent to TRT channel
        calls = mock_slack_client.bind_channel.call_args_list
        self.assertTrue(any('#forum-ocp-release-oversight' in str(call) for call in calls))

        alert_calls = mock_slack_client.say.call_args_list
        self.assertTrue(len(alert_calls) >= 1)

        # Check that at least one message mentions 12 hours and CRITICAL
        trt_alert_found = False
        for call in alert_calls:
            message = call[0][0]
            if '12 hours' in message and 'CRITICAL' in message:
                trt_alert_found = True
                break

        self.assertTrue(trt_alert_found, "TRT alert with 12-hour warning not found")

    async def test_12h_delay_no_trt_for_ga_versions(self):
        """Test that 12-hour delay does NOT alert TRT for GA/post-GA versions"""
        # Given: A nightly from 13 hours ago
        old_time = datetime.now(timezone.utc) - timedelta(hours=13)
        timestamp_str = old_time.strftime('%Y-%m-%d-%H%M%S')
        mock_nightly = {
            'name': f'4.22.0-0.nightly-{timestamp_str}',
            'phase': 'Accepted',
        }

        # Non pre-release version should NOT alert TRT
        mock_group_config = {'software_lifecycle': {'phase': 'end-of-life'}}

        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='#forum-ocp-release-oversight',
        )

        mock_slack_client = self.mock_runtime.new_slack_client.return_value

        # When: We check and alert
        with (
            patch.object(pipeline, '_get_latest_nightly', return_value=mock_nightly),
            patch.object(util, 'load_group_config', return_value=mock_group_config),
        ):
            await pipeline.run()

        # Then: Only release-artists should be alerted, NOT TRT
        calls = mock_slack_client.bind_channel.call_args_list

        # Should have called bind_channel only once for #art-release-4-22
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0][0], '#art-release-4-22')

        # Should have sent exactly one message (to art-release, not TRT)
        alert_calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(alert_calls), 1)

        # The message should NOT mention TRT
        message = alert_calls[0][0][0]
        self.assertNotIn('TRT', message)
        self.assertNotIn('CRITICAL', message)

    async def test_handles_missing_nightly(self):
        """Test that pipeline handles case when latest nightly cannot be retrieved"""
        # Given: No nightly data available
        mock_group_config = {'software_lifecycle': {'phase': 'pre-release'}}

        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='##forum-ocp-release-oversight',
        )

        mock_slack_client = self.mock_runtime.new_slack_client.return_value

        # When: We try to run with no nightly data
        with (
            patch.object(pipeline, '_get_latest_nightly', return_value=None),
            patch.object(util, 'load_group_config', return_value=mock_group_config),
        ):
            await pipeline.run()

        # Then: Should log warning but not crash or send alerts
        self.mock_runtime.logger.warning.assert_called()
        mock_slack_client.say.assert_not_called()

    async def test_handles_nightly_without_timestamp(self):
        """Test that pipeline handles nightly data with invalid timestamp format"""
        # Given: Nightly data with invalid name format (missing timestamp)
        mock_nightly = {
            'name': '4.22.0-0.nightly-invalid',  # Invalid format - no timestamp
            'phase': 'Accepted',
        }

        mock_group_config = {'software_lifecycle': {'phase': 'pre-release'}}

        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='#forum-ocp-release-oversight',
        )

        mock_slack_client = self.mock_runtime.new_slack_client.return_value

        # When: We try to run with incomplete nightly data
        with (
            patch.object(pipeline, '_get_latest_nightly', return_value=mock_nightly),
            patch.object(util, 'load_group_config', return_value=mock_group_config),
        ):
            await pipeline.run()

        # Then: Should log warning but not crash or send alerts
        self.mock_runtime.logger.warning.assert_called()
        mock_slack_client.say.assert_not_called()

    async def test_get_latest_nightly_success(self):
        """Test successful retrieval of latest nightly"""
        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='#forum-ocp-release-oversight',
        )

        mock_nightly_data = {
            'name': '4.22.0-0.konflux-nightly-2026-03-13-100000',
            'created': '2026-03-13T10:00:00Z',
            'phase': 'Accepted',
        }

        # The API returns a tags array with most recent first
        mock_response_data = {
            'tags': [
                mock_nightly_data,
                {
                    'name': '4.22.0-0.konflux-nightly-2026-03-13-080000',
                    'created': '2026-03-13T08:00:00Z',
                    'phase': 'Rejected',
                },
            ]
        }

        # Mock aiohttp response - properly set up async context manager
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock()

        with patch('pyartcd.pipelines.monitor_nightly_delays.aiohttp.ClientSession', return_value=mock_session):
            result = await pipeline._get_latest_nightly()

        # Should return the first (most recent) nightly
        self.assertEqual(result, mock_nightly_data)
        self.assertEqual(result['name'], '4.22.0-0.konflux-nightly-2026-03-13-100000')

    async def test_get_latest_nightly_failure(self):
        """Test handling of failed API call"""
        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,  # Will default to #art-release-4-22
            trt_channel='#forum-ocp-release-oversight',
        )

        # Mock aiohttp response with error - properly set up async context manager
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock()

        with patch('pyartcd.pipelines.monitor_nightly_delays.aiohttp.ClientSession', return_value=mock_session):
            result = await pipeline._get_latest_nightly()

        self.assertIsNone(result)
        self.mock_runtime.logger.error.assert_called()

    def test_default_channel_from_group(self):
        """Test that release artists channel defaults to version-specific channel"""
        # Test with None (should default to #art-release-4-22)
        pipeline = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel=None,
            trt_channel='#forum-ocp-release-oversight',
        )
        self.assertEqual(pipeline.release_artists_channel, '#art-release-4-22')

        # Test with explicit channel (should use provided value)
        pipeline_custom = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.22',
            build_system='konflux',
            release_artists_channel='#custom-channel',
            trt_channel='#forum-ocp-release-oversight',
        )
        self.assertEqual(pipeline_custom.release_artists_channel, '#custom-channel')

        # Test with different version
        pipeline_418 = MonitorNightlyDelaysPipeline(
            runtime=self.mock_runtime,
            group='openshift-4.18',
            build_system='konflux',
            release_artists_channel=None,
            trt_channel='#forum-ocp-release-oversight',
        )
        self.assertEqual(pipeline_418.release_artists_channel, '#art-release-4-18')
