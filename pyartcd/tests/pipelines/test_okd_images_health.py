import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.cli.images_health import ConcernCode
from pyartcd.pipelines.okd_images_health import ImagesHealthPipeline

DATA_PATH = "https://github.com/openshift-eng/ocp-build-data"


def _make_concern(image_name, group, code, **kwargs):
    concern = {
        "image_name": image_name,
        "group": group,
        "code": code,
        "latest_failed_build_time": "2025-12-17T10:00:00+00:00",
        "latest_failed_nvr": f"{image_name}-1.0-1",
        "latest_failed_build_record_id": "12345",
    }
    concern.update(kwargs)
    return concern


class TestGetReport(IsolatedAsyncioTestCase):
    def _make_pipeline(self, versions="4.21", image_list=""):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        return ImagesHealthPipeline(
            runtime=runtime,
            versions=versions,
            send_to_release_channel=False,
            send_to_okd_channel=False,
            data_path=DATA_PATH,
            data_gitref="",
            image_list=image_list,
            assembly="stream",
        )

    @patch("pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.okd_images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_redis_pre_filters_doozer_call(self, mock_get_failures, mock_cmd):
        """Redis failures scope the --images flag on the doozer subprocess."""
        mock_get_failures.return_value = {
            'ironic': {'failure_count': 3, 'url': '', 'nvr': ''},
            'ovn-kubernetes': {'failure_count': 2, 'url': '', 'nvr': ''},
        }
        doozer_report = [
            _make_concern("ironic", "okd-4.21", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
            _make_concern("ovn-kubernetes", "okd-4.21", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=2),
        ]
        mock_cmd.return_value = (0, json.dumps(doozer_report), '')

        pipeline = self._make_pipeline()
        # Mock _get_valid_images to return all images as valid
        pipeline._get_valid_images = AsyncMock(return_value={'ironic', 'ovn-kubernetes'})

        await pipeline.get_report('4.21')

        mock_get_failures.assert_called_once_with('build-failure', group='okd-4.21', logger=pipeline.runtime.logger)
        mock_cmd.assert_called_once()
        cmd = mock_cmd.call_args[0][0]
        images_arg = next(a for a in cmd if a.startswith('--images='))
        self.assertIn('ironic', images_arg)
        self.assertIn('ovn-kubernetes', images_arg)
        self.assertEqual(len(pipeline.report), 2)

    @patch("pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.okd_images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_image_list_intersects_with_redis(self, mock_get_failures, mock_cmd):
        """When --image-list is provided, only images in BOTH lists are queried."""
        mock_get_failures.return_value = {
            'ironic': {'failure_count': 3, 'url': '', 'nvr': ''},
            'ovn-kubernetes': {'failure_count': 2, 'url': '', 'nvr': ''},
        }
        doozer_report = [
            _make_concern("ironic", "okd-4.21", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]
        mock_cmd.return_value = (0, json.dumps(doozer_report), '')

        pipeline = self._make_pipeline(image_list="ironic")
        # Mock _get_valid_images to return ironic as valid
        pipeline._get_valid_images = AsyncMock(return_value={'ironic', 'ovn-kubernetes'})

        await pipeline.get_report('4.21')

        cmd = mock_cmd.call_args[0][0]
        images_arg = next(a for a in cmd if a.startswith('--images='))
        self.assertEqual(images_arg, '--images=ironic')

    @patch("pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.okd_images_health.util.get_counter_failures", new_callable=AsyncMock, return_value={})
    async def test_skips_bigquery_when_no_redis_failures(self, _mock_get_failures, mock_cmd):
        """When Redis reports no failures, doozer images:health is not called at all."""
        pipeline = self._make_pipeline()

        await pipeline.get_report('4.21')

        mock_cmd.assert_not_called()
        self.assertEqual(len(pipeline.report), 0)
        self.assertIn('4.21', pipeline.scanned_versions)

    @patch("pyartcd.pipelines.okd_images_health.util.is_okd_version_enabled", new_callable=AsyncMock)
    async def test_filters_disabled_versions(self, mock_is_enabled):
        """Requested versions without okd.enabled in build-data are skipped."""
        mock_is_enabled.side_effect = [True, False]

        pipeline = self._make_pipeline(versions="4.21,4.23")
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()

        await pipeline.run()

        self.assertEqual(pipeline.versions, ['4.21'])
        self.assertEqual(mock_is_enabled.await_count, 2)
        pipeline.get_report.assert_awaited_once_with('4.21')

    @patch("pyartcd.pipelines.okd_images_health.util.is_okd_version_enabled", new_callable=AsyncMock)
    async def test_discovers_enabled_versions_when_not_provided(self, mock_is_enabled):
        """When --versions is omitted, probe ACTIVE_OCP_VERSIONS and keep okd.enabled versions."""

        async def enabled_for_cmd(cmd):
            return any(arg == '--group=openshift-4.21' for arg in cmd)

        mock_is_enabled.side_effect = enabled_for_cmd

        pipeline = self._make_pipeline(versions="")
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()

        with patch(
            'pyartcd.pipelines.okd_images_health.ACTIVE_OCP_VERSIONS',
            ['4.21', '4.23'],
        ):
            await pipeline.run()

        self.assertEqual(pipeline.versions, ['4.21'])
        self.assertEqual(mock_is_enabled.await_count, 2)
        pipeline.get_report.assert_awaited_once_with('4.21')

    @patch(
        "pyartcd.pipelines.okd_images_health.util.is_okd_version_enabled", new_callable=AsyncMock, return_value=False
    )
    async def test_skips_when_no_enabled_versions(self, _mock_is_enabled):
        pipeline = self._make_pipeline(versions="4.23")
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()
        pipeline.notify_release_channel = AsyncMock()
        pipeline.notify_okd_channel = AsyncMock()

        await pipeline.run()

        self.assertEqual(pipeline.versions, [])
        pipeline.get_report.assert_not_called()
        pipeline.notify_okd_channel.assert_not_called()
