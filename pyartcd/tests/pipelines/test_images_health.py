from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from doozerlib.cli.images_health import ConcernCode
from pyartcd.pipelines.images_health import ImagesHealthPipeline

DATA_PATH = "https://github.com/openshift-eng/ocp-build-data"


class TestImagesHealthPipeline(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = MagicMock()
        self.mock_runtime.working_dir = MagicMock()
        self.mock_runtime.logger = MagicMock()

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()

        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

    async def test_notify_forum_ocp_art_with_no_concerns_after_filtering(self):
        # given
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = ImagesHealthPipeline(
            runtime=self.mock_runtime,
            versions="4.22",
            send_to_release_channel=False,
            send_to_forum_ocp_art=True,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
        )
        pipeline.report = [
            {
                "code": ConcernCode.NEVER_BUILT.value,
                "image_name": "never-built-image",
                "group": "openshift-4.22",
            },
            {
                "code": ConcernCode.LATEST_BUILD_SUCCEEDED.value,
                "image_name": "succeeded-image",
                "group": "openshift-4.22",
            },
        ]
        # when
        await pipeline.notify_forum_ocp_art()
        # then
        mock_slack_client.say.assert_called_once_with(
            ":white_check_mark: All images are healthy for all monitored releases"
        )

    async def test_notify_forum_ocp_art_with_concerns(self):
        # given
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = ImagesHealthPipeline(
            runtime=self.mock_runtime,
            versions="4.21",
            send_to_release_channel=False,
            send_to_forum_ocp_art=True,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
        )
        pipeline.report = [
            {
                "code": ConcernCode.FAILING_AT_LEAST_FOR.value,
                "image_name": "failing-image",
                "group": "openshift-4.21",
                "latest_failed_build_time": "2025-12-17T10:00:00+00:00",
                "latest_failed_nvr": "failing-image-1.0-1",
                "latest_failed_build_record_id": "12345",
            },
            {
                "code": ConcernCode.LATEST_ATTEMPT_FAILED.value,
                "image_name": "failing-image",
                "group": "openshift-4.20",
                "latest_success_idx": 3,
                "latest_failed_build_time": "2025-12-17T11:00:00+00:00",
                "latest_failed_nvr": "failing-image-2.0-1",
                "latest_failed_build_record_id": "12346",
            },
        ]
        mock_response = {"ts": ""}
        mock_slack_client.say.return_value = mock_response
        # when
        await pipeline.notify_forum_ocp_art()
        # then
        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 2)
        first_call_args = calls[0][0][0]
        self.assertIn(":alert: There are some issues to look into for Openshift builds:", first_call_args)

    async def test_notify_forum_ocp_art_with_mixed_concerns(self):
        # given
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = ImagesHealthPipeline(
            runtime=self.mock_runtime,
            versions="4.22",
            send_to_release_channel=False,
            send_to_forum_ocp_art=True,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
        )

        # Create a report with mixed concerns
        pipeline.report = [
            {
                "code": ConcernCode.NEVER_BUILT.value,
                "image_name": "never-built-image",
                "group": "openshift-4.22",
            },
            {
                "code": ConcernCode.LATEST_BUILD_SUCCEEDED.value,
                "image_name": "succeeded-image",
                "group": "openshift-4.22",
            },
            {
                "code": ConcernCode.FAILING_AT_LEAST_FOR.value,
                "image_name": "failing-image",
                "group": "openshift-4.22",
                "latest_failed_build_time": "2025-12-17T10:00:00+00:00",
                "latest_failed_nvr": "failing-image-1.0-1",
                "latest_failed_build_record_id": "12345",
            },
        ]
        mock_response = {"ts": ""}
        mock_slack_client.say.return_value = mock_response
        # when
        await pipeline.notify_forum_ocp_art()
        # then
        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 2)
        first_call_args = calls[0][0][0]
        self.assertIn(":alert:", first_call_args)

    async def test_notify_forum_ocp_art_with_empty_report(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = ImagesHealthPipeline(
            runtime=self.mock_runtime,
            versions="4.22",
            send_to_release_channel=False,
            send_to_forum_ocp_art=True,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
        )
        pipeline.report = []
        # when
        await pipeline.notify_forum_ocp_art()
        # then
        mock_slack_client.say.assert_called_once_with(
            ":white_check_mark: All images are healthy for all monitored releases"
        )
