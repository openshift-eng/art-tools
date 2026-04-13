from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.cli.images_health import ConcernCode
from pyartcd.pipelines.images_health import ImagesHealthPipeline

DATA_PATH = "https://github.com/openshift-eng/ocp-build-data"


def _make_ticket(key, labels):
    ticket = MagicMock()
    ticket.key = key
    ticket.fields.labels = labels
    return ticket


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


class TestSyncJira(TestCase):
    def _make_pipeline(self, image_list=""):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        pipeline = ImagesHealthPipeline(
            runtime=runtime,
            versions="5.0",
            send_to_release_channel=False,
            send_to_forum_ocp_art=False,
            data_path=DATA_PATH,
            data_gitref="",
            image_list=image_list,
            assembly="stream",
            sync_jira=True,
        )
        pipeline.scanned_versions = ["5.0"]
        return pipeline

    def test_creates_ticket_for_new_failure(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]

        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = []
        mock_jira.create_issue.return_value = MagicMock(key="ART-100")
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.create_issue.assert_called_once()
        kwargs = mock_jira.create_issue.call_args
        self.assertEqual(kwargs[1]["project"], "ART")
        self.assertEqual(kwargs[1]["summary"], "Image build failure: ironic (openshift-5.0)")
        self.assertIn("art:image-build-failure", kwargs[1]["labels"])
        self.assertIn("art:package:ironic", kwargs[1]["labels"])
        self.assertIn("art:group:openshift-5.0", kwargs[1]["labels"])
        mock_jira.close_task.assert_not_called()

    def test_skips_existing_ticket(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]

        existing_ticket = _make_ticket(
            "ART-99",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-5.0"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [existing_ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.create_issue.assert_not_called()
        mock_jira.close_task.assert_not_called()

    def test_closes_resolved_ticket(self):
        pipeline = self._make_pipeline()
        # Report only has a success — no failures
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.LATEST_BUILD_SUCCEEDED.value),
        ]

        stale_ticket = _make_ticket(
            "ART-50",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-5.0"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [stale_ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.create_issue.assert_not_called()
        mock_jira.close_task.assert_called_once_with("ART-50")

    def test_skips_close_for_unscanned_version(self):
        pipeline = self._make_pipeline()
        pipeline.scanned_versions = ["5.0"]
        pipeline.report = []

        # Ticket for a version we did NOT scan
        ticket_4_18 = _make_ticket(
            "ART-60",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-4.18"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [ticket_4_18]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.close_task.assert_not_called()

    def test_skips_close_for_unscanned_image(self):
        pipeline = self._make_pipeline(image_list="other-image")
        pipeline.report = []

        ticket = _make_ticket(
            "ART-70",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-5.0"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.close_task.assert_not_called()

    def test_fetches_all_pages(self):
        pipeline = self._make_pipeline()
        pipeline.report = []

        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = []
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.search_issues.assert_called_once()
        _, kwargs = mock_jira.search_issues.call_args
        self.assertFalse(kwargs.get("maxResults", True))


class TestSyncJiraIntegration(IsolatedAsyncioTestCase):
    def _make_pipeline(self, assembly="stream"):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        pipeline = ImagesHealthPipeline(
            runtime=runtime,
            versions="5.0",
            send_to_release_channel=False,
            send_to_forum_ocp_art=False,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly=assembly,
            sync_jira=True,
        )
        pipeline.scanned_versions = ["5.0"]
        return pipeline

    @patch.object(ImagesHealthPipeline, "get_report", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_rebase_failures", new_callable=AsyncMock)
    async def test_skips_jira_sync_for_non_stream_assembly(self, _mock_rebase, _mock_report):
        pipeline = self._make_pipeline(assembly="4.18.5")
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]
        mock_jira = MagicMock()
        pipeline.runtime.new_jira_client.return_value = mock_jira

        await pipeline.run()

        mock_jira.search_issues.assert_not_called()
        mock_jira.create_issue.assert_not_called()

    @patch.object(ImagesHealthPipeline, "get_report", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_rebase_failures", new_callable=AsyncMock)
    async def test_jira_failure_does_not_block_notifications(self, _mock_rebase, _mock_report):
        pipeline = self._make_pipeline()
        pipeline.report = []
        pipeline.send_to_release_channel = True
        pipeline.scanned_versions = ["5.0"]

        mock_jira = MagicMock()
        mock_jira.search_issues.side_effect = RuntimeError("Jira is down")
        pipeline.runtime.new_jira_client.return_value = mock_jira

        mock_slack = MagicMock()
        mock_slack.say = AsyncMock()
        pipeline.slack_client = mock_slack

        await pipeline.run()

        mock_slack.say.assert_called()
