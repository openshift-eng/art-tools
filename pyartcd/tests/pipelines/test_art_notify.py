import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.art_notify import ArtNotifyPipeline


class TestArtNotifyPipeline(unittest.TestCase):
    @patch("pyartcd.pipelines.art_notify.App")
    @patch("pyartcd.pipelines.art_notify.os.getenv")
    @patch("pyartcd.pipelines.art_notify.requests.get")
    def test_get_konflux_task_update_pr_with_open_pr(self, mock_requests_get, mock_os_getenv, mock_app):
        # Mock environment variable
        mock_os_getenv.return_value = "fake_github_token"

        # Mock response from GitHub API
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "state": "open",
                "title": "Update Konflux references",
                "html_url": "https://github.com/openshift-priv/art-konflux-template/pull/123",
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        # Initialize the pipeline and call the method
        pipeline = ArtNotifyPipeline(runtime=MagicMock(), channel="test-channel")
        result = pipeline._get_konflux_task_update_pr()

        # Assert the result
        expected_text = "* <https://github.com/openshift-priv/art-konflux-template/pull/123|Review Konflux Task Update PR>, refer docs (https://art-docs.engineering.redhat.com/konflux/update-konflux-task-bundles/)"
        self.assertEqual(result, expected_text)

    @patch("pyartcd.pipelines.art_notify.App")
    @patch("pyartcd.pipelines.art_notify.os.getenv")
    @patch("pyartcd.pipelines.art_notify.requests.get")
    def test_get_konflux_task_update_pr_with_no_open_pr(self, mock_requests_get, mock_os_getenv, mock_app):
        # Mock environment variable
        mock_os_getenv.return_value = "fake_github_token"

        # Mock response from GitHub API with no open PRs
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response

        # Initialize the pipeline and call the method
        pipeline = ArtNotifyPipeline(runtime=MagicMock(), channel="test-channel")
        result = pipeline._get_konflux_task_update_pr()

        # Assert the result
        self.assertIsNone(result)

    @patch("pyartcd.pipelines.art_notify.App")
    @patch("pyartcd.pipelines.art_notify.os.getenv")
    def test_get_konflux_task_update_pr_with_no_github_token(self, mock_os_getenv, mock_app):
        # Mock environment variable to return None
        mock_os_getenv.return_value = None

        # Initialize the pipeline
        pipeline = ArtNotifyPipeline(runtime=MagicMock(), channel="test-channel")

        # Assert that a ValueError is raised
        with self.assertRaises(ValueError):
            pipeline._get_konflux_task_update_pr()

    @patch("pyartcd.pipelines.art_notify.ArtNotifyPipeline._notify_rebase_failures", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.art_notify.ArtNotifyPipeline._notify_messages")
    @patch("pyartcd.pipelines.art_notify.SSLCertificateChecker.check_expired_certificates", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.art_notify.ArtNotifyPipeline._get_messages")
    @patch("pyartcd.pipelines.art_notify.ArtNotifyPipeline._get_failed_jobs_text")
    @patch("pyartcd.pipelines.art_notify.ArtNotifyPipeline._get_konflux_task_update_pr")
    @patch("pyartcd.pipelines.art_notify.App")
    def test_run(
        self,
        mock_app,
        mock_get_konflux_task_update_pr,
        mock_get_failed_jobs_text,
        mock_get_messages,
        mock_check_expired_certificates,
        mock_notify_messages,
        mock_notify_rebase_failures,
    ):
        # Mock the return values of the methods
        mock_get_konflux_task_update_pr.return_value = "konflux_pr_text"
        mock_get_failed_jobs_text.return_value = "failed_jobs_text"
        mock_get_messages.return_value = ["messages"]
        mock_check_expired_certificates.return_value = "expired_certificates"

        # Initialize the pipeline and call the run method
        pipeline = ArtNotifyPipeline(runtime=MagicMock(), channel="test-channel")
        import asyncio

        asyncio.run(pipeline.run())

        # Assert that the methods were called with the correct arguments
        mock_notify_messages.assert_called_once_with(
            "failed_jobs_text", ["messages"], ["expired_certificates", "konflux_pr_text"]
        )
        mock_notify_rebase_failures.assert_called_once()


if __name__ == "__main__":
    unittest.main()
