import unittest
from unittest.mock import MagicMock, patch

from pyartcd.pipelines.scheduled.schedule_verify_release_scan import scan_and_trigger


def _make_mr(iid, title, labels):
    mr = MagicMock()
    mr.iid = iid
    mr.title = title
    mr.labels = labels
    return mr


class TestScheduleVerifyReleaseScan(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.dry_run = False

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_dry_run_does_not_trigger_jenkins(self, mock_gitlab_cls, mock_jenkins):
        self.runtime.dry_run = True
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        with self.assertLogs("pyartcd.pipelines.scheduled.schedule_verify_release_scan", level="INFO") as logs:
            await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()
        self.assertIn("[DRY-RUN] MR !42 — would queue verify-release for 4.22/4.22.9", "\n".join(logs.output))

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_triggers_qualifying_mr(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(
            42, "Shipment for 4.22.9 (ship date: 2026-10-15)", ["stage-release-success", "stage-testing-success"]
        )
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_client.list_merge_requests.assert_called_once_with(
            "hybrid-platforms/art/ocp-shipment-data",
            state="opened",
            labels=["stage-release-success", "stage-testing-success"],
            per_page=100,
        )
        mock_jenkins.start_verify_release.assert_called_once_with(
            version="4.22", assembly="4.22.9", block_until_building=False
        )

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_skips_already_verified(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(
            42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-success", "verify-release-success"]
        )
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_skips_without_stage_testing(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_skips_non_ocp(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for logging 6.5.2", ["stage-release-success", "stage-testing-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_triggers_rc_assembly(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 5.0.0-rc.6", ["stage-release-success", "stage-testing-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_called_once_with(
            version="5.0", assembly="rc.6", block_until_building=False
        )

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_multiple_mrs(self, mock_gitlab_cls, mock_jenkins):
        mr1 = _make_mr(10, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-success"])
        mr2 = _make_mr(11, "Shipment for 5.0.3", ["stage-release-success", "stage-testing-success"])
        mr3 = _make_mr(
            12, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-success", "verify-release-success"]
        )
        mr4 = _make_mr(13, "Shipment for 4.21.15", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr1, mr2, mr3, mr4]

        await scan_and_trigger(self.runtime)

        self.assertEqual(mock_jenkins.start_verify_release.call_count, 2)

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_no_mrs(self, mock_gitlab_cls, mock_jenkins):
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = []

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_jenkins_failure_does_not_stop_scan(self, mock_gitlab_cls, mock_jenkins):
        mr1 = _make_mr(10, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-success"])
        mr2 = _make_mr(11, "Shipment for 5.0.3", ["stage-release-success", "stage-testing-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr1, mr2]
        mock_jenkins.start_verify_release.side_effect = [Exception("Jenkins down"), None]

        await scan_and_trigger(self.runtime)

        self.assertEqual(mock_jenkins.start_verify_release.call_count, 2)


if __name__ == "__main__":
    unittest.main()
