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

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_triggers_qualifying_mr(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9 (ship date: 2026-10-15)",
                       ["stage-release-success", "stage-testing-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_called_once_with(
            version="4.22", assembly="4.22.9", block_until_building=False
        )

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_skips_already_verified(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9",
                       ["stage-release-success", "stage-testing-success", "verify-release-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_skips_without_stage_testing(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_skips_non_ocp(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for logging 6.5.2",
                       ["stage-release-success", "stage-testing-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_triggers_rc_assembly(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 5.0.0-rc.6",
                       ["stage-release-success", "stage-testing-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_called_once_with(
            version="5.0", assembly="rc.6", block_until_building=False
        )

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_multiple_mrs(self, mock_gitlab_cls, mock_jenkins):
        mr1 = _make_mr(10, "Shipment for 4.22.9",
                        ["stage-release-success", "stage-testing-success"])
        mr2 = _make_mr(11, "Shipment for 5.0.3",
                        ["stage-release-success", "stage-testing-success"])
        mr3 = _make_mr(12, "Shipment for 4.22.9",
                        ["stage-release-success", "stage-testing-success", "verify-release-success"])
        mr4 = _make_mr(13, "Shipment for 4.21.15",
                        ["stage-release-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr1, mr2, mr3, mr4]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        self.assertEqual(mock_jenkins.start_verify_release.call_count, 2)

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_no_mrs(self, mock_gitlab_cls, mock_jenkins):
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = []
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_release.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_verify_release_scan.GitLabClient")
    async def test_jenkins_failure_does_not_stop_scan(self, mock_gitlab_cls, mock_jenkins):
        mr1 = _make_mr(10, "Shipment for 4.22.9",
                        ["stage-release-success", "stage-testing-success"])
        mr2 = _make_mr(11, "Shipment for 5.0.3",
                        ["stage-release-success", "stage-testing-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr1, mr2]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project
        mock_jenkins.start_verify_release.side_effect = [Exception("Jenkins down"), None]

        await scan_and_trigger(self.runtime)

        self.assertEqual(mock_jenkins.start_verify_release.call_count, 2)


if __name__ == "__main__":
    unittest.main()
