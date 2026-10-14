import unittest
from unittest.mock import MagicMock, patch

from pyartcd.pipelines.scheduled.schedule_stage_testing_scan import scan_and_trigger


def _make_mr(iid, title, labels):
    mr = MagicMock()
    mr.iid = iid
    mr.title = title
    mr.labels = labels
    mr.web_url = f"https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/{iid}"
    return mr


class TestScheduleStageTestingScan(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.dry_run = False

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_triggers_qualifying_mr(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9 (ship date: 2026-10-15)", ["stage-release-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_stage_testing.assert_called_once_with(
            version="4.22", assembly="4.22.9", block_until_building=False
        )

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_already_triggered(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-triggered"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_already_passed(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_failed(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-fail"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_non_ocp(self, mock_gitlab_cls, mock_jenkins):
        mr = _make_mr(42, "Shipment for logging 6.5.2", ["stage-release-success"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_multiple_mrs(self, mock_gitlab_cls, mock_jenkins):
        mr1 = _make_mr(10, "Shipment for 4.22.9", ["stage-release-success"])
        mr2 = _make_mr(11, "Shipment for 5.0.3", ["stage-release-success"])
        mr3 = _make_mr(12, "Shipment for logging 6.5.2", ["stage-release-success"])
        mr4 = _make_mr(13, "Shipment for 4.21.15", ["stage-release-success", "stage-testing-triggered"])
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = [mr1, mr2, mr3, mr4]
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        self.assertEqual(mock_jenkins.start_verify_stage_testing.call_count, 2)

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_no_mrs(self, mock_gitlab_cls, mock_jenkins):
        mock_project = MagicMock()
        mock_project.mergerequests.list.return_value = []
        mock_gitlab_cls.from_url.return_value.get_project.return_value = mock_project

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_stage_testing.assert_not_called()


if __name__ == "__main__":
    unittest.main()
