import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.scheduled.schedule_stage_testing_scan import scan_and_trigger


def _make_mr(iid, title, labels):
    mr = MagicMock()
    mr.iid = iid
    mr.title = title
    mr.labels = labels
    mr.web_url = f"https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/{iid}"
    return mr


IMAGE_AVAILABLE = {"some": "image-info"}


class TestScheduleStageTestingScan(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.dry_run = False

    @patch(
        "pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info",
        new_callable=AsyncMock,
        return_value=IMAGE_AVAILABLE,
    )
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_dry_run_does_not_trigger_jenkins(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        self.runtime.dry_run = True
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        with self.assertLogs("pyartcd.pipelines.scheduled.schedule_stage_testing_scan", level="INFO") as logs:
            await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_called_once_with("quay.io/openshift-release-dev/ocp-release:4.22.9-x86_64", registry_config=None)
        mock_jenkins.start_verify_stage_testing.assert_not_called()
        self.assertIn("[DRY-RUN] MR !42 — would queue verify-stage-testing for 4.22/4.22.9", "\n".join(logs.output))

    @patch(
        "pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info",
        new_callable=AsyncMock,
        return_value=IMAGE_AVAILABLE,
    )
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_triggers_qualifying_mr(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for 4.22.9 (ship date: 2026-10-15)", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_client.list_merge_requests.assert_called_once_with(
            "hybrid-platforms/art/ocp-shipment-data", state="opened", labels=["stage-release-success"], per_page=100
        )
        mock_get_image_info.assert_called_once_with("quay.io/openshift-release-dev/ocp-release:4.22.9-x86_64", registry_config=None)
        mock_jenkins.start_verify_stage_testing.assert_called_once_with(
            version="4.22", assembly="4.22.9", block_until_building=False
        )

    @patch(
        "pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info",
        new_callable=AsyncMock,
        return_value=None,
    )
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_image_not_available(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_called_once_with("quay.io/openshift-release-dev/ocp-release:4.22.9-x86_64", registry_config=None)
        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch(
        "pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info",
        new_callable=AsyncMock,
        side_effect=RuntimeError("network error"),
    )
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_on_image_check_error(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_already_triggered(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-triggered"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_not_called()
        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_already_passed(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_not_called()
        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_failed(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for 4.22.9", ["stage-release-success", "stage-testing-fail"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_not_called()
        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_skips_non_ocp(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for logging 6.5.2", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_not_called()
        mock_jenkins.start_verify_stage_testing.assert_not_called()

    @patch(
        "pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info",
        new_callable=AsyncMock,
        return_value=IMAGE_AVAILABLE,
    )
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_multiple_mrs(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr1 = _make_mr(10, "Shipment for 4.22.9", ["stage-release-success"])
        mr2 = _make_mr(11, "Shipment for 5.0.3", ["stage-release-success"])
        mr3 = _make_mr(12, "Shipment for logging 6.5.2", ["stage-release-success"])
        mr4 = _make_mr(13, "Shipment for 4.21.15", ["stage-release-success", "stage-testing-triggered"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr1, mr2, mr3, mr4]

        await scan_and_trigger(self.runtime)

        self.assertEqual(mock_jenkins.start_verify_stage_testing.call_count, 2)

    @patch(
        "pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info",
        new_callable=AsyncMock,
        return_value=IMAGE_AVAILABLE,
    )
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_triggers_rc_assembly(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mr = _make_mr(42, "Shipment for 5.0.0-rc.6 (ship date: 2026-10-15)", ["stage-release-success"])
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = [mr]

        await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_called_once_with("quay.io/openshift-release-dev/ocp-release:5.0.0-rc.6-x86_64", registry_config=None)
        mock_jenkins.start_verify_stage_testing.assert_called_once_with(
            version="5.0", assembly="rc.6", block_until_building=False
        )

    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.jenkins")
    @patch("pyartcd.pipelines.scheduled.schedule_stage_testing_scan.GitLabClient")
    async def test_no_mrs(self, mock_gitlab_cls, mock_jenkins, mock_get_image_info):
        mock_client = mock_gitlab_cls.from_url.return_value
        mock_client.list_merge_requests.return_value = []

        await scan_and_trigger(self.runtime)

        mock_get_image_info.assert_not_called()
        mock_jenkins.start_verify_stage_testing.assert_not_called()


if __name__ == "__main__":
    unittest.main()
