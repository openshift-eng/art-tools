import json
import unittest
from unittest.mock import MagicMock, patch

from pyartcd.pipelines.push_to_cdn_staging import (
    AdvisoryPushResult,
    PushToCdnStagingPipeline,
    PushToCdnStagingResult,
    _classify_push_jobs,
    _push_advisory,
    render_result,
)


class TestClassifyPushJobs(unittest.TestCase):
    def test_empty_list(self):
        self.assertEqual(_classify_push_jobs([]), "none")

    def test_all_complete(self):
        jobs = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
            {"id": 2, "status": "COMPLETE", "target": {"name": "cdn_docker_stage"}},
        ]
        self.assertEqual(_classify_push_jobs(jobs), "complete")

    def test_one_running(self):
        jobs = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
            {"id": 2, "status": "RUNNING", "target": {"name": "cdn_docker_stage"}},
        ]
        self.assertEqual(_classify_push_jobs(jobs), "in_progress")

    def test_latest_job_wins(self):
        jobs = [
            {"id": 1, "status": "FAILED", "target": {"name": "cdn_stage"}},
            {"id": 5, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
        ]
        self.assertEqual(_classify_push_jobs(jobs), "complete")

    def test_latest_failed(self):
        jobs = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
            {"id": 5, "status": "FAILED", "target": {"name": "cdn_stage"}},
        ]
        self.assertEqual(_classify_push_jobs(jobs), "failed")

    def test_mixed_running_and_complete(self):
        jobs = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
            {"id": 2, "status": "QUEUED", "target": {"name": "cdn_docker_stage"}},
        ]
        self.assertEqual(_classify_push_jobs(jobs), "in_progress")

    def test_failed_takes_precedence_over_running(self):
        jobs = [
            {"id": 1, "status": "RUNNING", "target": {"name": "cdn_stage"}},
            {"id": 2, "status": "FAILED", "target": {"name": "cdn_docker_stage"}},
        ]
        self.assertEqual(_classify_push_jobs(jobs), "failed")


class TestPushAdvisory(unittest.TestCase):
    @patch("pyartcd.pipelines.push_to_cdn_staging.get_cdn_push_status")
    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_skips_dropped(self, mock_status, mock_push_status):
        mock_status.return_value = "DROPPED_NO_SHIP"
        result = _push_advisory(12345, "rpm", False, MagicMock())
        self.assertEqual(result.status, "skipped")
        mock_push_status.assert_not_called()

    @patch("pyartcd.pipelines.push_to_cdn_staging.get_cdn_push_status")
    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_already_complete(self, mock_status, mock_push_status):
        mock_status.return_value = "QE"
        mock_push_status.return_value = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
        ]
        result = _push_advisory(12345, "image", False, MagicMock())
        self.assertEqual(result.status, "complete")

    @patch("pyartcd.pipelines.push_to_cdn_staging.get_cdn_push_status")
    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_in_progress(self, mock_status, mock_push_status):
        mock_status.return_value = "QE"
        mock_push_status.return_value = [
            {"id": 1, "status": "COMPLETE", "target": {"name": "cdn_stage"}},
            {"id": 2, "status": "RUNNING", "target": {"name": "cdn_docker_stage"}},
        ]
        result = _push_advisory(12345, "image", False, MagicMock())
        self.assertEqual(result.status, "in_progress")
        self.assertTrue(len(result.push_jobs) > 0)

    @patch("pyartcd.pipelines.push_to_cdn_staging.ErrataConnector")
    @patch("pyartcd.pipelines.push_to_cdn_staging.get_cdn_push_status")
    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_triggers_push(self, mock_status, mock_push_status, mock_connector):
        mock_status.return_value = "QE"
        mock_push_status.return_value = []
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = [{"id": 10}]
        mock_connector.return_value._post.return_value = mock_response
        result = _push_advisory(12345, "extras", False, MagicMock())
        self.assertEqual(result.status, "triggered")
        mock_connector.return_value._post.assert_called_once_with("/api/v1/erratum/12345/push?defaults=stage")

    @patch("pyartcd.pipelines.push_to_cdn_staging.ErrataConnector")
    @patch("pyartcd.pipelines.push_to_cdn_staging.get_cdn_push_status")
    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_retriggers_after_failure(self, mock_status, mock_push_status, mock_connector):
        mock_status.return_value = "QE"
        mock_push_status.return_value = [
            {"id": 1, "status": "FAILED", "target": {"name": "cdn_stage"}},
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = [{"id": 10}]
        mock_connector.return_value._post.return_value = mock_response
        result = _push_advisory(12345, "rpm", False, MagicMock())
        self.assertEqual(result.status, "triggered")
        mock_connector.return_value._post.assert_called_once()

    @patch("pyartcd.pipelines.push_to_cdn_staging.ErrataConnector")
    @patch("pyartcd.pipelines.push_to_cdn_staging.get_cdn_push_status")
    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_dependency_blocked(self, mock_status, mock_push_status, mock_connector):
        mock_status.return_value = "QE"
        mock_push_status.return_value = []
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "advisory has push dependencies"
        mock_connector.return_value._post.return_value = mock_response
        result = _push_advisory(12345, "rpm", False, MagicMock())
        self.assertEqual(result.status, "dependency_blocked")

    @patch("pyartcd.pipelines.push_to_cdn_staging.get_cdn_push_status")
    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_dry_run(self, mock_status, mock_push_status):
        mock_status.return_value = "QE"
        mock_push_status.return_value = []
        result = _push_advisory(12345, "rpm", True, MagicMock())
        self.assertEqual(result.status, "triggered")

    @patch("pyartcd.pipelines.push_to_cdn_staging._get_advisory_status")
    def test_status_fetch_failure(self, mock_status):
        mock_status.side_effect = Exception("connection error")
        result = _push_advisory(12345, "rpm", False, MagicMock())
        self.assertEqual(result.status, "failed")
        self.assertIn("connection error", result.error)


class TestPipeline(unittest.IsolatedAsyncioTestCase):
    def _make_runtime(self):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.config = {}
        runtime.logger = MagicMock()
        runtime.dry_run = False
        return runtime

    @patch("pyartcd.pipelines.push_to_cdn_staging._push_advisory")
    @patch("pyartcd.pipelines.push_to_cdn_staging.load_assembly_group")
    async def test_run_triggered(self, mock_load, mock_push):
        mock_load.return_value = {
            "advisories": {"rpm": 100, "image": 200, "extras": 300},
        }
        mock_push.return_value = AdvisoryPushResult(
            errata_id=100,
            impetus="rpm",
            status="triggered",
        )

        pipeline = PushToCdnStagingPipeline(
            runtime=self._make_runtime(),
            group="openshift-4.20",
            assembly="4.20.1",
            data_path="",
        )
        result = await pipeline.run()
        self.assertFalse(result.passed)
        self.assertEqual(len(result.advisories), 3)

    @patch("pyartcd.pipelines.push_to_cdn_staging._push_advisory")
    @patch("pyartcd.pipelines.push_to_cdn_staging.load_assembly_group")
    async def test_run_complete(self, mock_load, mock_push):
        mock_load.return_value = {
            "advisories": {"rpm": 100, "image": 200},
        }
        mock_push.return_value = AdvisoryPushResult(
            errata_id=100,
            impetus="rpm",
            status="complete",
        )

        pipeline = PushToCdnStagingPipeline(
            runtime=self._make_runtime(),
            group="openshift-4.20",
            assembly="4.20.1",
            data_path="",
        )
        result = await pipeline.run()
        self.assertTrue(result.passed)

    @patch("pyartcd.pipelines.push_to_cdn_staging.load_assembly_group")
    async def test_run_no_assembly(self, mock_load):
        mock_load.return_value = None
        pipeline = PushToCdnStagingPipeline(
            runtime=self._make_runtime(),
            group="openshift-4.20",
            assembly="4.20.1",
            data_path="",
        )
        with self.assertRaises(RuntimeError):
            await pipeline.run()

    @patch("pyartcd.pipelines.push_to_cdn_staging._push_advisory")
    @patch("pyartcd.pipelines.push_to_cdn_staging.load_assembly_group")
    async def test_skips_microshift(self, mock_load, mock_push):
        mock_load.return_value = {
            "advisories": {"rpm": 100, "microshift": 200},
        }
        mock_push.return_value = AdvisoryPushResult(
            errata_id=100,
            impetus="rpm",
            status="complete",
        )

        pipeline = PushToCdnStagingPipeline(
            runtime=self._make_runtime(),
            group="openshift-4.20",
            assembly="4.20.1",
            data_path="",
        )
        result = await pipeline.run()
        self.assertEqual(len(result.advisories), 1)
        self.assertEqual(result.advisories[0].impetus, "rpm")

    @patch("pyartcd.pipelines.push_to_cdn_staging._push_advisory")
    @patch("pyartcd.pipelines.push_to_cdn_staging.load_assembly_group")
    async def test_run_with_failure(self, mock_load, mock_push):
        mock_load.return_value = {
            "advisories": {"rpm": 100},
        }
        mock_push.return_value = AdvisoryPushResult(
            errata_id=100,
            impetus="rpm",
            status="failed",
            error="Push trigger failed",
        )

        pipeline = PushToCdnStagingPipeline(
            runtime=self._make_runtime(),
            group="openshift-4.20",
            assembly="4.20.1",
            data_path="",
        )
        result = await pipeline.run()
        self.assertFalse(result.passed)
        self.assertEqual(len(result.errors), 1)


class TestRenderResult(unittest.TestCase):
    def _make_result(self):
        return PushToCdnStagingResult(
            group="openshift-4.20",
            assembly="4.20.1",
            advisories=[
                AdvisoryPushResult(errata_id=100, impetus="rpm", status="complete"),
                AdvisoryPushResult(errata_id=200, impetus="image", status="complete"),
            ],
        )

    def test_text_output(self):
        text = render_result(self._make_result(), "text")
        self.assertIn("openshift-4.20", text)
        self.assertIn("[COMPLETE]", text)
        self.assertIn("All advisory pushes triggered or already complete.", text)

    def test_text_in_progress(self):
        result = PushToCdnStagingResult(
            group="openshift-4.20",
            assembly="4.20.1",
            advisories=[
                AdvisoryPushResult(errata_id=100, impetus="rpm", status="in_progress"),
                AdvisoryPushResult(errata_id=200, impetus="image", status="complete"),
            ],
        )
        text = render_result(result, "text")
        self.assertIn("[IN PROGRESS]", text)
        self.assertIn("IN PROGRESS: Some advisory pushes are still running.", text)

    def test_json_output(self):
        raw = render_result(self._make_result(), "json")
        data = json.loads(raw)
        self.assertTrue(data["passed"])
        self.assertEqual(len(data["advisories"]), 2)
        self.assertEqual(data["advisories"][0]["status"], "complete")

    def test_text_failure(self):
        result = PushToCdnStagingResult(
            group="openshift-4.20",
            assembly="4.20.1",
            advisories=[
                AdvisoryPushResult(errata_id=100, impetus="rpm", status="failed", error="timeout"),
            ],
            errors=["rpm (100): timeout"],
        )
        text = render_result(result, "text")
        self.assertIn("FAILED", text)
        self.assertIn("timeout", text)


if __name__ == "__main__":
    unittest.main()
