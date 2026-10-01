import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.verify_stage_testing import StageTestingPipeline


class TestStageTestingPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = MagicMock(return_value=MagicMock(mkdir=MagicMock()))

    @patch("pyartcd.pipelines.verify_stage_testing.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_already_passed(self, mock_gather):
        label_result = json.dumps(
            {
                "passed": True,
                "label": "stage-testing-success",
                "label_found": True,
                "mr_url": "https://example.com/mr/1",
            }
        )
        mock_gather.return_value = (0, label_result, "")

        pipeline = StageTestingPipeline(runtime=self.runtime, version="4.22", assembly="4.22.9")
        result = await pipeline.run()

        self.assertTrue(result["passed"])
        self.assertTrue(result["already_passed"])
        mock_gather.assert_called_once()

    @patch("pyartcd.pipelines.verify_stage_testing.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_trigger_then_immediate_success(self, mock_gather):
        label_result = json.dumps(
            {"passed": False, "label": "stage-testing-success", "label_found": False, "mr_url": ""}
        )
        trigger_result = json.dumps({"job_id": "job-123", "job_name": "test-job"})
        job_result = json.dumps(
            {
                "job_id": "job-123",
                "job_name": "test-job",
                "state": "success",
                "passed": True,
                "terminal": True,
                "label_added": True,
            }
        )
        mock_gather.side_effect = [
            (1, label_result, ""),
            (0, trigger_result, ""),
            (0, job_result, ""),
        ]

        pipeline = StageTestingPipeline(runtime=self.runtime, version="4.22", assembly="4.22.9")
        result = await pipeline.run()

        self.assertTrue(result["passed"])
        self.assertEqual(result["state"], "success")
        self.assertEqual(mock_gather.call_count, 3)

    @patch("pyartcd.pipelines.verify_stage_testing.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.verify_stage_testing.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_poll_then_success(self, mock_gather, mock_sleep):
        label_result = json.dumps(
            {"passed": False, "label": "stage-testing-success", "label_found": False, "mr_url": ""}
        )
        trigger_result = json.dumps({"job_id": "job-123", "job_name": "test-job"})
        pending_result = json.dumps(
            {
                "job_id": "job-123",
                "job_name": "test-job",
                "state": "pending",
                "passed": False,
                "terminal": False,
            }
        )
        success_result = json.dumps(
            {
                "job_id": "job-123",
                "job_name": "test-job",
                "state": "success",
                "passed": True,
                "terminal": True,
                "label_added": True,
            }
        )
        mock_gather.side_effect = [
            (1, label_result, ""),
            (0, trigger_result, ""),
            (1, pending_result, ""),
            (0, success_result, ""),
        ]

        pipeline = StageTestingPipeline(runtime=self.runtime, version="4.22", assembly="4.22.9", poll_interval=10)
        result = await pipeline.run()

        self.assertTrue(result["passed"])
        self.assertEqual(mock_sleep.call_count, 1)

    @patch("pyartcd.pipelines.verify_stage_testing.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_trigger_then_failure(self, mock_gather):
        label_result = json.dumps(
            {"passed": False, "label": "stage-testing-success", "label_found": False, "mr_url": ""}
        )
        trigger_result = json.dumps({"job_id": "job-123", "job_name": "test-job"})
        job_result = json.dumps(
            {
                "job_id": "job-123",
                "job_name": "test-job",
                "state": "failure",
                "passed": False,
                "terminal": True,
            }
        )
        mock_gather.side_effect = [
            (1, label_result, ""),
            (0, trigger_result, ""),
            (1, job_result, ""),
        ]

        pipeline = StageTestingPipeline(runtime=self.runtime, version="4.22", assembly="4.22.9")
        result = await pipeline.run()

        self.assertFalse(result["passed"])
        self.assertEqual(result["state"], "failure")

    @patch("pyartcd.pipelines.verify_stage_testing.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.verify_stage_testing.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_timeout(self, mock_gather, mock_sleep):
        label_result = json.dumps(
            {"passed": False, "label": "stage-testing-success", "label_found": False, "mr_url": ""}
        )
        trigger_result = json.dumps({"job_id": "job-123", "job_name": "test-job"})
        pending_result = json.dumps(
            {
                "job_id": "job-123",
                "job_name": "test-job",
                "state": "pending",
                "passed": False,
                "terminal": False,
            }
        )
        mock_gather.side_effect = [
            (1, label_result, ""),
            (0, trigger_result, ""),
            (1, pending_result, ""),
            (1, pending_result, ""),
            (1, pending_result, ""),
        ]

        pipeline = StageTestingPipeline(
            runtime=self.runtime, version="4.22", assembly="4.22.9", poll_interval=10, timeout=25
        )

        with self.assertRaises(TimeoutError):
            await pipeline.run()

    @patch("pyartcd.pipelines.verify_stage_testing.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_commands_built_correctly(self, mock_gather):
        label_result = json.dumps(
            {"passed": False, "label": "stage-testing-success", "label_found": False, "mr_url": ""}
        )
        trigger_result = json.dumps({"job_id": "job-123", "job_name": "test-job"})
        job_result = json.dumps(
            {
                "job_id": "job-123",
                "job_name": "test-job",
                "state": "success",
                "passed": True,
                "terminal": True,
            }
        )
        mock_gather.side_effect = [
            (1, label_result, ""),
            (0, trigger_result, ""),
            (0, job_result, ""),
        ]

        pipeline = StageTestingPipeline(runtime=self.runtime, version="4.22", assembly="4.22.9")
        await pipeline.run()

        label_cmd = mock_gather.call_args_list[0].args[0]
        self.assertIn("verify-stage-testing", label_cmd)
        self.assertNotIn("--trigger", label_cmd)
        self.assertNotIn("--job-id", label_cmd)

        trigger_cmd = mock_gather.call_args_list[1].args[0]
        self.assertIn("verify-stage-testing", trigger_cmd)
        self.assertIn("--trigger", trigger_cmd)

        check_cmd = mock_gather.call_args_list[2].args[0]
        self.assertIn("--job-id=job-123", check_cmd)

    @patch("pyartcd.pipelines.verify_stage_testing.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_label_check_failure_raises(self, mock_gather):
        mock_gather.return_value = (1, "", "some error")

        pipeline = StageTestingPipeline(runtime=self.runtime, version="4.22", assembly="4.22.9")

        with self.assertRaises(RuntimeError):
            await pipeline.run()


if __name__ == "__main__":
    unittest.main()
