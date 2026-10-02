import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.verify_cdn_push import VerifyCdnPushPipeline


class TestVerifyCdnPushPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = MagicMock(return_value=MagicMock(mkdir=MagicMock()))

    @patch("pyartcd.pipelines.verify_cdn_push.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_all_pushes_complete_immediately(self, mock_gather):
        result_json = {"passed": True, "failed": False, "advisories": []}
        mock_gather.return_value = (0, json.dumps(result_json), "")

        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )
        result = await pipeline.run()

        self.assertTrue(result["passed"])
        mock_gather.assert_called_once()

    @patch("pyartcd.pipelines.verify_cdn_push.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.verify_cdn_push.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_polls_until_complete(self, mock_gather, mock_sleep):
        pending = {
            "passed": False,
            "failed": False,
            "advisories": [{"advisory_id": 123, "pending": True, "push_jobs": []}],
        }
        complete = {"passed": True, "failed": False, "advisories": []}
        mock_gather.side_effect = [
            (1, json.dumps(pending), ""),
            (0, json.dumps(complete), ""),
        ]

        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
            poll_interval=10,
        )
        result = await pipeline.run()

        self.assertTrue(result["passed"])
        self.assertEqual(mock_gather.call_count, 2)
        mock_sleep.assert_called_once_with(10)

    @patch("pyartcd.pipelines.verify_cdn_push.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.verify_cdn_push.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_blocking_dependency_keeps_polling(self, mock_gather, mock_sleep):
        blocking = {
            "passed": False,
            "failed": False,
            "advisories": [
                {
                    "advisory_id": 123,
                    "pending": True,
                    "push_jobs": [],
                }
            ],
        }
        complete = {"passed": True, "failed": False, "advisories": []}
        mock_gather.side_effect = [
            (1, json.dumps(blocking), ""),
            (0, json.dumps(complete), ""),
        ]

        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
            poll_interval=10,
        )
        result = await pipeline.run()

        self.assertTrue(result["passed"])
        self.assertEqual(mock_gather.call_count, 2)
        mock_sleep.assert_called_once_with(10)

    @patch("pyartcd.pipelines.verify_cdn_push.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_hard_failure_fails_fast(self, mock_gather):
        failed = {
            "passed": False,
            "failed": True,
            "advisories": [
                {
                    "advisory_id": 123,
                    "failed": True,
                    "error": "error checking push status: 500 Server Error",
                    "push_jobs": [],
                }
            ],
        }
        mock_gather.return_value = (1, json.dumps(failed), "")

        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )
        result = await pipeline.run()

        self.assertFalse(result["passed"])
        self.assertTrue(result["failed"])
        mock_gather.assert_called_once()

    @patch("pyartcd.pipelines.verify_cdn_push.asyncio.sleep", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.verify_cdn_push.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_timeout(self, mock_gather, mock_sleep):
        pending = {
            "passed": False,
            "failed": False,
            "advisories": [{"advisory_id": 123, "pending": True, "push_jobs": []}],
        }
        mock_gather.return_value = (1, json.dumps(pending), "")

        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
            poll_interval=10,
            timeout=20,
        )

        with self.assertRaises(TimeoutError):
            await pipeline.run()

    @patch("pyartcd.pipelines.verify_cdn_push.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_invalid_json_raises(self, mock_gather):
        mock_gather.return_value = (1, "not json", "some error")

        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        with self.assertRaises(json.JSONDecodeError):
            await pipeline.run()

    @patch("pyartcd.pipelines.verify_cdn_push.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_unexpected_state_raises(self, mock_gather):
        unexpected = {"passed": False, "failed": False, "advisories": []}
        mock_gather.return_value = (0, json.dumps(unexpected), "")

        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        with self.assertRaises(RuntimeError):
            await pipeline.run()

    def test_elliott_cmd(self):
        pipeline = VerifyCdnPushPipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )
        cmd = pipeline._elliott_cmd
        self.assertIn("elliott", cmd)
        self.assertIn("--group=openshift-4.19", cmd)
        self.assertIn("--assembly=4.19.42", cmd)
        self.assertIn("verify-cdn-push", cmd)
        self.assertIn("--push", cmd)
        self.assertIn("-o", cmd)
        self.assertIn("json", cmd)


if __name__ == "__main__":
    unittest.main()
