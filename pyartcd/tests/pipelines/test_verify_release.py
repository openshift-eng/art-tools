import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.verify_release import (
    StepResult,
    StepStatus,
    VerifyReleasePipeline,
    VerifyReleaseResult,
)


class TestVerifyReleaseResult(unittest.TestCase):
    def test_passed_all_pass(self):
        result = VerifyReleaseResult(
            steps=[
                StepResult("cdn-push", StepStatus.PASS),
                StepResult("signatures", StepStatus.PASS),
            ]
        )
        self.assertTrue(result.passed)

    def test_passed_with_skip(self):
        result = VerifyReleaseResult(
            steps=[
                StepResult("cdn-push", StepStatus.SKIP, "skipped by user"),
                StepResult("signatures", StepStatus.PASS),
            ]
        )
        self.assertTrue(result.passed)

    def test_failed_on_fail(self):
        result = VerifyReleaseResult(
            steps=[
                StepResult("cdn-push", StepStatus.PASS),
                StepResult("signatures", StepStatus.FAIL, "signature check failed"),
            ]
        )
        self.assertFalse(result.passed)

    def test_summary(self):
        result = VerifyReleaseResult(
            steps=[
                StepResult("cdn-push", StepStatus.PASS),
                StepResult("signatures", StepStatus.FAIL, "signature check failed"),
                StepResult("payload", StepStatus.SKIP, "skipped by user"),
            ]
        )
        summary = result.summary()
        self.assertIn("cdn-push: PASS", summary)
        self.assertIn("signatures: FAIL — signature check failed", summary)
        self.assertIn("payload: SKIP — skipped by user", summary)


class TestVerifyReleasePipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.dry_run = False
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = MagicMock(return_value=MagicMock(mkdir=MagicMock()))

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_run_all_steps_pass(self, mock_assert):
        mock_assert.return_value = 0

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        result = await pipeline.run()

        self.assertTrue(result.passed)
        self.assertEqual(len(result.steps), 8)
        for step in result.steps:
            self.assertEqual(step.status, StepStatus.PASS)

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_run_with_skip_steps(self, mock_assert):
        mock_assert.return_value = 0

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
            skip_steps=["cdn-push", "kernel-tag"],
        )

        result = await pipeline.run()

        self.assertTrue(result.passed)
        skipped_steps = [s for s in result.steps if s.status == StepStatus.SKIP]
        self.assertEqual(len(skipped_steps), 2)
        self.assertIn("cdn-push", [s.name for s in skipped_steps])
        self.assertIn("kernel-tag", [s.name for s in skipped_steps])

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_run_step_failure(self, mock_assert):
        def mock_side_effect(cmd, **kwargs):
            if "verify-signatures" in cmd:
                raise ChildProcessError(f"Process {cmd!r} exited with code 1.")
            return 0

        mock_assert.side_effect = mock_side_effect

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        result = await pipeline.run()

        self.assertFalse(result.passed)
        sig_step = [s for s in result.steps if s.name == "signatures"][0]
        self.assertEqual(sig_step.status, StepStatus.FAIL)
        self.assertIn("exited with code 1", sig_step.message)

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_cdn_push_read_only(self, mock_assert):
        mock_assert.return_value = 0

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        await pipeline.run()

        cdn_push_calls = [call for call in mock_assert.call_args_list if "verify-cdn-push" in str(call)]
        self.assertTrue(any("--no-push" in str(call) for call in cdn_push_calls))


if __name__ == "__main__":
    unittest.main()
