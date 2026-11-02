import os
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

        env_patcher = patch.dict(
            os.environ,
            {
                "QUAY_AUTH_FILE": "/tmp/quay-auth.json",
                "XDG_RUNTIME_DIR": "/tmp/runtime",
            },
        )
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

        registry_config_patcher = patch("pyartcd.pipelines.verify_release.RegistryConfig")
        self.mock_registry_config = registry_config_patcher.start()
        self.addCleanup(registry_config_patcher.stop)
        self.mock_registry_config.return_value.__enter__.return_value = "/tmp/merged-auth.json"

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_shipment_label_forwards_dry_run(self, mock_assert):
        self.runtime.dry_run = True
        pipeline = VerifyReleasePipeline(runtime=self.runtime, version="4.19", assembly="4.19.42")

        result = await pipeline.run()

        self.assertTrue(result.passed)
        label_commands = [call.args[0] for call in mock_assert.call_args_list if "verify-release" in call.args[0]]
        self.assertEqual(len(label_commands), 1)
        self.assertEqual(label_commands[0][-3:], ["verify-release", "--mark", "--dry-run"])

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
        expected_steps = {
            "cdn-push",
            "csv-versions",
            "signatures",
            "image-grades",
            "payload",
            "qe-qualifier",
            "security-alerts",
            "kernel-tag",
            "cve-trackers",
            "stage-testing",
            "shipment-label",
        }
        self.assertEqual({s.name for s in result.steps}, expected_steps)
        for step in result.steps:
            self.assertEqual(step.status, StepStatus.PASS)

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_run_passes_registry_auth_to_elliott_commands(self, mock_assert):
        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        await pipeline.run()

        for call in mock_assert.call_args_list:
            command = call.args[0]
            child_env = call.kwargs["env"]
            self.assertIn("--registry-config=/tmp/merged-auth.json", command)
            self.assertEqual(child_env["QUAY_AUTH_FILE"], "/tmp/merged-auth.json")
            self.assertNotIn("REGISTRY_AUTH_FILE", child_env)
            self.assertNotIn("XDG_RUNTIME_DIR", child_env)

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
            if "verify-signatures" in cmd or "verify-csv-versions" in cmd:
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
        csv_step = [s for s in result.steps if s.name == "csv-versions"][0]
        self.assertEqual(csv_step.status, StepStatus.FAIL)
        self.assertIn("exited with code 1", csv_step.message)

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

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_shipment_label_runs_after_checks_pass(self, mock_assert):
        mock_assert.return_value = 0

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        result = await pipeline.run()

        self.assertTrue(result.passed)
        label_step = [s for s in result.steps if s.name == "shipment-label"][0]
        self.assertEqual(label_step.status, StepStatus.PASS)
        label_calls = [
            call for call in mock_assert.call_args_list if "verify-release" in str(call) and "--mark" in str(call)
        ]
        self.assertEqual(len(label_calls), 1)

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_failed_checks_mark_failure(self, mock_assert):
        def mock_side_effect(cmd, **kwargs):
            if "verify-signatures" in cmd:
                raise ChildProcessError("Process exited with code 1.")
            return 0

        mock_assert.side_effect = mock_side_effect

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        result = await pipeline.run()

        self.assertFalse(result.passed)
        label_steps = [s for s in result.steps if s.name == "shipment-label"]
        self.assertEqual(len(label_steps), 1)
        self.assertEqual(label_steps[0].status, StepStatus.PASS)
        self.assertEqual(mock_assert.call_args.args[0][-2:], ["verify-release", "--mark-failed"])

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_shipment_label_skip_by_user(self, mock_assert):
        mock_assert.return_value = 0

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
            skip_steps=["shipment-label"],
        )

        result = await pipeline.run()

        self.assertTrue(result.passed)
        label_step = [s for s in result.steps if s.name == "shipment-label"][0]
        self.assertEqual(label_step.status, StepStatus.SKIP)

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_shipment_label_failure_reported(self, mock_assert):
        def mock_side_effect(cmd, **kwargs):
            if "verify-release" in cmd and "--mark" in str(cmd):
                raise ChildProcessError("GITLAB_TOKEN not set")
            return 0

        mock_assert.side_effect = mock_side_effect

        pipeline = VerifyReleasePipeline(
            runtime=self.runtime,
            version="4.19",
            assembly="4.19.42",
        )

        result = await pipeline.run()

        self.assertFalse(result.passed)
        label_step = [s for s in result.steps if s.name == "shipment-label"][0]
        self.assertEqual(label_step.status, StepStatus.FAIL)

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_failed_checks_forward_dry_run(self, command):
        self.runtime.dry_run = True
        command.side_effect = lambda cmd, **kwargs: self.raise_check_error() if "verify-signatures" in cmd else None
        pipeline = VerifyReleasePipeline(self.runtime, "4.22", "4.22.9")
        result = await pipeline.run()
        self.assertFalse(result.passed)
        self.assertEqual(command.call_args.args[0][-3:], ["verify-release", "--mark-failed", "--dry-run"])

    @staticmethod
    def raise_check_error():
        raise ChildProcessError("signature check failed")

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_skip_shipment_label_suppresses_failure_marking(self, command):
        command.side_effect = lambda cmd, **kwargs: self.raise_check_error() if "verify-signatures" in cmd else None
        pipeline = VerifyReleasePipeline(self.runtime, "4.22", "4.22.9", skip_steps=["shipment-label"])
        result = await pipeline.run()
        self.assertFalse(result.passed)
        self.assertFalse(any("verify-release" in c.args[0] for c in command.call_args_list))

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_missing_registry_credentials_marks_failure_and_preserves_error(self, command):
        os.environ.pop("QUAY_AUTH_FILE", None)
        pipeline = VerifyReleasePipeline(self.runtime, "4.22", "4.22.9")
        with self.assertRaisesRegex(ValueError, "QUAY_AUTH_FILE"):
            await pipeline.run()
        command.assert_awaited_once()
        self.assertEqual(command.call_args.args[0][-2:], ["verify-release", "--mark-failed"])

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_registry_setup_failure_is_marked(self, command):
        self.mock_registry_config.return_value.__enter__.side_effect = RuntimeError("registry setup failed")
        pipeline = VerifyReleasePipeline(self.runtime, "4.22", "4.22.9")
        with self.assertRaisesRegex(RuntimeError, "registry setup failed"):
            await pipeline.run()
        self.assertEqual(command.call_args.args[0][-2:], ["verify-release", "--mark-failed"])

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_setup_error_is_preserved_if_failure_marking_fails(self, command):
        os.environ.pop("QUAY_AUTH_FILE", None)
        command.side_effect = ChildProcessError("GitLab unavailable")
        pipeline = VerifyReleasePipeline(self.runtime, "4.22", "4.22.9")
        with self.assertRaisesRegex(ValueError, "QUAY_AUTH_FILE"):
            await pipeline.run()

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_setup_failure_with_skip_or_dry_run_never_marks(self, command):
        os.environ.pop("QUAY_AUTH_FILE", None)
        for dry_run, skips in ((True, []), (False, ["shipment-label"])):
            with self.subTest(dry_run=dry_run, skips=skips):
                self.runtime.dry_run = dry_run
                pipeline = VerifyReleasePipeline(self.runtime, "4.22", "4.22.9", skip_steps=skips)
                with self.assertRaises(ValueError):
                    await pipeline.run()
                command.assert_not_called()

    @patch("pyartcd.pipelines.verify_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_failed_success_marking_also_attempts_fail_label(self, command):
        def run(cmd, **kwargs):
            if "verify-release" in cmd and "--mark" in cmd:
                raise ChildProcessError("success marking failed")

        command.side_effect = run
        pipeline = VerifyReleasePipeline(self.runtime, "4.22", "4.22.9")
        result = await pipeline.run()
        self.assertFalse(result.passed)
        labels = [c.args[0][-1] for c in command.call_args_list if "verify-release" in c.args[0]]
        self.assertEqual(labels, ["--mark", "--mark-failed"])
        self.assertEqual(result.steps[-1].name, "shipment-failure-label")


if __name__ == "__main__":
    unittest.main()
