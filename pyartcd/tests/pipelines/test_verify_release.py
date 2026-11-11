import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from elliottlib.cli.verify_qe_qualifier_cli import QualifierCheckResult, verify_qe_qualifier_cli
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
    async def test_qe_gate_depends_only_on_stable_qualifier(self, mock_assert):
        elliott_runtime = MagicMock()
        elliott_runtime.assembly = "4.19.42"
        nightly_tag = "4.19.0-0.nightly-2026-11-10-000000"

        def invoke_qe_check(command):
            # Elliott's Click wrapper requires an event loop in the invoking thread.
            with asyncio.Runner() as runner:
                asyncio.set_event_loop(runner.get_loop())
                return CliRunner().invoke(
                    verify_qe_qualifier_cli,
                    command[command.index("verify-qe-qualifier") + 1 :],
                    obj=elliott_runtime,
                )

        async def run_command(command, **kwargs):
            if "verify-qe-qualifier" in command:
                cli_result = await asyncio.to_thread(invoke_qe_check, command)
                if cli_result.exit_code:
                    raise ChildProcessError(cli_result.output)
            return 0

        mock_assert.side_effect = run_command
        for stable_badge, nightly_badge in [(True, False), (True, True), (False, True), (False, False)]:
            with self.subTest(stable_badge=stable_badge, nightly_badge=nightly_badge):
                mock_assert.reset_mock()

                async def check_qualifier(tag, arch, session):
                    return QualifierCheckResult(
                        release_tag=tag,
                        arch=arch,
                        badge_earned=stable_badge if tag == elliott_runtime.assembly else nightly_badge,
                    )

                with (
                    patch(
                        "elliottlib.cli.verify_qe_qualifier_cli.assembly_basis",
                        return_value={"reference_releases": {"x86_64": nightly_tag}},
                    ),
                    patch(
                        "elliottlib.cli.verify_qe_qualifier_cli.check_qe_qualifier", side_effect=check_qualifier
                    ) as mock_check,
                ):
                    pipeline = VerifyReleasePipeline(runtime=self.runtime, version="4.19", assembly="4.19.42")
                    result = await pipeline.run()

                self.assertEqual(result.passed, stable_badge, result.summary())
                qe_step = next(step for step in result.steps if step.name == "qe-qualifier")
                self.assertEqual(qe_step.status, StepStatus.PASS if stable_badge else StepStatus.FAIL)
                self.assertEqual(mock_check.await_count, 1)
                self.assertEqual(mock_check.await_args.args[:2], ("4.19.42", "amd64"))
                label_commands = [call for call in mock_assert.call_args_list if "verify-release" in call.args[0]]
                self.assertEqual(len(label_commands), 1 if stable_badge else 0)

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
    async def test_shipment_label_skipped_when_checks_fail(self, mock_assert):
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
        self.assertEqual(len(label_steps), 0)

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


if __name__ == "__main__":
    unittest.main()
