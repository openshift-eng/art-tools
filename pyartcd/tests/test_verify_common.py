import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner
from pyartcd.pipelines.verify_advisory_transition import verify_advisory_transition_cli
from pyartcd.pipelines.verify_cdn_push import verify_cdn_push_cli
from pyartcd.pipelines.verify_release import StepResult, StepStatus, VerifyReleaseResult, verify_release_cli
from pyartcd.pipelines.verify_stage_testing import verify_stage_testing_cli
from pyartcd.runtime import Runtime
from pyartcd.verify_common import notify_verify_result

MODULE = "pyartcd.verify_common"
JENKINS_URL = "https://jenkins.example.com/job/verify/42"
COMMANDS = (
    (
        "pyartcd.pipelines.verify_release",
        verify_release_cli,
        "VerifyReleasePipeline",
        VerifyReleaseResult([StepResult("signatures", StepStatus.PASS)]),
        VerifyReleaseResult([StepResult("signatures", StepStatus.FAIL)]),
    ),
    (
        "pyartcd.pipelines.verify_cdn_push",
        verify_cdn_push_cli,
        "VerifyCdnPushPipeline",
        {"passed": True},
        {"passed": False, "failed": True},
    ),
    (
        "pyartcd.pipelines.verify_stage_testing",
        verify_stage_testing_cli,
        "StageTestingPipeline",
        {"passed": True, "state": "success"},
        {"passed": False, "state": "fail"},
    ),
    (
        "pyartcd.pipelines.verify_advisory_transition",
        verify_advisory_transition_cli,
        "VerifyAdvisoryTransitionPipeline",
        True,
        None,
    ),
)


class TestVerifySlack(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.addCleanup(asyncio.set_event_loop, None)
        self.addCleanup(self.loop.close)
        self.runtime = Runtime({}, Path.cwd(), dry_run=False)
        self.enterContext(
            patch.dict(
                "os.environ",
                {
                    "SLACK_BOT_TOKEN": "test-token",
                    "BUILD_URL": JENKINS_URL + "/",
                    "JOB_NAME": "verify",
                    "BUILD_ID": "42",
                },
            )
        )
        self.client_class = self.enterContext(patch("pyartcd.slack.AsyncWebClient"))
        self.post = AsyncMock(return_value=SimpleNamespace(data={"ts": "123.456"}))
        self.client_class.return_value.chat_postMessage = self.post

    def notify(self, passed=True):
        self.loop.run_until_complete(notify_verify_result(self.runtime, "4.22", "4.22.9", passed))

    def invoke(self, command, report_to_slack=True):
        args = ["--version", "4.22", "--assembly", "4.22.9"]
        if report_to_slack:
            args.append("--report-to-slack")
        return CliRunner().invoke(command, args, obj=self.runtime)

    def assert_message(self, status):
        self.post.assert_awaited_once()
        payload = self.post.call_args.kwargs
        self.assertEqual(payload["text"], {"PASS": ":white_check_mark: PASS", "FAIL": ":x: FAIL"}[status])
        self.assertEqual(payload["channel"], "#art-release-4-22")
        self.assertEqual(payload["username"], "art-release-bot")
        self.assertEqual(len(payload["attachments"]), 1)
        self.assertEqual(payload["attachments"][0]["title"], f"Job: verify <{JENKINS_URL}/consoleFull|42>")
        self.assertIsNone(payload["thread_ts"])
        self.assertFalse(payload["unfurl_links"])
        self.assertFalse(payload["unfurl_media"])

    def test_status_uses_standard_jenkins_job_attachment(self):
        for passed, status in ((True, "PASS"), (False, "FAIL")):
            with self.subTest(passed=passed):
                self.post.reset_mock()
                self.notify(passed)
                self.assert_message(status)

    def test_dry_run_does_not_initialize_slack(self):
        self.runtime.dry_run = True
        self.notify()
        self.client_class.assert_not_called()
        self.post.assert_not_called()

    def test_notification_errors_are_only_logged(self):
        for failure in ("credentials", "slack"):
            with self.subTest(failure=failure):
                self.post.reset_mock(side_effect=True)
                with patch.object(self.runtime, "new_slack_client", wraps=self.runtime.new_slack_client) as new_client:
                    if failure == "credentials":
                        new_client.side_effect = ValueError("No Slack credentials")
                    else:
                        self.post.side_effect = RuntimeError("Slack unavailable")
                    with self.assertLogs(MODULE, level="WARNING"):
                        self.notify()
                    if failure == "credentials":
                        self.post.assert_not_called()

    def test_missing_build_url_does_not_send(self):
        with patch.dict("os.environ", {"BUILD_URL": ""}), self.assertLogs(MODULE, level="WARNING"):
            self.notify()
        self.post.assert_not_called()

    def test_commands_without_report_flag_do_not_use_slack(self):
        for credentials in (False, True):
            env = {
                "SLACK_BOT_TOKEN": "test-token" if credentials else "",
                "BUILD_URL": JENKINS_URL if credentials else "",
            }
            with patch.dict("os.environ", env):
                for module, command, pipeline, success, _ in COMMANDS:
                    for error in (None, RuntimeError("verification failed")):
                        with self.subTest(command=command.name, credentials=credentials, error=error):
                            with patch(f"{module}.{pipeline}") as worker:
                                worker.return_value.run = AsyncMock(return_value=success, side_effect=error)
                                with self.assertNoLogs(MODULE, level="WARNING"):
                                    result = self.invoke(command, report_to_slack=False)
                            self.assertEqual(result.exit_code, 1 if error else 0, result.output)
                            if error:
                                self.assertIs(result.exception, error)
        self.client_class.assert_not_called()
        self.post.assert_not_called()

    def test_all_workers_notify_once_on_success(self):
        for module, command, pipeline, success, _ in COMMANDS:
            with self.subTest(command=command.name):
                self.post.reset_mock()
                with patch(f"{module}.{pipeline}") as worker:
                    worker.return_value.run = AsyncMock(return_value=success)
                    result = self.invoke(command)
                self.assertEqual(result.exit_code, 0, result.output)
                self.assert_message("PASS")

    def test_failed_checks_notify_once_and_keep_failure_exit(self):
        for module, command, pipeline, _, failed in COMMANDS:
            if failed is None:
                continue
            with self.subTest(command=command.name):
                self.post.reset_mock()
                with patch(f"{module}.{pipeline}") as worker:
                    worker.return_value.run = AsyncMock(return_value=failed)
                    result = self.invoke(command)
                self.assertEqual(result.exit_code, 1, result.output)
                self.assert_message("FAIL")

    def test_all_workers_preserve_execution_errors_and_notify_failure(self):
        for module, command, pipeline, _, _ in COMMANDS:
            with self.subTest(command=command.name):
                self.post.reset_mock()
                error = TimeoutError("verification timed out")
                with patch(f"{module}.{pipeline}") as worker:
                    worker.return_value.run = AsyncMock(side_effect=error)
                    result = self.invoke(command)
                self.assertIs(result.exception, error)
                self.assert_message("FAIL")

    def test_initialization_errors_notify_failure(self):
        for module, command, pipeline, _, _ in COMMANDS:
            with self.subTest(command=command.name):
                self.post.reset_mock()
                error = OSError("unable to prepare worker")
                with patch(f"{module}.{pipeline}", side_effect=error):
                    result = self.invoke(command)
                self.assertIs(result.exception, error)
                self.assert_message("FAIL")

    def test_slack_outage_does_not_fail_successful_workers(self):
        self.post.side_effect = RuntimeError("Slack unavailable")
        for module, command, pipeline, success, _ in COMMANDS:
            with self.subTest(command=command.name):
                self.post.reset_mock()
                with patch(f"{module}.{pipeline}") as worker:
                    worker.return_value.run = AsyncMock(return_value=success)
                    with self.assertLogs(MODULE, level="WARNING"):
                        result = self.invoke(command)
                self.assertEqual(result.exit_code, 0, result.output)
                self.post.assert_awaited_once()

    def test_slack_outage_does_not_replace_original_worker_error(self):
        self.post.side_effect = RuntimeError("Slack unavailable")
        for module, command, pipeline, _, _ in COMMANDS:
            with self.subTest(command=command.name):
                self.post.reset_mock()
                error = RuntimeError("verification failed")
                with patch(f"{module}.{pipeline}") as worker:
                    worker.return_value.run = AsyncMock(side_effect=error)
                    with self.assertLogs(MODULE, level="WARNING"):
                        result = self.invoke(command)
                self.assertIs(result.exception, error)
                self.post.assert_awaited_once()

    def test_already_passed_stage_testing_does_not_notify(self):
        with patch("pyartcd.pipelines.verify_stage_testing.StageTestingPipeline") as worker:
            worker.return_value.run = AsyncMock(return_value={"passed": True, "already_passed": True})
            result = self.invoke(verify_stage_testing_cli)
        self.assertEqual(result.exit_code, 0, result.output)
        self.post.assert_not_called()

    def test_skipped_advisory_jobs_do_not_report_pass(self):
        module = "pyartcd.pipelines.verify_advisory_transition"
        with (
            patch(
                f"{module}.util.load_group_config",
                new_callable=AsyncMock,
                return_value={"shipment": {"url": "https://gitlab.example.com/art/shipments/-/merge_requests/42"}},
            ),
            patch(f"{module}.GitLabClient.from_url") as gitlab,
            patch(f"{module}.exectools.cmd_assert_async", new_callable=AsyncMock) as command,
            patch(f"{module}.exectools.cmd_gather_async", new_callable=AsyncMock) as inspect,
        ):
            for labels in (
                ["advisory-transition-success"],
                ["verify-release-success"],
                ["stage-release-success", "verify-release-success"],
            ):
                with self.subTest(labels=labels):
                    gitlab.return_value.get_mr_from_url.return_value.labels = labels
                    result = self.invoke(verify_advisory_transition_cli)
                    self.assertEqual(result.exit_code, 0, result.output)
                    self.post.assert_not_called()
                    command.assert_not_called()
                    inspect.assert_not_called()

    def test_advisory_pass_requires_successful_final_state_verification(self):
        module = "pyartcd.pipelines.verify_advisory_transition"
        with (
            tempfile.TemporaryDirectory() as workdir,
            patch.object(self.runtime, "working_dir", Path(workdir)),
            patch.dict("os.environ", {"QUAY_AUTH_FILE": "/tmp/test-quay-auth.json"}),
            patch(
                f"{module}.util.load_group_config",
                new_callable=AsyncMock,
                return_value={"shipment": {"url": "https://gitlab.example.com/art/shipments/-/merge_requests/42"}},
            ),
            patch(f"{module}.GitLabClient.from_url") as gitlab,
            patch(
                f"{module}.exectools.cmd_gather_async",
                new_callable=AsyncMock,
                return_value=(1, json.dumps({"advisories": [{"advisory_id": 100, "status": "QE"}]}), ""),
            ),
            patch(f"{module}.exectools.cmd_assert_async", new_callable=AsyncMock) as command,
        ):
            gitlab.return_value.get_mr_from_url.return_value.labels = [
                "stage-release-success",
                "prod-release-success",
                "verify-release-success",
            ]
            for final_check_passes in (True, False):
                with self.subTest(final_check_passes=final_check_passes):
                    self.post.reset_mock()
                    command.reset_mock()

                    async def run_command(cmd):
                        self.post.assert_not_called()
                        if cmd[-1] == "--mark" and not final_check_passes:
                            raise ChildProcessError("advisory remains in QE")

                    command.side_effect = run_command
                    result = self.invoke(verify_advisory_transition_cli)
                    self.assertEqual(result.exit_code, 0 if final_check_passes else 1, result.output)
                    self.assert_message("PASS" if final_check_passes else "FAIL")
                    final_checks = [c for c in command.call_args_list if c.args[0][-1] == "--mark"]
                    self.assertEqual(len(final_checks), 1)

    def test_dry_run_never_notifies_worker_results_or_errors(self):
        self.runtime.dry_run = True
        for module, command, pipeline, success, _ in COMMANDS:
            for error in (None, RuntimeError("verification failed")):
                with self.subTest(command=command.name, error=error):
                    with patch(f"{module}.{pipeline}") as worker:
                        worker.return_value.run = AsyncMock(return_value=success, side_effect=error)
                        result = self.invoke(command)
                    self.assertEqual(result.exit_code, 1 if error else 0, result.output)
        self.post.assert_not_called()
        self.client_class.assert_not_called()
