import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.verify_advisory_transition import VerifyAdvisoryTransitionPipeline

MODULE = "pyartcd.pipelines.verify_advisory_transition"
MR_URL = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42"


class TestVerifyAdvisoryTransition(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = self.enterContext(tempfile.TemporaryDirectory())
        self.quay_auth_file = Path(self.temp_dir) / "quay-auth.json"
        self.quay_auth_file.write_text(
            json.dumps(
                {
                    "auths": {
                        "quay.io/openshift-release-dev": {"auth": "test-namespace-auth"},
                        "quay.io": {"auth": "test-unrelated-auth"},
                    }
                }
            )
        )
        self.enterContext(patch.dict("os.environ", {"QUAY_AUTH_FILE": str(self.quay_auth_file)}))
        self.runtime = MagicMock(dry_run=False, working_dir=Path(self.temp_dir))
        self.pipeline = VerifyAdvisoryTransitionPipeline(self.runtime, "4.22", "4.22.9")
        self.config = {
            "advisories": {"rpm": 100, "rhcos": 101, "microshift": 102},
            "shipment": {"url": MR_URL},
        }
        self.load_config = self.enterContext(patch(f"{MODULE}.util.load_group_config", new_callable=AsyncMock))
        self.load_config.return_value = self.config
        self.gitlab = self.enterContext(patch(f"{MODULE}.GitLabClient.from_url")).return_value
        self.mr = self.gitlab.get_mr_from_url.return_value
        self.mr.labels = ["stage-release-success", "prod-release-success", "verify-release-success", "unrelated"]
        self.command = self.enterContext(patch(f"{MODULE}.exectools.cmd_assert_async", new_callable=AsyncMock))
        self.update = self.gitlab.add_mr_labels
        self.inspect = self.enterContext(patch(f"{MODULE}.exectools.cmd_gather_async", new_callable=AsyncMock))
        self.advisories = [
            {"advisory_id": 100, "status": "QE", "error": None},
            {"advisory_id": 101, "status": "QE", "error": None},
        ]
        self.set_inspection()

    def set_inspection(self, rc=1, error=None):
        self.inspect.return_value = (rc, json.dumps({"advisories": self.advisories, "error": error}), "")

    @staticmethod
    def raise_metadata_error():
        raise ChildProcessError("metadata unavailable")

    def assert_failure_marked(self):
        self.assertEqual(self.command.call_args.args[0][-2:], ["verify-advisory-status", "--mark-failed"])
        self.update.assert_not_called()

    async def test_metadata_precedes_transitions_and_label_is_added_last(self):
        async def run_command(cmd):
            self.update.assert_not_called()

        self.command.side_effect = run_command
        await self.pipeline.run()
        commands = [c.args[0] for c in self.command.call_args_list]
        self.assertEqual(commands[0][-1], "verify-metadata-url")
        self.assertEqual(
            [cmd[-7:] for cmd in commands[1:-1]],
            [
                ["change-state", "-s", "REL_PREP", "--from", "QE", "--advisory", "100"],
                ["change-state", "-s", "REL_PREP", "--from", "QE", "--advisory", "101"],
            ],
        )
        self.assertEqual(commands[-1][-2:], ["verify-advisory-status", "--mark"])
        self.inspect.assert_awaited_once()
        self.update.assert_not_called()

    async def test_metadata_uses_prepared_quay_auth_and_removes_temp_file(self):
        auth_paths = []

        async def run_command(cmd):
            if cmd[-1] != "verify-metadata-url":
                return
            auth_path = Path(next(arg.split("=", 1)[1] for arg in cmd if arg.startswith("--registry-config=")))
            auth_paths.append(auth_path)
            self.assertNotEqual(auth_path, self.quay_auth_file)
            self.assertEqual(
                json.loads(auth_path.read_text()),
                {
                    "auths": {
                        "quay.io/openshift-release-dev": {"auth": "test-namespace-auth"},
                    }
                },
            )

        self.command.side_effect = run_command
        await self.pipeline.run()

        self.assertEqual(len(auth_paths), 1)
        self.assertFalse(auth_paths[0].exists())
        self.assertTrue(self.quay_auth_file.exists())

    async def test_missing_quay_credentials_marks_failure_without_transitioning(self):
        with patch.dict("os.environ", {"QUAY_AUTH_FILE": ""}):
            with self.assertRaisesRegex(ValueError, "QUAY_AUTH_FILE"):
                await self.pipeline.run()

        self.assertEqual(self.command.await_count, 1)
        self.assert_failure_marked()
        self.inspect.assert_not_called()

    async def test_closed_and_merged_mrs_are_supported(self):
        for state in ("closed", "merged"):
            with self.subTest(state=state):
                self.mr.state = state
                self.command.reset_mock()
                self.update.reset_mock()
                await self.pipeline.run()
                self.assertEqual(self.command.await_count, 4)
                self.assertEqual(self.command.call_args.args[0][-2:], ["verify-advisory-status", "--mark"])
                self.update.assert_not_called()

    async def test_metadata_failure_marks_fail_without_transitioning(self):
        self.command.side_effect = lambda cmd: None if "--mark-failed" in cmd else self.raise_metadata_error()
        with self.assertRaises(ChildProcessError):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 2)
        self.assert_failure_marked()

    async def test_partial_transition_failure_can_be_retried(self):
        self.command.side_effect = [None, None, ChildProcessError("second advisory failed"), None]
        with self.assertRaises(ChildProcessError):
            await self.pipeline.run()
        self.assert_failure_marked()
        self.command.reset_mock(side_effect=True)
        self.update.reset_mock()
        self.mr.labels.append("advisory-transition-fail")
        await self.pipeline.run()
        # --from QE lets Elliott skip the advisory already moved on the first attempt.
        for c in self.command.call_args_list[1:-1]:
            self.assertIn("QE", c.args[0])
        self.assertEqual(self.command.call_args.args[0][-2:], ["verify-advisory-status", "--mark"])
        self.update.assert_not_called()

    async def test_dry_run_passes_dry_run_to_transitions_and_does_not_mark(self):
        self.runtime.dry_run = True
        await self.pipeline.run()
        commands = [c.args[0] for c in self.command.call_args_list]
        self.assertEqual(commands[0][-1], "verify-metadata-url")
        for cmd in commands[1:]:
            self.assertEqual(cmd[-1], "--dry-run")
        self.update.assert_not_called()

    async def test_already_completed_run_does_no_work(self):
        self.mr.labels.append("advisory-transition-success")
        await self.pipeline.run()
        self.command.assert_not_called()
        self.update.assert_not_called()

    async def test_unverified_release_is_rejected(self):
        self.mr.labels = ["stage-release-success", "prod-release-success"]
        with self.assertRaisesRegex(ValueError, "verify-release-success"):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 1)
        self.assert_failure_marked()

    async def test_missing_stage_success_waits_without_commands_or_failure_labels(self):
        for labels in (
            ["prod-release-success", "verify-release-success"],
            ["verify-release-success"],
            ["verify-release-success", "stage-release-skipped"],
            ["verify-release-success", "stage-release-fail"],
            ["verify-release-success", "advisory-transition-fail"],
            [],
            None,
        ):
            for dry_run in (False, True):
                with self.subTest(labels=labels, dry_run=dry_run):
                    self.mr.labels = labels
                    self.runtime.dry_run = dry_run
                    with self.assertLogs(MODULE, level="INFO") as logs:
                        await self.pipeline.run()
                    self.assertIn("Waiting for stage-release-success", "\n".join(logs.output))
                    self.command.assert_not_called()
                    self.inspect.assert_not_called()
                    self.update.assert_not_called()
                    self.assertFalse(self.pipeline.working_dir.exists())

    async def test_release_can_be_retried_after_stage_succeeds(self):
        self.mr.labels = ["prod-release-success", "verify-release-success"]
        await self.pipeline.run()
        self.command.assert_not_called()
        self.inspect.assert_not_called()
        self.mr.labels.append("stage-release-success")
        await self.pipeline.run()
        self.assertEqual(self.command.await_count, 4)
        self.assertEqual(self.command.call_args_list[0].args[0][-1], "verify-metadata-url")
        self.assertEqual(self.command.call_args.args[0][-2:], ["verify-advisory-status", "--mark"])

    async def test_missing_prod_success_waits_without_commands_or_failure_labels(self):
        for labels in (
            ["stage-release-success", "verify-release-success"],
            ["stage-release-success", "verify-release-success", "prod-release-fail"],
            ["stage-release-success", "verify-release-success", "advisory-transition-fail"],
            ["stage-release-success"],
        ):
            for dry_run in (False, True):
                with self.subTest(labels=labels, dry_run=dry_run):
                    self.mr.labels = labels
                    self.runtime.dry_run = dry_run
                    with self.assertLogs(MODULE, level="INFO") as logs:
                        await self.pipeline.run()
                    self.assertIn("Waiting for prod-release-success", "\n".join(logs.output))
                    self.command.assert_not_called()
                    self.inspect.assert_not_called()
                    self.update.assert_not_called()
                    self.assertFalse(self.pipeline.working_dir.exists())

    async def test_release_can_be_retried_after_prod_succeeds(self):
        self.mr.labels = ["stage-release-success", "verify-release-success"]
        await self.pipeline.run()
        self.command.assert_not_called()
        self.inspect.assert_not_called()
        self.mr.labels.append("prod-release-success")
        await self.pipeline.run()
        self.assertEqual(self.command.await_count, 4)
        self.assertEqual(self.command.call_args_list[0].args[0][-1], "verify-metadata-url")
        self.assertEqual(self.command.call_args.args[0][-2:], ["verify-advisory-status", "--mark"])

    async def test_missing_shipment_is_rejected(self):
        self.config.pop("shipment")
        with self.assertRaisesRegex(ValueError, "No shipment MR"):
            await self.pipeline.run()
        self.command.assert_not_called()

    async def test_invalid_advisory_inspection_is_rejected(self):
        for advisories in (
            [],
            [{"advisory_id": -1, "status": "QE"}],
            [{"advisory_id": 100, "status": None, "error": "API failed"}],
        ):
            with self.subTest(advisories=advisories):
                self.advisories = advisories
                self.set_inspection()
                self.command.reset_mock()
                self.update.reset_mock()
                with self.assertRaisesRegex(ValueError, "Unable to determine"):
                    await self.pipeline.run()
                self.assertEqual(self.command.await_count, 2)
                self.assert_failure_marked()

    async def test_final_verification_failure_marks_fail(self):
        self.command.side_effect = [None, None, None, ChildProcessError("QE advisory remains"), None]
        with self.assertRaisesRegex(ChildProcessError, "QE advisory remains"):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 5)
        self.assert_failure_marked()

    async def test_later_states_and_dropped_advisories_are_not_transitioned(self):
        for state in ("REL_PREP", "PUSH_READY", "IN_PUSH", "SHIPPED_LIVE", "DROPPED_NO_SHIP"):
            with self.subTest(state=state):
                self.advisories = [{"advisory_id": 100, "status": state}]
                self.set_inspection(rc=0)
                self.command.reset_mock()
                await self.pipeline.run()
                self.assertEqual(self.command.await_count, 2)
                self.assertEqual(self.command.call_args.args[0][-2:], ["verify-advisory-status", "--mark"])
                self.update.assert_not_called()

    async def test_new_files_is_not_transitioned_and_final_verifier_can_reject_it(self):
        self.advisories = [{"advisory_id": 100, "status": "NEW_FILES"}]
        self.set_inspection()
        self.command.side_effect = [None, ChildProcessError("NEW_FILES"), None]
        with self.assertRaises(ChildProcessError):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 3)
        self.assert_failure_marked()

    async def test_inspection_command_error_stops_transitions(self):
        self.inspect.return_value = (2, "", "configuration error")
        with self.assertRaisesRegex(RuntimeError, "Unable to inspect"):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 2)
        self.assert_failure_marked()

    async def test_configuration_failure_does_not_mark_an_unknown_mr(self):
        self.load_config.side_effect = RuntimeError("build-data unavailable")
        with self.assertRaisesRegex(RuntimeError, "build-data unavailable"):
            await self.pipeline.run()
        self.update.assert_not_called()
        self.command.assert_not_called()

    async def test_dry_run_failure_never_marks_fail(self):
        self.runtime.dry_run = True
        self.command.side_effect = lambda cmd: None if "--mark-failed" in cmd else self.raise_metadata_error()
        with self.assertRaisesRegex(ChildProcessError, "metadata unavailable"):
            await self.pipeline.run()
        self.update.assert_not_called()

    async def test_failure_marking_error_preserves_original_error(self):
        self.command.side_effect = lambda cmd: None if "--mark-failed" in cmd else self.raise_metadata_error()
        self.command.side_effect = [ChildProcessError("metadata unavailable"), ChildProcessError("marking failed")]
        with self.assertLogs(MODULE, level="ERROR") as logs:
            with self.assertRaisesRegex(ChildProcessError, "metadata unavailable"):
                await self.pipeline.run()
        self.assertIn("Unable to mark advisory transition failure", logs.output[0])
        self.assert_failure_marked()

    async def test_malformed_inspection_marks_fail(self):
        self.inspect.return_value = (1, "not-json", "")
        with self.assertRaises(json.JSONDecodeError):
            await self.pipeline.run()
        self.assert_failure_marked()
