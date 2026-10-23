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
        self.mr.labels = ["verify-release-success", "unrelated"]
        self.command = self.enterContext(patch(f"{MODULE}.exectools.cmd_assert_async", new_callable=AsyncMock))
        self.update = self.gitlab.get_project.return_value.mergerequests.update
        self.inspect = self.enterContext(patch(f"{MODULE}.exectools.cmd_gather_async", new_callable=AsyncMock))
        self.advisories = [
            {"advisory_id": 100, "status": "QE", "error": None},
            {"advisory_id": 101, "status": "QE", "error": None},
        ]
        self.set_inspection()

    def set_inspection(self, rc=1, error=None):
        self.inspect.return_value = (rc, json.dumps({"advisories": self.advisories, "error": error}), "")

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

    async def test_metadata_failure_does_not_transition_or_mark(self):
        self.command.side_effect = ChildProcessError("metadata unavailable")
        with self.assertRaises(ChildProcessError):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 1)
        self.update.assert_not_called()

    async def test_partial_transition_failure_can_be_retried(self):
        self.command.side_effect = [None, None, ChildProcessError("second advisory failed")]
        with self.assertRaises(ChildProcessError):
            await self.pipeline.run()
        self.update.assert_not_called()
        self.command.reset_mock(side_effect=True)
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
        self.mr.labels = []
        with self.assertRaisesRegex(ValueError, "verify-release-success"):
            await self.pipeline.run()
        self.command.assert_not_called()
        self.update.assert_not_called()

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
                with self.assertRaisesRegex(ValueError, "Unable to determine"):
                    await self.pipeline.run()
                self.assertEqual(self.command.await_count, 1)
                self.update.assert_not_called()

    async def test_final_verification_failure_leaves_release_eligible_for_retry(self):
        self.command.side_effect = [None, None, None, ChildProcessError("QE advisory remains")]
        with self.assertRaisesRegex(ChildProcessError, "QE advisory remains"):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 4)
        self.update.assert_not_called()

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
        self.command.side_effect = [None, ChildProcessError("NEW_FILES")]
        with self.assertRaises(ChildProcessError):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 2)
        self.update.assert_not_called()

    async def test_inspection_command_error_stops_transitions(self):
        self.inspect.return_value = (2, "", "configuration error")
        with self.assertRaisesRegex(RuntimeError, "Unable to inspect"):
            await self.pipeline.run()
        self.assertEqual(self.command.await_count, 1)
        self.update.assert_not_called()
