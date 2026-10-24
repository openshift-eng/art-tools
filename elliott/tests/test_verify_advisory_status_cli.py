import asyncio
import json
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Model
from click.testing import CliRunner
from elliottlib import constants
from elliottlib.cli.verify_advisory_status_cli import verify_advisory_status_cli
from elliottlib.verify_common import get_ocp_release_advisory_ids

MODULE = "elliottlib.cli.verify_advisory_status_cli"
MR_URL = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42"


class TestVerifyAdvisoryStatusCLI(TestCase):
    def setUp(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.addCleanup(loop.close)
        self.addCleanup(asyncio.set_event_loop, None)
        self.runtime = MagicMock(spec=["initialize", "dry_run", "group", "get_releases_config", "assembly"])
        self.runtime.group = "openshift-4.22"
        self.runtime.assembly = "4.22.9"
        self.states = {100: "REL_PREP", 101: "REL_PREP", 102: "QE"}
        api_cls = self.enterContext(patch(f"{MODULE}.AsyncErrataAPI"))
        self.api = api_cls.return_value.__aenter__.return_value
        self.api.get_advisory_state = AsyncMock(side_effect=lambda advisory_id: self.states.get(advisory_id))
        self.gitlab_cls = self.enterContext(patch(f"{MODULE}.GitLabClient"))
        self.client = self.gitlab_cls.from_url.return_value
        self.update = self.client.add_mr_labels
        self.assembly_ids = self.enterContext(patch(f"{MODULE}.get_ocp_release_advisory_ids"))
        self.assembly_ids.return_value = {"rpm": 100}
        self.shipment_url = self.enterContext(patch(f"{MODULE}.get_assembly_shipment_url", return_value=MR_URL))

    def invoke(self, *args):
        return CliRunner().invoke(verify_advisory_status_cli, [*args, "-o", "json"], obj=self.runtime)

    def test_reads_assembly_advisories_without_gitlab_when_not_marking(self):
        self.assembly_ids.return_value = {"rpm": 100, "rhcos": 101}
        result = self.invoke()
        self.assertEqual(result.exit_code, 0, result.output)
        data = json.loads(result.output)
        self.assertTrue(data["passed"])
        self.assertEqual([a["status"] for a in data["advisories"]], ["REL_PREP", "REL_PREP"])
        self.runtime.initialize.assert_called_once_with(no_group=False)
        self.assembly_ids.assert_called_once_with(self.runtime)
        self.gitlab_cls.from_url.assert_not_called()

    def test_all_states_follow_post_qe_policy(self):
        for state in [*constants.errata_states, "UNRECOGNIZED_STATE", None]:
            with self.subTest(state=state):
                self.states[100] = state
                result = self.invoke()
                expected_pass = state in ("REL_PREP", "PUSH_READY", "IN_PUSH", "SHIPPED_LIVE", "DROPPED_NO_SHIP")
                self.assertEqual(result.exit_code, 0 if expected_pass else 1, result.output)
                data = json.loads(result.output)
                self.assertEqual(data["passed"], expected_pass)
                self.assertEqual(data["advisories"][0]["skipped"], state == "DROPPED_NO_SHIP")

    def test_mismatch_fails_and_does_not_label(self):
        self.assembly_ids.return_value = {"rpm": 100, "rhcos": 102}
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 1, result.output)
        data = json.loads(result.output)
        self.assertTrue(data["advisories"][0]["passed"])
        self.assertFalse(data["advisories"][1]["passed"])
        self.gitlab_cls.from_url.assert_not_called()

    def test_unknown_state_fails_and_does_not_label(self):
        self.assembly_ids.return_value = {"rpm": 999}
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn("Unable to determine", result.output)
        self.gitlab_cls.from_url.assert_not_called()

    def test_api_exception_fails_and_does_not_label(self):
        self.api.get_advisory_state.side_effect = RuntimeError("Errata unavailable")
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn("Errata unavailable", result.output)
        self.gitlab_cls.from_url.assert_not_called()

    def test_label_added_only_after_all_checks_pass(self):
        def update(*args, **kwargs):
            self.assertEqual(self.api.get_advisory_state.await_count, 2)

        self.update.side_effect = update
        self.assembly_ids.return_value = {"rpm": 100, "rhcos": 101}
        self.states[101] = "PUSH_READY"
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 0, result.output)
        self.update.assert_called_once_with(MR_URL, ["advisory-transition-success"])
        self.assertTrue(json.loads(result.output)["label_added"])
        self.shipment_url.assert_called_once_with(self.runtime, required=True)
        self.client.get_mr_from_url.assert_not_called()

    def test_dry_run_still_checks_status_and_does_not_label(self):
        result = self.invoke("--mark", "--dry-run")
        self.assertEqual(result.exit_code, 0, result.output)
        self.api.get_advisory_state.assert_awaited_once_with(100)
        data = json.loads(result.output)
        self.assertTrue(data["dry_run"])
        self.assertFalse(data["label_added"])
        self.gitlab_cls.from_url.assert_not_called()

    def test_dry_run_does_not_hide_status_mismatch(self):
        self.assembly_ids.return_value = {"rhcos": 102}
        result = self.invoke("--dry-run")
        self.assertEqual(result.exit_code, 1, result.output)

    def test_label_error_fails_verification_command(self):
        self.update.side_effect = RuntimeError("403 Forbidden")
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 1, result.output)
        data = json.loads(result.output)
        self.assertFalse(data["passed"])
        self.assertFalse(data["label_added"])
        self.assertEqual(data["error"], "403 Forbidden")

    def test_microshift_is_excluded_by_assembly_helper(self):
        self.assembly_ids.side_effect = get_ocp_release_advisory_ids
        self.runtime.get_releases_config.return_value = Model(
            {"releases": {"4.22.9": {"assembly": {"group": {"advisories": {"rpm": 100, "microshift": 102}}}}}}
        )
        result = self.invoke()
        self.assertEqual(result.exit_code, 0, result.output)
        self.api.get_advisory_state.assert_awaited_once_with(100)

    def test_empty_or_placeholder_selection_is_rejected(self):
        for advisories in ({}, {"rpm": -1}):
            with self.subTest(advisories=advisories):
                self.assembly_ids.return_value = advisories
                result = self.invoke()
                self.assertEqual(result.exit_code, 2, result.output)
                self.api.get_advisory_state.assert_not_called()
                self.update.assert_not_called()

    def test_duplicate_assembly_ids_are_deduplicated(self):
        self.assembly_ids.return_value = {"rpm": 100, "rhcos": 100}
        result = self.invoke()
        self.assertEqual(result.exit_code, 0, result.output)
        self.api.get_advisory_state.assert_awaited_once_with(100)

    def test_missing_group_is_rejected(self):
        self.runtime.group = None
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 2, result.output)
        self.api.get_advisory_state.assert_not_called()
        self.update.assert_not_called()

    def test_missing_assembly_shipment_url_prevents_checks_and_marking(self):
        self.shipment_url.side_effect = RuntimeError("No shipment URL found")
        result = self.invoke("--mark")
        self.assertNotEqual(result.exit_code, 0)
        self.api.get_advisory_state.assert_not_called()
        self.update.assert_not_called()

    def test_dropped_advisory_is_skipped_and_does_not_block_marking(self):
        self.assembly_ids.return_value = {"rpm": 100, "rhcos": 101}
        self.states[101] = "DROPPED_NO_SHIP"
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 0, result.output)
        data = json.loads(result.output)
        self.assertTrue(data["advisories"][1]["skipped"])
        self.update.assert_called_once_with(MR_URL, ["advisory-transition-success"])

    def test_all_dropped_advisories_allow_completion(self):
        self.states[100] = "DROPPED_NO_SHIP"
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 0, result.output)
        self.update.assert_called_once_with(MR_URL, ["advisory-transition-success"])

    def test_dropped_advisory_does_not_hide_an_active_status_mismatch(self):
        self.assembly_ids.return_value = {"rpm": 100, "rhcos": 101}
        self.states.update({100: "QE", 101: "DROPPED_NO_SHIP"})
        result = self.invoke("--mark")
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertTrue(json.loads(result.output)["advisories"][1]["skipped"])
        self.gitlab_cls.from_url.assert_not_called()
