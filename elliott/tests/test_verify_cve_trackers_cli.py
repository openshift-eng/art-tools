import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.verify_cve_trackers_cli import (
    MissedTracker,
    VerifyCVETrackersResult,
    get_advisory_jira_issues,
    get_shipment_jira_issues,
    get_shipment_kinds,
    render_result,
    verify_cve_trackers,
)


def _mock_bug(bug_id, cve_id=None):
    bug = MagicMock()
    bug.id = bug_id
    bug.cve_id = cve_id
    return bug


class TestVerifyCVETrackersResult(unittest.TestCase):
    def test_ok_when_no_missed(self):
        result = VerifyCVETrackersResult()
        self.assertTrue(result.ok)
        self.assertFalse(result.failed)

    def test_failed_when_missed(self):
        result = VerifyCVETrackersResult(missed_trackers=[MissedTracker("OCPBUGS-1", "rpm", "RHSA advisories")])
        self.assertFalse(result.ok)
        self.assertTrue(result.failed)


class TestRenderResult(unittest.TestCase):
    def test_render_text_ok(self):
        result = VerifyCVETrackersResult()
        text = render_result(result, "text")
        self.assertIn("No missed CVE tracker bugs found", text)
        self.assertIn("Overall: OK", text)

    def test_render_text_fail(self):
        result = VerifyCVETrackersResult(
            missed_trackers=[
                MissedTracker("OCPBUGS-123", "rpm", "RHSA advisories"),
                MissedTracker("OCPBUGS-456", "rhcos", "shipment MR"),
            ]
        )
        text = render_result(result, "text")
        self.assertIn("2 missed CVE tracker bug(s)", text)
        self.assertIn("OCPBUGS-123", text)
        self.assertIn("OCPBUGS-456", text)
        self.assertIn("Overall: FAIL", text)

    def test_render_json_ok(self):
        result = VerifyCVETrackersResult()
        import json

        data = json.loads(render_result(result, "json"))
        self.assertTrue(data["ok"])
        self.assertEqual(data["missed_trackers"], [])

    def test_render_json_fail(self):
        result = VerifyCVETrackersResult(missed_trackers=[MissedTracker("OCPBUGS-1", "rpm", "RHSA advisories")])
        import json

        data = json.loads(render_result(result, "json"))
        self.assertFalse(data["ok"])
        self.assertEqual(len(data["missed_trackers"]), 1)
        self.assertEqual(data["missed_trackers"][0]["bug_id"], "OCPBUGS-1")


class TestGetAdvisoryJiraIssues(unittest.IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    async def test_get_advisory_jira_issues(self, mock_errata):
        mock_errata.get_bug_ids.return_value = {"jira": ["OCPBUGS-1", "OCPBUGS-2"], "bugzilla": []}
        issues = await get_advisory_jira_issues(12345)
        self.assertEqual(issues, {"OCPBUGS-1", "OCPBUGS-2"})
        mock_errata.get_bug_ids.assert_called_once_with(12345)


class TestGetShipmentJiraIssues(unittest.TestCase):
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_configs_from_mr")
    def test_get_shipment_jira_issues(self, mock_get_configs):
        mock_issue1 = MagicMock()
        mock_issue1.source = "redhat.atlassian.net"
        mock_issue1.id = "OCPBUGS-10"

        mock_issue2 = MagicMock()
        mock_issue2.source = "redhat.atlassian.net"
        mock_issue2.id = "OCPBUGS-20"

        mock_config = MagicMock()
        mock_config.shipment.data.releaseNotes.issues.fixed = [mock_issue1, mock_issue2]

        mock_get_configs.return_value = {"image": mock_config}

        issues = get_shipment_jira_issues("https://gitlab.example.com/mr/1", "openshift-4.18")
        self.assertEqual(issues, {"OCPBUGS-10", "OCPBUGS-20"})

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_configs_from_mr")
    def test_get_shipment_jira_issues_empty(self, mock_get_configs):
        mock_config = MagicMock()
        mock_config.shipment.data = None

        mock_get_configs.return_value = {"image": mock_config}

        issues = get_shipment_jira_issues("https://gitlab.example.com/mr/1", "openshift-4.18")
        self.assertEqual(issues, set())


class TestGetShipmentKinds(unittest.TestCase):
    @patch("elliottlib.cli.verify_cve_trackers_cli.assembly_config_struct")
    def test_get_shipment_kinds(self, mock_acs):
        mock_acs.return_value = {
            "shipment": {
                "advisories": [
                    {"kind": "image", "live_id": 123},
                    {"kind": "extras", "live_id": 456},
                    {"kind": "metadata"},
                    {"kind": "fbc"},
                ],
                "url": "https://gitlab.example.com/mr/1",
            },
        }
        runtime = MagicMock()
        kinds = get_shipment_kinds(runtime)
        self.assertEqual(kinds, {"image", "extras", "metadata", "fbc"})

    @patch("elliottlib.cli.verify_cve_trackers_cli.assembly_config_struct")
    def test_get_shipment_kinds_empty(self, mock_acs):
        mock_acs.return_value = {}
        runtime = MagicMock()
        kinds = get_shipment_kinds(runtime)
        self.assertEqual(kinds, set())


class TestVerifyCVETrackers(unittest.IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_no_trackers(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {}
        runtime = MagicMock()
        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_found_in_rhsa(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {
            "rpm": [_mock_bug("OCPBUGS-1", "CVE-2026-1111")],
            "rhcos": [_mock_bug("OCPBUGS-2", "CVE-2026-2222")],
        }
        mock_get_ads.return_value = {"rpm": 111, "image": 222}
        mock_get_mr.return_value = None

        mock_errata.get_raw_erratum.side_effect = lambda ad_id: {
            111: {"errata": {"rhsa": {}}},
            222: {"errata": {"rhba": {}}},
        }[ad_id]
        mock_errata.get_bug_ids.side_effect = lambda ad_id: {
            111: {"jira": ["OCPBUGS-1", "OCPBUGS-2", "OCPBUGS-3"], "bugzilla": []},
        }[ad_id]

        mock_bug_tracker = MagicMock()
        mock_bug_tracker.get_bugs.return_value = [
            _mock_bug("OCPBUGS-1", "CVE-2026-1111"),
            _mock_bug("OCPBUGS-2", "CVE-2026-2222"),
            _mock_bug("OCPBUGS-3", "CVE-2026-3333"),
        ]
        runtime = MagicMock()
        runtime.get_bug_tracker.return_value = mock_bug_tracker

        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_missing_from_rhsa(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {
            "rpm": [_mock_bug("OCPBUGS-1", "CVE-2026-1111"), _mock_bug("OCPBUGS-99", "CVE-2026-9999")],
        }
        mock_get_ads.return_value = {"rpm": 111}
        mock_get_mr.return_value = None

        mock_errata.get_raw_erratum.return_value = {"errata": {"rhsa": {}}}
        mock_errata.get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        mock_bug_tracker = MagicMock()
        mock_bug_tracker.get_bugs.return_value = [_mock_bug("OCPBUGS-1", "CVE-2026-1111")]
        runtime = MagicMock()
        runtime.get_bug_tracker.return_value = mock_bug_tracker

        result = await verify_cve_trackers(runtime)
        self.assertFalse(result.ok)
        self.assertEqual(len(result.missed_trackers), 1)
        self.assertEqual(result.missed_trackers[0].bug_id, "OCPBUGS-99")
        self.assertEqual(result.missed_trackers[0].source, "RHSA advisories")

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_kinds")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_missing_from_shipment(
        self,
        mock_find,
        mock_errata,
        mock_get_ads,
        mock_get_mr,
        mock_get_shipment_issues,
        mock_get_shipment_kinds,
    ):
        mock_find.return_value = {"image": [_mock_bug("OCPBUGS-50", "CVE-2026-5555")]}
        mock_get_ads.return_value = {}
        mock_get_mr.return_value = "https://gitlab.cee.redhat.com/mr/1"
        mock_get_shipment_issues.return_value = set()
        mock_get_shipment_kinds.return_value = {"image", "extras"}

        runtime = MagicMock()
        runtime.group = "openshift-4.18"
        result = await verify_cve_trackers(runtime)
        self.assertFalse(result.ok)
        missed_shipment = [t for t in result.missed_trackers if t.source == "shipment MR"]
        self.assertEqual(len(missed_shipment), 1)
        self.assertEqual(missed_shipment[0].bug_id, "OCPBUGS-50")

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_kinds")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_found_in_shipment(
        self,
        mock_find,
        mock_errata,
        mock_get_ads,
        mock_get_mr,
        mock_get_shipment_issues,
        mock_get_shipment_kinds,
    ):
        mock_find.return_value = {"image": [_mock_bug("OCPBUGS-50", "CVE-2026-5555")]}
        mock_get_ads.return_value = {}
        mock_get_mr.return_value = "https://gitlab.cee.redhat.com/mr/1"
        mock_get_shipment_issues.return_value = {"OCPBUGS-50", "OCPBUGS-60"}
        mock_get_shipment_kinds.return_value = {"image", "extras"}

        runtime = MagicMock()
        runtime.group = "openshift-4.18"
        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_advisory_error_propagates(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {"rpm": [_mock_bug("OCPBUGS-1", "CVE-2026-1111")]}
        mock_get_ads.return_value = {"rpm": 111}
        mock_get_mr.return_value = None
        mock_errata.get_raw_erratum.side_effect = RuntimeError("Errata API unavailable")

        runtime = MagicMock()
        with self.assertRaises(RuntimeError):
            await verify_cve_trackers(runtime)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_kinds")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_shipment_error_propagates(
        self,
        mock_find,
        mock_errata,
        mock_get_ads,
        mock_get_mr,
        mock_get_shipment_issues,
        mock_get_shipment_kinds,
    ):
        mock_find.return_value = {"image": [_mock_bug("OCPBUGS-50", "CVE-2026-5555")]}
        mock_get_ads.return_value = {}
        mock_get_mr.return_value = "https://gitlab.cee.redhat.com/mr/1"
        mock_get_shipment_issues.side_effect = RuntimeError("GitLab API unavailable")
        mock_get_shipment_kinds.return_value = {"image"}

        runtime = MagicMock()
        runtime.group = "openshift-4.18"
        with self.assertRaises(RuntimeError):
            await verify_cve_trackers(runtime)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_cve_covered_by_another_tracker(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        """Tracker bug not on advisory, but same CVE is covered by a different tracker on the advisory."""
        mock_find.return_value = {
            "rhcos": [_mock_bug("OCPBUGS-61147", "CVE-2025-49794")],
        }
        mock_get_ads.return_value = {"rhcos": 173004}
        mock_get_mr.return_value = None

        mock_errata.get_raw_erratum.return_value = {"errata": {"rhsa": {}}}
        mock_errata.get_bug_ids.return_value = {"jira": ["OCPBUGS-112843"], "bugzilla": []}

        mock_bug_tracker = MagicMock()
        mock_bug_tracker.get_bugs.return_value = [_mock_bug("OCPBUGS-112843", "CVE-2025-49794")]
        runtime = MagicMock()
        runtime.get_bug_tracker.return_value = mock_bug_tracker

        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_kinds")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_assembly_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_advisory_only_kind_skipped_in_shipment_check(
        self,
        mock_find,
        mock_errata,
        mock_get_ads,
        mock_get_mr,
        mock_get_shipment_issues,
        mock_get_shipment_kinds,
    ):
        """rhcos kind goes through advisory, not shipment — should not be checked against shipment MR."""
        mock_find.return_value = {
            "rhcos": [_mock_bug("OCPBUGS-100", "CVE-2026-1111")],
        }
        mock_get_ads.return_value = {"rhcos": 173004}
        mock_get_mr.return_value = "https://gitlab.cee.redhat.com/mr/1"
        mock_get_shipment_issues.return_value = set()
        mock_get_shipment_kinds.return_value = {"image", "extras"}

        mock_errata.get_raw_erratum.return_value = {"errata": {"rhsa": {}}}
        mock_errata.get_bug_ids.return_value = {"jira": ["OCPBUGS-100"], "bugzilla": []}

        mock_bug_tracker = MagicMock()
        mock_bug_tracker.get_bugs.return_value = [_mock_bug("OCPBUGS-100", "CVE-2026-1111")]
        runtime = MagicMock()
        runtime.get_bug_tracker.return_value = mock_bug_tracker
        runtime.group = "openshift-4.18"

        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)
        self.assertEqual(len(result.missed_trackers), 0)


if __name__ == "__main__":
    unittest.main()
