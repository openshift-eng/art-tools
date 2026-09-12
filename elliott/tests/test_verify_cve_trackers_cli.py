import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.verify_cve_trackers_cli import (
    MissedTracker,
    VerifyCVETrackersResult,
    get_advisory_jira_issues,
    get_shipment_jira_issues,
    render_result,
    verify_cve_trackers,
)


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


class TestVerifyCVETrackers(unittest.IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_mr_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_no_trackers(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {}
        runtime = MagicMock()
        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_mr_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_found_in_rhsa(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {"rpm": ["OCPBUGS-1"], "rhcos": ["OCPBUGS-2"]}
        mock_get_ads.return_value = {"rpm": 111, "image": 222}
        mock_get_mr.return_value = None

        mock_errata.get_raw_erratum.side_effect = lambda ad_id: {
            111: {"errata": {"rhsa": {}}},
            222: {"errata": {"rhba": {}}},
        }[ad_id]
        mock_errata.get_bug_ids.side_effect = lambda ad_id: {
            111: {"jira": ["OCPBUGS-1", "OCPBUGS-2", "OCPBUGS-3"], "bugzilla": []},
        }[ad_id]

        runtime = MagicMock()
        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_mr_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_missing_from_rhsa(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {"rpm": ["OCPBUGS-1", "OCPBUGS-99"]}
        mock_get_ads.return_value = {"rpm": 111}
        mock_get_mr.return_value = None

        mock_errata.get_raw_erratum.return_value = {"errata": {"rhsa": {}}}
        mock_errata.get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        runtime = MagicMock()
        result = await verify_cve_trackers(runtime)
        self.assertFalse(result.ok)
        self.assertEqual(len(result.missed_trackers), 1)
        self.assertEqual(result.missed_trackers[0].bug_id, "OCPBUGS-99")
        self.assertEqual(result.missed_trackers[0].source, "RHSA advisories")

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_mr_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_missing_from_shipment(
        self, mock_find, mock_errata, mock_get_ads, mock_get_mr, mock_get_shipment_issues
    ):
        mock_find.return_value = {"image": ["OCPBUGS-50"]}
        mock_get_ads.return_value = {}
        mock_get_mr.return_value = "https://gitlab.cee.redhat.com/mr/1"
        mock_get_shipment_issues.return_value = set()

        runtime = MagicMock()
        runtime.group = "openshift-4.18"
        result = await verify_cve_trackers(runtime)
        self.assertFalse(result.ok)
        missed_shipment = [t for t in result.missed_trackers if t.source == "shipment MR"]
        self.assertEqual(len(missed_shipment), 1)
        self.assertEqual(missed_shipment[0].bug_id, "OCPBUGS-50")

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_mr_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_trackers_found_in_shipment(
        self, mock_find, mock_errata, mock_get_ads, mock_get_mr, mock_get_shipment_issues
    ):
        mock_find.return_value = {"image": ["OCPBUGS-50"]}
        mock_get_ads.return_value = {}
        mock_get_mr.return_value = "https://gitlab.cee.redhat.com/mr/1"
        mock_get_shipment_issues.return_value = {"OCPBUGS-50", "OCPBUGS-60"}

        runtime = MagicMock()
        runtime.group = "openshift-4.18"
        result = await verify_cve_trackers(runtime)
        self.assertTrue(result.ok)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_mr_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_advisory_error_propagates(self, mock_find, mock_errata, mock_get_ads, mock_get_mr):
        mock_find.return_value = {"rpm": ["OCPBUGS-1"]}
        mock_get_ads.return_value = {"rpm": 111}
        mock_get_mr.return_value = None
        mock_errata.get_raw_erratum.side_effect = RuntimeError("Errata API unavailable")

        runtime = MagicMock()
        with self.assertRaises(RuntimeError):
            await verify_cve_trackers(runtime)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_shipment_mr_url")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_advisory_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.errata")
    @patch("elliottlib.cli.verify_cve_trackers_cli.find_cve_tracker_bugs", new_callable=AsyncMock)
    async def test_shipment_error_propagates(
        self, mock_find, mock_errata, mock_get_ads, mock_get_mr, mock_get_shipment_issues
    ):
        mock_find.return_value = {"image": ["OCPBUGS-50"]}
        mock_get_ads.return_value = {}
        mock_get_mr.return_value = "https://gitlab.cee.redhat.com/mr/1"
        mock_get_shipment_issues.side_effect = RuntimeError("GitLab API unavailable")

        runtime = MagicMock()
        runtime.group = "openshift-4.18"
        with self.assertRaises(RuntimeError):
            await verify_cve_trackers(runtime)


if __name__ == "__main__":
    unittest.main()
