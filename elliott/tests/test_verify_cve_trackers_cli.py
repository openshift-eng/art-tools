import json
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.verify_cve_trackers_cli import (
    VerifyCveTrackersResult,
    find_missing_cve_trackers,
    flatten_elliott_cve_trackers,
    get_jira_issues_from_shipment_data,
    get_jira_issues_from_shipment_mr,
    get_rhsa_jira_issues,
    get_shipment_mr_url,
    parse_shipment_metadata_from_data,
    render_result,
    verify_cve_trackers,
)

SAMPLE_SHIPMENT = {
    "shipment": {
        "metadata": {
            "group": "openshift-4.20",
            "assembly": "4.20.1",
        },
        "data": {
            "releaseNotes": {
                "issues": {
                    "fixed": [
                        {"id": "OCPBUGS-11111", "source": "issues.redhat.com"},
                        {"id": "OCPBUGS-22222", "source": "issues.redhat.com"},
                        {"id": "1234567", "source": "bugzilla.redhat.com"},
                    ]
                }
            }
        },
    }
}


class TestHelpers(unittest.TestCase):
    def test_get_shipment_mr_url(self):
        self.assertEqual(
            get_shipment_mr_url({"shipment": {"url": "https://gitlab.example.com/mr/1"}}),
            "https://gitlab.example.com/mr/1",
        )

    def test_get_shipment_mr_url_override_key(self):
        self.assertEqual(
            get_shipment_mr_url({"shipment!": {"url": "https://gitlab.example.com/mr/2"}}),
            "https://gitlab.example.com/mr/2",
        )

    def test_get_shipment_mr_url_missing(self):
        with self.assertRaises(RuntimeError):
            get_shipment_mr_url({})

    def test_get_jira_issues_from_shipment_data(self):
        self.assertEqual(
            get_jira_issues_from_shipment_data(SAMPLE_SHIPMENT),
            {"OCPBUGS-11111", "OCPBUGS-22222"},
        )

    def test_get_jira_issues_from_shipment_data_empty(self):
        self.assertEqual(get_jira_issues_from_shipment_data({}), set())

    def test_get_jira_issues_ocpbugs_prefix(self):
        data = {
            "shipment": {
                "data": {
                    "releaseNotes": {
                        "issues": {
                            "fixed": [
                                {"id": "OCPBUGS-99999", "source": "other.example.com"},
                            ]
                        }
                    }
                }
            }
        }
        self.assertEqual(get_jira_issues_from_shipment_data(data), {"OCPBUGS-99999"})

    def test_parse_shipment_metadata(self):
        group, assembly = parse_shipment_metadata_from_data(SAMPLE_SHIPMENT)
        self.assertEqual(group, "openshift-4.20")
        self.assertEqual(assembly, "4.20.1")

    def test_parse_shipment_metadata_missing(self):
        with self.assertRaises(ValueError):
            parse_shipment_metadata_from_data({"shipment": {"metadata": {}}})

    def test_flatten_elliott_cve_trackers_all(self):
        payload = {"rpm": ["OCPBUGS-1"], "image": ["OCPBUGS-2"]}
        result = flatten_elliott_cve_trackers(payload)
        self.assertEqual(result, ["OCPBUGS-1", "OCPBUGS-2"])

    def test_flatten_elliott_cve_trackers_with_kinds(self):
        payload = {
            "rpm": ["OCPBUGS-1"],
            "rhcos": ["OCPBUGS-2"],
            "image": ["OCPBUGS-3"],
        }
        result = flatten_elliott_cve_trackers(payload, kinds=("rpm", "rhcos"))
        self.assertEqual(result, ["OCPBUGS-1", "OCPBUGS-2"])

    def test_flatten_elliott_cve_trackers_empty(self):
        self.assertEqual(flatten_elliott_cve_trackers({}), [])

    def test_find_missing_cve_trackers(self):
        missing = find_missing_cve_trackers(
            ["OCPBUGS-1", "OCPBUGS-2", "OCPBUGS-3"],
            ["OCPBUGS-1", "OCPBUGS-3"],
        )
        self.assertEqual(missing, ["OCPBUGS-2"])

    def test_find_missing_cve_trackers_none_missing(self):
        missing = find_missing_cve_trackers(
            ["OCPBUGS-1"],
            ["OCPBUGS-1", "OCPBUGS-2"],
        )
        self.assertEqual(missing, [])

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_bug_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_raw_erratum")
    def test_get_rhsa_jira_issues(self, mock_get_raw_erratum, mock_get_bug_ids):
        mock_get_raw_erratum.side_effect = [
            {"errata": {"type": "RHSA", "status": "QE"}},
            {"errata": {"type": "RHBA", "status": "QE"}},
        ]
        mock_get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        advisories = {"rpm": 12345, "image": 12346}
        result = get_rhsa_jira_issues(advisories)
        self.assertEqual(result, {"OCPBUGS-1"})
        mock_get_bug_ids.assert_called_once_with(12345)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_bug_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_raw_erratum")
    def test_get_rhsa_jira_issues_skips_microshift(self, mock_get_raw_erratum, mock_get_bug_ids):
        advisories = {"microshift": 99999}
        result = get_rhsa_jira_issues(advisories)
        self.assertEqual(result, set())
        mock_get_raw_erratum.assert_not_called()

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_bug_ids")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_raw_erratum")
    def test_get_rhsa_jira_issues_skips_dropped(self, mock_get_raw_erratum, mock_get_bug_ids):
        mock_get_raw_erratum.return_value = {"errata": {"type": "RHSA", "status": "DROPPED_NO_SHIP"}}
        advisories = {"rpm": 12345}
        result = get_rhsa_jira_issues(advisories)
        self.assertEqual(result, set())
        mock_get_bug_ids.assert_not_called()


class TestVerifyCveTrackersResult(unittest.TestCase):
    def test_passed_no_missing(self):
        r = VerifyCveTrackersResult(group="g", assembly="a", shipment_mr_url="url")
        self.assertTrue(r.passed)

    def test_failed_advisory_missing(self):
        r = VerifyCveTrackersResult(
            group="g",
            assembly="a",
            shipment_mr_url="url",
            missing_advisory_trackers=["OCPBUGS-1"],
        )
        self.assertFalse(r.passed)

    def test_failed_shipment_missing(self):
        r = VerifyCveTrackersResult(
            group="g",
            assembly="a",
            shipment_mr_url="url",
            missing_shipment_trackers=["OCPBUGS-2"],
        )
        self.assertFalse(r.passed)

    def test_missing_trackers_deduped(self):
        r = VerifyCveTrackersResult(
            group="g",
            assembly="a",
            shipment_mr_url="url",
            missing_advisory_trackers=["OCPBUGS-1", "OCPBUGS-2"],
            missing_shipment_trackers=["OCPBUGS-2", "OCPBUGS-3"],
        )
        self.assertEqual(r.missing_trackers, ["OCPBUGS-1", "OCPBUGS-2", "OCPBUGS-3"])


class TestRenderResult(unittest.TestCase):
    def test_text_pass(self):
        r = VerifyCveTrackersResult(group="openshift-4.20", assembly="4.20.1", shipment_mr_url="https://mr")
        text = render_result(r, "text")
        self.assertIn("openshift-4.20", text)
        self.assertIn("PASS", text)
        self.assertIn("All CVE tracker bugs are covered", text)

    def test_text_fail(self):
        r = VerifyCveTrackersResult(
            group="openshift-4.20",
            assembly="4.20.1",
            shipment_mr_url="https://mr",
            missing_advisory_trackers=["OCPBUGS-1"],
        )
        text = render_result(r, "text")
        self.assertIn("FAIL", text)
        self.assertIn("OCPBUGS-1", text)

    def test_json(self):
        r = VerifyCveTrackersResult(
            group="openshift-4.20",
            assembly="4.20.1",
            shipment_mr_url="https://mr",
            advisory_elliott_trackers=["OCPBUGS-1"],
        )
        data = json.loads(render_result(r, "json"))
        self.assertEqual(data["group"], "openshift-4.20")
        self.assertTrue(data["passed"])
        self.assertEqual(data["advisory_elliott_trackers"], ["OCPBUGS-1"])


class TestVerifyCveTrackers(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_jira_issues_from_shipment_mr")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_rhsa_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.load_assembly_group", new_callable=AsyncMock)
    async def test_all_pass(self, mock_load, mock_rhsa, mock_shipment):
        mock_load.return_value = {
            "advisories": {"rpm": 111},
            "shipment": {"url": "https://gitlab/mr/1"},
        }
        mock_rhsa.return_value = {"OCPBUGS-1"}
        mock_shipment.return_value = (
            {"OCPBUGS-2"},
            ["file.yaml"],
            ("openshift-4.20", "4.20.1"),
        )

        result = await verify_cve_trackers(
            "openshift-4.20",
            "4.20.1",
            "https://github.com/ocp-build-data",
            advisory_trackers={"rpm": ["OCPBUGS-1"]},
            shipment_trackers={"image": ["OCPBUGS-2"]},
        )
        self.assertTrue(result.passed)

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_jira_issues_from_shipment_mr")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_rhsa_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.load_assembly_group", new_callable=AsyncMock)
    async def test_missing_advisory_tracker(self, mock_load, mock_rhsa, mock_shipment):
        mock_load.return_value = {
            "advisories": {"rpm": 111},
            "shipment": {"url": "https://gitlab/mr/1"},
        }
        mock_rhsa.return_value = set()
        mock_shipment.return_value = (set(), ["file.yaml"], ("openshift-4.20", "4.20.1"))

        result = await verify_cve_trackers(
            "openshift-4.20",
            "4.20.1",
            "https://github.com/ocp-build-data",
            advisory_trackers={"rpm": ["OCPBUGS-1"]},
            shipment_trackers={},
        )
        self.assertFalse(result.passed)
        self.assertIn("OCPBUGS-1", result.missing_advisory_trackers)

    @patch("elliottlib.cli.verify_cve_trackers_cli.load_assembly_group", new_callable=AsyncMock)
    async def test_assembly_not_found(self, mock_load):
        mock_load.return_value = None
        with self.assertRaises(RuntimeError):
            await verify_cve_trackers(
                "openshift-4.20",
                "4.20.1",
                "https://github.com/ocp-build-data",
                advisory_trackers={},
                shipment_trackers={},
            )

    @patch("elliottlib.cli.verify_cve_trackers_cli.get_jira_issues_from_shipment_mr")
    @patch("elliottlib.cli.verify_cve_trackers_cli.get_rhsa_jira_issues")
    @patch("elliottlib.cli.verify_cve_trackers_cli.load_assembly_group", new_callable=AsyncMock)
    async def test_shipment_mr_mismatch(self, mock_load, mock_rhsa, mock_shipment):
        mock_load.return_value = {
            "advisories": {},
            "shipment": {"url": "https://gitlab/mr/1"},
        }
        mock_rhsa.return_value = set()
        mock_shipment.return_value = (set(), ["file.yaml"], ("openshift-4.19", "4.19.5"))

        with self.assertRaises(ValueError, msg="does not match"):
            await verify_cve_trackers(
                "openshift-4.20",
                "4.20.1",
                "https://github.com/ocp-build-data",
                advisory_trackers={},
                shipment_trackers={},
            )
