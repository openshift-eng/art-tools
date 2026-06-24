import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.verify_cve_trackers import (
    VerifyCveTrackersPipeline,
    find_missing_cve_trackers,
    flatten_elliott_cve_trackers,
    get_jira_issues_from_shipment_data,
    get_jira_issues_from_shipment_mr,
    get_rhsa_jira_issues,
    get_shipment_mr_url,
    render_result,
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

SAMPLE_EXTRAS = {
    "shipment": {
        "metadata": {
            "group": "openshift-4.20",
            "assembly": "4.20.1",
        },
        "data": {
            "releaseNotes": {
                "issues": {
                    "fixed": [
                        {"id": "OCPBUGS-33333", "source": "issues.redhat.com"},
                    ]
                }
            }
        },
    }
}


class TestVerifyCveTrackersHelpers(unittest.TestCase):
    def test_get_shipment_mr_url_with_override_key(self):
        self.assertEqual(
            get_shipment_mr_url({"shipment!": {"url": "https://gitlab.example.com/mr/1"}}),
            "https://gitlab.example.com/mr/1",
        )

    def test_get_jira_issues_from_shipment_data(self):
        self.assertEqual(
            get_jira_issues_from_shipment_data(SAMPLE_SHIPMENT),
            {"OCPBUGS-11111", "OCPBUGS-22222"},
        )

    def test_flatten_elliott_cve_trackers_advisory_kinds(self):
        payload = {
            "rpm": ["OCPBUGS-1"],
            "rhcos": ["OCPBUGS-2"],
            "image": ["OCPBUGS-3"],
        }
        self.assertEqual(
            flatten_elliott_cve_trackers(payload, kinds=("rpm", "rhcos")),
            ["OCPBUGS-1", "OCPBUGS-2"],
        )

    def test_find_missing_cve_trackers(self):
        missing = find_missing_cve_trackers(
            ["OCPBUGS-1", "OCPBUGS-2"],
            ["OCPBUGS-1"],
        )
        self.assertEqual(missing, ["OCPBUGS-2"])

    @patch("pyartcd.pipelines.verify_cve_trackers.get_bug_ids")
    @patch("pyartcd.pipelines.verify_cve_trackers.get_raw_erratum")
    def test_get_rhsa_jira_issues(self, mock_get_raw_erratum, mock_get_bug_ids):
        mock_get_raw_erratum.side_effect = [
            {"errata": {"type": "RHSA", "status": "QE"}},
            {"errata": {"type": "RHBA", "status": "QE"}},
        ]
        mock_get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        advisories = {"rpm": 12345, "image": 12346}
        self.assertEqual(get_rhsa_jira_issues(advisories), {"OCPBUGS-1"})

    @patch("pyartcd.pipelines.verify_cve_trackers.GitLabClient")
    def test_get_jira_issues_from_shipment_mr(self, mock_gitlab_client_cls):
        mock_file_image = MagicMock()
        mock_file_image.decode.return_value.decode.return_value = """
shipment:
  metadata:
    group: openshift-4.20
    assembly: 4.20.1
  data:
    releaseNotes:
      issues:
        fixed:
          - id: OCPBUGS-11111
            source: issues.redhat.com
"""
        mock_file_extras = MagicMock()
        mock_file_extras.decode.return_value.decode.return_value = """
shipment:
  metadata:
    group: openshift-4.20
    assembly: 4.20.1
  data:
    releaseNotes:
      issues:
        fixed:
          - id: OCPBUGS-33333
            source: issues.redhat.com
"""

        mock_project = MagicMock()
        mock_project.files.get.side_effect = [mock_file_image, mock_file_extras]

        mock_mr = MagicMock()
        mock_mr.source_branch = "prepare-shipment-4.20.1"
        mock_mr.changes.return_value = {
            "changes": [
                {"new_path": "shipment/ocp/.../4.20.1.image.yaml"},
                {"new_path": "shipment/ocp/.../4.20.1.extras.yaml"},
            ]
        }

        mock_client = MagicMock()
        mock_client.get_mr_from_url.return_value = mock_mr
        mock_client.get_project.return_value = mock_project
        mock_gitlab_client_cls.from_url.return_value = mock_client
        mock_gitlab_client_cls._parse_mr_url.return_value = (
            "hybrid-platforms/art/ocp-shipment-data",
            1,
        )

        jira_issues, files, metadata = get_jira_issues_from_shipment_mr(
            "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/1",
            gitlab_token="token",
        )

        self.assertEqual(jira_issues, {"OCPBUGS-11111", "OCPBUGS-33333"})
        self.assertEqual(len(files), 2)
        self.assertEqual(metadata, ("openshift-4.20", "4.20.1"))

    def test_get_shipment_mr_url(self):
        self.assertEqual(
            get_shipment_mr_url({"shipment": {"url": "https://gitlab.example.com/mr/1"}}),
            "https://gitlab.example.com/mr/1",
        )

    def test_get_shipment_mr_url_missing(self):
        with self.assertRaisesRegex(RuntimeError, "shipment.url not found"):
            get_shipment_mr_url({})

    def test_render_result_text_failure(self):
        result = MagicMock()
        result.group = "openshift-4.20"
        result.assembly = "4.20.1"
        result.shipment_mr_url = "https://example/mr/1"
        result.shipment_files = ["a.yaml", "b.yaml"]
        result.missing_advisory_trackers = ["OCPBUGS-100"]
        result.missing_shipment_trackers = ["OCPBUGS-99999"]
        result.passed = False

        rendered = render_result(result, "text")
        self.assertIn("Shipment MR:", rendered)
        self.assertIn("MISSING CVE TRACKER BUGS IN SHIPMENT MR", rendered)


class TestVerifyCveTrackersPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_run_checks_advisory_and_shipment(self):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.working_dir.__truediv__ = lambda self, other: f"/tmp/{other}"
        runtime.config = {}
        runtime.logger = MagicMock()

        pipeline = VerifyCveTrackersPipeline(
            runtime=runtime,
            group="openshift-4.20",
            assembly="4.20.1",
            data_path="https://github.com/openshift-eng/ocp-build-data",
        )

        with (
            patch(
                "pyartcd.pipelines.verify_cve_trackers.load_assembly_group",
                new_callable=AsyncMock,
                return_value={
                    "advisories": {"rpm": 12345},
                    "shipment": {
                        "url": "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/1"
                    },
                },
            ),
            patch(
                "pyartcd.pipelines.verify_cve_trackers.get_rhsa_jira_issues",
                return_value={"OCPBUGS-11111"},
            ),
            patch(
                "pyartcd.pipelines.verify_cve_trackers.get_jira_issues_from_shipment_mr",
                return_value=(
                    {"OCPBUGS-11111", "OCPBUGS-22222"},
                    ["image.yaml", "extras.yaml"],
                    ("openshift-4.20", "4.20.1"),
                ),
            ),
            patch.object(
                pipeline,
                "_load_elliott_output",
                side_effect=[
                    {"rpm": ["OCPBUGS-11111"]},
                    {"image": ["OCPBUGS-11111", "OCPBUGS-22222"]},
                ],
            ),
        ):
            result = await pipeline.run()

        self.assertTrue(result.passed)
        self.assertEqual(result.shipment_files, ["image.yaml", "extras.yaml"])
