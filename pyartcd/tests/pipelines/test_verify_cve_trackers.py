import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.verify_cve_trackers import (
    VerifyCveTrackersPipeline,
    VerifyCveTrackersResult,
    find_missing_cve_trackers,
    flatten_elliott_cve_trackers,
    get_jira_issues_from_shipment_data,
    get_jira_issues_from_shipment_mr,
    get_rhsa_jira_issues,
    get_shipment_mr_url,
    parse_shipment_metadata_from_data,
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

    def test_render_result_json(self):
        result = VerifyCveTrackersResult(
            group="openshift-4.20",
            assembly="4.20.1",
            shipment_mr_url="https://example/mr/1",
            shipment_files=["a.yaml", "b.yaml"],
            missing_advisory_trackers=["OCPBUGS-100"],
            missing_shipment_trackers=["OCPBUGS-99999"],
            advisory_elliott_trackers=["OCPBUGS-100", "OCPBUGS-200"],
            shipment_elliott_trackers=["OCPBUGS-99999", "OCPBUGS-88888"],
        )

        rendered = render_result(result, "json")
        data = json.loads(rendered)
        self.assertEqual(data["group"], "openshift-4.20")
        self.assertEqual(data["assembly"], "4.20.1")
        self.assertFalse(data["passed"])
        self.assertEqual(len(data["missing_trackers"]), 2)
        self.assertIn("OCPBUGS-100", data["missing_trackers"])
        self.assertIn("OCPBUGS-99999", data["missing_trackers"])

    def test_render_result_text_success(self):
        result = VerifyCveTrackersResult(
            group="openshift-4.20",
            assembly="4.20.1",
            shipment_mr_url="https://example/mr/1",
        )

        rendered = render_result(result, "text")
        self.assertIn("All CVE tracker bugs are covered", rendered)
        self.assertNotIn("MISSING", rendered)

    def test_get_jira_issues_from_shipment_data_non_dict_issue(self):
        data = {
            "shipment": {
                "data": {
                    "releaseNotes": {
                        "issues": {
                            "fixed": [
                                "not-a-dict",
                                {"id": "OCPBUGS-11111", "source": "issues.redhat.com"},
                            ]
                        }
                    }
                }
            }
        }
        result = get_jira_issues_from_shipment_data(data)
        self.assertEqual(result, {"OCPBUGS-11111"})

    def test_get_jira_issues_from_shipment_data_no_issue_id(self):
        data = {
            "shipment": {
                "data": {
                    "releaseNotes": {
                        "issues": {
                            "fixed": [
                                {"source": "issues.redhat.com"},
                                {"id": "OCPBUGS-11111", "source": "issues.redhat.com"},
                            ]
                        }
                    }
                }
            }
        }
        result = get_jira_issues_from_shipment_data(data)
        self.assertEqual(result, {"OCPBUGS-11111"})

    def test_parse_shipment_metadata_missing_group(self):
        data = {"shipment": {"metadata": {"assembly": "4.20.1"}}}
        with self.assertRaisesRegex(ValueError, "Shipment YAML must define"):
            parse_shipment_metadata_from_data(data)

    def test_parse_shipment_metadata_missing_assembly(self):
        data = {"shipment": {"metadata": {"group": "openshift-4.20"}}}
        with self.assertRaisesRegex(ValueError, "Shipment YAML must define"):
            parse_shipment_metadata_from_data(data)

    def test_get_jira_issues_from_shipment_mr_no_token(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ValueError, "GITLAB_TOKEN environment variable is required"):
                get_jira_issues_from_shipment_mr("https://gitlab.example.com/mr/1")

    @patch("pyartcd.pipelines.verify_cve_trackers.GitLabClient")
    def test_get_jira_issues_from_shipment_mr_no_yaml_files(self, mock_gitlab_client_cls):
        mock_mr = MagicMock()
        mock_mr.source_branch = "main"
        mock_mr.changes.return_value = {"changes": [{"new_path": "README.md"}]}

        mock_client = MagicMock()
        mock_client.get_mr_from_url.return_value = mock_mr
        mock_gitlab_client_cls.from_url.return_value = mock_client
        mock_gitlab_client_cls._parse_mr_url.return_value = ("project", 1)

        with self.assertRaisesRegex(ValueError, "No shipment YAML files found"):
            get_jira_issues_from_shipment_mr(
                "https://gitlab.example.com/mr/1",
                gitlab_token="token",
            )

    @patch("pyartcd.pipelines.verify_cve_trackers.GitLabClient")
    def test_get_jira_issues_from_shipment_mr_mixed_assemblies(self, mock_gitlab_client_cls):
        mock_file1 = MagicMock()
        mock_file1.decode.return_value.decode.return_value = """
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
        mock_file2 = MagicMock()
        mock_file2.decode.return_value.decode.return_value = """
shipment:
  metadata:
    group: openshift-4.20
    assembly: 4.20.2
  data:
    releaseNotes:
      issues:
        fixed:
          - id: OCPBUGS-22222
            source: issues.redhat.com
"""

        mock_project = MagicMock()
        mock_project.files.get.side_effect = [mock_file1, mock_file2]

        mock_mr = MagicMock()
        mock_mr.source_branch = "main"
        mock_mr.changes.return_value = {
            "changes": [
                {"new_path": "file1.yaml"},
                {"new_path": "file2.yaml"},
            ]
        }

        mock_client = MagicMock()
        mock_client.get_mr_from_url.return_value = mock_mr
        mock_client.get_project.return_value = mock_project
        mock_gitlab_client_cls.from_url.return_value = mock_client
        mock_gitlab_client_cls._parse_mr_url.return_value = ("project", 1)

        with self.assertRaisesRegex(ValueError, "Shipment MR contains mixed assemblies"):
            get_jira_issues_from_shipment_mr(
                "https://gitlab.example.com/mr/1",
                gitlab_token="token",
            )

    @patch("pyartcd.pipelines.verify_cve_trackers.get_bug_ids")
    @patch("pyartcd.pipelines.verify_cve_trackers.get_raw_erratum")
    def test_get_rhsa_jira_issues_with_dropped_advisory(self, mock_get_raw_erratum, mock_get_bug_ids):
        mock_get_raw_erratum.side_effect = [
            {"errata": {"type": "RHSA", "status": "DROPPED_NO_SHIP"}},
            {"errata": {"type": "RHSA", "status": "QE"}},
        ]
        mock_get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        advisories = {"rpm": 12345, "image": 12346}
        result = get_rhsa_jira_issues(advisories)
        # First advisory is DROPPED_NO_SHIP, should be skipped
        self.assertEqual(result, {"OCPBUGS-1"})
        # get_bug_ids should only be called once (for second advisory)
        self.assertEqual(mock_get_bug_ids.call_count, 1)

    @patch("pyartcd.pipelines.verify_cve_trackers.get_bug_ids")
    @patch("pyartcd.pipelines.verify_cve_trackers.get_raw_erratum")
    def test_get_rhsa_jira_issues_with_null_advisory(self, mock_get_raw_erratum, mock_get_bug_ids):
        advisories = {"rpm": None, "image": 12346}
        mock_get_raw_erratum.return_value = {"errata": {"type": "RHSA", "status": "QE"}}
        mock_get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        get_rhsa_jira_issues(advisories)
        # None advisory should be skipped
        self.assertEqual(mock_get_raw_erratum.call_count, 1)
        self.assertEqual(mock_get_bug_ids.call_count, 1)

    @patch("pyartcd.pipelines.verify_cve_trackers.get_bug_ids")
    @patch("pyartcd.pipelines.verify_cve_trackers.get_raw_erratum")
    def test_get_rhsa_jira_issues_with_negative_advisory(self, mock_get_raw_erratum, mock_get_bug_ids):
        advisories = {"rpm": -1, "image": 12346}
        mock_get_raw_erratum.return_value = {"errata": {"type": "RHSA", "status": "QE"}}
        mock_get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        get_rhsa_jira_issues(advisories)
        # Negative advisory should be skipped
        self.assertEqual(mock_get_raw_erratum.call_count, 1)
        self.assertEqual(mock_get_bug_ids.call_count, 1)

    @patch("pyartcd.pipelines.verify_cve_trackers.get_bug_ids")
    @patch("pyartcd.pipelines.verify_cve_trackers.get_raw_erratum")
    def test_get_rhsa_jira_issues_skips_microshift(self, mock_get_raw_erratum, mock_get_bug_ids):
        advisories = {"microshift": 12345, "rpm": 12346}
        mock_get_raw_erratum.return_value = {"errata": {"type": "RHSA", "status": "QE"}}
        mock_get_bug_ids.return_value = {"jira": ["OCPBUGS-1"], "bugzilla": []}

        get_rhsa_jira_issues(advisories)
        # microshift should be skipped
        self.assertEqual(mock_get_raw_erratum.call_count, 1)
        self.assertEqual(mock_get_bug_ids.call_count, 1)


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

    async def test_run_with_missing_trackers(self):
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
                return_value={"OCPBUGS-11111"},  # Only one issue in advisory
            ),
            patch(
                "pyartcd.pipelines.verify_cve_trackers.get_jira_issues_from_shipment_mr",
                return_value=(
                    {"OCPBUGS-11111"},  # Only one issue in shipment
                    ["image.yaml", "extras.yaml"],
                    ("openshift-4.20", "4.20.1"),
                ),
            ),
            patch.object(
                pipeline,
                "_load_elliott_output",
                side_effect=[
                    {"rpm": ["OCPBUGS-11111", "OCPBUGS-99999"]},  # Elliott found extra tracker in advisory
                    {"image": ["OCPBUGS-11111", "OCPBUGS-88888"]},  # Elliott found extra tracker in shipment
                ],
            ),
        ):
            result = await pipeline.run()

        # Verify that missing trackers were detected
        self.assertFalse(result.passed)
        self.assertEqual(len(result.missing_advisory_trackers), 1)
        self.assertIn("OCPBUGS-99999", result.missing_advisory_trackers)
        self.assertEqual(len(result.missing_shipment_trackers), 1)
        self.assertIn("OCPBUGS-88888", result.missing_shipment_trackers)
        self.assertEqual(len(result.missing_trackers), 2)
        self.assertIn("OCPBUGS-99999", result.missing_trackers)
        self.assertIn("OCPBUGS-88888", result.missing_trackers)

    async def test_run_assembly_group_not_found(self):
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

        with patch(
            "pyartcd.pipelines.verify_cve_trackers.load_assembly_group",
            new_callable=AsyncMock,
            return_value=None,
        ):
            with self.assertRaisesRegex(RuntimeError, "Failed to load assembly group config"):
                await pipeline.run()

    async def test_run_group_assembly_mismatch(self):
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
                    "shipment": {"url": "https://gitlab.cee.redhat.com/mr/1"},
                },
            ),
            patch(
                "pyartcd.pipelines.verify_cve_trackers.get_rhsa_jira_issues",
                return_value=set(),
            ),
            patch(
                "pyartcd.pipelines.verify_cve_trackers.get_jira_issues_from_shipment_mr",
                return_value=(
                    set(),
                    ["image.yaml"],
                    ("openshift-4.19", "4.19.1"),  # Different group/assembly
                ),
            ),
            patch.object(
                pipeline,
                "_load_elliott_output",
                side_effect=[{}, {}],
            ),
        ):
            with self.assertRaisesRegex(ValueError, "Shipment MR metadata .* does not match"):
                await pipeline.run()

    async def test_run_with_ocp_build_data_url_from_config(self):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.working_dir.__truediv__ = lambda self, other: f"/tmp/{other}"
        runtime.config = {"build_config": {"ocp_build_data_url": "https://github.com/custom/ocp-build-data"}}
        runtime.logger = MagicMock()

        pipeline = VerifyCveTrackersPipeline(
            runtime=runtime,
            group="openshift-4.20",
            assembly="4.20.1",
            data_path="https://github.com/openshift-eng/ocp-build-data",
        )

        # Verify elliott env vars were set correctly
        self.assertEqual(pipeline._elliott_env_vars["ELLIOTT_DATA_PATH"], "https://github.com/custom/ocp-build-data")

    @patch("artcommonlib.exectools.cmd_gather_async")
    async def test_load_elliott_output_success(self, mock_cmd_gather):
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

        mock_cmd_gather.return_value = (
            0,
            '{"rpm": ["OCPBUGS-1"], "image": ["OCPBUGS-2"]}',
            "",
        )

        result = await pipeline._load_elliott_output("brew", permissive=True)

        self.assertEqual(result, {"rpm": ["OCPBUGS-1"], "image": ["OCPBUGS-2"]})
        mock_cmd_gather.assert_awaited_once()
        call_args = mock_cmd_gather.call_args
        self.assertIn("elliott", call_args[0][0])
        self.assertIn("--build-system", call_args[0][0])
        self.assertIn("brew", call_args[0][0])
        self.assertIn("--permissive", call_args[0][0])
        self.assertEqual(call_args[1]["env"], pipeline._elliott_env_vars)
        self.assertIsNone(call_args[1]["stderr"])

    @patch("artcommonlib.exectools.cmd_gather_async")
    async def test_load_elliott_output_failure(self, mock_cmd_gather):
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

        mock_cmd_gather.return_value = (1, "", "elliott find-bugs failed")

        with self.assertRaisesRegex(RuntimeError, "elliott find-bugs failed with status=1"):
            await pipeline._load_elliott_output("konflux", permissive=False)

        mock_cmd_gather.assert_awaited_once()
        call_args = mock_cmd_gather.call_args
        self.assertIn("konflux", call_args[0][0])
        self.assertNotIn("--permissive", call_args[0][0])

    @patch("artcommonlib.exectools.cmd_gather_async")
    async def test_load_elliott_output_empty(self, mock_cmd_gather):
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

        mock_cmd_gather.return_value = (0, "", "")

        result = await pipeline._load_elliott_output("brew", permissive=False)

        self.assertEqual(result, {})
        mock_cmd_gather.assert_awaited_once()
