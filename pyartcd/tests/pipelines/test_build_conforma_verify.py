import unittest

from pyartcd.pipelines.build_conforma_verify import BuildConformaVerifyPipeline

SAMPLE_EC_REPORT_LOG = """\
Enterprise Contract Verification Results
=========================================

✕ [Violation] step_image_registries.allowed_step_image_registry_prefixes
  ImageRef: quay.io/redhat-prod/ocp-v4.0-art-dev@sha256:abc123def456
  Reason: Step 0 in task build-image-manifest uses
    disallowed image ref "registry.access.redhat.com/ubi9/go-toolset:1.21"
  Term: step_image_registries
  Title: Task step images come from permitted registry
  Description: Verify that all task step images come from allowed registries
  Solution: Use an image from an allowed registry

✕ [Violation] cve_results_found.cve_results_found
  ImageRef: quay.io/redhat-prod/ocp-v4.0-art-dev@sha256:abc123def456
  Reason: No CVE scan results found for this image
  Term: cve_results_found
  Title: CVE scan results found
  Description: Ensure CVE scan was performed
  Solution: Run a CVE scan

✕ [Violation] step_image_registries.allowed_step_image_registry_prefixes
  ImageRef: quay.io/redhat-prod/ocp-v4.0-art-dev@sha256:789ghi012jkl
  Reason: Step 2 in task source-build uses
    disallowed image ref "docker.io/library/golang:1.21"
  Term: step_image_registries
  Title: Task step images come from permitted registry
  Description: Verify that all task step images come from allowed registries
  Solution: Use an image from an allowed registry
"""

SAMPLE_DIGEST_TO_NAME = {
    "sha256:abc123def456": "ose-cluster-api",
    "sha256:789ghi012jkl": "ose-machine-config-operator",
}


class TestParseViolationsFromLog(unittest.TestCase):
    def test_parses_multiple_violations(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(SAMPLE_EC_REPORT_LOG, SAMPLE_DIGEST_TO_NAME)
        self.assertEqual(len(violations), 3)

    def test_extracts_rule_names(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(SAMPLE_EC_REPORT_LOG, SAMPLE_DIGEST_TO_NAME)
        rules = [v["rule"] for v in violations]
        self.assertEqual(
            rules,
            [
                "step_image_registries.allowed_step_image_registry_prefixes",
                "cve_results_found.cve_results_found",
                "step_image_registries.allowed_step_image_registry_prefixes",
            ],
        )

    def test_resolves_component_names_from_digest(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(SAMPLE_EC_REPORT_LOG, SAMPLE_DIGEST_TO_NAME)
        self.assertEqual(violations[0]["component_name"], "ose-cluster-api")
        self.assertEqual(violations[1]["component_name"], "ose-cluster-api")
        self.assertEqual(violations[2]["component_name"], "ose-machine-config-operator")

    def test_extracts_titles(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(SAMPLE_EC_REPORT_LOG, SAMPLE_DIGEST_TO_NAME)
        self.assertEqual(violations[0]["title"], "Task step images come from permitted registry")
        self.assertEqual(violations[1]["title"], "CVE scan results found")

    def test_extracts_multiline_reason(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(SAMPLE_EC_REPORT_LOG, SAMPLE_DIGEST_TO_NAME)
        self.assertIn("disallowed image ref", violations[0]["reason"])
        self.assertIn("registry.access.redhat.com/ubi9/go-toolset:1.21", violations[0]["reason"])

    def test_extracts_image_refs(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(SAMPLE_EC_REPORT_LOG, SAMPLE_DIGEST_TO_NAME)
        self.assertEqual(
            violations[0]["image_ref"],
            "quay.io/redhat-prod/ocp-v4.0-art-dev@sha256:abc123def456",
        )
        self.assertEqual(
            violations[2]["image_ref"],
            "quay.io/redhat-prod/ocp-v4.0-art-dev@sha256:789ghi012jkl",
        )

    def test_empty_log_returns_no_violations(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log("", {})
        self.assertEqual(violations, [])

    def test_log_without_violations_returns_empty(self):
        log = "Enterprise Contract Verification Results\n=========\nAll checks passed.\n"
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(log, {})
        self.assertEqual(violations, [])

    def test_unknown_digest_falls_back_to_image_ref(self):
        violations = BuildConformaVerifyPipeline._parse_violations_from_log(SAMPLE_EC_REPORT_LOG, {})
        self.assertEqual(
            violations[0]["component_name"],
            "quay.io/redhat-prod/ocp-v4.0-art-dev@sha256:abc123def456",
        )
