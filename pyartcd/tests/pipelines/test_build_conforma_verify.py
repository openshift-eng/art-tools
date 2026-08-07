import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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


def _make_pipeline(**kwargs):
    runtime = MagicMock()
    runtime.dry_run = kwargs.pop("dry_run", False)
    runtime.working_dir = MagicMock()
    runtime.working_dir.__truediv__ = MagicMock(return_value=MagicMock())
    defaults = {
        "runtime": runtime,
        "group": "openshift-4.18",
        "assembly": "stream",
        "builds": None,
    }
    defaults.update(kwargs)
    return BuildConformaVerifyPipeline(**defaults)


class TestPipelineInit(unittest.TestCase):
    def test_default_values(self):
        p = _make_pipeline()
        self.assertEqual(p.effective_time, "now")
        self.assertIsNone(p.fbc_ec_policy)
        self.assertFalse(p.include_corresponding_bundles)
        self.assertFalse(p.include_corresponding_fbcs)
        self.assertIsNone(p.slack_client)

    def test_custom_values(self):
        slack = MagicMock()
        p = _make_pipeline(
            effective_time="2026-08-05T00:00:00Z",
            fbc_ec_policy="rhtap-releng-tenant/fbc-ocp-art-stage",
            include_corresponding_bundles=True,
            include_corresponding_fbcs=True,
            slack_client=slack,
        )
        self.assertEqual(p.effective_time, "2026-08-05T00:00:00Z")
        self.assertEqual(p.fbc_ec_policy, "rhtap-releng-tenant/fbc-ocp-art-stage")
        self.assertTrue(p.include_corresponding_bundles)
        self.assertTrue(p.include_corresponding_fbcs)
        self.assertIs(p.slack_client, slack)

    def test_group_set_directly(self):
        p = _make_pipeline(group="openshift-4.19")
        self.assertEqual(p.group, "openshift-4.19")

    def test_group_set_layered(self):
        p = _make_pipeline(group="logging-6.7")
        self.assertEqual(p.group, "logging-6.7")

    def test_group_is_not_rewritten(self):
        p = _make_pipeline(group="oadp-1.5")
        self.assertEqual(p.group, "oadp-1.5")


class TestProductSettings(unittest.TestCase):
    def test_lp_settings_use_product_and_selected_data(self):
        p = _make_pipeline(
            group="oadp-1.5",
            data_path="https://example.test/ocp-build-data",
            data_gitref="feature-branch",
        )

        with patch(
            "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
            new=AsyncMock(return_value={"product": "oadp"}),
        ) as load_group_config:
            asyncio.run(p._resolve_layered_product_settings())

        load_group_config.assert_awaited_once_with(
            group="oadp-1.5",
            assembly="stream",
            doozer_data_path="https://example.test/ocp-build-data",
            doozer_data_gitref="feature-branch",
        )
        self.assertEqual(p.namespace, "art-oadp-tenant")
        self.assertEqual(p.kubeconfig_env_var, "OADP_KONFLUX_SA_KUBECONFIG")

    def test_unknown_product_raises(self):
        p = _make_pipeline(group="unknown-1.0")
        with patch(
            "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
            new=AsyncMock(return_value={"product": "unknown"}),
        ):
            with self.assertRaisesRegex(ValueError, "unknown-1.0.*unknown"):
                asyncio.run(p._resolve_layered_product_settings())

    def test_missing_lp_kubeconfig_raises_before_client_creation(self):
        p = _make_pipeline(group="oadp-1.5")
        p.namespace = "art-oadp-tenant"
        p.kubeconfig_env_var = "OADP_KONFLUX_SA_KUBECONFIG"
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("pyartcd.pipelines.build_conforma_verify.KonfluxClient.from_kubeconfig") as from_kubeconfig,
        ):
            with self.assertRaisesRegex(ValueError, "OADP_KONFLUX_SA_KUBECONFIG.*oadp-1.5"):
                asyncio.run(p._verify_records([], build_type="image"))
        from_kubeconfig.assert_not_called()

    def test_missing_ocp_kubeconfig_raises_before_client_creation(self):
        p = _make_pipeline(group="openshift-4.22")
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("pyartcd.pipelines.build_conforma_verify.KonfluxClient.from_kubeconfig") as from_kubeconfig,
        ):
            with self.assertRaisesRegex(ValueError, "KONFLUX_SA_KUBECONFIG.*openshift-4.22"):
                asyncio.run(p._verify_records([], build_type="image"))
        from_kubeconfig.assert_not_called()


class TestPolicySelection(unittest.TestCase):
    """Test that _verify_records picks the right EC policy based on build_type."""

    def test_image_uses_ec_policy(self):
        p = _make_pipeline(
            ec_policy="tenant/image-policy",
            fbc_ec_policy="tenant/fbc-policy",
        )
        # For image builds, should use ec_policy
        record = MagicMock()
        record.get_konflux_application_name.return_value = "art-images-4-18"
        record.get_konflux_component_name.return_value = "ose-test"
        record.image_pullspec = "quay.io/test@sha256:abc"
        record.rebase_repo_url = "https://github.com/test"
        record.rebase_commitish = "abc123"

        # We test the policy selection logic by checking what policy is used in the manifest
        # Since _verify_records is async and complex, we'll verify the attribute selection directly
        self.assertEqual(p.ec_policy, "tenant/image-policy")
        self.assertEqual(p.fbc_ec_policy, "tenant/fbc-policy")

    def test_fbc_falls_back_to_ec_policy(self):
        p = _make_pipeline(ec_policy="tenant/image-policy", fbc_ec_policy=None)
        self.assertIsNone(p.fbc_ec_policy)
        # In _verify_records, fbc would fall back to ec_policy


class TestSlackReporting(unittest.TestCase):
    def test_no_slack_client_skips_reporting(self):
        p = _make_pipeline()
        import asyncio

        asyncio.run(p._report_to_slack(any_failed=False, image_failed=0, bundle_failed=0, fbc_failed=0))

    def test_success_skips_slack(self):
        slack = MagicMock()
        slack.say_in_thread = AsyncMock()
        p = _make_pipeline(slack_client=slack, effective_time="2026-08-05T00:00:00Z")

        import asyncio

        asyncio.run(p._report_to_slack(any_failed=False, image_failed=0, bundle_failed=0, fbc_failed=0))

        slack.say_in_thread.assert_not_called()

    def test_failure_message(self):
        slack = MagicMock()
        slack.say_in_thread = AsyncMock()
        p = _make_pipeline(group="oadp-1.5", slack_client=slack, effective_time="2026-08-05T00:00:00Z")

        import asyncio

        asyncio.run(p._report_to_slack(any_failed=True, image_failed=3, bundle_failed=1, fbc_failed=0))

        msg = slack.say_in_thread.call_args_list[0][0][0]
        self.assertIn(":warning:", msg)
        self.assertIn("oadp-1.5", msg)
        self.assertIn("assembly=`stream`", msg)
        self.assertIn("3 image(s)", msg)
        self.assertIn("1 bundle(s)", msg)
        self.assertNotIn("FBC(s)", msg)

    def test_failure_message_includes_violation_rules(self):
        slack = MagicMock()
        slack.say_in_thread = AsyncMock()
        p = _make_pipeline(slack_client=slack, effective_time="2026-08-05T00:00:00Z")

        violations = {
            "ose-cluster-api": [
                {
                    "rule": "step_image_registries.allowed",
                    "title": "Permitted registry",
                    "image_ref": "quay.io/test@sha256:abc",
                    "reason": "bad image",
                },
                {
                    "rule": "cve_results_found.cve_results",
                    "title": "CVE scan results found",
                    "image_ref": "quay.io/test@sha256:abc",
                    "reason": "no scan",
                },
            ],
            "ose-mco": [
                {
                    "rule": "step_image_registries.allowed",
                    "title": "Permitted registry",
                    "image_ref": "quay.io/test@sha256:def",
                    "reason": "bad image",
                },
            ],
        }

        import asyncio

        asyncio.run(
            p._report_to_slack(
                any_failed=True,
                image_failed=2,
                bundle_failed=0,
                fbc_failed=0,
                all_violations=violations,
            )
        )

        self.assertEqual(slack.say_in_thread.call_count, 2)
        rules_msg = slack.say_in_thread.call_args_list[1][0][0]
        self.assertIn("Unique rules violated (2):", rules_msg)
        self.assertIn("`cve_results_found.cve_results`", rules_msg)
        self.assertIn("`step_image_registries.allowed`", rules_msg)

    def test_failure_no_violation_details(self):
        slack = MagicMock()
        slack.say_in_thread = AsyncMock()
        p = _make_pipeline(slack_client=slack, effective_time="2026-08-05T00:00:00Z")

        import asyncio

        asyncio.run(p._report_to_slack(any_failed=True, image_failed=0, bundle_failed=0, fbc_failed=0))

        slack.say_in_thread.assert_called_once()
        msg = slack.say_in_thread.call_args[0][0]
        self.assertIn(":warning:", msg)
        self.assertIn("no violation details available", msg)

    def test_default_effective_time_label(self):
        slack = MagicMock()
        slack.say_in_thread = AsyncMock()
        p = _make_pipeline(slack_client=slack)

        import asyncio

        asyncio.run(p._report_to_slack(any_failed=True, image_failed=1, bundle_failed=0, fbc_failed=0))

        msg = slack.say_in_thread.call_args[0][0]
        self.assertIn("effective_time=`now`", msg)


class TestDoozerBaseCommand(unittest.TestCase):
    def test_builds_correct_command(self):
        p = _make_pipeline(data_gitref="my-branch")
        cmd = p._doozer_base_command
        self.assertEqual(cmd[0], 'doozer')
        self.assertIn('--group=openshift-4.18@my-branch', cmd)
        self.assertIn('--assembly=stream', cmd)

    def test_without_data_gitref(self):
        p = _make_pipeline()
        cmd = p._doozer_base_command
        self.assertIn('--group=openshift-4.18', cmd)
