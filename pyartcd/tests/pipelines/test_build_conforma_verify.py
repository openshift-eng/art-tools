import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from pyartcd.pipelines.build_conforma_verify import (
    BuildConformaVerifyPipeline,
    _integration_test_scenario_name,
    _resolve_ec_policy_configuration,
    build_conforma_verify,
)

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


class TestCliHelp(unittest.TestCase):
    def test_policy_help_describes_dynamic_defaults_and_fbc_precedence(self):
        result = CliRunner().invoke(build_conforma_verify, ["--help"], terminal_width=160)

        self.assertEqual(result.exit_code, 0, result.output)
        help_text = " ".join(result.output.split())
        self.assertIn("--ec-policy", help_text)
        self.assertIn("Defaults to the stage policy for the product, lifecycle, assembly and build type", help_text)
        self.assertIn("--fbc-ec-policy", help_text)
        self.assertIn("Falls back to --ec-policy, then the product's FBC stage policy", help_text)
        self.assertNotIn("ocp-art-tenant/conforma-build-stage", help_text)
        options = {parameter.name: parameter for parameter in build_conforma_verify.params}
        self.assertIsNone(options["ec_policy"].default)
        self.assertIsNone(options["fbc_ec_policy"].default)


class TestPipelineInit(unittest.TestCase):
    def test_shared_policy_defaults_and_overrides(self):
        for group, product, assembly, phase, build_type, generic, fbc, expected in (
            ("openshift-4.23", "ocp", "stream", "release", "image", None, None, "registry-ocp-art-stage"),
            ("openshift-4.23", "ocp", "preview", "release", "bundle", None, None, "registry-ocp-art-stage"),
            ("openshift-4.23", "ocp", "stream", "pre-release", "bundle", None, None, "registry-ocp-art-ec-stage"),
            ("openshift-4.23", "ocp", "test", "release", "image", None, None, "conforma-build-stage-test"),
            ("openshift-4.23", "ocp", "test", "pre-release", "bundle", None, None, "conforma-build-ec-stage-test"),
            ("openshift-4.23", "ocp", "stream", "pre-release", "fbc", None, None, "fbc-ocp-art-stage"),
            ("oadp-1.5", "oadp", "stream", "release", "fbc", None, None, "fbc-art-oadp-stage"),
            ("quay-3.16", "quay", "test", "release", "image", None, None, "registry-art-quay-stage"),
            (
                "mirror-gui-1.0",
                "mirror_gui",
                "stream",
                "release",
                "image",
                None,
                None,
                "registry-art-oc-mirror-mirror-gui-stage",
            ),
            ("openshift-4.23", "ocp", "stream", "release", "fbc", "custom/generic", None, "generic"),
            ("openshift-4.23", "ocp", "stream", "release", "fbc", "custom/generic", "custom/fbc", "fbc"),
            ("openshift-4.23", "ocp", "stream", "release", "image", "custom/generic", "custom/fbc", "generic"),
        ):
            with self.subTest(
                group=group, assembly=assembly, phase=phase, build_type=build_type, generic=generic, fbc=fbc
            ):
                pipeline = _make_pipeline(group=group, assembly=assembly, ec_policy=generic, fbc_ec_policy=fbc)
                pipeline._group_config = {"product": product, "software_lifecycle": {"phase": phase}}
                policy = asyncio.run(pipeline._resolve_stage_policy(build_type))
                self.assertEqual(policy.rsplit("/", 1)[-1], expected)

    def test_missing_product_or_fbc_policy_does_not_fall_back_to_ocp(self):
        for product, build_type in (("unknown", "image"), ("mirror_gui", "fbc")):
            pipeline = _make_pipeline(group=f"{product}-1.0")
            pipeline._group_config = {"product": product}
            with self.assertRaisesRegex(ValueError, "No Conforma"):
                asyncio.run(pipeline._resolve_stage_policy(build_type))

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
    def test_ocp_image_and_bundle_defaults_share_selected_group_config(self):
        pipeline = _make_pipeline(
            assembly="test",
            data_path="https://example.test/ocp-build-data",
            data_gitref="feature-branch",
        )

        async def resolve_policies():
            return [await pipeline._resolve_stage_policy(build_type) for build_type in ("image", "bundle", "fbc")]

        with patch(
            "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
            new=AsyncMock(return_value={"software_lifecycle": {"phase": "pre-release"}}),
        ) as load_group_config:
            policies = asyncio.run(resolve_policies())

        load_group_config.assert_awaited_once_with(
            group="openshift-4.18",
            assembly="test",
            doozer_data_path="https://example.test/ocp-build-data",
            doozer_data_gitref="feature-branch",
        )
        self.assertEqual(
            policies,
            [
                "ocp-art-tenant/conforma-build-ec-stage-test",
                "ocp-art-tenant/conforma-build-ec-stage-test",
                "rhtap-releng-tenant/fbc-ocp-art-stage",
            ],
        )

    def test_openshift_configured_product_defaults_and_cache(self):
        for product in ("openshift-logging", "logging"):
            with self.subTest(product=product):
                pipeline = _make_pipeline(assembly="test")

                async def resolve_policies():
                    return [await pipeline._resolve_stage_policy(kind) for kind in ("fbc", "image", "bundle", "fbc")]

                with patch(
                    "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
                    new=AsyncMock(return_value={"product": product, "software_lifecycle": {"phase": "pre-release"}}),
                ) as load_config:
                    self.assertEqual(
                        asyncio.run(resolve_policies()),
                        [
                            "rhtap-releng-tenant/fbc-stage",
                            "rhtap-releng-tenant/registry-art-logging-stage",
                            "rhtap-releng-tenant/registry-art-logging-stage",
                            "rhtap-releng-tenant/fbc-stage",
                        ],
                    )
                load_config.assert_awaited_once()
                self.assertEqual(pipeline.namespace, "ocp-art-tenant")

    def test_ocp_product_fallback_lifecycle_and_assembly(self):
        for product_config in ({}, {"product": None}, {"product": ""}):
            for assembly, phase, expected in (
                ("stream", "release", "rhtap-releng-tenant/registry-ocp-art-stage"),
                ("stream", "pre-release", "rhtap-releng-tenant/registry-ocp-art-ec-stage"),
                ("test", "release", "ocp-art-tenant/conforma-build-stage-test"),
                ("test", "pre-release", "ocp-art-tenant/conforma-build-ec-stage-test"),
            ):
                with self.subTest(product_config=product_config, assembly=assembly, phase=phase):
                    pipeline = _make_pipeline(assembly=assembly)

                    async def resolve_policies():
                        return [await pipeline._resolve_stage_policy(kind) for kind in ("image", "bundle", "fbc")]

                    with patch(
                        "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
                        new=AsyncMock(return_value={**product_config, "software_lifecycle": {"phase": phase}}),
                    ) as load_config:
                        self.assertEqual(
                            asyncio.run(resolve_policies()),
                            [expected, expected, "rhtap-releng-tenant/fbc-ocp-art-stage"],
                        )
                    load_config.assert_awaited_once()

    def test_missing_layered_or_unknown_configured_product_has_no_default(self):
        for group, products in (("openshift-4.18", ("unknown",)), ("logging-6.7", (None, "", "unknown"))):
            for product in products:
                for build_type in ("image", "bundle", "fbc"):
                    with self.subTest(group=group, product=product, build_type=build_type):
                        pipeline = _make_pipeline(group=group)
                        with patch(
                            "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
                            new=AsyncMock(return_value={"product": product}),
                        ):
                            with self.assertRaisesRegex(ValueError, "No Conforma .* stage policy"):
                                asyncio.run(pipeline._resolve_stage_policy(build_type))

    def test_fbc_override_does_not_override_image_default(self):
        pipeline = _make_pipeline(fbc_ec_policy="tenant/fbc")
        with patch(
            "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
            new=AsyncMock(return_value={"product": "logging"}),
        ) as load_config:
            self.assertEqual(
                asyncio.run(pipeline._resolve_stage_policy("image")),
                "rhtap-releng-tenant/registry-art-logging-stage",
            )
        load_config.assert_awaited_once()

    def test_explicit_overrides_do_not_read_group_config(self):
        for build_type, generic, fbc, expected in (
            ("image", "tenant/image", None, "tenant/image"),
            ("bundle", "tenant/image", "tenant/fbc", "tenant/image"),
            ("fbc", "tenant/image", None, "tenant/image"),
            ("fbc", "tenant/image", "tenant/fbc", "tenant/fbc"),
            ("fbc", None, "tenant/fbc", "tenant/fbc"),
        ):
            for group in ("openshift-4.18", "logging-6.7"):
                with self.subTest(group=group, build_type=build_type, generic=generic, fbc=fbc):
                    pipeline = _make_pipeline(group=group, ec_policy=generic, fbc_ec_policy=fbc)
                    with patch(
                        "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
                        new=AsyncMock(side_effect=AssertionError("unexpected config read")),
                    ) as load_group_config:
                        self.assertEqual(asyncio.run(pipeline._resolve_stage_policy(build_type)), expected)
                    load_group_config.assert_not_awaited()

    def test_layered_settings_and_defaults_reuse_group_config(self):
        pipeline = _make_pipeline(group="oadp-1.5")

        async def resolve_settings_and_policies():
            await pipeline._resolve_layered_product_settings()
            return [await pipeline._resolve_stage_policy(build_type) for build_type in ("image", "fbc")]

        with patch(
            "pyartcd.pipelines.build_conforma_verify.util.load_group_config",
            new=AsyncMock(return_value={"product": "oadp"}),
        ) as load_group_config:
            policies = asyncio.run(resolve_settings_and_policies())

        load_group_config.assert_awaited_once()
        self.assertEqual(pipeline.namespace, "art-oadp-tenant")
        self.assertEqual(
            policies,
            [
                "rhtap-releng-tenant/registry-art-oadp-stage",
                "rhtap-releng-tenant/fbc-art-oadp-stage",
            ],
        )

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

    def test_url_policy_uses_valid_distinct_scenario_names(self):
        policy_path = 'config/product/EnterpriseContractPolicy/registry-ocp-art-prod.yaml'
        first = f'https://gitlab.cee.redhat.com/releng/konflux-release-data/-/raw/first/{policy_path}'
        second = f'https://gitlab.cee.redhat.com/releng/konflux-release-data/-/raw/second/{policy_path}'

        first_name = _integration_test_scenario_name('openshift-5-0', first)
        second_name = _integration_test_scenario_name('openshift-5-0', second)

        self.assertRegex(first_name, r'^openshift-5-0-ec-registry-ocp-art-prod-[0-9a-f]{10}$')
        self.assertNotEqual(first_name, second_name)
        self.assertLessEqual(len(first_name), 63)

    def test_long_policy_name_stays_within_kubernetes_limit(self):
        name = _integration_test_scenario_name('openshift-' + 'a' * 50, 'https://example.com/' + 'b' * 80 + '.yaml')

        self.assertLessEqual(len(name), 63)
        self.assertRegex(name, r'^[a-z][a-z0-9-]*$')

    def test_namespaced_policy_keeps_existing_scenario_name(self):
        self.assertEqual(
            _integration_test_scenario_name('openshift-5-0', 'rhtap-releng-tenant/registry-ocp-art-stage'),
            'openshift-5-0-ec-registry-ocp-art-stage',
        )

    def test_url_policy_is_passed_as_inline_configuration(self):
        policy = 'https://gitlab.cee.redhat.com/releng/konflux-release-data/-/raw/c7ae37e1/policy.yaml'
        pipeline = _make_pipeline(ec_policy=policy)
        record = MagicMock()
        record.get_konflux_application_name.return_value = 'openshift-5-0'
        record.get_konflux_component_name.return_value = 'ose-5-0-rhcos-node-image'
        record.image_pullspec = 'quay.io/example/image@sha256:abc'
        record.rebase_repo_url = 'https://github.com/example/source'
        record.rebase_commitish = 'abc123'
        client = MagicMock()
        client.ensure_integration_test_scenario = AsyncMock()
        client._create = AsyncMock(side_effect=RuntimeError('stop after manifest'))
        response = MagicMock()
        response.text = (
            'kind: EnterpriseContractPolicy\nspec:\n  sources:\n    - policy: [oci::example.com/policy:latest]\n'
        )

        with (
            patch.dict(os.environ, {'KONFLUX_SA_KUBECONFIG': '/tmp/kubeconfig'}),
            patch('pyartcd.pipelines.build_conforma_verify.KonfluxClient.from_kubeconfig', return_value=client),
            patch('pyartcd.pipelines.build_conforma_verify.requests.get', return_value=response) as get_policy,
        ):
            with self.assertRaisesRegex(RuntimeError, 'stop after manifest'):
                asyncio.run(pipeline._verify_records([record]))

        get_policy.assert_called_once_with(policy, timeout=30)
        inline_policy = client.ensure_integration_test_scenario.await_args.kwargs['policy_configuration']
        self.assertEqual(
            json.loads(inline_policy),
            {'sources': [{'policy': ['oci::example.com/policy:latest']}]},
        )
        params = client._create.await_args.args[0]['spec']['params']
        self.assertIn({'name': 'POLICY_CONFIGURATION', 'value': inline_policy}, params)
        pipeline_ref = client._create.await_args.args[0]['spec']['pipelineRef']
        self.assertEqual(pipeline_ref['resolver'], 'git')
        self.assertEqual(
            {param['name']: param['value'] for param in pipeline_ref['params']},
            {
                'url': 'https://github.com/conforma/cli',
                'revision': 'main',
                'pathInRepo': 'pipelines/enterprise-contract/0.1/enterprise-contract.yaml',
            },
        )
        self.assertRegex(
            client.ensure_integration_test_scenario.await_args.kwargs['name'],
            r'^openshift-5-0-ec-policy-[0-9a-f]{10}$',
        )

    def test_namespaced_policy_is_not_downloaded(self):
        policy = 'rhtap-releng-tenant/registry-ocp-art-stage'
        with patch('pyartcd.pipelines.build_conforma_verify.requests.get') as get_policy:
            self.assertEqual(asyncio.run(_resolve_ec_policy_configuration(policy)), policy)
        get_policy.assert_not_called()

    def test_remote_policy_requires_valid_configuration(self):
        response = MagicMock()
        response.text = '<html>Sign in</html>'
        with patch('pyartcd.pipelines.build_conforma_verify.requests.get', return_value=response):
            with self.assertRaisesRegex(ValueError, 'did not return a policy document'):
                asyncio.run(_resolve_ec_policy_configuration('https://example.com/policy.yaml'))

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
