import os
from copy import deepcopy
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from doozerlib.backend.konflux_client import API_VERSION, KIND_RELEASE_PLAN, KonfluxClient
from elliottlib.cli.konflux_release_cli import validate_rpa_cli, validate_snapshot_against_rpa
from elliottlib.shipment_model import Shipment
from kubernetes.dynamic import ResourceInstance


class TestRPAValidation(IsolatedAsyncioTestCase):
    def setUp(self):
        self.shipment = Shipment.model_validate(
            {
                "metadata": {
                    "product": "ocp",
                    "group": "openshift-5.0",
                    "assembly": "5.0.0",
                    "application": "openshift-5-0",
                },
                "environments": {
                    "stage": {"releasePlan": "custom-stage-plan"},
                    "prod": {"releasePlan": "custom-prod-plan"},
                },
                "snapshot": {
                    "nvrs": ["test-1-1"],
                    "spec": {
                        "application": "openshift-5-0",
                        "components": [
                            {
                                "name": "test-component",
                                "containerImage": "quay.io/test/image@sha256:abc",
                                "source": {"git": {"url": "https://example.com/test", "revision": "abc"}},
                            }
                        ],
                    },
                },
                "data": {"releaseNotes": {"type": "RHBA"}},
            }
        )
        self.client = AsyncMock()
        self.client.default_namespace = "tenant"
        self.resources = {}
        for env in ("stage", "prod"):
            self.resources[(KIND_RELEASE_PLAN, f"custom-{env}-plan", None)] = {
                "metadata": {
                    "name": f"custom-{env}-plan",
                    "labels": {
                        "release.appstudio.openshift.io/releasePlanAdmission": f"unusual-{env}-admission",
                    },
                },
                "spec": {"application": "openshift-5-0", "target": f"managed-{env}"},
            }
            self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")] = {
                "metadata": {"name": f"unusual-{env}-admission", "namespace": f"managed-{env}"},
                "spec": {"data": {"mapping": {"components": [{"name": "test-component"}]}}},
            }

        async def get(api_version, kind, name, namespace=None):
            self.assertEqual(api_version, API_VERSION)
            return ResourceInstance(
                None, {"apiVersion": api_version, "kind": kind, **deepcopy(self.resources[(kind, name, namespace)])}
            )

        self.client._get.side_effect = get

    async def validate(self, env="stage", kind="image"):
        await validate_snapshot_against_rpa(self.shipment, env, kind, self.client)

    async def test_ocp_5_uses_configured_plans_and_target_namespaces(self):
        await self.validate()
        self.assertEqual(self.client._get.await_args_list[0].args[2], "custom-stage-plan")
        self.assertEqual(self.client._get.await_args_list[2].args[2], "custom-prod-plan")

    async def test_checks_production_first_when_requested(self):
        await self.validate("prod")
        self.assertEqual(self.client._get.await_args_list[0].args[2], "custom-prod-plan")

    async def test_layered_products_and_unrecognized_groups_are_not_skipped(self):
        for product, group in (
            ("oadp", "oadp-1.5"),
            ("mta", "mta-8.2"),
            ("logging", "logging-6.6"),
            ("new-product", "nonstandard-group"),
        ):
            with self.subTest(product=product):
                self.shipment.metadata.product = product
                self.shipment.metadata.group = group
                self.resources[("ReleasePlanAdmission", "unusual-prod-admission", "managed-prod")]["spec"]["data"][
                    "mapping"
                ]["components"] = []
                with self.assertRaisesRegex(ValueError, "unusual-prod-admission"):
                    await self.validate()

    async def test_rejects_missing_component_in_stage_or_production(self):
        for env in ("stage", "prod"):
            with self.subTest(env=env):
                rpa = self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")]
                rpa["spec"]["data"]["mapping"]["components"] = [{"name": "other-component"}]
                with self.assertRaisesRegex(ValueError, "test-component"):
                    await self.validate()
                rpa["spec"]["data"]["mapping"]["components"] = [{"name": "test-component"}]

    async def test_rejects_plan_without_admission_label(self):
        self.resources[(KIND_RELEASE_PLAN, "custom-stage-plan", None)]["metadata"]["labels"] = {}
        with self.assertRaisesRegex(ValueError, "custom-stage-plan.*releasePlanAdmission"):
            await self.validate()

    async def test_rejects_plan_without_target_namespace(self):
        del self.resources[(KIND_RELEASE_PLAN, "custom-stage-plan", None)]["spec"]["target"]
        with self.assertRaisesRegex(ValueError, "custom-stage-plan.*target"):
            await self.validate()

    async def test_rejects_plan_for_different_application(self):
        self.resources[(KIND_RELEASE_PLAN, "custom-stage-plan", None)]["spec"]["application"] = "wrong-app"
        with self.assertRaisesRegex(ValueError, "application"):
            await self.validate()

    async def test_read_failure_is_not_silently_skipped(self):
        self.client._get.side_effect = ValueError("Forbidden")
        with self.assertRaisesRegex(ValueError, "Forbidden"):
            await self.validate()

    async def test_intentionally_skipped_stage_still_validates_production(self):
        self.shipment.environments.stage.releasePlan = "skipped"
        await self.validate("prod")
        self.assertEqual(self.client._get.await_count, 2)

    async def test_cannot_validate_a_skipped_requested_environment(self):
        self.shipment.environments.stage.releasePlan = "skipped"
        with self.assertRaisesRegex(ValueError, "stage"):
            await self.validate()

    async def test_production_cannot_be_skipped_during_stage_validation(self):
        self.shipment.environments.prod.releasePlan = "skipped"
        with self.assertRaisesRegex(ValueError, "prod"):
            await self.validate()

    async def test_rejects_missing_or_placeholder_release_plan(self):
        for plan in ("", "n/a", "   "):
            with self.subTest(plan=plan):
                self.shipment.environments.prod.releasePlan = plan
                with self.assertRaisesRegex(ValueError, "prod"):
                    await self.validate()

    async def test_fbc_remains_explicitly_skipped(self):
        await self.validate(kind="fbc")
        self.client._get.assert_not_awaited()

    async def test_invalid_environment_fails(self):
        with self.assertRaisesRegex(ValueError, "env"):
            await self.validate("invalid")

    async def test_pipeline_resolves_product_kubeconfig_and_namespace(self):
        self.shipment.metadata.product = "mta"
        with (
            patch.dict(os.environ, {"MTA_KONFLUX_SA_KUBECONFIG": "/tmp/mta-kubeconfig"}),
            patch.object(KonfluxClient, "from_kubeconfig", autospec=True, return_value=self.client) as factory,
        ):
            await validate_snapshot_against_rpa(self.shipment, "stage", "image")
        factory.assert_called_once_with(
            default_namespace="art-mta-tenant", config_file="/tmp/mta-kubeconfig", context=None, dry_run=True
        )

    def test_cli_uses_job_credentials_without_connection_options(self):
        self.shipment.metadata.product = "mta"
        runtime = MagicMock()
        runtime.shipment_gitdata.load_yaml_file.return_value = {"shipment": self.shipment.model_dump(exclude_none=True)}
        with (
            patch.dict(os.environ, {"MTA_KONFLUX_SA_KUBECONFIG": "/tmp/mta-job-kubeconfig"}),
            patch.object(KonfluxClient, "from_kubeconfig", autospec=True, return_value=self.client) as factory,
        ):
            result = CliRunner().invoke(
                validate_rpa_cli, ["--config", "shipment.yaml", "--env", "stage", "--kind", "image"], obj=runtime
            )
        self.assertEqual(result.exit_code, 0, str(result.exception))
        factory.assert_called_once_with(
            default_namespace="art-mta-tenant", config_file="/tmp/mta-job-kubeconfig", context=None, dry_run=True
        )
