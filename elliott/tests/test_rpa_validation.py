import os
from copy import deepcopy
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from doozerlib.backend.konflux_client import API_VERSION, KIND_RELEASE_PLAN, KonfluxClient
from elliottlib.cli.konflux_release_cli import CreateReleaseCli, validate_rpa_cli, validate_snapshot_against_rpa
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
                    "namespace": "tenant",
                    "labels": {
                        "release.appstudio.openshift.io/releasePlanAdmission": f"unusual-{env}-admission",
                    },
                },
                "spec": {"application": "openshift-5-0", "target": f"managed-{env}"},
            }
            self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")] = {
                "metadata": {"name": f"unusual-{env}-admission", "namespace": f"managed-{env}"},
                "spec": {
                    "applications": ["openshift-5-0"],
                    "origin": "tenant",
                    "data": {"mapping": {"components": [{"name": "test-component"}]}},
                },
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

    def configure_fbc(self):
        self.shipment.metadata.fbc = True
        self.shipment.data = None
        for env in ("stage", "prod"):
            self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")]["spec"]["data"] = {
                "fbc": {"allowedPackages": ["actual-operator", "extra-operator"]}
            }

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_uses_rendered_package_names_and_renders_once_for_both_environments(self, render):
        self.configure_fbc()
        render.return_value = [{"schema": "olm.package", "name": "actual-operator"}]
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": "/tmp/job-auth"}):
            await self.validate(kind="fbc")
        render.assert_awaited_once()
        self.assertEqual(render.await_args.args, ("quay.io/test/image@sha256:abc",))
        self.assertEqual(render.await_args.kwargs["auth"].path, "/tmp/job-auth")
        self.assertEqual(self.client._get.await_count, 4)

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_missing_fbc_package_is_rejected_in_either_environment(self, render):
        self.configure_fbc()
        render.return_value = [{"schema": "olm.package", "name": "actual-operator"}]
        for env in ("stage", "prod"):
            with self.subTest(env=env):
                allowed = self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")][
                    "spec"
                ]["data"]["fbc"]["allowedPackages"]
                allowed.remove("actual-operator")
                with self.assertRaisesRegex(ValueError, f"{env}.*actual-operator"):
                    await self.validate(kind="fbc")
                allowed.append("actual-operator")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_checks_all_packages_and_deduplicates_identical_images(self, render):
        self.configure_fbc()
        duplicate = self.shipment.snapshot.spec.components[0].model_copy(update={"name": "second-fragment"})
        self.shipment.snapshot.spec.components.append(duplicate)
        render.return_value = [
            {"schema": "olm.package", "name": "actual-operator"},
            {"schema": "olm.package", "name": "missing-operator"},
        ]
        with self.assertRaisesRegex(ValueError, "test-component.*missing-operator"):
            await self.validate(kind="fbc")
        render.return_value[1]["name"] = "extra-operator"
        render.reset_mock()
        await self.validate(kind="fbc")
        render.assert_awaited_once()

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_each_distinct_fbc_image_is_validated(self, render):
        self.configure_fbc()
        second = self.shipment.snapshot.spec.components[0].model_copy(
            update={"name": "second-fragment", "containerImage": "quay.io/test/second@sha256:def"}
        )
        self.shipment.snapshot.spec.components.append(second)
        render.side_effect = [
            [{"schema": "olm.package", "name": "actual-operator"}],
            [{"schema": "olm.package", "name": "missing-operator"}],
        ]
        with self.assertRaisesRegex(ValueError, "second-fragment.*missing-operator"):
            await self.validate(kind="fbc")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_duplicate_component_names_cannot_hide_disallowed_packages(self, render):
        self.configure_fbc()
        second = self.shipment.snapshot.spec.components[0].model_copy(
            update={"containerImage": "quay.io/test/second@sha256:def"}
        )
        self.shipment.snapshot.spec.components.append(second)
        render.side_effect = [
            [{"schema": "olm.package", "name": "missing-operator"}],
            [{"schema": "olm.package", "name": "actual-operator"}],
        ]
        with self.assertRaisesRegex(ValueError, "test-component.*missing-operator"):
            await self.validate(kind="fbc")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_rejects_missing_empty_or_malformed_allowlist(self, render):
        self.configure_fbc()
        fbc = self.resources[("ReleasePlanAdmission", "unusual-stage-admission", "managed-stage")]["spec"]["data"][
            "fbc"
        ]
        for value in (None, [], "actual-operator", [None], [""]):
            with self.subTest(value=value):
                fbc["allowedPackages"] = value
                with self.assertRaisesRegex(ValueError, "allowedPackages"):
                    await self.validate(kind="fbc")
        render.assert_not_awaited()

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_rejects_catalogs_without_valid_packages(self, render):
        self.configure_fbc()
        for blobs in (
            [],
            [{"schema": "olm.bundle", "package": "actual-operator"}],
            [{"schema": "olm.package"}],
            [{"schema": "olm.package", "name": ""}],
            [{"schema": "olm.package", "name": 42}],
        ):
            with self.subTest(blobs=blobs):
                render.return_value = blobs
                with self.assertRaisesRegex(ValueError, "test-component.*package"):
                    await self.validate(kind="fbc")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_render_failure_has_component_context(self, render):
        self.configure_fbc()
        render.side_effect = IOError("registry unauthorized")
        with self.assertRaisesRegex(ValueError, "test-component.*registry unauthorized"):
            await self.validate(kind="fbc")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_explicit_registry_auth_takes_precedence(self, render):
        self.configure_fbc()
        render.return_value = [{"schema": "olm.package", "name": "actual-operator"}]
        with patch.dict(os.environ, {"QUAY_AUTH_FILE": "/tmp/job-auth"}):
            await validate_snapshot_against_rpa(
                self.shipment, "stage", "fbc", self.client, registry_auth_file="/tmp/release-ci-auth"
            )
        self.assertEqual(render.await_args.kwargs["auth"].path, "/tmp/release-ci-auth")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_release_creation_passes_pull_secret_and_rejects_missing_fbc_package_before_writes(self, render):
        self.configure_fbc()
        render.return_value = [{"schema": "olm.package", "name": "missing-operator"}]
        original_get = self.client._get.side_effect

        async def get(version, kind, name, namespace=None):
            if kind == "Application":
                return ResourceInstance(None, {"apiVersion": version, "kind": kind, "metadata": {"name": name}})
            return await original_get(version, kind, name, namespace)

        self.client._get.side_effect = get
        self.client.verify_connection = MagicMock()
        runtime = MagicMock()
        runtime.product = self.shipment.metadata.product
        runtime.group = self.shipment.metadata.group
        runtime.assembly = self.shipment.metadata.assembly
        runtime.shipment_gitdata.load_yaml_file.return_value = {"shipment": self.shipment.model_dump(exclude_none=True)}
        with patch.object(KonfluxClient, "from_kubeconfig", autospec=True, return_value=self.client):
            command = CreateReleaseCli(
                runtime,
                "shipment.yaml",
                "prod",
                {"namespace": "tenant", "kubeconfig": None, "context": None},
                image_repo_pull_secret="/tmp/release-ci-auth",
                dry_run=True,
                kind="fbc",
            )
            with self.assertRaisesRegex(ValueError, "missing-operator"):
                await command.run()
        self.client._create.assert_not_awaited()
        self.assertEqual(render.await_args.kwargs["auth"].path, "/tmp/release-ci-auth")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    def test_standalone_fbc_command_uses_registry_environment_credentials(self, render):
        self.configure_fbc()
        render.return_value = [{"schema": "olm.package", "name": "actual-operator"}]
        runtime = MagicMock()
        runtime.shipment_gitdata.load_yaml_file.return_value = {"shipment": self.shipment.model_dump(exclude_none=True)}
        with (
            patch.dict(os.environ, {"QUAY_AUTH_FILE": "", "IMAGE_REPO_PULL_SECRET": "/tmp/ci-auth"}),
            patch.object(KonfluxClient, "from_kubeconfig", autospec=True, return_value=self.client),
        ):
            result = CliRunner().invoke(
                validate_rpa_cli, ["--config", "shipment.yaml", "--env", "prod", "--kind", "fbc"], obj=runtime
            )
        self.assertEqual(result.exit_code, 0, str(result.exception))
        render.assert_awaited_once()
        self.assertEqual(render.await_args.kwargs["auth"].path, "/tmp/ci-auth")

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

    async def test_rpa_application_mismatch_warns_in_both_environments(self):
        for env in ("stage", "prod"):
            spec = self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")]["spec"]
            for applications in (None, [], ["different-app"], "openshift-5-0"):
                with self.subTest(env=env, applications=applications):
                    spec["applications"] = applications
                    with self.assertLogs("art_tools.elliottlib.cli.konflux_release_cli", level="WARNING") as logs:
                        await self.validate()
                    self.assertRegex("\n".join(logs.output), f"{env}.*applications.*openshift-5-0")
            spec["applications"] = ["openshift-5-0"]

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_application_warning_does_not_skip_package_validation(self, render):
        self.configure_fbc()
        render.return_value = [{"schema": "olm.package", "name": "missing-operator"}]
        self.resources[("ReleasePlanAdmission", "unusual-stage-admission", "managed-stage")]["spec"]["applications"] = [
            "other-app"
        ]
        with self.assertLogs("art_tools.elliottlib.cli.konflux_release_cli", level="WARNING") as logs:
            with self.assertRaisesRegex(ValueError, "missing-operator"):
                await self.validate(kind="fbc")
        self.assertIn("applications", "\n".join(logs.output))

    async def test_rpa_origin_must_match_release_plan_namespace_in_both_environments(self):
        for env in ("stage", "prod"):
            spec = self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")]["spec"]
            for origin in (None, "", "other-tenant"):
                with self.subTest(env=env, origin=origin):
                    spec["origin"] = origin
                    with self.assertRaisesRegex(ValueError, f"{env}.*origin.*tenant"):
                        await self.validate()
            spec["origin"] = "tenant"

    async def test_binding_uses_rp_namespace_and_allows_multiple_applications(self):
        for env in ("stage", "prod"):
            self.resources[(KIND_RELEASE_PLAN, f"custom-{env}-plan", None)]["metadata"]["namespace"] = "custom-tenant"
            spec = self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")]["spec"]
            spec["origin"] = "custom-tenant"
            spec["applications"] = ["another-app", "openshift-5-0"]
        await self.validate()

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fbc_binding_failure_prevents_rendering(self, render):
        self.configure_fbc()
        self.resources[("ReleasePlanAdmission", "unusual-stage-admission", "managed-stage")]["spec"]["origin"] = (
            "other-tenant"
        )
        with self.assertRaisesRegex(ValueError, "origin"):
            await self.validate(kind="fbc")
        render.assert_not_awaited()

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_blocked_rpa_fails_for_image_and_fbc_before_rendering(self, render):
        for env in ("stage", "prod"):
            rpa = self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")]
            rpa["metadata"]["labels"] = {"release.appstudio.openshift.io/block-releases": "true"}
            for kind in ("image", "fbc"):
                with self.subTest(env=env, kind=kind):
                    with self.assertRaisesRegex(ValueError, f"{env}.*block-releases"):
                        await self.validate(env=env, kind=kind)
            del rpa["metadata"]["labels"]
        render.assert_not_awaited()

    async def test_absent_or_false_block_label_allows_validation(self):
        for value in (None, "false"):
            with self.subTest(value=value):
                for env in ("stage", "prod"):
                    metadata = self.resources[("ReleasePlanAdmission", f"unusual-{env}-admission", f"managed-{env}")][
                        "metadata"
                    ]
                    metadata["labels"] = (
                        {} if value is None else {"release.appstudio.openshift.io/block-releases": value}
                    )
                await self.validate()
