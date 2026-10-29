from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.backend.konflux_client import KonfluxClient
from elliottlib.shipment_model import ShipmentConfig
from kubernetes.dynamic import ResourceInstance
from pyartcd.pipelines.prepare_release_lp import PrepareReleaseLPPipeline
from pyartcd.pipelines.release_from_fbc import ReleaseFromFbcPipeline


class TestLPPreparationRPAValidation(IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.shipment = ShipmentConfig.model_validate(
            {
                "shipment": {
                    "metadata": {
                        "product": "oadp",
                        "group": "oadp-1.5",
                        "assembly": "1.5.8",
                        "application": "oadp-1-5",
                    },
                    "environments": {"stage": {"releasePlan": "custom-stage"}, "prod": {"releasePlan": "custom-prod"}},
                    "snapshot": {
                        "nvrs": ["oadp-container-v1.5.8-1.el9.p2"],
                        "spec": {
                            "application": "oadp-1-5",
                            "components": [
                                {
                                    "name": "oadp-operator",
                                    "containerImage": "quay.io/test/image@sha256:abc",
                                    "source": {"git": {"url": "https://example.com/source", "revision": "abc"}},
                                }
                            ],
                        },
                    },
                },
            }
        )
        self.allowed = {"stage": ["oadp-operator"], "prod": ["oadp-operator"]}
        self.reads = []

        async def get(version, kind, name, namespace=None):
            self.reads.append((kind, name, namespace))
            env = name.rsplit("-", 1)[-1]
            if kind == "ReleasePlan":
                data = {
                    "metadata": {
                        "namespace": "art-oadp-tenant",
                        "labels": {"release.appstudio.openshift.io/releasePlanAdmission": f"admission-{env}"},
                    },
                    "spec": {"target": "managed", "application": "oadp-1-5"},
                }
            else:
                data = {
                    "spec": {
                        "applications": ["oadp-1-5"],
                        "origin": "art-oadp-tenant",
                        "data": {
                            "mapping": {"components": [{"name": name} for name in self.allowed[env]]},
                            "fbc": {"allowedPackages": self.allowed[env]},
                        },
                    }
                }
            return ResourceInstance(None, {"apiVersion": version, "kind": kind, **data})

        self.client = MagicMock()
        self.client._get = AsyncMock(side_effect=get)
        self.factory = self.enterContext(
            patch.object(KonfluxClient, "from_kubeconfig", autospec=True, return_value=self.client)
        )

    def make_pipeline(self, name, create_mr=True, fbc_only=False):
        runtime = MagicMock()
        runtime.dry_run = True
        runtime.working_dir = Path(self.directory.name)
        runtime.config = {}
        config = self.shipment.model_copy(deep=True)
        config.shipment.metadata.fbc = fbc_only
        if name == "prepare-release-lp":
            pipeline = PrepareReleaseLPPipeline(runtime, "oadp-1.5", "1.5.8", create_mr=create_mr)
            pipeline._check_env_vars = MagicMock()
            pipeline._setup_working_dir = MagicMock()
            pipeline._setup_shipment_repo = AsyncMock()
            pipeline._load_product_from_group_config = AsyncMock(return_value="oadp")
            pipeline._load_assembly = AsyncMock(return_value={"assembly": {"type": "standard"}})
            pipeline._extract_operand_nvrs = MagicMock(return_value=[] if fbc_only else config.shipment.snapshot.nvrs)
            pipeline._trigger_bundle_build = AsyncMock(return_value=([], []))
            pipeline._trigger_fbc_build = AsyncMock(
                return_value=(["oadp-operator-fbc-1.5.8-1.ocp5.0"] if fbc_only else [], [])
            )
            pipeline._load_release_notes_template = MagicMock(return_value=None)
            pipeline._create_snapshot = AsyncMock(return_value=[config.shipment.snapshot])
            pipeline._create_shipment_config = MagicMock(return_value=config)
            pipeline._create_shipment_mr = AsyncMock(return_value=None)
            pipeline._write_shipment_file = AsyncMock()
            publish = pipeline._create_shipment_mr
            write = pipeline._write_shipment_file
        else:
            pipeline = ReleaseFromFbcPipeline(
                runtime,
                "oadp-1.5",
                "1.5.8",
                fbc_pullspecs=[],
                create_mr=create_mr,
                extra_image_nvrs=config.shipment.snapshot.nvrs,
            )
            pipeline.check_env_vars = MagicMock()
            pipeline.setup_working_dir = MagicMock()
            pipeline.setup_shipment_repo = AsyncMock()
            pipeline._load_product_from_group_config = AsyncMock(return_value="oadp")
            pipeline._load_layered_product_shipment_mr = MagicMock(return_value=None)
            pipeline._load_release_notes_template = MagicMock(return_value=None)
            pipeline.create_snapshot = AsyncMock(return_value=config.shipment.snapshot)
            pipeline.create_shipment_config = MagicMock(return_value=config)
            pipeline.create_shipment_mr = AsyncMock(return_value="https://gitlab.example/project/-/merge_requests/1")
            pipeline._update_layered_product_shipment_mr = AsyncMock()
            pipeline.set_shipment_mr_ready = AsyncMock()
            pipeline.write_shipment_files_locally = AsyncMock()
            publish = pipeline.create_shipment_mr
            write = pipeline.write_shipment_files_locally
            if fbc_only:
                fbc_nvr = "oadp-operator-fbc-1.5.8-1.ocp5.0"
                pipeline.fbc_pullspecs = [config.shipment.snapshot.spec.components[0].containerImage]
                pipeline.extra_image_nvrs = []
                pipeline.extract_fbc_nvr = MagicMock(return_value=fbc_nvr)
                pipeline._load_ignore_patch_version_from_group_config = AsyncMock(return_value=False)
                pipeline.validate_fbc_related_images = AsyncMock(return_value=[])
                pipeline.categorize_nvrs = MagicMock(return_value={"image": [], "fbc": [fbc_nvr], "external": []})
        return pipeline, publish, write

    async def test_both_pipelines_validate_configured_rpas_before_publishing(self):
        for name in ("prepare-release-lp", "release-from-fbc"):
            with self.subTest(pipeline=name):
                self.reads.clear()
                pipeline, publish, _ = self.make_pipeline(name)

                async def publish_after_validation(*args, **kwargs):
                    self.assertEqual(
                        self.reads,
                        [
                            ("ReleasePlan", "custom-prod", None),
                            ("ReleasePlanAdmission", "admission-prod", "managed"),
                            ("ReleasePlan", "custom-stage", None),
                            ("ReleasePlanAdmission", "admission-stage", "managed"),
                        ],
                    )

                publish.side_effect = publish_after_validation
                await pipeline.run()
                publish.assert_awaited_once()

    async def test_missing_components_block_publication_in_both_pipelines(self):
        for name in ("prepare-release-lp", "release-from-fbc"):
            for env in ("stage", "prod"):
                with self.subTest(pipeline=name, env=env):
                    self.allowed[env] = ["unrelated-component"]
                    pipeline, publish, write = self.make_pipeline(name)
                    with self.assertRaisesRegex(ValueError, "oadp-operator"):
                        await pipeline.run()
                    publish.assert_not_awaited()
                    write.assert_not_awaited()
                    self.allowed[env] = ["oadp-operator"]

    async def test_local_drafts_do_not_require_release_plan_access(self):
        for name in ("prepare-release-lp", "release-from-fbc"):
            with self.subTest(pipeline=name):
                self.factory.reset_mock()
                pipeline, publish, write = self.make_pipeline(name, create_mr=False)
                await pipeline.run()
                publish.assert_not_awaited()
                write.assert_awaited_once()
                self.factory.assert_not_called()

    async def test_invalid_components_leave_existing_merge_requests_untouched(self):
        self.allowed["prod"] = ["unrelated-component"]
        for name in ("prepare-release-lp", "release-from-fbc"):
            with self.subTest(pipeline=name):
                pipeline, publish, _ = self.make_pipeline(name)
                pipeline._configured_shipment_mr_url = "https://gitlab.example/project/-/merge_requests/42"
                pipeline.__dict__["_gitlab"] = MagicMock()
                if name == "release-from-fbc":
                    pipeline._load_layered_product_shipment_mr.return_value = pipeline._configured_shipment_mr_url
                mr = MagicMock(state="opened", title="Shipment for oadp 1.5.8", labels=[])
                module = f"pyartcd.pipelines.{name.replace('-', '_')}"
                with (
                    patch(
                        f"{module}.validate_shipment_mr_for_operation",
                        new_callable=AsyncMock,
                        return_value=(mr, MagicMock()),
                    ),
                    patch(f"{module}.reconcile_shipment_mr", new_callable=AsyncMock) as reconcile,
                ):
                    with self.assertRaisesRegex(ValueError, "oadp-operator"):
                        await pipeline.run()
                mr.save.assert_not_called()
                reconcile.assert_not_awaited()
                publish.assert_not_awaited()

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_numbered_fbc_shipments_validate_packages_before_publication(self, render):
        render.return_value = [{"schema": "olm.package", "name": "oadp-operator"}]
        for name in ("prepare-release-lp", "release-from-fbc"):
            with self.subTest(pipeline=name):
                render.reset_mock()
                self.client._get.reset_mock()
                pipeline, publish, _ = self.make_pipeline(name, fbc_only=True)
                await pipeline.run()
                self.assertEqual(list(publish.await_args.args[0]), ["fbc01"])
                render.assert_awaited_once()
                self.assertEqual(self.client._get.await_count, 4)

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_missing_fbc_package_blocks_shipment_mr(self, render):
        render.return_value = [{"schema": "olm.package", "name": "missing-operator"}]
        for name in ("prepare-release-lp", "release-from-fbc"):
            with self.subTest(pipeline=name):
                pipeline, publish, write = self.make_pipeline(name, fbc_only=True)
                with self.assertRaisesRegex(ValueError, "missing-operator"):
                    await pipeline.run()
                publish.assert_not_awaited()
                write.assert_not_awaited()
