import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import click
from elliottlib.shipment_model import ComponentSource, GitSource, Snapshot, SnapshotComponent, SnapshotSpec
from pyartcd.pipelines.binary_release_konflux import BinaryReleaseKonfluxPipeline


def _make_snapshot(app="oc-mirror-2-0"):
    return Snapshot(
        spec=SnapshotSpec(
            application=app,
            components=[
                SnapshotComponent(
                    name="oc-mirror-2-0-oc-mirror",
                    containerImage="quay.io/test/image@sha256:abc",
                    source=ComponentSource(git=GitSource(url="https://github.com/test/repo", revision="abc123")),
                )
            ],
        ),
        nvrs=["oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9"],
    )


class TestGroupNvrsByRhelVersion(unittest.TestCase):
    def test_groups_el8_and_el9(self):
        nvrs = [
            "foo-container-1.0-1.assembly.stream.el8",
            "foo-container-1.0-1.assembly.stream.el9",
        ]
        groups = BinaryReleaseKonfluxPipeline._group_nvrs_by_rhel_version(nvrs)
        self.assertEqual(sorted(groups.keys()), ["el8", "el9"])
        self.assertEqual(groups["el8"], ["foo-container-1.0-1.assembly.stream.el8"])
        self.assertEqual(groups["el9"], ["foo-container-1.0-1.assembly.stream.el9"])

    def test_single_rhel_version(self):
        nvrs = [
            "foo-container-1.0-1.assembly.stream.el9",
            "bar-container-2.0-1.assembly.stream.el9",
        ]
        groups = BinaryReleaseKonfluxPipeline._group_nvrs_by_rhel_version(nvrs)
        self.assertEqual(list(groups.keys()), ["el9"])
        self.assertEqual(len(groups["el9"]), 2)

    def test_no_rhel_suffix_falls_back_to_default(self):
        nvrs = ["foo-container-1.0-1.assembly.stream"]
        groups = BinaryReleaseKonfluxPipeline._group_nvrs_by_rhel_version(nvrs)
        self.assertEqual(list(groups.keys()), ["default"])

    def test_sorted_keys(self):
        nvrs = [
            "foo-container-1.0-1.el9",
            "bar-container-1.0-1.el8",
        ]
        groups = BinaryReleaseKonfluxPipeline._group_nvrs_by_rhel_version(nvrs)
        self.assertEqual(list(groups.keys()), ["el8", "el9"])


class TestBinaryReleaseKonfluxPipeline(unittest.TestCase):
    def _make_pipeline(self, nvrs=None, create_mr=False, dry_run=False):
        runtime = MagicMock()
        runtime.dry_run = dry_run
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = BinaryReleaseKonfluxPipeline(
            runtime=runtime,
            group="oc-mirror-2.0",
            assembly="stream",
            nvrs=nvrs or ["oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9"],
            create_mr=create_mr,
        )
        pipeline.product = "oc-mirror"
        return pipeline

    def test_nvrs_stored(self):
        pipeline = self._make_pipeline(nvrs=["foo-container-1.0-1.el9"])
        self.assertEqual(pipeline.nvrs, ["foo-container-1.0-1.el9"])

    def test_target_release_date_defaults_to_none(self):
        pipeline = self._make_pipeline()
        self.assertIsNone(pipeline.target_release_date)

    def test_create_shipment_config_raises_without_product(self):
        """create_shipment_config should raise if self.product was never set (run() not called)."""
        pipeline = self._make_pipeline()
        pipeline.product = None
        snapshot = _make_snapshot()

        with self.assertRaises(RuntimeError):
            pipeline.create_shipment_config(snapshot)

    def test_create_shipment_config_never_has_release_notes(self):
        """CDN RPAs carry their own static release notes, so shipment.data must always be None."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        config = pipeline.create_shipment_config(snapshot)

        self.assertIsNone(config.shipment.data)

    def test_create_shipment_config_metadata_fields(self):
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        config = pipeline.create_shipment_config(snapshot)

        self.assertEqual(config.shipment.metadata.product, "oc-mirror")
        self.assertEqual(config.shipment.metadata.application, "oc-mirror-2-0")
        self.assertEqual(config.shipment.metadata.group, "oc-mirror-2.0")
        self.assertEqual(config.shipment.metadata.assembly, "stream")
        self.assertFalse(config.shipment.metadata.fbc)

    def test_create_shipment_config_reads_release_plans_from_config_yaml(self):
        """Should read stage/prod releasePlan for the snapshot's application from config.yaml
        in the shipment data repo checkout."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "applications:\n"
                "  oc-mirror-2-0:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: oc-mirror-cdn-stage\n"
                "      prod:\n"
                "        releasePlan: oc-mirror-cdn-prod\n"
            )

            config = pipeline.create_shipment_config(snapshot)

        self.assertEqual(config.shipment.environments.stage.releasePlan, "oc-mirror-cdn-stage")
        self.assertEqual(config.shipment.environments.prod.releasePlan, "oc-mirror-cdn-prod")

    def test_create_shipment_config_missing_config_defaults_to_na(self):
        """When config.yaml doesn't exist (or has no entry for the app), fall back to 'n/a'."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)

            config = pipeline.create_shipment_config(snapshot)

        self.assertEqual(config.shipment.environments.stage.releasePlan, "n/a")
        self.assertEqual(config.shipment.environments.prod.releasePlan, "n/a")

    def test_create_shipment_config_raises_when_create_mr_and_no_release_plan(self):
        """When create_mr=True and releasePlan can't be resolved, raise ValueError."""
        pipeline = self._make_pipeline()
        pipeline.create_mr = True
        snapshot = _make_snapshot()

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)

            with self.assertRaises(ValueError):
                pipeline.create_shipment_config(snapshot)

    def test_create_shipment_config_accepts_skipped_stage(self):
        """stage releasePlan: Skipped is allowed for whitelisted products."""
        pipeline = self._make_pipeline()
        pipeline.create_mr = True
        pipeline.product = "openshift_agent_installer"
        snapshot = _make_snapshot(app="installer-ove-ui-4-22")

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "applications:\n"
                "  installer-ove-ui-4-22:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: Skipped\n"
                "      prod:\n"
                "        releasePlan: art-agent-installer-iso-release-4-22\n"
            )

            config = pipeline.create_shipment_config(snapshot)

        self.assertEqual(config.shipment.environments.stage.releasePlan, "Skipped")
        self.assertEqual(config.shipment.environments.prod.releasePlan, "art-agent-installer-iso-release-4-22")
        self.assertTrue(pipeline.stage_release_skipped)

    def test_create_shipment_config_rejects_skipped_stage_for_non_whitelisted_product(self):
        """Non-whitelisted products cannot use stage releasePlan: Skipped."""
        pipeline = self._make_pipeline()
        pipeline.create_mr = True
        # product remains "oc-mirror" from _make_pipeline — not on the whitelist
        snapshot = _make_snapshot()

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "applications:\n"
                "  oc-mirror-2-0:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: Skipped\n"
                "      prod:\n"
                "        releasePlan: oc-mirror-cdn-prod\n"
            )

            with self.assertRaises(ValueError) as ctx:
                pipeline.create_shipment_config(snapshot)

        self.assertIn("not allowed to use stage releasePlan", str(ctx.exception))
        self.assertFalse(pipeline.stage_release_skipped)

    def test_create_shipment_config_rejects_skipped_prod(self):
        """prod releasePlan: Skipped is forbidden."""
        pipeline = self._make_pipeline()
        pipeline.create_mr = True
        snapshot = _make_snapshot()

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "applications:\n"
                "  oc-mirror-2-0:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: oc-mirror-cdn-stage\n"
                "      prod:\n"
                "        releasePlan: skipped\n"
            )

            with self.assertRaises(ValueError) as ctx:
                pipeline.create_shipment_config(snapshot)

        self.assertIn("Skipping prod is forbidden", str(ctx.exception))
        self.assertFalse(pipeline.stage_release_skipped)

    def test_create_shipment_config_does_not_set_release_notes_in_rpa_for_non_ocp(self):
        pipeline = self._make_pipeline()
        config = pipeline.create_shipment_config(_make_snapshot())

        dumped = config.model_dump(exclude_unset=True, exclude_none=True)
        self.assertNotIn("release_notes_in_rpa", dumped["shipment"]["metadata"])

    def test_create_shipment_config_uses_snapshot_application(self):
        """metadata.application is derived directly from the snapshot's application."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot(app="some-other-app")

        config = pipeline.create_shipment_config(snapshot)

        self.assertEqual(config.shipment.metadata.application, "some-other-app")
        self.assertEqual(config.shipment.snapshot.spec.application, "some-other-app")

    def test_create_shipment_config_uses_rhel_suffixed_key(self):
        """When rhel_suffix is given, config.yaml lookup uses '{app}-{rhel_suffix}'."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(
                "applications:\n"
                "  oc-mirror-2-0-el9:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: oc-mirror-cdn-el9-stage\n"
                "      prod:\n"
                "        releasePlan: oc-mirror-cdn-el9-prod\n"
            )

            config = pipeline.create_shipment_config(snapshot, rhel_suffix="el9")

        self.assertEqual(config.shipment.environments.stage.releasePlan, "oc-mirror-cdn-el9-stage")
        self.assertEqual(config.shipment.environments.prod.releasePlan, "oc-mirror-cdn-el9-prod")

    def test_create_shipment_config_rhel_suffix_falls_back_to_plain_app_key(self):
        """When the RHEL-versioned key is absent, fall back to the plain application key."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            config_path = Path(tmpdir) / "config.yaml"
            # Only the plain key exists, no 'oc-mirror-2-0-el9'
            config_path.write_text(
                "applications:\n"
                "  oc-mirror-2-0:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: oc-mirror-cdn-stage\n"
                "      prod:\n"
                "        releasePlan: oc-mirror-cdn-prod\n"
            )

            config = pipeline.create_shipment_config(snapshot, rhel_suffix="el9")

        self.assertEqual(config.shipment.environments.stage.releasePlan, "oc-mirror-cdn-stage")
        self.assertEqual(config.shipment.environments.prod.releasePlan, "oc-mirror-cdn-prod")

    def test_write_shipment_file_default_kind_is_image(self):
        """Without a custom kind the filename contains 'image'."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()
        config = pipeline.create_shipment_config(snapshot)

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            pipeline.shipment_data_repo.write_file = AsyncMock()
            filepath = asyncio.run(pipeline._write_shipment_file("image", config, "prod", "20260804120000"))

        self.assertIn(".image.", filepath)

    def test_write_shipment_file_custom_kind_in_filename(self):
        """Custom advisory_kind appears in the filename."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()
        config = pipeline.create_shipment_config(snapshot)

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            pipeline.shipment_data_repo.write_file = AsyncMock()
            filepath = asyncio.run(pipeline._write_shipment_file("image-el8", config, "prod", "20260804120000"))

        self.assertIn(".image-el8.", filepath)


class TestOcpGroupShipment(unittest.TestCase):
    """openshift-* groups ship CDN binaries with an explicit releasePlan key and shipment kind."""

    def _runtime(self):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}
        return runtime

    def _make_pipeline(self, create_mr=False):
        pipeline = BinaryReleaseKonfluxPipeline(
            runtime=self._runtime(),
            group="openshift-4.22",
            assembly="stream",
            nvrs=["openshift-install-delivery-container-v4.22.0-202610191200.p2.g1234567.assembly.stream.el9"],
            create_mr=create_mr,
            release_plan_key="openshift-4-22-cdn",
            shipment_kind="cdn",
        )
        pipeline.product = "ocp"
        return pipeline

    def test_openshift_group_requires_release_plan_key_and_kind(self):
        for kwargs in (
            {},
            {"release_plan_key": "openshift-4-22-cdn"},
            {"shipment_kind": "cdn"},
            {"release_plan_key": "openshift-4-22", "shipment_kind": "image"},
            {"release_plan_key": "openshift-4-22", "shipment_kind": "cdn"},
            {"release_plan_key": "openshift-4-22-cdn", "shipment_kind": "cdn-dev-preview"},
        ):
            with self.assertRaises(ValueError):
                BinaryReleaseKonfluxPipeline(
                    runtime=self._runtime(),
                    group="openshift-4.22",
                    assembly="stream",
                    nvrs=["openshift-install-delivery-container-v4.22.0-1.el9"],
                    **kwargs,
                )

    def test_shipment_kind_allow_list(self):
        for kind in ("../x", "cdn/../../x", "rpm"):
            with self.assertRaises(ValueError):
                BinaryReleaseKonfluxPipeline(
                    runtime=self._runtime(),
                    group="oc-mirror-2.0",
                    assembly="stream",
                    nvrs=["oc-mirror-container-v2.0.0-1.el9"],
                    shipment_kind=kind,
                )

    def test_openshift_group_accepts_matching_cdn_key_and_kind(self):
        for key, kind in (("openshift-4-22-cdn", "cdn"), ("openshift-4-22-cdn-dev-preview", "cdn-dev-preview")):
            pipeline = BinaryReleaseKonfluxPipeline(
                runtime=self._runtime(),
                group="openshift-4.22",
                assembly="stream",
                nvrs=["openshift-install-delivery-container-v4.22.0-1.el9"],
                release_plan_key=key,
                shipment_kind=kind,
            )
            self.assertEqual(pipeline.shipment_kind, kind)

    @patch("pyartcd.pipelines.binary_release_konflux.exectools.cmd_gather_async")
    def test_openshift_group_product_falls_back_to_ocp(self, mock_cmd_gather_async):
        pipeline = self._make_pipeline()
        mock_cmd_gather_async.side_effect = Exception("No default specified and unable to find key: product")

        result = asyncio.run(pipeline._load_product_from_group_config())

        self.assertEqual(result, "ocp")

    def test_release_plan_key_overrides_application_lookup(self):
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot(app="openshift-4-22")

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            (Path(tmpdir) / "config.yaml").write_text(
                "applications:\n"
                "  openshift-4-22:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: ocp-art-advisory-stage-4-22\n"
                "      prod:\n"
                "        releasePlan: ocp-art-advisory-prod-4-22\n"
                "  openshift-4-22-cdn:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: ocp-art-cdn-stage-4-22\n"
                "      prod:\n"
                "        releasePlan: ocp-art-cdn-prod-4-22\n"
            )

            config = pipeline.create_shipment_config(snapshot)

        self.assertEqual(config.shipment.environments.stage.releasePlan, "ocp-art-cdn-stage-4-22")
        self.assertEqual(config.shipment.environments.prod.releasePlan, "ocp-art-cdn-prod-4-22")
        self.assertEqual(config.shipment.metadata.application, "openshift-4-22")
        self.assertEqual(config.shipment.metadata.product, "ocp")
        self.assertTrue(config.shipment.metadata.release_notes_in_rpa)
        self.assertIsNone(config.shipment.data)

    def test_release_plan_key_does_not_fall_back_to_application(self):
        pipeline = self._make_pipeline(create_mr=True)
        snapshot = _make_snapshot(app="openshift-4-22")

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            (Path(tmpdir) / "config.yaml").write_text(
                "applications:\n"
                "  openshift-4-22:\n"
                "    environments:\n"
                "      stage:\n"
                "        releasePlan: ocp-art-advisory-stage-4-22\n"
                "      prod:\n"
                "        releasePlan: ocp-art-advisory-prod-4-22\n"
            )

            with self.assertRaises(ValueError) as ctx:
                pipeline.create_shipment_config(snapshot)

        self.assertIn("openshift-4-22-cdn", str(ctx.exception))

    def test_run_uses_shipment_kind(self):
        pipeline = self._make_pipeline()
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="ocp")
        pipeline.create_snapshot = AsyncMock(return_value=_make_snapshot(app="openshift-4-22"))
        shipment_config = MagicMock()
        pipeline.create_shipment_config = MagicMock(return_value=shipment_config)
        pipeline.write_shipment_files_locally = AsyncMock()

        asyncio.run(pipeline.run())

        shipments_by_kind = pipeline.write_shipment_files_locally.await_args.args[0]
        self.assertEqual(shipments_by_kind, {"cdn": shipment_config})

    def test_write_shipment_file_uses_ocp_product_path_and_kind(self):
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot(app="openshift-4-22")
        config = pipeline.create_shipment_config(snapshot)

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.shipment_data_repo._directory = Path(tmpdir)
            pipeline.shipment_data_repo.write_file = AsyncMock()
            filepath = asyncio.run(pipeline._write_shipment_file("cdn", config, "prod", "20261019120000"))

        self.assertEqual(filepath, "shipment/ocp/openshift-4.22/openshift-4-22/prod/stream.cdn.20261019120000.yaml")

    def _rule(self, name):
        rule = MagicMock()
        rule.name = name
        return rule

    def test_keep_only_art_approval_rule_deletes_ert_and_docs(self):
        pipeline = self._make_pipeline()
        rules = [self._rule("ART"), self._rule("ERT"), self._rule("Docs")]
        mr = MagicMock()
        mr.approval_rules.list.return_value = rules
        mock_gitlab = MagicMock()
        mock_gitlab.get_mr_from_url.return_value = mr
        pipeline.__dict__["_gitlab"] = mock_gitlab

        pipeline._keep_only_art_approval_rule("https://gitlab.cee.redhat.com/a/b/-/merge_requests/1")

        rules[0].delete.assert_not_called()
        rules[1].delete.assert_called_once()
        rules[2].delete.assert_called_once()

    def test_keep_only_art_approval_rule_dry_run_touches_nothing(self):
        pipeline = self._make_pipeline()
        pipeline.dry_run = True
        mock_gitlab = MagicMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        pipeline._keep_only_art_approval_rule("https://gitlab.cee.redhat.com/a/b/-/merge_requests/1")

        mock_gitlab.get_mr_from_url.assert_not_called()

    def test_keep_only_art_approval_rule_swallows_gitlab_errors(self):
        pipeline = self._make_pipeline()
        mock_gitlab = MagicMock()
        mock_gitlab.get_mr_from_url.side_effect = Exception("boom")
        pipeline.__dict__["_gitlab"] = mock_gitlab

        pipeline._keep_only_art_approval_rule("https://gitlab.cee.redhat.com/a/b/-/merge_requests/1")


class TestLoadProductFromGroupConfig(unittest.TestCase):
    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        return BinaryReleaseKonfluxPipeline(
            runtime=runtime,
            group="oc-mirror-2.0",
            assembly="stream",
            nvrs=["oc-mirror-container-2.0-1-1.el9"],
        )

    @patch("pyartcd.pipelines.binary_release_konflux.exectools.cmd_gather_async")
    def test_loads_product_from_doozer(self, mock_cmd_gather_async):
        pipeline = self._make_pipeline()
        mock_cmd_gather_async.return_value = (0, "oc-mirror\n", "")

        result = asyncio.run(pipeline._load_product_from_group_config())

        self.assertEqual(result, "oc-mirror")

    @patch("pyartcd.pipelines.binary_release_konflux.exectools.cmd_gather_async")
    def test_falls_back_to_group_name_when_doozer_fails(self, mock_cmd_gather_async):
        pipeline = self._make_pipeline()
        mock_cmd_gather_async.side_effect = Exception("doozer boom")

        result = asyncio.run(pipeline._load_product_from_group_config())

        self.assertEqual(result, "oc-mirror")

    @patch("pyartcd.pipelines.binary_release_konflux.exectools.cmd_gather_async")
    def test_falls_back_to_group_name_when_doozer_returns_none(self, mock_cmd_gather_async):
        pipeline = self._make_pipeline()
        mock_cmd_gather_async.return_value = (0, "None\n", "")

        result = asyncio.run(pipeline._load_product_from_group_config())

        self.assertEqual(result, "oc-mirror")


class TestCreateSnapshot(unittest.TestCase):
    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        return BinaryReleaseKonfluxPipeline(
            runtime=runtime,
            group="oc-mirror-2.0",
            assembly="stream",
            nvrs=["oc-mirror-container-2.0-1-1.el9"],
        )

    def test_no_builds_returns_none(self):
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline.create_snapshot([]))
        self.assertIsNone(result)

    @patch("pyartcd.pipelines.binary_release_konflux.exectools.cmd_gather_async")
    def test_creates_snapshot_from_elliott_output(self, mock_cmd_gather_async):
        pipeline = self._make_pipeline()
        yaml_output = (
            "spec:\n"
            "  application: oc-mirror-2-0\n"
            "  components:\n"
            "  - name: oc-mirror-2-0-oc-mirror\n"
            "    containerImage: quay.io/test/image@sha256:abc\n"
            "    source:\n"
            "      git:\n"
            "        url: https://github.com/test/repo\n"
            "        revision: abc123\n"
        )
        mock_cmd_gather_async.return_value = (0, yaml_output, "")

        builds = ["oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9"]
        result = asyncio.run(pipeline.create_snapshot(builds))

        self.assertIsNotNone(result)
        self.assertEqual(result.spec.application, "oc-mirror-2-0")
        self.assertEqual(result.nvrs, builds)

        call_args = mock_cmd_gather_async.call_args[0][0]
        self.assertIn("snapshot", call_args)
        self.assertIn("new", call_args)


class TestEmbargoedNvrDefensiveCheck(unittest.TestCase):
    """run() must refuse to release if any provided NVR is embargoed (private-fix)."""

    def _make_pipeline(self, nvrs):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = BinaryReleaseKonfluxPipeline(
            runtime=runtime,
            group="oc-mirror-2.0",
            assembly="stream",
            nvrs=nvrs,
        )
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="oc-mirror")
        return pipeline

    def test_embargoed_nvr_raises(self):
        embargoed_nvr = "oc-mirror-container-2.0-202607291654.p3.g90b54b1.assembly.stream.el9"
        pipeline = self._make_pipeline([embargoed_nvr])

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(pipeline.run())
        self.assertIn("embargoed", str(ctx.exception))
        self.assertIn(embargoed_nvr, str(ctx.exception))

    def test_non_embargoed_nvr_does_not_raise_embargo_error(self):
        nvr = "oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9"
        pipeline = self._make_pipeline([nvr])
        pipeline.create_snapshot = AsyncMock(return_value=_make_snapshot())
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.write_shipment_files_locally = AsyncMock()

        asyncio.run(pipeline.run())

    def test_run_raises_if_no_snapshot_created(self):
        nvr = "oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9"
        pipeline = self._make_pipeline([nvr])
        pipeline.create_snapshot = AsyncMock(return_value=None)

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(pipeline.run())
        self.assertIn("No snapshot", str(ctx.exception))


class TestSetShipmentMrReady(unittest.TestCase):
    """Tests for BinaryReleaseKonfluxPipeline.set_shipment_mr_ready()."""

    def _make_pipeline(self, dry_run=False):
        runtime = MagicMock()
        runtime.dry_run = dry_run
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = BinaryReleaseKonfluxPipeline(
            runtime=runtime,
            group="oc-mirror-2.0",
            assembly="stream",
            nvrs=["oc-mirror-container-2.0-1-1.el9"],
            create_mr=True,
        )
        pipeline.product = "oc-mirror"
        pipeline.shipment_mr_url = "https://gitlab.cee.redhat.com/test/repo/-/merge_requests/42"
        return pipeline

    @patch("asyncio.sleep", new_callable=AsyncMock)
    def test_set_shipment_mr_ready_happy_path(self, mock_sleep):
        pipeline = self._make_pipeline(dry_run=False)

        mock_mr = MagicMock()
        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock(return_value=mock_mr)
        mock_gitlab.trigger_ci_pipeline = AsyncMock(return_value="https://gitlab.cee.redhat.com/pipeline/123")
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_awaited_once_with(pipeline.shipment_mr_url)
        mock_sleep.assert_awaited_once_with(30)
        mock_gitlab.trigger_ci_pipeline.assert_awaited_once_with(mock_mr)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    def test_set_shipment_mr_ready_dry_run(self, mock_sleep):
        """Dry-run must not call GitLab (placeholder MR URLs would 404)."""
        pipeline = self._make_pipeline(dry_run=True)
        pipeline.shipment_mr_url = "https://gitlab.cee.redhat.com/placeholder/placeholder/-/merge_requests/placeholder"

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock()
        mock_gitlab.trigger_ci_pipeline = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_not_awaited()
        mock_sleep.assert_not_awaited()
        mock_gitlab.trigger_ci_pipeline.assert_not_awaited()


class TestCliValidation(unittest.TestCase):
    """Test CLI argument validation via CliRunner against the real Click command."""

    def _invoke(self, extra_args):
        from click.testing import CliRunner
        from pyartcd.pipelines.binary_release_konflux import binary_release_konflux
        from pyartcd.runtime import Runtime

        asyncio.set_event_loop(asyncio.new_event_loop())

        mock_runtime = MagicMock(spec=Runtime)
        mock_runtime.dry_run = False
        mock_runtime.working_dir = MagicMock()
        mock_runtime.working_dir.absolute.return_value = MagicMock()
        mock_runtime.config = {}

        runner = CliRunner()
        base_args = ["--group", "oc-mirror-2.0", "--assembly", "stream"]
        return runner.invoke(binary_release_konflux, base_args + extra_args, obj=mock_runtime, standalone_mode=False)

    def test_empty_nvrs_raises_error(self):
        result = self._invoke(["--nvrs", "  ,  ,"])
        self.assertIsInstance(result.exception, click.ClickException)
        self.assertIn("at least one valid NVR", str(result.exception))

    @patch("pyartcd.pipelines.binary_release_konflux.BinaryReleaseKonfluxPipeline")
    def test_valid_nvrs_does_not_raise(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        result = self._invoke(["--nvrs", "oc-mirror-container-2.0-1-1.el9"])
        self.assertNotIsInstance(result.exception, click.ClickException)

    @patch("pyartcd.pipelines.binary_release_konflux.BinaryReleaseKonfluxPipeline")
    def test_release_plan_key_and_shipment_kind_passed_to_pipeline(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        self._invoke(
            [
                "--nvrs",
                "oc-mirror-container-2.0-1-1.el9",
                "--release-plan-key",
                "openshift-4-22-cdn",
                "--shipment-kind",
                "cdn",
            ]
        )
        kwargs = mock_pipeline_cls.call_args.kwargs
        self.assertEqual(kwargs["release_plan_key"], "openshift-4-22-cdn")
        self.assertEqual(kwargs["shipment_kind"], "cdn")

    @patch("pyartcd.pipelines.binary_release_konflux.BinaryReleaseKonfluxPipeline")
    def test_invalid_target_release_date_raises(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        result = self._invoke(["--nvrs", "oc-mirror-container-2.0-1-1.el9", "--target-release-date", "not-a-date"])
        self.assertIsInstance(result.exception, click.exceptions.BadParameter)


if __name__ == '__main__':
    unittest.main()
