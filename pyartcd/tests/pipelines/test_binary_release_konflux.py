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
        pipeline = self._make_pipeline(dry_run=True)

        mock_mr = MagicMock()
        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock(return_value=mock_mr)
        mock_gitlab.trigger_ci_pipeline = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_awaited_once_with(pipeline.shipment_mr_url)
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
    def test_invalid_target_release_date_raises(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        result = self._invoke(["--nvrs", "oc-mirror-container-2.0-1-1.el9", "--target-release-date", "not-a-date"])
        self.assertIsInstance(result.exception, click.exceptions.BadParameter)


if __name__ == '__main__':
    unittest.main()
