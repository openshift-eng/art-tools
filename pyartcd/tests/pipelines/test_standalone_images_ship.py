import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import click
from pyartcd.pipelines.standalone_images_ship import SILENT_BY_DEFAULT_COMPONENTS, StandaloneImagesShipPipeline


def _make_pipeline(
    nvrs=None,
    jiras=None,
    create_mr=False,
    dry_run=False,
    silent=None,
):
    runtime = MagicMock()
    runtime.dry_run = dry_run
    runtime.working_dir = MagicMock()
    runtime.working_dir.absolute.return_value = MagicMock()
    runtime.config = {}

    pipeline = StandaloneImagesShipPipeline(
        runtime=runtime,
        group="openshift-5.2",
        assembly="stream",
        nvrs=nvrs or ["operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9"],
        stage_release_plan="ocp-art-stage-silent-5.2",
        prod_release_plan="ocp-art-prod-silent-5.2",
        jiras=jiras,
        create_mr=create_mr,
        silent=silent,
    )
    return pipeline


class TestParseJiraKey(unittest.TestCase):
    def test_bare_key(self):
        self.assertEqual(StandaloneImagesShipPipeline._parse_jira_key("ART-25199"), "ART-25199")

    def test_browse_url(self):
        self.assertEqual(
            StandaloneImagesShipPipeline._parse_jira_key("https://redhat.atlassian.net/browse/ART-25199"),
            "ART-25199",
        )

    def test_invalid_key_rejected(self):
        self.assertEqual(StandaloneImagesShipPipeline._parse_jira_key("not-a-jira-key"), "")

    def test_wrong_host_rejected(self):
        self.assertEqual(
            StandaloneImagesShipPipeline._parse_jira_key("https://example.com/browse/ART-25199"),
            "",
        )

    def test_constructor_rejects_invalid_jira(self):
        with self.assertRaises(ValueError) as ctx:
            _make_pipeline(jiras=["not-a-jira-key"])
        self.assertIn("Invalid Jira", str(ctx.exception))

    def test_constructor_accepts_valid_jiras(self):
        pipeline = _make_pipeline(jiras=["ART-25199", "ART-26635"])
        self.assertEqual(pipeline.jiras, ["ART-25199", "ART-26635"])


class TestResolveSilent(unittest.TestCase):
    def test_operator_registry_defaults_silent(self):
        pipeline = _make_pipeline(
            nvrs=["operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9"]
        )
        self.assertTrue(pipeline.silent)

    def test_other_component_defaults_public(self):
        pipeline = _make_pipeline(nvrs=["some-other-image-container-v1.0.0-1.el9"])
        self.assertFalse(pipeline.silent)

    def test_explicit_silent_override_wins(self):
        pipeline = _make_pipeline(nvrs=["some-other-image-container-v1.0.0-1.el9"], silent=True)
        self.assertTrue(pipeline.silent)

    def test_explicit_public_override_wins(self):
        pipeline = _make_pipeline(
            nvrs=["operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9"],
            silent=False,
        )
        self.assertFalse(pipeline.silent)

    def test_mixed_components_without_override_raises(self):
        with self.assertRaises(ValueError) as ctx:
            _make_pipeline(
                nvrs=[
                    "operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9",
                    "some-other-image-container-v1.0.0-1.el9",
                ]
            )
        self.assertIn("different default silent/public classification", str(ctx.exception))

    def test_silent_by_default_components_contains_operator_registry(self):
        # Guard against accidentally renaming/removing the one entry this feature
        # was built for, without a test noticing.
        self.assertIn("operator-registry-container", SILENT_BY_DEFAULT_COMPONENTS)


class TestCreateShipmentConfig(unittest.TestCase):
    def _snapshot(self, application="openshift-5-2"):
        from elliottlib.shipment_model import ComponentSource, GitSource, Snapshot, SnapshotComponent, SnapshotSpec

        return Snapshot(
            spec=SnapshotSpec(
                application=application,
                components=[
                    SnapshotComponent(
                        name="ose-5-2-operator-registry",
                        containerImage="quay.io/test/image@sha256:abc",
                        source=ComponentSource(git=GitSource(url="https://github.com/test/repo", revision="abc123")),
                    )
                ],
            ),
            nvrs=["operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9"],
        )

    def test_raises_without_product(self):
        pipeline = _make_pipeline()
        with self.assertRaises(RuntimeError):
            pipeline.create_shipment_config(self._snapshot())

    def test_metadata_fields(self):
        pipeline = _make_pipeline()
        pipeline.product = "ocp"
        config = pipeline.create_shipment_config(self._snapshot())

        metadata = config.shipment.metadata
        self.assertEqual(metadata.product, "ocp")
        self.assertEqual(metadata.application, "openshift-5-2")
        self.assertEqual(metadata.group, "openshift-5.2")
        self.assertEqual(metadata.assembly, "stream")
        self.assertFalse(metadata.fbc)
        self.assertFalse(metadata.advisory_required)
        self.assertFalse(metadata.docs_approval_required)
        self.assertTrue(metadata.release_notes_in_rpa)

    def test_environments_use_given_release_plans(self):
        pipeline = _make_pipeline()
        pipeline.product = "ocp"
        config = pipeline.create_shipment_config(self._snapshot())

        self.assertEqual(config.shipment.environments.stage.releasePlan, "ocp-art-stage-silent-5.2")
        self.assertEqual(config.shipment.environments.prod.releasePlan, "ocp-art-prod-silent-5.2")

    def test_application_derived_from_snapshot(self):
        pipeline = _make_pipeline()
        pipeline.product = "ocp"
        config = pipeline.create_shipment_config(self._snapshot(application="some-other-app"))

        self.assertEqual(config.shipment.metadata.application, "some-other-app")

    def test_no_release_notes_for_silent_shipment(self):
        pipeline = _make_pipeline()
        pipeline.product = "ocp"
        config = pipeline.create_shipment_config(self._snapshot())

        self.assertIsNone(config.shipment.data)

    def test_public_shipment_requires_advisory_and_docs_approval(self):
        pipeline = _make_pipeline(nvrs=["some-other-image-container-v1.0.0-1.el9"])
        pipeline.product = "ocp"
        config = pipeline.create_shipment_config(self._snapshot())

        metadata = config.shipment.metadata
        self.assertTrue(metadata.advisory_required)
        self.assertTrue(metadata.docs_approval_required)
        # openshift-* release notes still come from the RPA rather than being typed
        # into the shipment file, silent or public (matches binary_release_konflux.py).
        self.assertTrue(metadata.release_notes_in_rpa)
        self.assertIsNone(config.shipment.data)


class TestCreateSnapshot(unittest.TestCase):
    @patch("pyartcd.pipelines.standalone_images_ship.exectools.cmd_gather_async")
    def test_single_application_succeeds(self, mock_cmd_gather_async):
        pipeline = _make_pipeline()
        yaml_output = (
            "spec:\n"
            "  application: openshift-5-2\n"
            "  components:\n"
            "  - name: ose-5-2-operator-registry\n"
            "    containerImage: quay.io/test/image@sha256:abc\n"
            "    source:\n"
            "      git:\n"
            "        url: https://github.com/test/repo\n"
            "        revision: abc123\n"
        )
        mock_cmd_gather_async.return_value = (0, yaml_output, "")

        nvrs = ["operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9"]
        result = asyncio.run(pipeline.create_snapshot(nvrs))

        self.assertEqual(result.spec.application, "openshift-5-2")
        self.assertEqual(result.nvrs, nvrs)

        call_args = mock_cmd_gather_async.call_args[0][0]
        self.assertIn("snapshot", call_args)
        self.assertIn("new", call_args)

    @patch("pyartcd.pipelines.standalone_images_ship.exectools.cmd_gather_async")
    def test_multiple_applications_raises(self, mock_cmd_gather_async):
        pipeline = _make_pipeline()
        yaml_output = (
            "spec:\n  application: app-one\n  components: []\n---\nspec:\n  application: app-two\n  components: []\n"
        )
        mock_cmd_gather_async.return_value = (0, yaml_output, "")

        with self.assertRaises(ValueError) as ctx:
            asyncio.run(pipeline.create_snapshot(["nvr-one", "nvr-two"]))
        self.assertIn("single application", str(ctx.exception))


class TestEmbargoedNvrDefensiveCheck(unittest.TestCase):
    """run() must refuse to release if any provided NVR is embargoed (private-fix)."""

    def _make_pipeline(self, nvrs):
        pipeline = _make_pipeline_for_run(nvrs)
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="ocp")
        return pipeline

    def test_embargoed_nvr_raises(self):
        embargoed_nvr = "operator-registry-container-v5.2.0-202611252026.p3.gffec6b4.assembly.stream.el9"
        pipeline = self._make_pipeline([embargoed_nvr])

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(pipeline.run())
        self.assertIn("embargoed", str(ctx.exception))
        self.assertIn(embargoed_nvr, str(ctx.exception))

    def test_non_embargoed_nvr_does_not_raise_embargo_error(self):
        nvr = "operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9"
        pipeline = self._make_pipeline([nvr])
        pipeline.create_snapshot = AsyncMock(return_value=MagicMock())
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.write_shipment_file_locally = AsyncMock()

        asyncio.run(pipeline.run())


def _make_pipeline_for_run(nvrs):
    runtime = MagicMock()
    runtime.dry_run = False
    runtime.working_dir = MagicMock()
    runtime.working_dir.absolute.return_value = MagicMock()
    runtime.config = {}

    return StandaloneImagesShipPipeline(
        runtime=runtime,
        group="openshift-5.2",
        assembly="stream",
        nvrs=nvrs,
        stage_release_plan="ocp-art-stage-silent-5.2",
        prod_release_plan="ocp-art-prod-silent-5.2",
        create_mr=False,
    )


class TestMrWordingReflectsSilent(unittest.TestCase):
    """The MR title/description/commit message must say "silent" or "public"
    depending on self.silent, not always say "silent" regardless (ART-26635 review)."""

    def test_mr_description_silent(self):
        pipeline = _make_pipeline()  # defaults to silent (operator-registry)
        pipeline.product = "ocp"
        self.assertIn("Silent image shipment", pipeline._mr_description())

    def test_mr_description_public(self):
        pipeline = _make_pipeline(nvrs=["some-other-image-container-v1.0.0-1.el9"])
        pipeline.product = "ocp"
        self.assertIn("Public image shipment", pipeline._mr_description())
        self.assertNotIn("Silent", pipeline._mr_description())

    def _make_pipeline_for_mr(self, silent):
        nvrs = (
            ["operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9"]
            if silent
            else ["some-other-image-container-v1.0.0-1.el9"]
        )
        pipeline = _make_pipeline(nvrs=nvrs, create_mr=True)
        pipeline.product = "ocp"
        pipeline.shipment_data_repo = MagicMock()
        pipeline.shipment_data_repo.create_branch = AsyncMock()
        pipeline.shipment_data_repo.add_all = AsyncMock()
        pipeline.shipment_data_repo.log_diff = AsyncMock()
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)
        pipeline._write_shipment_file = AsyncMock(return_value="shipment/ocp/openshift-5.2/openshift-5-2/x.yml")
        pipeline._get_gitlab_project = MagicMock(return_value=MagicMock(id=1))
        return pipeline

    def test_commit_message_and_mr_title_silent(self):
        pipeline = self._make_pipeline_for_mr(silent=True)
        asyncio.run(pipeline.create_shipment_mr(MagicMock()))

        commit_message = pipeline.shipment_data_repo.commit_push.call_args[0][0]
        self.assertIn("silent", commit_message)
        self.assertNotIn("public", commit_message)

    def test_commit_message_and_mr_title_public(self):
        pipeline = self._make_pipeline_for_mr(silent=False)
        asyncio.run(pipeline.create_shipment_mr(MagicMock()))

        commit_message = pipeline.shipment_data_repo.commit_push.call_args[0][0]
        self.assertIn("public", commit_message)
        self.assertNotIn("silent", commit_message)

        mr_create_kwargs = pipeline._get_gitlab_project.return_value.mergerequests.create.call_args[0][0]
        self.assertIn("Public shipment", mr_create_kwargs["title"])


class TestSetShipmentMrReady(unittest.TestCase):
    def _make_pipeline(self, dry_run=False):
        pipeline = _make_pipeline(create_mr=True, dry_run=dry_run)
        pipeline.product = "ocp"
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

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock()
        mock_gitlab.trigger_ci_pipeline = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_not_awaited()
        mock_sleep.assert_not_awaited()
        mock_gitlab.trigger_ci_pipeline.assert_not_awaited()


class TestCliValidation(unittest.TestCase):
    """Test CLI argument wiring via CliRunner against the real Click command."""

    def _invoke(self, extra_args):
        from click.testing import CliRunner
        from pyartcd.pipelines.standalone_images_ship import standalone_images_ship
        from pyartcd.runtime import Runtime

        asyncio.set_event_loop(asyncio.new_event_loop())

        mock_runtime = MagicMock(spec=Runtime)
        mock_runtime.dry_run = False
        mock_runtime.working_dir = MagicMock()
        mock_runtime.working_dir.absolute.return_value = MagicMock()
        mock_runtime.config = {}

        runner = CliRunner()
        base_args = [
            "--group",
            "openshift-5.2",
            "--assembly",
            "stream",
            "--stage-release-plan",
            "ocp-art-stage-silent-5.2",
            "--prod-release-plan",
            "ocp-art-prod-silent-5.2",
        ]
        return runner.invoke(standalone_images_ship, base_args + extra_args, obj=mock_runtime, standalone_mode=False)

    def test_missing_nvr_raises_error(self):
        result = self._invoke([])
        self.assertIsInstance(result.exception, (click.UsageError, SystemExit))

    @patch("pyartcd.pipelines.standalone_images_ship.StandaloneImagesShipPipeline")
    def test_valid_single_nvr_does_not_raise(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        result = self._invoke(["--nvr", "operator-registry-container-v5.2.0-1.el9"])
        self.assertIsNone(result.exception)

    @patch("pyartcd.pipelines.standalone_images_ship.StandaloneImagesShipPipeline")
    def test_multiple_nvrs_and_jiras_passed_to_pipeline(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        self._invoke(
            [
                "--nvr",
                "nvr-one",
                "--nvr",
                "nvr-two",
                "--jira",
                "ART-25199",
                "--jira",
                "ART-26635",
            ]
        )
        kwargs = mock_pipeline_cls.call_args.kwargs
        self.assertEqual(kwargs["nvrs"], ["nvr-one", "nvr-two"])
        self.assertEqual(kwargs["jiras"], ["ART-25199", "ART-26635"])

    @patch("pyartcd.pipelines.standalone_images_ship.StandaloneImagesShipPipeline")
    def test_create_mr_flag_passed_to_pipeline(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        self._invoke(["--nvr", "nvr-one", "--create-mr"])
        kwargs = mock_pipeline_cls.call_args.kwargs
        self.assertTrue(kwargs["create_mr"])

    @patch("pyartcd.pipelines.standalone_images_ship.StandaloneImagesShipPipeline")
    def test_silent_flag_passed_to_pipeline(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        self._invoke(["--nvr", "nvr-one", "--silent"])
        kwargs = mock_pipeline_cls.call_args.kwargs
        self.assertIs(kwargs["silent"], True)

    @patch("pyartcd.pipelines.standalone_images_ship.StandaloneImagesShipPipeline")
    def test_public_flag_passed_to_pipeline(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        self._invoke(["--nvr", "nvr-one", "--public"])
        kwargs = mock_pipeline_cls.call_args.kwargs
        self.assertIs(kwargs["silent"], False)

    @patch("pyartcd.pipelines.standalone_images_ship.StandaloneImagesShipPipeline")
    def test_silent_defaults_to_none_when_unset(self, mock_pipeline_cls):
        mock_pipeline_cls.return_value.run = AsyncMock()
        self._invoke(["--nvr", "nvr-one"])
        kwargs = mock_pipeline_cls.call_args.kwargs
        self.assertIsNone(kwargs["silent"])


if __name__ == '__main__':
    unittest.main()
