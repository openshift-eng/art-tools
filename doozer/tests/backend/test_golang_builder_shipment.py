import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from artcommonlib.model import Model
from doozerlib.backend.golang_builder_shipment import (
    GolangBuilderShipmentHandler,
    basic_auth_url,
    derive_golang_group,
    format_shipment_mr_title,
)
from doozerlib.constants import ART_IMAGES_BASE_APPLICATION

SAMPLE_GOLANG_NVR = "openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.el9"


class TestFormatShipmentMrTitle(unittest.TestCase):
    def test_title_matches_shipment_convention(self):
        self.assertEqual(
            format_shipment_mr_title("rhel-9-golang-1.25"),
            "Shipment for rhel-9-golang-1.25",
        )


class TestBasicAuthUrl(unittest.TestCase):
    def test_injects_token(self):
        url = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data.git"
        result = basic_auth_url(url, "mytoken")
        self.assertIn("oauth2:mytoken@", result)
        self.assertIn("gitlab.cee.redhat.com", result)
        self.assertTrue(result.startswith("https://"))


class TestBuildShipmentConfig(unittest.TestCase):
    def _make_handler(self, env="prod"):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(
            runtime=runtime,
            art_jira="ART-20930",
        )
        return handler

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    def test_build_shipment_config_prod(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (
            0,
            """
spec:
  application: art-images-base
  components:
    - name: golang-builder-v1.25-rhel9
      containerImage: quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:abc123
      source:
        git:
          url: https://github.com/openshift-priv/builder
          revision: abc123
""",
            "",
        )

        handler = self._make_handler()
        nvrs = ["golang-builder-container-v1.25-202606220000.el9"]
        snapshot = handler._build_inline_snapshot(
            nvr=nvrs[0],
            container_image="quay.io/test@sha256:abc123",
            rebase_repo_url="https://github.com/openshift-priv/builder",
            rebase_commitish="abc123",
        )
        config = handler._build_shipment_config(
            snapshot=snapshot,
            nvrs=nvrs,
            golang_group="rhel-9-golang-1.25",
        )

        self.assertEqual(config.shipment.metadata.product, "ocp")
        self.assertEqual(config.shipment.metadata.application, ART_IMAGES_BASE_APPLICATION)
        self.assertEqual(config.shipment.metadata.group, "golang")
        self.assertFalse(config.shipment.metadata.advisory_required)
        self.assertEqual(
            config.shipment.environments.stage.releasePlan,
            "ocp-art-golang-builder-ec",
        )
        self.assertEqual(
            config.shipment.environments.prod.releasePlan,
            "ocp-art-golang-builder-prod",
        )
        self.assertIn(
            "https://redhat.atlassian.net/browse/ART-20930",
            config.shipment.data.releaseNotes.references,
        )

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    def test_build_shipment_config_uses_split_release_plans(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (
            0,
            """
spec:
  application: art-images-base
  components:
    - name: golang-builder-v1.26-rhel9
      containerImage: quay.io/test@sha256:def456
      source:
        git:
          url: https://github.com/openshift-priv/builder
          revision: def456
""",
            "",
        )

        handler = self._make_handler()
        nvrs = ["golang-builder-container-v1.26-202606220000.el9"]
        snapshot = handler._build_inline_snapshot(
            nvr=nvrs[0],
            container_image="quay.io/test@sha256:def456",
            rebase_repo_url="https://github.com/openshift-priv/builder",
            rebase_commitish="def456",
        )
        config = handler._build_shipment_config(
            snapshot=snapshot,
            nvrs=nvrs,
            golang_group="rhel-9-golang-1.26",
        )

        self.assertEqual(
            config.shipment.environments.stage.releasePlan,
            "ocp-art-golang-builder-ec",
        )
        self.assertEqual(
            config.shipment.environments.prod.releasePlan,
            "ocp-art-golang-builder-prod",
        )


class TestCreateShipmentMR(IsolatedAsyncioTestCase):
    def _make_handler(self):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(
            runtime=runtime,
            art_jira="ART-20930",
        )
        handler.shipment_data_repo = AsyncMock()
        handler.shipment_data_repo._directory = Path("/tmp/test-working/shipment-data-push")
        handler.shipment_data_repo.commit_push = AsyncMock(return_value=True)
        return handler

    def _make_config_mock(self):
        config = Mock()
        config.shipment.metadata.application = "art-images-base"
        config.model_dump.return_value = {"shipment": {}}
        return config

    @patch("doozerlib.backend.golang_builder_shipment.python_gitlab")
    async def test_creates_mr_with_correct_title(self, mock_gitlab):
        handler = self._make_handler()
        handler._gitlab_token = "test-token"
        config = self._make_config_mock()

        mock_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.cee.redhat.com/test/-/merge_requests/1"
        mock_project.mergerequests.create.return_value = mock_mr
        mock_gitlab.Gitlab.return_value.projects.get.return_value = mock_project

        with patch("pathlib.Path.mkdir"):
            result = await handler._create_shipment_mr(
                config,
                golang_group="rhel-9-golang-1.25",
                env="prod",
                release_plan="ocp-art-golang-builder-prod",
                nvrs=["some-nvr"],
            )

        self.assertEqual(result, mock_mr.web_url)
        create_args = mock_project.mergerequests.create.call_args[0][0]
        self.assertEqual(create_args["title"], "Shipment for rhel-9-golang-1.25")
        self.assertEqual(create_args["target_branch"], "main")


class TestSetupReposNoToken(IsolatedAsyncioTestCase):
    @patch.dict("os.environ", {}, clear=True)
    async def test_missing_gitlab_token_raises(self):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(runtime=runtime)
        with self.assertRaises(ValueError):
            await handler._setup_repos()


class TestSnapshotWithQuayAuth(IsolatedAsyncioTestCase):
    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {"QUAY_AUTH_FILE": "/tmp/quay-auth.json"})
    async def test_pull_secret_flag_added(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (
            0,
            """
spec:
  application: art-images-base
  components:
    - name: golang-builder-v1.25-rhel9
      containerImage: quay.io/test@sha256:abc
      source:
        git:
          url: https://github.com/openshift-priv/builder
          revision: abc123
""",
            "",
        )
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(runtime=runtime)
        await handler._create_snapshot_via_elliott([SAMPLE_GOLANG_NVR])
        cmd_args = mock_cmd.call_args[0][0]
        self.assertTrue(any("--pull-secret=" in str(a) for a in cmd_args))


class TestApplicationOverride(IsolatedAsyncioTestCase):
    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_elliott_app_overridden_to_art_images_base(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (
            0,
            """
spec:
  application: rhel-9-golang-1-25
  components:
    - name: golang-builder-v1.25-rhel9
      containerImage: quay.io/test@sha256:abc
      source:
        git:
          url: https://github.com/openshift-priv/builder
          revision: abc123
""",
            "",
        )
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(runtime=runtime, art_jira="ART-20930")
        nvr = "openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.el9"
        snapshot = await handler._create_snapshot_via_elliott([nvr])

        self.assertEqual(snapshot.spec.application, ART_IMAGES_BASE_APPLICATION)
        self.assertEqual(snapshot.spec.components[0].name, "golang-builder-v1.25-rhel9")


class TestCreateSnapshotErrors(IsolatedAsyncioTestCase):
    def _make_handler(self):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        return GolangBuilderShipmentHandler(runtime=runtime)

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_elliott_nonzero_rc_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (1, "", "elliott error: NVR not found")
        handler = self._make_handler()
        with self.assertRaises(RuntimeError) as ctx:
            await handler._create_snapshot_via_elliott([SAMPLE_GOLANG_NVR])
        self.assertIn("elliott snapshot new failed", str(ctx.exception))

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_empty_stdout_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (0, "", "")
        handler = self._make_handler()
        with self.assertRaises(ValueError) as ctx:
            await handler._create_snapshot_via_elliott([SAMPLE_GOLANG_NVR])
        self.assertIn("invalid output", str(ctx.exception))

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_missing_spec_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (0, "apiVersion: v1\nkind: Snapshot\n", "")
        handler = self._make_handler()
        with self.assertRaises(ValueError) as ctx:
            await handler._create_snapshot_via_elliott([SAMPLE_GOLANG_NVR])
        self.assertIn("missing 'spec'", str(ctx.exception))


class TestCommitPushFailure(IsolatedAsyncioTestCase):
    async def test_push_failure_raises(self):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(
            runtime=runtime,
            art_jira="ART-20930",
        )
        handler.shipment_data_repo = AsyncMock()
        handler.shipment_data_repo._directory = Path("/tmp/test-working/shipment-data-push")
        handler.shipment_data_repo.commit_push = AsyncMock(return_value=False)

        config = Mock()
        config.shipment.metadata.application = "art-images-base"
        config.model_dump.return_value = {"shipment": {}}

        with patch("pathlib.Path.mkdir"):
            with self.assertRaises(RuntimeError) as ctx:
                await handler._create_shipment_mr(
                    config,
                    golang_group="rhel-9-golang-1.25",
                    env="prod",
                    release_plan="ocp-art-golang-builder-prod",
                    nvrs=["some-nvr"],
                )
        self.assertIn("Failed to push", str(ctx.exception))


class TestCreateShipmentFromNvrs(IsolatedAsyncioTestCase):
    @patch("doozerlib.backend.golang_builder_shipment.derive_golang_group", return_value="rhel-9-golang-1.25")
    async def test_create_shipment_from_nvrs_wires_steps(self, mock_derive):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(runtime=runtime)
        handler._setup_repos = AsyncMock()
        handler._create_snapshot_via_elliott = AsyncMock(return_value=Mock(spec=["spec"]))
        handler._create_snapshot_via_elliott.return_value.spec.application = ART_IMAGES_BASE_APPLICATION
        handler._build_shipment_config = Mock(return_value=Mock())
        handler._create_shipment_mr = AsyncMock(return_value="https://gitlab.example.com/-/merge_requests/1")

        result = await handler.create_shipment_from_nvrs(
            ["openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.el9"],
        )

        self.assertEqual(result, "https://gitlab.example.com/-/merge_requests/1")
        handler._setup_repos.assert_called_once()
        handler._create_snapshot_via_elliott.assert_called_once()
        handler._create_shipment_mr.assert_called_once()


class TestCreateShipmentInline(IsolatedAsyncioTestCase):
    async def test_create_shipment_returns_none_on_failure(self):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(runtime=runtime)
        handler._setup_repos = AsyncMock(side_effect=RuntimeError("boom"))

        result = await handler.create_shipment(
            nvr="openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9",
            container_image="quay.io/test@sha256:abc",
            rebase_repo_url="https://example.com/repo.git",
            rebase_commitish="abc123",
        )
        self.assertIsNone(result)


class TestBuildInlineSnapshot(unittest.TestCase):
    def test_inline_snapshot_uses_component_name_and_application(self):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(runtime=runtime)
        nvr = "openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9"
        snapshot = handler._build_inline_snapshot(
            nvr=nvr,
            container_image="quay.io/test@sha256:abc",
            rebase_repo_url="https://example.com/repo.git",
            rebase_commitish="abc123",
        )
        self.assertEqual(snapshot.spec.application, ART_IMAGES_BASE_APPLICATION)
        self.assertEqual(snapshot.nvrs, [nvr])
        self.assertEqual(snapshot.spec.components[0].containerImage, "quay.io/test@sha256:abc")
        self.assertEqual(snapshot.spec.components[0].name, "golang-builder-v1.25-rhel9")


class TestApplyRpaComponentNames(unittest.TestCase):
    def test_renames_konflux_cr_name_from_elliott(self):
        from elliottlib.shipment_model import ComponentSource, GitSource, Snapshot, SnapshotComponent, SnapshotSpec

        nvr = "openshift-golang-builder-container-v1.25.11-202607210212.p2.g6245b3b.el9"
        snapshot = Snapshot(
            spec=SnapshotSpec(
                application=ART_IMAGES_BASE_APPLICATION,
                components=[
                    SnapshotComponent(
                        name="rhel-9-golang-1-25-openshift-golang-builder",
                        containerImage="quay.io/test@sha256:abc",
                        source=ComponentSource(
                            git=GitSource(
                                url="https://github.com/openshift-eng/ocp-build-data",
                                revision="abc123",
                            ),
                        ),
                    )
                ],
            ),
            nvrs=[nvr],
        )
        GolangBuilderShipmentHandler._apply_rpa_component_names(snapshot, [nvr])
        self.assertEqual(snapshot.spec.components[0].name, "golang-builder-v1.25-rhel9")


class TestDeriveGolangGroup(unittest.TestCase):
    def test_from_rpm_nvr(self):
        self.assertEqual(derive_golang_group(["golang-1.25.9-1.el9"]), "rhel-9-golang-1.25")

    def test_from_konflux_image_nvr(self):
        nvr = "openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9"
        self.assertEqual(derive_golang_group([nvr]), "rhel-9-golang-1.25")

    def test_unknown_nvr_raises(self):
        with self.assertRaises(ValueError):
            derive_golang_group(["not-a-golang-nvr-1.0-1.noarch"])


class TestShipmentFilePath(IsolatedAsyncioTestCase):
    """Verify that _create_shipment_mr writes the YAML under the expected path.

    The real production path is:
        shipment/<product>/golang/<application>/<env>/<filename>

    Note: the directory segment is the literal string ``"golang"``, not the
    version-specific golang_group (e.g. ``"rhel-9-golang-1.25"``).
    """

    async def test_write_file_uses_golang_literal_segment(self):
        runtime = Mock()
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.working_dir = "/tmp"
        handler = GolangBuilderShipmentHandler(
            runtime=runtime,
            art_jira="ART-20930",
        )

        recorded_paths = []

        async def capture_write(path, content):
            recorded_paths.append(Path(path))

        handler.shipment_data_repo = AsyncMock()
        handler.shipment_data_repo._directory = Path("/tmp/fake-repo")
        handler.shipment_data_repo.create_branch = AsyncMock()
        handler.shipment_data_repo.write_file = capture_write
        handler.shipment_data_repo.add_all = AsyncMock()
        handler.shipment_data_repo.log_diff = AsyncMock()
        handler.shipment_data_repo.commit_push = AsyncMock(return_value=True)
        handler._gitlab_token = "fake-token"

        from unittest.mock import patch as _patch

        mock_mr = Mock()
        mock_mr.web_url = "https://gitlab.example.com/-/merge_requests/42"
        mock_mr.iid = 42

        mock_project = Mock()
        mock_project.mergerequests = Mock()
        mock_project.mergerequests.create = Mock(return_value=mock_mr)
        mock_mr_obj = Mock()
        mock_mr_obj.approval_rules = Mock()
        mock_mr_obj.approval_rules.list = Mock(return_value=[])
        mock_project.mergerequests.get = Mock(return_value=mock_mr_obj)

        config = Mock()
        config.shipment = Mock()
        config.shipment.metadata = Mock()
        config.shipment.metadata.application = "art-images-base"
        config.model_dump.return_value = {"shipment": {}}

        with _patch("doozerlib.backend.golang_builder_shipment.python_gitlab") as mock_gl_mod:
            mock_gl_mod.Gitlab.return_value.projects.get.return_value = mock_project
            with _patch("pathlib.Path.mkdir"):
                await handler._create_shipment_mr(
                    config,
                    golang_group="rhel-9-golang-1.25",
                    env="prod",
                    release_plan="ocp-art-golang-builder-prod",
                    nvrs=["some-nvr"],
                )

        self.assertEqual(len(recorded_paths), 1)
        path = recorded_paths[0]
        parts = path.parts
        # Expected: shipment/ocp/golang/art-images-base/prod/<filename>
        self.assertEqual(parts[0], "shipment")
        self.assertEqual(parts[1], "ocp")
        self.assertEqual(parts[2], "golang")  # literal "golang", not golang_group
        self.assertEqual(parts[3], "art-images-base")
        self.assertEqual(parts[4], "prod")
        self.assertTrue(parts[5].endswith(".yaml"), f"Expected .yaml filename, got {parts[5]}")


if __name__ == "__main__":
    unittest.main()
