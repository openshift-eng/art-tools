import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from artcommonlib.model import Model
from doozerlib.constants import ART_IMAGES_BASE_APPLICATION
from doozerlib.backend.golang_builder_shipment import (
    GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP,
    GolangBuilderShipmentHandler,
    derive_golang_group,
    resolve_env_from_runtime,
)


class TestResolveReleasePlan(unittest.TestCase):
    def test_prod_returns_prod_plan(self):
        plan = GolangBuilderShipmentHandler.resolve_release_plan("prod")
        self.assertEqual(plan, "ocp-art-golang-builder-prod-rhel9")

    def test_ec_returns_ec_plan(self):
        plan = GolangBuilderShipmentHandler.resolve_release_plan("ec")
        self.assertEqual(plan, "ocp-art-golang-builder-ec-rhel9")

    def test_unknown_env_raises(self):
        with self.assertRaises(ValueError):
            GolangBuilderShipmentHandler.resolve_release_plan("staging")

    def test_map_keys_are_complete(self):
        self.assertIn("prod", GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP)
        self.assertIn("ec", GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP)


class TestResolveEnvFromRuntime(unittest.TestCase):
    def test_pre_release_returns_ec(self):
        runtime = Mock()
        runtime.group_config = Model({"software_lifecycle": {"phase": "pre-release"}})
        self.assertEqual(resolve_env_from_runtime(runtime), "ec")

    def test_release_returns_prod(self):
        runtime = Mock()
        runtime.group_config = Model({"software_lifecycle": {"phase": "release"}})
        self.assertEqual(resolve_env_from_runtime(runtime), "prod")

    def test_missing_lifecycle_returns_prod(self):
        runtime = Mock()
        runtime.group_config = Model({})
        self.assertEqual(resolve_env_from_runtime(runtime), "prod")


class TestBasicAuthUrl(unittest.TestCase):
    def test_injects_token(self):
        url = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data.git"
        result = GolangBuilderShipmentHandler.basic_auth_url(url, "mytoken")
        self.assertIn("oauth2:mytoken@", result)
        self.assertIn("gitlab.cee.redhat.com", result)
        self.assertTrue(result.startswith("https://"))


class TestBuildShipmentConfig(unittest.TestCase):
    def _make_handler(self, env="prod"):
        runtime = Mock()
        runtime.dry_run = False
        runtime.product = "ocp"
        runtime.group = "openshift-4.22"
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        handler = GolangBuilderShipmentHandler(
            runtime=runtime,
            ocp_version="4.22",
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
            env="prod",
            release_plan="ocp-art-golang-builder-prod-rhel9",
            ocp_version="4.22",
        )

        self.assertEqual(config.shipment.metadata.product, "ocp")
        self.assertEqual(config.shipment.metadata.application, ART_IMAGES_BASE_APPLICATION)
        self.assertEqual(config.shipment.metadata.group, "rhel-9-golang-1.25")
        self.assertEqual(
            config.shipment.environments.prod.releasePlan,
            "ocp-art-golang-builder-prod-rhel9",
        )
        self.assertIn(
            "https://redhat.atlassian.net/browse/ART-20930",
            config.shipment.data.releaseNotes.references,
        )

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    def test_build_shipment_config_ec(self, mock_unlink, mock_cmd):
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
            env="ec",
            release_plan="ocp-art-golang-builder-ec-rhel9",
            ocp_version="4.22",
        )

        self.assertEqual(
            config.shipment.environments.prod.releasePlan,
            "ocp-art-golang-builder-ec-rhel9",
        )


class TestCreateShipmentMR(IsolatedAsyncioTestCase):
    def _make_handler(self, dry_run=False):
        runtime = Mock()
        runtime.dry_run = dry_run
        runtime.product = "ocp"
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        handler = GolangBuilderShipmentHandler(
            runtime=runtime,
            dry_run=dry_run,
            ocp_version="4.22",
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
    async def test_dry_run_returns_placeholder(self, mock_gitlab):
        handler = self._make_handler(dry_run=True)
        config = self._make_config_mock()

        mock_project = MagicMock()
        mock_gitlab.Gitlab.return_value.projects.get.return_value = mock_project

        with patch("pathlib.Path.mkdir"):
            result = await handler._create_shipment_mr(
                config,
                golang_group="rhel-9-golang-1.25",
                env="prod",
                release_plan="ocp-art-golang-builder-prod-rhel9",
                nvrs=["some-nvr"],
                ocp_version="4.22",
            )

        self.assertIn("placeholder", result)
        mock_project.mergerequests.create.assert_not_called()

    @patch("doozerlib.backend.golang_builder_shipment.python_gitlab")
    async def test_creates_mr_with_correct_title(self, mock_gitlab):
        handler = self._make_handler(dry_run=False)
        config = self._make_config_mock()

        mock_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.cee.redhat.com/test/-/merge_requests/1"
        mock_project.mergerequests.create.return_value = mock_mr
        mock_gitlab.Gitlab.return_value.projects.get.return_value = mock_project

        with patch("pathlib.Path.mkdir"):
            with patch.dict("os.environ", {"GITLAB_TOKEN": "test-token"}):
                result = await handler._create_shipment_mr(
                    config,
                    golang_group="rhel-9-golang-1.25",
                    env="prod",
                    release_plan="ocp-art-golang-builder-prod-rhel9",
                    nvrs=["some-nvr"],
                    ocp_version="4.22",
                )

        self.assertEqual(result, mock_mr.web_url)
        create_args = mock_project.mergerequests.create.call_args[0][0]
        self.assertIn("Golang builder shipment", create_args["title"])
        self.assertEqual(create_args["target_branch"], "main")


class TestSetupReposNoToken(IsolatedAsyncioTestCase):
    @patch.dict("os.environ", {}, clear=True)
    async def test_missing_gitlab_token_raises(self):
        runtime = Mock()
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
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
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        handler = GolangBuilderShipmentHandler(runtime=runtime)
        await handler._create_snapshot_via_elliott(["some-nvr"], "rhel-9-golang-1.25")
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
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        handler = GolangBuilderShipmentHandler(runtime=runtime, art_jira="ART-20930", ocp_version="4.22")
        snapshot = await handler._create_snapshot_via_elliott(["some-nvr"], "rhel-9-golang-1.25")

        self.assertEqual(snapshot.spec.application, ART_IMAGES_BASE_APPLICATION)


class TestCreateSnapshotErrors(IsolatedAsyncioTestCase):
    def _make_handler(self):
        runtime = Mock()
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        return GolangBuilderShipmentHandler(runtime=runtime)

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_elliott_nonzero_rc_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (1, "", "elliott error: NVR not found")
        handler = self._make_handler()
        with self.assertRaises(RuntimeError) as ctx:
            await handler._create_snapshot_via_elliott(["some-nvr"], "rhel-9-golang-1.25")
        self.assertIn("elliott snapshot new failed", str(ctx.exception))

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_empty_stdout_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (0, "", "")
        handler = self._make_handler()
        with self.assertRaises(ValueError) as ctx:
            await handler._create_snapshot_via_elliott(["some-nvr"], "rhel-9-golang-1.25")
        self.assertIn("invalid output", str(ctx.exception))

    @patch("doozerlib.backend.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("doozerlib.backend.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_missing_spec_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (0, "apiVersion: v1\nkind: Snapshot\n", "")
        handler = self._make_handler()
        with self.assertRaises(ValueError) as ctx:
            await handler._create_snapshot_via_elliott(["some-nvr"], "rhel-9-golang-1.25")
        self.assertIn("missing 'spec'", str(ctx.exception))


class TestCommitPushFailure(IsolatedAsyncioTestCase):
    async def test_push_failure_raises(self):
        runtime = Mock()
        runtime.config = {}
        runtime.product = "ocp"
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        handler = GolangBuilderShipmentHandler(
            runtime=runtime,
            ocp_version="4.22",
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
                    release_plan="ocp-art-golang-builder-prod-rhel9",
                    nvrs=["some-nvr"],
                    ocp_version="4.22",
                )
        self.assertIn("Failed to push", str(ctx.exception))


class TestCreateShipmentFromNvrs(IsolatedAsyncioTestCase):
    async def test_create_shipment_from_nvrs_wires_steps(self):
        runtime = Mock()
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        handler = GolangBuilderShipmentHandler(runtime=runtime, ocp_version="4.22")
        handler._setup_working_dir = Mock()
        handler._setup_repos = AsyncMock()
        handler._create_snapshot_via_elliott = AsyncMock(return_value=Mock(spec=["spec"]))
        handler._create_snapshot_via_elliott.return_value.spec.application = ART_IMAGES_BASE_APPLICATION
        handler._build_shipment_config = Mock(return_value=Mock())
        handler._create_shipment_mr = AsyncMock(return_value="https://gitlab.example.com/-/merge_requests/1")

        result = await handler.create_shipment_from_nvrs(
            ["some-nvr"],
            golang_group="rhel-9-golang-1.25",
            env="prod",
        )

        self.assertEqual(result, "https://gitlab.example.com/-/merge_requests/1")
        handler._setup_working_dir.assert_called_once()
        handler._setup_repos.assert_called_once()
        handler._create_snapshot_via_elliott.assert_called_once()
        handler._create_shipment_mr.assert_called_once()


class TestCreateShipmentInline(IsolatedAsyncioTestCase):
    async def test_create_shipment_returns_none_on_failure(self):
        runtime = Mock()
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
        runtime.group_config = Model({})
        runtime.product = "ocp"
        runtime.group = "openshift-4.22"
        handler = GolangBuilderShipmentHandler(runtime=runtime, ocp_version="4.22")
        handler._setup_working_dir = Mock(side_effect=RuntimeError("boom"))

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
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        runtime.logger = Mock()
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


class TestDeriveGolangGroup(unittest.TestCase):
    def test_from_rpm_nvr(self):
        self.assertEqual(derive_golang_group(["golang-1.25.9-1.el9"]), "rhel-9-golang-1.25")

    def test_from_konflux_image_nvr(self):
        nvr = "openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9"
        self.assertEqual(derive_golang_group([nvr]), "rhel-9-golang-1.25")

    def test_unknown_nvr_raises(self):
        with self.assertRaises(ValueError):
            derive_golang_group(["not-a-golang-nvr-1.0-1.noarch"])


class TestShipmentFilePath(unittest.TestCase):
    def test_path_format(self):
        application = "art-images-base"
        product = "ocp"
        golang_group = "rhel-9-golang-1.25"
        env = "prod"
        expected_prefix = Path("shipment") / "ocp" / "rhel-9-golang-1.25" / "art-images-base" / "prod"
        actual = Path("shipment") / product / golang_group / application / env
        self.assertEqual(actual, expected_prefix)


if __name__ == "__main__":
    unittest.main()
