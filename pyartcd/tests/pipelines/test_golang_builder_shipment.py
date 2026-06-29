import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from doozerlib.constants import ART_IMAGES_BASE_APPLICATION
from pyartcd.pipelines.golang_builder_shipment import (
    GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP,
    GolangBuilderShipmentPipeline,
    derive_golang_group,
    resolve_lifecycle_env,
)


class TestResolveReleasePlan(unittest.TestCase):
    """Test lifecycle -> releasePlan mapping."""

    def test_prod_returns_prod_plan(self):
        plan = GolangBuilderShipmentPipeline.resolve_release_plan("prod")
        self.assertEqual(plan, "ocp-art-golang-builder-prod-rhel9")

    def test_ec_returns_ec_plan(self):
        plan = GolangBuilderShipmentPipeline.resolve_release_plan("ec")
        self.assertEqual(plan, "ocp-art-golang-builder-ec-rhel9")

    def test_unknown_env_raises(self):
        with self.assertRaises(ValueError):
            GolangBuilderShipmentPipeline.resolve_release_plan("staging")

    def test_map_keys_are_complete(self):
        self.assertIn("prod", GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP)
        self.assertIn("ec", GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP)


class _FakeResponse:
    def __init__(self, status, body=""):
        self.status = status
        self._body = body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _FakeSession:
    def __init__(self, response):
        self._response = response

    def get(self, *args, **kwargs):
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestResolveLifecycleEnv(IsolatedAsyncioTestCase):
    """Test auto-resolution of prod/ec from ocp-build-data group.yml."""

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_release_phase_returns_prod(self, mock_session_cls):
        resp = _FakeResponse(200, "software_lifecycle:\n  phase: release\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.18")
        self.assertEqual(result, "prod")

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_pre_release_phase_returns_ec(self, mock_session_cls):
        resp = _FakeResponse(200, "software_lifecycle:\n  phase: pre-release\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.23")
        self.assertEqual(result, "ec")

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_missing_lifecycle_defaults_to_prod(self, mock_session_cls):
        resp = _FakeResponse(200, "vars:\n  GOLANG_VERSION: '1.22'\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.16")
        self.assertEqual(result, "prod")

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_http_error_raises(self, mock_session_cls):
        resp = _FakeResponse(404)
        mock_session_cls.return_value = _FakeSession(resp)
        with self.assertRaises(RuntimeError):
            await resolve_lifecycle_env("99.99")


class TestBasicAuthUrl(unittest.TestCase):
    def test_injects_token(self):
        url = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data.git"
        result = GolangBuilderShipmentPipeline.basic_auth_url(url, "mytoken")
        self.assertIn("oauth2:mytoken@", result)
        self.assertIn("gitlab.cee.redhat.com", result)
        self.assertTrue(result.startswith("https://"))


class TestPipelineInit(unittest.TestCase):
    def _make_runtime(self, dry_run=False):
        runtime = Mock()
        runtime.dry_run = dry_run
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        return runtime

    def test_init_sorts_nvrs(self):
        runtime = self._make_runtime()
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["golang-builder-container-v1.25-1.el9", "golang-builder-container-v1.25-1.el8"],
        )
        self.assertEqual(pipeline.product, "ocp")
        self.assertEqual(
            pipeline.nvrs,
            [
                "golang-builder-container-v1.25-1.el8",
                "golang-builder-container-v1.25-1.el9",
            ],
        )
        # env and release_plan are None until run() resolves them
        self.assertIsNone(pipeline.env)
        self.assertIsNone(pipeline.release_plan)


class TestBuildShipmentConfig(IsolatedAsyncioTestCase):
    def _make_pipeline(self, env="prod"):
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["golang-builder-container-v1.25-202606220000.el9"],
            art_jira="ART-20930",
        )
        pipeline.env = env
        pipeline.release_plan = pipeline.resolve_release_plan(env)
        return pipeline

    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_build_shipment_config_prod(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (
            0,
            """
apiVersion: appstudio.redhat.com/v1alpha1
kind: Snapshot
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

        pipeline = self._make_pipeline(env="prod")
        config = await pipeline.build_shipment_config()

        self.assertEqual(config.shipment.metadata.product, "ocp")
        self.assertEqual(config.shipment.metadata.application, "art-images-base")
        self.assertEqual(config.shipment.metadata.group, "rhel-9-golang-1.25")
        self.assertEqual(config.shipment.metadata.assembly, "stream")
        self.assertEqual(
            config.shipment.environments.prod.releasePlan,
            "ocp-art-golang-builder-prod-rhel9",
        )
        self.assertEqual(config.shipment.data.releaseNotes.type, "RHBA")
        self.assertIn(
            "https://redhat.atlassian.net/browse/ART-20930",
            config.shipment.data.releaseNotes.references,
        )

        cmd_args = mock_cmd.call_args[0][0]
        self.assertTrue(any("--builds-file=" in str(a) for a in cmd_args))

    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_build_shipment_config_ec(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (
            0,
            """
apiVersion: appstudio.redhat.com/v1alpha1
kind: Snapshot
spec:
  application: art-images-base
  components:
    - name: golang-builder-v1.26-rhel9
      containerImage: quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:def456
      source:
        git:
          url: https://github.com/openshift-priv/builder
          revision: def456
""",
            "",
        )

        pipeline = self._make_pipeline(env="ec")
        config = await pipeline.build_shipment_config()

        self.assertEqual(
            config.shipment.environments.prod.releasePlan,
            "ocp-art-golang-builder-ec-rhel9",
        )


class TestCreateShipmentMR(IsolatedAsyncioTestCase):
    def _make_pipeline(self, dry_run=False):
        runtime = Mock()
        runtime.dry_run = dry_run
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
            art_jira="ART-20930",
        )
        pipeline.env = "prod"
        pipeline.release_plan = "ocp-art-golang-builder-prod-rhel9"
        pipeline._gitlab_token = "test-token"
        pipeline.shipment_data_repo = AsyncMock()
        pipeline.shipment_data_repo._directory = Path("/tmp/test-working/shipment-data-push")
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)
        return pipeline

    def _make_config_mock(self):
        config = Mock()
        config.shipment.metadata.application = "art-images-base"
        config.model_dump.return_value = {"shipment": {}}
        return config

    @patch("pyartcd.pipelines.golang_builder_shipment.python_gitlab")
    async def test_dry_run_returns_placeholder(self, mock_gitlab):
        pipeline = self._make_pipeline(dry_run=True)
        pipeline.env = "prod"
        config = self._make_config_mock()

        mock_project = MagicMock()
        mock_gitlab.Gitlab.return_value.projects.get.return_value = mock_project

        with patch("pathlib.Path.mkdir"):
            result = await pipeline.create_shipment_mr(config)

        self.assertIn("placeholder", result)
        mock_project.mergerequests.create.assert_not_called()

    @patch("pyartcd.pipelines.golang_builder_shipment.python_gitlab")
    async def test_creates_mr_with_correct_title(self, mock_gitlab):
        pipeline = self._make_pipeline(dry_run=False)
        config = self._make_config_mock()

        mock_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.cee.redhat.com/test/-/merge_requests/1"
        mock_project.mergerequests.create.return_value = mock_mr
        mock_gitlab.Gitlab.return_value.projects.get.return_value = mock_project

        with patch("pathlib.Path.mkdir"):
            result = await pipeline.create_shipment_mr(config)

        self.assertEqual(result, mock_mr.web_url)
        create_args = mock_project.mergerequests.create.call_args[0][0]
        self.assertIn("Golang builder shipment", create_args["title"])
        self.assertEqual(create_args["target_branch"], "main")


class TestSetupReposNoToken(IsolatedAsyncioTestCase):
    @patch.dict("os.environ", {}, clear=True)
    async def test_missing_gitlab_token_raises(self):
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
        )
        with self.assertRaises(ValueError):
            await pipeline.setup_repos()


class TestSnapshotWithQuayAuth(IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
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
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
        )
        await pipeline._create_snapshot()
        cmd_args = mock_cmd.call_args[0][0]
        self.assertTrue(any("--pull-secret=" in str(a) for a in cmd_args))


class TestApplicationOverride(IsolatedAsyncioTestCase):
    """Verify snapshot application is overridden to art-images-base."""

    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_elliott_app_overridden_to_art_images_base(self, mock_unlink, mock_cmd):
        # elliott returns "rhel-9-golang-1-25" as the application
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
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
            art_jira="ART-20930",
        )
        pipeline.env = "prod"
        pipeline.release_plan = pipeline.resolve_release_plan("prod")

        config = await pipeline.build_shipment_config()

        self.assertEqual(config.shipment.metadata.application, ART_IMAGES_BASE_APPLICATION)
        self.assertEqual(config.shipment.snapshot.spec.application, ART_IMAGES_BASE_APPLICATION)


class TestCreateSnapshotErrors(IsolatedAsyncioTestCase):
    """Test error paths in _create_snapshot."""

    def _make_pipeline(self):
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        return GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
        )

    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_elliott_nonzero_rc_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (1, "", "elliott error: NVR not found")
        pipeline = self._make_pipeline()
        with self.assertRaises(RuntimeError) as ctx:
            await pipeline._create_snapshot()
        self.assertIn("elliott snapshot new failed", str(ctx.exception))

    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_empty_stdout_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (0, "", "")
        pipeline = self._make_pipeline()
        with self.assertRaises(ValueError) as ctx:
            await pipeline._create_snapshot()
        self.assertIn("invalid output", str(ctx.exception))

    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_missing_spec_raises(self, mock_unlink, mock_cmd):
        mock_cmd.return_value = (0, "apiVersion: v1\nkind: Snapshot\n", "")
        pipeline = self._make_pipeline()
        with self.assertRaises(ValueError) as ctx:
            await pipeline._create_snapshot()
        self.assertIn("missing 'spec'", str(ctx.exception))


class TestCommitPushFailure(IsolatedAsyncioTestCase):
    """Test create_shipment_mr when commit_push returns False."""

    async def test_push_failure_raises(self):
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
            art_jira="ART-20930",
        )
        pipeline.env = "prod"
        pipeline.release_plan = "ocp-art-golang-builder-prod-rhel9"
        pipeline._gitlab_token = "test-token"
        pipeline.shipment_data_repo = AsyncMock()
        pipeline.shipment_data_repo._directory = Path("/tmp/test-working/shipment-data-push")
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=False)

        config = Mock()
        config.shipment.metadata.application = "art-images-base"
        config.model_dump.return_value = {"shipment": {}}

        with patch("pathlib.Path.mkdir"):
            with self.assertRaises(RuntimeError) as ctx:
                await pipeline.create_shipment_mr(config)
        self.assertIn("Failed to push", str(ctx.exception))


class TestRunOrchestration(IsolatedAsyncioTestCase):
    """Test run() wiring."""

    @patch("pyartcd.pipelines.golang_builder_shipment.resolve_lifecycle_env")
    async def test_run_calls_steps_in_order(self, mock_resolve_env):
        mock_resolve_env.return_value = "prod"
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
        )
        pipeline.setup_working_dir = Mock()
        pipeline.setup_repos = AsyncMock()
        pipeline.build_shipment_config = AsyncMock(return_value=Mock())
        pipeline.create_shipment_mr = AsyncMock(return_value="https://gitlab.example.com/-/merge_requests/1")

        result = await pipeline.run()

        self.assertEqual(result, "https://gitlab.example.com/-/merge_requests/1")
        pipeline.setup_working_dir.assert_called_once()
        pipeline.setup_repos.assert_called_once()
        pipeline.build_shipment_config.assert_called_once()
        pipeline.create_shipment_mr.assert_called_once()
        self.assertEqual(pipeline.env, "prod")
        self.assertEqual(pipeline.release_plan, "ocp-art-golang-builder-prod-rhel9")


class TestResolveLifecycleEnvUnknownPhase(IsolatedAsyncioTestCase):
    """Test unknown phase falls back to prod."""

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_unknown_phase_defaults_to_prod(self, mock_session_cls):
        resp = _FakeResponse(200, "software_lifecycle:\n  phase: maintenance\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.14")
        self.assertEqual(result, "prod")


class TestBuildShipmentConfigNoJira(IsolatedAsyncioTestCase):
    """Test build_shipment_config without art_jira."""

    @patch("pyartcd.pipelines.golang_builder_shipment.exectools.cmd_gather_async")
    @patch("pyartcd.pipelines.golang_builder_shipment.os.unlink")
    @patch.dict("os.environ", {}, clear=False)
    async def test_no_jira_omits_references(self, mock_unlink, mock_cmd):
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
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
            art_jira="",
        )
        pipeline.env = "prod"
        pipeline.release_plan = pipeline.resolve_release_plan("prod")

        config = await pipeline.build_shipment_config()

        self.assertFalse(
            hasattr(config.shipment.data.releaseNotes, 'references') and config.shipment.data.releaseNotes.references
        )


class TestSetupReposSuccess(IsolatedAsyncioTestCase):
    """Test setup_repos happy path."""

    @patch.dict("os.environ", {"GITLAB_TOKEN": "test-token"}, clear=False)
    async def test_setup_repos_configures_repo(self):
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            golang_group="rhel-9-golang-1.25",
            nvrs=["some-nvr"],
        )
        pipeline.shipment_data_repo = AsyncMock()

        await pipeline.setup_repos()

        self.assertEqual(pipeline._gitlab_token, "test-token")
        pipeline.shipment_data_repo.setup.assert_called_once()
        setup_kwargs = pipeline.shipment_data_repo.setup.call_args[1]
        self.assertIn("oauth2:test-token@", setup_kwargs["remote_url"])
        pipeline.shipment_data_repo.fetch_switch_branch.assert_called_once_with("main")


class TestDeriveGolangGroup(unittest.TestCase):
    """Test golang group derivation from NVR patterns."""

    def test_from_rpm_nvr(self):
        self.assertEqual(derive_golang_group(["golang-1.25.9-1.el9"]), "rhel-9-golang-1.25")

    def test_from_rpm_nvr_el8(self):
        self.assertEqual(derive_golang_group(["golang-1.22.3-2.el8"]), "rhel-8-golang-1.22")

    def test_from_konflux_image_nvr(self):
        nvr = "openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9"
        self.assertEqual(derive_golang_group([nvr]), "rhel-9-golang-1.25")

    def test_unknown_nvr_raises(self):
        with self.assertRaises(ValueError):
            derive_golang_group(["not-a-golang-nvr-1.0-1.noarch"])

    def test_init_derives_group_from_image_nvr(self):
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            nvrs=["openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9"],
        )
        self.assertEqual(pipeline.golang_group, "rhel-9-golang-1.25")

    def test_init_derives_group_from_rpm_nvr(self):
        runtime = Mock()
        runtime.dry_run = False
        runtime.config = {}
        runtime.working_dir = Path("/tmp/test-working")
        pipeline = GolangBuilderShipmentPipeline(
            runtime=runtime,
            ocp_version="4.22",
            nvrs=["golang-1.25.9-1.el9"],
        )
        self.assertEqual(pipeline.golang_group, "rhel-9-golang-1.25")


class TestShipmentFilePath(unittest.TestCase):
    """Verify the shipment YAML file path matches the convention."""

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
