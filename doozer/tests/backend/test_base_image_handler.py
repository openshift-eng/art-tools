from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from doozerlib.backend.base_image_handler import BaseImageHandler, BaseImageSnapshotInput
from doozerlib.image import ImageMetadata


class TestBaseImageHandler(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.22"
        self.runtime.group_config.name = "openshift-4.22"
        self.runtime.assembly = "stream"
        self.runtime.product = "ocp"
        self.runtime.variant = BuildVariant.OCP

        image_model = Model(
            {
                "name": "test-base",
                "base_only": True,
                "base_image_release": {"enabled": True},
                "distgit": {"component": "ose-test-base-container"},
            }
        )
        data_obj = Model({"key": "test-base", "data": image_model, "filename": "test-base.yaml"})
        self.metadata = ImageMetadata(self.runtime, data_obj)
        self.metadata.distgit_key = "test-base"

        self.nvr = "test-base-container-v1.0.0-1.el9"
        self.image_pullspec = "quay.io/test/test-base:latest"

        self.default_input = BaseImageSnapshotInput(
            nvr=self.nvr,
            distgit_key="test-base",
            container_image=self.image_pullspec,
            rebase_repo_url="https://example.com/repo.git",
            rebase_commitish="abc123",
            is_golang_builder=False,
        )

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_snapshot_release_success(self, mock_kubeconfig, mock_namespace, mock_konflux_client_init):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        mock_konflux_client_init.return_value = AsyncMock()

        handler = BaseImageHandler(self.runtime, dry_run=True)
        self.runtime.image_map = {"test-base": self.metadata}

        with patch.object(handler, "_snapshot_from_component", new=AsyncMock(return_value="test-snapshot")):
            with patch.object(handler, "_create_release_from_snapshot", new=AsyncMock(return_value="test-release")):
                with patch.object(handler, "_wait_for_release_completion", return_value=True):
                    result = await handler.snapshot_release(self.default_input)

        self.assertIsNotNone(result)
        self.assertEqual(result, ("test-release", "test-snapshot"))

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_snapshot_release_builds_one_component(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        mock_konflux_client_init.return_value = AsyncMock()

        handler = BaseImageHandler(self.runtime, dry_run=True)
        self.runtime.image_map = {"test-base": self.metadata}

        with patch.object(handler, "_snapshot_from_component", new=AsyncMock(return_value="snap")) as mock_snap:
            with patch.object(handler, "_create_release_from_snapshot", new=AsyncMock(return_value="rel")):
                with patch.object(handler, "_wait_for_release_completion", return_value=True):
                    await handler.snapshot_release(self.default_input)

        mock_snap.assert_awaited_once()
        comp = mock_snap.await_args.args[0]
        self.assertEqual(comp["containerImage"], self.image_pullspec)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_snapshot_release_unknown_metadata_returns_none(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        mock_konflux_client_init.return_value = AsyncMock()

        handler = BaseImageHandler(self.runtime, dry_run=False)
        self.runtime.image_map = {}

        result = await handler.snapshot_release(self.default_input)

        self.assertIsNone(result)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_snapshot_release_empty_pullspec_returns_none(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        mock_konflux_client_init.return_value = AsyncMock()

        bad = BaseImageSnapshotInput(
            nvr="x-container-v1-1.el9",
            distgit_key="test-base",
            container_image="",
        )
        handler = BaseImageHandler(self.runtime, dry_run=True)
        self.runtime.image_map = {"test-base": self.metadata}

        result = await handler.snapshot_release(bad)
        self.assertIsNone(result)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_snapshot_release_golang_builder_success(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME

        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        mock_konflux_client_init.return_value = AsyncMock()

        golang_builder_model = Model(
            {
                "name": GOLANG_BUILDER_IMAGE_NAME,
                "base_image_release": {"enabled": True},
                "distgit": {"component": "ose-golang-builder-container"},
            }
        )
        golang_data = Model({"key": "golang-builder", "data": golang_builder_model, "filename": "golang-builder.yaml"})
        golang_metadata = ImageMetadata(self.runtime, golang_data)
        golang_metadata.distgit_key = "golang-builder"

        golang_nvr = "golang-builder-container-v1.0.0-1.el9"
        inp = BaseImageSnapshotInput(
            nvr=golang_nvr,
            distgit_key="golang-builder",
            container_image="quay.io/test/golang-builder:latest",
            rebase_repo_url="https://example.com/golang-builder.git",
            rebase_commitish="def456",
            is_golang_builder=True,
        )

        handler = BaseImageHandler(self.runtime, dry_run=True)
        self.runtime.image_map = {"golang-builder": golang_metadata}

        with patch.object(handler, "_snapshot_from_component", new=AsyncMock(return_value="golang-snapshot")):
            with patch.object(handler, "_create_release_from_snapshot", new=AsyncMock(return_value="golang-release")):
                with patch.object(handler, "_wait_for_release_completion", return_value=True):
                    result = await handler.snapshot_release(inp)

        self.assertIsNotNone(result)
        self.assertEqual(result, ("golang-release", "golang-snapshot"))

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_create_release_from_snapshot_sets_release_annotations(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        created_release = MagicMock()
        created_release.metadata.name = "test-release"
        konflux_client._create.return_value = created_release
        konflux_client.resource_url = MagicMock(return_value="https://konflux.example/releases/test-release")
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.runtime, dry_run=False)

        with patch.object(handler, "_wait_for_snapshot_availability", new=AsyncMock(return_value=True)):
            await handler._create_release_from_snapshot("test-snapshot", self.default_input)

        konflux_client._get.assert_awaited_once()
        konflux_client._create.assert_awaited_once()
        release_obj = konflux_client._create.await_args.args[0]
        annotations = release_obj["metadata"]["annotations"]
        self.assertEqual(annotations["art.redhat.com/nvr"], self.nvr)
        self.assertEqual(annotations["art.redhat.com/distgit-key"], "test-base")
        self.assertNotIn("art.redhat.com/job-url", annotations)
        self.assertEqual(release_obj["metadata"]["generateName"], "openshift-4-22-base-image-release-")

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_create_release_from_snapshot_sets_job_url_when_build_url_set(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        created_release = MagicMock()
        created_release.metadata.name = "test-release"
        konflux_client._create.return_value = created_release
        konflux_client.resource_url = MagicMock(return_value="https://konflux.example/releases/test-release")
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.runtime, dry_run=False)

        with patch.object(handler, "_wait_for_snapshot_availability", new=AsyncMock(return_value=True)):
            with patch.dict("os.environ", {"BUILD_URL": "https://jenkins.example/job/123/"}):
                await handler._create_release_from_snapshot("test-snapshot", self.default_input)

        release_obj = konflux_client._create.await_args.args[0]
        self.assertEqual(
            release_obj["metadata"]["annotations"]["art.redhat.com/job-url"],
            "https://jenkins.example/job/123/",
        )

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_create_release_from_snapshot_rhmtc_uses_product_plan_and_application(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "art-mtc-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        created_release = MagicMock()
        created_release.metadata.name = "test-release"
        konflux_client._create.return_value = created_release
        konflux_client.resource_url = MagicMock(return_value="https://konflux.example/releases/test-release")
        mock_konflux_client_init.return_value = konflux_client

        self.runtime.product = "rhmtc"

        handler = BaseImageHandler(self.runtime, dry_run=False)

        with patch.object(handler, "_wait_for_snapshot_availability", new=AsyncMock(return_value=True)):
            await handler._create_release_from_snapshot("test-snapshot", self.default_input)

        konflux_client._get.assert_awaited_once()
        self.assertEqual(konflux_client._get.await_args.args[2], "mtc-images-base-silent")

        konflux_client._create.assert_awaited_once()
        release_obj = konflux_client._create.await_args.args[0]
        self.assertEqual(release_obj["spec"]["releasePlan"], "mtc-images-base-silent")
        self.assertEqual(
            release_obj["metadata"]["labels"]["appstudio.openshift.io/application"],
            "mtc-images-base",
        )
