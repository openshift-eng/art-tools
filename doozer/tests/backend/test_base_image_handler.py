import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Model
from doozerlib.backend.base_image_handler import BaseImageHandler
from doozerlib.image import ImageMetadata


class TestBaseImageHandler(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.22"
        self.runtime.group_config.name = "openshift-4.22"
        self.runtime.assembly = "stream"
        self.runtime.product = "ocp"

        image_model = Model({"name": "test-base", "base_only": True, "snapshot_release": True})
        data_obj = Model({"key": "test-base", "data": image_model, "filename": "test-base.yaml"})
        self.metadata = ImageMetadata(self.runtime, data_obj)
        self.metadata.distgit_key = "test-base"

        self.nvr = "test-base-container-v1.0.0-1.el9"
        self.image_pullspec = "quay.io/test/test-base:latest"

    @patch("doozerlib.backend.base_image_handler.cmd_gather_async")
    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("artcommonlib.util.resolve_konflux_namespace_by_product")
    @patch("artcommonlib.util.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_success(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init, mock_cmd_gather
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        mock_cmd_gather.return_value = (0, "", "")

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.metadata, self.nvr, self.image_pullspec, dry_run=True)

        with patch.object(handler, "_create_snapshot", return_value="test-snapshot"):
            with patch.object(handler, "_create_release_from_snapshot", return_value="test-release"):
                with patch.object(handler, "_wait_for_release_completion", return_value=True):
                    result = await handler.process_base_image_completion()

        self.assertIsNotNone(result)
        self.assertEqual(result, ("test-release", "test-snapshot"))

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("artcommonlib.util.resolve_konflux_namespace_by_product")
    @patch("artcommonlib.util.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_failure(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.metadata, self.nvr, self.image_pullspec, dry_run=False)

        with patch.object(handler, "_create_snapshot", return_value=None):
            result = await handler.process_base_image_completion()

        self.assertIsNone(result)
