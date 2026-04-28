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

        image_model = Model(
            {
                "name": "test-base",
                "base_only": True,
                "snapshot_release": True,
                "distgit": {"component": "ose-test-base-container"},
            }
        )
        data_obj = Model({"key": "test-base", "data": image_model, "filename": "test-base.yaml"})
        self.metadata = ImageMetadata(self.runtime, data_obj)
        self.metadata.distgit_key = "test-base"

        self.nvr = "test-base-container-v1.0.0-1.el9"
        self.image_pullspec = "quay.io/test/test-base:latest"

        self.nvr_list = [self.nvr]

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_success(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.runtime, self.nvr_list, dry_run=True)

        # Mock the component_map and build records
        mock_build_record = MagicMock()
        mock_build_record.name = "test-base"
        mock_build_record.image_pullspec = self.image_pullspec
        mock_build_record.rebase_repo_url = "https://example.com/repo.git"
        mock_build_record.rebase_commitish = "abc123"

        self.runtime.component_map = {"test-base-container": self.metadata}

        with patch.object(handler, "_fetch_build_records", return_value={self.nvr: mock_build_record}):
            with patch.object(handler, "_create_snapshot", return_value="test-snapshot"):
                with patch.object(handler, "_create_release_from_snapshot", return_value="test-release"):
                    with patch.object(handler, "_wait_for_release_completion", return_value=True):
                        result = await handler.process_base_image_completion()

        self.assertIsNotNone(result)
        self.assertEqual(result, ("test-release", "test-snapshot"))

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_failure(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.runtime, self.nvr_list, dry_run=False)

        # Mock empty image_map to cause failure
        self.runtime.image_map = {}

        with patch.object(handler, "_fetch_build_records", return_value={}):
            result = await handler.process_base_image_completion()

        self.assertIsNone(result)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_critical_errors_with_success(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        valid_nvr = "valid-base-container-v1.0.0-1.el9"
        invalid_nvr = "invalid-base-container-v1.0.0-1.el9"

        handler = BaseImageHandler(self.runtime, [valid_nvr, invalid_nvr], dry_run=True)

        valid_build_record = MagicMock()
        valid_build_record.name = "valid-base"
        valid_build_record.image_pullspec = "quay.io/test/valid-base:latest"

        invalid_build_record = MagicMock()
        invalid_build_record.name = "invalid-base"
        invalid_build_record.image_pullspec = None

        self.runtime.image_map = {"valid-base": self.metadata}

        build_records = {valid_nvr: valid_build_record, invalid_nvr: invalid_build_record}

        with patch.object(handler, "_fetch_build_records", return_value=build_records):
            with patch.object(handler, "_create_snapshot", return_value="test-snapshot"):
                with patch.object(handler, "_create_release_from_snapshot", return_value="test-release"):
                    with patch.object(handler, "_wait_for_release_completion", return_value=True):
                        with self.assertRaises(RuntimeError) as context:
                            await handler.process_base_image_completion()

        error_message = str(context.exception)
        self.assertIn("Snapshot/Release completed successfully", error_message)
        self.assertIn("1 critical validation failures", error_message)
        self.assertIn("Could not resolve metadata", error_message)
        self.assertIn("invalid-base", error_message)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_metadata_missing_error(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        valid_nvr = "valid-base-container-v1.0.0-1.el9"
        missing_metadata_nvr = "missing-metadata-container-v1.0.0-1.el9"

        handler = BaseImageHandler(self.runtime, [valid_nvr, missing_metadata_nvr], dry_run=True)

        valid_build_record = MagicMock()
        valid_build_record.name = "valid-base"
        valid_build_record.image_pullspec = "quay.io/test/valid-base:latest"

        missing_metadata_record = MagicMock()
        missing_metadata_record.name = "missing-metadata"
        missing_metadata_record.image_pullspec = "quay.io/test/missing:latest"

        self.runtime.image_map = {"valid-base": self.metadata}

        build_records = {valid_nvr: valid_build_record, missing_metadata_nvr: missing_metadata_record}

        with patch.object(handler, "_fetch_build_records", return_value=build_records):
            with patch.object(handler, "_create_snapshot", return_value="test-snapshot"):
                with patch.object(handler, "_create_release_from_snapshot", return_value="test-release"):
                    with patch.object(handler, "_wait_for_release_completion", return_value=True):
                        with self.assertRaises(RuntimeError) as context:
                            await handler.process_base_image_completion()

        error_message = str(context.exception)
        self.assertIn("Could not resolve metadata", error_message)
        self.assertIn("missing-metadata", error_message)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_missing_component_name_error(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        valid_nvr = "valid-base-container-v1.0.0-1.el9"
        missing_name_nvr = "missing-name-container-v1.0.0-1.el9"

        handler = BaseImageHandler(self.runtime, [valid_nvr, missing_name_nvr], dry_run=True)

        valid_build_record = MagicMock()
        valid_build_record.name = "valid-base"
        valid_build_record.image_pullspec = "quay.io/test/valid-base:latest"

        missing_name_record = MagicMock()
        missing_name_record.name = None
        missing_name_record.image_pullspec = "quay.io/test/missing-name:latest"

        self.runtime.image_map = {"valid-base": self.metadata}

        build_records = {valid_nvr: valid_build_record, missing_name_nvr: missing_name_record}

        with patch.object(handler, "_fetch_build_records", return_value=build_records):
            with patch.object(handler, "_create_snapshot", return_value="test-snapshot"):
                with patch.object(handler, "_create_release_from_snapshot", return_value="test-release"):
                    with patch.object(handler, "_wait_for_release_completion", return_value=True):
                        with self.assertRaises(RuntimeError) as context:
                            await handler.process_base_image_completion()

        error_message = str(context.exception)
        self.assertIn("No component name found", error_message)
        self.assertIn(missing_name_nvr, error_message)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_process_base_image_completion_golang_builder_success(
        self, mock_kubeconfig, mock_namespace, mock_konflux_client_init
    ):
        """Test that golang builders can successfully go through base image workflow."""
        from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME

        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"

        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        # Create golang builder metadata
        golang_builder_model = Model(
            {
                "name": GOLANG_BUILDER_IMAGE_NAME,
                "snapshot_release": True,
                "distgit": {"component": "ose-golang-builder-container"},
            }
        )
        golang_data = Model({"key": "golang-builder", "data": golang_builder_model, "filename": "golang-builder.yaml"})
        golang_metadata = ImageMetadata(self.runtime, golang_data)
        golang_metadata.distgit_key = "golang-builder"

        golang_nvr = "golang-builder-container-v1.0.0-1.el9"
        handler = BaseImageHandler(self.runtime, [golang_nvr], dry_run=True)

        # Mock the build record for golang builder
        golang_build_record = MagicMock()
        golang_build_record.name = "golang-builder"
        golang_build_record.image_pullspec = "quay.io/test/golang-builder:latest"
        golang_build_record.rebase_repo_url = "https://example.com/golang-builder.git"
        golang_build_record.rebase_commitish = "def456"

        self.runtime.image_map = {"golang-builder": golang_metadata}

        with patch.object(handler, "_fetch_build_records", return_value={golang_nvr: golang_build_record}):
            with patch.object(handler, "_create_snapshot", return_value="golang-snapshot"):
                with patch.object(handler, "_create_release_from_snapshot", return_value="golang-release"):
                    with patch.object(handler, "_wait_for_release_completion", return_value=True):
                        result = await handler.process_base_image_completion()

        # Golang builder should successfully complete the workflow
        self.assertIsNotNone(result)
        self.assertEqual(result, ("golang-release", "golang-snapshot"))

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    async def test_create_release_from_snapshot_adds_nvr_annotation(
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

        handler = BaseImageHandler(
            self.runtime,
            [self.nvr, "another-base-container-v1.0.0-1.el8"],
            dry_run=False,
        )

        with patch.object(handler, "_wait_for_snapshot_availability", new=AsyncMock(return_value=True)):
            await handler._create_release_from_snapshot("test-snapshot")

        konflux_client._get.assert_awaited_once()
        konflux_client._create.assert_awaited_once()
        release_obj = konflux_client._create.await_args.args[0]
        self.assertEqual(
            release_obj["metadata"]["annotations"]["art.redhat.com/nvrs"],
            "another-base-container-v1.0.0-1.el8,test-base-container-v1.0.0-1.el9",
        )
