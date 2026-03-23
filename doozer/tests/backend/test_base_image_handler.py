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

        self.image_data_list = [(self.metadata, self.nvr, self.image_pullspec)]

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

        handler = BaseImageHandler(self.runtime, self.image_data_list, dry_run=True)

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

        handler = BaseImageHandler(self.runtime, self.image_data_list, dry_run=False)

        with patch.object(handler, "_create_snapshot", return_value=None):
            result = await handler.process_base_image_completion()

        self.assertIsNone(result)

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    def test_derive_component_name_all_rpa_components(self, mock_kubeconfig, mock_namespace, mock_konflux_client_init):
        """Test component name derivation against actual ReleasePlanAdmission.yml components."""
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.runtime, self.image_data_list, dry_run=True)
        group_name = "openshift-4.22"

        test_cases = [
            ("openshift-base-rhel9", "ose-4-22-openshift-base-rhel9"),
            ("openshift-enterprise-base-rhel9", "ose-4-22-openshift-enterprise-base-rhel9"),
            ("openshift-base-nodejs", "ose-4-22-openshift-base-nodejs-rhel9"),
            ("ose-aws-efs-utils", "ose-4-22-ose-aws-efs-utils-base"),
            ("ose-azure-storage-azcopy-base", "ose-4-22-ose-azure-storage-azcopy-base"),
            ("ose-haproxy-router-base", "ose-4-22-ose-haproxy-router-base"),
            ("ose-ovn-kubernetes-base", "ose-4-22-ovn-kubernetes-base"),
            ("ose-installer-etcd-artifacts", "ose-4-22-ose-installer-etcd-artifacts"),
            ("ose-installer-kube-apiserver-artifacts", "ose-4-22-ose-installer-kube-apiserver-artifacts"),
        ]

        for input_component, expected in test_cases:
            with self.subTest(component=input_component):
                actual = handler._derive_component_name(group_name, input_component)
                self.assertEqual(
                    actual,
                    expected,
                    f"Component name derivation failed:\n"
                    f"  Group: {group_name}\n"
                    f"  Component: {input_component}\n"
                    f"  Expected: {expected}\n"
                    f"  Actual: {actual}",
                )

    @patch("doozerlib.backend.base_image_handler.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_namespace_by_product")
    @patch("doozerlib.backend.base_image_handler.resolve_konflux_kubeconfig_by_product")
    def test_derive_component_name_different_versions(self, mock_kubeconfig, mock_namespace, mock_konflux_client_init):
        """Test component name derivation works with different OCP versions."""
        mock_namespace.return_value = "ocp-art-tenant"
        mock_kubeconfig.return_value = "/path/to/kubeconfig"
        konflux_client = AsyncMock()
        mock_konflux_client_init.return_value = konflux_client

        handler = BaseImageHandler(self.runtime, self.image_data_list, dry_run=True)

        version_tests = [
            ("openshift-4.21", "openshift-base-rhel9", "ose-4-21-openshift-base-rhel9"),
            ("openshift-4.21", "openshift-enterprise-base-rhel9", "ose-4-21-openshift-enterprise-base-rhel9"),
            ("openshift-4.21", "openshift-base-nodejs", "ose-4-21-openshift-base-nodejs-rhel9"),
            ("openshift-4.21", "ose-aws-efs-utils", "ose-4-21-ose-aws-efs-utils-base"),
            ("openshift-4.21", "ose-azure-storage-azcopy-base", "ose-4-21-ose-azure-storage-azcopy-base"),
            ("openshift-4.21", "ose-haproxy-router-base", "ose-4-21-ose-haproxy-router-base"),
            ("openshift-4.21", "ose-installer-etcd-artifacts", "ose-4-21-ose-installer-etcd-artifacts"),
            (
                "openshift-4.21",
                "ose-installer-kube-apiserver-artifacts",
                "ose-4-21-ose-installer-kube-apiserver-artifacts",
            ),
            ("openshift-4.21", "ose-ovn-kubernetes-base", "ose-4-21-ovn-kubernetes-base"),
            ("openshift-4.22", "openshift-base-rhel9", "ose-4-22-openshift-base-rhel9"),
            ("openshift-4.22", "openshift-enterprise-base-rhel9", "ose-4-22-openshift-enterprise-base-rhel9"),
            ("openshift-4.22", "openshift-base-nodejs", "ose-4-22-openshift-base-nodejs-rhel9"),
            ("openshift-4.22", "ose-aws-efs-utils", "ose-4-22-ose-aws-efs-utils-base"),
            ("openshift-4.22", "ose-azure-storage-azcopy-base", "ose-4-22-ose-azure-storage-azcopy-base"),
            ("openshift-4.22", "ose-haproxy-router-base", "ose-4-22-ose-haproxy-router-base"),
            ("openshift-4.22", "ose-installer-etcd-artifacts", "ose-4-22-ose-installer-etcd-artifacts"),
            (
                "openshift-4.22",
                "ose-installer-kube-apiserver-artifacts",
                "ose-4-22-ose-installer-kube-apiserver-artifacts",
            ),
            ("openshift-4.22", "ose-ovn-kubernetes-base", "ose-4-22-ovn-kubernetes-base"),
        ]

        for group_name, component, expected in version_tests:
            with self.subTest(group=group_name, component=component):
                actual = handler._derive_component_name(group_name, component)
                self.assertEqual(
                    actual,
                    expected,
                    f"Version-specific component name derivation failed:\n"
                    f"  Group: {group_name}\n"
                    f"  Component: {component}\n"
                    f"  Expected: {expected}\n"
                    f"  Actual: {actual}",
                )
