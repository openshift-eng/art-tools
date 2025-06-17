from unittest import TestCase
from unittest.mock import MagicMock, patch

from artcommonlib.model import Missing
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder


class TestKonfluxCachi2(TestCase):
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.image.ImageMetadata.is_cachi2_enabled")
    def test_prefetch_1(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()

        self.assertEqual(builder._prefetch(metadata=metadata), [])

    @patch("doozerlib.image.ImageMetadata.is_cachi2_enabled")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_2(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["unknown"]

        self.assertEqual(builder._prefetch(metadata=metadata), [])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.image.ImageMetadata.is_cachi2_enabled")
    def test_prefetch_3(self, mock_is_cachito_enabled, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["gomod"]

        self.assertEqual(builder._prefetch(metadata=metadata), [{"type": "gomod", "path": "."}])

    @patch("doozerlib.image.ImageMetadata.is_cachi2_enabled")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    def test_prefetch_4(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': 'api'}]}

        self.assertEqual(builder._prefetch(metadata=metadata), [{"type": "gomod", "path": "api"}])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.image.ImageMetadata.is_cachi2_enabled")
    def test_prefetch_5(self, mock_is_cachito_enabled, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}, {"path": "api"}, {"path": "client/pkg"}]}

        self.assertEqual(
            builder._prefetch(metadata=metadata),
            [{"type": "gomod", "path": "."}, {"type": "gomod", "path": "api"}, {"type": "gomod", "path": "client/pkg"}],
        )

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.image.ImageMetadata.is_cachi2_enabled")
    def test_prefetch_6(self, mock_is_cachito_enabled, mock_konflux_client_init):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["npm", "gomod"]
        metadata.config.cachito.packages = {'npm': [{'path': 'web'}], 'gomod': [{'path': '.'}]}

        self.assertEqual(
            builder._prefetch(metadata=metadata), [{'type': 'gomod', 'path': '.'}, {'type': 'npm', 'path': 'web'}]
        )
