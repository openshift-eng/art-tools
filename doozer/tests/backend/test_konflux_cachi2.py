from unittest import TestCase

from artcommonlib.model import Missing
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch


class TestKonfluxCachi2(TestCase):
    def test_cachi2_enabled_1(self):
        metadata = MagicMock()
        metadata.config.konflux.cachi2.enabled = True

        self.assertTrue(KonfluxImageBuilder._is_cachi2_enabled(metadata))

    def test_cachi2_enabled_2(self):
        metadata = MagicMock()
        metadata.config.konflux.cachi2.enabled = False

        self.assertFalse(KonfluxImageBuilder._is_cachi2_enabled(metadata))

    def test_cachi2_enabled_3(self):
        metadata = MagicMock()
        metadata.config.konflux.cachi2.enabled = None
        metadata.runtime.group_config.konflux.cachi2.enabled = True

        self.assertTrue(KonfluxImageBuilder._is_cachi2_enabled(metadata))

    def test_cachi2_enabled_4(self):
        metadata = MagicMock()
        metadata.config.konflux.cachi2.enabled = Missing
        metadata.runtime.group_config.konflux.cachi2.enabled = False

        self.assertFalse(KonfluxImageBuilder._is_cachi2_enabled(metadata))

    @patch("artcommonlib.util.is_cachito_enabled")
    def test_cachi2_enabled_5(self, is_cachito_enabled):
        metadata = MagicMock()
        metadata.config.konflux.cachi2.enabled = Missing
        metadata.runtime.group_config.konflux.cachi2.enabled = Missing
        is_cachito_enabled.return_value = True

        self.assertTrue(KonfluxImageBuilder._is_cachi2_enabled(metadata))

    @patch("artcommonlib.util.is_cachito_enabled")
    def test_cachi2_enabled_6(self, is_cachito_enabled):
        metadata = MagicMock()
        metadata.config.konflux.cachi2.enabled = False
        metadata.runtime.group_config.konflux.cachi2.enabled = Missing
        is_cachito_enabled.return_value = True

        self.assertFalse(KonfluxImageBuilder._is_cachi2_enabled(metadata))

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.konflux_image_builder.KonfluxImageBuilder._is_cachi2_enabled")
    def test_prefetch_1(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()

        self.assertEqual(builder._prefetch(metadata=metadata), [])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.konflux_image_builder.KonfluxImageBuilder._is_cachi2_enabled")
    def test_prefetch_2(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["unknown"]

        self.assertEqual(builder._prefetch(metadata=metadata), [])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.konflux_image_builder.KonfluxImageBuilder._is_cachi2_enabled")
    def test_prefetch_3(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["gomod"]

        self.assertEqual(builder._prefetch(metadata=metadata), [{"type": "gomod", "path": "."}])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.konflux_image_builder.KonfluxImageBuilder._is_cachi2_enabled")
    def test_prefetch_4(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {'gomod': [{'path': 'api'}]}

        self.assertEqual(builder._prefetch(metadata=metadata), [{"type": "gomod", "path": "api"}])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.konflux_image_builder.KonfluxImageBuilder._is_cachi2_enabled")
    def test_prefetch_4(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["gomod"]
        metadata.config.cachito.packages = {"gomod": [{"path": "."}, {"path": "api"}, {"path": "client/pkg"}]}

        self.assertEqual(builder._prefetch(metadata=metadata), [{"type": "gomod", "path": "."}, {"type": "gomod", "path": "api"}, {"type": "gomod", "path": "client/pkg"}])

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("doozerlib.backend.konflux_image_builder.KonfluxImageBuilder._is_cachi2_enabled")
    def test_prefetch_5(self, mock_konflux_client_init, mock_is_cachito_enabled):
        builder = KonfluxImageBuilder(MagicMock())
        mock_is_cachito_enabled.return_value = False
        metadata = MagicMock()
        metadata.config.content.source.pkg_managers = ["npm", "gomod"]
        metadata.config.cachito.packages = {'npm': [{'path': 'web'}], 'gomod': [{'path': '.'}]}

        self.assertEqual(builder._prefetch(metadata=metadata), [{'type': 'gomod', 'path': '.'}, {'type': 'npm', 'path': 'web'}])
