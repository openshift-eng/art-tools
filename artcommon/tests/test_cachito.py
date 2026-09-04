from unittest import TestCase
from unittest.mock import MagicMock

from artcommonlib.model import Missing
from artcommonlib.util import is_cachito_enabled


class TestKonfluxCachi2(TestCase):
    def test_cachito_metadata_config(self):
        metadata = MagicMock()
        group_config = MagicMock()

        metadata.config.cachito.enabled = True
        group_config.cachito.enabled = False

        self.assertTrue(is_cachito_enabled(metadata, group_config, logger=MagicMock()))

    def test_cachito_group_config_1(self):
        metadata = MagicMock()
        group_config = MagicMock()

        metadata.config.cachito.enabled = Missing
        group_config.cachito.enabled = True

        self.assertTrue(is_cachito_enabled(metadata, group_config, logger=MagicMock()))

    def test_cachito_group_config_2(self):
        metadata = MagicMock()
        group_config = MagicMock()

        metadata.config.cachito.enabled = Missing
        group_config.cachito.enabled = False

        self.assertFalse(is_cachito_enabled(metadata, group_config, logger=MagicMock()))

    def test_cachito_group_config_3(self):
        metadata = MagicMock()
        group_config = MagicMock()

        metadata.config.cachito.enabled = Missing
        group_config.cachito.enabled = True

        self.assertTrue(is_cachito_enabled(metadata, group_config, logger=MagicMock()))

    def test_cachito_has_source(self):
        metadata = MagicMock()
        group_config = MagicMock()

        metadata.config.cachito.enabled = Missing
        group_config.cachito.enabled = True
        metadata.has_source.return_value = False

        self.assertFalse(is_cachito_enabled(metadata, group_config, logger=MagicMock()))

    def test_cachito_has_source_2(self):
        metadata = MagicMock()
        group_config = MagicMock()

        metadata.config.cachito.enabled = Missing
        group_config.cachito.enabled = True
        metadata.has_source.return_value = True

        self.assertTrue(is_cachito_enabled(metadata, group_config, logger=MagicMock()))
