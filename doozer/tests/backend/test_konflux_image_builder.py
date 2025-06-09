import unittest
from unittest.mock import MagicMock, patch

from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder

from artcommon.artcommonlib.model import Missing


class TestIsLockfileGenerationEnabled(unittest.TestCase):
    def test_lockfile_enabled_metadata_override_true(self):
        self.logger = MagicMock()
        metadata = MagicMock()
        metadata.config.konflux.cachi2.lockfile.enabled = True
        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = False
        with patch.object(KonfluxImageBuilder, "_is_cachi2_enabled", return_value=True):
            result = KonfluxImageBuilder.is_lockfile_generation_enabled(metadata, self.logger)
        self.assertTrue(result)
        self.logger.info.assert_any_call("Lockfile generation set from metadata config True")

    def test_lockfile_enabled_metadata_override_false(self):
        self.logger = MagicMock()
        metadata = MagicMock()
        metadata.config.konflux.cachi2.lockfile.enabled = False
        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = True
        with patch.object(KonfluxImageBuilder, "_is_cachi2_enabled", return_value=True):
            result = KonfluxImageBuilder.is_lockfile_generation_enabled(metadata, self.logger)
        self.assertFalse(result)

    def test_lockfile_enabled_group_override_true(self):
        self.logger = MagicMock()
        metadata = MagicMock()
        metadata.config.konflux.cachi2.lockfile.enabled = None
        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = True
        with patch.object(KonfluxImageBuilder, "_is_cachi2_enabled", return_value=True):
            result = KonfluxImageBuilder.is_lockfile_generation_enabled(metadata, self.logger)
        self.assertTrue(result)
        self.logger.info.assert_any_call("Lockfile generation set from group config True")

    def test_lockfile_enabled_group_override_false(self):
        self.logger = MagicMock()
        metadata = MagicMock()
        metadata.config.konflux.cachi2.lockfile.enabled = None
        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = False
        with patch.object(KonfluxImageBuilder, "_is_cachi2_enabled", return_value=True):
            result = KonfluxImageBuilder.is_lockfile_generation_enabled(metadata, self.logger)
        self.assertFalse(result)

    def test_lockfile_enabled_missing_overrides(self):
        self.logger = MagicMock()
        metadata = MagicMock()
        metadata.config.konflux.cachi2.lockfile.enabled = Missing
        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = Missing
        with patch.object(KonfluxImageBuilder, "_is_cachi2_enabled", return_value=True):
            result = KonfluxImageBuilder.is_lockfile_generation_enabled(metadata, self.logger)
        self.assertTrue(result)

    def test_lockfile_enabled_cachi2_disabled(self):
        self.logger = MagicMock()
        metadata = MagicMock()
        metadata.config.konflux.cachi2.lockfile.enabled = True
        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = True
        with patch.object(KonfluxImageBuilder, "_is_cachi2_enabled", return_value=False):
            result = KonfluxImageBuilder.is_lockfile_generation_enabled(metadata, self.logger)
        self.assertFalse(result)

    def test_lockfile_enabled_all_missing(self):
        self.logger = MagicMock()
        metadata = MagicMock()
        metadata.config.konflux.cachi2.lockfile.enabled = Missing
        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = Missing
        with patch.object(KonfluxImageBuilder, "_is_cachi2_enabled", return_value=Missing):
            result = KonfluxImageBuilder.is_lockfile_generation_enabled(metadata, self.logger)
        self.assertFalse(result)
