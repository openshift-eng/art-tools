import unittest
from unittest.mock import MagicMock

from artcommonlib.metadata import MetadataBase
from artcommonlib.model import Model


class TestBridgeBugMirroringEnabled(unittest.TestCase):
    """Test MetadataBase.bridge_bug_mirroring_enabled property logic."""

    def _make_meta(self, group_config: dict, comp_config: dict) -> MetadataBase:
        meta = MagicMock(spec=MetadataBase)
        meta.runtime = MagicMock()
        meta.runtime.group_config = Model(group_config)
        meta.config = Model(comp_config)
        meta.bridge_bug_mirroring_enabled = MetadataBase.bridge_bug_mirroring_enabled.fget(meta)
        return meta

    def test_group_disabled(self):
        meta = self._make_meta(
            group_config={"bridge_release": {"bug_mirroring": {"enabled": False}}},
            comp_config={},
        )
        self.assertFalse(meta.bridge_bug_mirroring_enabled)

    def test_group_missing(self):
        meta = self._make_meta(group_config={}, comp_config={})
        self.assertFalse(meta.bridge_bug_mirroring_enabled)

    def test_group_enabled_no_comp_override(self):
        meta = self._make_meta(
            group_config={"bridge_release": {"bug_mirroring": {"enabled": True}}},
            comp_config={},
        )
        self.assertTrue(meta.bridge_bug_mirroring_enabled)

    def test_group_enabled_comp_disables(self):
        meta = self._make_meta(
            group_config={"bridge_release": {"bug_mirroring": {"enabled": True}}},
            comp_config={"bridge_release": {"bug_mirroring": {"enabled": False}}},
        )
        self.assertFalse(meta.bridge_bug_mirroring_enabled)

    def test_group_enabled_comp_explicitly_enables(self):
        meta = self._make_meta(
            group_config={"bridge_release": {"bug_mirroring": {"enabled": True}}},
            comp_config={"bridge_release": {"bug_mirroring": {"enabled": True}}},
        )
        self.assertTrue(meta.bridge_bug_mirroring_enabled)

    def test_group_disabled_comp_enables_still_false(self):
        """Group-level disable is a hard gate; component override cannot override it."""
        meta = self._make_meta(
            group_config={"bridge_release": {"bug_mirroring": {"enabled": False}}},
            comp_config={"bridge_release": {"bug_mirroring": {"enabled": True}}},
        )
        self.assertFalse(meta.bridge_bug_mirroring_enabled)
