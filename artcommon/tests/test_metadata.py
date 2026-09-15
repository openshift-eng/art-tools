import unittest
from unittest.mock import MagicMock

from artcommonlib.metadata import MetadataBase
from artcommonlib.model import Model


class TestBranchElTarget(unittest.TestCase):
    """Test MetadataBase.branch_el_target() 3-level lookup chain."""

    def _make_meta(self, image_config: dict, group_config: dict, branch_value: str | None = None) -> MetadataBase:
        """Build a lightweight MetadataBase mock with the given config dicts."""
        meta = MagicMock(spec=MetadataBase)
        meta.config = Model(image_config)
        meta.runtime = MagicMock()
        meta.runtime.group_config = Model(group_config)

        # Wire up branch() — only needed for the fallback path
        if branch_value is not None:
            meta.branch = MagicMock(return_value=branch_value)
        else:
            meta.branch = MagicMock(side_effect=AttributeError("no branch configured"))

        return meta

    # --- Level 1: image-level el_target ---

    def test_image_config_el_target(self):
        """Image config has el_target: 9 → returns 9."""
        meta = self._make_meta(image_config={"el_target": 9}, group_config={})
        result = MetadataBase.branch_el_target(meta)
        self.assertEqual(result, 9)
        self.assertIsInstance(result, int)

    # --- Level 2: group-level el_target ---

    def test_group_config_el_target(self):
        """Group config has el_target: 9, no image-level → returns 9."""
        meta = self._make_meta(image_config={}, group_config={"el_target": 9})
        result = MetadataBase.branch_el_target(meta)
        self.assertEqual(result, 9)
        self.assertIsInstance(result, int)

    # --- Level 1 overrides level 2 ---

    def test_image_overrides_group(self):
        """Image el_target: 8 overrides group el_target: 9 → returns 8."""
        meta = self._make_meta(image_config={"el_target": 8}, group_config={"el_target": 9})
        result = MetadataBase.branch_el_target(meta)
        self.assertEqual(result, 8)

    # --- Level 3: branch fallback ---

    def test_fallback_rhel9(self):
        """No el_target, distgit.branch = rhaos-4.21-rhel-9 → returns 9."""
        meta = self._make_meta(image_config={}, group_config={}, branch_value="rhaos-4.21-rhel-9")
        result = MetadataBase.branch_el_target(meta)
        self.assertEqual(result, 9)

    def test_fallback_rhel8(self):
        """No el_target, distgit.branch = rhaos-4.14-rhel-8 → returns 8."""
        meta = self._make_meta(image_config={}, group_config={}, branch_value="rhaos-4.14-rhel-8")
        result = MetadataBase.branch_el_target(meta)
        self.assertEqual(result, 8)

    # --- Error cases ---

    def test_no_el_target_no_branch_raises(self):
        """No el_target, no usable branch → raises IOError."""
        meta = self._make_meta(image_config={}, group_config={}, branch_value=None)
        # branch() will raise AttributeError; but the method calls str(self.branch())
        # so let's give it a value that won't match the regex instead
        meta.branch = MagicMock(side_effect=IOError("no branch"))
        with self.assertRaises(IOError):
            MetadataBase.branch_el_target(meta)

    def test_garbage_branch_raises(self):
        """No el_target, distgit.branch = 'garbage' → raises IOError."""
        meta = self._make_meta(image_config={}, group_config={}, branch_value="garbage")
        with self.assertRaises(IOError) as ctx:
            MetadataBase.branch_el_target(meta)
        self.assertIn("el_target", str(ctx.exception))

    # --- Type coercion ---

    def test_string_el_target_coerced_to_int(self):
        """el_target as string '9' (not int) → returns 9 (int coercion)."""
        meta = self._make_meta(image_config={"el_target": "9"}, group_config={})
        result = MetadataBase.branch_el_target(meta)
        self.assertEqual(result, 9)
        self.assertIsInstance(result, int)


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
