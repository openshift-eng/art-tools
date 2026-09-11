import unittest
from unittest.mock import MagicMock

from artcommonlib.assembly import AssemblyTypes
from elliottlib.runtime import Runtime
from flexmock import flexmock


class TestGetMajorMinorPatch(unittest.TestCase):
    def setUp(self):
        self.runtime = Runtime()
        self.runtime._logger = MagicMock()

    def test_valid_assembly(self):
        self.runtime.assembly = "4.10.1"

        result = self.runtime.get_major_minor_patch()
        self.assertEqual(result, ["4", "10", "1"])

    def test_invalid_assembly(self):
        self.runtime.assembly = "stream"
        with self.assertRaises(ValueError):
            self.runtime.get_major_minor_patch()

    def test_valid_assembly_with_major_min_only(self):
        self.runtime.assembly = "4.10"
        with self.assertRaises(ValueError):
            self.runtime.get_major_minor_patch()

    def test_valid_assembly_preview(self):
        self.runtime.assembly = "ec.2"
        self.runtime.assembly_type = AssemblyTypes.PREVIEW
        self.runtime.group_config = flexmock(vars=flexmock(MAJOR="4", MINOR="10"))
        result = self.runtime.get_major_minor_patch()
        self.assertEqual(result, ("4", "10", 0))

    def test_valid_assembly_candidate(self):
        self.runtime.assembly = "rc.2"
        self.runtime.assembly_type = AssemblyTypes.CANDIDATE
        self.runtime.group_config = flexmock(vars=flexmock(MAJOR="4", MINOR="10"))
        result = self.runtime.get_major_minor_patch()
        self.assertEqual(result, ("4", "10", 0))

    def test_valid_assembly_custom(self):
        self.runtime.assembly = "custom"
        self.runtime.assembly_type = AssemblyTypes.CUSTOM
        with self.assertRaises(ValueError):
            self.runtime.get_major_minor_patch()


class TestGetExtraVars(unittest.TestCase):
    """Elliott Runtime.get_extra_vars() always returns empty (no --var support)."""

    def test_returns_empty_dict(self):
        rt = Runtime()
        rt._logger = MagicMock()
        self.assertEqual(rt.get_extra_vars(), {})


class TestGetReplaceVarsRespectGroupConfig(unittest.TestCase):
    """Verify that metadata-loading paths use get_replace_vars(group_config)
    so an assembly's runtime_assembly override is honoured."""

    def test_group_config_runtime_assembly_override_preserved(self):
        """When group_config.vars.runtime_assembly == 'stream' (set by an
        assembly override in releases.yml), get_replace_vars must return
        runtime_assembly='stream' — NOT self.assembly ('art23398')."""
        from artcommonlib.model import Model

        rt = Runtime()
        rt._logger = MagicMock()
        rt.assembly = "art23398"
        rt.group_config = Model(dict_to_model={"vars": {"runtime_assembly": "stream", "MAJOR": 5}})

        result = rt.get_replace_vars(rt.group_config)
        # The assembly override ('stream') must survive, not be overwritten by self.assembly
        self.assertEqual(result["runtime_assembly"], "stream")
        # Other vars from group_config are also present
        self.assertEqual(result["MAJOR"], 5)
