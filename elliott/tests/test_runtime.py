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
