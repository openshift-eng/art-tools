import unittest
from unittest.mock import MagicMock

from elliottlib.runtime import Runtime


class TestGetMajorMinorPatch(unittest.TestCase):
    def setUp(self):
        self.runtime = Runtime()
        self.runtime._logger = MagicMock()

    def test_valid_assembly(self):
        self.runtime.assembly = "4.10.0"

        result = self.runtime.get_major_minor_patch()
        self.assertEqual(result, ["4", "10", "0"])

    def test_invalid_assembly(self):
        self.runtime.assembly = "stream"
        with self.assertRaises(ValueError):
            self.runtime.get_major_minor_patch()

    def test_valid_assembly_with_major_min_only(self):
        self.runtime.assembly = "4.10"
        with self.assertRaises(ValueError):
            self.runtime.get_major_minor_patch()
