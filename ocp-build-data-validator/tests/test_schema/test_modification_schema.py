import unittest
import schema
from validator.schema.modification_schema import modification


class TestModificationSchema(unittest.TestCase):
    def test_modification_with_valid_data(self):
        input = {
            "action": "add",
            "source": "https://example.org/some-path/gating.yaml",
            "path": "gating.yaml",
            "overwriting": True,
        }
        self.assertTrue(modification(input))

    def test_modification_with_invalid_data(self):
        input = {
            "action": "unknown",
        }
        with self.assertRaises(schema.SchemaError):
            modification(input)
