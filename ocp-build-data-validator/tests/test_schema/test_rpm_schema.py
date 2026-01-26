import unittest

from validator.schema import rpm_schema


class TestRpmSchema(unittest.TestCase):
    def test_validate_with_valid_data(self):
        valid_data = {
            "content": {},
            "name": "my-name",
            "owners": [
                "owner-a",
            ],
        }
        self.assertIsNone(rpm_schema.validate("filename", valid_data))

    def test_validate_with_invalid_data(self):
        invalid_data = {
            "content": {},
            "owners": [
                "owner-a",
            ],
        }
        self.assertEqual("Missing key: 'name'", rpm_schema.validate("filename", invalid_data))

    def test_supports_optional_targets(self):
        data = {
            "content": {},
            "name": "my-name",
            "owners": [
                "owner-a",
            ],
            "targets": [
                "rhaos-{MAJOR}.{MINOR}-rhel-7-candidate",
                "rhaos-{MAJOR}.{MINOR}-rhel-8-candidate",
            ],
        }
        self.assertIsNone(rpm_schema.validate("filename", data))
