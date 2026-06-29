import unittest

from validator.schema import group_schema


class TestGroupSchema(unittest.TestCase):
    def test_validate_with_valid_bridge_release_config(self):
        valid_data = {
            "name": "openshift-4.23",
            "vars": {"MAJOR": 4, "MINOR": 23},
            "bridge_release": {
                "basis_group": "openshift-5.0",
                "bug_mirroring": {"enabled": True},
            },
        }
        self.assertEqual("", group_schema.validate("group.yml", valid_data))

    def test_validate_with_invalid_bridge_release_config(self):
        invalid_data = {
            "name": "openshift-4.23",
            "vars": {"MAJOR": 4, "MINOR": 23},
            "bridge_release": {
                "basis_group": "openshift-5.0",
                "bug_mirroring": {"enabled": "yes"},
            },
        }
        self.assertIn("'yes' is not of type 'boolean'", group_schema.validate("group.yml", invalid_data))

    def test_validate_with_mismatched_bridge_release_basis_group(self):
        invalid_data = {
            "name": "openshift-4.23",
            "vars": {"MAJOR": 4, "MINOR": 23},
            "bridge_release": {
                "basis_group": "openshift-5.1",
                "bug_mirroring": {"enabled": True},
            },
        }
        self.assertIn("must be 'openshift-5.0'", group_schema.validate("group.yml", invalid_data))

    def test_validate_with_templated_group_name_and_valid_bridge_release(self):
        # Real group.yml files always use an unresolved "{MAJOR}.{MINOR}" template for
        # `name`, substituted at runtime by doozer/elliott. The validator must resolve
        # it using `vars` before comparing against `bridge_release.basis_group`.
        valid_data = {
            "name": "openshift-{MAJOR}.{MINOR}",
            "vars": {"MAJOR": 4, "MINOR": 23},
            "bridge_release": {
                "basis_group": "openshift-5.0",
                "bug_mirroring": {"enabled": True},
            },
        }
        self.assertEqual("", group_schema.validate("group.yml", valid_data))

    def test_validate_with_templated_group_name_and_mismatched_bridge_release(self):
        invalid_data = {
            "name": "openshift-{MAJOR}.{MINOR}",
            "vars": {"MAJOR": 4, "MINOR": 23},
            "bridge_release": {
                "basis_group": "openshift-5.1",
                "bug_mirroring": {"enabled": True},
            },
        }
        self.assertIn("must be 'openshift-5.0'", group_schema.validate("group.yml", invalid_data))
