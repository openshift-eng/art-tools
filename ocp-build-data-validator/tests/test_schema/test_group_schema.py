import unittest

from validator.schema import group_schema


class TestGroupSchema(unittest.TestCase):
    def test_validate_custom_integration_test_scenarios(self):
        valid_data = {
            "konflux": {
                "integration_test_scenarios": ["abi-qe-prow-compact"],
            }
        }
        self.assertEqual("", group_schema.validate("group.yml", valid_data))

    def test_reject_duplicate_custom_integration_test_scenarios(self):
        invalid_data = {
            "konflux": {
                "integration_test_scenarios": ["abi-qe-prow-compact", "abi-qe-prow-compact"],
            }
        }
        self.assertIn("has non-unique elements", group_schema.validate("group.yml", invalid_data))

    def test_validate_custom_integration_test_snapshot_annotations(self):
        valid_data = {
            "konflux": {
                "integration_test_snapshot_annotations": {
                    "pac.test.appstudio.openshift.io/branch": "release-4.22",
                },
            }
        }
        self.assertEqual("", group_schema.validate("group.yml", valid_data))

    def test_reject_non_string_custom_integration_test_snapshot_annotation(self):
        invalid_data = {
            "konflux": {
                "integration_test_snapshot_annotations": {
                    "pac.test.appstudio.openshift.io/branch": 422,
                },
            }
        }
        self.assertIn("is not of type 'string'", group_schema.validate("group.yml", invalid_data))

    def test_reject_controller_managed_custom_integration_test_snapshot_annotation(self):
        invalid_data = {
            "konflux": {
                "integration_test_snapshot_annotations": {
                    "test.appstudio.openshift.io/status": "[]",
                },
            }
        }
        self.assertIn("should not be valid", group_schema.validate("group.yml", invalid_data))

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

    def test_validate_reposync_requires_enabled(self):
        data_missing_enabled = {
            "repos": {
                "my-repo": {
                    "conf": {"baseurl": {"x86_64": "https://example.com/repo/"}},
                    "reposync": {"latest_only": False},
                }
            }
        }
        result = group_schema.validate("group.yml", data_missing_enabled)
        self.assertIn("'enabled' is a required property", result)

    def test_validate_reposync_with_enabled(self):
        data_with_enabled = {
            "repos": {
                "my-repo": {
                    "conf": {"baseurl": {"x86_64": "https://example.com/repo/"}},
                    "reposync": {"enabled": False},
                }
            }
        }
        self.assertEqual("", group_schema.validate("group.yml", data_with_enabled))

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

    def test_validate_with_okd_enabled_flag(self):
        valid_data = {
            "name": "openshift-4.21",
            "vars": {"MAJOR": 4, "MINOR": 21},
            "okd": {
                "enabled": True,
                "konflux": {"build_priority": 8},
            },
        }
        self.assertEqual("", group_schema.validate("group.yml", valid_data))

    def test_validate_with_invalid_okd_enabled_flag(self):
        invalid_data = {
            "name": "openshift-4.21",
            "vars": {"MAJOR": 4, "MINOR": 21},
            "okd": {
                "enabled": "yes",
            },
        }
        self.assertIn("'yes' is not of type 'boolean'", group_schema.validate("group.yml", invalid_data))
