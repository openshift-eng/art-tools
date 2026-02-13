import os
import shutil
import tempfile
import unittest

from validator.schema import image_schema


class TestImageSchema(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_validate_with_valid_data(self):
        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_with_invalid_data(self):
        invalid_data = {
            "from": {},
            "name": 1234,
        }
        self.assertIn("1234 is not of type 'string'", image_schema.validate("filename", invalid_data))

    def test_validate_with_invalid_content_source_git_url(self):
        url = "https://github.com/openshift/csi-node-driver-registrar"
        invalid_data = {
            "content": {
                "source": {
                    "git": {
                        "branch": {
                            "target": "test",
                        },
                        "url": url,
                    },
                },
            },
            "name": "1234",
            "from": {},
        }
        self.assertIn(
            "'https://github.com/openshift/csi-node-driver-registrar' does not match",
            image_schema.validate("filename", invalid_data),
        )

    def test_validate_with_valid_content_source_git_url(self):
        url = "git@github.com:openshift/csi-node-driver-registrar.git"
        valid_data = {
            "content": {
                "source": {
                    "git": {
                        "branch": {
                            "target": "test",
                        },
                        "url": url,
                    },
                },
            },
            "name": "1234",
            "from": {},
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_with_valid_subscription_label(self):
        data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "update-csv": {
                "manifests-dir": "...",
                "bundle-dir": "...",
                "registry": "...",
                "valid-subscription-label": '["foo", "bar", "baz"]',
            },
        }
        self.assertIsNone(image_schema.validate("filename", data))

    def test_validate_without_valid_subscription_label(self):
        data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "update-csv": {
                "manifests-dir": "...",
                "bundle-dir": "...",
                "registry": "...",
            },
        }
        self.assertIn(
            "is not valid",
            image_schema.validate("filename", data),
        )

    def test_validate_with_valid_konflux_cachi2_lockfile_rpms(self):
        """Test valid konflux.cachi2.lockfile.rpms configuration"""
        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "konflux": {
                "mode": "enabled",
                "cachi2": {
                    "enabled": True,
                    "lockfile": {"enabled": True, "rpms": ["package1", "package2", "package3"]},
                },
            },
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_with_empty_konflux_cachi2_lockfile_rpms(self):
        """Test empty rpms array is valid"""
        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "konflux": {"cachi2": {"lockfile": {"rpms": []}}},
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_with_invalid_konflux_cachi2_lockfile_rpms_type(self):
        """Test invalid rpms data type (not array)"""
        invalid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "konflux": {"cachi2": {"lockfile": {"rpms": "not-an-array"}}},
        }
        self.assertIn("'not-an-array' is not of type 'array'", image_schema.validate("filename", invalid_data))

    def test_validate_with_invalid_konflux_cachi2_lockfile_rpms_items(self):
        """Test invalid rpm items (not strings)"""
        invalid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "konflux": {"cachi2": {"lockfile": {"rpms": ["valid-package", 123, "another-valid-package"]}}},
        }
        self.assertIn("123 is not of type 'string'", image_schema.validate("filename", invalid_data))

    def test_validate_with_valid_konflux_cachi2_lockfile_enabled(self):
        """Test valid konflux.cachi2.lockfile.enabled configuration"""
        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "konflux": {"cachi2": {"lockfile": {"enabled": True}}},
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_with_invalid_konflux_cachi2_lockfile_enabled(self):
        """Test invalid konflux.cachi2.lockfile.enabled type"""
        invalid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "konflux": {"cachi2": {"lockfile": {"enabled": "not-a-boolean"}}},
        }
        self.assertIn("'not-a-boolean' is not of type 'boolean'", image_schema.validate("filename", invalid_data))

    def test_validate_with_valid_komplux_cachi2_enabled(self):
        """Test valid konflux.cachi2.enabled configuration"""
        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "konflux": {"cachi2": {"enabled": True}},
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_with_combined_konflux_configurations(self):
        """Test valid combined konflux configurations"""
        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "konflux": {
                "mode": "enabled",
                "cachito": {"mode": "emulation"},
                "cachi2": {"enabled": True, "lockfile": {"enabled": True, "rpms": ["rpm1", "rpm2"]}},
            },
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_dependents(self):
        images_dir = self.temp_dir
        with open(os.path.join(images_dir, "image1.yml"), "w") as f:
            f.write("test")
        with open(os.path.join(images_dir, "image2.yml"), "w") as f:
            f.write("test")

        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "dependents": ["image1", "image2"],
        }
        self.assertIsNone(image_schema.validate("filename", valid_data, images_dir=images_dir))

        invalid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "dependents": ["image1", "image3"],
        }
        self.assertIn(
            "Dependent image 'image3' not found", image_schema.validate("filename", invalid_data, images_dir=images_dir)
        )

    def test_validate_with_valid_konflux_cachi2_artifact_lockfile(self):
        """Test valid konflux.cachi2.artifact_lockfile configuration"""
        valid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo", "bar"]},
            "konflux": {
                "cachi2": {
                    "artifact_lockfile": {
                        "enabled": True,
                        "resources": ["https://example.com/cert1.pem", "https://example.com/cert2.pem"],
                        "path": ".",
                    }
                }
            },
        }
        self.assertIsNone(image_schema.validate("filename", valid_data))

    def test_validate_with_invalid_konflux_cachi2_artifact_lockfile_enabled(self):
        """Test invalid artifact_lockfile.enabled type"""
        invalid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "konflux": {"cachi2": {"artifact_lockfile": {"enabled": "not-a-boolean"}}},
        }
        self.assertIn("'not-a-boolean' is not of type 'boolean'", image_schema.validate("filename", invalid_data))

    def test_validate_with_invalid_konflux_cachi2_artifact_lockfile_resources(self):
        """Test invalid artifact_lockfile.resources type"""
        invalid_data = {
            "from": {},
            "name": "my-name",
            "for_payload": True,
            "konflux": {"cachi2": {"artifact_lockfile": {"resources": "not-an-array"}}},
        }
        self.assertIn("'not-an-array' is not of type 'array'", image_schema.validate("filename", invalid_data))

    def test_validate_enabled_repos_with_new_style_repos(self):
        """
        Test validation of enabled_repos with new-style repo definitions (repos/ directory).
        """
        images_dir = self.temp_dir
        ocp_build_data_dir = os.path.dirname(images_dir)
        repos_dir = os.path.join(ocp_build_data_dir, "repos")
        os.makedirs(repos_dir, exist_ok=True)

        # Create valid repo definitions in new style
        with open(os.path.join(repos_dir, "rhel-9-appstream-rpms.yml"), "w") as f:
            f.write("- name: rhel-9-appstream-rpms\n  conf: {}\n")
        with open(os.path.join(repos_dir, "rhel-9-baseos-rpms.yml"), "w") as f:
            f.write("- name: rhel-9-baseos-rpms\n  conf: {}\n")

        # Test with valid repo references
        valid_data = {
            "from": {},
            "name": "test-image",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo"]},
            "enabled_repos": ["rhel-9-appstream-rpms", "rhel-9-baseos-rpms"],
        }
        self.assertIsNone(image_schema.validate("filename", valid_data, images_dir=images_dir))

        # Test with invalid repo reference
        invalid_data = {
            "from": {},
            "name": "test-image",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo"]},
            "enabled_repos": ["rhel-9-appstream-rpms", "rhel-9-invalid-repo"],
        }
        error = image_schema.validate("filename", invalid_data, images_dir=images_dir)
        self.assertIsNotNone(error)
        self.assertIn("Repository 'rhel-9-invalid-repo' not found", error)

    def test_validate_enabled_repos_with_old_style_repos(self):
        """
        Test validation of enabled_repos with old-style repo definitions (group.yml repos section).
        """
        images_dir = self.temp_dir
        ocp_build_data_dir = os.path.dirname(images_dir)

        # Create group.yml with old-style repos
        with open(os.path.join(ocp_build_data_dir, "group.yml"), "w") as f:
            f.write(
                "repos:\n"
                "  rhel-8-server-rpms:\n"
                "    conf:\n"
                "      baseurl: http://example.com\n"
                "  rhel-8-fast-datapath-rpms:\n"
                "    conf:\n"
                "      baseurl: http://example.com\n"
            )

        # Test with valid repo references
        valid_data = {
            "from": {},
            "name": "test-image",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo"]},
            "enabled_repos": ["rhel-8-server-rpms", "rhel-8-fast-datapath-rpms"],
        }
        self.assertIsNone(image_schema.validate("filename", valid_data, images_dir=images_dir))

        # Test with invalid repo reference
        invalid_data = {
            "from": {},
            "name": "test-image",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo"]},
            "enabled_repos": ["rhel-8-server-rpms", "rhel-8-nonexistent-rpms"],
        }
        error = image_schema.validate("filename", invalid_data, images_dir=images_dir)
        self.assertIsNotNone(error)
        self.assertIn("Repository 'rhel-8-nonexistent-rpms' not found", error)

    def test_validate_enabled_repos_with_mixed_style_repos(self):
        """
        Test validation of enabled_repos with both old and new style repo definitions.
        """
        images_dir = self.temp_dir
        ocp_build_data_dir = os.path.dirname(images_dir)
        repos_dir = os.path.join(ocp_build_data_dir, "repos")
        os.makedirs(repos_dir, exist_ok=True)

        # Create new-style repos
        with open(os.path.join(repos_dir, "rhel-9-appstream-rpms.yml"), "w") as f:
            f.write("- name: rhel-9-appstream-rpms\n  conf: {}\n")

        # Create old-style repos in group.yml
        with open(os.path.join(ocp_build_data_dir, "group.yml"), "w") as f:
            f.write("repos:\n  rhel-8-server-rpms:\n    conf:\n      baseurl: http://example.com\n")

        # Test with repos from both sources
        valid_data = {
            "from": {},
            "name": "test-image",
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["foo"]},
            "enabled_repos": ["rhel-9-appstream-rpms", "rhel-8-server-rpms"],
        }
        self.assertIsNone(image_schema.validate("filename", valid_data, images_dir=images_dir))
