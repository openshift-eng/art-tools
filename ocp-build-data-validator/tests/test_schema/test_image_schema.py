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
            'from': {},
            'name': 'my-name',
            'for_payload': True,
        }
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_with_invalid_data(self):
        invalid_data = {
            'from': {},
            'name': 1234,
        }
        self.assertIn("1234 is not of type 'string'", image_schema.validate('filename', invalid_data))

    def test_validate_with_invalid_content_source_git_url(self):
        url = 'https://github.com/openshift/csi-node-driver-registrar'
        invalid_data = {
            'content': {
                'source': {
                    'git': {
                        'branch': {
                            'target': 'test',
                        },
                        'url': url,
                    },
                },
            },
            'name': '1234',
            'from': {},
        }
        self.assertIn(
            "'https://github.com/openshift/csi-node-driver-registrar' does not match",
            image_schema.validate('filename', invalid_data),
        )

    def test_validate_with_valid_content_source_git_url(self):
        url = 'git@github.com:openshift/csi-node-driver-registrar.git'
        valid_data = {
            'content': {
                'source': {
                    'git': {
                        'branch': {
                            'target': 'test',
                        },
                        'url': url,
                    },
                },
            },
            'name': '1234',
            'from': {},
            'for_payload': True,
        }
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_with_valid_subscription_label(self):
        data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'update-csv': {
                'manifests-dir': '...',
                'bundle-dir': '...',
                'registry': '...',
                'valid-subscription-label': '["foo", "bar", "baz"]',
            },
        }
        self.assertIsNone(image_schema.validate('filename', data))

    def test_validate_without_valid_subscription_label(self):
        data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'update-csv': {
                'manifests-dir': '...',
                'bundle-dir': '...',
                'registry': '...',
            },
        }
        self.assertIn(
            "is not valid",
            image_schema.validate('filename', data),
        )

    def test_validate_with_valid_konflux_cachi2_lockfile_rpms(self):
        """Test valid konflux.cachi2.lockfile.rpms configuration"""
        valid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {
                'mode': 'enabled',
                'cachi2': {
                    'enabled': True,
                    'lockfile': {'enabled': True, 'force': False, 'rpms': ['package1', 'package2', 'package3']},
                },
            },
        }
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_with_empty_konflux_cachi2_lockfile_rpms(self):
        """Test empty rpms array is valid"""
        valid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {'cachi2': {'lockfile': {'rpms': []}}},
        }
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_with_invalid_konflux_cachi2_lockfile_rpms_type(self):
        """Test invalid rpms data type (not array)"""
        invalid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {'cachi2': {'lockfile': {'rpms': 'not-an-array'}}},
        }
        self.assertIn("'not-an-array' is not of type 'array'", image_schema.validate('filename', invalid_data))

    def test_validate_with_invalid_konflux_cachi2_lockfile_rpms_items(self):
        """Test invalid rpm items (not strings)"""
        invalid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {'cachi2': {'lockfile': {'rpms': ['valid-package', 123, 'another-valid-package']}}},
        }
        self.assertIn("123 is not of type 'string'", image_schema.validate('filename', invalid_data))

    def test_validate_with_valid_konflux_cachi2_lockfile_enabled(self):
        """Test valid konflux.cachi2.lockfile.enabled configuration"""
        valid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {'cachi2': {'lockfile': {'enabled': True}}},
        }
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_with_invalid_konflux_cachi2_lockfile_enabled(self):
        """Test invalid konflux.cachi2.lockfile.enabled type"""
        invalid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {'cachi2': {'lockfile': {'enabled': 'not-a-boolean'}}},
        }
        self.assertIn("'not-a-boolean' is not of type 'boolean'", image_schema.validate('filename', invalid_data))

    def test_validate_with_valid_konflux_cachi2_lockfile_force(self):
        """Test valid konflux.cachi2.lockfile.force configuration"""
        valid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {'cachi2': {'lockfile': {'force': False}}},
        }
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_with_invalid_konflux_cachi2_lockfile_force(self):
        """Test invalid konflux.cachi2.lockfile.force type"""
        invalid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {'cachi2': {'lockfile': {'force': 'not-a-boolean'}}},
        }
        self.assertIn("'not-a-boolean' is not of type 'boolean'", image_schema.validate('filename', invalid_data))

    def test_validate_with_valid_komplux_cachi2_enabled(self):
        """Test valid konflux.cachi2.enabled configuration"""
        valid_data = {'from': {}, 'name': 'my-name', 'for_payload': True, 'konflux': {'cachi2': {'enabled': True}}}
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_with_combined_konflux_configurations(self):
        """Test valid combined konflux configurations"""
        valid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'konflux': {
                'mode': 'enabled',
                'cachito': {'mode': 'emulation'},
                'cachi2': {'enabled': True, 'lockfile': {'enabled': True, 'force': False, 'rpms': ['rpm1', 'rpm2']}},
            },
        }
        self.assertIsNone(image_schema.validate('filename', valid_data))

    def test_validate_dependents(self):
        images_dir = self.temp_dir
        with open(os.path.join(images_dir, "image1.yml"), "w") as f:
            f.write("test")
        with open(os.path.join(images_dir, "image2.yml"), "w") as f:
            f.write("test")

        valid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'dependents': ['image1', 'image2'],
        }
        self.assertIsNone(image_schema.validate('filename', valid_data, images_dir=images_dir))

        invalid_data = {
            'from': {},
            'name': 'my-name',
            'for_payload': True,
            'dependents': ['image1', 'image3'],
        }
        self.assertIn(
            "Dependent image 'image3' not found", image_schema.validate('filename', invalid_data, images_dir=images_dir)
        )
