import unittest

from validator.schema import image_schema


class TestImageSchema(unittest.TestCase):
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
