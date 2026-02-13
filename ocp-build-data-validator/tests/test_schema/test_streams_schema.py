import unittest

from validator.schema import streams_schema


class TestStreamsSchema(unittest.TestCase):
    def test_validate_with_valid_data(self):
        valid_data = {
            "rhel": {
                "image": "openshift/ose-base:ubi8",
                "mirror": True,
                "upstream_image_base": "registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}.art",
                "transform": "rhel-8/base-repos",
                "upstream_image": "upstream_image: registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}",
            },
            "rhel-8-golang-ci-build-root": {
                "image": "not_applicable",
                "upstream_image_base": "registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.16-openshift-{MAJOR}.{MINOR}",
                "upstream_image": "upstream_image: registry.ci.openshift.org/openshift/release:rhel-8-release-golang-1.16-openshift-{MAJOR}.{MINOR}",
            },
        }
        self.assertIsNone(streams_schema.validate("filename", valid_data))

    def test_validate_with_invalid_data(self):
        invalid_data = {
            "invalid-image": {
                "image": 1234,
                "upstream_image_base": "registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}.art",
                "upstream_image": "upstream_image: registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}",
            },
            "invalid-mirror": {
                "image": "openshift/ose-base:ubi8",
                "mirror": "yes",
                "upstream_image_base": "registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}.art",
                "upstream_image": "upstream_image: registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}",
            },
            "invalid-upstream_image_base": {
                "image": "openshift/ose-base:ubi8",
                "upstream_image_base": "",
                "upstream_image": "upstream_image: registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}",
            },
            "invalid-transform": {
                "image": "openshift/ose-base:ubi8",
                "upstream_image_base": "registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}.art",
                "transform": True,
                "upstream_image": "upstream_image: registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}",
            },
            "invalid-upstream_image": {
                "image": "openshift/ose-base:ubi8",
                "upstream_image_base": "registry.ci.openshift.org/ocp/builder:rhel-8-base-openshift-{MAJOR}.{MINOR}.art",
                "upstream_image": 1234,
            },
        }

        expected_errors = {
            "invalid-image": "Key 'invalid-image' error:\nKey 'image' error:\n1234 should be instance of 'str'",
            "invalid-mirror": "Key 'invalid-mirror' error:\nKey 'mirror' error:\n'yes' should be instance of 'bool'",
            "invalid-upstream_image_base": "Key 'invalid-upstream_image_base' error:\nKey 'upstream_image_base' error:\nlen('') should evaluate to True",
            "invalid-transform": "Key 'invalid-transform' error:\nKey 'transform' error:\nTrue should be instance of 'str'",
            "invalid-upstream_image": "Key 'invalid-upstream_image' error:\nKey 'upstream_image' error:\n1234 should be instance of 'str'",
        }

        for k, v in invalid_data.items():
            self.assertEqual(expected_errors[k], streams_schema.validate("filename", {k: v}))
