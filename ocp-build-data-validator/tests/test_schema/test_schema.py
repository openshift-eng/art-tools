import unittest

from flexmock import flexmock
from validator.schema import image_schema, rpm_schema, validate


class TestSchema(unittest.TestCase):
    def test_route_to_image_schema_validation(self):
        flexmock(image_schema).should_receive("validate").once()
        validate("images/my-image.yml", "data")

    def test_route_to_rpm_schema_validation(self):
        flexmock(rpm_schema).should_receive("validate").once()
        validate("rpms/my-rpm.yml", "data")

    def test_unsupported_schema(self):
        err = validate("unknown/my-file.yml", "data")
        self.assertEqual(
            err,
            (
                "Could not determine a schema\n"
                "Supported schemas: image, rpm\n"
                "Make sure the file is placed in either dir "
                '"images" or "rpms"'
            ),
        )
