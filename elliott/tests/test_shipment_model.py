import json
import re
import unittest
from pathlib import Path

from elliottlib.shipment_model import Shipment, ShipmentConfig


class TestShipmentModel(unittest.TestCase):
    def test_shipment_schema_updated(self):
        """Test that the generated schema matches the existing schema file in the ocp-build-data-validator repo"""
        # Generate the schema the same way as the make task "make gen-shipment-schema"
        generated_schema = ShipmentConfig.model_json_schema(mode="validation")
        generated_json = json.dumps(generated_schema, indent=2)

        # Read the existing schema file
        # Get the path relative to the project root
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent  # Go up to art-tools directory
        schema_file_path = (
            project_root / "ocp-build-data-validator" / "validator" / "json_schemas" / "shipment.schema.json"
        )

        with open(schema_file_path, 'r') as f:
            existing_schema_content = f.read()

        # Normalize both strings to handle timestamp differences and trailing newlines
        def normalize_schema(schema_str):
            # Remove timestamp from comment (replace with fixed string)
            normalized = re.sub(
                r'"Schema generated on \d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC from elliottlib\.shipment_model"',
                '"Schema generated on YYYY-MM-DD HH:MM UTC from elliottlib.shipment_model"',
                schema_str,
            )
            # Strip trailing whitespace/newlines
            return normalized.strip()

        normalized_generated = normalize_schema(generated_json)
        normalized_existing = normalize_schema(existing_schema_content)

        # Compare the normalized schemas
        self.assertEqual(
            normalized_generated,
            normalized_existing,
            "Generated schema does not match the existing schema file. "
            "Run 'make gen-shipment-schema' to update the schema file.",
        )

    def _create_base_shipment_data(self, fbc=False, application="test-app"):
        """Helper method to create base shipment data for tests"""
        return {
            "metadata": {
                "product": "test-product",
                "application": application,
                "group": "test-group",
                "assembly": "test-assembly",
                "fbc": fbc,
            },
            "environments": {"stage": {"releasePlan": "stage-plan"}, "prod": {"releasePlan": "prod-plan"}},
        }

    def _create_release_notes_data(self):
        """Helper method to create release notes data for tests"""
        return {
            "data": {
                "releaseNotes": {
                    "type": "RHEA",
                    "live_id": "12345",
                    "synopsis": "Test synopsis",
                    "topic": "Test topic",
                    "description": "Test description",
                    "solution": "Test solution",
                }
            }
        }

    def _create_snapshot_data(self, application="test-app"):
        """Helper method to create snapshot data for tests"""
        return {
            "snapshot": {
                "spec": {
                    "application": application,
                    "components": [
                        {
                            "name": "test-component",
                            "containerImage": "test-image:latest",
                            "source": {"git": {"url": "https://github.com/test/repo", "revision": "main"}},
                        }
                    ],
                },
                "nvrs": ["test-nvr-1.0.0-1.el9"],
            }
        }

    def test_fbc_shipment_validation_happy_path(self):
        """Test that FBC shipment without data.releaseNotes passes validation"""
        shipment_data = self._create_base_shipment_data(fbc=True)
        # FBC shipment should not have data.releaseNotes - this should pass
        shipment = Shipment(**shipment_data)
        self.assertTrue(shipment.metadata.fbc)
        self.assertIsNone(shipment.data)

    def test_fbc_shipment_validation_unhappy_path(self):
        """Test that FBC shipment with data.releaseNotes fails validation"""
        shipment_data = self._create_base_shipment_data(fbc=True)
        shipment_data.update(self._create_release_notes_data())

        with self.assertRaises(ValueError) as context:
            Shipment(**shipment_data)

        self.assertIn("FBC shipment is not expected to have data.releaseNotes defined", str(context.exception))

    def test_regular_shipment_validation_happy_path(self):
        """Test that regular shipment with data.releaseNotes passes validation"""
        shipment_data = self._create_base_shipment_data(fbc=False)
        shipment_data.update(self._create_release_notes_data())

        shipment = Shipment(**shipment_data)
        self.assertFalse(shipment.metadata.fbc)
        self.assertIsNotNone(shipment.data)
        self.assertIsNotNone(shipment.data.releaseNotes)

    def test_regular_shipment_validation_unhappy_path(self):
        """Test that regular OpenShift shipment without data.releaseNotes fails validation"""
        # Use an OpenShift group to trigger the validation
        shipment_data = self._create_base_shipment_data(fbc=False)
        shipment_data["metadata"]["group"] = "openshift-4.17"
        # Regular OpenShift shipment should have data.releaseNotes - this should fail

        with self.assertRaises(ValueError) as context:
            Shipment(**shipment_data)

        self.assertIn("A regular shipment is expected to have data.releaseNotes defined", str(context.exception))

    def test_non_openshift_shipment_validation_happy_path(self):
        """Test that non-OpenShift shipment without data.releaseNotes passes validation"""
        # Non-OpenShift products should not require data.releaseNotes
        shipment_data = self._create_base_shipment_data(fbc=False)
        shipment_data["metadata"]["group"] = "oadp-1.3"  # Non-OpenShift group

        shipment = Shipment(**shipment_data)
        self.assertFalse(shipment.metadata.fbc)
        self.assertEqual(shipment.metadata.group, "oadp-1.3")
        # Should pass validation without releaseNotes

    def test_snapshot_application_match_happy_path(self):
        """Test that shipment with matching snapshot application passes validation"""
        shipment_data = self._create_base_shipment_data(fbc=True, application="test-app")
        shipment_data.update(self._create_snapshot_data(application="test-app"))

        shipment = Shipment(**shipment_data)
        self.assertEqual(shipment.metadata.application, shipment.snapshot.spec.application)

    def test_snapshot_application_match_unhappy_path(self):
        """Test that shipment with non-matching snapshot application fails validation"""
        shipment_data = self._create_base_shipment_data(fbc=True, application="test-app")
        shipment_data.update(self._create_snapshot_data(application="different-app"))

        with self.assertRaises(ValueError) as context:
            Shipment(**shipment_data)

        self.assertIn(
            "shipment.snapshot.spec.application=different-app is expected to be the same as shipment.metadata.application=test-app",
            str(context.exception),
        )


if __name__ == "__main__":
    unittest.main()
