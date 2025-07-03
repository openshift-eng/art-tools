import json
import re
import unittest
from pathlib import Path

from elliottlib.shipment_model import ShipmentConfig


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


if __name__ == "__main__":
    unittest.main()
