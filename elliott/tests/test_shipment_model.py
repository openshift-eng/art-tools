import json
import re
import unittest
from pathlib import Path

from elliottlib.shipment_model import MappingDefaults, Shipment, ShipmentConfig


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
                    "live_id": 12345,
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

    def test_docs_approval_required_defaults_true_and_roundtrips(self):
        """Test that docs_approval_required defaults to True and that setting it to False is accepted and round-trips"""
        shipment_data = self._create_base_shipment_data(fbc=False)
        shipment_data.update(self._create_release_notes_data())

        # Defaults to True when unset
        shipment = Shipment(**shipment_data)
        self.assertTrue(shipment.metadata.docs_approval_required)

        # Setting it to False is accepted and survives a model_dump -> re-validate round-trip
        shipment_data["metadata"]["docs_approval_required"] = False
        shipment = Shipment(**shipment_data)
        self.assertFalse(shipment.metadata.docs_approval_required)
        roundtripped = Shipment.model_validate(shipment.model_dump())
        self.assertFalse(roundtripped.metadata.docs_approval_required)

    def test_release_tag_mapping_roundtrips_without_resolving_templates(self):
        shipment_data = self._create_base_shipment_data()
        shipment_data.update(self._create_release_notes_data())
        shipment_data['metadata']['assembly'] = '3.18.3'
        tags = ['v{{ assembly }}', 'v{{assembly}}-{{ release_timestamp }}']
        shipment_data['data']['mapping'] = {'defaults': {'tags': tags}}

        shipment = Shipment(**shipment_data)
        dumped = shipment.model_dump(exclude_none=True)
        self.assertEqual(dumped['data']['mapping'], {'defaults': {'tags': tags}})
        self.assertEqual(Shipment.model_validate(dumped).data.mapping.defaults.tags, tags)

    def test_release_tag_mapping_rejects_repository_configuration(self):
        shipment_data = self._create_base_shipment_data()
        shipment_data.update(self._create_release_notes_data())
        shipment_data['data']['mapping'] = {
            'defaults': {'tags': ['latest']},
            'components': [{'name': 'test', 'repositories': [{'url': 'example.com/test'}]}],
        }
        with self.assertRaisesRegex(ValueError, 'Extra inputs are not permitted'):
            Shipment(**shipment_data)

    def test_mapping_only_shipment_without_release_notes(self):
        shipment_data = self._create_base_shipment_data()
        shipment_data['data'] = {'mapping': {'defaults': {'tags': ['v{{ assembly }}']}}}
        shipment = Shipment(**shipment_data)
        self.assertIsNone(shipment.data.releaseNotes)
        self.assertNotIn('releaseNotes', shipment.model_dump(exclude_none=True)['data'])

    def test_openshift_mapping_does_not_replace_required_release_notes(self):
        shipment_data = self._create_base_shipment_data()
        shipment_data['metadata']['group'] = 'openshift-4.18'
        shipment_data['data'] = {'mapping': {'defaults': {'tags': ['v{{ assembly }}']}}}
        with self.assertRaisesRegex(ValueError, 'A regular shipment is expected to have data.releaseNotes defined'):
            Shipment(**shipment_data)

    def test_fbc_shipment_rejects_tag_mapping(self):
        shipment_data = self._create_base_shipment_data(fbc=True)
        shipment_data['data'] = {'mapping': {'defaults': {'tags': ['v{{ assembly }}']}}}
        with self.assertRaisesRegex(ValueError, 'FBC shipment is not expected to have data.mapping defined'):
            Shipment(**shipment_data)

    def test_assembly_tags_are_validated_when_loading_shipment(self):
        for assembly in ('stream', '', '3.18.3+metadata', 'bad/assembly', '{{ labels.version }}'):
            with self.subTest(assembly=assembly):
                shipment_data = self._create_base_shipment_data()
                shipment_data.update(self._create_release_notes_data())
                shipment_data['metadata']['assembly'] = assembly
                shipment_data['data']['mapping'] = {'defaults': {'tags': ['v{{ assembly }}']}}
                with self.assertRaises(ValueError):
                    Shipment(**shipment_data)

    def test_release_tags_resolve_only_assembly(self):
        tags = [
            'v{{ assembly }}',
            'v{{assembly}}-{{ release_timestamp }}',
            'v{{ assembly }}-{{ incrementer }}',
            '{{ labels.version }}-{{ labels.release }}',
            '{{ digest_sha }}',
        ]
        mapping = MappingDefaults(tags=tags)
        self.assertEqual(
            mapping.resolve_tags('3.18.3-rc.1'),
            [
                'v3.18.3-rc.1',
                'v3.18.3-rc.1-{{ release_timestamp }}',
                'v3.18.3-rc.1-{{ incrementer }}',
                '{{ labels.version }}-{{ labels.release }}',
                '{{ digest_sha }}',
            ],
        )
        self.assertEqual(mapping.tags, tags)

    def test_release_tags_reject_invalid_tag_syntax(self):
        for tag in ('', '.invalid', '-invalid', 'bad/tag', 'bad tag', 'v{{ assembly }', 'x' * 129):
            with self.subTest(tag=tag):
                with self.assertRaisesRegex(ValueError, 'Invalid release tag'):
                    MappingDefaults(tags=[tag]).resolve_tags('3.18.3')

    def test_release_tags_reject_empty_tag_list(self):
        with self.assertRaises(ValueError):
            MappingDefaults(tags=[])

    def test_static_release_tags_do_not_require_named_assembly(self):
        self.assertEqual(MappingDefaults(tags=['latest']).resolve_tags('stream'), ['latest'])

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
