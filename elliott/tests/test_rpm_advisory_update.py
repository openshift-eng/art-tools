"""
Test cases for RPM_ADVISORY placeholder update fix
Tests that RPM_ADVISORY gets updated when converting RHBA to RHSA
"""

import unittest
from unittest.mock import Mock, patch
from elliottlib.cli.attach_cve_flaws_cli import AttachCveFlaws
from elliottlib.bzutil import BugzillaBug
from elliottlib.shipment_model import ReleaseNotes, Issues, Issue


class TestRPMAdvisoryUpdate(unittest.TestCase):
    """Test that RPM_ADVISORY placeholder is updated when converting to RHSA"""

    def setUp(self):
        """Setup mock runtime"""
        self.mock_runtime = Mock()
        self.mock_runtime.get_errata_config.return_value = {}
        self.mock_runtime.get_major_minor_patch.return_value = (4, 20, 12)
        self.mock_runtime.get_major_minor.return_value = (4, 20)
        self.mock_runtime.group_config.advisories = {"rpm": 157799}
        self.mock_runtime.build_system = 'brew'
        self.mock_runtime.image_metas.return_value = []  # Empty image metas for Konflux test
        self.mock_runtime.group = 'openshift-4.20'

    @patch("elliottlib.cli.attach_cve_flaws_cli.get_errata_live_id")
    @patch("elliottlib.cli.attach_cve_flaws_cli.get_advisory_boilerplate")
    def test_brew_flow_updates_rpm_advisory(self, mock_boilerplate, mock_live_id):
        """Test Brew flow: get_updated_advisory_rhsa() updates RPM_ADVISORY"""

        # Simulate: First call returns RHBA, second returns RHSA
        mock_live_id.side_effect = ["RHBA-2026:0331", "RHSA-2026:0338"]

        mock_boilerplate.return_value = {
            'security_reviewer': 'reviewer@redhat.com',
            'synopsis': 'Security update',
            'topic': 'Security fixes',
            'solution': 'See {RPM_ADVISORY}',
            'description': 'Details: {RPM_ADVISORY}',
        }

        # Create pipeline
        pipeline = AttachCveFlaws(
            self.mock_runtime,
            advisory_id=0,
            into_default_advisories=False,
            default_advisory_type="image",
            output="json",
            noop=False,
        )

        # Initial value: RHBA
        self.assertEqual(pipeline._replace_vars["RPM_ADVISORY"], "RHBA-2026:0331")

        # Mock advisory (RHBA type)
        advisory = Mock(
            errata_type="RHBA",
            cve_names="",
            security_impact="Low",
            topic="Bug fixes",
            update=Mock()
        )

        # Flaw bugs
        flaw_bugs = [Mock(alias=['CVE-2024-1'], severity='important', summary='CVE-2024-1 test')]

        # Call the method
        pipeline.get_updated_advisory_rhsa(mock_boilerplate.return_value, advisory, flaw_bugs)

        # Verify: RPM_ADVISORY was updated to RHSA
        self.assertEqual(pipeline._replace_vars["RPM_ADVISORY"], "RHSA-2026:0338")
        self.assertEqual(mock_live_id.call_count, 2)

    @patch("elliottlib.cli.attach_cve_flaws_cli.get_konflux_component_by_component")
    @patch("elliottlib.cli.attach_cve_flaws_cli.get_errata_live_id")
    @patch("elliottlib.cli.attach_cve_flaws_cli.get_advisory_boilerplate")
    def test_konflux_flow_updates_rpm_advisory(self, mock_boilerplate, mock_live_id, mock_get_konflux_component):
        """Test Konflux flow: update_release_notes() updates RPM_ADVISORY"""

        # Simulate: First call returns RHBA, second returns RHSA
        mock_live_id.side_effect = ["RHBA-2026:0331", "RHSA-2026:0338"]

        mock_boilerplate.return_value = {
            'synopsis': 'Security update',
            'topic': 'Security fixes',
            'solution': 'See {RPM_ADVISORY}',
            'description': 'Details: {RPM_ADVISORY}',
        }

        # Mock Konflux component mapping
        mock_get_konflux_component.return_value = 'test-konflux-component'

        # Create pipeline
        pipeline = AttachCveFlaws(
            self.mock_runtime,
            advisory_id=0,
            into_default_advisories=False,
            default_advisory_type="image",
            output="json",
            noop=False,
        )

        # Initial value: RHBA
        self.assertEqual(pipeline._replace_vars["RPM_ADVISORY"], "RHBA-2026:0331")

        # Release notes (RHBA type)
        release_notes = ReleaseNotes(
            type='RHBA',
            synopsis='Bug fix',
            topic='Fixes',
            solution='Standard',
            description='Fixes',
            issues=Issues(fixed=[Issue(id='OCPBUGS-1', source='jira')])
        )

        # Flaw bugs and trackers
        flaw_bugs = [
            BugzillaBug(Mock(id=1, alias=['CVE-2024-1'], summary='CVE-2024-1 test', severity='important'))
        ]
        tracker_bugs = [BugzillaBug(Mock(id=1, whiteboard='component: test'))]
        tracker_flaws = {1: [1]}

        # Call the method
        pipeline.update_release_notes(
            release_notes,
            flaw_bugs,
            tracker_bugs,
            tracker_flaws,
            attached_components={'test-konflux-component'}
        )

        # Verify: RPM_ADVISORY was updated to RHSA
        self.assertEqual(pipeline._replace_vars["RPM_ADVISORY"], "RHSA-2026:0338")
        self.assertEqual(mock_live_id.call_count, 2)


if __name__ == '__main__':
    unittest.main()
