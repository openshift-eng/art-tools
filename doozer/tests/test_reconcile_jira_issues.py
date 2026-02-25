#!/usr/bin/env python3

"""
Unit tests for the reconcile_jira_issues function in images_streams.py.
These tests focus on exercising the JIRA customfield update logic
that runs when a new JIRA issue is created for a reconciliation PR.

Run with:
    uv run pytest --verbose --color=yes doozer/tests/test_reconcile_jira_issues.py
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from doozerlib.cli.images_streams import reconcile_jira_issues


def _build_mocks(
    major: int = 4,
    minor: int = 18,
    project: str = "OCPBUGS",
    component: str = "Release",
    target_release: str = "4.18.z",
    release_notes_text: object = None,
    release_notes_type: object = None,
    existing_issue: bool = False,
):
    """
    Build the mock objects needed to invoke reconcile_jira_issues.

    Arg(s):
        major (int): OCP major version.
        minor (int): OCP minor version.
        project (str): JIRA project key.
        component (str): JIRA component name.
        target_release (str): Target version string from bug.yml.
        release_notes_text: Value for customfield_12317313 on the created issue.
        release_notes_type: Value for customfield_12320850 on the created issue.
        existing_issue (bool): If True, simulate that a JIRA issue already exists.

    Return Value(s):
        tuple: (runtime, pr_map, mock_issue, mock_jira_client)
    """
    # Runtime mock
    runtime = MagicMock()
    runtime.get_major_minor_fields.return_value = (major, minor)

    # gitdata.load_data returns bug.yml with target_release
    bug_data = MagicMock()
    bug_data.data = {'target_release': [f'{major}.{minor}.0', target_release]}
    runtime.gitdata.load_data.return_value = bug_data

    # Logger
    runtime.logger = MagicMock()

    # Image metadata
    image_meta = MagicMock()
    image_meta.get_jira_info.return_value = (project, component)
    image_meta.get_component_name.return_value = 'ose-metallb-operator'
    image_meta.name = 'ose-metallb-operator'
    runtime.image_map = {'ose-metallb-operator': image_meta}

    # JIRA client
    mock_jira_client = MagicMock()
    runtime.build_jira_client.return_value = mock_jira_client

    # project_components and projects
    mock_project = MagicMock()
    mock_project.name = project
    mock_jira_client.projects.return_value = [mock_project]

    mock_component = MagicMock()
    mock_component.name = component
    mock_jira_client.project_components.return_value = [mock_component]

    # Created issue mock
    mock_issue = MagicMock()
    mock_issue.key = 'OCPBUGS-99999'
    mock_issue.fields = MagicMock()
    mock_issue.fields.summary = f"ART requests updates to {major}.{minor} image ose-metallb-operator"

    # Set the release notes custom fields on the created issue
    mock_issue.fields.customfield_12317313 = release_notes_text
    mock_issue.fields.customfield_12320850 = release_notes_type

    mock_jira_client.create_issue.return_value = mock_issue

    # search_issues: return existing issue or empty list
    if existing_issue:
        mock_jira_client.search_issues.return_value = [mock_issue]
    else:
        mock_jira_client.search_issues.return_value = []

    # PR mock
    mock_pr = MagicMock()
    mock_pr.html_url = 'https://github.com/openshift/metallb-operator/pull/42'
    mock_pr.title = 'ART reconciliation PR'
    mock_pr.get_issue_comments.return_value = []
    pr_map = {'ose-metallb-operator': (mock_pr, None)}

    return runtime, pr_map, mock_issue, mock_jira_client


class TestReconcileJiraIssuesCustomFields(unittest.TestCase):
    """
    Tests for the customfield update logic in reconcile_jira_issues.
    """

    @patch('doozerlib.cli.images_streams.connect_issue_with_pr')
    def test_target_version_customfield_is_set(self, mock_connect):
        """
        Verify that customfield_12319940 (Target Version) is updated
        on the newly created JIRA issue.
        """
        runtime, pr_map, mock_issue, mock_jira_client = _build_mocks(
            target_release='4.18.z',
        )

        reconcile_jira_issues(runtime, pr_map, dry_run=False)

        # Verify create_issue was called
        mock_jira_client.create_issue.assert_called_once()

        # Verify issue.update was called with Target Version customfield
        calls = mock_issue.update.call_args_list
        target_version_call = calls[0]
        updated_fields = target_version_call.kwargs.get('fields') or target_version_call[1].get('fields')
        self.assertIn('customfield_12319940', updated_fields)
        self.assertEqual(updated_fields['customfield_12319940'], [{'name': '4.18.z'}])

    @patch('doozerlib.cli.images_streams.connect_issue_with_pr')
    def test_release_notes_customfields_set_when_none(self, mock_connect):
        """
        When both Release Notes Text and Release Notes Type are None
        on the created issue, verify they get set to default values.
        """
        runtime, pr_map, mock_issue, mock_jira_client = _build_mocks(
            release_notes_text=None,
            release_notes_type=None,
        )

        reconcile_jira_issues(runtime, pr_map, dry_run=False)

        # There should be two update calls:
        # 1. Target Version
        # 2. Release Notes Text + Release Notes Type
        calls = mock_issue.update.call_args_list
        self.assertGreaterEqual(len(calls), 2, f"Expected at least 2 update calls, got {len(calls)}")

        release_notes_call = calls[1]
        updated_fields = release_notes_call.kwargs.get('fields') or release_notes_call[1].get('fields')
        self.assertEqual(updated_fields['customfield_12317313'], 'N/A')
        self.assertEqual(updated_fields['customfield_12320850'], {'value': 'Release Note Not Required'})

    @patch('doozerlib.cli.images_streams.connect_issue_with_pr')
    def test_release_notes_customfields_not_set_when_already_populated(self, mock_connect):
        """
        When Release Notes fields already have values, verify they
        are NOT overwritten.
        """
        runtime, pr_map, mock_issue, mock_jira_client = _build_mocks(
            release_notes_text='Some release note text',
            release_notes_type=MagicMock(value='Release Note Required'),
        )

        reconcile_jira_issues(runtime, pr_map, dry_run=False)

        # Only the Target Version update should have been called
        calls = mock_issue.update.call_args_list
        self.assertEqual(len(calls), 1, f"Expected 1 update call (Target Version only), got {len(calls)}")

        updated_fields = calls[0].kwargs.get('fields') or calls[0][1].get('fields')
        self.assertIn('customfield_12319940', updated_fields)
        self.assertNotIn('customfield_12317313', updated_fields)

    @patch('doozerlib.cli.images_streams.connect_issue_with_pr')
    def test_dry_run_skips_issue_creation(self, mock_connect):
        """
        In dry_run mode, no JIRA issue should be created and no
        customfields should be updated.
        """
        runtime, pr_map, mock_issue, mock_jira_client = _build_mocks()

        reconcile_jira_issues(runtime, pr_map, dry_run=True)

        mock_jira_client.create_issue.assert_not_called()
        mock_issue.update.assert_not_called()

    @patch('doozerlib.cli.images_streams.connect_issue_with_pr')
    def test_existing_issue_skips_creation(self, mock_connect):
        """
        When a JIRA issue already exists for the PR, no new issue
        should be created.
        """
        runtime, pr_map, mock_issue, mock_jira_client = _build_mocks(
            existing_issue=True,
        )

        reconcile_jira_issues(runtime, pr_map, dry_run=False)

        mock_jira_client.create_issue.assert_not_called()
        mock_issue.update.assert_not_called()

    @patch('doozerlib.cli.images_streams.connect_issue_with_pr')
    def test_target_version_update_failure_is_handled(self, mock_connect):
        """
        When the Target Version update fails, the error should be
        logged but not propagated — the function should continue.
        """
        runtime, pr_map, mock_issue, mock_jira_client = _build_mocks()

        # Make the first update call raise an exception
        mock_issue.update.side_effect = [Exception("JIRA field not found"), None]

        # Should not raise
        reconcile_jira_issues(runtime, pr_map, dry_run=False)

        runtime.logger.error.assert_called_once()
        error_msg = runtime.logger.error.call_args[0][0]
        self.assertIn('Target Version', error_msg)

    @patch('doozerlib.cli.images_streams.connect_issue_with_pr')
    def test_version_below_4_16_skips_entirely(self, mock_connect):
        """
        For groups below 4.16, reconcile_jira_issues should return
        immediately without doing anything.
        """
        runtime, pr_map, mock_issue, mock_jira_client = _build_mocks(
            major=4, minor=15,
        )

        reconcile_jira_issues(runtime, pr_map, dry_run=False)

        runtime.build_jira_client.assert_not_called()
        mock_jira_client.create_issue.assert_not_called()


if __name__ == '__main__':
    unittest.main()
