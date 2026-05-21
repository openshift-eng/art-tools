import os
import unittest
from unittest.mock import Mock, patch

from elliottlib import shipment_utils


class TestShipmentUtils(unittest.TestCase):
    """Test cases for shipment_utils module"""

    def setUp(self):
        """Setup test fixtures"""
        self.mock_gitlab_token = "test-token"
        self.test_mr_url = "https://gitlab.com/test-project/-/merge_requests/123"

        # Sample YAML content for testing
        self.sample_yaml_content = """
shipment:
  metadata:
    product: "test-product"
    application: "test-app"
    group: "test-group"
    assembly: "test-assembly"
    fbc: false
  environments:
    stage:
      releasePlan: "stage-plan"
    prod:
      releasePlan: "prod-plan"
  snapshot:
    nvrs:
      - "test-rpm-1.0.0-1.el8"
      - "test-container-v1.0.0-202312010000.p0.git12345"
    spec:
      application: "test-app"
      components:
        - name: "test-rpm"
          source:
            git:
              url: "https://github.com/test-rpm.git"
              revision: "abc123"
          containerImage: "foo"
        - name: "test-container"
          source:
            git:
              url: "https://github.com/test-container.git"
              revision: "def456"
          containerImage: "bar"
  data:
    releaseNotes:
      type: "RHBA"
      synopsis: "Test synopsis"
      topic: "Test topic"
      description: "Test description"
      solution: "Test solution"
      cves: []
"""

        # Mock file diff for testing
        self.mock_file_diff = {'new_path': 'rpm.yaml', 'old_path': None}

        # Mock GitLab objects
        self.mock_project = Mock()
        self.mock_mr = Mock()
        self.mock_source_project = Mock()
        self.mock_diff_info = Mock()
        self.mock_diff = Mock()
        self.mock_file_content = Mock()

    @patch('artcommonlib.gitlab.gitlab.Gitlab')
    @patch.dict(os.environ, {'GITLAB_TOKEN': 'test-token'})
    def test_get_shipment_configs_by_kind_multiple_kinds(self, mock_gitlab_class):
        """Test retrieval of multiple shipment configs by different kinds"""
        # Setup mocks
        mock_gitlab = mock_gitlab_class.return_value
        mock_gitlab.projects.get.side_effect = [self.mock_project, self.mock_source_project]

        self.mock_project.mergerequests.get.return_value = self.mock_mr
        self.mock_mr.source_project_id = "source-project-id"
        self.mock_mr.source_branch = "test-branch"

        # Mock diff data with multiple files
        self.mock_diff_info.id = "diff-id"
        self.mock_mr.diffs.list.return_value = [self.mock_diff_info]
        self.mock_mr.diffs.get.return_value = self.mock_diff

        mock_rpm_diff = {'new_path': 'rpm.yaml', 'old_path': None}
        mock_image_diff = {'new_path': 'image.yml', 'old_path': None}
        self.mock_diff.diffs = [mock_rpm_diff, mock_image_diff]

        # Mock file content
        self.mock_file_content.decode.return_value.decode.return_value = self.sample_yaml_content
        self.mock_source_project.files.get.return_value = self.mock_file_content

        # Execute test
        result = shipment_utils.get_shipment_configs_from_mr(self.test_mr_url, ("rpm", "image"))

        # Assertions
        self.assertEqual(len(result), 2)
        self.assertIn('rpm', result)
        self.assertIn('image', result)

    @patch('artcommonlib.gitlab.gitlab.Gitlab')
    @patch.dict(os.environ, {'GITLAB_TOKEN': 'test-token'})
    def test_get_shipment_configs_by_kind_no_matching_files(self, mock_gitlab_class):
        """Test when no files match the requested kinds"""
        # Setup mocks
        mock_gitlab = mock_gitlab_class.return_value
        mock_gitlab.projects.get.side_effect = [self.mock_project, self.mock_source_project]

        self.mock_project.mergerequests.get.return_value = self.mock_mr
        self.mock_mr.source_project_id = "source-project-id"
        self.mock_mr.source_branch = "test-branch"

        # Mock diff data with no matching files
        self.mock_diff_info.id = "diff-id"
        self.mock_mr.diffs.list.return_value = [self.mock_diff_info]
        self.mock_mr.diffs.get.return_value = self.mock_diff

        mock_non_matching_diff = {'new_path': 'unrelated.txt', 'old_path': None}
        self.mock_diff.diffs = [mock_non_matching_diff]

        # Execute test
        result = shipment_utils.get_shipment_configs_from_mr(self.test_mr_url, ("rpm", "image"))

        # Assertions
        self.assertEqual(result, {})

    @patch('artcommonlib.gitlab.gitlab.Gitlab')
    @patch.dict(os.environ, {'GITLAB_TOKEN': 'test-token'})
    def test_get_shipment_configs_by_kind_duplicate_kind(self, mock_gitlab_class):
        """Test error handling when multiple configs found for same kind"""
        # Setup mocks
        mock_gitlab = mock_gitlab_class.return_value
        mock_gitlab.projects.get.side_effect = [self.mock_project, self.mock_source_project]

        self.mock_project.mergerequests.get.return_value = self.mock_mr
        self.mock_mr.source_project_id = "source-project-id"
        self.mock_mr.source_branch = "test-branch"

        # Mock diff data with duplicate kinds
        self.mock_diff_info.id = "diff-id"
        self.mock_mr.diffs.list.return_value = [self.mock_diff_info]
        self.mock_mr.diffs.get.return_value = self.mock_diff

        mock_rpm_diff1 = {'new_path': 'rpm.yaml', 'old_path': None}
        mock_rpm_diff2 = {'new_path': 'rpm-extra.yaml', 'old_path': None}
        self.mock_diff.diffs = [mock_rpm_diff1, mock_rpm_diff2]

        # Mock file content
        self.mock_file_content.decode.return_value.decode.return_value = self.sample_yaml_content
        self.mock_source_project.files.get.return_value = self.mock_file_content

        # Execute test and expect error
        with self.assertRaises(ValueError) as context:
            shipment_utils.get_shipment_configs_from_mr(self.test_mr_url, ("rpm",))

        self.assertIn("Multiple shipment configs found for rpm", str(context.exception))

    @patch('elliottlib.shipment_utils.get_shipment_configs_from_mr')
    def test_get_builds_from_mr_success(self, mock_get_configs):
        """Test successful build extraction from merge request"""
        # Setup mock shipment config
        mock_shipment_config_rpm = Mock()
        mock_shipment_config_rpm.shipment.snapshot.nvrs = ["test-rpm-1.0.0-1.el8"]
        mock_shipment_config_image = Mock()
        mock_shipment_config_image.shipment.snapshot.nvrs = ["test-container-v1.0.0-202312010000.p0.git12345"]
        mock_get_configs.return_value = {'rpm': mock_shipment_config_rpm, 'image': mock_shipment_config_image}

        # Execute test
        result = shipment_utils.get_builds_from_mr(self.test_mr_url)

        # Assertions
        self.assertEqual(
            result, {'rpm': ['test-rpm-1.0.0-1.el8'], 'image': ['test-container-v1.0.0-202312010000.p0.git12345']}
        )

        # Verify the underlying function was called correctly
        mock_get_configs.assert_called_once_with(self.test_mr_url)

    def test_default_kinds_parameter(self):
        """Test that default kinds parameter works correctly"""
        with patch('artcommonlib.gitlab.gitlab.Gitlab') as mock_gitlab_class:
            os.environ['GITLAB_TOKEN'] = self.mock_gitlab_token
            mock_gitlab = mock_gitlab_class.return_value
            mock_gitlab.projects.get.side_effect = [self.mock_project, self.mock_source_project]

            self.mock_project.mergerequests.get.return_value = self.mock_mr
            self.mock_mr.source_project_id = "source-project-id"
            self.mock_mr.source_branch = "test-branch"

            # Mock diff data
            self.mock_diff_info.id = "diff-id"
            self.mock_mr.diffs.list.return_value = [self.mock_diff_info]
            self.mock_mr.diffs.get.return_value = self.mock_diff
            self.mock_diff.diffs = []

            # Call without specifying kinds to test default
            result = shipment_utils.get_shipment_configs_from_mr(self.test_mr_url)

            # Should not raise an error and return empty dict since no files match
            self.assertEqual(result, {})

    @patch('artcommonlib.gitlab.gitlab.Gitlab')
    @patch.dict(os.environ, {'GITLAB_TOKEN': 'test-token'})
    def test_get_shipment_configs_by_kind_invalid_yaml(self, mock_gitlab_class):
        """Test error handling when YAML content is invalid"""
        # Setup mocks
        mock_gitlab = mock_gitlab_class.return_value
        mock_gitlab.projects.get.side_effect = [self.mock_project, self.mock_source_project]

        self.mock_project.mergerequests.get.return_value = self.mock_mr
        self.mock_mr.source_project_id = "source-project-id"
        self.mock_mr.source_branch = "test-branch"

        # Mock diff data
        self.mock_diff_info.id = "diff-id"
        self.mock_mr.diffs.list.return_value = [self.mock_diff_info]
        self.mock_mr.diffs.get.return_value = self.mock_diff
        self.mock_diff.diffs = [self.mock_file_diff]

        # Mock invalid YAML content
        invalid_yaml = "invalid: yaml: content: [unclosed"
        self.mock_file_content.decode.return_value.decode.return_value = invalid_yaml
        self.mock_source_project.files.get.return_value = self.mock_file_content

        # Execute test and expect error
        with self.assertRaises(Exception):
            shipment_utils.get_shipment_configs_from_mr(self.test_mr_url, ("rpm",))

    @patch('artcommonlib.gitlab.gitlab.Gitlab')
    @patch.dict(os.environ, {'GITLAB_TOKEN': 'test-token'})
    def test_get_shipment_configs_by_kind_non_yaml_files(self, mock_gitlab_class):
        """Test that non-YAML files are properly ignored"""
        # Setup mocks
        mock_gitlab = mock_gitlab_class.return_value
        mock_gitlab.projects.get.side_effect = [self.mock_project, self.mock_source_project]

        self.mock_project.mergerequests.get.return_value = self.mock_mr
        self.mock_mr.source_project_id = "source-project-id"
        self.mock_mr.source_branch = "test-branch"

        # Mock diff data with non-YAML files
        self.mock_diff_info.id = "diff-id"
        self.mock_mr.diffs.list.return_value = [self.mock_diff_info]
        self.mock_mr.diffs.get.return_value = self.mock_diff

        mock_txt_diff = {'new_path': 'rpm.txt', 'old_path': None}
        mock_json_diff = {'new_path': 'image.json', 'old_path': None}
        mock_py_diff = {'new_path': 'script.py', 'old_path': None}
        self.mock_diff.diffs = [mock_txt_diff, mock_json_diff, mock_py_diff]

        # Execute test
        result = shipment_utils.get_shipment_configs_from_mr(self.test_mr_url, ("rpm", "image"))

        # Assertions - should return empty dict since no YAML files
        self.assertEqual(result, {})

    @patch('artcommonlib.gitlab.gitlab.Gitlab')
    @patch.dict(os.environ, {'GITLAB_TOKEN': 'test-token'})
    def test_get_shipment_configs_by_kind_all_default_kinds(self, mock_gitlab_class):
        """Test with files matching all default kinds"""
        # Setup mocks
        mock_gitlab = mock_gitlab_class.return_value
        mock_gitlab.projects.get.side_effect = [self.mock_project, self.mock_source_project]

        self.mock_project.mergerequests.get.return_value = self.mock_mr
        self.mock_mr.source_project_id = "source-project-id"
        self.mock_mr.source_branch = "test-branch"

        # Mock diff data with all default kinds
        self.mock_diff_info.id = "diff-id"
        self.mock_mr.diffs.list.return_value = [self.mock_diff_info]
        self.mock_mr.diffs.get.return_value = self.mock_diff

        mock_diffs = [
            {'new_path': 'fbc.yaml', 'old_path': None},
            {'new_path': 'image.yml', 'old_path': None},
            {'new_path': 'extras.yaml', 'old_path': None},
            {'new_path': 'microshift-bootc.yml', 'old_path': None},
            {'new_path': 'metadata.yaml', 'old_path': None},
        ]
        self.mock_diff.diffs = mock_diffs

        # Mock file content
        self.mock_file_content.decode.return_value.decode.return_value = self.sample_yaml_content
        self.mock_source_project.files.get.return_value = self.mock_file_content

        # Execute test with default kinds
        result = shipment_utils.get_shipment_configs_from_mr(self.test_mr_url)

        # Assertions
        expected_kinds = {"fbc", "image", "extras", "microshift-bootc", "metadata"}
        self.assertEqual(set(result.keys()), expected_kinds)


@patch.dict(os.environ, {"GITLAB_TOKEN": "fake-token"})
class TestGetBugIdsFromOpenShipmentMrs(unittest.TestCase):
    """Test cases for get_bug_ids_from_open_shipment_mrs"""

    @patch("elliottlib.shipment_utils.get_shipment_configs_from_mr")
    @patch("elliottlib.shipment_utils.GitLabClient")
    def test_filters_bugs_from_matching_group(self, mock_gitlab_cls, mock_get_configs):
        """Bugs from open MRs matching the group should be returned."""
        mock_client = Mock()
        mock_gitlab_cls.from_url.return_value = mock_client

        mock_mr = Mock()
        mock_mr.web_url = "https://gitlab.example.com/project/-/merge_requests/1"
        mock_client.list_merge_requests.return_value = [mock_mr]

        mock_shipment = Mock()
        mock_shipment.shipment.metadata.group = "openshift-4.18"
        mock_shipment.shipment.metadata.assembly = "4.18.39"
        mock_shipment.shipment.metadata.fbc = False
        mock_shipment.shipment.data.releaseNotes.issues.fixed = [
            Mock(id="OCPBUGS-100", source="issues.redhat.com"),
            Mock(id="OCPBUGS-200", source="issues.redhat.com"),
        ]
        mock_get_configs.return_value = {"image": mock_shipment}

        result = shipment_utils.get_bug_ids_from_open_shipment_mrs(
            shipment_data_url="https://gitlab.example.com/project",
            group="openshift-4.18",
        )

        self.assertEqual(result, {"OCPBUGS-100", "OCPBUGS-200"})

    @patch("elliottlib.shipment_utils.get_shipment_configs_from_mr")
    @patch("elliottlib.shipment_utils.GitLabClient")
    def test_excludes_current_assembly(self, mock_gitlab_cls, mock_get_configs):
        """Bugs from the current assembly's MR should be excluded."""
        mock_client = Mock()
        mock_gitlab_cls.from_url.return_value = mock_client

        mock_mr = Mock()
        mock_mr.web_url = "https://gitlab.example.com/project/-/merge_requests/1"
        mock_client.list_merge_requests.return_value = [mock_mr]

        mock_shipment = Mock()
        mock_shipment.shipment.metadata.group = "openshift-4.18"
        mock_shipment.shipment.metadata.assembly = "4.18.40"
        mock_shipment.shipment.metadata.fbc = False
        mock_shipment.shipment.data.releaseNotes.issues.fixed = [
            Mock(id="OCPBUGS-300", source="issues.redhat.com"),
        ]
        mock_get_configs.return_value = {"image": mock_shipment}

        result = shipment_utils.get_bug_ids_from_open_shipment_mrs(
            shipment_data_url="https://gitlab.example.com/project",
            group="openshift-4.18",
            exclude_assembly="4.18.40",
        )

        self.assertEqual(result, set())

    @patch("elliottlib.shipment_utils.get_shipment_configs_from_mr")
    @patch("elliottlib.shipment_utils.GitLabClient")
    def test_skips_different_group(self, mock_gitlab_cls, mock_get_configs):
        """Bugs from MRs for a different OCP group should not be returned."""
        mock_client = Mock()
        mock_gitlab_cls.from_url.return_value = mock_client

        mock_mr = Mock()
        mock_mr.web_url = "https://gitlab.example.com/project/-/merge_requests/1"
        mock_client.list_merge_requests.return_value = [mock_mr]

        mock_shipment = Mock()
        mock_shipment.shipment.metadata.group = "openshift-4.17"
        mock_shipment.shipment.metadata.assembly = "4.17.10"
        mock_shipment.shipment.metadata.fbc = False
        mock_shipment.shipment.data.releaseNotes.issues.fixed = [
            Mock(id="OCPBUGS-999", source="issues.redhat.com"),
        ]
        mock_get_configs.return_value = {"image": mock_shipment}

        result = shipment_utils.get_bug_ids_from_open_shipment_mrs(
            shipment_data_url="https://gitlab.example.com/project",
            group="openshift-4.18",
        )

        self.assertEqual(result, set())

    @patch.dict(os.environ, {"GITLAB_TOKEN": ""})
    def test_raises_when_no_gitlab_token(self):
        """When GITLAB_TOKEN env var is empty, GitLabClient raises ValueError."""
        with self.assertRaises(ValueError):
            shipment_utils.get_bug_ids_from_open_shipment_mrs(
                shipment_data_url="https://gitlab.example.com/project",
                group="openshift-4.18",
            )

    @patch("elliottlib.shipment_utils.get_shipment_configs_from_mr")
    @patch("elliottlib.shipment_utils.GitLabClient")
    def test_skips_fbc_shipments(self, mock_gitlab_cls, mock_get_configs):
        """FBC shipments (no releaseNotes) should be skipped without error."""
        mock_client = Mock()
        mock_gitlab_cls.from_url.return_value = mock_client

        mock_mr = Mock()
        mock_mr.web_url = "https://gitlab.example.com/project/-/merge_requests/1"
        mock_client.list_merge_requests.return_value = [mock_mr]

        mock_fbc_shipment = Mock()
        mock_fbc_shipment.shipment.metadata.group = "openshift-4.18"
        mock_fbc_shipment.shipment.metadata.assembly = "4.18.39"
        mock_fbc_shipment.shipment.metadata.fbc = True
        mock_fbc_shipment.shipment.data = None

        mock_image_shipment = Mock()
        mock_image_shipment.shipment.metadata.group = "openshift-4.18"
        mock_image_shipment.shipment.metadata.assembly = "4.18.39"
        mock_image_shipment.shipment.metadata.fbc = False
        mock_image_shipment.shipment.data.releaseNotes.issues.fixed = [
            Mock(id="OCPBUGS-500", source="issues.redhat.com"),
        ]

        mock_get_configs.return_value = {
            "fbc": mock_fbc_shipment,
            "image": mock_image_shipment,
        }

        result = shipment_utils.get_bug_ids_from_open_shipment_mrs(
            shipment_data_url="https://gitlab.example.com/project",
            group="openshift-4.18",
        )

        self.assertEqual(result, {"OCPBUGS-500"})

    @patch("elliottlib.shipment_utils.get_shipment_configs_from_mr")
    @patch("elliottlib.shipment_utils.GitLabClient")
    def test_handles_mr_parse_error_gracefully(self, mock_gitlab_cls, mock_get_configs):
        """If parsing an MR fails, skip it and continue with others."""
        mock_client = Mock()
        mock_gitlab_cls.from_url.return_value = mock_client

        mock_mr_bad = Mock()
        mock_mr_bad.web_url = "https://gitlab.example.com/project/-/merge_requests/1"
        mock_mr_good = Mock()
        mock_mr_good.web_url = "https://gitlab.example.com/project/-/merge_requests/2"
        mock_client.list_merge_requests.return_value = [mock_mr_bad, mock_mr_good]

        mock_shipment = Mock()
        mock_shipment.shipment.metadata.group = "openshift-4.18"
        mock_shipment.shipment.metadata.assembly = "4.18.39"
        mock_shipment.shipment.metadata.fbc = False
        mock_shipment.shipment.data.releaseNotes.issues.fixed = [
            Mock(id="OCPBUGS-700", source="issues.redhat.com"),
        ]

        mock_get_configs.side_effect = [
            ValueError("YAML parse error"),
            {"image": mock_shipment},
        ]

        result = shipment_utils.get_bug_ids_from_open_shipment_mrs(
            shipment_data_url="https://gitlab.example.com/project",
            group="openshift-4.18",
        )

        self.assertEqual(result, {"OCPBUGS-700"})

    @patch("elliottlib.shipment_utils.get_shipment_configs_from_mr")
    @patch("elliottlib.shipment_utils.GitLabClient")
    def test_shipment_with_no_issues_fixed(self, mock_gitlab_cls, mock_get_configs):
        """Shipments with no issues.fixed should be handled gracefully."""
        mock_client = Mock()
        mock_gitlab_cls.from_url.return_value = mock_client

        mock_mr = Mock()
        mock_mr.web_url = "https://gitlab.example.com/project/-/merge_requests/1"
        mock_client.list_merge_requests.return_value = [mock_mr]

        mock_shipment = Mock()
        mock_shipment.shipment.metadata.group = "openshift-4.18"
        mock_shipment.shipment.metadata.assembly = "4.18.39"
        mock_shipment.shipment.metadata.fbc = False
        mock_shipment.shipment.data.releaseNotes.issues = None
        mock_get_configs.return_value = {"image": mock_shipment}

        result = shipment_utils.get_bug_ids_from_open_shipment_mrs(
            shipment_data_url="https://gitlab.example.com/project",
            group="openshift-4.18",
        )

        self.assertEqual(result, set())


if __name__ == '__main__':
    unittest.main()
