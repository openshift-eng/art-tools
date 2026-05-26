"""
Unit tests for GitLabClient.
"""

import unittest
from unittest.mock import MagicMock, patch


class TestGitLabClient(unittest.TestCase):
    @patch("artcommonlib.gitlab.gitlab.Gitlab")
    def test_list_merge_requests_delegates_to_python_gitlab(self, mock_gitlab_class):
        """
        list_merge_requests should call project.mergerequests.list
        with the given state and extra kwargs.
        """
        from artcommonlib.gitlab import GitLabClient

        mock_gl = mock_gitlab_class.return_value
        mock_project = MagicMock()
        mock_gl.projects.get.return_value = mock_project
        mock_mr1 = MagicMock()
        mock_mr2 = MagicMock()
        mock_project.mergerequests.list.return_value = [mock_mr1, mock_mr2]

        client = GitLabClient("https://gitlab.example.com", "fake-token")
        result = client.list_merge_requests("group/project", state="opened", target_branch="main")

        mock_gl.projects.get.assert_called_with("group/project")
        mock_project.mergerequests.list.assert_called_once_with(state="opened", get_all=True, target_branch="main")
        self.assertEqual(result, [mock_mr1, mock_mr2])

    @patch("artcommonlib.gitlab.gitlab.Gitlab")
    def test_list_merge_requests_defaults_to_opened(self, mock_gitlab_class):
        """
        list_merge_requests should default state to 'opened'.
        """
        from artcommonlib.gitlab import GitLabClient

        mock_gl = mock_gitlab_class.return_value
        mock_project = MagicMock()
        mock_gl.projects.get.return_value = mock_project
        mock_project.mergerequests.list.return_value = []

        client = GitLabClient("https://gitlab.example.com", "fake-token")
        client.list_merge_requests("group/project")

        mock_project.mergerequests.list.assert_called_once_with(state="opened", get_all=True)

    @patch("artcommonlib.gitlab.gitlab.Gitlab")
    def test_from_url_extracts_server_and_reads_token_from_env(self, mock_gitlab_class):
        """
        from_url should extract the server base URL and read GITLAB_TOKEN from env.
        """
        import os
        from unittest.mock import patch as mock_patch

        from artcommonlib.gitlab import GitLabClient

        with mock_patch.dict(os.environ, {"GITLAB_TOKEN": "env-token"}):
            client = GitLabClient.from_url("https://gitlab.cee.redhat.com/group/project/-/merge_requests/1")

        mock_gitlab_class.assert_called_once_with("https://gitlab.cee.redhat.com", private_token="env-token")
        self.assertIsInstance(client, GitLabClient)

    @patch("artcommonlib.gitlab.gitlab.Gitlab")
    def test_from_url_uses_explicit_token_over_env(self, mock_gitlab_class):
        """
        from_url should prefer an explicit token over the env var.
        """
        from artcommonlib.gitlab import GitLabClient

        client = GitLabClient.from_url("https://gitlab.example.com/project", gitlab_token="explicit-token")

        mock_gitlab_class.assert_called_once_with("https://gitlab.example.com", private_token="explicit-token")
        self.assertIsInstance(client, GitLabClient)


if __name__ == "__main__":
    unittest.main()
