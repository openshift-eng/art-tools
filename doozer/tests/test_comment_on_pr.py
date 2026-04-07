import unittest
from unittest.mock import MagicMock, patch

from doozerlib.comment_on_pr import CommentOnPr


def _make_comment_mock(body: str):
    """Create a mock comment object with .body attribute (PyGithub style)."""
    mock = MagicMock()
    mock.body = body
    return mock


def _make_pr_mock(number: int, html_url: str, closed_at=None):
    """Create a mock PR object with PyGithub-style attributes."""
    mock = MagicMock()
    mock.number = number
    mock.html_url = html_url
    mock.closed_at = closed_at
    return mock


class TestCommentOnPr(unittest.TestCase):
    def setUp(self):
        self.distgit_dir = "distgit_dir"
        self.commit = "commit_sha"
        self.nvr = "nvr"
        self.build_id = "build_id"
        self.distgit_name = "distgit_name"
        self.comment = (
            '**[ART PR BUILD NOTIFIER]**\n\nDistgit: distgit_name\nThis PR has been included in build '
            '[nvr](https://brewweb.engineering.redhat.com/brew/buildinfo?buildID=build_id).\n'
            'All builds following this will include this PR.'
        )

    def test_list_comments(self):
        pr_no = 1
        mock_comment = _make_comment_mock("test comment")
        mock_issue = MagicMock()
        mock_issue.get_comments.return_value = [mock_comment]
        mock_gh_repo = MagicMock()
        mock_gh_repo.get_issue.return_value = mock_issue

        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        comment_on_pr.pr = _make_pr_mock(pr_no, "https://github.com/owner/repo/pull/1")
        comment_on_pr.gh_repo = mock_gh_repo

        result = comment_on_pr.list_comments()

        mock_gh_repo.get_issue.assert_called_once_with(pr_no)
        mock_issue.get_comments.assert_called_once()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].body, "test comment")

    @patch.object(CommentOnPr, "list_comments")
    def test_check_if_comment_exist(self, mock_list_comments):
        mock_comment = _make_comment_mock("[ART PR BUILD NOTIFIER]\n\nDistgit: distgit_name")
        mock_list_comments.return_value = [mock_comment]
        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        result = comment_on_pr.check_if_comment_exist()
        self.assertTrue(result)

    @patch.object(CommentOnPr, "list_comments")
    def test_check_if_comment_exist_when_comment_does_not_exist(self, mock_list_comments):
        mock_comment = _make_comment_mock("test comment")
        mock_list_comments.return_value = [mock_comment]
        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        result = comment_on_pr.check_if_comment_exist()
        self.assertFalse(result)

    @patch.object(CommentOnPr, "check_if_comment_exist")
    def test_post_comment(self, mock_check_if_comment_exist):
        pr_no = 1
        mock_issue = MagicMock()
        mock_gh_repo = MagicMock()
        mock_gh_repo.get_issue.return_value = mock_issue

        mock_check_if_comment_exist.return_value = False
        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        comment_on_pr.pr = _make_pr_mock(pr_no, "https://github.com/owner/repo/pull/1")
        comment_on_pr.gh_repo = mock_gh_repo
        comment_on_pr.owner = "owner"
        comment_on_pr.repo = "repo"

        comment_on_pr.post_comment()

        mock_gh_repo.get_issue.assert_called_once_with(pr_no)
        mock_issue.create_comment.assert_called_once_with(body=self.comment)

    def test_get_pr_from_commit(self):
        mock_pr = _make_pr_mock(1, "test_url")
        mock_commit = MagicMock()
        mock_commit.get_pulls.return_value = [mock_pr]
        mock_gh_repo = MagicMock()
        mock_gh_repo.get_commit.return_value = mock_commit

        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        comment_on_pr.gh_repo = mock_gh_repo
        comment_on_pr.commit = self.commit

        comment_on_pr.set_pr_from_commit()

        mock_gh_repo.get_commit.assert_called_once_with(self.commit)
        mock_commit.get_pulls.assert_called_once()
        self.assertEqual(comment_on_pr.pr.html_url, "test_url")
        self.assertEqual(comment_on_pr.pr.number, 1)

    def test_multiple_prs_for_merge_commit(self):
        mock_pr1 = _make_pr_mock(1, "test_url")
        mock_pr2 = _make_pr_mock(2, "test_url_2")
        mock_commit = MagicMock()
        mock_commit.get_pulls.return_value = [mock_pr1, mock_pr2]
        mock_gh_repo = MagicMock()
        mock_gh_repo.get_commit.return_value = mock_commit

        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        comment_on_pr.gh_repo = mock_gh_repo
        comment_on_pr.commit = self.commit

        with self.assertRaises(Exception):
            comment_on_pr.set_pr_from_commit()

    @patch("doozerlib.comment_on_pr.get_github_client_for_org")
    def test_set_github_client(self, mock_get_client):
        mock_repo = MagicMock()
        mock_client = MagicMock()
        mock_client.get_repo.return_value = mock_repo
        mock_get_client.return_value = mock_client

        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        comment_on_pr.owner = "owner"
        comment_on_pr.repo = "repo"

        comment_on_pr.set_github_client()

        mock_get_client.assert_called_once_with("owner")
        mock_client.get_repo.assert_called_once_with("owner/repo")
        self.assertIsNotNone(comment_on_pr.gh_repo)
        self.assertEqual(comment_on_pr.gh_repo, mock_repo)

    @patch('doozerlib.comment_on_pr.DockerfileParser')
    def test_get_source_details(self, mock_parser):
        comment_on_pr = CommentOnPr(self.distgit_dir, self.nvr, self.build_id, self.distgit_name)
        # Mocking the labels dictionary of the DockerfileParser object
        mock_parser.return_value.labels = {
            "io.openshift.build.commit.url": "https://github.com/openshift/origin/commit/660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5",
        }

        # Calling the get_source_details method
        comment_on_pr.set_repo_details()

        # Asserting that the owner, commit, and repo attributes are set correctly
        self.assertEqual(comment_on_pr.owner, 'openshift')
        self.assertEqual(comment_on_pr.commit, '660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5')
        self.assertEqual(comment_on_pr.repo, 'origin')
