from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from artcommonlib import constants, exectools
from artcommonlib.model import Missing, Model
from doozerlib.source_resolver import SourceResolution, SourceResolver
from flexmock import flexmock


class SourceResolverTestCase(TestCase):
    @staticmethod
    def create_source_resolver():
        return SourceResolver(
            sources_base_dir="/path/to/sources",
            cache_dir="/path/to/cache",
            group_config=Model(),
        )

    @patch("doozerlib.source_resolver.get_github_git_auth_env", return_value={"GIT_ASKPASS": "/tmp/askpass.sh"})
    @patch("doozerlib.source_resolver.art_util.ensure_github_https_url", side_effect=lambda u: u)
    def test_get_remote_branch_ref(self, mock_ensure, mock_auth):
        sr = self.create_source_resolver()
        flexmock(exectools).should_receive("cmd_assert").once().and_return("spam", "")
        res = sr._get_remote_branch_ref("giturl", "branch")
        self.assertEqual(res, "spam")

        flexmock(exectools).should_receive("cmd_assert").once().and_return("", "")
        self.assertIsNone(sr._get_remote_branch_ref("giturl", "branch"))

        flexmock(exectools).should_receive("cmd_assert").once().and_raise(Exception("whatever"))
        self.assertIsNone(sr._get_remote_branch_ref("giturl", "branch"))

    def test_detect_remote_source_branch(self):
        sr = self.create_source_resolver()
        source_details = dict(
            url='some_git_repo',
            branch=dict(
                target='main_branch',
                fallback='fallback_branch',
                stage='stage_branch',
            ),
        )

        # got a hit on the first branch
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").once().and_return("spam")
        self.assertEqual(("main_branch", "spam"), sr.detect_remote_source_branch(source_details, stage=False))

        # got a hit on the fallback branch
        (flexmock(SourceResolver).should_receive("_get_remote_branch_ref").and_return(None).and_return("eggs"))
        self.assertEqual(("fallback_branch", "eggs"), sr.detect_remote_source_branch(source_details, stage=False))

        # no target or fallback branch
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").and_return(None)
        with self.assertRaises(IOError):
            sr.detect_remote_source_branch(source_details, stage=False)

        # request stage branch, get it
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").once().and_return("spam")
        self.assertEqual(("stage_branch", "spam"), sr.detect_remote_source_branch(source_details, stage=True))

        # request stage branch, not there
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").once().and_return(None)
        with self.assertRaises(IOError):
            sr.detect_remote_source_branch(source_details, stage=True)

    def test_get_source_dir(self):
        source = Mock(source_path="/path/to/sources/foo")
        metadata = Mock()
        metadata.config.content.source.path = Missing
        self.assertEqual(Path("/path/to/sources/foo"), SourceResolver.get_source_dir(source, metadata, check=False))

        metadata.config.content.source.path = "subdir"
        self.assertEqual(
            Path("/path/to/sources/foo/subdir"), SourceResolver.get_source_dir(source, metadata, check=False)
        )

    # SourceResolution tests
    def test_source_resolution_with_pull_url(self):
        """Test SourceResolution with separate pull URL."""
        resolution = SourceResolution(
            source_path="/path/to/source",
            url="https://push.example.com/repo.git",
            branch="main",
            https_url="https://push.example.com/repo.git",
            commit_hash="deadbeef",
            committer_date=datetime.fromtimestamp(0, timezone.utc),
            latest_tag="v1.0.0",
            has_public_upstream=False,
            public_upstream_url="https://push.example.com/repo.git",
            public_upstream_branch="main",
            pull_url="https://pull.example.com/repo.git",
        )

        self.assertEqual(resolution.url, "https://push.example.com/repo.git")
        self.assertEqual(resolution.pull_url, "https://pull.example.com/repo.git")

    def test_source_resolution_without_pull_url(self):
        """Test SourceResolution without pull URL defaults to None."""
        resolution = SourceResolution(
            source_path="/path/to/source",
            url="https://push.example.com/repo.git",
            branch="main",
            https_url="https://push.example.com/repo.git",
            commit_hash="deadbeef",
            committer_date=datetime.fromtimestamp(0, timezone.utc),
            latest_tag="v1.0.0",
            has_public_upstream=False,
            public_upstream_url="https://push.example.com/repo.git",
            public_upstream_branch="main",
        )

        self.assertEqual(resolution.url, "https://push.example.com/repo.git")
        self.assertEqual(resolution.pull_url, None)

    @patch("artcommonlib.util.convert_remote_git_to_https")
    def test_https_pull_url_property_with_pull_url(self, mock_convert):
        """Test https_pull_url property with separate pull URL."""
        mock_convert.return_value = "https://converted.pull.example.com/repo.git"

        resolution = SourceResolution(
            source_path="/path/to/source",
            url="https://push.example.com/repo.git",
            branch="main",
            https_url="https://push.example.com/repo.git",
            commit_hash="deadbeef",
            committer_date=datetime.fromtimestamp(0, timezone.utc),
            latest_tag="v1.0.0",
            has_public_upstream=False,
            public_upstream_url="https://push.example.com/repo.git",
            public_upstream_branch="main",
            pull_url="git@pull.example.com:repo.git",
        )

        result = resolution.https_pull_url
        mock_convert.assert_called_with("git@pull.example.com:repo.git")
        self.assertEqual(result, "https://converted.pull.example.com/repo.git")

    def test_https_pull_url_property_without_pull_url(self):
        """Test https_pull_url property without pull URL returns https_url."""
        resolution = SourceResolution(
            source_path="/path/to/source",
            url="https://push.example.com/repo.git",
            branch="main",
            https_url="https://push.example.com/repo.git",
            commit_hash="deadbeef",
            committer_date=datetime.fromtimestamp(0, timezone.utc),
            latest_tag="v1.0.0",
            has_public_upstream=False,
            public_upstream_url="https://push.example.com/repo.git",
            public_upstream_branch="main",
        )

        result = resolution.https_pull_url
        self.assertEqual(result, "https://push.example.com/repo.git")

    def test_detect_remote_source_branch_uses_pull_url(self):
        """Test that detect_remote_source_branch uses url_pull when available."""
        sr = self.create_source_resolver()
        source_details = dict(
            url='https://push.example.com/repo.git',
            url_pull='https://pull.example.com/repo.git',
            branch=dict(
                target='main_branch',
                fallback='fallback_branch',
            ),
        )

        # Mock the _get_remote_branch_ref to expect pull URL
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").with_args(
            'https://pull.example.com/repo.git', 'main_branch'
        ).once().and_return("commit_hash")

        result = sr.detect_remote_source_branch(source_details, stage=False)
        self.assertEqual(result, ("main_branch", "commit_hash"))

    def test_detect_remote_source_branch_falls_back_to_url(self):
        """Test that detect_remote_source_branch falls back to url when url_pull is not available."""
        sr = self.create_source_resolver()
        source_details = dict(
            url='https://push.example.com/repo.git',
            branch=dict(
                target='main_branch',
                fallback='fallback_branch',
            ),
        )

        # Mock the _get_remote_branch_ref to expect main URL
        flexmock(SourceResolver).should_receive("_get_remote_branch_ref").with_args(
            'https://push.example.com/repo.git', 'main_branch'
        ).once().and_return("commit_hash")

        result = sr.detect_remote_source_branch(source_details, stage=False)
        self.assertEqual(result, ("main_branch", "commit_hash"))

    # Branch protection tests (ART-14540)
    @patch("doozerlib.source_resolver.get_github_client_for_org")
    def test_check_branch_protection_protected(self, mock_get_client):
        """
        Test that _check_branch_protection returns True when the branch is protected.
        """
        mock_branch = Mock(protected=True)
        mock_get_client.return_value.get_repo.return_value.get_branch.return_value = mock_branch
        result = SourceResolver._check_branch_protection("git@github.com:openshift/etcd.git", "openshift-4.19")
        self.assertTrue(result)
        mock_get_client.assert_called_once_with("openshift")
        mock_get_client.return_value.get_repo.assert_called_with("openshift/etcd")
        mock_get_client.return_value.get_repo.return_value.get_branch.assert_called_with("openshift-4.19")

    @patch("doozerlib.source_resolver.get_github_client_for_org")
    def test_check_branch_protection_unprotected(self, mock_get_client):
        """
        Test that _check_branch_protection returns False when the branch is not protected.
        """
        mock_branch = Mock(protected=False)
        mock_get_client.return_value.get_repo.return_value.get_branch.return_value = mock_branch
        result = SourceResolver._check_branch_protection("https://github.com/openshift/etcd", "openshift-4.19")
        self.assertFalse(result)

    @patch("doozerlib.source_resolver.get_github_client_for_org")
    def test_check_branch_protection_not_found(self, mock_get_client):
        """
        Test that _check_branch_protection returns True (fail-open) when repo/branch
        is not found (404). Let the clone step surface the real error.
        """
        from github import UnknownObjectException

        mock_get_client.return_value.get_repo.return_value.get_branch.side_effect = UnknownObjectException(
            404, {"message": "Not Found"}, None
        )
        result = SourceResolver._check_branch_protection("https://github.com/openshift/etcd", "openshift-4.19")
        self.assertTrue(result)

    @patch("doozerlib.source_resolver.get_github_client_for_org")
    def test_check_branch_protection_api_error(self, mock_get_client):
        """
        Test that _check_branch_protection returns True on GitHub API errors
        (don't block builds on API issues).
        """
        from github import GithubException

        mock_get_client.return_value.get_repo.return_value.get_branch.side_effect = GithubException(
            500, {"message": "Internal Server Error"}, None
        )
        result = SourceResolver._check_branch_protection("https://github.com/openshift/etcd", "openshift-4.19")
        self.assertTrue(result)

    @patch("doozerlib.source_resolver.get_github_client_for_org")
    def test_check_branch_protection_network_error(self, mock_get_client):
        """
        Test that _check_branch_protection returns True on network errors.
        """
        mock_get_client.return_value.get_repo.side_effect = Exception("Connection refused")
        result = SourceResolver._check_branch_protection("https://github.com/openshift/etcd", "openshift-4.19")
        self.assertTrue(result)

    def test_check_branch_protection_non_github(self):
        """
        Test that _check_branch_protection skips non-GitHub repos.
        """
        result = SourceResolver._check_branch_protection("https://gitlab.internal.example.com/org/repo", "main")
        self.assertTrue(result)

    @patch("doozerlib.source_resolver.get_github_client_for_org")
    def test_check_branch_protection_auth_failure_failopen(self, mock_get_client):
        """
        Test that _check_branch_protection returns True (fail-open) when auth fails.
        """
        mock_get_client.side_effect = EnvironmentError("No credentials configured")
        result = SourceResolver._check_branch_protection("https://github.com/openshift/etcd", "openshift-4.19")
        self.assertTrue(result)

    @patch("doozerlib.source_resolver.get_github_client_for_org")
    def test_check_branch_protection_special_chars_in_branch(self, mock_get_client):
        """
        Test that branch names with special characters are passed correctly to PyGithub.
        """
        mock_branch = Mock(protected=True)
        mock_get_client.return_value.get_repo.return_value.get_branch.return_value = mock_branch
        SourceResolver._check_branch_protection("https://github.com/openshift/etcd", "release/4.19")
        mock_get_client.return_value.get_repo.return_value.get_branch.assert_called_with("release/4.19")

    @patch("doozerlib.source_resolver.get_github_git_auth_env")
    def test_setup_and_fetch_public_upstream_source_uses_auth(self, mock_auth):
        """
        Test that setup_and_fetch_public_upstream_source passes auth env to git fetch.
        """
        mock_auth.return_value = {"GIT_ASKPASS": "/tmp/askpass.sh", "GIT_PASSWORD": "token123"}
        flexmock(exectools).should_receive("cmd_assert").with_args(["git", "-C", "/src", "remote"]).once().and_return(
            "origin", ""
        )
        flexmock(exectools).should_receive("cmd_assert").with_args(
            ["git", "-C", "/src", "remote", "add", "--", "public_upstream", "https://github.com/openshift/repo"]
        ).once().and_return("", "")

        expected_env = {**constants.GIT_NO_PROMPTS, "GIT_ASKPASS": "/tmp/askpass.sh", "GIT_PASSWORD": "token123"}
        flexmock(exectools).should_receive("cmd_assert").with_args(
            ["git", "-C", "/src", "fetch", "--", "public_upstream", "main"],
            retries=3,
            set_env=expected_env,
        ).once().and_return("", "")

        SourceResolver.setup_and_fetch_public_upstream_source("https://github.com/openshift/repo", "main", "/src")
        mock_auth.assert_called_once_with(url="https://github.com/openshift/repo")
