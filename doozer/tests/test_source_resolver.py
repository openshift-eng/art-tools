from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from artcommonlib import exectools
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

    def test_get_remote_branch_ref(self):
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
