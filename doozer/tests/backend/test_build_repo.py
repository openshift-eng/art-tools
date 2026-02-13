from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from doozerlib.backend.build_repo import BuildRepo


class TestBuildRepo(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.repo = BuildRepo(
            url="https://git.example.com/repo.git",
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
        )

    @patch("pathlib.Path.exists", return_value=False)
    async def test_exists(self, _):
        self.assertFalse(self.repo.exists())

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "", ""))
    async def test_clone(self, gather_git: Mock, run_git: Mock):
        await self.repo.clone()
        run_git.assert_any_call(["init", "/path/to/repo"])
        gather_git.assert_any_call(
            ["-C", "/path/to/repo", "fetch", "--depth=1", "origin", self.repo.branch], check=False
        )

    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "deadbeef", ""))
    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_commit(self, run_git: AsyncMock, gather_git: AsyncMock):
        await self.repo.commit("commit message")
        run_git.assert_any_await(["-C", "/path/to/repo", "add", "."])
        run_git.assert_any_await(["-C", "/path/to/repo", "commit", "-m", "commit message"])
        gather_git.assert_awaited_once_with(["-C", "/path/to/repo", "rev-parse", "HEAD"], check=False)
        self.assertEqual(self.repo.commit_hash, "deadbeef")

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_push(self, run_git: AsyncMock):
        await self.repo.push()
        run_git.assert_any_await(["-C", "/path/to/repo", "push", "origin", "HEAD"])
        run_git.assert_any_await(["-C", "/path/to/repo", "push", "origin", "--tags"])

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_push_force(self, run_git: AsyncMock):
        await self.repo.push(force=True)
        run_git.assert_any_await(["-C", "/path/to/repo", "push", "origin", "HEAD", "--force"])
        run_git.assert_any_await(["-C", "/path/to/repo", "push", "origin", "--tags"])

    @patch("pathlib.Path.exists", return_value=True)
    @patch("shutil.rmtree")
    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "", ""))
    async def test_ensure_source_upcycle(self, gather_git: AsyncMock, run_git: AsyncMock, rmtree: Mock, _):
        await self.repo.ensure_source(upcycle=True)
        rmtree.assert_called_with("/path/to/repo")
        run_git.assert_any_await(["init", "/path/to/repo"])
        gather_git.assert_any_await(
            ["-C", "/path/to/repo", "fetch", "--depth=1", "origin", self.repo.branch], check=False
        )

    @patch("doozerlib.backend.build_repo.BuildRepo.clone")
    @patch("pathlib.Path.exists", return_value=True)
    @patch("shutil.rmtree")
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "", ""))
    async def test_ensure_source_no_upcycle(self, gather_git: AsyncMock, rmtree: Mock, _, clone: AsyncMock):
        await self.repo.ensure_source(upcycle=False)
        clone.assert_not_called()
        rmtree.assert_not_called()

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "origin", ""))
    async def test_set_remote_url(self, gather_git: AsyncMock, run_git: AsyncMock):
        await self.repo.set_remote_url("https://git.example.com/new-repo.git")
        gather_git.assert_awaited_once_with(["-C", "/path/to/repo", "remote"], stderr=None)
        run_git.assert_any_await(
            ["-C", "/path/to/repo", "remote", "set-url", "origin", "https://git.example.com/new-repo.git"]
        )

        gather_git.reset_mock()
        gather_git.reset_mock()
        gather_git.return_value = (0, "other", "")
        await self.repo.set_remote_url("https://git.example.com/new-repo.git")
        gather_git.assert_awaited_once_with(["-C", "/path/to/repo", "remote"], stderr=None)
        run_git.assert_any_await(
            ["-C", "/path/to/repo", "remote", "add", "origin", "https://git.example.com/new-repo.git"]
        )

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_delete_all_files(self, run_git: AsyncMock):
        await self.repo.delete_all_files()
        run_git.assert_awaited_once_with(["-C", "/path/to/repo", "rm", "-rf", "--ignore-unmatch", "."])

    @patch("doozerlib.backend.build_repo.BuildRepo._get_commit_hash")
    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_switch(self, run_git: AsyncMock, _):
        await self.repo.switch("new-branch")
        run_git.assert_awaited_once_with(["-C", "/path/to/repo", "switch", "new-branch"])
        self.assertEqual(self.repo.branch, "new-branch")

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_tag(self, run_git: AsyncMock):
        await self.repo.tag("v1.0.0")
        run_git.assert_awaited_once_with(["-C", "/path/to/repo", "tag", "-fam", "v1.0.0", "--", "v1.0.0"])

    @patch("pathlib.Path.exists", return_value=False)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "deadbeef", ""))
    async def test_from_local_dir_not_found(self, _, __):
        local_dir = Path("/path/to/repo")
        with patch("pathlib.Path.exists", return_value=False):
            with self.assertRaises(FileNotFoundError):
                await BuildRepo.from_local_dir(local_dir)

    @patch("doozerlib.backend.build_repo.BuildRepo._get_commit_hash", return_value="deadbeef")
    @patch("doozerlib.backend.build_repo.Path")
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "whatever", ""))
    async def test_from_local_dir_found(self, gather_git: AsyncMock, MockPath: Mock, _):
        MockPath.joinpath.return_value.exists.return_value = True
        local_dir = Path("/path/to/repo")
        repo = await BuildRepo.from_local_dir(local_dir)
        self.assertEqual(repo.url, "whatever")
        self.assertEqual(repo.branch, "whatever")
        self.assertEqual(repo.commit_hash, "deadbeef")

    # Dual URL tests
    def test_init_with_pull_url(self):
        """Test BuildRepo initialization with separate pull URL."""
        push_url = "https://git.example.com/push-repo.git"
        pull_url = "https://git.example.com/pull-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=pull_url,
        )
        self.assertEqual(repo.url, push_url)
        self.assertEqual(repo.pull_url, pull_url)

    def test_init_without_pull_url(self):
        """Test BuildRepo initialization without pull URL falls back to main URL."""
        push_url = "https://git.example.com/push-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
        )
        self.assertEqual(repo.url, push_url)
        self.assertEqual(repo.pull_url, push_url)

    def test_https_pull_url_property(self):
        """Test https_pull_url property returns correct URL."""
        push_url = "https://git.example.com/push-repo.git"
        pull_url = "https://git.example.com/pull-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=pull_url,
        )
        with patch("artcommonlib.util.convert_remote_git_to_https") as mock_convert:
            mock_convert.return_value = "https://converted.example.com/pull-repo.git"
            result = repo.https_pull_url
            mock_convert.assert_called_with(pull_url)
            self.assertEqual(result, "https://converted.example.com/pull-repo.git")

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "", ""))
    async def test_clone_with_dual_urls(self, gather_git: Mock, run_git: Mock):
        """Test clone operation with separate pull and push URLs."""
        push_url = "https://git.example.com/push-repo.git"
        pull_url = "https://git.example.com/pull-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=pull_url,
        )
        await repo.clone()

        # Verify git init
        run_git.assert_any_call(["init", "/path/to/repo"])

        # Verify origin remote set to push URL
        gather_git.assert_any_call(["-C", "/path/to/repo", "remote"], stderr=None)
        run_git.assert_any_call(["-C", "/path/to/repo", "remote", "add", "origin", push_url])

        # Verify pull remote set to pull URL
        run_git.assert_any_call(["-C", "/path/to/repo", "remote", "add", "pull", pull_url])

        # Verify fetch from pull remote
        gather_git.assert_any_call(["-C", "/path/to/repo", "fetch", "--depth=1", "pull", repo.branch], check=False)

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "", ""))
    async def test_clone_with_same_urls(self, gather_git: Mock, run_git: Mock):
        """Test clone operation when pull and push URLs are the same."""
        push_url = "https://git.example.com/push-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=push_url,  # Same as push URL
        )
        await repo.clone()

        # Verify no separate pull remote is created
        run_git.assert_any_call(["-C", "/path/to/repo", "remote", "add", "origin", push_url])

        # Verify fetch from origin remote
        gather_git.assert_any_call(["-C", "/path/to/repo", "fetch", "--depth=1", "origin", repo.branch], check=False)

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "", ""))
    async def test_fetch_uses_pull_remote(self, gather_git: Mock, run_git: Mock):
        """Test fetch operation uses pull remote when available."""
        push_url = "https://git.example.com/push-repo.git"
        pull_url = "https://git.example.com/pull-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=pull_url,
        )
        await repo.fetch("test-branch")

        # Verify fetch uses pull remote
        gather_git.assert_called_with(["-C", "/path/to/repo", "fetch", "--depth=1", "pull", "test-branch"], check=False)

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    @patch("artcommonlib.git_helper.gather_git_async", return_value=(0, "", ""))
    async def test_fetch_uses_origin_when_same_urls(self, gather_git: Mock, run_git: Mock):
        """Test fetch operation uses origin remote when URLs are the same."""
        push_url = "https://git.example.com/push-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=push_url,  # Same as push URL
        )
        await repo.fetch("test-branch")

        # Verify fetch uses origin remote
        gather_git.assert_called_with(
            ["-C", "/path/to/repo", "fetch", "--depth=1", "origin", "test-branch"], check=False
        )

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_set_pull_remote(self, run_git: Mock):
        """Test set_pull_remote method."""
        push_url = "https://git.example.com/push-repo.git"
        pull_url = "https://git.example.com/pull-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=pull_url,
        )
        with patch.object(repo, "set_remote_url") as mock_set_remote:
            await repo.set_pull_remote("https://example.com/new-pull.git")
            mock_set_remote.assert_called_once_with("https://example.com/new-pull.git", "pull")

    @patch("artcommonlib.git_helper.run_git_async", return_value=0)
    async def test_push_always_uses_origin(self, run_git: Mock):
        """Test push operation always uses origin remote regardless of pull URL."""
        push_url = "https://git.example.com/push-repo.git"
        pull_url = "https://git.example.com/pull-repo.git"
        repo = BuildRepo(
            url=push_url,
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None,
            pull_url=pull_url,
        )
        await repo.push()

        # Verify push uses origin remote (push URL)
        run_git.assert_any_call(["-C", "/path/to/repo", "push", "origin", "HEAD"])
        run_git.assert_any_call(["-C", "/path/to/repo", "push", "origin", "--tags"])

    @patch("doozerlib.backend.build_repo.BuildRepo._get_commit_hash", return_value="deadbeef")
    @patch("doozerlib.backend.build_repo.Path")
    @patch("artcommonlib.git_helper.gather_git_async")
    async def test_from_local_dir_with_pull_remote(self, gather_git: AsyncMock, MockPath: Mock, _):
        """Test from_local_dir method detects pull remote when available."""
        push_url = "https://git.example.com/push-repo.git"
        pull_url = "https://git.example.com/pull-repo.git"
        MockPath.joinpath.return_value.exists.return_value = True

        # Mock git config calls for origin and pull remotes
        def mock_gather_git(cmd, **kwargs):
            if "remote.origin.url" in cmd:
                return (0, push_url, "")
            elif "remote.pull.url" in cmd:
                return (0, pull_url, "")
            elif "rev-parse" in cmd:
                return (0, "my-branch", "")
            return (0, "", "")

        gather_git.side_effect = mock_gather_git

        local_dir = Path("/path/to/repo")
        repo = await BuildRepo.from_local_dir(local_dir)

        self.assertEqual(repo.url, push_url)
        self.assertEqual(repo.pull_url, pull_url)
        self.assertEqual(repo.branch, "my-branch")
        self.assertEqual(repo.commit_hash, "deadbeef")

    @patch("doozerlib.backend.build_repo.BuildRepo._get_commit_hash", return_value="deadbeef")
    @patch("doozerlib.backend.build_repo.Path")
    @patch("artcommonlib.git_helper.gather_git_async")
    async def test_from_local_dir_without_pull_remote(self, gather_git: AsyncMock, MockPath: Mock, _):
        """Test from_local_dir method when no pull remote exists."""
        push_url = "https://git.example.com/push-repo.git"
        MockPath.joinpath.return_value.exists.return_value = True

        # Mock git config calls - no pull remote
        def mock_gather_git(cmd, **kwargs):
            if "remote.origin.url" in cmd:
                return (0, push_url, "")
            elif "remote.pull.url" in cmd:
                return (1, "", "")  # Not found
            elif "rev-parse" in cmd:
                return (0, "my-branch", "")
            return (0, "", "")

        gather_git.side_effect = mock_gather_git

        local_dir = Path("/path/to/repo")
        repo = await BuildRepo.from_local_dir(local_dir)

        self.assertEqual(repo.url, push_url)
        self.assertEqual(repo.pull_url, push_url)  # Falls back to push URL when no pull remote
        self.assertEqual(repo.branch, "my-branch")
