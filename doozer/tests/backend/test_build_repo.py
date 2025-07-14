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
        await self.repo.push(prod=False)
        run_git.assert_any_await(["-C", "/path/to/repo", "push", "origin", "HEAD"])
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
