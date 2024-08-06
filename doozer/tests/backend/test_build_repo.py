from unittest import IsolatedAsyncioTestCase

from unittest.mock import Mock, patch

from doozerlib.backend.build_repo import BuildRepo


class TestBuildRepo(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.repo = BuildRepo(
            url="https://git.example.com/repo.git",
            branch="my-branch",
            local_dir="/path/to/repo",
            logger=None
        )

    @patch("pathlib.Path.exists", return_value=False)
    async def test_exists(self, _):
        self.assertFalse(self.repo.exists())

    @patch("artcommonlib.git_helper.run_git", return_value=0)
    @patch("artcommonlib.git_helper.gather_git", return_value=(0, "", ""))
    async def test_clone(self, gather_git: Mock, run_git: Mock):
        await self.repo.clone()
        run_git.assert_any_call(["init", "/path/to/repo"])
        gather_git.assert_any_call(["-C", "/path/to/repo", "fetch", "--depth=1", "origin", self.repo.branch], check=False)

    @patch("artcommonlib.git_helper.run_git", return_value=0)
    async def test_commit(self, run_git: Mock):
        await self.repo.commit("commit message")
        run_git.assert_called_with(["-C", "/path/to/repo", "commit", "-m", "commit message"])

    @patch("artcommonlib.git_helper.run_git", return_value=0)
    async def test_push(self, run_git: Mock):
        await self.repo.push()
        run_git.assert_called_with(["-C", "/path/to/repo", "push", "origin", self.repo.branch])

    @patch("artcommonlib.git_helper.gather_git", return_value=0)
    async def test_get_commit_hash(self, gather_git: Mock):
        gather_git.return_value = (0, "commit-hash", "")
        self.assertEqual(await self.repo.get_commit_hash(), "commit-hash")

    @patch("pathlib.Path.exists", return_value=True)
    @patch("shutil.rmtree")
    @patch("artcommonlib.git_helper.run_git", return_value=0)
    @patch("artcommonlib.git_helper.gather_git", return_value=(0, "", ""))
    async def test_ensure_source_upcycle(self, gather_git: Mock, run_git: Mock, rmtree: Mock, _):
        await self.repo.ensure_source(upcycle=True)
        rmtree.assert_called_with("/path/to/repo")
        run_git.assert_any_call(["init", "/path/to/repo"])
        gather_git.assert_any_call(["-C", "/path/to/repo", "fetch", "--depth=1", "origin", self.repo.branch], check=False)
