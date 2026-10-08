import tempfile
import unittest
from pathlib import Path

from artcommonlib.github_auth import build_git_auth_env
from pyartcd.git import GitRepository


class TestGitRepositoryAuth(unittest.IsolatedAsyncioTestCase):
    async def test_setup_keeps_gitlab_credentials_out_of_remote_config(self):
        token = "glpat_git_repository_test_secret"
        push_url = "https://gitlab.example.com/group/repo.git"
        pull_url = "https://gitlab.example.com/group/repo.git"

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "shipment-data"
            repo = GitRepository(repo_path)

            await repo.setup(
                remote_url=push_url,
                upstream_remote_url=pull_url,
                remote_auth_envs={"origin": build_git_auth_env(token, username="oauth2")},
            )

            config = (repo_path / ".git" / "config").read_text()
            self.assertIn(f"url = {push_url}", config)
            self.assertIn(f"url = {pull_url}", config)
            self.assertNotIn(token, config)

            origin_env = repo._git_env_for_remote("origin")
            self.assertEqual(origin_env["GIT_USERNAME"], "oauth2")
            self.assertEqual(origin_env["GIT_PASSWORD"], token)
            self.assertNotIn(token, repo._git_env_for_remote("upstream").get("GIT_PASSWORD", ""))

    async def test_setup_rejects_credentials_embedded_in_http_url(self):
        token = "glpat_rejected_test_secret"
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir) / "shipment-data"
            repo = GitRepository(repo_path)

            with self.assertRaisesRegex(ValueError, "must not contain embedded credentials") as ctx:
                await repo.setup(remote_url=f"https://oauth2:{token}@gitlab.example.com/group/repo.git")

            self.assertNotIn(token, str(ctx.exception))
            self.assertFalse(repo_path.exists())
