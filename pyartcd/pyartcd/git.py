# pyartcd Git helpers

import os
from logging import getLogger
from pathlib import Path
from typing import Union

import aiofiles
from artcommonlib import exectools
from artcommonlib.constants import GIT_NO_PROMPTS
from artcommonlib.github_auth import get_github_git_auth_env
from artcommonlib.util import ensure_github_https_url
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

LOGGER = getLogger(__name__)


class GitRepository:
    def __init__(self, directory: Union[str, Path], dry_run: bool = False) -> None:
        self._directory = Path(directory)
        self._dry_run = dry_run
        self._remote_urls: dict[str, str] = {}

    @staticmethod
    def _local_env() -> dict[str, str]:
        """Build env dict for local-only git operations (add, commit, status, diff, etc.)
        that never contact a remote and therefore don't need GitHub auth."""
        env = os.environ.copy()
        env.update(GIT_NO_PROMPTS)
        return env

    @staticmethod
    def _git_env(url: str | None = None) -> dict[str, str]:
        """Build env dict with GIT_NO_PROMPTS and GitHub HTTPS auth (if available)."""
        env = os.environ.copy()
        env.update(GIT_NO_PROMPTS)
        env.update(get_github_git_auth_env(url=url))
        return env

    def _git_env_for_remote(self, remote: str) -> dict[str, str]:
        """Build env dict resolved for a specific named remote (origin, upstream, etc.)."""
        return self._git_env(url=self._remote_urls.get(remote))

    async def setup(self, remote_url, upstream_remote_url=None):
        """Initialize a git repository with specified remote URL and an optional upstream remote URL."""
        remote_url = ensure_github_https_url(remote_url)
        self._remote_urls["origin"] = remote_url
        if upstream_remote_url:
            upstream_remote_url = ensure_github_https_url(upstream_remote_url)
            self._remote_urls["upstream"] = upstream_remote_url
        self._directory.mkdir(parents=True, exist_ok=True)
        env = self._git_env(url=remote_url)
        repo_dir = str(self._directory)
        await exectools.cmd_assert_async(["git", "init", "--", repo_dir], env=env)

        # Add remotes
        _, out, _ = await exectools.cmd_gather_async(["git", "-C", repo_dir, "remote"], env=env)
        remotes = set(out.strip().split())
        if "origin" not in remotes:
            await exectools.cmd_assert_async(
                ["git", "-C", repo_dir, "remote", "add", "--", "origin", remote_url], env=env
            )
        else:
            await exectools.cmd_assert_async(
                ["git", "-C", repo_dir, "remote", "set-url", "--", "origin", remote_url], env=env
            )
        if upstream_remote_url:
            if 'upstream' not in remotes:
                await exectools.cmd_assert_async(
                    ["git", "-C", repo_dir, "remote", "add", "--", "upstream", upstream_remote_url], env=env
                )
            else:
                await exectools.cmd_assert_async(
                    ["git", "-C", repo_dir, "remote", "set-url", "--", "upstream", upstream_remote_url], env=env
                )
        elif 'upstream' in remotes:
            await exectools.cmd_assert_async(["git", "-C", repo_dir, "remote", "remove", "upstream"], env=env)

    async def read_file(self, relative_filepath: Union[str, Path]) -> str:
        """Read a file from the git repository."""
        path = self._directory / relative_filepath
        async with aiofiles.open(path, "r") as f:
            content = await f.read()
        return content

    async def write_file(self, relative_filepath: Union[str, Path], content: str) -> Path:
        """Write content to a file in the git repository."""
        path = self._directory / relative_filepath
        async with aiofiles.open(path, "w") as f:
            await f.write(content)
        return path

    async def add_all(self):
        """Add all files to the git repository."""
        env = self._local_env()
        await exectools.cmd_assert_async(["git", "-C", str(self._directory), "add", "."], env=env)

    async def log_diff(self, ref: str = "HEAD"):
        """Log diff of the current working tree against the specified reference."""
        env = self._local_env()
        await exectools.cmd_assert_async(["git", "-C", str(self._directory), "--no-pager", "diff", ref], env=env)

    async def create_branch(self, branch: str):
        """Create a new branch in the git repository."""
        env = self._local_env()
        repo_dir = str(self._directory)
        await exectools.cmd_assert_async(["git", "-C", repo_dir, "checkout", "-b", branch], env=env)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_exception_type(ChildProcessError))
    async def does_branch_exist_on_remote(self, branch: str, remote: str) -> bool:
        """Check if a branch exists on the remote repository."""
        env = self._git_env_for_remote(remote)
        repo_dir = str(self._directory)
        cmd = ["git", "-C", repo_dir, "ls-remote", "--heads", remote, branch]
        _, out, _ = await exectools.cmd_gather_async(cmd, env=env)
        return branch in out

    async def fetch_switch_branch(self, branch, upstream_ref=None, remote=None):
        """Fetch `upstream_ref` from the remote repo, create the `branch` and start it at `upstream_ref`.
        If `branch` already exists, then reset it to `upstream_ref`.
        """
        repo_dir = str(self._directory)
        if remote:
            fetch_remote = remote
        else:
            # Determine which remote to fetch from (local-only operation)
            _, out, _ = await exectools.cmd_gather_async(["git", "-C", repo_dir, "remote"], env=self._local_env())
            remotes = set(out.strip().split())
            fetch_remote = "upstream" if "upstream" in remotes else "origin"
        env = self._git_env_for_remote(fetch_remote)
        await exectools.cmd_assert_async(
            ["git", "-C", repo_dir, "fetch", "--depth=1", "--", fetch_remote, upstream_ref or branch], env=env
        )

        # Check out FETCH_HEAD
        await exectools.cmd_assert_async(["git", "-C", repo_dir, "checkout", "-f", "FETCH_HEAD"], env=env)
        await exectools.cmd_assert_async(["git", "-C", repo_dir, "checkout", "-B", branch], env=env)
        await exectools.cmd_assert_async(["git", "-C", repo_dir, "submodule", "update", "--init"], env=env)

        # Clean workdir
        await exectools.cmd_assert_async(["git", "-C", repo_dir, "clean", "-fdx"], env=env)

    async def commit_push(self, commit_message: str, safe: bool = False) -> bool:
        """Create a commit that includes all file changes in the working tree and push the commit to the remote repository.
        If there are no changes in thw working tree, do nothing.
        """
        # Make sure all files are added to the index
        await self.add_all()

        local_env = self._local_env()
        repo_dir = str(self._directory)
        cmd = ["git", "-C", repo_dir, "status", "--porcelain", "--untracked-files=no"]
        _, out, _ = await exectools.cmd_gather_async(cmd, env=local_env)
        if not out.strip():  # Nothing to commit
            return False

        # Commit the changes
        cmd = ["git", "-C", repo_dir, "commit", "--message", commit_message]
        await exectools.cmd_assert_async(cmd, env=local_env)

        push_env = self._git_env_for_remote("origin")

        @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_exception_type(ChildProcessError))
        async def _push():
            cmd = ["git", "-C", repo_dir, "push", "--force-with-lease" if safe else "--force", "origin", "HEAD"]
            if not self._dry_run:
                await exectools.cmd_assert_async(cmd, env=push_env)
            else:
                LOGGER.warning("[DRY RUN] Would have run %s", cmd)

        await _push()
        return True
