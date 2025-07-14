import asyncio
import logging
import re
import shutil
from pathlib import Path
from typing import List, Optional, Sequence, Union, cast

from artcommonlib import exectools, git_helper
from artcommonlib import util as art_util
from artcommonlib.telemetry import start_as_current_span_async
from doozerlib import constants
from doozerlib.image import ImageMetadata
from doozerlib.source_resolver import SourceResolution
from opentelemetry import trace

LOGGER = logging.getLogger(__name__)
TRACER = trace.get_tracer(__name__)


class BuildRepo:
    """A class to clone a build source repository into a local directory."""

    def __init__(
        self, url: str, branch: Optional[str], local_dir: Union[str, Path], logger: Optional[logging.Logger] = None
    ) -> None:
        """Initialize a BuildRepo object.
        :param url: The URL of the build source repository.
        :param branch: The branch of the build source repository to clone. None to not switch to any branch.
        :param local_dir: The local directory to clone the build source repository into.
        :param logger: A logger object to use for logging messages
        """
        self.url = url
        self.branch = branch
        self.local_dir = Path(local_dir)
        self._commit_hash: Optional[str] = None
        self._logger = logger or LOGGER

    @property
    def https_url(self) -> str:
        """Get the HTTPS URL of the build source repository."""
        return art_util.convert_remote_git_to_https(self.url)

    @property
    def commit_hash(self) -> Optional[str]:
        """Get the commit hash of the current commit in the build source repository.
        Returns None if the branch has no commits yet.
        """
        return self._commit_hash

    def exists(self) -> bool:
        """Check if the local directory already exists."""
        return self.local_dir.joinpath(".git").exists()

    @start_as_current_span_async(TRACER, "build_repo.ensure_source")
    async def ensure_source(self, upcycle: bool = False, strict: bool = False):
        """Ensure that the build source repository is cloned into the local directory.
        :param upcycle: If True, the local directory will be deleted and recreated if it already exists.
        :param strict: If True, raise an exception if the branch is not found in the build source repository.
                       Otherwise, create a new branch instead.
        """
        local_dir = str(self.local_dir)
        needs_clone = True
        if self.exists():
            if upcycle:
                self._logger.info("Upcycling existing build source repository at %s", local_dir)
                await exectools.to_thread(shutil.rmtree, local_dir)
            else:
                self._logger.info("Reusing existing build source repository at %s", local_dir)
                self._commit_hash = await self._get_commit_hash(local_dir, strict=strict)
                needs_clone = False
        if needs_clone:
            self._logger.info(
                "Cloning build source repository %s on branch %s into %s...", self.url, self.branch, self.local_dir
            )
            await self.clone(strict=strict)

    @start_as_current_span_async(TRACER, "build_repo.clone")
    async def clone(self, strict: bool = False):
        """Clone the build source repository into the local directory.
        :param strict: If True, raise an exception if the branch is not found in the build source repository;
                       otherwise, create a new branch instead.
        """
        local_dir = str(self.local_dir)
        self._logger.info(
            "Cloning build source repository %s on branch %s into %s...", self.url, self.branch, local_dir
        )
        await self.init()
        await self.set_remote_url(self.url)
        self._commit_hash = None
        if self.branch is not None:
            if await self.fetch(self.branch, strict=strict):
                await self.switch(self.branch)
            else:
                self._logger.info(
                    "Branch %s not found in build source repository; creating a new branch instead", self.branch
                )
                await self.switch(self.branch, orphan=True)

    @start_as_current_span_async(TRACER, "build_repo.init")
    async def init(self):
        """Initialize the local directory as a git repository."""
        local_dir = str(self.local_dir)
        await git_helper.run_git_async(["init", local_dir])

    @start_as_current_span_async(TRACER, "build_repo.set_remote_url")
    async def set_remote_url(self, url: Optional[str], remote_name: str = "origin"):
        """Set the URL of the remote in the build source repository.
        :param url: The URL of the remote. None to use the default URL specified in the constructor.
        :param remote_name: The name of the remote.
        """
        if url is None:
            url = self.url
        local_dir = str(self.local_dir)
        _, out, _ = await git_helper.gather_git_async(["-C", local_dir, "remote"], stderr=None)
        if remote_name not in out.strip().split():
            await git_helper.run_git_async(["-C", local_dir, "remote", "add", remote_name, url])
        else:
            await git_helper.run_git_async(["-C", local_dir, "remote", "set-url", remote_name, url])

    @start_as_current_span_async(TRACER, "build_repo.fetch")
    async def fetch(self, refspec: str, depth: Optional[int] = 1, strict: bool = False):
        """Fetch a refspec from the build source repository.
        :param refspec: The refspec to fetch.
        :param depth: The depth of the fetch. None to fetch the entire history.
        :param strict: If True, raise an exception if the refspec is not found in the remote.
        :return: True if the fetch was successful; False otherwise
        """
        local_dir = str(self.local_dir)
        fetch_options = []
        if depth is not None:
            fetch_options.append(f"--depth={depth}")
        rc, _, err = await git_helper.gather_git_async(
            ["-C", local_dir, "fetch"] + fetch_options + ["origin", refspec], check=False
        )
        if rc != 0:
            if not strict and "fatal: couldn't find remote ref" in err:
                self._logger.warning("Failed to fetch %s from %s: %s", refspec, self.url, err)
                return False
            raise ChildProcessError(f"Failed to fetch {refspec} from {self.url}: {err}")
        return True

    @start_as_current_span_async(TRACER, "build_repo.switch")
    async def switch(self, branch: str, detach: bool = False, orphan: bool = False):
        """Switch to a different branch in the build source repository.
        :param branch: The branch to switch to.
        """
        local_dir = str(self.local_dir)
        options = []
        if detach:
            options.append("--detach")
        if orphan:
            options.append("--orphan")
        await git_helper.run_git_async(["-C", local_dir, "switch"] + options + [branch])
        self.branch = branch
        self._commit_hash = await self._get_commit_hash(local_dir)

    @start_as_current_span_async(TRACER, "build_repo.delete_all_files")
    async def delete_all_files(self):
        """Delete all files in the local directory."""
        await git_helper.run_git_async(["-C", str(self.local_dir), "rm", "-rf", "--ignore-unmatch", "."])

    @staticmethod
    @start_as_current_span_async(TRACER, "build_repo.get_commit_hash")
    async def _get_commit_hash(local_dir: str, strict: bool = False) -> Optional[str]:
        """Get the commit hash of the current commit in the build source repository.
        :return: The commit hash of the current commit; None if the branch has no commits yet.
        """
        rc, out, err = await git_helper.gather_git_async(["-C", str(local_dir), "rev-parse", "HEAD"], check=False)
        if rc != 0:
            if "unknown revision or path not in the working tree" in err:
                # This branch has no commits yet
                if strict:
                    raise IOError(f"No commits found in build source repository at {local_dir}")
                return None
            raise ChildProcessError(f"Failed to get commit hash: {err}")
        return out.strip()

    @start_as_current_span_async(TRACER, "build_repo.commit")
    async def commit(self, message: str, allow_empty: bool = False, force: bool = False):
        """Commit changes in the local directory to the build source repository."""
        local_dir = str(self.local_dir)
        await git_helper.run_git_async(["-C", local_dir, "add", "."] + (["-f"] if force else []))
        commit_opts = []
        if allow_empty:
            commit_opts.append("--allow-empty")
        await git_helper.run_git_async(["-C", local_dir, "commit"] + commit_opts + ["-m", message])
        self._commit_hash = await self._get_commit_hash(local_dir, strict=True)

    @start_as_current_span_async(TRACER, "build_repo.tag")
    async def tag(self, tag: str):
        """Tag the current commit in the build source repository.

        :param tag: The tag to apply to the current commit.
        """
        local_dir = str(self.local_dir)
        await git_helper.run_git_async(["-C", local_dir, "tag", "-fam", tag, "--", tag])

    @start_as_current_span_async(TRACER, "build_repo.push")
    async def push(self, prod=True):
        """Push changes in the local directory to the build source repository."""
        local_dir = str(self.local_dir)

        if prod:
            # By default, we always want to push to openshift-priv
            pattern = r"(git@[^:]+:)[^/]+(/.*)"
            replacement = rf"\1{constants.KONFLUX_DEFAULT_REBASE_REMOTE}\2"
            remote_url = re.sub(pattern, replacement, self.url)
            await self.set_remote_url(url=remote_url, remote_name="origin")

        await git_helper.run_git_async(["-C", local_dir, "push", "origin", "HEAD"])
        try:
            await git_helper.run_git_async(["-C", local_dir, "push", "origin", "--tags"])
        except Exception as e:
            # Rebase should not fail if the tags are not pushed successfully
            self._logger.warning(e)

    @staticmethod
    @start_as_current_span_async(TRACER, "build_repo.from_local_dir")
    async def from_local_dir(local_dir: Union[str, Path], logger: Optional[logging.Logger] = None):
        """Create a BuildRepo object from an existing local directory.
        :param local_dir: The local directory containing the build source repository.
        :param logger: A logger object to use for logging messages
        :return: A BuildRepo object
        :raises FileNotFoundError: If the local directory is not a git repository
        """
        local_dir = Path(local_dir)
        if not local_dir.joinpath(".git").exists():
            raise FileNotFoundError(f"{local_dir} is not a git repository")
        local_dir = str(local_dir)
        _, url, _ = await git_helper.gather_git_async(["-C", local_dir, "config", "--get", "remote.origin.url"])
        _, branch, _ = await git_helper.gather_git_async(["-C", local_dir, "rev-parse", "--abbrev-ref", "HEAD"])
        repo = BuildRepo(url.strip(), branch.strip(), local_dir, logger)
        repo._commit_hash = await BuildRepo._get_commit_hash(local_dir)
        return repo
