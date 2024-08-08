import logging
import shutil
from pathlib import Path
from typing import Optional, Union

from artcommonlib import git_helper
from artcommonlib import util as art_util

LOGGER = logging.getLogger(__name__)


class BuildRepo:
    """ A class to clone a build source repository into a local directory.
    """

    def __init__(self,
                 url: str,
                 branch: str,
                 local_dir: Union[str, Path],
                 logger: Optional[logging.Logger]) -> None:
        """ Initialize a BuildRepo object.
        :param url: The URL of the build source repository.
        :param branch: The branch of the build source repository to clone.
        :param local_dir: The local directory to clone the build source repository into.
        :param logger: A logger object to use for logging messages
        """
        self.url = url
        self.branch = branch
        self.local_dir = Path(local_dir)
        self._logger = logger or LOGGER

    @property
    def https_url(self) -> str:
        """ Get the HTTPS URL of the build source repository.
        """
        return art_util.convert_remote_git_to_https(self.url)

    def exists(self) -> bool:
        """ Check if the local directory already exists.
        """
        return self.local_dir.joinpath(".git").exists()

    async def ensure_source(self, upcycle: bool = False):
        """ Ensure that the build source repository is cloned into the local directory.
        :param upcycle: If True, the local directory will be deleted and recreated if it already exists.
        """
        local_dir = str(self.local_dir)
        needs_clone = True
        if self.exists():
            if upcycle:
                self._logger.info("Upcycling existing build source repository at %s", local_dir)
                shutil.rmtree(local_dir)
            else:
                self._logger.info("Reusing existing build source repository at %s", local_dir)
                needs_clone = False
        if needs_clone:
            self._logger.info("Cloning build source repository %s on branch %s into %s...", self.url, self.branch, self.local_dir)
            await self.clone()

    async def clone(self):
        """ Clone the build source repository into the local directory.
        """
        local_dir = str(self.local_dir)
        self._logger.info("Cloning build source repository %s on branch %s into %s...", self.url, self.branch, local_dir)
        await git_helper.run_git(["init", local_dir])
        _, out, _ = await git_helper.gather_git(["-C", local_dir, "remote"], stderr=None)
        if 'origin' not in out.strip().split():
            await git_helper.run_git(["-C", local_dir, "remote", "add", "origin", self.url])
        else:
            await git_helper.run_git(["-C", local_dir, "remote", "set-url", "origin", self.url])
        rc, _, err = await git_helper.gather_git(["-C", local_dir, "fetch", "--depth=1", "origin", self.branch], check=False)
        if rc != 0:
            if "fatal: couldn't find remote ref" in err:
                self._logger.info("Branch %s not found in build source repository; creating a new branch instead", self.branch)
                await git_helper.run_git(["-C", local_dir, "checkout", "--orphan", self.branch])
            else:
                raise ChildProcessError(f"Failed to fetch {self.branch} from {self.url}: {err}")
        else:
            await git_helper.run_git(["-C", local_dir, "checkout", "-B", self.branch, "-t", f"origin/{self.branch}"])

    async def commit(self, message: str, allow_empty: bool = False):
        """ Commit changes in the local directory to the build source repository."""
        local_dir = str(self.local_dir)
        await git_helper.run_git(["-C", local_dir, "add", "."])
        commit_opts = []
        if allow_empty:
            commit_opts.append("--allow-empty")
        await git_helper.run_git(["-C", local_dir, "commit"] + commit_opts + ["-m", message])

    async def push(self):
        """ Push changes in the local directory to the build source repository."""
        local_dir = str(self.local_dir)
        await git_helper.run_git(["-C", local_dir, "push", "origin", self.branch])

    async def get_commit_hash(self) -> str:
        """ Get the commit hash of the current commit in the build source repository.
        """
        local_dir = str(self.local_dir)
        _, out, _ = await git_helper.gather_git(["-C", local_dir, "rev-parse", "HEAD"])
        return out.strip()
