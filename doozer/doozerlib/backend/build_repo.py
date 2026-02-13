import logging
import shutil
import urllib.parse
from pathlib import Path
from typing import Optional, Union

import aiofiles
from artcommonlib import exectools, git_helper
from artcommonlib import util as art_util
from artcommonlib.telemetry import start_as_current_span_async
from opentelemetry import trace

LOGGER = logging.getLogger(__name__)
TRACER = trace.get_tracer(__name__)


class BuildRepo:
    """A class to clone a build source repository into a local directory."""

    def __init__(
        self,
        url: str,
        branch: Optional[str],
        local_dir: Union[str, Path],
        username: Optional[str] = None,
        password: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        pull_url: Optional[str] = None,
    ) -> None:
        """Initialize a BuildRepo object.
        :param url: The URL of the build source repository (for pushing).
        :param branch: The branch of the build source repository to clone. None to not switch to any branch.
        :param local_dir: The local directory to clone the build source repository into.
        :param username: The username to use for accessing the build source repository (optional).
        :param password: The password to use for accessing the build source repository (optional).
        :param logger: A logger object to use for logging messages
        :param pull_url: The URL of the repository to pull from. If None, uses url for both pull and push.
        """
        self.url = url
        self.pull_url = pull_url or url
        self.branch = branch
        self.local_dir = Path(local_dir)
        self._username = username
        self._password = password
        self._commit_hash: Optional[str] = None
        self._logger = logger or LOGGER

    @property
    def https_url(self) -> str:
        """Get the HTTPS URL of the build source repository (push URL)."""
        return art_util.convert_remote_git_to_https(self.url)

    @property
    def https_pull_url(self) -> str:
        """Get the HTTPS URL of the pull source repository."""
        return art_util.convert_remote_git_to_https(self.pull_url)

    @property
    def commit_hash(self) -> Optional[str]:
        """Get the commit hash of the current commit in the build source repository.
        Returns None if the branch has no commits yet.
        """
        return self._commit_hash

    async def _set_credentials(self, username: str, password: str):
        """Set the credentials for accessing the build source repository.
        This will store the credentials in the ~/.git-credentials file.

        :param username: The username to use for accessing the build source repository.
        :param password: The password to use for accessing the build source repository.
        :raises ValueError: If the username or password is not provided.
        """
        self._logger.info("Setting credentials for build source repository")
        # Generate the credentials string
        if not username or not password:
            raise ValueError("Username and password must be provided to set credentials")
        parsed_url = urllib.parse.urlparse(self.url)
        domain = parsed_url.netloc.split("@")[-1]
        new_netloc = f"{urllib.parse.quote(username)}:{urllib.parse.quote(password)}@{domain}"
        credentials_string = urllib.parse.urlunparse(
            (parsed_url.scheme, new_netloc, parsed_url.path, parsed_url.params, parsed_url.query, parsed_url.fragment)
        )

        # Run git config credential.helper store
        git_config_cmd = ["config", "credential.helper", "store"]
        await git_helper.run_git_async(git_config_cmd, cwd=str(self.local_dir))
        # Run git config credential.useHttpPath true
        git_config_cmd = ["config", "credential.useHttpPath", "true"]
        await git_helper.run_git_async(git_config_cmd, cwd=str(self.local_dir))

        # Check if the credentials are already set in ~/.git-credentials
        credentials_file = Path.home() / ".git-credentials"
        if credentials_file.exists():
            self._logger.info("Checking if credentials already exist in %s", credentials_file)
            async with aiofiles.open(credentials_file, "r") as f:
                lines = await f.readlines()
            for line in lines:
                if line.strip() == credentials_string:
                    self._logger.info("Credentials already set for %s", self.url)
                    return

        # write credentials to ~/.git-credentials
        LOGGER.info("Writing credentials to %s", credentials_file)
        async with aiofiles.open(credentials_file, "a+") as f:
            await f.write(f"{credentials_string}\n")

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
                "Cloning build source repository %s on branch %s into %s...", self.pull_url, self.branch, self.local_dir
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
            "Cloning build source repository %s on branch %s into %s...", self.pull_url, self.branch, local_dir
        )
        await self.init()
        await self.set_remote_url(self.url)  # Set origin for push
        if self.pull_url != self.url:
            await self.set_pull_remote(self.pull_url)  # Set pull remote for fetch
        self._commit_hash = None
        if self._username and self._password:
            await self._set_credentials(self._username, self._password)
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

    @start_as_current_span_async(TRACER, "build_repo.set_pull_remote")
    async def set_pull_remote(self, pull_url: str):
        """Set the URL of the pull remote in the build source repository.
        :param pull_url: The URL of the pull remote.
        """
        await self.set_remote_url(pull_url, "pull")

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
        # Use pull remote if different from push URL, otherwise use origin
        remote_name = "pull" if self.pull_url != self.url else "origin"
        rc, _, err = await git_helper.gather_git_async(
            ["-C", local_dir, "fetch"] + fetch_options + [remote_name, refspec], check=False
        )
        if rc != 0:
            if not strict and "fatal: couldn't find remote ref" in err:
                self._logger.warning("Failed to fetch %s from %s: %s", refspec, self.pull_url, err)
                return False
            raise ChildProcessError(f"Failed to fetch {refspec} from {self.pull_url}: {err}")
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
    async def push(self, force: bool = False):
        """Push changes in the local directory to the build source repository.
        :param force: If True, use --force for rebase operations.
        """
        local_dir = str(self.local_dir)
        push_cmd = ["-C", local_dir, "push", "origin", "HEAD"]
        if force:
            push_cmd.append("--force")
            self._logger.info("Using force push for branch %s", self.branch)
        await git_helper.run_git_async(push_cmd)
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

        # Check if pull remote exists
        pull_url = None
        rc, pull_remote_url, _ = await git_helper.gather_git_async(
            ["-C", local_dir, "config", "--get", "remote.pull.url"], check=False
        )
        if rc == 0:
            pull_url = pull_remote_url.strip()

        repo = BuildRepo(url.strip(), branch.strip(), local_dir, logger=logger, pull_url=pull_url)
        repo._commit_hash = await BuildRepo._get_commit_hash(local_dir)
        return repo
