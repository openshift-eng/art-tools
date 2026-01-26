import copy
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional, Sequence

from artcommonlib import constants, exectools
from artcommonlib import util as art_util
from artcommonlib.lock import get_named_semaphore

LOGGER = logging.getLogger(__name__)


def git_clone(remote_url: str, target_dir: str, gitargs=[], set_env={}, timeout=0, git_cache_dir: Optional[str] = None):
    # Do not change the outer scope param list
    gitargs = copy.copy(gitargs)

    if git_cache_dir:
        Path(git_cache_dir).mkdir(parents=True, exist_ok=True)
        normalized_url = art_util.convert_remote_git_to_https(remote_url)
        # Strip special chars out of normalized url to create a human friendly, but unique filename
        file_friendly_url = normalized_url.split("//")[-1].replace("/", "_")
        repo_dir = os.path.join(git_cache_dir, file_friendly_url)
        LOGGER.info(f"Cache for {remote_url} going to {repo_dir}")

        if not os.path.exists(repo_dir):
            LOGGER.info(f"Initializing cache directory for git remote: {remote_url}")

            # If the cache directory for this repo does not exist yet, we will create one.
            # But we must do so carefully to minimize races with any other doozer instance
            # running on the machine.
            with get_named_semaphore(
                repo_dir, is_dir=True
            ):  # also make sure we cooperate with other threads in this process.
                tmp_repo_dir = tempfile.mkdtemp(dir=git_cache_dir)
                exectools.cmd_assert(f"git init --bare {tmp_repo_dir}")
                with exectools.Dir(tmp_repo_dir):
                    exectools.cmd_assert(f"git remote add origin {remote_url}")

                try:
                    os.rename(tmp_repo_dir, repo_dir)
                except:
                    # There are two categories of failure
                    # 1. Another doozer instance already created the directory, in which case we are good to go.
                    # 2. Something unexpected is preventing the rename.
                    if not os.path.exists(repo_dir):
                        # Not sure why the rename failed. Raise to user.
                        raise

        # If we get here, we have a bare repo with a remote set
        # Pull content to update the cache. This should be safe for multiple doozer instances to perform.
        LOGGER.info(f"Updating cache directory for git remote: {remote_url}")
        # Fire and forget this fetch -- just used to keep cache as fresh as possible
        exectools.fire_and_forget(repo_dir, "git fetch --all")
        gitargs.extend(["--dissociate", "--reference-if-able", repo_dir])

    gitargs.append("--recurse-submodules")

    LOGGER.info(f"Cloning to: {target_dir}")

    # Perform the clone (including --reference args if cache_dir was set)
    cmd = []
    if timeout:
        cmd.extend(["timeout", f"{timeout}"])
    cmd.extend(["git", "clone", remote_url])
    cmd.extend(gitargs)
    cmd.append(target_dir)
    exectools.cmd_assert(cmd, retries=3, on_retry=["rm", "-rf", target_dir], set_env=set_env)


async def run_git_async(
    args: Sequence[str], env: Optional[Dict[str, str]] = None, check: bool = True, stdout=sys.stderr, **kwargs
):
    """Run a git command and optionally raises an exception if the return code of the command indicates failure.
    :param args: List of arguments to pass to git
    :param env: Optional environment variables to set
    :param check: If True, raise an exception if the git command fails
    :param kwargs: Additional arguments to pass to exectools.cmd_assert_async
    :return: exit code of the git command
    """
    # set up env vars for git
    set_env = os.environ.copy()
    set_env.update(constants.GIT_NO_PROMPTS)
    if env:
        set_env.update(env)
    return await exectools.cmd_assert_async(["git"] + list(args), check=check, env=set_env, stdout=stdout, **kwargs)


async def gather_git_async(args: Sequence[str], env: Optional[Dict[str, str]] = None, check: bool = True, **kwargs):
    """Run a git command asynchronously and returns rc,stdout,stderr as a tuple
    :param args: List of arguments to pass to git
    :param env: Optional environment variables to set
    :param check: If True, raise an exception if the git command fails
    :param kwargs: Additional arguments to pass to exectools.cmd_gather_async
    :return: exit code of the git command
    """
    # set up env vars for git
    set_env = os.environ.copy()
    set_env.update(constants.GIT_NO_PROMPTS)
    if env:
        set_env.update(env)
    return await exectools.cmd_gather_async(["git"] + list(args), check=check, env=set_env, **kwargs)


def gather_git(args: Sequence[str], **kwargs):
    """Run a git command asynchronously and returns rc,stdout,stderr as a tuple
    :param args: List of arguments to pass to git
    :param kwargs: Additional arguments to pass to exectools.cmd_gather_async
    :return: exit code of the git command
    """

    return exectools.cmd_gather(["git"] + list(args), **kwargs)
