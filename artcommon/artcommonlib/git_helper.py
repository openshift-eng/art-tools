import copy
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional, Sequence

from artcommonlib import constants, exectools
from artcommonlib import util as art_util
from artcommonlib.github_auth import get_github_git_auth_env
from artcommonlib.lock import get_named_semaphore

LOGGER = logging.getLogger(__name__)


def git_clone(remote_url: str, target_dir: str, gitargs=[], set_env={}, timeout=0, git_cache_dir: Optional[str] = None):
    # Do not change the outer scope param list
    gitargs = copy.copy(gitargs)
    set_env = dict(set_env)

    # Convert GitHub SSH URLs to HTTPS and inject GIT_ASKPASS auth
    remote_url = art_util.ensure_github_https_url(remote_url)
    git_auth = get_github_git_auth_env(url=remote_url)
    set_env.update(git_auth)

    if git_cache_dir:
        Path(git_cache_dir).mkdir(parents=True, exist_ok=True)
        normalized_url = art_util.convert_remote_git_to_https(remote_url)
        file_friendly_url = normalized_url.split('//')[-1].replace('/', '_')
        repo_dir = os.path.join(git_cache_dir, file_friendly_url)
        LOGGER.info(f'Cache for {remote_url} going to {repo_dir}')

        if not os.path.exists(repo_dir):
            LOGGER.info(f'Initializing cache directory for git remote: {remote_url}')

            with get_named_semaphore(repo_dir, is_dir=True):
                tmp_repo_dir = tempfile.mkdtemp(dir=git_cache_dir)
                exectools.cmd_assert(f'git init --bare {tmp_repo_dir}', set_env=set_env)
                with exectools.Dir(tmp_repo_dir):
                    exectools.cmd_assert(f'git remote add origin {remote_url}', set_env=set_env)

                try:
                    os.rename(tmp_repo_dir, repo_dir)
                except:
                    if not os.path.exists(repo_dir):
                        raise

        LOGGER.info(f'Updating cache directory for git remote: {remote_url}')
        cache_env = {**os.environ, **set_env}
        exectools.fire_and_forget(repo_dir, 'git fetch --all', env=cache_env)
        gitargs.extend(['--dissociate', '--reference-if-able', repo_dir])

    gitargs.append('--recurse-submodules')

    LOGGER.info(f'Cloning to: {target_dir}')

    cmd = []
    if timeout:
        cmd.extend(['timeout', f'{timeout}'])
    cmd.extend(['git', 'clone', remote_url])
    cmd.extend(gitargs)
    cmd.append(target_dir)
    exectools.cmd_assert(cmd, retries=3, on_retry=["rm", "-rf", target_dir], set_env=set_env)


async def run_git_async(
    args: Sequence[str],
    env: Optional[Dict[str, str]] = None,
    check: bool = True,
    stdout=sys.stderr,
    github_url: Optional[str] = None,
    **kwargs,
):
    """Run a git command and optionally raises an exception if the return code of the command indicates failure.
    :param args: List of arguments to pass to git
    :param env: Optional environment variables to set
    :param check: If True, raise an exception if the git command fails
    :param github_url: Optional GitHub URL for App auth installation resolution
    :param kwargs: Additional arguments to pass to exectools.cmd_assert_async
    :return: exit code of the git command
    """
    set_env = os.environ.copy()
    set_env.update(constants.GIT_NO_PROMPTS)
    set_env.update(get_github_git_auth_env(url=github_url))
    if env:
        set_env.update(env)
    return await exectools.cmd_assert_async(['git'] + list(args), check=check, env=set_env, stdout=stdout, **kwargs)


async def gather_git_async(
    args: Sequence[str],
    env: Optional[Dict[str, str]] = None,
    check: bool = True,
    github_url: Optional[str] = None,
    **kwargs,
):
    """Run a git command asynchronously and returns rc,stdout,stderr as a tuple
    :param args: List of arguments to pass to git
    :param env: Optional environment variables to set
    :param check: If True, raise an exception if the git command fails
    :param github_url: Optional GitHub URL for App auth installation resolution
    :param kwargs: Additional arguments to pass to exectools.cmd_gather_async
    :return: exit code of the git command
    """
    set_env = os.environ.copy()
    set_env.update(constants.GIT_NO_PROMPTS)
    set_env.update(get_github_git_auth_env(url=github_url))
    if env:
        set_env.update(env)
    return await exectools.cmd_gather_async(['git'] + list(args), check=check, env=set_env, **kwargs)


def gather_git(args: Sequence[str], **kwargs):
    """Run a git command asynchronously and returns rc,stdout,stderr as a tuple
    :param args: List of arguments to pass to git
    :param kwargs: Additional arguments to pass to exectools.cmd_gather_async
    :return: exit code of the git command
    """

    return exectools.cmd_gather(['git'] + list(args), **kwargs)
