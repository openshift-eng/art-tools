import sys
import unittest
from unittest.mock import ANY, Mock, patch


class TestGitHelper(unittest.IsolatedAsyncioTestCase):
    @patch('artcommonlib.exectools.cmd_assert_async')
    async def test_run_git(self, cmd_assert_async: Mock):
        # Test that run_git calls exectools.cmd_assert_async with the correct arguments
        from artcommonlib import git_helper

        args = ["init", "local_dir"]
        check = True
        await git_helper.run_git_async(args, check=check)
        cmd_assert_async.assert_called_once_with(["git"] + args, env=ANY, check=check, stdout=sys.stderr)

    @patch('artcommonlib.exectools.cmd_gather_async')
    async def test_gather_git(self, cmd_gather_async: Mock):
        # Test that gather_git calls exectools.cmd_gather_async with the correct arguments
        from artcommonlib import git_helper

        args = ["init", "local_dir"]
        check = True
        await git_helper.gather_git_async(args, check=check)
        cmd_gather_async.assert_called_once_with(["git"] + args, env=ANY, check=check)
