import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.plashets import CLEANUP_SCRIPT, copy_to_remote, run_cleanup_script

BASE = '/mnt/data/pub/RHOCP/plashets/4.20/stream/el9'


class TestCleanupScript(unittest.TestCase):
    def test_script_exists(self):
        assert CLEANUP_SCRIPT.exists(), f"Expected script at {CLEANUP_SCRIPT}"

    def test_script_has_shebang(self):
        content = CLEANUP_SCRIPT.read_text()
        assert content.startswith('#!/bin/bash')


class TestRunCleanupScript(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.plashets.asyncio.subprocess.create_subprocess_exec', new_callable=AsyncMock)
    async def test_passes_args_via_stdin(self, mock_exec):
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(b'', b''))
        mock_proc.returncode = 0
        mock_exec.return_value = mock_proc

        await run_cleanup_script('remote-host', Path(BASE), keep=3, dry_run=False)

        cmd = mock_exec.call_args[0]
        assert cmd[:4] == ('ssh', 'remote-host', '--', 'bash')
        assert '-s' in cmd
        assert BASE in cmd
        assert '3' in cmd
        assert '--dry-run' not in cmd

        # Script content was piped via stdin
        script_input = mock_proc.communicate.call_args[1]['input']
        assert b'#!/bin/bash' in script_input

    @patch('pyartcd.plashets.asyncio.subprocess.create_subprocess_exec', new_callable=AsyncMock)
    async def test_dry_run_passes_flag(self, mock_exec):
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(b'', b''))
        mock_proc.returncode = 0
        mock_exec.return_value = mock_proc

        await run_cleanup_script('remote-host', Path(BASE), keep=5, dry_run=True)

        cmd = mock_exec.call_args[0]
        assert '5' in cmd
        assert '--dry-run' in cmd

    @patch('pyartcd.plashets.asyncio.subprocess.create_subprocess_exec', new_callable=AsyncMock)
    async def test_logs_warning_on_failure(self, mock_exec):
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(b'', b''))
        mock_proc.returncode = 1
        mock_exec.return_value = mock_proc

        with self.assertLogs('pyartcd', level='WARNING') as cm:
            await run_cleanup_script('remote-host', Path(BASE), keep=3, dry_run=False)
        assert any('Cleanup script failed' in msg for msg in cm.output)


class TestCopyToRemoteCleanup(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.plashets.run_cleanup_script', new_callable=AsyncMock)
    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_cleanup_runs_after_rsync(self, mock_cmd, mock_cleanup):
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path(BASE),
            dry_run=False,
        )
        # mkdir + rsync
        assert mock_cmd.await_count == 2
        # cleanup called with correct args
        mock_cleanup.assert_awaited_once_with('remote-host', Path(BASE), keep=3, dry_run=False)

    @patch('pyartcd.plashets.run_cleanup_script', new_callable=AsyncMock)
    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_dry_run_passes_through(self, mock_cmd, mock_cleanup):
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path(BASE),
            dry_run=True,
        )
        # mkdir + rsync skipped in dry_run
        mock_cmd.assert_not_awaited()
        # cleanup still called with dry_run=True
        mock_cleanup.assert_awaited_once_with('remote-host', Path(BASE), keep=3, dry_run=True)
