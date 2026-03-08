import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from pyartcd.plashets import copy_to_remote


class TestCopyToRemoteCleanup(unittest.IsolatedAsyncioTestCase):
    """Verify that copy_to_remote cleans up old date-based directories after rsync."""

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_cleanup_runs_after_rsync(self, mock_cmd):
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path('/mnt/data/pub/RHOCP/plashets/4.20/stream/el9'),
            dry_run=False,
        )
        # 3 SSH/rsync calls: mkdir, rsync, cleanup
        assert mock_cmd.await_count == 3
        cleanup_cmd = mock_cmd.call_args_list[2][0][0]
        assert cleanup_cmd[:3] == ['ssh', 'remote-host', '--']
        script = cleanup_cmd[-1]
        assert 'head -n -3' in script
        assert 'rm -rf' in script

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_dry_run_lists_but_does_not_delete(self, mock_cmd):
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path('/mnt/data/pub/RHOCP/plashets/4.20/stream/el9'),
            dry_run=True,
        )
        # Only the cleanup SSH call runs (mkdir and rsync are skipped)
        mock_cmd.assert_awaited_once()
        cleanup_cmd = mock_cmd.call_args[0][0]
        script = cleanup_cmd[-1]
        assert 'Would remove' in script
        assert 'rm -rf' not in script
