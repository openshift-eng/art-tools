from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from pyartcd.s3 import _verify_synced_to_mirrors, sync_repo_to_s3_mirror
from tenacity import wait_none

# Disable retry wait for tests
sync_repo_to_s3_mirror.retry.wait = wait_none()


CLOUDFLARE_ENDPOINT = "https://fake-r2.cloudflarestorage.com"


class TestVerifySyncedToMirrors(IsolatedAsyncioTestCase):
    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_passes_when_both_empty(self, mock_gather: AsyncMock):
        mock_gather.return_value = (0, "", "")
        await _verify_synced_to_mirrors("/local/dir", "/enterprise/reposync/5.0")
        self.assertEqual(mock_gather.call_count, 2)

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_raises_on_s3_mismatch(self, mock_gather: AsyncMock):
        mock_gather.return_value = (0, "(dryrun) upload: ./foo.rpm to s3://art-srv-enterprise/path/foo.rpm\n", "")
        with self.assertRaises(IOError) as ctx:
            await _verify_synced_to_mirrors("/local/dir", "/enterprise/reposync/5.0")
        self.assertIn("S3 post-sync verification failed", str(ctx.exception))
        self.assertIn("foo.rpm", str(ctx.exception))

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_raises_on_r2_mismatch(self, mock_gather: AsyncMock):
        mock_gather.side_effect = [
            (0, "", ""),  # S3 check passes
            (0, "(dryrun) upload: ./bar.rpm to s3://art-srv-enterprise/path/bar.rpm\n", ""),  # R2 fails
        ]
        with self.assertRaises(IOError) as ctx:
            await _verify_synced_to_mirrors("/local/dir", "/enterprise/reposync/5.0")
        self.assertIn("R2 post-sync verification failed", str(ctx.exception))
        self.assertIn("bar.rpm", str(ctx.exception))

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_verify_uses_size_only_dryrun(self, mock_gather: AsyncMock):
        mock_gather.return_value = (0, "", "")
        await _verify_synced_to_mirrors("/local/dir", "/enterprise/reposync/5.0")
        s3_cmd = mock_gather.call_args_list[0][0][0]
        self.assertIn("--size-only", s3_cmd)
        self.assertIn("--dryrun", s3_cmd)
        self.assertNotIn("--exact-timestamps", s3_cmd)

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_verify_checks_r2_with_cloudflare_profile(self, mock_gather: AsyncMock):
        mock_gather.return_value = (0, "", "")
        await _verify_synced_to_mirrors("/local/dir", "/enterprise/reposync/5.0")
        r2_cmd = mock_gather.call_args_list[1][0][0]
        self.assertIn("--profile", r2_cmd)
        self.assertIn("cloudflare", r2_cmd)
        self.assertIn("--endpoint-url", r2_cmd)
        self.assertIn(CLOUDFLARE_ENDPOINT, r2_cmd)


class TestSyncRepoToS3Mirror(IsolatedAsyncioTestCase):
    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3._verify_synced_to_mirrors", new_callable=AsyncMock)
    @patch("pyartcd.s3.sync_dir_to_s3_mirror", new_callable=AsyncMock)
    async def test_calls_verify_after_sync(self, mock_sync_dir: AsyncMock, mock_verify: AsyncMock):
        await sync_repo_to_s3_mirror.__wrapped__("/local", "/enterprise/reposync/5.0")
        mock_verify.assert_awaited_once_with("/local", "/enterprise/reposync/5.0")

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3._verify_synced_to_mirrors", new_callable=AsyncMock)
    @patch("pyartcd.s3.sync_dir_to_s3_mirror", new_callable=AsyncMock)
    async def test_skips_verify_on_dry_run(self, mock_sync_dir: AsyncMock, mock_verify: AsyncMock):
        await sync_repo_to_s3_mirror.__wrapped__("/local", "/enterprise/reposync/5.0", dry_run=True)
        mock_verify.assert_not_awaited()

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3._verify_synced_to_mirrors", new_callable=AsyncMock)
    @patch("pyartcd.s3.sync_dir_to_s3_mirror", new_callable=AsyncMock)
    async def test_three_passes_with_remove_old(self, mock_sync_dir: AsyncMock, mock_verify: AsyncMock):
        await sync_repo_to_s3_mirror.__wrapped__("/local", "/enterprise/reposync/5.0", remove_old=True)
        self.assertEqual(mock_sync_dir.await_count, 3)

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3._verify_synced_to_mirrors", new_callable=AsyncMock)
    @patch("pyartcd.s3.sync_dir_to_s3_mirror", new_callable=AsyncMock)
    async def test_two_passes_without_remove_old(self, mock_sync_dir: AsyncMock, mock_verify: AsyncMock):
        await sync_repo_to_s3_mirror.__wrapped__("/local", "/enterprise/reposync/5.0", remove_old=False)
        self.assertEqual(mock_sync_dir.await_count, 2)

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3._verify_synced_to_mirrors", new_callable=AsyncMock)
    @patch("pyartcd.s3.sync_dir_to_s3_mirror", new_callable=AsyncMock)
    async def test_retries_on_verify_failure(self, mock_sync_dir: AsyncMock, mock_verify: AsyncMock):
        mock_verify.side_effect = [IOError("R2 mismatch"), None]
        await sync_repo_to_s3_mirror("/local", "/enterprise/reposync/5.0")
        self.assertEqual(mock_verify.await_count, 2)
        self.assertEqual(mock_sync_dir.await_count, 6)  # 3 passes x 2 attempts

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3._verify_synced_to_mirrors", new_callable=AsyncMock)
    @patch("pyartcd.s3.sync_dir_to_s3_mirror", new_callable=AsyncMock)
    async def test_retries_on_sync_failure(self, mock_sync_dir: AsyncMock, mock_verify: AsyncMock):
        mock_sync_dir.side_effect = [Exception("S3 down"), None, None]
        await sync_repo_to_s3_mirror("/local", "/enterprise/reposync/5.0", remove_old=False)
        self.assertEqual(mock_sync_dir.await_count, 3)  # 1 fail + 2 passes on retry
        mock_verify.assert_awaited_once()

    @patch.dict("os.environ", {"CLOUDFLARE_ENDPOINT": CLOUDFLARE_ENDPOINT})
    @patch("pyartcd.s3._verify_synced_to_mirrors", new_callable=AsyncMock)
    @patch("pyartcd.s3.sync_dir_to_s3_mirror", new_callable=AsyncMock)
    async def test_raises_after_all_retries_exhausted(self, mock_sync_dir: AsyncMock, mock_verify: AsyncMock):
        mock_verify.side_effect = IOError("R2 mismatch")
        with self.assertRaises(IOError):
            await sync_repo_to_s3_mirror("/local", "/enterprise/reposync/5.0", remove_old=False)
        self.assertEqual(mock_verify.await_count, 3)  # 3 attempts then give up
