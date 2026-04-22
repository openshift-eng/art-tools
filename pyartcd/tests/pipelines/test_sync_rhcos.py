import hashlib
import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.sync_rhcos import SyncRhcosPipeline


class TestSyncRhcosPipeline(IsolatedAsyncioTestCase):
    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.logger = MagicMock()
        runtime.working_dir = Path(self.tmpdir)

        defaults = dict(
            runtime=runtime,
            arch="x86_64",
            build_id="9.6.20250121-0",
            version="4.19.0-ec.0",
            mirror_prefix="pre-release",
            base_dir="/pub/openshift-v4/x86_64/dependencies/rhcos",
            synclist=str(Path(self.tmpdir) / "synclist.txt"),
            no_latest=False,
            signing_env=None,
        )
        defaults.update(overrides)
        return SyncRhcosPipeline(**defaults)

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmpdir.name

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_parse_synclist(self):
        synclist = Path(self.tmpdir) / "synclist.txt"
        synclist.write_text(
            "https://example.com/rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz\nhttps://example.com/rhcos-9.6.20250121-0-metal.x86_64.raw.gz\n"
        )
        pipeline = self._make_pipeline(synclist=str(synclist))
        urls = pipeline._parse_synclist()
        self.assertEqual(len(urls), 2)
        self.assertIn("qemu", urls[0])

    def test_parse_synclist_skips_blank_lines(self):
        synclist = Path(self.tmpdir) / "synclist.txt"
        synclist.write_text("https://example.com/file1\n\nhttps://example.com/file2\n\n")
        pipeline = self._make_pipeline(synclist=str(synclist))
        urls = pipeline._parse_synclist()
        self.assertEqual(len(urls), 2)

    def test_parse_synclist_missing_file(self):
        pipeline = self._make_pipeline(synclist="/nonexistent/synclist.txt")
        with self.assertRaises(FileNotFoundError):
            pipeline._parse_synclist()

    def test_rename_with_version(self):
        pipeline = self._make_pipeline()
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        (pipeline.staging_dir / "rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz").write_bytes(b"qemu-data")
        (pipeline.staging_dir / "rhcos-9.6.20250121-0-metal.x86_64.raw.gz").write_bytes(b"metal-data")
        (pipeline.staging_dir / "unrelated-file.txt").write_bytes(b"other")

        pipeline._rename_with_version()

        self.assertTrue((pipeline.staging_dir / "rhcos-4.19.0-ec.0-x86_64-qemu.x86_64.qcow2.gz").exists())
        self.assertTrue((pipeline.staging_dir / "rhcos-4.19.0-ec.0-x86_64-metal.x86_64.raw.gz").exists())

        qemu_link = pipeline.staging_dir / "rhcos-qemu.x86_64.qcow2.gz"
        self.assertTrue(qemu_link.is_symlink())
        self.assertEqual(os.readlink(qemu_link), "rhcos-4.19.0-ec.0-x86_64-qemu.x86_64.qcow2.gz")

        self.assertTrue((pipeline.staging_dir / "unrelated-file.txt").exists())
        self.assertFalse((pipeline.staging_dir / "unrelated-file.txt").is_symlink())

    def test_rename_with_version_arch_already_in_version(self):
        pipeline = self._make_pipeline(version="4.19.0-ec.0-x86_64")
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        (pipeline.staging_dir / "rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz").write_bytes(b"data")
        pipeline._rename_with_version()

        self.assertTrue((pipeline.staging_dir / "rhcos-4.19.0-ec.0-x86_64-qemu.x86_64.qcow2.gz").exists())

    def test_create_installer_symlinks(self):
        pipeline = self._make_pipeline()
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        versioned = pipeline.staging_dir / "rhcos-4.19.0-live-iso.x86_64.iso"
        versioned.write_bytes(b"iso-data")
        live_link = pipeline.staging_dir / "rhcos-live-iso.x86_64.iso"
        live_link.symlink_to("rhcos-4.19.0-live-iso.x86_64.iso")

        pipeline._create_installer_symlinks()

        installer_link = pipeline.staging_dir / "rhcos-installer-iso.x86_64.iso"
        self.assertTrue(installer_link.is_symlink())
        self.assertEqual(os.readlink(installer_link), "rhcos-4.19.0-live-iso.x86_64.iso")

    def test_create_installer_symlinks_no_overwrite(self):
        pipeline = self._make_pipeline()
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        versioned = pipeline.staging_dir / "rhcos-4.19.0-live-iso.x86_64.iso"
        versioned.write_bytes(b"iso-data")
        live_link = pipeline.staging_dir / "rhcos-live-iso.x86_64.iso"
        live_link.symlink_to("rhcos-4.19.0-live-iso.x86_64.iso")

        existing = pipeline.staging_dir / "rhcos-installer-iso.x86_64.iso"
        existing.write_bytes(b"existing")

        pipeline._create_installer_symlinks()

        self.assertFalse(existing.is_symlink())

    def test_generate_rhcos_id_txt(self):
        pipeline = self._make_pipeline()
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        pipeline._generate_rhcos_id_txt()

        id_file = pipeline.staging_dir / "rhcos-id.txt"
        self.assertTrue(id_file.exists())
        self.assertEqual(id_file.read_text(), "9.6.20250121-0\n")

    def test_generate_sha256sum(self):
        pipeline = self._make_pipeline()
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        file_a = pipeline.staging_dir / "alpha.bin"
        file_a.write_bytes(b"alpha-content")
        file_b = pipeline.staging_dir / "beta.bin"
        file_b.write_bytes(b"beta-content")

        link = pipeline.staging_dir / "gamma.bin"
        link.symlink_to("alpha.bin")

        (pipeline.staging_dir / "rhcos-id.txt").write_text("some-id\n")

        pipeline._generate_sha256sum()

        sha256_file = pipeline.staging_dir / "sha256sum.txt"
        self.assertTrue(sha256_file.exists())

        lines = sha256_file.read_text().strip().split("\n")
        self.assertEqual(len(lines), 3)

        entries = {}
        for line in lines:
            sha, name = line.split("  ", 1)
            entries[name] = sha

        self.assertIn("alpha.bin", entries)
        self.assertIn("beta.bin", entries)
        self.assertIn("gamma.bin", entries)
        self.assertEqual(entries["alpha.bin"], entries["gamma.bin"])
        self.assertEqual(entries["alpha.bin"], hashlib.sha256(b"alpha-content").hexdigest())

        self.assertNotIn("rhcos-id.txt", entries)
        self.assertNotIn("sha256sum.txt", entries)

    def test_generate_sha256sum_sorted(self):
        pipeline = self._make_pipeline()
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        (pipeline.staging_dir / "zzz.bin").write_bytes(b"z")
        (pipeline.staging_dir / "aaa.bin").write_bytes(b"a")
        (pipeline.staging_dir / "mmm.bin").write_bytes(b"m")

        pipeline._generate_sha256sum()

        lines = (pipeline.staging_dir / "sha256sum.txt").read_text().strip().split("\n")
        names = [line.split("  ", 1)[1] for line in lines]
        self.assertEqual(names, sorted(names))

    def test_extract_major_minor(self):
        pipeline = self._make_pipeline(version="4.19.0-ec.0")
        self.assertEqual(pipeline._extract_major_minor(), "4.19")

        pipeline = self._make_pipeline(version="4.21.3")
        self.assertEqual(pipeline._extract_major_minor(), "4.21")

    def test_extract_major_minor_invalid(self):
        pipeline = self._make_pipeline(version="invalid")
        with self.assertRaises(ValueError):
            pipeline._extract_major_minor()

    def test_next_major_minor(self):
        self.assertEqual(SyncRhcosPipeline._next_major_minor("4.19"), "4.20")
        self.assertEqual(SyncRhcosPipeline._next_major_minor("4.9"), "4.10")

    @patch("pyartcd.pipelines.sync_rhcos.util.mirror_to_s3", new_callable=AsyncMock)
    async def test_sync_to_s3(self, mock_mirror):
        pipeline = self._make_pipeline()
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        await pipeline._sync_to_s3("/some/source", "s3://bucket/dest/")
        mock_mirror.assert_awaited_once_with(
            source="/some/source",
            dest="s3://bucket/dest/",
            dry_run=False,
            delete=True,
        )

    @patch("pyartcd.pipelines.sync_rhcos.util.mirror_to_s3", new_callable=AsyncMock)
    async def test_sync_to_s3_dry_run(self, mock_mirror):
        pipeline = self._make_pipeline()
        pipeline.runtime.dry_run = True
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        await pipeline._sync_to_s3("/some/source", "s3://bucket/dest/")
        mock_mirror.assert_awaited_once_with(
            source="/some/source",
            dest="s3://bucket/dest/",
            dry_run=True,
            delete=True,
        )

    @patch("pyartcd.pipelines.sync_rhcos.AsyncSignatory")
    async def test_sign_sha256sum(self, mock_signatory_cls):
        pipeline = self._make_pipeline(signing_env="prod")
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)
        sha256_file = pipeline.staging_dir / "sha256sum.txt"
        sha256_file.write_text("abc123  file.bin\n")

        mock_signatory = AsyncMock()
        mock_signatory_cls.return_value = mock_signatory
        mock_signatory.__aenter__ = AsyncMock(return_value=mock_signatory)
        mock_signatory.__aexit__ = AsyncMock(return_value=False)

        with patch.dict(os.environ, {"SIGNING_CERT": "/fake/cert", "SIGNING_KEY": "/fake/key"}):
            await pipeline._sign_sha256sum()

        mock_signatory_cls.assert_called_once()
        _, kwargs = mock_signatory_cls.call_args
        self.assertEqual(kwargs["sig_keyname"], "redhatrelease2")

        mock_signatory.sign_message_digest.assert_awaited_once()
        call_kwargs = mock_signatory.sign_message_digest.call_args[1]
        self.assertEqual(call_kwargs["product"], "openshift")
        self.assertEqual(call_kwargs["release_name"], "4.19.0-ec.0")

    @patch("pyartcd.pipelines.sync_rhcos.AsyncSignatory")
    async def test_sign_sha256sum_stage(self, mock_signatory_cls):
        pipeline = self._make_pipeline(signing_env="stage")
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)
        (pipeline.staging_dir / "sha256sum.txt").write_text("abc123  file.bin\n")

        mock_signatory = AsyncMock()
        mock_signatory_cls.return_value = mock_signatory
        mock_signatory.__aenter__ = AsyncMock(return_value=mock_signatory)
        mock_signatory.__aexit__ = AsyncMock(return_value=False)

        with patch.dict(os.environ, {"SIGNING_CERT": "/fake/cert", "SIGNING_KEY": "/fake/key"}):
            await pipeline._sign_sha256sum()

        _, kwargs = mock_signatory_cls.call_args
        self.assertEqual(kwargs["sig_keyname"], "beta2")

    async def test_sign_sha256sum_skips_without_creds(self):
        pipeline = self._make_pipeline(signing_env="prod")
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)
        (pipeline.staging_dir / "sha256sum.txt").write_text("abc123  file.bin\n")

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SIGNING_CERT", None)
            os.environ.pop("SIGNING_KEY", None)
            await pipeline._sign_sha256sum()

        pipeline.runtime.logger.warning.assert_called()

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest_no_next_version(self, mock_cmd):
        mock_cmd.return_value = (1, "", "")
        pipeline = self._make_pipeline()
        result = await pipeline._should_update_global_latest("4.20")
        self.assertTrue(result)

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest_next_exists(self, mock_cmd):
        mock_cmd.return_value = (0, "PRE 4.20.0/\n", "")
        pipeline = self._make_pipeline()
        result = await pipeline._should_update_global_latest("4.20")
        self.assertFalse(result)

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest_empty_stdout(self, mock_cmd):
        mock_cmd.return_value = (0, "", "")
        pipeline = self._make_pipeline()
        result = await pipeline._should_update_global_latest("4.20")
        self.assertTrue(result)

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest_error(self, mock_cmd):
        mock_cmd.return_value = (1, "", "access denied")
        pipeline = self._make_pipeline()
        with self.assertRaises(ChildProcessError):
            await pipeline._should_update_global_latest("4.20")

    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._sync_to_s3", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._should_update_global_latest", new_callable=AsyncMock)
    async def test_update_latest_pre_release(self, mock_should_update, mock_sync):
        mock_should_update.return_value = True
        pipeline = self._make_pipeline(mirror_prefix="pre-release")
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        await pipeline._update_latest()

        sync_calls = mock_sync.call_args_list
        dests = [call.args[1] for call in sync_calls]
        self.assertTrue(any("latest-4.19" in d for d in dests))
        self.assertTrue(any(d.endswith("/latest/") for d in dests))

    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._sync_to_s3", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._should_update_global_latest", new_callable=AsyncMock)
    async def test_update_latest_pre_release_skip_global(self, mock_should_update, mock_sync):
        mock_should_update.return_value = False
        pipeline = self._make_pipeline(mirror_prefix="pre-release")
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        await pipeline._update_latest()

        sync_calls = mock_sync.call_args_list
        dests = [call.args[1] for call in sync_calls]
        self.assertTrue(any("latest-4.19" in d for d in dests))
        self.assertFalse(any(d.endswith("/latest/") for d in dests))

    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._sync_to_s3", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._should_update_global_latest", new_callable=AsyncMock)
    async def test_update_latest_stable(self, mock_should_update, mock_sync):
        mock_should_update.return_value = True
        pipeline = self._make_pipeline(mirror_prefix="4.19", version="4.19.3")
        pipeline.staging_dir.mkdir(parents=True, exist_ok=True)

        await pipeline._update_latest()

        sync_calls = mock_sync.call_args_list
        dests = [call.args[1] for call in sync_calls]
        self.assertTrue(any("4.19/latest/" in d for d in dests))
        self.assertTrue(any(d.endswith("/rhcos/latest/") for d in dests))

    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._update_latest", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._sign_sha256sum", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._sync_to_s3", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._download_images", new_callable=AsyncMock)
    async def test_run_full_pipeline(self, mock_download, mock_sync, mock_sign, mock_latest):
        synclist = Path(self.tmpdir) / "synclist.txt"
        synclist.write_text("https://example.com/rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz\n")

        pipeline = self._make_pipeline(signing_env="prod", synclist=str(synclist))

        def fake_download(urls):
            pipeline.staging_dir.mkdir(parents=True, exist_ok=True)
            (pipeline.staging_dir / "rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz").write_bytes(b"data")

        mock_download.side_effect = fake_download

        await pipeline.run()

        mock_download.assert_awaited_once()
        mock_sign.assert_awaited_once()
        mock_sync.assert_awaited()
        mock_latest.assert_awaited_once()

    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._update_latest", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._sync_to_s3", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._download_images", new_callable=AsyncMock)
    async def test_run_no_signing(self, mock_download, mock_sync, mock_latest):
        synclist = Path(self.tmpdir) / "synclist.txt"
        synclist.write_text("https://example.com/rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz\n")

        pipeline = self._make_pipeline(signing_env=None, synclist=str(synclist))

        def fake_download(urls):
            pipeline.staging_dir.mkdir(parents=True, exist_ok=True)
            (pipeline.staging_dir / "rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz").write_bytes(b"data")

        mock_download.side_effect = fake_download

        await pipeline.run()

        mock_sync.assert_awaited()
        mock_latest.assert_awaited_once()

    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._sync_to_s3", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.sync_rhcos.SyncRhcosPipeline._download_images", new_callable=AsyncMock)
    async def test_run_no_latest(self, mock_download, mock_sync):
        synclist = Path(self.tmpdir) / "synclist.txt"
        synclist.write_text("https://example.com/rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz\n")

        pipeline = self._make_pipeline(no_latest=True, synclist=str(synclist))

        def fake_download(urls):
            pipeline.staging_dir.mkdir(parents=True, exist_ok=True)
            (pipeline.staging_dir / "rhcos-9.6.20250121-0-qemu.x86_64.qcow2.gz").write_bytes(b"data")

        mock_download.side_effect = fake_download

        await pipeline.run()

        sync_calls = mock_sync.call_args_list
        self.assertEqual(len(sync_calls), 1)
        self.assertIn(f"pre-release/{pipeline.version}/", sync_calls[0].args[1])
