import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.sync_rhcos_specialized import SyncRhcosSpecializedPipeline


class TestSyncRhcosSpecializedPipeline(IsolatedAsyncioTestCase):
    def _make_pipeline(self, sync_type="bfb", stream=None, build="9.6.20250707-1.3"):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.logger = MagicMock()
        runtime.working_dir = Path(self.tmpdir)

        if stream is None:
            stream = "4.20-9.6-nvidia-bfb" if sync_type == "bfb" else "rhel-9.6-te-preview"

        return SyncRhcosSpecializedPipeline(runtime, stream, build, sync_type)

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.tmpdir = self._tmpdir.name

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_init_and_version_extraction(self):
        # BFB
        bfb = self._make_pipeline(sync_type="bfb", stream="4.20-9.6-nvidia-bfb")
        self.assertEqual(bfb.major_minor, "4.20")
        self.assertEqual(bfb.arch, "aarch64")
        self.assertIn("rhcos-nvidiabfb", bfb.s3_base_url)

        # Confidential
        conf = self._make_pipeline(sync_type="confidential", stream="rhel-9.6-te-preview")
        self.assertEqual(conf.major_minor, "9.6")
        self.assertEqual(conf.arch, "x86_64")
        self.assertIn("rhcos-confidential", conf.s3_base_url)

        # OCP4NV
        ocp4nv = self._make_pipeline(sync_type="ocp4nv", stream="rhel-10.2-ocp4nv")
        self.assertEqual(ocp4nv.major_minor, "10.2")
        self.assertEqual(ocp4nv.arch, "aarch64")
        self.assertEqual(ocp4nv.allowlist, {"live-iso"})
        self.assertIn("rhcos-ocp4nv", ocp4nv.s3_base_url)

        # Invalid type
        with self.assertRaises(ValueError):
            SyncRhcosSpecializedPipeline(bfb.runtime, "test", "build", "invalid")

    def test_build_destinations(self):
        # BFB GA
        bfb = self._make_pipeline(sync_type="bfb")
        bfb.ocp_version = "4.20.1"
        bfb.is_prerelease = False
        versioned, latest = bfb.build_bfb_destinations()
        self.assertIn("4.20/4.20.1/", versioned[0])
        self.assertIn("4.20/latest", latest[0])

        # BFB prerelease
        bfb.ocp_version = "4.20.0-ec.1"
        bfb.is_prerelease = True
        versioned, latest = bfb.build_bfb_destinations()
        self.assertIn("pre-release/4.20.0-ec.1/", versioned[0])
        self.assertIn("pre-release/latest-4.20", latest[1])

        # Confidential
        conf = self._make_pipeline(sync_type="confidential")
        versioned, latest = conf.build_rhel_based_destinations()
        self.assertIn("9.6/9.6.20250707-1.3", versioned[0])
        self.assertIn("9.6/latest", latest[0])
        self.assertTrue(latest[1].endswith("/latest"))

        # OCP4NV uses the same RHEL-version-based structure as confidential images.
        ocp4nv = self._make_pipeline(sync_type="ocp4nv", stream="rhel-10.2-ocp4nv", build="10.2.20261001-0101")
        versioned, latest = ocp4nv.build_mirror_destinations()
        self.assertIn("10.2/10.2.20261001-0101", versioned[0])
        self.assertIn("10.2/latest", latest[0])

    def test_stable_filenames(self):
        pipeline = self._make_pipeline(sync_type="confidential", build="9.6.20260309-0")
        pipeline.artifacts_dir.mkdir(parents=True, exist_ok=True)
        (pipeline.artifacts_dir / "rhcos-9.6.20260309-0-azure.x86_64.vhd.gz").write_bytes(b"data")

        pipeline.add_stable_files_to_artifacts_dir()

        self.assertTrue((pipeline.artifacts_dir / "rhcos-azure.x86_64.vhd.gz").exists())

    def test_stable_filenames_invalid_pattern(self):
        # Test warning when filename doesn't match expected pattern
        pipeline = self._make_pipeline(sync_type="confidential", build="9.6.20260309-0")
        pipeline.artifacts_dir.mkdir(parents=True, exist_ok=True)

        # Create file without build ID pattern
        (pipeline.artifacts_dir / "unexpected-filename.tar.gz").write_bytes(b"data")

        pipeline.add_stable_files_to_artifacts_dir()

        # Should log warning about inability to create stable filename
        pipeline.runtime.logger.warning.assert_called()
        warning_msg = pipeline.runtime.logger.warning.call_args[0][0]
        self.assertIn("Could not construct stable filename", warning_msg)

        # Should not create any stable copy
        self.assertEqual(len(list(pipeline.artifacts_dir.iterdir())), 1)

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest(self, mock_cmd):
        pipeline = self._make_pipeline(sync_type="confidential", stream="rhel-9.6-te-preview")

        # No higher version
        mock_cmd.return_value = (0, "PRE 9.5/\nPRE 9.6/\n", "")
        self.assertTrue(await pipeline.should_update_global_latest())

        # Higher version exists (9.8 > 9.6, handles gaps)
        mock_cmd.return_value = (0, "PRE 9.6/\nPRE 9.8/\n", "")
        self.assertFalse(await pipeline.should_update_global_latest())

        # Empty bucket
        mock_cmd.return_value = (1, "", "")
        self.assertTrue(await pipeline.should_update_global_latest())

        # Ignores non-version dirs
        mock_cmd.return_value = (0, "PRE latest/\nPRE 9.6/\n", "")
        self.assertTrue(await pipeline.should_update_global_latest())

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest_double_digit_minor(self, mock_cmd):
        # Test that 4.10 > 4.9 (numeric, not lexicographic)
        mock_cmd.return_value = (0, "PRE 4.9/\nPRE 4.10/\n", "")
        pipeline = self._make_pipeline(sync_type="bfb", stream="4.9-9.6-nvidia-bfb")
        pipeline.is_prerelease = False

        self.assertFalse(await pipeline.should_update_global_latest())

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest_prerelease(self, mock_cmd):
        # Test prerelease checks latest-* directories
        mock_cmd.return_value = (0, "PRE latest-4.19/\nPRE latest-4.20/\n", "")
        pipeline = self._make_pipeline(sync_type="bfb", stream="4.19-9.6-nvidia-bfb")
        pipeline.is_prerelease = True

        # Should NOT update global latest since latest-4.20 exists
        self.assertFalse(await pipeline.should_update_global_latest())

        # Should update when only current version exists
        mock_cmd.return_value = (0, "PRE latest-4.19/\n", "")
        self.assertTrue(await pipeline.should_update_global_latest())

    @patch("artcommonlib.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_should_update_global_latest_error(self, mock_cmd):
        # Test error handling when aws s3 ls fails with stderr
        mock_cmd.return_value = (1, "", "AccessDenied: Access Denied")
        pipeline = self._make_pipeline(sync_type="confidential")

        with self.assertRaises(ChildProcessError) as ctx:
            await pipeline.should_update_global_latest()

        self.assertIn("AccessDenied", str(ctx.exception))

    def test_discover_artifacts(self):
        pipeline = self._make_pipeline(sync_type="confidential")
        pipeline.meta_json = {
            "images": {
                "azure": {"path": "rhcos-9.6.20260309-0-azure.x86_64.vhd.gz"},
                "metal": {"path": "rhcos-9.6.20260309-0-metal.x86_64.raw.gz"},  # Not in allowlist
            }
        }

        artifacts = pipeline.discover_artifacts()

        self.assertEqual(len(artifacts), 1)
        self.assertIn("azure", artifacts[0])
        self.assertNotIn("metal", "".join(artifacts))

    def test_discover_artifacts_missing_from_allowlist(self):
        # Test warning when allowlist item is missing from meta.json
        pipeline = self._make_pipeline(sync_type="confidential")
        pipeline.meta_json = {
            "images": {
                "azure": {"path": "rhcos-9.6.20260309-0-azure.x86_64.vhd.gz"},
                # "qemu" is in allowlist but missing from meta.json
            }
        }

        artifacts = pipeline.discover_artifacts()

        # Should still discover azure
        self.assertEqual(len(artifacts), 1)
        self.assertIn("azure", artifacts[0])

        # Should log warning about missing qemu
        pipeline.runtime.logger.warning.assert_called()
        warning_msg = pipeline.runtime.logger.warning.call_args[0][0]
        self.assertIn("qemu", warning_msg)

    def test_discover_artifacts_bfb_allowlist(self):
        # Test BFB discovers correct artifacts and filters others
        pipeline = self._make_pipeline(sync_type="bfb")
        pipeline.meta_json = {
            "images": {
                "nvidiabfb": {"path": "rhcos-9.6.20250707-1.3-nvidiabfb.aarch64.bfb"},
                "ostree": {"path": "rhcos-9.6.20250707-1.3-ostree.aarch64.tar"},
                "oci-manifest": {"path": "rhcos-9.6.20250707-1.3-oci-manifest.json"},
                "qemu": {"path": "rhcos-9.6.20250707-1.3-qemu.aarch64.qcow2.gz"},  # Not in BFB allowlist
            }
        }

        artifacts = pipeline.discover_artifacts()

        # Should discover BFB allowlisted items
        self.assertEqual(len(artifacts), 3)
        artifact_str = "".join(artifacts)
        self.assertIn("nvidiabfb", artifact_str)
        self.assertIn("ostree", artifact_str)
        self.assertIn("oci-manifest", artifact_str)
        # Should NOT include qemu (confidential allowlist, not BFB)
        self.assertNotIn("qemu", artifact_str)

    def test_discover_artifacts_ocp4nv_allowlist(self):
        pipeline = self._make_pipeline(sync_type="ocp4nv", stream="rhel-10.2-ocp4nv")
        pipeline.meta_json = {
            "images": {
                "live-iso": {"path": "rhcos-10.2.20261001-0101-live-iso.aarch64.iso"},
                "live-kernel": {"path": "rhcos-10.2.20261001-0101-live-kernel.aarch64"},
            }
        }

        artifacts = pipeline.discover_artifacts()

        self.assertEqual(artifacts, ["rhcos-10.2.20261001-0101-live-iso.aarch64.iso"])

    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.sync_artifacts",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.download_all_artifacts",
        new_callable=AsyncMock,
    )
    async def test_run_fails_when_no_allowlisted_artifacts(self, mock_download, mock_sync):
        pipeline = self._make_pipeline(sync_type="ocp4nv", stream="rhel-10.2-ocp4nv")
        pipeline.fetch_rhcos_metadata = AsyncMock(
            return_value={"images": {"live-kernel": {"path": "rhcos-live-kernel.aarch64"}}}
        )

        with self.assertRaisesRegex(ValueError, r"No allowlisted artifacts found.*rhel-10\.2-ocp4nv.*meta\.json"):
            await pipeline.run()

        mock_download.assert_not_awaited()
        mock_sync.assert_not_awaited()

    @patch("pyartcd.pipelines.sync_rhcos_specialized.util.mirror_to_s3", new_callable=AsyncMock)
    async def test_sync_to_destination(self, mock_mirror):
        pipeline = self._make_pipeline(sync_type="confidential")
        pipeline.runtime.dry_run = True
        pipeline.artifacts_dir.mkdir(parents=True, exist_ok=True)

        await pipeline.sync_to_destination("s3://bucket/path/")

        mock_mirror.assert_awaited_once()
        self.assertEqual(mock_mirror.call_args[1]["dry_run"], True)
        self.assertEqual(mock_mirror.call_args[1]["delete"], True)

    @patch("pyartcd.pipelines.sync_rhcos_specialized.util.mirror_to_s3", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.should_update_global_latest",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.download_all_artifacts",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.fetch_rhcos_metadata",
        new_callable=AsyncMock,
    )
    async def test_run_confidential_full(self, mock_fetch, mock_download, mock_should_update, mock_mirror):
        mock_fetch.return_value = {"images": {"azure": {"path": "rhcos-9.6.20260309-0-azure.x86_64.vhd.gz"}}}
        mock_should_update.return_value = True
        mock_download.side_effect = lambda artifacts: pipeline.artifacts_dir.mkdir(parents=True, exist_ok=True)

        pipeline = self._make_pipeline(sync_type="confidential", build="9.6.20260309-0")
        await pipeline.run()

        # Verify order and count
        mock_fetch.assert_awaited_once()
        mock_download.assert_awaited_once()
        mock_should_update.assert_awaited_once()
        self.assertEqual(mock_mirror.await_count, 3)  # versioned + version-latest + global-latest

        # Verify paths
        sync_calls = [call[1]["dest"] for call in mock_mirror.call_args_list]
        self.assertIn("9.6/9.6.20260309-0", sync_calls[0])
        self.assertIn("9.6/latest", sync_calls[1])
        self.assertTrue(sync_calls[2].endswith("/latest"))

    @patch("pyartcd.pipelines.sync_rhcos_specialized.util.mirror_to_s3", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.should_update_global_latest",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.download_all_artifacts",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.fetch_rhcos_metadata",
        new_callable=AsyncMock,
    )
    async def test_run_bfb_full(self, mock_fetch, mock_download, mock_should_update, mock_mirror):
        mock_fetch.return_value = {
            "coreos-assembler.oci-imported-labels": {"rhcos.version": "4.20.1"},
            "images": {"nvidiabfb": {"path": "rhcos-9.6.20250707-1.3-nvidiabfb.aarch64.bfb"}},
        }
        mock_should_update.return_value = True
        mock_download.side_effect = lambda artifacts: pipeline.artifacts_dir.mkdir(parents=True, exist_ok=True)

        pipeline = self._make_pipeline(sync_type="bfb")
        await pipeline.run()

        self.assertEqual(pipeline.ocp_version, "4.20.1")
        self.assertFalse(pipeline.is_prerelease)
        self.assertEqual(mock_mirror.await_count, 3)

    @patch("pyartcd.pipelines.sync_rhcos_specialized.util.mirror_to_s3", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.should_update_global_latest",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.download_all_artifacts",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.fetch_rhcos_metadata",
        new_callable=AsyncMock,
    )
    async def test_run_skips_global_latest_when_higher_exists(
        self, mock_fetch, mock_download, mock_should_update, mock_mirror
    ):
        mock_fetch.return_value = {"images": {"azure": {"path": "rhcos-9.6.20260309-0-azure.x86_64.vhd.gz"}}}
        mock_should_update.return_value = False
        mock_download.side_effect = lambda artifacts: pipeline.artifacts_dir.mkdir(parents=True, exist_ok=True)

        pipeline = self._make_pipeline(sync_type="confidential", build="9.6.20260309-0")
        await pipeline.run()

        self.assertEqual(mock_mirror.await_count, 2)  # Only versioned + version-latest, no global

    @patch("pyartcd.pipelines.sync_rhcos_specialized.util.mirror_to_s3", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.should_update_global_latest",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.download_all_artifacts",
        new_callable=AsyncMock,
    )
    @patch(
        "pyartcd.pipelines.sync_rhcos_specialized.SyncRhcosSpecializedPipeline.fetch_rhcos_metadata",
        new_callable=AsyncMock,
    )
    async def test_sync_artifacts_stable_files_timing(self, mock_fetch, mock_download, mock_should_update, mock_mirror):
        # Verify stable files are added after versioned sync but before latest sync
        mock_fetch.return_value = {"images": {"azure": {"path": "rhcos-9.6.20260309-0-azure.x86_64.vhd.gz"}}}
        mock_should_update.return_value = True

        def create_artifacts(artifacts):
            pipeline.artifacts_dir.mkdir(parents=True, exist_ok=True)
            (pipeline.artifacts_dir / "rhcos-9.6.20260309-0-azure.x86_64.vhd.gz").write_bytes(b"data")

        mock_download.side_effect = create_artifacts

        call_log = []

        async def track_sync(source, dest, dry_run, delete):
            # Track which files exist at each sync call
            files = [f.name for f in Path(source).iterdir()]
            call_log.append({"dest": dest, "files": files})

        mock_mirror.side_effect = track_sync

        pipeline = self._make_pipeline(sync_type="confidential", build="9.6.20260309-0")
        await pipeline.run()

        # First sync (versioned) should have only original file
        self.assertIn("9.6/9.6.20260309-0", call_log[0]["dest"])
        self.assertIn("rhcos-9.6.20260309-0-azure.x86_64.vhd.gz", call_log[0]["files"])
        self.assertNotIn("rhcos-azure.x86_64.vhd.gz", call_log[0]["files"])

        # Second and third syncs (latest dirs) should have both original and stable
        for sync_call in call_log[1:]:
            self.assertIn("rhcos-9.6.20260309-0-azure.x86_64.vhd.gz", sync_call["files"])
            self.assertIn("rhcos-azure.x86_64.vhd.gz", sync_call["files"])
