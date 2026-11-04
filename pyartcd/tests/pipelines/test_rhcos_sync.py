"""
Regression coverage for mirroring every RHCOS variant without path collisions.

Run with: uv run pytest pyartcd/tests/pipelines/test_rhcos_sync.py
"""

import json
import tempfile
from contextlib import nullcontext
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from pyartcd.pipelines.rhcos_sync import RhcosSyncPipeline, compute_rhcos_stream
from pyartcd.pipelines.sync_rhcos import SyncRhcosPipeline


class TestRhcosSyncVariants(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        runtime = MagicMock()
        runtime.working_dir = Path(self.tmpdir.name)
        runtime.dry_run = False
        self.pipeline = RhcosSyncPipeline(runtime, "5.0.0-x86_64")
        self.pipeline._parse_release_tag()
        self.pipeline.name = "5.0.0"
        self.pipeline.mirror_prefix = "5.0"
        self.pipeline.rhcos_build = "9.8.20261026-0"
        self.pipeline.extra_streams = {
            "rhel-10": self._stream("10.2.20261026-0"),
            "rhel-9": self._stream("9.8.20261026-0"),
        }

    @staticmethod
    def _stream(build_id: str) -> dict:
        """
        Create stream metadata containing a boot image build ID.
        """
        return {"architectures": {"x86_64": {"artifacts": {"qemu": {"release": build_id}}}}}

    def _mock_http(self, existing: dict[str, str]) -> MagicMock:
        """
        Serve mirror markers and distinct source artifacts without network access.
        """
        session = MagicMock()

        def get(url: str) -> MagicMock:
            """
            Return a mirror marker or a source meta.json response.
            """
            response = MagicMock()
            if url.endswith("rhcos-id.txt"):
                response.status = 200 if url in existing else 404
                response.text = AsyncMock(return_value=existing.get(url, ""))
            else:
                build_id = url.split("/builds/")[1].split("/")[0]
                response.status = 200
                response.json = AsyncMock(
                    return_value={"images": {"qemu": {"path": f"rhcos-{build_id}-qemu.x86_64.qcow2.gz"}}}
                )
            context = MagicMock()
            context.__aenter__ = AsyncMock(return_value=response)
            return context

        session.get.side_effect = get
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=session)
        return context

    async def _run_mirroring(self, existing: dict[str, str] | None = None) -> tuple[list[dict], MagicMock, AsyncMock]:
        """
        Run staging, renaming, checksums, and path selection with mocked external I/O.
        """
        uploads = []

        async def download(pipeline: SyncRhcosPipeline, urls: list[str]) -> None:
            """
            Materialize a different artifact for each variant.
            """
            for url in urls:
                (pipeline.staging_dir / url.rsplit("/", 1)[-1]).write_text(pipeline.build_id)

        async def mirror(**kwargs) -> None:
            """
            Capture the files that would be uploaded before staging changes.
            """
            source = Path(kwargs["source"])
            uploads.append(
                {
                    **kwargs,
                    "build_id": (source / "rhcos-id.txt").read_text().strip(),
                    "checksums": (source / "sha256sum.txt").read_text(),
                    "artifact": (source / "rhcos-qemu.x86_64.qcow2.gz").read_text(),
                }
            )

        http = self._mock_http(existing or {})
        with (
            patch("pyartcd.pipelines.rhcos_sync.aiohttp.ClientSession", return_value=http),
            patch.object(SyncRhcosPipeline, "_download_images", new=download),
            patch.object(SyncRhcosPipeline, "_sign_sha256sum", new_callable=AsyncMock) as sign,
            patch.object(
                SyncRhcosPipeline, "_should_update_global_latest", new_callable=AsyncMock, return_value=True
            ) as guard,
            patch("pyartcd.pipelines.sync_rhcos.util.mirror_to_s3", side_effect=mirror),
        ):
            await self.pipeline._sync_boot_images()
        self.assertEqual(sign.await_count, len({upload["build_id"] for upload in uploads}))
        return uploads, http.__aenter__.return_value, guard

    async def test_stable_variants_use_sibling_prefixes_and_isolated_files(self):
        uploads, session, guard = await self._run_mirroring()
        base = "s3://art-srv-enterprise/pub/openshift-v4/x86_64/dependencies/rhcos"
        self.assertEqual(
            [upload["dest"] for upload in uploads],
            [
                f"{base}/5.0/5.0.0/",
                f"{base}/5.0/latest/",
                f"{base}/latest/",
                f"{base}/5.0-el10/5.0.0/",
                f"{base}/5.0-el10/latest/",
                f"{base}/latest-el10/",
            ],
        )
        for upload in uploads:
            expected_build = "10.2.20261026-0" if "el10" in upload["dest"] else "9.8.20261026-0"
            self.assertEqual(upload["build_id"], expected_build)
            self.assertEqual(upload["artifact"], expected_build)
            self.assertIn("rhcos-5.0.0-x86_64-qemu.x86_64.qcow2.gz", upload["checksums"])
        self.assertNotEqual(uploads[0]["source"], uploads[3]["source"])
        self.assertNotEqual(uploads[0]["checksums"], uploads[3]["checksums"])
        self.assertEqual([call.args[0] for call in guard.await_args_list], ["5.1", "5.1-el10"])
        urls = [call.args[0] for call in session.get.call_args_list]
        self.assertTrue(any("/rhel-10.2/builds/10.2.20261026-0/" in url for url in urls))
        self.assertEqual(sum(url.endswith("meta.json") for url in urls), 2)

    async def test_pre_release_directories_and_latest_are_suffixed(self):
        self.pipeline.name = "5.0.0-rc.8"
        self.pipeline.mirror_prefix = "pre-release"
        uploads, _, guard = await self._run_mirroring()
        base = "s3://art-srv-enterprise/pub/openshift-v4/x86_64/dependencies/rhcos/pre-release"
        self.assertEqual(
            [upload["dest"] for upload in uploads],
            [
                f"{base}/5.0.0-rc.8/",
                f"{base}/latest-5.0/",
                f"{base}/latest/",
                f"{base}/5.0.0-rc.8-el10/",
                f"{base}/latest-5.0-el10/",
                f"{base}/latest-el10/",
            ],
        )
        self.assertEqual(
            [call.args[0] for call in guard.await_args_list],
            ["pre-release/latest-5.1", "pre-release/latest-5.1-el10"],
        )

    async def test_nightly_dev_directories_are_suffixed_without_latest_updates(self):
        self.pipeline.tag = "5.0.0-0.nightly-2026-10-30-010101"
        await self.pipeline._resolve_version_name()
        uploads, _, guard = await self._run_mirroring()
        self.assertEqual(len(uploads), 2)
        self.assertTrue(uploads[0]["dest"].endswith("/pre-release/dev-5.0/"))
        self.assertTrue(uploads[1]["dest"].endswith("/pre-release/dev-5.0-el10/"))
        guard.assert_not_awaited()

    async def test_existing_primary_does_not_skip_missing_el10(self):
        marker = "https://mirror.openshift.com/pub/openshift-v4/x86_64/dependencies/rhcos/5.0/5.0.0/rhcos-id.txt"
        uploads, session, _ = await self._run_mirroring({marker: "9.8.20261026-0\n"})
        self.assertEqual(len(uploads), 3)
        self.assertTrue(all("el10" in upload["dest"] for upload in uploads))
        self.assertFalse(any("/rhel-9.8/builds/" in call.args[0] for call in session.get.call_args_list))

    async def test_matching_markers_skip_both_variants(self):
        base = "https://mirror.openshift.com/pub/openshift-v4/x86_64/dependencies/rhcos"
        uploads, session, _ = await self._run_mirroring(
            {
                f"{base}/5.0/5.0.0/rhcos-id.txt": "9.8.20261026-0",
                f"{base}/5.0-el10/5.0.0/rhcos-id.txt": "10.2.20261026-0",
            }
        )
        self.assertEqual(uploads, [])
        self.assertEqual(session.get.call_count, 2)

    async def test_fast_channel_check_includes_additional_variant_without_publishing(self):
        base = "https://mirror.openshift.com/pub/openshift-v4/x86_64/dependencies/rhcos"
        primary_marker = f"{base}/5.0/5.0.0/rhcos-id.txt"
        el10_marker = f"{base}/5.0-el10/5.0.0/rhcos-id.txt"
        for existing, expected in (
            ({primary_marker: "9.8.20261026-0"}, True),
            ({primary_marker: "9.8.20261026-0", el10_marker: "10.2.20261026-0"}, False),
        ):
            with self.subTest(expected=expected):
                http = self._mock_http(existing)
                with (
                    patch.object(self.pipeline, "_registry_auth", return_value=nullcontext()),
                    patch.object(self.pipeline, "_run_preamble", new_callable=AsyncMock),
                    patch("pyartcd.pipelines.rhcos_sync.aiohttp.ClientSession", return_value=http),
                    patch.object(self.pipeline, "_sync_boot_images", new_callable=AsyncMock) as sync,
                    patch.object(self.pipeline, "_sign_containers", new_callable=AsyncMock) as sign,
                ):
                    self.assertEqual(await self.pipeline.check_needs_happening(), expected)
                session = http.__aenter__.return_value
                self.assertEqual([call.args[0] for call in session.get.call_args_list], [primary_marker, el10_marker])
                sync.assert_not_awaited()
                sign.assert_not_awaited()

    async def test_pre_release_marker_matches_suffixed_upload_path(self):
        self.pipeline.name = "5.0.0-rc.8"
        self.pipeline.mirror_prefix = "pre-release"
        base = "https://mirror.openshift.com/pub/openshift-v4/x86_64/dependencies/rhcos/pre-release"
        uploads, _, _ = await self._run_mirroring(
            {
                f"{base}/5.0.0-rc.8/rhcos-id.txt": "9.8.20261026-0",
                f"{base}/5.0.0-rc.8-el10/rhcos-id.txt": "10.2.20261026-0",
            }
        )
        self.assertEqual(uploads, [])

    async def test_force_bypasses_each_marker(self):
        self.pipeline.force = True
        _, session, _ = await self._run_mirroring()
        self.assertFalse(any(call.args[0].endswith("rhcos-id.txt") for call in session.get.call_args_list))

    async def test_no_latest_and_dry_run_apply_to_all_variants(self):
        self.pipeline.no_latest = True
        self.pipeline.runtime.dry_run = True
        uploads, _, guard = await self._run_mirroring()
        self.assertEqual(len(uploads), 2)
        self.assertTrue(all(upload["dry_run"] for upload in uploads))
        guard.assert_not_awaited()

    async def test_legacy_single_stream_keeps_destination(self):
        self.pipeline.extra_streams = {}
        self.pipeline.no_latest = True
        uploads, _, _ = await self._run_mirroring()
        self.assertEqual(len(uploads), 1)
        self.assertTrue(uploads[0]["dest"].endswith("/rhcos/5.0/5.0.0/"))

    async def test_ocp422_uses_422_el10_prefix(self):
        self.pipeline.ocp_version = "4.22"
        self.pipeline.name = "4.22.0"
        self.pipeline.mirror_prefix = "4.22"
        self.pipeline.no_latest = True
        uploads, _, _ = await self._run_mirroring()
        self.assertTrue(uploads[1]["dest"].endswith("/rhcos/4.22-el10/4.22.0/"))

    async def test_sign_only_does_not_mirror_variants(self):
        self.pipeline.sign_only = True
        with (
            patch.object(self.pipeline, "_resolve_version_name", new_callable=AsyncMock),
            patch.object(self.pipeline, "_extract_rhcos_metadata", new_callable=AsyncMock),
            patch.object(self.pipeline, "_sync_boot_images", new_callable=AsyncMock) as sync,
            patch.object(self.pipeline, "_sign_containers", new_callable=AsyncMock) as sign,
        ):
            await self.pipeline._run_pipeline()
        sync.assert_not_awaited()
        sign.assert_awaited_once()

    async def test_extracts_additional_variants_from_installer_manifest(self):
        bootimages = {
            "data": {
                "stream": json.dumps(self._stream(self.pipeline.rhcos_build)),
                "streams": json.dumps(self.pipeline.extra_streams),
            }
        }

        async def extract(image: str, source: str, destination: str, **kwargs) -> None:
            """
            Supply the installer manifest consumed by metadata extraction.
            """
            (Path(destination) / "coreos-bootimages.yaml").write_text(yaml.safe_dump(bootimages))

        self.pipeline.extra_streams = {}
        with (
            patch("pyartcd.pipelines.rhcos_sync.oc.get_release_image_pullspec_async", new_callable=AsyncMock),
            patch("pyartcd.pipelines.rhcos_sync.oc.extract_release_image_async", side_effect=extract),
        ):
            await self.pipeline._extract_rhcos_metadata()
        variants = self.pipeline._get_boot_image_variants()
        self.assertEqual([variant.name for variant in variants], ["primary", "rhel-10"])
        self.assertEqual(variants[1].stream_suffix, "-el10")

    def test_missing_build_id_fails_with_variant_name(self):
        self.pipeline.extra_streams["rhel-10"]["architectures"]["x86_64"]["artifacts"] = {}
        with self.assertRaisesRegex(ValueError, "Cannot extract RHCOS build ID for variant rhel-10"):
            self.pipeline._get_boot_image_variants()

    def test_same_el_major_with_distinct_builds_cannot_overwrite(self):
        self.pipeline.extra_streams["rhel-10-other"] = self._stream("10.2.20261027-0")
        with self.assertRaisesRegex(ValueError, "Multiple distinct RHCOS builds map to mirror suffix -el10"):
            self.pipeline._get_boot_image_variants()

    def test_unsupported_architecture_is_logged(self):
        self.pipeline.extra_streams["rhel-10"] = {"architectures": {"aarch64": {}}}
        self.assertEqual(len(self.pipeline._get_boot_image_variants()), 1)
        self.pipeline.logger.warning.assert_called_once_with(
            "RHCOS variant %s has no architecture %s; skipping boot images", "rhel-10", "x86_64"
        )

    async def test_additional_variant_failure_is_logged_and_propagated(self):
        with patch.object(self.pipeline, "_sync_boot_image_variant", new_callable=AsyncMock) as sync:
            sync.side_effect = [None, RuntimeError("HTTP 404")]
            with self.assertRaisesRegex(RuntimeError, "HTTP 404"):
                await self.pipeline._sync_boot_images()
        self.pipeline.logger.exception.assert_called_once_with(
            "Boot image sync failed for RHCOS variant %s (build %s)", "rhel-10", "10.2.20261026-0"
        )

    async def test_empty_artifact_list_fails_with_variant_name(self):
        variant = self.pipeline._get_boot_image_variants()[1]
        response = MagicMock(status=200)
        response.json = AsyncMock(return_value={"images": {}})
        http = self._mock_http({})
        http.__aenter__.return_value.get.side_effect = None
        http.__aenter__.return_value.get.return_value.__aenter__.return_value = response
        with patch("pyartcd.pipelines.rhcos_sync.aiohttp.ClientSession", return_value=http):
            with self.assertRaisesRegex(ValueError, "No boot image artifacts found for RHCOS variant rhel-10"):
                await self.pipeline._generate_synclist(variant)


class TestComputeRhcosStream(TestCase):
    """Coverage for the RHCOS release-browser stream identifier computation."""

    def test_rhel8_legacy_uses_bare_ocp_version(self):
        # RHEL 8 legacy (4.12) resolves to the bare OCP version.
        self.assertEqual(compute_rhcos_stream("4.12", "412.86.202402272018-0"), "4.12")

    def test_rhel9_legacy_uses_suffixed_stream(self):
        # RHEL 9 legacy (4.13) keeps the suffixed stream.
        self.assertEqual(compute_rhcos_stream("4.13", "413.92.202402131523-0"), "4.13-9.2")

    def test_new_format_uses_rhel_prefix(self):
        self.assertEqual(compute_rhcos_stream("4.19", "9.6.20250121-0"), "rhel-9.6")
        self.assertEqual(compute_rhcos_stream("5.0", "9.8.20261026-0"), "rhel-9.8")
