from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Model
from doozerlib import rhcos
from doozerlib.cli.scan_sources import ConfigScanSources
from doozerlib.metadata import RebuildHint, RebuildHintCode


class TestScanSourcesCli(IsolatedAsyncioTestCase):
    @patch("artcommonlib.exectools.cmd_assert")
    def test_tagged_rhcos_id(self, mock_cmd):
        mock_cmd.return_value = (
            '{"image": {"dockerImageMetadata": {"Config": {"Labels": {"org.opencontainers.image.version": "id-1"}}}}}',
            "stderr",
        )

        runtime = MagicMock(rpm_map=[], build_system='brew')
        cli = ConfigScanSources(runtime, "kc.conf", False)

        self.assertEqual("id-1", cli._tagged_rhcos_id("cname", "4.2", "s390x", True))
        self.assertIn("--kubeconfig 'kc.conf'", mock_cmd.call_args_list[0][0][0])
        self.assertIn("--namespace 'ocp-s390x-priv'", mock_cmd.call_args_list[0][0][0])
        self.assertIn("istag '4.2-art-latest-s390x-priv", mock_cmd.call_args_list[0][0][0])

    @patch("doozerlib.cli.scan_sources.ConfigScanSources._tagged_rhcos_id", autospec=True)
    @patch("doozerlib.cli.scan_sources.ConfigScanSources._latest_rhcos_build_id", autospec=True)
    @patch("doozerlib.cli.scan_sources.rhcos.RHCOSBuildInspector", autospec=True)
    @patch("doozerlib.cli.scan_sources.rhcos.RHCOSBuildFinder.latest_container", autospec=True)
    async def _test_detect_rhcos_status(self, mock_finder, mock_inspector, mock_latest, mock_tagged):
        mock_tagged.return_value = "id-1"
        mock_latest.return_value = "id-2"
        build_inspector = AsyncMock()
        build_inspector.find_non_latest_rpms.return_value = AsyncMock(return_value=None)
        mock_inspector.return_value = build_inspector
        mock_finder.return_value = "4.2", "pullspec"
        runtime = MagicMock(group_config=Model())
        runtime.get_minor_version.return_value = "4.2"
        runtime.get_major_minor_fields.return_value = 4, 2
        runtime.arches = ['s390x']

        cli = ConfigScanSources(runtime, "dummy", False)

        statuses = await cli._detect_rhcos_status()
        self.assertEqual(2, len(statuses), "expect public and private status reported")
        self.assertTrue(all(s['updated'] for s in statuses), "expect changed status reported")
        self.assertTrue(all("id-1" in s['reason'] for s in statuses), "expect previous id in reason")
        self.assertTrue(all("id-2" in s['reason'] for s in statuses), "expect changed id in reason")

    @patch('doozerlib.rhcos.RHCOSBuildFinder.latest_rhcos_build_id')
    def test_build_find_failure(self, mock_get_build):
        # pedantic to have this test but don't want this to silently break again
        mock_get_build.side_effect = Exception("test")
        with self.assertRaises(Exception):
            cli = ConfigScanSources(MagicMock(), "dummy", False)
            cli._latest_rhcos_build_id("4.9", "aarch64", False)

        mock_get_build.side_effect = rhcos.RHCOSNotFound("test")
        cli._latest_rhcos_build_id("4.9", "aarch64", False)

    @patch("doozerlib.cli.scan_sources.exectools.parallel_exec")
    async def test_scan_sources_skips_broken_canonical_image_and_continues(self, mock_parallel_exec):
        broken_image = MagicMock()
        broken_image.meta_type = "image"
        broken_image.enabled = True
        broken_image.mode = "enabled"
        broken_image.distgit_key = "broken-image"
        broken_image.ensure_canonical_builders_resolved.side_effect = Exception("bad canonical")
        broken_image.does_image_need_change = AsyncMock()

        good_image = MagicMock()
        good_image.meta_type = "image"
        good_image.enabled = True
        good_image.mode = "enabled"
        good_image.distgit_key = "good-image"
        good_image.ensure_canonical_builders_resolved.return_value = None
        good_image.needs_rebuild.return_value = RebuildHint(RebuildHintCode.BUILD_IS_UP_TO_DATE, "no change")
        good_image.build_root_tag.return_value = "good-buildroot"
        good_image.does_image_need_change = AsyncMock(return_value=None)

        runtime = MagicMock()
        runtime.rpm_metas.return_value = []
        runtime.image_metas.return_value = [broken_image, good_image]
        runtime.load_disabled = False

        def fake_parallel_exec(f, args, n_threads):
            return MagicMock(get=MagicMock(return_value=[f(arg, None) for arg in args]))

        mock_parallel_exec.side_effect = fake_parallel_exec

        scanner = ConfigScanSources(runtime, "dummy", False)
        scanner.scan_for_upstream_changes(MagicMock())

        self.assertEqual(
            scanner.issues,
            [
                {
                    "name": "broken-image",
                    "issue": "Failed resolving canonical builders before upstream commit checks: bad canonical",
                }
            ],
        )
        self.assertIn("broken-image", scanner.skipped_image_dgks)
        broken_image.needs_rebuild.assert_not_called()
        good_image.needs_rebuild.assert_called_once()

        await scanner.check_changing_rpms()

        broken_image.ensure_canonical_builders_resolved.assert_called_once()
        broken_image.does_image_need_change.assert_not_called()
        good_image.ensure_canonical_builders_resolved.assert_called()
        good_image.does_image_need_change.assert_awaited_once()
