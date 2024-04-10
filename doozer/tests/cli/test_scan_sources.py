from unittest import TestCase
from unittest.mock import MagicMock, patch

from artcommonlib.model import Model
from doozerlib.cli.scan_sources import ConfigScanSources
from doozerlib import rhcos


class TestScanSourcesCli(TestCase):

    @patch("artcommonlib.exectools.cmd_assert")
    def test_tagged_rhcos_id(self, mock_cmd):
        mock_cmd.return_value = ("id-1", "stderr")

        runtime = MagicMock(rpm_map=[])
        cli = ConfigScanSources(runtime, "kc.conf", False)

        self.assertEqual("id-1", cli._tagged_rhcos_id("cname", "4.2", "s390x", True))
        self.assertIn("--kubeconfig 'kc.conf'", mock_cmd.call_args_list[0][0][0])
        self.assertIn("--namespace 'ocp-s390x-priv'", mock_cmd.call_args_list[0][0][0])
        self.assertIn("istag '4.2-art-latest-s390x-priv", mock_cmd.call_args_list[0][0][0])

    @patch("doozerlib.cli.scan_sources.ConfigScanSources._tagged_rhcos_id", autospec=True)
    @patch("doozerlib.cli.scan_sources.ConfigScanSources._latest_rhcos_build_id", autospec=True)
    def test_detect_rhcos_status(self, mock_latest, mock_tagged):
        mock_tagged.return_value = "id-1"
        mock_latest.return_value = "id-2"
        runtime = MagicMock(group_config=Model())
        runtime.get_minor_version.return_value = "4.2"
        runtime.arches = ['s390x']

        cli = ConfigScanSources(runtime, "dummy", False)

        statuses = cli._detect_rhcos_status()
        self.assertEqual(2, len(statuses), "expect public and private status reported")
        self.assertTrue(all(s['changed'] for s in statuses), "expect changed status reported")
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
