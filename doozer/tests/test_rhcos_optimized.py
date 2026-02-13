import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from artcommonlib.model import Model
from doozerlib import rhcos
from doozerlib.repodata import Repodata


class TestRhcosOptimized(unittest.TestCase):
    def setUp(self):
        self.logger = MagicMock()
        self.runtime = MagicMock()
        self.runtime.group_config = Model({})
        self.runtime.logger = self.logger

    @patch("doozerlib.rhcos.RHCOSBuildFinder.rhcos_build_meta")
    @patch("doozerlib.rhcos.OutdatedRPMFinder")
    @patch("doozerlib.repos.Repo.get_repodata_threadsafe")
    @patch("doozerlib.rhcos.RHCOSBuildInspector.get_os_metadata_rpm_list")
    def test_find_non_latest_rpms_split(
        self,
        get_os_metadata_rpm_list: Mock,
        get_repodata_threadsafe: AsyncMock,
        OutdatedRPMFinderMock: Mock,
        rhcos_build_meta_mock: Mock,
    ):
        async def _run_test():
            # Setup
            rhcos_build_meta_mock.return_value = {}
            self.runtime.group_config.rhcos = Model({"enabled_repos": ["rhel-9-baseos", "rhel-10-baseos"]})

            # Mock repos
            repo9 = MagicMock()
            repo10 = MagicMock()
            self.runtime.repos = {"rhel-9-baseos": repo9, "rhel-10-baseos": repo10}

            # Mock repodata
            repodata9 = Repodata(name="rhel-9-baseos", primary_rpms=[])
            repodata10 = Repodata(name="rhel-10-baseos", primary_rpms=[])

            f9 = asyncio.Future()
            f9.set_result(repodata9)
            repo9.get_repodata_threadsafe = MagicMock(return_value=f9)

            f10 = asyncio.Future()
            f10.set_result(repodata10)
            repo10.get_repodata_threadsafe = MagicMock(return_value=f10)

            # Mock installed RPMs
            # Format: name, epoch, version, release, arch, repo_name
            get_os_metadata_rpm_list.return_value = [
                ["rpm9", "0", "1.0", "1.el9", "x86_64", "rhel-coreos"],
                ["rpm10", "0", "1.0", "1.el10", "x86_64", "rhel-coreos-10"],
            ]

            # Mock OutdatedRPMFinder
            finder_instance = OutdatedRPMFinderMock.return_value
            finder_instance.find_non_latest_rpms.side_effect = [[], []]  # Return empty list for both calls

            # Execute
            # We need to mock get_build_id_from_rhcos_pullspec if we pass pullspecs
            # But we can pass empty pullspecs and build_id
            rhcos_build = rhcos.RHCOSBuildInspector(self.runtime, {}, "x86_64", build_id="4.12.0")

            await rhcos_build.find_non_latest_rpms()

            # Verify
            # 1. Verify repodata fetching
            repo9.get_repodata_threadsafe.assert_called()
            repo10.get_repodata_threadsafe.assert_called()

            # 2. Verify OutdatedRPMFinder calls
            self.assertEqual(finder_instance.find_non_latest_rpms.call_count, 2)

            # Check first call (RHEL 9)
            args9, _ = finder_instance.find_non_latest_rpms.call_args_list[0]
            rpms_checked9 = args9[0]
            repodatas_passed9 = args9[1]

            self.assertEqual(len(rpms_checked9), 1)
            self.assertEqual(rpms_checked9[0]["name"], "rpm9")
            self.assertEqual(len(repodatas_passed9), 1)
            self.assertEqual(repodatas_passed9[0].name, "rhel-9-baseos")

            # Check second call (RHEL 10)
            args10, _ = finder_instance.find_non_latest_rpms.call_args_list[1]
            rpms_checked10 = args10[0]
            repodatas_passed10 = args10[1]

            self.assertEqual(len(rpms_checked10), 1)
            self.assertEqual(rpms_checked10[0]["name"], "rpm10")
            self.assertEqual(len(repodatas_passed10), 1)
            self.assertEqual(repodatas_passed10[0].name, "rhel-10-baseos")

        asyncio.run(_run_test())


if __name__ == "__main__":
    unittest.main()
