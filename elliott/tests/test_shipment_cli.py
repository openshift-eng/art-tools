"""
Tests for elliottlib.cli.shipment_cli, focusing on advisory type selection
(RHEA for X.Y.0 releases, RHBA for z-stream).
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.assembly import AssemblyTypes
from elliottlib.cli.shipment_cli import InitShipmentCli


class TestInitShipmentCliAdvisoryType(unittest.IsolatedAsyncioTestCase):
    """
    Verify that InitShipmentCli.run() sets RHEA for X.Y.0 releases
    and RHBA for z-stream releases.
    """

    def _make_runtime(self, major: str, minor: str, patch: str, assembly_type: AssemblyTypes = AssemblyTypes.STANDARD):
        runtime = MagicMock()
        runtime.get_major_minor_patch.return_value = (major, minor, patch)
        runtime.get_errata_config.return_value = {}
        runtime.product = "openshift"
        runtime.group = f"openshift-{major}.{minor}"
        runtime.assembly = f"{major}.{minor}.{patch}"
        runtime.assembly_type = assembly_type
        runtime.shipment_gitdata.load_yaml_file.return_value = {}
        return runtime

    @patch("elliottlib.cli.shipment_cli.get_advisory_boilerplate")
    @patch("elliottlib.cli.shipment_cli.konflux_application_name", return_value="test-app")
    async def test_xy0_release_uses_rhea(self, _mock_app_name, mock_boilerplate):
        """
        X.Y.0 release should produce RHEA advisory type.
        """
        mock_boilerplate.return_value = {
            "synopsis": "synopsis {MAJOR}.{MINOR}.{PATCH}",
            "topic": "topic",
            "description": "desc",
            "solution": "sol",
        }
        runtime = self._make_runtime("4", "18", "0")
        cli = InitShipmentCli(runtime=runtime, kind="microshift-bootc")
        result = await cli.run()

        mock_boilerplate.assert_called_once_with(
            runtime=runtime, et_data={}, art_advisory_key="microshift-bootc", errata_type="RHEA"
        )
        release_notes = result["shipment"]["data"]["releaseNotes"]
        self.assertEqual(release_notes["type"], "RHEA")

    @patch("elliottlib.cli.shipment_cli.get_advisory_boilerplate")
    @patch("elliottlib.cli.shipment_cli.konflux_application_name", return_value="test-app")
    async def test_zstream_release_uses_rhba(self, _mock_app_name, mock_boilerplate):
        """
        Z-stream release (patch > 0) should produce RHBA advisory type.
        """
        mock_boilerplate.return_value = {
            "synopsis": "synopsis {MAJOR}.{MINOR}.{PATCH}",
            "topic": "topic",
            "description": "desc",
            "solution": "sol",
        }
        runtime = self._make_runtime("4", "18", "3")
        cli = InitShipmentCli(runtime=runtime, kind="microshift-bootc")
        result = await cli.run()

        mock_boilerplate.assert_called_once_with(
            runtime=runtime, et_data={}, art_advisory_key="microshift-bootc", errata_type="RHBA"
        )
        release_notes = result["shipment"]["data"]["releaseNotes"]
        self.assertEqual(release_notes["type"], "RHBA")

    @patch("elliottlib.cli.shipment_cli.get_advisory_boilerplate")
    @patch("elliottlib.cli.shipment_cli.konflux_application_name", return_value="test-app")
    async def test_fbc_kind_skips_advisory_data(self, _mock_app_name, mock_boilerplate):
        """
        FBC kind should not include advisory data at all.
        """
        runtime = self._make_runtime("4", "18", "0")
        with patch("elliottlib.cli.shipment_cli.KonfluxFbcBuilder") as mock_fbc:
            mock_fbc.get_application_name.return_value = "fbc-app"
            cli = InitShipmentCli(runtime=runtime, kind="fbc")
            result = await cli.run()

        mock_boilerplate.assert_not_called()
        self.assertNotIn("data", result["shipment"])

    @patch("elliottlib.cli.shipment_cli.get_advisory_boilerplate")
    @patch("elliottlib.cli.shipment_cli.konflux_application_name", return_value="test-app")
    async def test_image_kind_xy0_uses_rhea(self, _mock_app_name, mock_boilerplate):
        """
        Image kind for X.Y.0 should also get RHEA.
        """
        mock_boilerplate.return_value = {
            "synopsis": "syn",
            "topic": "top",
            "description": "desc",
            "solution": "sol",
        }
        runtime = self._make_runtime("4", "19", "0")
        cli = InitShipmentCli(runtime=runtime, kind="image")
        result = await cli.run()

        mock_boilerplate.assert_called_once_with(
            runtime=runtime, et_data={}, art_advisory_key="image", errata_type="RHEA"
        )
        release_notes = result["shipment"]["data"]["releaseNotes"]
        self.assertEqual(release_notes["type"], "RHEA")

    @patch("elliottlib.cli.shipment_cli.get_advisory_boilerplate")
    @patch("elliottlib.cli.shipment_cli.konflux_application_name", return_value="test-app")
    async def test_candidate_xy0_uses_rhba(self, _mock_app_name, mock_boilerplate):
        """
        CANDIDATE assembly ending in .0 should still use RHBA, not RHEA.
        """
        mock_boilerplate.return_value = {
            "synopsis": "syn",
            "topic": "top",
            "description": "desc",
            "solution": "sol",
        }
        runtime = self._make_runtime("4", "18", "0", assembly_type=AssemblyTypes.CANDIDATE)
        cli = InitShipmentCli(runtime=runtime, kind="image")
        result = await cli.run()

        mock_boilerplate.assert_called_once_with(
            runtime=runtime, et_data={}, art_advisory_key="image", errata_type="RHBA"
        )
        release_notes = result["shipment"]["data"]["releaseNotes"]
        self.assertEqual(release_notes["type"], "RHBA")


if __name__ == "__main__":
    unittest.main()
