import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from artcommonlib.konflux.konflux_build_record import ArtifactType, KonfluxBuildOutcome, KonfluxBuildRecord
from elliottlib.cli.get_network_mode_cli import GetNetworkModeCli
from elliottlib.imagecfg import ImageMetadata


class TestGetNetworkModeCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "stream"
        self.runtime.konflux_db = MagicMock()
        self.runtime.konflux_db.bind = MagicMock()

        # Create mock image metadata
        self.image_meta1 = MagicMock(spec=ImageMetadata)
        self.image_meta1.distgit_key = "ironic"
        self.image_meta2 = MagicMock(spec=ImageMetadata)
        self.image_meta2.distgit_key = "ironic-inspector"

        self.runtime.image_metas = MagicMock(return_value=[self.image_meta1, self.image_meta2])

        self.build1 = MagicMock(spec=KonfluxBuildRecord)
        self.build1.name = "ironic"
        self.build1.hermetic = True
        self.build1.nvr = "ironic-container-v4.21.0-1"
        self.build1.image_pullspec = "registry.example.com/ironic@sha256:abc123"

        self.build2 = MagicMock(spec=KonfluxBuildRecord)
        self.build2.name = "ironic-inspector"
        self.build2.hermetic = False
        self.build2.nvr = "ironic-inspector-container-v4.21.0-1"
        self.build2.image_pullspec = "registry.example.com/ironic-inspector@sha256:def456"

        self.builds = [self.build1, self.build2]

    async def test_basic_functionality(self):
        """Test basic get-network-mode functionality"""
        mock_get_latest_builds = AsyncMock(return_value=self.builds)
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=False, only_hermetic=False, only_open=False)
        results = await cli.get_results()
        self.assertEqual(
            results,
            [
                {
                    "name": self.build1.name,
                    "nvr": self.build1.nvr,
                    "network_mode": "hermetic",
                    "pullspec": self.build1.image_pullspec,
                },
                {
                    "name": self.build2.name,
                    "nvr": self.build2.nvr,
                    "network_mode": "open",
                    "pullspec": self.build2.image_pullspec,
                },
            ],
        )

        # Verify get_latest_builds was called correctly
        mock_get_latest_builds.assert_called_once_with(
            names=["ironic", "ironic-inspector"],
            group="openshift-4.21",
            assembly="stream",
            exclude_large_columns=True,
            outcome=KonfluxBuildOutcome.SUCCESS,
            artifact_type=ArtifactType.IMAGE,
        )

    async def test_only_hermetic(self):
        """Test --only-hermetic flag filters to only hermetic builds"""
        mock_get_latest_builds = AsyncMock(return_value=self.builds)
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=False, only_hermetic=True, only_open=False)
        results = await cli.get_results()
        self.assertEqual(
            results,
            [
                {
                    "name": self.build1.name,
                    "nvr": self.build1.nvr,
                    "network_mode": "hermetic",
                    "pullspec": self.build1.image_pullspec,
                },
            ],
        )

        # Verify get_latest_builds was called
        mock_get_latest_builds.assert_called_once()

    async def test_only_open(self):
        """Test --only-open flag filters to only open builds"""
        mock_get_latest_builds = AsyncMock(return_value=self.builds)
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=False, only_hermetic=False, only_open=True)
        results = await cli.get_results()
        self.assertEqual(
            results,
            [
                {
                    "name": self.build2.name,
                    "nvr": self.build2.nvr,
                    "network_mode": "open",
                    "pullspec": self.build2.image_pullspec,
                },
            ],
        )

        # Verify get_latest_builds was called
        mock_get_latest_builds.assert_called_once()

    @patch("click.echo")
    async def test_json_output(self, mock_echo):
        """Test --json flag outputs JSON format"""
        mock_get_latest_builds = AsyncMock(return_value=self.builds)
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=True, only_hermetic=False, only_open=False)
        await cli.run()

        # Verify JSON was output
        mock_echo.assert_called()
        # Check that the call contains JSON
        json_call = [call for call in mock_echo.call_args_list if isinstance(call[0][0], str)][0]
        output = json_call[0][0]
        parsed = json.loads(output)
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 2)  # Two images
        # Check that one has network_mode and one doesn't (no build found)
        network_modes = [item.get("network_mode") for item in parsed]
        self.assertIn("hermetic", network_modes)
        self.assertIn("open", network_modes)

    @patch("click.echo")
    async def test_no_builds_found(self, mock_echo):
        """Test behavior when no builds are found for images"""
        # Return empty list or None builds
        mock_get_latest_builds = AsyncMock(return_value=[None, None])
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=False, only_hermetic=False, only_open=False)
        await cli.run()

        # Should still output table with N/A values
        mock_echo.assert_called()
        # Check that table header was printed
        calls = [str(call) for call in mock_echo.call_args_list]
        self.assertTrue(any("Image" in str(call) for call in calls))

    async def test_mixed_build_results(self):
        """Test behavior with mix of found and missing builds"""
        # Second build is None (not found)
        mock_get_latest_builds = AsyncMock(return_value=[self.build1, None])
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=False, only_hermetic=False, only_open=False)
        results = await cli.get_results()
        self.assertEqual(
            results,
            [
                {
                    "name": "ironic",
                    "network_mode": "hermetic",
                    "nvr": "ironic-container-v4.21.0-1",
                    "pullspec": "registry.example.com/ironic@sha256:abc123",
                }
            ],
        )

        # Should handle both found and missing builds gracefully
        mock_get_latest_builds.assert_called_once()

    async def test_assembly_defaults_to_stream(self):
        """Test that assembly defaults to 'stream' when not provided"""
        self.runtime.assembly = None

        mock_get_latest_builds = AsyncMock(return_value=[])
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=False, only_hermetic=False, only_open=False)
        await cli.get_results()

        # Verify assembly defaults to 'stream'
        mock_get_latest_builds.assert_called_once()
        call_args = mock_get_latest_builds.call_args
        self.assertEqual(call_args.kwargs["assembly"], "stream")

    @patch("click.echo")
    async def test_table_output_format(self, mock_echo):
        """Test that table output has correct format"""
        mock_get_latest_builds = AsyncMock(return_value=[self.build1])
        self.runtime.konflux_db.get_latest_builds = mock_get_latest_builds

        cli = GetNetworkModeCli(self.runtime, as_json=False, only_hermetic=False, only_open=False)
        await cli.run()

        # Verify table header was printed
        calls = [str(call[0][0]) for call in mock_echo.call_args_list]
        self.assertTrue(any("Image" in call and "Network Mode" in call for call in calls))
        # Verify separator line was printed
        self.assertTrue(any("-" * 155 in call for call in calls))
