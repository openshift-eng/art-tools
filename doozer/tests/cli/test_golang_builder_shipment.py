"""Tests for doozer golang-builder-shipment CLI command."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner


class TestGolangBuilderShipmentCli(unittest.IsolatedAsyncioTestCase):
    def _make_runtime(self, dry_run=False):
        runtime = MagicMock()
        runtime.dry_run = dry_run
        return runtime

    @patch("doozerlib.cli.golang_builder_shipment.GolangBuilderShipmentHandler")
    def test_cli_invokes_handler(self, mock_handler_cls):
        mock_handler = mock_handler_cls.return_value
        mock_handler.create_shipment_from_nvrs = AsyncMock(return_value="https://gitlab.example.com/mr/1")

        runner = CliRunner()
        from doozerlib.cli import cli

        result = runner.invoke(
            cli,
            [
                "--group",
                "golang",
                "golang-builder-shipment",
                "openshift-golang-builder-container-v1.25.9-1.el9",
            ],
            obj=MagicMock(dry_run=False),
            catch_exceptions=False,
        )
        self.assertIn("https://gitlab.example.com/mr/1", result.output)
        mock_handler.create_shipment_from_nvrs.assert_called_once()

    def test_cli_requires_nvrs(self):
        runner = CliRunner()
        from doozerlib.cli import cli

        result = runner.invoke(
            cli,
            ["--group", "golang", "golang-builder-shipment"],
            obj=MagicMock(dry_run=False),
        )
        self.assertNotEqual(result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
