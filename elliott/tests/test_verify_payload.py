"""
Tests for Konflux payload verification shipment selection.
"""

import unittest
from unittest.mock import MagicMock, patch

from elliottlib.cli.verify_payload import VerifyPayloadPipeline


class TestVerifyPayloadPipeline(unittest.IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_payload.get_shipment_config_from_mr")
    async def test_get_shipment_nvrs_uses_principal_image_shipment(self, mock_get_shipment_config):
        """Payload verification reads builds from the selected principal image shipment."""
        image_shipment = MagicMock()
        image_shipment.shipment.snapshot.nvrs = ["test-container-v1.0.0-202312010000.p0.git12345"]
        mock_get_shipment_config.return_value = image_shipment

        pipeline = VerifyPayloadPipeline(MagicMock(), "quay.io/example/release:4.20.1-x86_64")
        pipeline.assembly_group_config = {
            "shipment": {"url": "https://gitlab.example.com/project/-/merge_requests/1"}
        }

        result = await pipeline.get_shipment_nvrs()

        self.assertEqual(result, {"test-container": "test-container-v1.0.0-202312010000.p0.git12345"})
        mock_get_shipment_config.assert_called_once_with(
            "https://gitlab.example.com/project/-/merge_requests/1", "image"
        )
