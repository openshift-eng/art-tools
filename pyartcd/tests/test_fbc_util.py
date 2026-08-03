import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from pyartcd.fbc_util import (
    extract_ocp_version_from_nvr,
    validate_fbc_related_images,
)


class TestExtractOcpVersionFromNvr(unittest.TestCase):
    def test_extracts_version_from_lp_fbc_nvr(self):
        self.assertEqual(
            extract_ocp_version_from_nvr("cluster-logging-operator-fbc-6.4.1-1234.ocp4.18"),
            "4.18",
        )

    def test_extracts_version_multi_digit_minor(self):
        self.assertEqual(
            extract_ocp_version_from_nvr("oadp-operator-fbc-1.5.3-999.ocp4.21"),
            "4.21",
        )

    def test_returns_none_for_ocp_fbc_nvr(self):
        """OCP FBC NVRs don't have .ocp suffix."""
        self.assertIsNone(extract_ocp_version_from_nvr("cluster-nfd-operator-fbc-4.18.0-20260101"))

    def test_returns_none_for_non_fbc_nvr(self):
        self.assertIsNone(extract_ocp_version_from_nvr("search-v2-api-container-2.17.3-1"))


class TestValidateFbcRelatedImages(unittest.TestCase):
    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_passes_when_related_images_match(self, mock_labels, mock_extract):
        mock_labels.side_effect = [
            {'nvr': 'op-fbc-1.0-1.ocp4.18', 'doozer_key': 'my-operator'},
            {'nvr': 'op-fbc-1.0-1.ocp4.19', 'doozer_key': 'my-operator'},
        ]
        mock_extract.side_effect = [
            ["image-a-1.0-1", "image-b-1.0-1"],
            ["image-a-1.0-1", "image-b-1.0-1"],
        ]

        result = asyncio.run(
            validate_fbc_related_images(
                ["quay.io/fbc:ocp4.18", "quay.io/fbc:ocp4.19"],
                "my-product",
            )
        )

        self.assertEqual(sorted(result), ["image-a-1.0-1", "image-b-1.0-1"])

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_fails_when_related_images_mismatch(self, mock_labels, mock_extract):
        mock_labels.side_effect = [
            {'nvr': 'op-fbc-1.0-1.ocp4.18', 'doozer_key': 'my-operator'},
            {'nvr': 'op-fbc-1.0-1.ocp4.19', 'doozer_key': 'my-operator'},
        ]
        mock_extract.side_effect = [
            ["image-a-1.0-1", "image-b-1.0-1"],
            ["image-a-1.0-1", "image-c-1.0-1"],
        ]

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(
                validate_fbc_related_images(
                    ["quay.io/fbc:ocp4.18", "quay.io/fbc:ocp4.19"],
                    "my-product",
                )
            )
        self.assertIn("mismatched", str(ctx.exception))

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_single_fbc_skips_comparison(self, mock_labels, mock_extract):
        mock_labels.return_value = {'nvr': 'op-fbc-1.0-1.ocp4.18', 'doozer_key': 'my-operator'}
        mock_extract.return_value = ["image-a-1.0-1"]

        result = asyncio.run(
            validate_fbc_related_images(
                ["quay.io/fbc:ocp4.18"],
                "my-product",
            )
        )

        self.assertEqual(result, ["image-a-1.0-1"])

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_different_operators_may_differ(self, mock_labels, mock_extract):
        """FBCs from different operators may have different related images."""
        mock_labels.side_effect = [
            {'nvr': 'op-a-fbc-1.0-1.ocp4.18', 'doozer_key': 'operator-a'},
            {'nvr': 'op-b-fbc-1.0-1.ocp4.18', 'doozer_key': 'operator-b'},
        ]
        mock_extract.side_effect = [
            ["image-a-1.0-1"],
            ["image-b-1.0-1"],
        ]

        result = asyncio.run(
            validate_fbc_related_images(
                ["quay.io/fbc-a:ocp4.18", "quay.io/fbc-b:ocp4.18"],
                "my-product",
            )
        )

        self.assertEqual(sorted(result), ["image-a-1.0-1", "image-b-1.0-1"])


if __name__ == '__main__':
    unittest.main()
