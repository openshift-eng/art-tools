from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Missing
from elliottlib.cli.verify_payload import VerifyPayloadPipeline


class TestPopulateMissingInShippedKonflux(IsolatedAsyncioTestCase):
    def _make_pipeline(self, image_metas=None):
        """Create a VerifyPayloadPipeline with a mocked runtime."""
        runtime = MagicMock()
        runtime.registry_config = "/path/to/auth.json"
        runtime.image_metas.return_value = image_metas or []

        pipeline = VerifyPayloadPipeline(runtime, "test-payload")
        return pipeline

    def _make_image_meta(self, component_name, distgit_key, delivery_repo_names):
        """Create a mock ImageMetadata with the given config."""
        meta = MagicMock()
        meta.get_component_name.return_value = component_name
        meta.distgit_key = distgit_key
        meta.config.delivery.delivery_repo_names = delivery_repo_names
        return meta

    @patch("elliottlib.cli.verify_payload._is_image_released", new_callable=AsyncMock)
    async def test_shipped_images_removed_from_missing(self, mock_released):
        """Shipped images are removed from missing_in_errata and added to in_shipped_advisory."""
        mock_released.return_value = True

        meta = self._make_image_meta("ose-cli-container", "cli", ["openshift4/ose-cli-rhel9"])
        pipeline = self._make_pipeline(image_metas=[meta])

        missing_in_errata = {"ose-cli-container": "ose-cli-container-4.19.0-202505210330.p0.el9"}
        in_shipped_advisory = []

        await pipeline._populate_missing_in_shipped_konflux(missing_in_errata, in_shipped_advisory)

        self.assertEqual(missing_in_errata, {})
        self.assertEqual(len(in_shipped_advisory), 1)
        self.assertEqual(in_shipped_advisory[0]["nvr"], "ose-cli-container-4.19.0-202505210330.p0.el9")
        self.assertEqual(in_shipped_advisory[0]["delivery_repo"], "openshift4/ose-cli-rhel9")

    @patch("elliottlib.cli.verify_payload._is_image_released", new_callable=AsyncMock)
    async def test_non_shipped_images_remain(self, mock_released):
        """Non-shipped images remain in missing_in_errata."""
        mock_released.return_value = False

        meta = self._make_image_meta("ose-cli-container", "cli", ["openshift4/ose-cli-rhel9"])
        pipeline = self._make_pipeline(image_metas=[meta])

        missing_in_errata = {"ose-cli-container": "ose-cli-container-4.19.0-202505210330.p0.el9"}
        in_shipped_advisory = []

        await pipeline._populate_missing_in_shipped_konflux(missing_in_errata, in_shipped_advisory)

        self.assertEqual(len(missing_in_errata), 1)
        self.assertIn("ose-cli-container", missing_in_errata)
        self.assertEqual(in_shipped_advisory, [])

    @patch("elliottlib.cli.verify_payload._is_image_released", new_callable=AsyncMock)
    async def test_no_delivery_repo_names_skipped(self, mock_released):
        """Images with no delivery_repo_names are skipped and stay in missing_in_errata."""
        meta = self._make_image_meta("ose-cli-container", "cli", Missing)
        pipeline = self._make_pipeline(image_metas=[meta])

        missing_in_errata = {"ose-cli-container": "ose-cli-container-4.19.0-202505210330.p0.el9"}
        in_shipped_advisory = []

        await pipeline._populate_missing_in_shipped_konflux(missing_in_errata, in_shipped_advisory)

        self.assertEqual(len(missing_in_errata), 1)
        self.assertIn("ose-cli-container", missing_in_errata)
        self.assertEqual(in_shipped_advisory, [])
        mock_released.assert_not_called()

    @patch("elliottlib.cli.verify_payload._is_image_released", new_callable=AsyncMock)
    async def test_empty_delivery_repo_names_skipped(self, mock_released):
        """Images with empty delivery_repo_names list are skipped."""
        meta = self._make_image_meta("ose-cli-container", "cli", [])
        pipeline = self._make_pipeline(image_metas=[meta])

        missing_in_errata = {"ose-cli-container": "ose-cli-container-4.19.0-202505210330.p0.el9"}
        in_shipped_advisory = []

        await pipeline._populate_missing_in_shipped_konflux(missing_in_errata, in_shipped_advisory)

        self.assertEqual(len(missing_in_errata), 1)
        self.assertEqual(in_shipped_advisory, [])
        mock_released.assert_not_called()

    @patch("elliottlib.cli.verify_payload._is_image_released", new_callable=AsyncMock)
    async def test_empty_missing_in_errata_noop(self, mock_released):
        """Empty missing_in_errata is a no-op."""
        pipeline = self._make_pipeline()

        missing_in_errata = {}
        in_shipped_advisory = []

        await pipeline._populate_missing_in_shipped_konflux(missing_in_errata, in_shipped_advisory)

        self.assertEqual(missing_in_errata, {})
        self.assertEqual(in_shipped_advisory, [])
        mock_released.assert_not_called()

    @patch("elliottlib.cli.verify_payload._is_image_released", new_callable=AsyncMock)
    async def test_mixed_shipped_and_non_shipped(self, mock_released):
        """Mixed shipped/non-shipped images are handled correctly."""
        # First image (ose-cli) is shipped, second (ose-installer) is not
        mock_released.side_effect = [True, False]

        meta_cli = self._make_image_meta("ose-cli-container", "cli", ["openshift4/ose-cli-rhel9"])
        meta_installer = self._make_image_meta(
            "ose-installer-container", "installer", ["openshift4/ose-installer-rhel9"]
        )
        pipeline = self._make_pipeline(image_metas=[meta_cli, meta_installer])

        missing_in_errata = {
            "ose-cli-container": "ose-cli-container-4.19.0-202505210330.p0.el9",
            "ose-installer-container": "ose-installer-container-4.19.0-202505210330.p0.el9",
        }
        in_shipped_advisory = []

        await pipeline._populate_missing_in_shipped_konflux(missing_in_errata, in_shipped_advisory)

        # ose-cli should be removed (shipped), ose-installer should remain
        self.assertNotIn("ose-cli-container", missing_in_errata)
        self.assertIn("ose-installer-container", missing_in_errata)
        self.assertEqual(len(in_shipped_advisory), 1)
        self.assertEqual(in_shipped_advisory[0]["nvr"], "ose-cli-container-4.19.0-202505210330.p0.el9")

    @patch("elliottlib.cli.verify_payload._is_image_released", new_callable=AsyncMock)
    async def test_no_image_meta_found_skipped(self, mock_released):
        """Images with no matching image metadata are skipped."""
        # Provide an image meta that doesn't match the missing component name
        meta = self._make_image_meta("different-container", "different", ["openshift4/different-rhel9"])
        pipeline = self._make_pipeline(image_metas=[meta])

        missing_in_errata = {"ose-cli-container": "ose-cli-container-4.19.0-202505210330.p0.el9"}
        in_shipped_advisory = []

        await pipeline._populate_missing_in_shipped_konflux(missing_in_errata, in_shipped_advisory)

        self.assertEqual(len(missing_in_errata), 1)
        self.assertIn("ose-cli-container", missing_in_errata)
        self.assertEqual(in_shipped_advisory, [])
        mock_released.assert_not_called()
