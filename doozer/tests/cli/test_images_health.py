import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Missing, Model
from artcommonlib.variants import BuildVariant
from doozerlib.cli.images_health import ImagesHealthPipeline


class TestImagesHealthOKDModeFiltering(unittest.IsolatedAsyncioTestCase):
    """
    Tests for OKD variant mode filtering in images:health command.

    These tests verify that the OKD mode override (okd.mode) correctly overrides
    the general mode field when determining which images to include/skip.
    """

    def setUp(self):
        """
        Set up test fixtures.
        """
        self.mock_runtime = MagicMock()
        self.mock_runtime.group_config = MagicMock()
        self.mock_runtime.group_config.name = "openshift-4.20"
        self.mock_runtime.group = "openshift-4.20"
        self.mock_runtime.konflux_db = MagicMock()
        self.mock_runtime.konflux_db.bind = MagicMock()

    def _create_image_meta(self, distgit_key: str, mode: str, okd_mode=Missing):
        """
        Create a mock ImageMetadata object with specified mode configuration.

        Arg(s):
            distgit_key (str): The distgit key for the image.
            mode (str): The top-level mode ('enabled' or 'disabled').
            okd_mode: The OKD-specific mode override (default: Missing).
        Return Value(s):
            Mock: A mock ImageMetadata object.
        """
        image_meta = MagicMock()
        image_meta.distgit_key = distgit_key
        image_meta.mode = mode

        # Create config with mode and okd section
        config_dict = {'mode': mode, 'for_release': True, 'konflux': {'mode': 'enabled'}}

        if okd_mode is not Missing:
            config_dict['okd'] = {'mode': okd_mode}
        else:
            config_dict['okd'] = Missing

        image_meta.config = Model(config_dict)
        return image_meta

    async def test_okd_variant_mode_disabled_no_override(self):
        """
        Test OKD variant with mode: disabled and no okd.mode override.

        Expected: Image should be skipped.
        """
        image_meta = self._create_image_meta('test-image', 'disabled')
        self.mock_runtime.image_metas.return_value = [image_meta]

        pipeline = ImagesHealthPipeline(runtime=self.mock_runtime, limit=100, variant=BuildVariant.OKD)

        with patch.object(pipeline, 'get_concerns', new=AsyncMock()) as mock_get_concerns:
            await pipeline.run()
            # Image should be skipped, so get_concerns should not be called
            mock_get_concerns.assert_not_called()

    async def test_okd_variant_mode_disabled_okd_enabled(self):
        """
        Test OKD variant with mode: disabled but okd.mode: enabled.

        Expected: Image should NOT be skipped (OKD override takes precedence).
        """
        image_meta = self._create_image_meta('test-image', 'disabled', okd_mode='enabled')
        self.mock_runtime.image_metas.return_value = [image_meta]

        pipeline = ImagesHealthPipeline(runtime=self.mock_runtime, limit=100, variant=BuildVariant.OKD)

        with patch.object(pipeline, 'get_concerns', new=AsyncMock()) as mock_get_concerns:
            await pipeline.run()
            # Image should NOT be skipped, so get_concerns should be called once
            mock_get_concerns.assert_called_once_with(image_meta)

    async def test_okd_variant_mode_enabled_okd_disabled(self):
        """
        Test OKD variant with mode: enabled but okd.mode: disabled.

        Expected: Image should be skipped (OKD override takes precedence).
        """
        image_meta = self._create_image_meta('test-image', 'enabled', okd_mode='disabled')
        self.mock_runtime.image_metas.return_value = [image_meta]

        pipeline = ImagesHealthPipeline(runtime=self.mock_runtime, limit=100, variant=BuildVariant.OKD)

        with patch.object(pipeline, 'get_concerns', new=AsyncMock()) as mock_get_concerns:
            await pipeline.run()
            # Image should be skipped, so get_concerns should not be called
            mock_get_concerns.assert_not_called()

    async def test_okd_variant_mode_enabled_no_override(self):
        """
        Test OKD variant with mode: enabled and no okd.mode override.

        Expected: Image should NOT be skipped.
        """
        image_meta = self._create_image_meta('test-image', 'enabled')
        self.mock_runtime.image_metas.return_value = [image_meta]

        pipeline = ImagesHealthPipeline(runtime=self.mock_runtime, limit=100, variant=BuildVariant.OKD)

        with patch.object(pipeline, 'get_concerns', new=AsyncMock()) as mock_get_concerns:
            await pipeline.run()
            # Image should NOT be skipped, so get_concerns should be called once
            mock_get_concerns.assert_called_once_with(image_meta)

    async def test_non_okd_variant_mode_disabled(self):
        """
        Test non-OKD variant (OCP) with mode: disabled.

        Expected: Image should be skipped.
        """
        image_meta = self._create_image_meta('test-image', 'disabled')
        self.mock_runtime.image_metas.return_value = [image_meta]

        pipeline = ImagesHealthPipeline(runtime=self.mock_runtime, limit=100, variant=BuildVariant.OCP)

        with patch.object(pipeline, 'get_concerns', new=AsyncMock()) as mock_get_concerns:
            await pipeline.run()
            # Image should be skipped, so get_concerns should not be called
            mock_get_concerns.assert_not_called()

    async def test_non_okd_variant_mode_enabled(self):
        """
        Test non-OKD variant (OCP) with mode: enabled.

        Expected: Image should NOT be skipped.
        """
        image_meta = self._create_image_meta('test-image', 'enabled')
        self.mock_runtime.image_metas.return_value = [image_meta]

        pipeline = ImagesHealthPipeline(runtime=self.mock_runtime, limit=100, variant=BuildVariant.OCP)

        with patch.object(pipeline, 'get_concerns', new=AsyncMock()) as mock_get_concerns:
            await pipeline.run()
            # Image should NOT be skipped, so get_concerns should be called once
            mock_get_concerns.assert_called_once_with(image_meta)

    async def test_multiple_images_mixed_modes(self):
        """
        Test OKD variant with multiple images having different mode configurations.

        Expected: Only images with effective mode 'enabled' should be processed.
        """
        image1 = self._create_image_meta('image-1', 'disabled')  # Should skip
        image2 = self._create_image_meta('image-2', 'disabled', okd_mode='enabled')  # Should NOT skip
        image3 = self._create_image_meta('image-3', 'enabled', okd_mode='disabled')  # Should skip
        image4 = self._create_image_meta('image-4', 'enabled')  # Should NOT skip

        self.mock_runtime.image_metas.return_value = [image1, image2, image3, image4]

        pipeline = ImagesHealthPipeline(runtime=self.mock_runtime, limit=100, variant=BuildVariant.OKD)

        with patch.object(pipeline, 'get_concerns', new=AsyncMock()) as mock_get_concerns:
            await pipeline.run()
            # Only image2 and image4 should be processed
            self.assertEqual(mock_get_concerns.call_count, 2)
            mock_get_concerns.assert_any_call(image2)
            mock_get_concerns.assert_any_call(image4)
