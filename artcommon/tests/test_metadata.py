"""
Tests for the metadata module, specifically testing the get_latest_konflux_build method
and its enforce_network_mode parameter.
"""

import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.metadata import MetadataBase


class TestGetLatestKonfluxBuildEnforceNetworkMode(IsolatedAsyncioTestCase):
    """
    Test the enforce_network_mode parameter in get_latest_konflux_build at the metadata layer.
    This ensures that the metadata correctly calls get_konflux_network_mode() and passes
    the hermetic value to the KonfluxDB layer.
    """

    async def test_get_latest_konflux_build_hermetic_mode(self):
        """
        Test that enforce_network_mode=True adds hermetic=True filter for hermetic images
        """
        # given
        runtime = MagicMock()
        runtime.build_system = "konflux"
        runtime.assembly = "stream"
        runtime.group = "openshift-4.20"
        runtime.konflux_db = MagicMock()
        runtime.logger = MagicMock()

        data_obj = MagicMock()
        data_obj.key = "test-image"
        data_obj.filename = "test-image.yml"
        data_obj.path = "/path/to/test-image.yml"
        data_obj.data = {"name": "test-image"}

        runtime.get_releases_config = MagicMock(return_value={})
        meta = MetadataBase("image", runtime, data_obj)
        meta.meta_type = "image"
        meta.distgit_key = "test-image"
        meta.branch_el_target = MagicMock(return_value=9)

        expected_build = MagicMock(nvr="test-image-1.0.0-1.el9", hermetic=True)
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=expected_build)

        # when
        with patch.object(meta, "get_konflux_network_mode", return_value="hermetic"):
            result = await meta.get_latest_konflux_build(enforce_network_mode=True)
            meta.get_konflux_network_mode.assert_called_once()

        # then
        runtime.konflux_db.get_latest_build.assert_called_once()
        call_kwargs = runtime.konflux_db.get_latest_build.call_args[1]

        self.assertIn("extra_patterns", call_kwargs)
        self.assertEqual(call_kwargs["extra_patterns"]["hermetic"], True)
        self.assertEqual(result, expected_build)

    async def test_get_latest_konflux_build_open_mode(self):
        """
        Test that enforce_network_mode=True adds hermetic=False filter for open images
        """
        # given
        runtime = MagicMock()
        runtime.build_system = "konflux"
        runtime.assembly = "stream"
        runtime.group = "openshift-4.20"
        runtime.konflux_db = MagicMock()
        runtime.logger = MagicMock()

        data_obj = MagicMock()
        data_obj.key = "test-image"
        data_obj.filename = "test-image.yml"
        data_obj.path = "/path/to/test-image.yml"
        data_obj.data = {"name": "test-image"}

        runtime.get_releases_config = MagicMock(return_value={})
        meta = MetadataBase("image", runtime, data_obj)
        meta.meta_type = "image"
        meta.distgit_key = "test-image"
        meta.branch_el_target = MagicMock(return_value=9)

        expected_build = MagicMock(nvr="test-image-1.0.0-1.el9", hermetic=False)
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=expected_build)

        # when
        with patch.object(meta, "get_konflux_network_mode", return_value="open"):
            result = await meta.get_latest_konflux_build(enforce_network_mode=True)
            meta.get_konflux_network_mode.assert_called_once()

        # then
        runtime.konflux_db.get_latest_build.assert_called_once()
        call_kwargs = runtime.konflux_db.get_latest_build.call_args[1]

        self.assertIn("extra_patterns", call_kwargs)
        self.assertEqual(call_kwargs["extra_patterns"]["hermetic"], False)
        self.assertEqual(result, expected_build)

    async def test_get_latest_konflux_build_no_enforce_network_mode(self):
        """
        Test that enforce_network_mode=False does not add hermetic filter
        """
        # given
        runtime = MagicMock()
        runtime.build_system = "konflux"
        runtime.assembly = "stream"
        runtime.group = "openshift-4.20"
        runtime.konflux_db = MagicMock()
        runtime.logger = MagicMock()

        data_obj = MagicMock()
        data_obj.key = "test-image"
        data_obj.filename = "test-image.yml"
        data_obj.path = "/path/to/test-image.yml"
        data_obj.data = {"name": "test-image"}

        runtime.get_releases_config = MagicMock(return_value={})
        meta = MetadataBase("image", runtime, data_obj)
        meta.meta_type = "image"
        meta.distgit_key = "test-image"
        meta.branch_el_target = MagicMock(return_value=9)

        expected_build = MagicMock(nvr="test-image-1.0.0-1.el9")
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=expected_build)

        # when
        with patch.object(meta, "get_konflux_network_mode", return_value="hermetic") as mock_network_mode:
            result = await meta.get_latest_konflux_build(enforce_network_mode=False)
            mock_network_mode.assert_not_called()

        # then
        runtime.konflux_db.get_latest_build.assert_called_once()
        call_kwargs = runtime.konflux_db.get_latest_build.call_args[1]

        if "extra_patterns" in call_kwargs:
            self.assertNotIn("hermetic", call_kwargs["extra_patterns"])
        self.assertEqual(result, expected_build)

    async def test_get_latest_konflux_build_merges_extra_patterns(self):
        """
        Test that enforce_network_mode=True merges hermetic with existing extra_patterns
        """
        # given
        runtime = MagicMock()
        runtime.build_system = "konflux"
        runtime.assembly = "stream"
        runtime.group = "openshift-4.20"
        runtime.konflux_db = MagicMock()
        runtime.logger = MagicMock()

        data_obj = MagicMock()
        data_obj.key = "test-image"
        data_obj.filename = "test-image.yml"
        data_obj.path = "/path/to/test-image.yml"
        data_obj.data = {"name": "test-image"}

        runtime.get_releases_config = MagicMock(return_value={})
        meta = MetadataBase("image", runtime, data_obj)
        meta.meta_type = "image"
        meta.distgit_key = "test-image"
        meta.branch_el_target = MagicMock(return_value=9)

        expected_build = MagicMock(nvr="test-image-1.0.0-1.el9")
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=expected_build)
        existing_patterns = {"release": "b45ea65"}

        # when
        with patch.object(meta, "get_konflux_network_mode", return_value="hermetic"):
            result = await meta.get_latest_konflux_build(enforce_network_mode=True, extra_patterns=existing_patterns)
            meta.get_konflux_network_mode.assert_called_once()

        # then
        runtime.konflux_db.get_latest_build.assert_called_once()
        call_kwargs = runtime.konflux_db.get_latest_build.call_args[1]

        self.assertIn("extra_patterns", call_kwargs)
        self.assertEqual(call_kwargs["extra_patterns"]["hermetic"], True)
        self.assertEqual(call_kwargs["extra_patterns"]["release"], "b45ea65")
        self.assertEqual(result, expected_build)


if __name__ == "__main__":
    unittest.main()
