import unittest
from unittest import mock

import pytest
from pyartcd.pipelines.mirror_images_to_ci import MirrorImagesToCIPipeline
from pyartcd.runtime import Runtime


class TestMirrorImagesToCIPipeline:
    """Tests for MirrorImagesToCIPipeline class."""

    @pytest.fixture
    def mock_runtime(self):
        """Create mock Runtime instance."""
        runtime = mock.MagicMock(spec=Runtime)
        runtime.logger = mock.MagicMock()
        runtime.working_dir = mock.MagicMock()
        runtime.doozer_working = "/workspace/doozer_working"
        runtime.dry_run = False
        return runtime

    def test_init_with_defaults(self, mock_runtime):
        """Test initialization with default parameters."""
        pipeline = MirrorImagesToCIPipeline(mock_runtime, version="4.17")

        assert pipeline.runtime == mock_runtime
        assert pipeline.version == "4.17"
        assert pipeline.assembly == "stream"
        assert pipeline.update_images_only_when_missing is False

    def test_init_with_custom_params(self, mock_runtime):
        """Test initialization with custom parameters."""
        pipeline = MirrorImagesToCIPipeline(
            mock_runtime,
            version="4.17",
            assembly="4.17.1",
            update_images_only_when_missing=True,
        )

        assert pipeline.version == "4.17"
        assert pipeline.assembly == "4.17.1"
        assert pipeline.update_images_only_when_missing is True

    def test_validate_invalid_version_format(self, mock_runtime):
        """Test validation fails for invalid version format."""
        with pytest.raises(ValueError, match="Invalid VERSION format"):
            MirrorImagesToCIPipeline(mock_runtime, version="invalid")

    def test_validate_version_required(self, mock_runtime):
        """Test validation fails when VERSION is empty string."""
        with pytest.raises(ValueError, match="VERSION is required"):
            MirrorImagesToCIPipeline(mock_runtime, version="")

    def test_validate_invalid_assembly_format(self, mock_runtime):
        """Test validation fails for invalid assembly format."""
        with pytest.raises(ValueError, match="Invalid ASSEMBLY format"):
            MirrorImagesToCIPipeline(mock_runtime, version="4.17", assembly="invalid@assembly")

    def test_images_parsed_as_list(self, mock_runtime):
        """Test comma-separated IMAGES string is parsed into a list."""
        pipeline = MirrorImagesToCIPipeline(
            mock_runtime,
            version="4.17",
            images="ci-openshift-base.rhel10,ci-openshift-base.rhel9",
        )
        assert pipeline.images == ["ci-openshift-base.rhel10", "ci-openshift-base.rhel9"]

    def test_images_empty_string_yields_empty_list(self, mock_runtime):
        """Test empty IMAGES string yields empty list."""
        pipeline = MirrorImagesToCIPipeline(mock_runtime, version="4.17", images="")
        assert pipeline.images == []

    def test_filter_args_with_stream_only(self, mock_runtime):
        """Test _filter_args returns only --stream when no images."""
        pipeline = MirrorImagesToCIPipeline(mock_runtime, version="4.17", only_stream="rhel10")
        assert pipeline._filter_args == "--stream rhel10"

    def test_init_data_path_defaults_to_official(self, mock_runtime):
        """Test data_path defaults to OCP_BUILD_DATA_URL."""
        from pyartcd.constants import OCP_BUILD_DATA_URL

        pipeline = MirrorImagesToCIPipeline(mock_runtime, version="4.17")

        assert pipeline.data_path == OCP_BUILD_DATA_URL
        assert pipeline.data_gitref == ""


class TestMirrorImagesToCIRun(unittest.IsolatedAsyncioTestCase):
    """Tests for the run() workflow, verifying it skips the SHA change-detection gate."""

    def _mock_runtime(self):
        runtime = mock.MagicMock(spec=Runtime)
        runtime.logger = mock.MagicMock()
        runtime.working_dir = mock.MagicMock()
        runtime.doozer_working = "/workspace/doozer_working"
        runtime.dry_run = False
        return runtime

    @mock.patch('pyartcd.pipelines.mirror_images_to_ci.jenkins')
    async def test_run_mirrors_unconditionally(self, _mock_jenkins):
        """Test run() clones build data and mirrors images without a change-detection gate."""
        pipeline = MirrorImagesToCIPipeline(self._mock_runtime(), version="4.17")

        with (
            mock.patch.object(pipeline, '_clone_ocp_build_data', new=mock.AsyncMock(return_value="/tmp/group")),
            mock.patch.object(pipeline, '_create_registry_config') as mock_registry_config,
            mock.patch.object(pipeline, '_run_doozer_command', new=mock.AsyncMock(return_value=(0, "", ""))),
            mock.patch.object(pipeline, '_cleanup'),
        ):
            mock_registry_config.return_value.__enter__ = mock.Mock(return_value="/tmp/auth.json")
            mock_registry_config.return_value.__exit__ = mock.Mock(return_value=False)

            rc = await pipeline.run()

            self.assertEqual(rc, 0)
            pipeline._clone_ocp_build_data.assert_awaited_once_with("4.17")
            pipeline._run_doozer_command.assert_awaited_once()
            args, _ = pipeline._run_doozer_command.call_args
            self.assertEqual(args[1], "images:streams mirror")

    @mock.patch('pyartcd.pipelines.mirror_images_to_ci.jenkins')
    async def test_run_cleans_up_on_failure(self, _mock_jenkins):
        """Test run() cleans up the clone directory and re-raises on failure."""
        pipeline = MirrorImagesToCIPipeline(self._mock_runtime(), version="4.17")

        with (
            mock.patch.object(pipeline, '_clone_ocp_build_data', new=mock.AsyncMock(return_value="/tmp/group")),
            mock.patch.object(pipeline, '_create_registry_config', side_effect=RuntimeError("boom")),
            mock.patch.object(pipeline, '_cleanup') as mock_cleanup,
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await pipeline.run()

            mock_cleanup.assert_called_once_with("/tmp/group")
