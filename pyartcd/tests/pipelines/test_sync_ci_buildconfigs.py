import unittest
from unittest import mock

import pytest
from pyartcd.pipelines.sync_ci_buildconfigs import SyncCIBuildconfigsPipeline
from pyartcd.runtime import Runtime


class TestSyncCIBuildconfigsPipeline:
    """Tests for SyncCIBuildconfigsPipeline class."""

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
        pipeline = SyncCIBuildconfigsPipeline(mock_runtime, for_release="4.17")

        assert pipeline.runtime == mock_runtime
        assert pipeline.version == "4.17"
        assert pipeline.assembly == "stream"

    def test_validate_invalid_version_format(self, mock_runtime):
        """Test validation fails for invalid version format."""
        with pytest.raises(ValueError, match="Invalid FOR_RELEASE format"):
            SyncCIBuildconfigsPipeline(mock_runtime, for_release="invalid")

    def test_validate_for_release_required(self, mock_runtime):
        """Test validation fails when FOR_RELEASE is empty string."""
        with pytest.raises(ValueError, match="FOR_RELEASE is required"):
            SyncCIBuildconfigsPipeline(mock_runtime, for_release="")

    def test_validate_invalid_assembly_format(self, mock_runtime):
        """Test validation fails for invalid assembly format."""
        with pytest.raises(ValueError, match="Invalid ASSEMBLY format"):
            SyncCIBuildconfigsPipeline(mock_runtime, for_release="4.17", assembly="invalid@assembly")

    def test_images_parsed_as_list(self, mock_runtime):
        """Test comma-separated IMAGES string is parsed into a list."""
        pipeline = SyncCIBuildconfigsPipeline(
            mock_runtime,
            for_release="4.17",
            images="ci-openshift-base.rhel10,ci-openshift-base.rhel9",
        )
        assert pipeline.images == ["ci-openshift-base.rhel10", "ci-openshift-base.rhel9"]

    def test_filter_args_with_stream_only(self, mock_runtime):
        """Test _filter_args returns only --stream when no images."""
        pipeline = SyncCIBuildconfigsPipeline(mock_runtime, for_release="4.17", only_stream="rhel10")
        assert pipeline._filter_args == "--stream rhel10"

    def test_live_test_mode_false_for_stream_assembly(self, mock_runtime):
        """Test _live_test_mode is False for the default stream assembly."""
        pipeline = SyncCIBuildconfigsPipeline(mock_runtime, for_release="4.17")
        assert pipeline._live_test_mode is False

    def test_live_test_mode_true_for_test_assembly(self, mock_runtime):
        """Test _live_test_mode is True when assembly is test."""
        pipeline = SyncCIBuildconfigsPipeline(mock_runtime, for_release="4.17", assembly="test")
        assert pipeline._live_test_mode is True


class TestSyncCIBuildconfigsRun(unittest.IsolatedAsyncioTestCase):
    """Tests for the run() workflow: gen-buildconfigs, start-builds, check-upstream in sequence."""

    def _mock_runtime(self):
        runtime = mock.MagicMock(spec=Runtime)
        runtime.logger = mock.MagicMock()
        runtime.working_dir = mock.MagicMock()
        runtime.doozer_working = "/workspace/doozer_working"
        runtime.dry_run = False
        return runtime

    @mock.patch('pyartcd.pipelines.sync_ci_buildconfigs.jenkins')
    async def test_run_executes_steps_in_order(self, _mock_jenkins):
        """Test run() clones build data then runs gen-buildconfigs, start-builds, check-upstream in order."""
        pipeline = SyncCIBuildconfigsPipeline(self._mock_runtime(), for_release="4.17")

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
            self.assertEqual(pipeline._run_doozer_command.await_count, 3)
            subcommands = [call.args[1] for call in pipeline._run_doozer_command.await_args_list]
            self.assertEqual(
                subcommands,
                ["images:streams gen-buildconfigs", "images:streams start-builds", "images:streams check-upstream"],
            )

    @mock.patch('pyartcd.pipelines.sync_ci_buildconfigs.jenkins')
    async def test_run_passes_live_test_mode_for_test_assembly(self, _mock_jenkins):
        """Test run() passes --live-test-mode to all three doozer subcommands when assembly is test."""
        pipeline = SyncCIBuildconfigsPipeline(self._mock_runtime(), for_release="4.17", assembly="test")

        with (
            mock.patch.object(pipeline, '_clone_ocp_build_data', new=mock.AsyncMock(return_value="/tmp/group")),
            mock.patch.object(pipeline, '_create_registry_config') as mock_registry_config,
            mock.patch.object(pipeline, '_run_doozer_command', new=mock.AsyncMock(return_value=(0, "", ""))),
            mock.patch.object(pipeline, '_cleanup'),
        ):
            mock_registry_config.return_value.__enter__ = mock.Mock(return_value="/tmp/auth.json")
            mock_registry_config.return_value.__exit__ = mock.Mock(return_value=False)

            await pipeline.run()

            for call in pipeline._run_doozer_command.call_args_list:
                args, _ = call
                self.assertIn("--live-test-mode", args[2])

    @mock.patch('pyartcd.pipelines.sync_ci_buildconfigs.jenkins')
    async def test_run_cleans_up_on_failure(self, _mock_jenkins):
        """Test run() cleans up the clone directory and re-raises on failure."""
        pipeline = SyncCIBuildconfigsPipeline(self._mock_runtime(), for_release="4.17")

        with (
            mock.patch.object(pipeline, '_clone_ocp_build_data', new=mock.AsyncMock(return_value="/tmp/group")),
            mock.patch.object(pipeline, '_create_registry_config', side_effect=RuntimeError("boom")),
            mock.patch.object(pipeline, '_cleanup') as mock_cleanup,
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await pipeline.run()

            mock_cleanup.assert_called_once_with("/tmp/group")
