from unittest import mock

import pytest
from pyartcd.pipelines.sync_ci_images import SyncCIImagesPipeline
from pyartcd.runtime import Runtime


class TestSyncCIImagesPipeline:
    """Tests for SyncCIImagesPipeline class."""

    @pytest.fixture
    def mock_runtime(self):
        """Create mock Runtime instance."""
        runtime = mock.MagicMock(spec=Runtime)
        runtime.logger = mock.MagicMock()
        runtime.working_dir = mock.MagicMock()
        runtime.doozer_working = "/workspace/doozer_working"
        return runtime

    def test_init_with_defaults(self, mock_runtime):
        """Test initialization with default parameters."""
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")

        assert pipeline.runtime == mock_runtime
        assert pipeline.version == "4.17"
        assert pipeline.assembly == "stream"
        assert pipeline.skip_prs is False
        assert pipeline.force_run is False

    def test_init_with_custom_params(self, mock_runtime):
        """Test initialization with custom parameters."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            assembly="4.17.1",
            skip_prs=True,
            force_run=True,
        )

        assert pipeline.version == "4.17"
        assert pipeline.assembly == "4.17.1"
        assert pipeline.skip_prs is True
        assert pipeline.force_run is True

    def test_validate_invalid_version_format(self, mock_runtime):
        """Test validation fails for invalid version format."""
        with pytest.raises(ValueError, match="Invalid FOR_RELEASE format"):
            SyncCIImagesPipeline(mock_runtime, for_release="invalid")

    def test_validate_for_release_required(self, mock_runtime):
        """Test validation fails when FOR_RELEASE is empty string."""
        with pytest.raises(ValueError, match="FOR_RELEASE is required"):
            SyncCIImagesPipeline(mock_runtime, for_release="")

    def test_validate_auto_sets_skip_prs_for_only_stream(self, mock_runtime):
        """Test SKIP_PRS automatically set to true when ONLY_STREAM specified."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            only_stream="golang",
            skip_prs=False,  # Will be overridden
        )

        assert pipeline.skip_prs is True

    def test_validate_valid_version_formats(self, mock_runtime):
        """Test validation passes for valid version formats."""
        # Single digit versions
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")
        assert pipeline.version == "4.17"

        # Double digit minor version
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.18")
        assert pipeline.version == "4.18"

    def test_validate_invalid_version_formats(self, mock_runtime):
        """Test validation fails for various invalid version formats."""
        # Missing minor version
        with pytest.raises(ValueError, match="Invalid FOR_RELEASE format"):
            SyncCIImagesPipeline(mock_runtime, for_release="4")

        # Three components
        with pytest.raises(ValueError, match="Invalid FOR_RELEASE format"):
            SyncCIImagesPipeline(mock_runtime, for_release="4.17.1")

        # Text instead of numbers
        with pytest.raises(ValueError, match="Invalid FOR_RELEASE format"):
            SyncCIImagesPipeline(mock_runtime, for_release="four.seventeen")

    def test_validate_valid_assembly_formats(self, mock_runtime):
        """Test validation passes for valid assembly formats."""
        # Simple name
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17", assembly="stream")
        assert pipeline.assembly == "stream"

        # With version numbers
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17", assembly="4.17.1")
        assert pipeline.assembly == "4.17.1"

        # With dashes and underscores
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.18", assembly="4.18.0-ec.3")
        assert pipeline.assembly == "4.18.0-ec.3"

    def test_validate_invalid_assembly_format(self, mock_runtime):
        """Test validation fails for invalid assembly format."""
        # Special characters not allowed
        with pytest.raises(ValueError, match="Invalid ASSEMBLY format"):
            SyncCIImagesPipeline(mock_runtime, for_release="4.17", assembly="invalid@assembly")

    def test_validate_skip_prs_not_overridden_when_only_stream_not_set(self, mock_runtime):
        """Test SKIP_PRS is not modified when ONLY_STREAM is not set."""
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17", skip_prs=False)

        assert pipeline.skip_prs is False

    def test_validate_skip_prs_already_true_not_changed(self, mock_runtime):
        """Test SKIP_PRS stays true when already set and ONLY_STREAM specified."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            only_stream="golang",
            skip_prs=True,  # Already true
        )

        assert pipeline.skip_prs is True

    def test_validate_auto_sets_skip_prs_for_images(self, mock_runtime):
        """Test SKIP_PRS automatically set to true when IMAGES specified."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            images="ci-openshift-base.rhel10",
            skip_prs=False,
        )
        assert pipeline.skip_prs is True

    def test_images_parsed_as_list(self, mock_runtime):
        """Test comma-separated IMAGES string is parsed into a list."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            images="ci-openshift-base.rhel10,ci-openshift-base.rhel9",
        )
        assert pipeline.images == ["ci-openshift-base.rhel10", "ci-openshift-base.rhel9"]

    def test_images_empty_string_yields_empty_list(self, mock_runtime):
        """Test empty IMAGES string yields empty list."""
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17", images="")
        assert pipeline.images == []

    def test_images_whitespace_handling(self, mock_runtime):
        """Test IMAGES strips whitespace around entries."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            images=" ci-openshift-base.rhel10 , ci-openshift-base.rhel9 ",
        )
        assert pipeline.images == ["ci-openshift-base.rhel10", "ci-openshift-base.rhel9"]

    def test_filter_args_with_stream_only(self, mock_runtime):
        """Test _filter_args returns only --stream when no images."""
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17", only_stream="rhel10")
        assert pipeline._filter_args == "--stream rhel10"

    def test_filter_args_with_images_only(self, mock_runtime):
        """Test _filter_args returns only --image args when no stream."""
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17", images="ci-openshift-base.rhel10")
        assert pipeline._filter_args == "--image ci-openshift-base.rhel10"

    def test_filter_args_with_both(self, mock_runtime):
        """Test _filter_args returns both --stream and --image args."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            only_stream="rhel10",
            images="ci-openshift-base.rhel10,ci-openshift-base.rhel9",
        )
        assert (
            pipeline._filter_args == "--stream rhel10 --image ci-openshift-base.rhel10 --image ci-openshift-base.rhel9"
        )

    def test_filter_args_empty_when_neither(self, mock_runtime):
        """Test _filter_args returns empty string when neither stream nor images."""
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")
        assert pipeline._filter_args == ""

    def test_init_with_custom_data_path(self, mock_runtime):
        """Test initialization with custom data path and gitref."""
        pipeline = SyncCIImagesPipeline(
            mock_runtime,
            for_release="4.17",
            data_path="https://github.com/myorg/ocp-build-data-fork",
            data_gitref="my-branch",
        )

        assert pipeline.data_path == "https://github.com/myorg/ocp-build-data-fork"
        assert pipeline.data_gitref == "my-branch"

    def test_init_data_path_defaults_to_official(self, mock_runtime):
        """Test data_path defaults to OCP_BUILD_DATA_URL."""
        from pyartcd.constants import OCP_BUILD_DATA_URL

        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")

        assert pipeline.data_path == OCP_BUILD_DATA_URL
        assert pipeline.data_gitref == ""


class TestCredentialManagement:
    """Tests for credential validation and RegistryConfig setup."""

    @pytest.fixture
    def mock_runtime(self):
        runtime = mock.MagicMock(spec=Runtime)
        runtime.logger = mock.MagicMock()
        runtime.working_dir = mock.MagicMock()
        runtime.doozer_working = "/workspace/doozer_working"
        return runtime

    def test_get_required_env_success(self, mock_runtime):
        """Test getting required environment variable."""
        with mock.patch.dict('os.environ', {'TEST_VAR': 'test_value'}):
            pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")
            value = pipeline._get_required_env('TEST_VAR')
            assert value == 'test_value'

    def test_get_required_env_missing_raises(self, mock_runtime):
        """Test error when required env var is missing."""
        pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")

        with pytest.raises(ValueError, match="Required environment variable TEST_VAR not set"):
            pipeline._get_required_env('TEST_VAR')

    def test_get_required_env_file_not_found(self, mock_runtime):
        """Test error when credential file doesn't exist."""
        with mock.patch.dict('os.environ', {'QUAY_AUTH_FILE': '/nonexistent/file'}):
            pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")

            with pytest.raises(FileNotFoundError, match="QUAY_AUTH_FILE file not found"):
                pipeline._get_required_env('QUAY_AUTH_FILE')

    @mock.patch('pyartcd.pipelines.sync_ci_images.RegistryConfig')
    def test_create_registry_config(self, mock_registry_config, mock_runtime, tmp_path):
        """Test RegistryConfig creation with all required credentials."""
        # Create temporary credential files
        quay_auth = tmp_path / "quay_auth.json"
        kubeconfig = tmp_path / "kubeconfig"
        quay_auth.write_text('{"auths": {}}')
        kubeconfig.write_text('apiVersion: v1')

        with mock.patch.dict(
            'os.environ',
            {
                'QUAY_AUTH_FILE': str(quay_auth),
                'KUBECONFIG': str(kubeconfig),
                'QCI_USER': 'test_user',
                'QCI_PASSWORD': 'test_pass',
            },
        ):
            pipeline = SyncCIImagesPipeline(mock_runtime, for_release="4.17")
            pipeline._create_registry_config()

            # Verify RegistryConfig called with correct parameters
            mock_registry_config.assert_called_once()
            call_kwargs = mock_registry_config.call_args.kwargs

            assert call_kwargs['source_files'] == [str(quay_auth)]
            assert call_kwargs['kubeconfig'] == str(kubeconfig)
            assert len(call_kwargs['registries']) == 6
            # Verify uses constants, not hardcoded strings
            from artcommonlib.constants import REGISTRY_CI_OPENSHIFT

            assert REGISTRY_CI_OPENSHIFT in call_kwargs['registries']
            assert len(call_kwargs['credentials']) == 1
