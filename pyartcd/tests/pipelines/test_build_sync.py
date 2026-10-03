"""Tests for build_sync pipeline QCI mirroring functionality."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class TestMirrorRhcosToQci(unittest.IsolatedAsyncioTestCase):
    """Tests for _mirror_rhcos_to_qci functionality."""

    def _create_pipeline(self, version='4.17', assembly='stream', doozer_data_gitref=''):
        """Create a BuildSyncPipeline instance with mocked dependencies."""
        with patch('pyartcd.pipelines.build_sync.GroupRuntime'):
            from pyartcd.pipelines.build_sync import BuildSyncPipeline

            runtime = MagicMock()
            runtime.logger = MagicMock()
            runtime.dry_run = False
            runtime.working_dir = '/tmp/test'
            runtime.config = MagicMock()

            pipeline = BuildSyncPipeline(
                runtime=runtime,
                version=version,
                assembly=assembly,
                publish=False,
                data_path='https://github.com/openshift-eng/ocp-build-data',
                emergency_ignore_issues=False,
                retrigger_current_nightly=False,
                doozer_data_gitref=doozer_data_gitref,
                images='',
                exclude_arches='',
                skip_multiarch_payload=False,
                embargo_permit_ack=False,
                build_system='konflux',
            )
            pipeline.group_runtime = MagicMock()
            pipeline.slack_client = AsyncMock()
            pipeline._registry_config = '/tmp/auth.json'
            return pipeline

    @patch.dict('os.environ', {'QCI_USER': 'test_user', 'QCI_PASSWORD': 'test_pass'})
    @patch('pyartcd.pipelines.build_sync.rhcos')
    async def test_mirror_rhcos_to_qci_success(self, mock_rhcos):
        """Test successful mirroring of RHCOS images to QCI."""
        pipeline = self._create_pipeline()
        mock_rhcos.get_container_names.return_value = {'rhel-coreos', 'rhel-coreos-extensions'}

        with patch.object(pipeline, '_mirror_single_rhcos_to_qci', new_callable=AsyncMock) as mock_mirror:
            await pipeline._mirror_rhcos_to_qci()

            # Should be called for each tag, both public and private
            assert mock_mirror.call_count == 4  # 2 tags * 2 (public + private)

    @patch.dict('os.environ', {}, clear=True)
    async def test_mirror_rhcos_to_qci_skips_without_credentials(self):
        """Test QCI mirroring is skipped when credentials not available."""
        pipeline = self._create_pipeline()

        with patch.object(pipeline, '_mirror_single_rhcos_to_qci', new_callable=AsyncMock) as mock_mirror:
            await pipeline._mirror_rhcos_to_qci()

            # Should not attempt to mirror without credentials
            mock_mirror.assert_not_called()

    async def test_mirror_rhcos_to_qci_skips_old_versions(self):
        """Test QCI mirroring is skipped for versions < 4.12."""
        pipeline = self._create_pipeline(version='4.11')

        with patch.dict('os.environ', {'QCI_USER': 'user', 'QCI_PASSWORD': 'pass'}):
            with patch.object(pipeline, '_mirror_single_rhcos_to_qci', new_callable=AsyncMock) as mock_mirror:
                await pipeline._mirror_rhcos_to_qci()

                mock_mirror.assert_not_called()

    async def test_mirror_rhcos_to_qci_skips_non_stream_assembly(self):
        """Test QCI mirroring is skipped for non-stream assemblies."""
        pipeline = self._create_pipeline(assembly='4.17.1')

        with patch.dict('os.environ', {'QCI_USER': 'user', 'QCI_PASSWORD': 'pass'}):
            with patch.object(pipeline, '_mirror_single_rhcos_to_qci', new_callable=AsyncMock) as mock_mirror:
                await pipeline._mirror_rhcos_to_qci()

                mock_mirror.assert_not_called()

    async def test_mirror_rhcos_to_qci_skips_custom_gitref(self):
        """Test QCI mirroring is skipped when custom data gitref is specified."""
        pipeline = self._create_pipeline(doozer_data_gitref='custom-branch')

        with patch.dict('os.environ', {'QCI_USER': 'user', 'QCI_PASSWORD': 'pass'}):
            with patch.object(pipeline, '_mirror_single_rhcos_to_qci', new_callable=AsyncMock) as mock_mirror:
                await pipeline._mirror_rhcos_to_qci()

                mock_mirror.assert_not_called()


class TestMirrorSingleRhcosToQci(unittest.IsolatedAsyncioTestCase):
    """Tests for _mirror_single_rhcos_to_qci functionality."""

    def _create_pipeline(self, dry_run=False):
        """Create a BuildSyncPipeline instance with mocked dependencies."""
        with patch('pyartcd.pipelines.build_sync.GroupRuntime'):
            from pyartcd.pipelines.build_sync import BuildSyncPipeline

            runtime = MagicMock()
            runtime.logger = MagicMock()
            runtime.dry_run = dry_run
            runtime.working_dir = '/tmp/test'
            runtime.config = MagicMock()

            pipeline = BuildSyncPipeline(
                runtime=runtime,
                version='4.17',
                assembly='stream',
                publish=False,
                data_path='https://github.com/openshift-eng/ocp-build-data',
                emergency_ignore_issues=False,
                retrigger_current_nightly=False,
                doozer_data_gitref='',
                images='',
                exclude_arches='',
                skip_multiarch_payload=False,
                embargo_permit_ack=False,
                build_system='konflux',
            )
            pipeline.group_runtime = MagicMock()
            pipeline.slack_client = AsyncMock()
            pipeline._registry_config = '/tmp/auth.json'
            return pipeline

    @patch.dict('os.environ', {'KUBECONFIG': '/tmp/kubeconfig'})
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_mirror_single_rhcos_public(self, mock_cmd_assert, mock_cmd_gather):
        """Test mirroring a single RHCOS tag to public QCI."""
        pipeline = self._create_pipeline()

        # Mock getting the pullspec from imagestream
        mock_cmd_gather.return_value = (0, 'quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123', '')

        await pipeline._mirror_single_rhcos_to_qci('rhel-coreos', '20240101120000', private=False)

        # Verify oc get istag was called for public namespace
        mock_cmd_gather.assert_called_once()
        assert '-n ocp ' in mock_cmd_gather.call_args[0][0]
        assert 'art-latest:rhel-coreos' in mock_cmd_gather.call_args[0][0]

        # Verify oc image mirror was called twice (prunable + floating)
        assert mock_cmd_assert.call_count == 2

    @patch.dict('os.environ', {'KUBECONFIG': '/tmp/kubeconfig'})
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_mirror_single_rhcos_private(self, mock_cmd_assert, mock_cmd_gather):
        """Test mirroring a single RHCOS tag to private QCI."""
        pipeline = self._create_pipeline()

        mock_cmd_gather.return_value = (0, 'quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123', '')

        await pipeline._mirror_single_rhcos_to_qci('rhel-coreos', '20240101120000', private=True)

        # Private uses same public ART imagestream as source
        mock_cmd_gather.assert_called_once()
        assert '-n ocp ' in mock_cmd_gather.call_args[0][0]

        # Should still mirror to QCI with _priv suffix
        assert mock_cmd_assert.call_count == 2
        first_call = mock_cmd_assert.call_args_list[0][0][0]
        assert '_priv_' in first_call

    @patch.dict('os.environ', {'KUBECONFIG': '/tmp/kubeconfig'})
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_mirror_single_rhcos_dry_run(self, mock_cmd_assert, mock_cmd_gather):
        """Test dry run mode doesn't actually mirror."""
        pipeline = self._create_pipeline(dry_run=True)

        mock_cmd_gather.return_value = (0, 'quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123', '')

        await pipeline._mirror_single_rhcos_to_qci('rhel-coreos', '20240101120000', private=False)

        # Verify oc get istag was called (to get pullspec)
        mock_cmd_gather.assert_called_once()

        # Verify oc image mirror was NOT called in dry run
        mock_cmd_assert.assert_not_called()

    @patch.dict('os.environ', {'KUBECONFIG': '/tmp/kubeconfig'})
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_mirror_single_rhcos_missing_pullspec(self, mock_cmd_gather):
        """Test handling when pullspec is not found."""
        pipeline = self._create_pipeline()

        # Simulate failed oc get istag
        mock_cmd_gather.return_value = (1, '', 'not found')

        # Should not raise, just log warning and return
        await pipeline._mirror_single_rhcos_to_qci('rhel-coreos', '20240101120000', private=False)

    @patch.dict('os.environ', {'KUBECONFIG': '/tmp/kubeconfig'})
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_mirror_qci_tag_naming(self, mock_cmd_assert, mock_cmd_gather):
        """Test QCI tag naming convention and oc image mirror command."""
        pipeline = self._create_pipeline()
        pipeline._registry_config = '/tmp/registry_config.json'

        mock_cmd_gather.return_value = (0, 'quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123', '')

        await pipeline._mirror_single_rhcos_to_qci('rhel-coreos-extensions', '20240101120000', private=False)

        # Verify correct tag naming pattern and oc image mirror command
        calls = mock_cmd_assert.call_args_list
        prunable_call = calls[0][0][0]
        assert 'oc image mirror' in prunable_call
        assert '--keep-manifest-list' in prunable_call
        assert '--registry-config=' in prunable_call
        assert 'quay.io/openshift/ci:20240101120000_prune_art__ocp_4.17_rhel-coreos-extensions' in prunable_call

        floating_call = calls[1][0][0]
        assert 'quay.io/openshift/ci:art__ocp_4.17_rhel-coreos-extensions' in floating_call


class TestRegistryConfigWithQci(unittest.IsolatedAsyncioTestCase):
    """Tests for RegistryConfig setup with QCI credentials."""

    @patch('pyartcd.pipelines.build_sync.RegistryConfig')
    @patch('pyartcd.pipelines.build_sync.GroupRuntime')
    @patch.dict(
        'os.environ',
        {
            'QUAY_AUTH_FILE': '/tmp/quay_auth.json',
            'KUBECONFIG': '/tmp/kubeconfig',
            'QCI_USER': 'test_user',
            'QCI_PASSWORD': 'test_pass',
        },
    )
    async def test_registry_config_includes_qci_credentials(self, mock_group_runtime, mock_registry_config):
        """Test RegistryConfig is created with QCI credentials."""
        from pyartcd.pipelines.build_sync import BuildSyncPipeline

        runtime = MagicMock()
        runtime.logger = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = '/tmp/test'
        runtime.config = MagicMock()

        # Setup mock context manager
        mock_registry_config.return_value.__enter__ = MagicMock(return_value='/tmp/merged_auth.json')
        mock_registry_config.return_value.__exit__ = MagicMock(return_value=False)

        pipeline = BuildSyncPipeline(
            runtime=runtime,
            version='4.17',
            assembly='stream',
            publish=False,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            emergency_ignore_issues=False,
            retrigger_current_nightly=False,
            doozer_data_gitref='',
            images='',
            exclude_arches='',
            skip_multiarch_payload=False,
            embargo_permit_ack=False,
            build_system='konflux',
        )
        pipeline.group_runtime = MagicMock()
        pipeline._run_pipeline = AsyncMock()

        await pipeline.run()

        # Verify RegistryConfig was called with QCI credentials
        mock_registry_config.assert_called_once()
        call_kwargs = mock_registry_config.call_args.kwargs

        from artcommonlib.constants import REGISTRY_QUAY_CI

        # QCI should NOT be in the registries list (it's for source file extraction)
        assert REGISTRY_QUAY_CI not in call_kwargs['registries']

        # QCI should be in the credentials list (explicit credentials)
        assert len(call_kwargs['credentials']) == 1
        cred = call_kwargs['credentials'][0]
        assert cred.registry == REGISTRY_QUAY_CI

    @patch('pyartcd.pipelines.build_sync.RegistryConfig')
    @patch('pyartcd.pipelines.build_sync.GroupRuntime')
    @patch.dict(
        'os.environ',
        {
            'QUAY_AUTH_FILE': '/tmp/quay_auth.json',
            'KUBECONFIG': '/tmp/kubeconfig',
        },
        clear=True,
    )
    async def test_registry_config_without_qci_credentials(self, mock_group_runtime, mock_registry_config):
        """Test RegistryConfig is created without QCI credentials when not available."""
        from pyartcd.pipelines.build_sync import BuildSyncPipeline

        runtime = MagicMock()
        runtime.logger = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = '/tmp/test'
        runtime.config = MagicMock()

        mock_registry_config.return_value.__enter__ = MagicMock(return_value='/tmp/merged_auth.json')
        mock_registry_config.return_value.__exit__ = MagicMock(return_value=False)

        pipeline = BuildSyncPipeline(
            runtime=runtime,
            version='4.17',
            assembly='stream',
            publish=False,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            emergency_ignore_issues=False,
            retrigger_current_nightly=False,
            doozer_data_gitref='',
            images='',
            exclude_arches='',
            skip_multiarch_payload=False,
            embargo_permit_ack=False,
            build_system='konflux',
        )
        pipeline.group_runtime = MagicMock()
        pipeline._run_pipeline = AsyncMock()

        await pipeline.run()

        # Verify RegistryConfig was called without QCI credentials
        mock_registry_config.assert_called_once()
        call_kwargs = mock_registry_config.call_args.kwargs

        # Should have empty credentials list
        assert call_kwargs['credentials'] == []

        # Should have logged a warning
        runtime.logger.warning.assert_called()
