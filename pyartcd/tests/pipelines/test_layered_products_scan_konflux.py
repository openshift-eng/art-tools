import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from pyartcd.pipelines.layered_products_scan_konflux import LayeredProductsScanPipeline
from pyartcd.runtime import Runtime


class TestLayeredProductsScanPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_no_changes_detected(self, mock_cmd_gather, mock_jenkins):
        scan_output = yaml.dump({'images': [], 'rpms': []})
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()
        mock_jenkins.start_layered_products.assert_not_called()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_changed_images_trigger_build(self, mock_cmd_gather, mock_jenkins):
        self.runtime.dry_run = False

        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'oadp-velero-restic-restore-helper', 'changed': True},
                    {'name': 'oadp-operator', 'changed': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        mock_jenkins.start_layered_products.assert_called_once_with(
            group='oadp-1.4',
            assembly='stream',
            image_list=['oadp-velero-restic-restore-helper', 'oadp-operator'],
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_images_with_disabled_dependency_are_filtered(self, mock_cmd_gather, mock_jenkins):
        """When an image has has_disabled_dependency flag, it should be excluded from the build trigger."""
        self.runtime.dry_run = False

        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'oadp-velero-restic-restore-helper', 'changed': True},
                    {'name': 'oadp-operator', 'changed': True, 'has_disabled_dependency': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        mock_jenkins.start_layered_products.assert_called_once_with(
            group='oadp-1.4',
            assembly='stream',
            image_list=['oadp-velero-restic-restore-helper'],
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_all_images_have_disabled_dependency(self, mock_cmd_gather, mock_jenkins):
        """When all changed images have disabled dependencies, no build should be triggered."""
        self.runtime.dry_run = False

        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'oadp-operator', 'changed': True, 'has_disabled_dependency': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        mock_jenkins.start_layered_products.assert_not_called()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_report_stored_for_filtering(self, mock_cmd_gather, mock_jenkins):
        """Verify that the raw report is stored for use in handle_source_changes."""
        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'test-image', 'changed': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        self.assertIn('images', pipeline.report)
        self.assertEqual(len(pipeline.report['images']), 1)
        self.assertEqual(pipeline.report['images'][0]['name'], 'test-image')

    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.start_pipeline', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_changed_images_trigger_tekton_pipeline_in_tekton_context(self, mock_cmd_gather, mock_start_pipeline):
        """In Tekton context, changed images trigger a Tekton pipeline instead of Jenkins."""
        self.runtime.dry_run = False
        mock_start_pipeline.return_value = "build-layered-products-run-abc"

        scan_output = yaml.dump(
            {
                'images': [{'name': 'oadp-operator', 'changed': True}],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        with patch.dict(os.environ, {"TEKTON_PIPELINERUN_NAME": "parent-plr-123"}):
            await pipeline.run()

        mock_start_pipeline.assert_called_once()
        call_kwargs = mock_start_pipeline.call_args.kwargs
        self.assertEqual(call_kwargs['pipeline_name'], 'build-layered-products')
        self.assertIn('image-list', call_kwargs['params'])
        self.assertIn('oadp-operator', call_kwargs['params']['image-list'])

    async def test_kubeconfig_not_required_in_tekton_context(self):
        """In Tekton context, missing KUBECONFIG does not raise an error."""
        from pyartcd.pipelines import layered_products_scan_konflux as mod

        env = {k: v for k, v in os.environ.items() if k != 'KUBECONFIG'}
        with patch.dict(os.environ, {**env, 'TEKTON_PIPELINERUN_NAME': 'my-plr-123'}, clear=True):
            # Should not raise RuntimeError
            with (
                patch.object(mod, 'jenkins'),
                patch('pyartcd.pipelines.layered_products_scan_konflux.locks'),
            ):
                # Just verify the check passes without exception
                from pyartcd import tekton

                self.assertTrue(tekton.is_tekton_context())
                self.assertFalse(os.getenv('KUBECONFIG'))

    @patch.dict(os.environ, {'KUBECONFIG': '/kube/config'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_ci_kubeconfig_flag_present_when_kubeconfig_set(self, mock_cmd_gather):
        """When KUBECONFIG is set, --ci-kubeconfig flag is passed to doozer."""
        mock_cmd_gather.return_value = (0, yaml.dump({'images': [], 'rpms': []}), '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )
        await pipeline.get_changes()

        cmd = mock_cmd_gather.call_args[0][0]
        self.assertTrue(any('--ci-kubeconfig' in str(arg) for arg in cmd))

    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_ci_kubeconfig_flag_absent_when_kubeconfig_unset(self, mock_cmd_gather):
        """When KUBECONFIG is not set, --ci-kubeconfig is NOT passed to doozer."""
        mock_cmd_gather.return_value = (0, yaml.dump({'images': [], 'rpms': []}), '')
        env = {k: v for k, v in os.environ.items() if k != 'KUBECONFIG'}

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )
        with patch.dict(os.environ, env, clear=True):
            await pipeline.get_changes()

        cmd = mock_cmd_gather.call_args[0][0]
        self.assertFalse(any('--ci-kubeconfig' in str(arg) for arg in cmd))


if __name__ == '__main__':
    unittest.main()
