#!/usr/bin/env python3

"""
Unit tests for okd_scan pipeline.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

import yaml
from pyartcd.pipelines.okd_scan import OkdScanPipeline
from pyartcd.runtime import Runtime


class TestOkdScanPipeline(unittest.IsolatedAsyncioTestCase):
    """
    Test OKD scan pipeline functionality.
    """

    def setUp(self):
        """
        Set up test fixtures.
        """
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.okd_scan.jenkins')
    @patch('pyartcd.pipelines.okd_scan.exectools.cmd_gather_async')
    async def test_no_changes_detected(self, mock_cmd_gather, mock_jenkins):
        """
        Test pipeline when no changes are detected.
        """
        # Mock scan-sources output with no changed images
        scan_output = yaml.dump({'images': []})
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = OkdScanPipeline(
            runtime=self.runtime,
            version='4.21',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        # Should not trigger any builds
        mock_jenkins.start_okd.assert_not_called()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.okd_scan.constants.OKD_ENABLED_VERSIONS', ['4.21', '4.22'])
    @patch('pyartcd.pipelines.okd_scan.jenkins')
    @patch('pyartcd.pipelines.okd_scan.exectools.cmd_gather_async')
    async def test_valid_rebuild_reason_triggers_build(self, mock_cmd_gather, mock_jenkins):
        """
        Test that images with valid rebuild reasons trigger OKD builds.
        """
        # Set dry_run to False so jenkins.start_okd is actually called
        self.runtime.dry_run = False

        # Mock scan-sources output with changed images
        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'test-image-1', 'changed': True, 'code': 'NEW_UPSTREAM_COMMIT'},
                    {'name': 'test-image-2', 'changed': True, 'code': 'ANCESTOR_CHANGING'},
                ]
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = OkdScanPipeline(
            runtime=self.runtime,
            version='4.21',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        # Should trigger OKD build with both images
        mock_jenkins.start_okd.assert_called_once_with(
            build_version='4.21', assembly='stream', image_list=['test-image-1', 'test-image-2']
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.okd_scan.constants.OKD_ENABLED_VERSIONS', ['4.21', '4.22'])
    @patch('pyartcd.pipelines.okd_scan.jenkins')
    @patch('pyartcd.pipelines.okd_scan.exectools.cmd_gather_async')
    async def test_all_changed_images_trigger_build(self, mock_cmd_gather, mock_jenkins):
        """
        Test that all changed images from scan-sources trigger builds.
        Since we use --variant=okd, doozer only reports valid OKD rebuild reasons.
        """
        # Set dry_run to False so jenkins.start_okd is actually called
        self.runtime.dry_run = False

        # Mock scan-sources output with changed images
        # With --variant=okd, doozer won't report ARCHES_CHANGE or NETWORK_MODE_CHANGE
        scan_output = yaml.dump({'images': [{'name': 'test-image-1', 'changed': True}]})
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = OkdScanPipeline(
            runtime=self.runtime,
            version='4.21',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        # Should trigger build with the changed image
        mock_jenkins.start_okd.assert_called_once_with(
            build_version='4.21', assembly='stream', image_list=['test-image-1']
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.okd_scan.constants.OKD_ENABLED_VERSIONS', ['4.22'])
    @patch('pyartcd.pipelines.okd_scan.jenkins')
    @patch('pyartcd.pipelines.okd_scan.exectools.cmd_gather_async')
    async def test_version_not_enabled_skips_build(self, mock_cmd_gather, mock_jenkins):
        """
        Test that pipeline exits early for versions not in OKD_ENABLED_VERSIONS.
        """
        pipeline = OkdScanPipeline(
            runtime=self.runtime,
            version='4.21',  # Not in enabled versions
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        # Should exit early without scanning or triggering builds
        mock_cmd_gather.assert_not_called()
        mock_jenkins.start_okd.assert_not_called()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.okd_scan.constants.OKD_ENABLED_VERSIONS', ['4.21'])
    @patch('pyartcd.pipelines.okd_scan.jenkins')
    @patch('pyartcd.pipelines.okd_scan.exectools.cmd_gather_async')
    async def test_multiple_changed_images(self, mock_cmd_gather, mock_jenkins):
        """
        Test that multiple changed images all trigger a build.
        With --variant=okd, all reported changes are valid OKD rebuild reasons.
        """
        # Set dry_run to False so jenkins.start_okd is actually called
        self.runtime.dry_run = False

        # Mock scan-sources output with multiple changed images
        # With --variant=okd, doozer has already filtered to valid rebuild reasons
        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'image-1', 'changed': True},
                    {'name': 'image-2', 'changed': True},
                    {'name': 'image-3', 'changed': True},
                ]
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = OkdScanPipeline(
            runtime=self.runtime,
            version='4.21',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        # Should trigger build with all changed images
        mock_jenkins.start_okd.assert_called_once_with(
            build_version='4.21', assembly='stream', image_list=['image-1', 'image-2', 'image-3']
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.okd_scan.constants.OKD_ENABLED_VERSIONS', ['4.21'])
    @patch('pyartcd.pipelines.okd_scan.jenkins')
    @patch('pyartcd.pipelines.okd_scan.exectools.cmd_gather_async')
    async def test_scan_uses_variant_okd(self, mock_cmd_gather, mock_jenkins):
        """
        Test that the pipeline calls doozer scan-sources with --variant=okd.
        This ensures filtering happens in doozer, not in Python.
        """
        # Set dry_run to False so pipeline actually runs scan
        self.runtime.dry_run = False

        # Mock scan-sources output
        scan_output = yaml.dump({'images': []})
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = OkdScanPipeline(
            runtime=self.runtime,
            version='4.21',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        # Verify --variant=okd was passed to doozer
        cmd_calls = mock_cmd_gather.call_args_list
        self.assertEqual(len(cmd_calls), 1)
        cmd = cmd_calls[0][0][0]  # First positional argument of first call
        self.assertIn('--variant=okd', cmd)


if __name__ == '__main__':
    unittest.main()
