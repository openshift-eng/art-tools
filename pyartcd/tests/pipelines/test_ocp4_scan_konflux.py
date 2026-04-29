#!/usr/bin/env python3

import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import yaml
from pyartcd.pipelines.ocp4_scan_konflux import Ocp4ScanPipeline
from pyartcd.runtime import Runtime


class TestOcp4ScanKonfluxPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

    def _make_pipeline(self):
        return Ocp4ScanPipeline(
            runtime=self.runtime,
            version='4.21',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.exectools.cmd_gather_async')
    async def test_get_changes_captures_issues_and_command_failure(self, mock_cmd_gather):
        scan_output = yaml.dump(
            {
                'images': [{'name': 'test-image', 'changed': True}],
                'issues': [
                    {'name': 'test-image', 'issue': 'Failed scanning image during upstream commit checks: boom'}
                ],
            }
        )
        mock_cmd_gather.return_value = (1, scan_output, '')

        pipeline = self._make_pipeline()

        await pipeline.get_changes()

        self.assertTrue(pipeline.command_failed)
        self.assertEqual(pipeline.command_failure_message, 'scan-sources command failed with exit code 1')
        self.assertEqual(
            pipeline.issues,
            [{'name': 'test-image', 'issue': 'Failed scanning image during upstream commit checks: boom'}],
        )
        self.assertEqual(pipeline.changes, {'images': ['test-image']})

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.jenkins')
    async def test_run_marks_job_unstable_when_changes_and_issues_found(self, mock_jenkins):
        pipeline = self._make_pipeline()

        async def fake_get_changes():
            pipeline.changes = {'images': ['test-image']}
            pipeline.issues = [
                {'name': 'test-image', 'issue': 'Failed scanning image during upstream commit checks: boom'}
            ]
            pipeline.command_failed = False
            pipeline.report = {'images': [{'name': 'test-image', 'changed': True}]}

        pipeline.get_changes = AsyncMock(side_effect=fake_get_changes)
        pipeline.get_rhcos_inconsistencies = AsyncMock()
        pipeline.handle_source_changes = Mock()
        pipeline.handle_rhcos_changes = AsyncMock()

        with self.assertRaises(SystemExit) as ctx:
            await pipeline.run()

        self.assertEqual(ctx.exception.code, 2)
        pipeline.handle_source_changes.assert_called_once()
        pipeline.handle_rhcos_changes.assert_awaited_once()
        mock_jenkins.update_description.assert_called_with('Scan failures: test-image<br/>')

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.jenkins')
    async def test_run_fails_when_issues_found_without_changes(self, mock_jenkins):
        pipeline = self._make_pipeline()

        async def fake_get_changes():
            pipeline.changes = {}
            pipeline.issues = [
                {'name': 'test-image', 'issue': 'Could not rebase into -priv as it needs manual reconciliation'}
            ]
            pipeline.command_failed = False
            pipeline.report = {'images': []}

        pipeline.get_changes = AsyncMock(side_effect=fake_get_changes)
        pipeline.get_rhcos_inconsistencies = AsyncMock()
        pipeline.handle_source_changes = Mock()
        pipeline.handle_rhcos_changes = AsyncMock()

        with self.assertRaises(RuntimeError) as ctx:
            await pipeline.run()

        self.assertIn('scan-sources reported issues but found no valid changes', str(ctx.exception))
        pipeline.handle_source_changes.assert_called_once()
        pipeline.handle_rhcos_changes.assert_awaited_once()
        mock_jenkins.update_description.assert_called_with('Scan failures: test-image<br/>')

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.jenkins')
    async def test_run_fails_when_scan_command_failed(self, mock_jenkins):
        pipeline = self._make_pipeline()

        async def fake_get_changes():
            pipeline.changes = {'images': ['test-image']}
            pipeline.issues = [
                {'name': 'test-image', 'issue': 'Failed scanning image during upstream commit checks: boom'}
            ]
            pipeline.command_failed = True
            pipeline.command_failure_message = 'scan-sources command failed with exit code 1'
            pipeline.report = {'images': [{'name': 'test-image', 'changed': True}]}

        pipeline.get_changes = AsyncMock(side_effect=fake_get_changes)
        pipeline.get_rhcos_inconsistencies = AsyncMock()
        pipeline.handle_source_changes = Mock()
        pipeline.handle_rhcos_changes = AsyncMock()

        with self.assertRaises(RuntimeError) as ctx:
            await pipeline.run()

        self.assertEqual(str(ctx.exception), 'scan-sources command failed with exit code 1')
        pipeline.handle_source_changes.assert_called_once()
        pipeline.handle_rhcos_changes.assert_awaited_once()
        mock_jenkins.update_description.assert_called_with('Scan failures: test-image<br/>')


class TestBridgeBugMirroring(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = False
        self.runtime.working_dir = Path("/tmp")
        self.runtime.new_slack_client.return_value = AsyncMock()
        self.pipeline = Ocp4ScanPipeline(
            runtime=self.runtime,
            version="4.23",
            data_path="https://github.com/openshift-eng/ocp-build-data",
            assembly="stream",
            data_gitref="",
            image_list="",
        )

    async def test_run_invokes_bridge_bug_mirroring(self):
        self.pipeline.get_changes = AsyncMock()
        self.pipeline.get_rhcos_inconsistencies = AsyncMock()
        self.pipeline.handle_bridge_bug_mirroring = AsyncMock()
        self.pipeline.handle_source_changes = MagicMock()
        self.pipeline.handle_rhcos_changes = AsyncMock()

        await self.pipeline.run()

        self.pipeline.handle_bridge_bug_mirroring.assert_awaited_once()
        self.pipeline.handle_source_changes.assert_called_once()
        self.pipeline.handle_rhcos_changes.assert_awaited_once()

    @patch("pyartcd.pipelines.ocp4_scan_konflux.util.load_group_config")
    @patch("pyartcd.pipelines.ocp4_scan_konflux.exectools.cmd_assert_async")
    async def test_handle_bridge_bug_mirroring_skips_when_disabled(self, mock_cmd_assert, mock_load_group_config):
        mock_load_group_config.return_value = {}

        await self.pipeline.handle_bridge_bug_mirroring()

        mock_cmd_assert.assert_not_called()

    @patch("pyartcd.pipelines.ocp4_scan_konflux.util.load_group_config")
    @patch("pyartcd.pipelines.ocp4_scan_konflux.exectools.cmd_assert_async")
    async def test_handle_bridge_bug_mirroring_runs_elliott(self, mock_cmd_assert, mock_load_group_config):
        mock_load_group_config.return_value = {
            "bridge_release": {
                "basis_group": "openshift-5.0",
                "bug_mirroring": {"enabled": True},
            }
        }

        await self.pipeline.handle_bridge_bug_mirroring()

        cmd = mock_cmd_assert.call_args.args[0]
        self.assertIn("find-bugs:bridge-mirror", cmd)
        self.assertIn("--build-system=konflux", cmd)
        self.assertIn("--group=openshift-4.23", cmd)

    @patch("pyartcd.pipelines.ocp4_scan_konflux.util.load_group_config")
    @patch("pyartcd.pipelines.ocp4_scan_konflux.exectools.cmd_assert_async")
    async def test_handle_bridge_bug_mirroring_alerts_on_failure(self, mock_cmd_assert, mock_load_group_config):
        mock_load_group_config.return_value = {
            "bridge_release": {
                "basis_group": "openshift-5.0",
                "bug_mirroring": {"enabled": True},
            }
        }
        mock_cmd_assert.side_effect = ChildProcessError("boom")
        slack_client = AsyncMock()
        slack_client.bind_channel = MagicMock()
        self.runtime.new_slack_client.return_value = slack_client

        with self.assertRaises(ChildProcessError):
            await self.pipeline.handle_bridge_bug_mirroring()

        slack_client.bind_channel.assert_called_once_with("openshift-4.23")
        slack_client.say.assert_awaited_once_with("Bridge bug mirroring failed for 4.23. Please investigate")


if __name__ == '__main__':
    unittest.main()
