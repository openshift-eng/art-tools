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
            skip_rpms=False,
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.exectools.cmd_gather_async')
    async def test_get_changes_passes_cutoff_override_or_uses_doozer_default(self, mock_cmd_gather):
        mock_cmd_gather.return_value = (0, yaml.safe_dump({'images': [], 'rpms': []}), '')
        for cutoff in [None, '2026-11-16', '2026-11-16T09:00:00-05:00']:
            with self.subTest(cutoff=cutoff):
                pipeline = self._make_pipeline()
                pipeline.rebuild_before = cutoff
                await pipeline.get_changes()
                cmd = mock_cmd_gather.call_args.args[0]
                cutoff_args = [arg for arg in cmd if arg.startswith('--rebuild-before=')]
                self.assertEqual(cutoff_args, [] if cutoff is None else [f'--rebuild-before={cutoff}'])

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
        pipeline.handle_bridge_bug_mirroring = AsyncMock()
        pipeline.handle_source_changes = Mock()
        pipeline.handle_rhcos_changes = AsyncMock()
        pipeline._handle_scan_issues = AsyncMock()

        # Reconciliation issues should NOT fail the job
        await pipeline.run()

        pipeline.handle_source_changes.assert_called_once()
        pipeline.handle_rhcos_changes.assert_awaited_once()
        pipeline._handle_scan_issues.assert_awaited_once()
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
        pipeline.handle_bridge_bug_mirroring = AsyncMock()
        pipeline.handle_source_changes = Mock()
        pipeline.handle_rhcos_changes = AsyncMock()
        pipeline._handle_scan_issues = AsyncMock()

        # Reconciliation issues alone should NOT fail the job
        await pipeline.run()

        pipeline.handle_source_changes.assert_called_once()
        pipeline.handle_rhcos_changes.assert_awaited_once()
        pipeline._handle_scan_issues.assert_awaited_once()
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
        pipeline.handle_bridge_bug_mirroring = AsyncMock()
        pipeline.handle_source_changes = Mock()
        pipeline.handle_rhcos_changes = AsyncMock()
        pipeline._handle_scan_issues = AsyncMock()

        with self.assertRaises(RuntimeError) as ctx:
            await pipeline.run()

        self.assertEqual(str(ctx.exception), 'scan-sources command failed with exit code 1')
        pipeline.handle_source_changes.assert_called_once()
        pipeline.handle_rhcos_changes.assert_awaited_once()
        pipeline._handle_scan_issues.assert_awaited_once()
        mock_jenkins.update_description.assert_called_with('Scan failures: test-image<br/>')

    async def test_handle_scan_issues_posts_slack_warning(self):
        pipeline = self._make_pipeline()
        pipeline.issues = [
            {'name': 'image-a', 'issue': 'Could not rebase into -priv as it needs manual reconciliation'},
            {'name': 'image-b', 'issue': 'git push to openshift-priv failed'},
        ]

        slack_client = AsyncMock()
        slack_client.bind_channel = MagicMock()
        slack_client.say = AsyncMock(return_value={'message': {'ts': '1234567890.123456'}})
        self.runtime.new_slack_client.return_value = slack_client

        await pipeline._handle_scan_issues()

        slack_client.bind_channel.assert_called_once_with('4.21')
        # First call: the warning header
        first_call = slack_client.say.call_args_list[0]
        self.assertIn(':warning:', first_call.args[0])
        self.assertIn('4.21', first_call.args[0])
        # Subsequent calls: one per issue, threaded
        self.assertEqual(slack_client.say.call_count, 3)
        second_call = slack_client.say.call_args_list[1]
        self.assertIn('image-a', second_call.args[0])
        self.assertEqual(second_call.args[1], '1234567890.123456')
        third_call = slack_client.say.call_args_list[2]
        self.assertIn('image-b', third_call.args[0])
        self.assertEqual(third_call.args[1], '1234567890.123456')

    async def test_handle_scan_issues_noop_when_no_issues(self):
        pipeline = self._make_pipeline()
        pipeline.issues = []
        self.runtime.new_slack_client.return_value = AsyncMock()

        await pipeline._handle_scan_issues()

        # Should not create a Slack client at all
        self.runtime.new_slack_client.assert_not_called()

    async def test_handle_scan_issues_logs_slack_failure_without_crashing(self):
        pipeline = self._make_pipeline()
        pipeline.issues = [
            {'name': 'image-a', 'issue': 'Could not rebase into -priv as it needs manual reconciliation'},
        ]

        slack_client = AsyncMock()
        slack_client.bind_channel = MagicMock()
        slack_client.say = AsyncMock(side_effect=RuntimeError('Slack API error'))
        self.runtime.new_slack_client.return_value = slack_client

        # Should NOT raise — Slack failures are caught and logged
        await pipeline._handle_scan_issues()

        slack_client.bind_channel.assert_called_once_with('4.21')
        slack_client.say.assert_awaited_once()


class TestBridgeBugMirroring(unittest.IsolatedAsyncioTestCase):
    """Cover bridge bug mirroring orchestration in the Konflux scan pipeline."""

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
            skip_rpms=False,
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

        await self.pipeline.handle_bridge_bug_mirroring()

        slack_client.bind_channel.assert_called_once_with("openshift-4.23")
        slack_client.say.assert_awaited_once_with("Bridge bug mirroring failed for 4.23. Please investigate")


class TestTektonScanner(unittest.IsolatedAsyncioTestCase):
    @patch.dict(os.environ, {'TASKRUN_NAME': 'task', 'TEKTON_PIPELINERUN_NAME': 'scan'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.tekton.annotate_current_pipelinerun')
    @patch('pyartcd.pipelines.ocp4_scan_konflux.jenkins')
    @patch('pyartcd.jenkins.update_title')
    async def test_source_changes_still_trigger_jenkins_from_tekton(self, update_title, jenkins, annotate):
        runtime = MagicMock(spec=Runtime)
        runtime.dry_run = False
        runtime.working_dir = Path('/tmp')
        pipeline = Ocp4ScanPipeline(runtime, '5.1', '', 'stream', '', '', False)
        pipeline.changes = {'images': ['operator'], 'rpms': ['rpm']}
        pipeline.report = {'images': [{'name': 'operator', 'changed': True}]}
        pipeline.handle_source_changes()
        update_title.assert_not_called()
        jenkins.update_description.assert_not_called()
        jenkins.start_ocp4_konflux.assert_called_once_with(
            build_version='5.1', assembly='stream', image_list=['operator'], rpm_list=['rpm']
        )
        annotate.assert_any_call({'art.openshift.io/scan-status': '[SOURCE CHANGES]'})
        annotate.assert_called_with({'art.openshift.io/scan-summary': 'Changed 1 RPMs; Changed 1 OCP images'})

    @patch.dict(os.environ, {'KUBECONFIG': '/wrong-cluster'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_explicit_ci_credentials_do_not_change_local_cluster_context(self, cmd):
        runtime = MagicMock(spec=Runtime)
        runtime.dry_run = True
        runtime.working_dir = Path('/tmp')
        pipeline = Ocp4ScanPipeline(runtime, '5.1', '', 'stream', '', '', False, ci_kubeconfig='/ci-creds')
        cmd.return_value = (0, '{}', '')
        await pipeline.get_changes()
        self.assertIn('--ci-kubeconfig=/ci-creds', cmd.call_args.args[0])
        self.assertIn('--dry-run', cmd.call_args.args[0])
        self.assertEqual(os.environ['KUBECONFIG'], '/wrong-cluster')

    @patch.dict(os.environ, {'TASKRUN_NAME': 'task', 'TEKTON_PIPELINERUN_NAME': 'scan'})
    @patch('pyartcd.pipelines.ocp4_scan_konflux.tekton.annotate_current_pipelinerun')
    @patch('pyartcd.pipelines.ocp4_scan_konflux.jenkins')
    async def test_dry_run_reports_changes_without_triggering_jenkins(self, jenkins, annotate):
        runtime = MagicMock(spec=Runtime)
        runtime.dry_run = True
        runtime.working_dir = Path('/tmp')
        pipeline = Ocp4ScanPipeline(runtime, '4.22', '', 'stream', '', '', False)
        pipeline.changes = {'images': ['operator'], 'rpms': ['rpm']}
        pipeline.report = {'images': [{'name': 'operator', 'changed': True}]}
        pipeline.handle_source_changes()
        pipeline.rhcos_outdated = True
        await pipeline.handle_rhcos_changes()
        pipeline.rhcos_outdated = False
        pipeline.rhcos_updated = True
        await pipeline.handle_rhcos_changes()
        pipeline.issues = [{'name': 'operator', 'issue': 'Scan failed'}]
        pipeline._report_issues()
        jenkins.start_ocp4_konflux.assert_not_called()
        jenkins.start_rhcos.assert_not_called()
        jenkins.start_build_sync.assert_not_called()
        annotate.assert_called_with(
            {
                'art.openshift.io/scan-summary': (
                    'Changed 1 RPMs; Changed 1 OCP images; RHCOS outdated; Scan failures: operator'
                )
            }
        )


class TestTektonScannerLocks(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import inspect

        from pyartcd.pipelines.ocp4_scan_konflux import ocp4_scan

        self.scan = inspect.unwrap(ocp4_scan.callback)
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = False
        self.runtime.working_dir = Path('/tmp')
        self.pipeline = MagicMock()
        self.pipeline.skipped = True
        self.scan_key = 'lock:scan-konflux:4.22'
        self.build_key = 'lock:build-konflux:4.22:stream'
        self.held = set()
        self.events = []
        self.managers = []
        self.failure = None

        async def scan():
            self.assertEqual(self.held, {self.scan_key, self.build_key})
            self.events.append(('scan',))
            self.pipeline.skipped = False
            if self.failure:
                raise self.failure

        self.pipeline.run = AsyncMock(side_effect=scan)
        self.env = patch.dict(
            os.environ,
            {
                'TASKRUN_NAME': 'scan-task',
                'TEKTON_PIPELINERUN_NAME': 'scan-123',
                'TASKRUN_NAMESPACE': 'art-openshift-tenant',
                # Lock ownership must not depend on a UI link's format.
                'BUILD_URL': 'https://pipelines.example/#/run/art-openshift-tenant/scan-123',
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)

    def make_lock_manager(self, _lock):
        from contextlib import asynccontextmanager

        manager = MagicMock()
        manager.is_locked = AsyncMock(side_effect=lambda name: name in self.held)
        manager.get_lock_id = AsyncMock(return_value='job/another-build/123')
        manager.destroy = AsyncMock()

        @asynccontextmanager
        async def acquired(resource, lock_identifier):
            self.held.add(resource)
            self.events.append(('acquire', resource, lock_identifier))
            try:
                yield
            finally:
                self.held.remove(resource)
                self.events.append(('release', resource))

        async def lock(**kwargs):
            return acquired(**kwargs)

        manager.lock = AsyncMock(side_effect=lock)
        self.managers.append(manager)
        return manager

    async def invoke(self):
        with (
            patch('pyartcd.pipelines.ocp4_scan_konflux.Ocp4ScanPipeline', return_value=self.pipeline),
            patch('pyartcd.pipelines.ocp4_scan_konflux.jenkins.init_jenkins') as init_jenkins,
            patch('pyartcd.tekton.annotate_current_pipelinerun') as annotate,
            patch('pyartcd.locks.LockManager.from_lock', side_effect=self.make_lock_manager),
        ):
            try:
                await self.scan(
                    runtime=self.runtime,
                    version='4.22',
                    assembly='stream',
                    data_path='https://github.com/openshift-eng/ocp-build-data',
                    data_gitref='',
                    image_list='',
                    skip_rpms=False,
                    ci_kubeconfig='/ci-creds',
                    rebuild_before=None,
                )
            finally:
                init_jenkins.assert_called_once()
                for manager in self.managers:
                    manager.destroy.assert_awaited_once()
        return annotate

    async def test_live_scan_acquires_both_locks_with_tekton_owner_then_release(self):
        from pyartcd.tekton_locks import parse_k8s_lock_path

        await self.invoke()
        self.pipeline.run.assert_awaited_once()
        self.assertEqual([event[1] for event in self.events if event[0] == 'acquire'], [self.scan_key, self.build_key])
        for event in self.events:
            if event[0] == 'acquire':
                self.assertEqual(parse_k8s_lock_path(event[2]), ('art-openshift-tenant', 'scan-123', 'PipelineRun'))
        self.assertEqual(self.events[-2:], [('release', self.build_key), ('release', self.scan_key)])
        self.assertFalse(self.held)

    async def test_live_scan_skips_when_either_jenkins_lock_is_held(self):
        for key in (self.scan_key, self.build_key):
            with self.subTest(key=key):
                self.held = {key}
                self.events.clear()
                self.managers.clear()
                self.pipeline.run.reset_mock()
                annotate = await self.invoke()
                self.pipeline.run.assert_not_awaited()
                self.assertEqual(self.held, {key})
                annotate.assert_called_once_with({'art.openshift.io/scan-status': '[SKIPPED][LOCKED]'})

    async def test_existing_scan_lock_skips_scanning(self):
        self.held.add(self.scan_key)
        annotate = await self.invoke()
        self.pipeline.run.assert_not_awaited()
        self.assertEqual(self.events, [])
        self.assertEqual(self.held, {self.scan_key})
        annotate.assert_called_once_with({'art.openshift.io/scan-status': '[SKIPPED][LOCKED]'})

    async def test_existing_build_lock_skips_scanning_and_releases_scan_lock(self):
        self.held.add(self.build_key)
        annotate = await self.invoke()
        self.pipeline.run.assert_not_awaited()
        self.assertEqual(self.events[-1], ('release', self.scan_key))
        self.assertEqual(self.held, {self.build_key})
        annotate.assert_called_once_with({'art.openshift.io/scan-status': '[SKIPPED][LOCKED]'})

    async def test_scan_failure_releases_both_locks(self):
        self.failure = RuntimeError('scan failed')
        with self.assertRaisesRegex(RuntimeError, 'scan failed'):
            await self.invoke()
        self.assertFalse(self.held)
        self.assertEqual(self.events[-2:], [('release', self.build_key), ('release', self.scan_key)])

    async def test_dry_runs_skip_locks_on_both_platforms(self):
        self.runtime.dry_run = True
        self.held = {self.scan_key, self.build_key}

        async def scan():
            self.pipeline.skipped = False

        self.pipeline.run = AsyncMock(side_effect=scan)
        for is_tekton in (False, True):
            with (
                self.subTest(is_tekton=is_tekton),
                patch('pyartcd.pipelines.ocp4_scan_konflux.tekton.is_tekton_context', return_value=is_tekton),
                patch('pyartcd.pipelines.ocp4_scan_konflux.Ocp4ScanPipeline', return_value=self.pipeline),
                patch('pyartcd.pipelines.ocp4_scan_konflux.jenkins') as jenkins,
                patch('pyartcd.util.update_run_title') as title,
                patch('pyartcd.locks.LockManager.from_lock') as locks,
            ):
                self.pipeline.skipped = True
                self.pipeline.run.reset_mock()
                await self.scan(
                    runtime=self.runtime,
                    version='4.22',
                    assembly='stream',
                    data_path='https://github.com/openshift-eng/ocp-build-data',
                    data_gitref='',
                    image_list='',
                    skip_rpms=False,
                    ci_kubeconfig='/ci-creds',
                    rebuild_before=None,
                )
                locks.assert_not_called()
                self.pipeline.run.assert_awaited_once()
                title.assert_not_called()
                self.assertEqual(self.held, {self.scan_key, self.build_key})
                if is_tekton:
                    jenkins.init_jenkins.assert_not_called()
                else:
                    jenkins.init_jenkins.assert_called_once()


if __name__ == '__main__':
    unittest.main()
