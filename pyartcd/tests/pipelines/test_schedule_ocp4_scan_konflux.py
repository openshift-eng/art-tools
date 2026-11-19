import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

from click.testing import CliRunner
from pyartcd.pipelines.scheduled import schedule_ocp4_scan_konflux as scheduler


class TestOcpScanScheduler(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(working_dir=Path('/tmp'), dry_run=False)
        self.lock_manager = MagicMock(is_locked=AsyncMock(return_value=False))
        self.namespace = 'art-openshift-tenant'

    @patch.object(scheduler.tekton, 'start_pipeline_run')
    @patch.object(scheduler.util, 'is_build_permitted', return_value=True)
    @patch.dict('os.environ', {'K8S_JOB_NAME': 'ocp4-scan-konflux-hp-1234'})
    async def test_starts_existing_pipeline_with_jenkins_scan_inputs(self, is_permitted, start_run):
        await scheduler.run_for('4.22', self.runtime, self.lock_manager, tekton_namespace=self.namespace)

        self.lock_manager.is_locked.assert_has_awaits(
            [call('lock:scan-konflux:4.22'), call('lock:build-konflux:4.22:stream')]
        )
        is_permitted.assert_awaited_once()
        start_run.assert_called_once_with(
            'ocp4-scan-konflux',
            {'version': '4.22'},
            namespace=self.namespace,
            run_name='ocp4-scan-konflux-hp-1234-4-22',
        )

    @patch.object(scheduler.tekton, 'start_pipeline_run')
    @patch.object(scheduler.util, 'is_build_permitted', return_value=True)
    @patch.dict('os.environ', {'K8S_JOB_NAME': 'ocp4-scan-konflux-hp-' + 'a' * 43})
    async def test_long_job_names_produce_bounded_deterministic_run_names(self, _is_permitted, start_run):
        for version in ('4.22', '4.22', '4.21'):
            await scheduler.run_for(version, self.runtime, self.lock_manager, tekton_namespace=self.namespace)

        first, repeated, other_version = [invocation.kwargs['run_name'] for invocation in start_run.call_args_list]
        self.assertEqual(len(first), 63)
        self.assertEqual(first, repeated)
        self.assertNotEqual(first, other_version)

    @patch.object(scheduler.tekton, 'start_pipeline_run')
    @patch.object(scheduler.util, 'is_build_permitted', return_value=False)
    async def test_checks_locks_and_freeze_before_tekton_dispatch(self, is_permitted, start_run):
        for lock_states in ([True], [False, True], [False, False]):
            with self.subTest(lock_states=lock_states):
                self.lock_manager.is_locked.side_effect = lock_states
                is_permitted.reset_mock()
                await scheduler.run_for('4.22', self.runtime, self.lock_manager, tekton_namespace=self.namespace)
                self.assertEqual(is_permitted.await_count, int(not any(lock_states)))
                start_run.assert_not_called()

    @patch.object(scheduler.tekton, 'start_pipeline_run')
    @patch.object(scheduler.util, 'is_build_permitted', return_value=True)
    async def test_tekton_dry_run_does_not_submit_a_scan(self, _is_permitted, start_run):
        self.runtime.dry_run = True
        await scheduler.run_for('4.22', self.runtime, self.lock_manager, tekton_namespace=self.namespace)
        start_run.assert_not_called()

    @patch.object(scheduler.jenkins, 'start_ocp4_scan_konflux')
    @patch.object(scheduler.util, 'is_build_permitted', return_value=True)
    async def test_jenkins_serial_waits_for_the_scan(self, _is_permitted, start_scan):
        await scheduler.run_for('4.23', self.runtime, self.lock_manager, serial=True)
        start_scan.assert_called_once_with(version='4.23', block_until_building=True, block_until_complete=True)


class TestOcpScanSchedulerCli(TestCase):
    def setUp(self):
        self.runtime = MagicMock(spec=scheduler.Runtime, dry_run=False, logger=MagicMock())
        self.slack = MagicMock(say=AsyncMock())
        self.runtime.new_slack_client.return_value = self.slack

    def invoke(self, *args):
        with asyncio.Runner() as runner:
            asyncio.set_event_loop(runner.get_loop())
            return CliRunner().invoke(scheduler.ocp4_scan_konflux, args, obj=self.runtime)

    @patch.object(scheduler, 'LockManager')
    @patch.object(scheduler.redis, 'redis_url')
    @patch.object(scheduler.jenkins, 'init_jenkins')
    def test_rejects_an_empty_tekton_namespace(self, init_jenkins, redis_url, lock_manager):
        self.runtime.dry_run = True
        result = self.invoke('--version=4.22', '--tekton-namespace=')
        self.assertEqual(result.exit_code, 2)
        self.assertIn('--tekton-namespace must be a non-empty namespace name', result.output)
        init_jenkins.assert_not_called()
        redis_url.assert_not_called()
        lock_manager.assert_not_called()

    @patch.object(scheduler, 'LockManager')
    @patch.object(scheduler.redis, 'redis_url')
    def test_alerts_on_redis_setup_failure(self, redis_url, lock_manager):
        error = scheduler.redis.RedisError('missing Redis configuration')
        redis_url.side_effect = error

        result = self.invoke('--version=4.22', '--tekton-namespace=art-openshift-tenant')

        self.assertIs(result.exception, error)
        self.slack.bind_channel.assert_called_once_with('#art-release')
        self.slack.say.assert_awaited_once()
        lock_manager.assert_not_called()

    @patch.object(scheduler, 'run_for', new_callable=AsyncMock)
    @patch.object(scheduler, 'LockManager')
    @patch.object(scheduler.redis, 'redis_url', return_value='redis://mock')
    def test_tekton_dispatch_deduplicates_versions(self, _redis_url, lock_manager_class, run_for):
        lock_manager_class.return_value.destroy = AsyncMock()
        result = self.invoke(
            '--version=4.22', '--version=4.21', '--version=4.22', '--tekton-namespace=art-openshift-tenant'
        )
        self.assertEqual(result.exit_code, 0, result.exception)
        self.assertEqual([invocation.args[0] for invocation in run_for.await_args_list], ['4.22', '4.21'])

    @patch.object(scheduler, 'run_for', new_callable=AsyncMock)
    @patch.object(scheduler, 'LockManager')
    @patch.object(scheduler.redis, 'redis_url', return_value='redis://mock')
    def test_partial_dispatch_failure_waits_for_remaining_versions_and_reports_all_errors(
        self, _redis_url, lock_manager_class, run_for
    ):
        events = []
        failed_dispatch = asyncio.Event()
        dispatch_error = RuntimeError('failed to dispatch 4.21')
        other_dispatch_error = ValueError('failed to dispatch 4.19')

        async def dispatch(version, *_args, **_kwargs):
            if version == '4.21':
                asyncio.current_task().add_done_callback(lambda _: failed_dispatch.set())
                raise dispatch_error
            if version == '4.19':
                raise other_dispatch_error
            if version == '4.20':
                await failed_dispatch.wait()
            events.append(version)

        def log_error(_message, version, *, exc_info):
            events.append((version, exc_info))

        async def alert(_message):
            events.append('alert')
            raise OSError('Slack unavailable')

        async def destroy():
            events.append('destroy')

        run_for.side_effect = dispatch
        self.runtime.logger.error.side_effect = log_error
        self.slack.say.side_effect = alert
        lock_manager_class.return_value.destroy = AsyncMock(side_effect=destroy)

        result = self.invoke(
            '--version=4.22',
            '--version=4.21',
            '--version=4.20',
            '--version=4.19',
            '--tekton-namespace=art-openshift-tenant',
        )

        self.assertIs(result.exception, dispatch_error)
        self.assertEqual(
            events, ['4.22', '4.20', ('4.21', dispatch_error), ('4.19', other_dispatch_error), 'alert', 'destroy']
        )
        self.slack.bind_channel.assert_called_once_with('#art-release')
        self.slack.say.assert_awaited_once()
        lock_manager_class.return_value.destroy.assert_awaited_once()
        self.runtime.logger.exception.assert_called_once()
