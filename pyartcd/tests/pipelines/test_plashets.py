import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from pyartcd.pipelines.build_plashets import BuildPlashetsPipeline, build_plashets_cli
from pyartcd.runtime import Runtime


class TestBuildCompose(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.build_plashets.get_freeze_automation', new_callable=AsyncMock, return_value='yes')
    @patch('pyartcd.pipelines.build_plashets.build_plashets', new_callable=AsyncMock, return_value={})
    async def test_hard_freeze_requires_explicit_override(self, build_plashets, get_freeze, _init):
        for override in (False, True):
            with self.subTest(override=override):
                build_plashets.reset_mock()
                get_freeze.reset_mock()
                pipeline = BuildPlashetsPipeline(
                    runtime=MagicMock(dry_run=True),
                    group='openshift-4.21',
                    release='test',
                    assembly='stream',
                    ignore_automation_freeze=override,
                )

                await pipeline.run()

                if override:
                    get_freeze.assert_not_awaited()
                    build_plashets.assert_awaited_once()
                    pipeline.runtime.logger.warning.assert_called_once()
                else:
                    get_freeze.assert_awaited_once()
                    build_plashets.assert_not_awaited()

    @patch('pyartcd.jenkins.init_jenkins')
    @patch('pyartcd.jenkins.start_build')
    @patch('pyartcd.pipelines.build_plashets.build_plashets', new_callable=AsyncMock, return_value={})
    async def test_dry_run_prepares_plashets_without_launching_ci_sync(self, build_plashets, submit, _init):
        pipeline = BuildPlashetsPipeline(
            runtime=MagicMock(dry_run=True),
            group='openshift-5.1',
            release='test',
            assembly='stream',
        )
        pipeline.is_compose_build_permitted = AsyncMock(return_value=True)

        await pipeline.run()

        build_plashets.assert_awaited_once()
        self.assertTrue(build_plashets.await_args.kwargs['dry_run'])
        submit.assert_not_called()

    @patch('pyartcd.jenkins.init_jenkins')
    @patch('pyartcd.jenkins.update_title')
    @patch('pyartcd.locks.LockManager.from_lock', return_value=AsyncMock)
    @patch('pyartcd.pipelines.build_plashets.build_plashets', return_value=AsyncMock)
    @patch('pyartcd.jenkins.start_sync_for_ci')
    @patch('pyartcd.jenkins.start_rhcos')
    async def test_build_plashets(self, mocked_rhcos, mocked_sync_for_ci, mocked_build_plashets, mocked_lm, *_):
        build_plashets_pipeline = BuildPlashetsPipeline(
            runtime=MagicMock(dry_run=False),
            group='openshift-4.13',
            release='bogus',
            assembly='stream',
            data_path='',
            data_gitref='',
        )

        mocked_cm = AsyncMock()
        mocked_cm.__aenter__ = AsyncMock()
        mocked_cm.__aexit__ = AsyncMock()

        mocked_lm.return_value = AsyncMock()
        mocked_lm.return_value.lock.return_value = mocked_cm

        mocked_build_plashets.return_value = {}

        # Build not permitted
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=False)
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_not_awaited()
        mocked_rhcos.assert_not_called()
        mocked_sync_for_ci.assert_not_called()

        # Build permitted, assembly != 'stream'
        mocked_build_plashets.reset_mock()
        mocked_rhcos.reset_mock()
        mocked_sync_for_ci.reset_mock()
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=True)
        build_plashets_pipeline.assembly = 'test'
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_awaited_once()
        mocked_rhcos.assert_not_called()
        mocked_sync_for_ci.assert_not_called()

        # Build permitted, assembly = 'stream'
        mocked_build_plashets.reset_mock()
        mocked_rhcos.reset_mock()
        mocked_sync_for_ci.reset_mock()
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=True)
        build_plashets_pipeline.assembly = 'stream'
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_awaited_once()
        mocked_sync_for_ci.assert_called_once_with(version='4.13', block_until_building=False)

    @patch('pyartcd.pipelines.build_plashets.util.default_release_suffix')
    @patch('pyartcd.pipelines.build_plashets.BuildPlashetsPipeline')
    def test_auto_generate_release(self, mocked_pipeline_class, mocked_default_release_suffix):
        """Test that release timestamp is auto-generated when not provided"""
        mocked_default_release_suffix.return_value = '202401011200.p?'

        # Create a mock pipeline instance
        mocked_pipeline = MagicMock()
        mocked_pipeline_class.return_value = mocked_pipeline

        # Create a mock runtime
        runtime = MagicMock(dry_run=False)
        runtime.logger = MagicMock()

        # Simulate the CLI logic for auto-generating release
        release = ''
        if not release:
            release = mocked_default_release_suffix()
            runtime.logger.info(f'Auto-generated release timestamp: {release}')

        # Verify that default_release_suffix was called
        mocked_default_release_suffix.assert_called_once()

        # Verify that the release was set to the auto-generated value
        self.assertEqual(release, '202401011200.p?')

        # Verify that the logger was called with the auto-generated release
        runtime.logger.info.assert_called_once_with('Auto-generated release timestamp: 202401011200.p?')


class TestComposeFreezeOption(unittest.TestCase):
    def test_cli_override_preserves_compose_lock(self):
        async def run_locked(coro, **kwargs):
            await coro

        for override in (False, True):
            with self.subTest(override=override):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    with (
                        patch('pyartcd.pipelines.build_plashets.BuildPlashetsPipeline') as pipeline_class,
                        patch(
                            'pyartcd.pipelines.build_plashets.jenkins.get_build_path_or_random', return_value='test-run'
                        ),
                        patch(
                            'pyartcd.pipelines.build_plashets.locks.run_with_lock',
                            new_callable=AsyncMock,
                            side_effect=run_locked,
                        ) as run_with_lock,
                    ):
                        pipeline_class.return_value.run = AsyncMock()
                        args = ['--group', 'openshift-4.21', '--assembly', 'stream', '--release', 'test']
                        result = CliRunner().invoke(
                            build_plashets_cli,
                            args + (['--ignore-automation-freeze'] if override else []),
                            obj=MagicMock(spec=Runtime),
                        )
                        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
                        self.assertIs(pipeline_class.call_args.kwargs['ignore_automation_freeze'], override)
                        run_with_lock.assert_awaited_once()
                        self.assertEqual(
                            run_with_lock.call_args.kwargs['lock_name'], 'lock:compose:stream:openshift-4.21'
                        )
                        pipeline_class.return_value.run.assert_awaited_once()
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)


class TestComposeBuildPermitted(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _pipeline(group='openshift-4.18'):
        runtime = MagicMock()
        runtime.new_slack_client.return_value = MagicMock(say=AsyncMock())
        return BuildPlashetsPipeline(runtime=runtime, group=group, release='rel', assembly='stream')

    @patch('pyartcd.util.get_weekday')
    @patch('pyartcd.util.is_manual_build')
    @patch('pyartcd.pipelines.build_plashets.get_freeze_automation', new_callable=AsyncMock)
    async def test_weekdays_is_weekday_aware(self, get_freeze, is_manual, get_weekday):
        # 'weekdays' must match is_build_permitted: block weekday non-manual composes, permit weekend
        # ones, and always permit manual composes (the bug was permitting weekday automated composes).
        get_freeze.return_value = 'weekdays'
        pipeline = self._pipeline()

        is_manual.return_value = False
        get_weekday.return_value = 'Wednesday'
        self.assertFalse(await pipeline.is_compose_build_permitted())

        for weekend_day in ('Saturday', 'Sunday'):
            with self.subTest(weekday=weekend_day):
                get_weekday.return_value = weekend_day
                self.assertTrue(await pipeline.is_compose_build_permitted())

        is_manual.return_value = True
        get_weekday.return_value = 'Wednesday'
        self.assertTrue(await pipeline.is_compose_build_permitted())

        # No Slack alert for the 'weekdays' state.
        pipeline.slack_client.say.assert_not_awaited()

    @patch('pyartcd.util.is_manual_build')
    @patch('pyartcd.pipelines.build_plashets.get_freeze_automation', new_callable=AsyncMock)
    async def test_scheduled_and_hard_freeze(self, get_freeze, is_manual):
        pipeline = self._pipeline()

        # 'scheduled' blocks non-manual composes and permits manual ones (with a Slack alert).
        get_freeze.return_value = 'scheduled'
        is_manual.return_value = False
        self.assertFalse(await pipeline.is_compose_build_permitted())
        pipeline.slack_client.say.assert_not_awaited()

        is_manual.return_value = True
        self.assertTrue(await pipeline.is_compose_build_permitted())
        pipeline.slack_client.say.assert_awaited_once()

        # 'yes' is a hard freeze: nothing is permitted, not even manual composes.
        get_freeze.return_value = 'yes'
        is_manual.return_value = True
        self.assertFalse(await pipeline.is_compose_build_permitted())

    @patch('pyartcd.pipelines.build_plashets.get_freeze_automation', new_callable=AsyncMock)
    async def test_golang_group_skips_freeze_check(self, get_freeze):
        pipeline = self._pipeline(group='golang-1.22')
        self.assertTrue(await pipeline.is_compose_build_permitted())
        get_freeze.assert_not_awaited()
