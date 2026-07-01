from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.scheduled.schedule_okd_scan import resolve_schedule_versions, run_for


class TestResolveScheduleVersions(IsolatedAsyncioTestCase):
    @patch(
        'pyartcd.pipelines.scheduled.schedule_okd_scan._is_version_okd_enabled',
        new_callable=AsyncMock,
    )
    async def test_explicit_versions_filters_disabled(self, is_enabled):
        is_enabled.side_effect = [True, False]
        runtime = MagicMock()
        runtime.logger = MagicMock()

        result = await resolve_schedule_versions(runtime, ('4.21', '4.23'))

        self.assertEqual(result, ['4.21'])
        self.assertEqual(is_enabled.await_count, 2)
        runtime.logger.info.assert_any_call(
            'Version %s is not enabled for OKD (set okd.enabled: true in group.yml on openshift-%s). Skipping.',
            '4.23',
            '4.23',
        )

    @patch('pyartcd.pipelines.scheduled.schedule_okd_scan.util.get_okd_enabled_versions', new_callable=AsyncMock)
    async def test_discovers_from_build_data(self, get_enabled):
        get_enabled.return_value = ['4.21', '4.22']
        runtime = MagicMock()
        runtime.working_dir = MagicMock()

        result = await resolve_schedule_versions(runtime, tuple())

        get_enabled.assert_awaited_once()
        self.assertEqual(result, ['4.21', '4.22'])

    @patch(
        'pyartcd.pipelines.scheduled.schedule_okd_scan.util.get_okd_enabled_versions',
        new_callable=AsyncMock,
        return_value=[],
    )
    async def test_empty_when_none_enabled(self, _get_enabled):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()

        result = await resolve_schedule_versions(runtime, tuple())

        self.assertEqual(result, [])
        runtime.logger.info.assert_any_call('No OKD-enabled versions found in build-data; nothing to schedule')


class TestRunFor(IsolatedAsyncioTestCase):
    @patch('pyartcd.pipelines.scheduled.schedule_okd_scan.jenkins.start_okd_scan_konflux')
    @patch(
        'pyartcd.pipelines.scheduled.schedule_okd_scan._is_version_okd_enabled',
        new_callable=AsyncMock,
        return_value=False,
    )
    @patch(
        'pyartcd.pipelines.scheduled.schedule_okd_scan.util.is_build_permitted',
        new_callable=AsyncMock,
        return_value=True,
    )
    async def test_skips_disabled_before_jenkins(self, _permitted, _enabled, start_scan):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        lock_manager = MagicMock()
        lock_manager.is_locked = AsyncMock(return_value=False)

        await run_for('4.23', runtime, lock_manager)

        start_scan.assert_not_called()
        runtime.logger.info.assert_any_call(
            '[%s] OKD not enabled in build-data (okd.enabled); skipping okd-scan schedule',
            '4.23',
        )

    @patch('pyartcd.pipelines.scheduled.schedule_okd_scan.jenkins.start_okd_scan_konflux')
    @patch(
        'pyartcd.pipelines.scheduled.schedule_okd_scan._is_version_okd_enabled',
        new_callable=AsyncMock,
        return_value=True,
    )
    @patch(
        'pyartcd.pipelines.scheduled.schedule_okd_scan.util.is_build_permitted',
        new_callable=AsyncMock,
        return_value=True,
    )
    async def test_schedules_when_enabled(self, _permitted, _enabled, start_scan):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        lock_manager = MagicMock()
        lock_manager.is_locked = AsyncMock(return_value=False)

        await run_for('4.21', runtime, lock_manager)

        start_scan.assert_called_once_with(version='4.21', block_until_building=False)
