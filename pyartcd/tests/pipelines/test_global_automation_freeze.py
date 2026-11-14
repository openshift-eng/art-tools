from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.scheduled import schedule_ocp4_scan, schedule_ocp4_scan_konflux


class TestGlobalAutomationFreeze(IsolatedAsyncioTestCase):
    @patch('pyartcd.util.is_global_automation_frozen')
    @patch('pyartcd.util.exectools.cmd_gather_async', return_value=(0, 'no', ''))
    async def test_schedulers_respect_master_switch(self, read_group, global_freeze):
        runtime = MagicMock()
        lock_manager = MagicMock(is_locked=AsyncMock(return_value=False))
        for scheduler, trigger in [
            (schedule_ocp4_scan, 'start_ocp4_scan'),
            (schedule_ocp4_scan_konflux, 'start_ocp4_scan_konflux'),
        ]:
            for frozen in [True, False]:
                for version in ['4.18', '4.22']:
                    with self.subTest(scheduler=scheduler.__name__, frozen=frozen, version=version):
                        global_freeze.return_value = frozen
                        read_group.reset_mock()
                        with patch.object(scheduler.jenkins, trigger) as start_scan:
                            await scheduler.run_for(version, runtime, lock_manager)
                            if frozen:
                                start_scan.assert_not_called()
                                read_group.assert_not_awaited()
                            else:
                                start_scan.assert_called_once()
                                self.assertEqual(start_scan.call_args.kwargs['version'], version)
                                read_group.assert_awaited_once()

    @patch('pyartcd.util.is_global_automation_frozen', side_effect=TimeoutError)
    @patch('pyartcd.jenkins.start_ocp4_scan_konflux')
    async def test_config_read_failure_does_not_trigger_builds(self, start_scan, global_freeze):
        with self.assertRaises(TimeoutError):
            await schedule_ocp4_scan_konflux.run_for(
                '4.22', MagicMock(), MagicMock(is_locked=AsyncMock(return_value=False))
            )
        start_scan.assert_not_called()
