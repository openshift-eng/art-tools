from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.scheduled import schedule_ocp4_scan_konflux


class TestGlobalAutomationFreeze(IsolatedAsyncioTestCase):
    @patch('pyartcd.util.get_global_freeze_automation', new_callable=AsyncMock)
    @patch('pyartcd.util.exectools.cmd_gather_async', return_value=(0, 'no', ''))
    async def test_schedulers_respect_master_switch(self, read_group, global_freeze):
        runtime = MagicMock(dry_run=False)
        lock_manager = MagicMock(is_locked=AsyncMock(return_value=False))
        for scheduler, trigger in [
            (schedule_ocp4_scan_konflux, 'start_ocp4_scan_konflux'),
        ]:
            # Global 'yes' is a hard stop (group never read); global 'no' defers to the group,
            # whose read here returns 'no' so the scan is scheduled.
            for global_state, should_trigger in [('yes', False), ('no', True)]:
                for version in ['4.18', '4.22']:
                    with self.subTest(scheduler=scheduler.__name__, global_state=global_state, version=version):
                        global_freeze.return_value = global_state
                        read_group.reset_mock()
                        with patch.object(scheduler.jenkins, trigger) as start_scan:
                            await scheduler.run_for(version, runtime, lock_manager)
                            if should_trigger:
                                start_scan.assert_called_once()
                                self.assertEqual(start_scan.call_args.kwargs['version'], version)
                                read_group.assert_awaited_once()
                            else:
                                start_scan.assert_not_called()
                                read_group.assert_not_awaited()

    @patch('pyartcd.util.is_manual_build')
    @patch('pyartcd.util.get_global_freeze_automation', new_callable=AsyncMock, return_value='scheduled')
    @patch('pyartcd.util.exectools.cmd_gather_async', return_value=(0, 'no', ''))
    async def test_global_scheduled_is_manual_aware(self, read_group, global_freeze, manual_build):
        # A global 'scheduled' permits manual runs but blocks scheduled (non-manual) runs. The group
        # value is never read because the non-'no' global state takes precedence.
        runtime = MagicMock(dry_run=False)
        lock_manager = MagicMock(is_locked=AsyncMock(return_value=False))
        for manual, should_trigger in [(True, True), (False, False)]:
            with self.subTest(manual=manual):
                manual_build.return_value = manual
                read_group.reset_mock()
                with patch.object(schedule_ocp4_scan_konflux.jenkins, 'start_ocp4_scan_konflux') as start_scan:
                    await schedule_ocp4_scan_konflux.run_for('4.22', runtime, lock_manager)
                    self.assertEqual(start_scan.called, should_trigger)
                    read_group.assert_not_awaited()

    @patch('pyartcd.util.get_global_freeze_automation', side_effect=TimeoutError)
    @patch('pyartcd.jenkins.start_ocp4_scan_konflux')
    async def test_config_read_failure_does_not_trigger_builds(self, start_scan, global_freeze):
        # Fail closed: an error reading the master switch must never result in a build being triggered.
        with self.assertRaises(TimeoutError):
            await schedule_ocp4_scan_konflux.run_for(
                '4.22', MagicMock(), MagicMock(is_locked=AsyncMock(return_value=False))
            )
        start_scan.assert_not_called()
