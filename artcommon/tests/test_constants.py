import unittest

from artcommonlib import constants


class TestFreezeAutomationConstants(unittest.TestCase):
    def test_valid_values_cover_the_constants(self):
        self.assertEqual(
            set(constants.VALID_FREEZE_AUTOMATION_VALUES),
            {
                constants.FREEZE_AUTOMATION_YES,
                constants.FREEZE_AUTOMATION_SCHEDULED,
                constants.FREEZE_AUTOMATION_WEEKDAYS,
                constants.FREEZE_AUTOMATION_NO,
            },
        )
        self.assertEqual(constants.FREEZE_AUTOMATION_YES, 'yes')
        self.assertEqual(constants.FREEZE_AUTOMATION_SCHEDULED, 'scheduled')
        self.assertEqual(constants.FREEZE_AUTOMATION_WEEKDAYS, 'weekdays')
        self.assertEqual(constants.FREEZE_AUTOMATION_NO, 'no')

    def test_normalize_booleans(self):
        self.assertEqual(constants.normalize_freeze_automation(True), 'yes')
        self.assertEqual(constants.normalize_freeze_automation(False), 'no')

    def test_normalize_strings(self):
        expectations = {
            'yes': 'yes',
            'YES': 'yes',
            'true': 'yes',
            'True': 'yes',
            'no': 'no',
            'false': 'no',
            'False': 'no',
            'scheduled': 'scheduled',
            'SCHEDULED': 'scheduled',
            'weekdays': 'weekdays',
            ' weekdays ': 'weekdays',
        }
        for value, expected in expectations.items():
            with self.subTest(value=value):
                self.assertEqual(constants.normalize_freeze_automation(value), expected)
        # Every normalized result must be one of the valid values.
        for value in expectations:
            self.assertIn(
                constants.normalize_freeze_automation(value),
                constants.VALID_FREEZE_AUTOMATION_VALUES,
            )

    def test_normalize_rejects_junk(self):
        for bad in ['bogus', 'maybe', '', 42, None, [], {}, 1.5]:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    constants.normalize_freeze_automation(bad)


if __name__ == '__main__':
    unittest.main()
