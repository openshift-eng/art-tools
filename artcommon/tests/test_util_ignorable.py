"""
Tests for should_honor_ignorable_repos() function in artcommonlib.util
"""

import unittest
from unittest.mock import MagicMock, patch

from artcommonlib.model import Missing, Model
from artcommonlib.util import should_honor_ignorable_repos


class TestShouldHonorIgnorableRepos(unittest.TestCase):
    """
    Test suite for should_honor_ignorable_repos() decision logic
    """

    def test_force_flag_always_wins_during_release(self):
        """Force flag should override all other logic, even during release phase with upcoming GA"""
        runtime = MagicMock(spec=['group', 'group_config'])
        runtime.group = 'openshift-4.17'
        runtime.group_config = Model({'software_lifecycle': {'phase': 'release'}})

        # Even during release phase with upcoming GA, force flag should win
        with patch('artcommonlib.util.is_release_next_week', return_value=True):
            self.assertTrue(should_honor_ignorable_repos(runtime, force_ignore=True))

    def test_force_flag_wins_during_pre_release(self):
        """Force flag should work during pre-release phase too"""
        runtime = MagicMock()
        runtime.group_config = Model({'software_lifecycle': {'phase': 'pre-release'}})

        self.assertTrue(should_honor_ignorable_repos(runtime, force_ignore=True))

    def test_pre_release_phase_honors_ignorable(self):
        """Pre-release phase should always honor ignorable repos"""
        runtime = MagicMock()
        runtime.group_config = Model({'software_lifecycle': {'phase': 'pre-release'}})

        self.assertTrue(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_signing_phase_honors_ignorable(self):
        """Signing phase should always honor ignorable repos"""
        runtime = MagicMock()
        runtime.group_config = Model({'software_lifecycle': {'phase': 'signing'}})

        self.assertTrue(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_eol_phase_honors_ignorable(self):
        """End-of-life phase should always honor ignorable repos"""
        runtime = MagicMock()
        runtime.group_config = Model({'software_lifecycle': {'phase': 'eol'}})

        self.assertTrue(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_release_phase_with_upcoming_ga_does_not_honor(self):
        """During release phase with GA within 2 days, should NOT honor ignorable"""
        runtime = MagicMock(spec=['group', 'group_config'])
        runtime.group = 'openshift-4.17'
        runtime.group_config = Model({'software_lifecycle': {'phase': 'release'}})

        with patch('artcommonlib.util.is_release_next_week', return_value=True):
            self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_release_phase_without_upcoming_ga_honors(self):
        """During release phase without upcoming GA (>2 days away), should honor ignorable"""
        runtime = MagicMock(spec=['group', 'group_config'])
        runtime.group = 'openshift-4.17'
        runtime.group_config = Model({'software_lifecycle': {'phase': 'release'}})

        with patch('artcommonlib.util.is_release_next_week', return_value=False):
            self.assertTrue(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_missing_group_config_defaults_to_not_honor(self):
        """Missing group_config should default to NOT honoring (fail-safe)"""
        runtime = MagicMock()
        runtime.group_config = None

        self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_missing_software_lifecycle_defaults_to_not_honor(self):
        """Missing software_lifecycle should default to NOT honoring (fail-safe)"""
        runtime = MagicMock()
        runtime.group_config = Model({})

        self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_software_lifecycle_is_missing_sentinel(self):
        """software_lifecycle as Missing sentinel should default to NOT honoring"""
        runtime = MagicMock()
        runtime.group_config = MagicMock()
        runtime.group_config.software_lifecycle = Missing

        self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_missing_phase_defaults_to_not_honor(self):
        """Missing phase value should default to NOT honoring (fail-safe)"""
        runtime = MagicMock()
        runtime.group_config = Model({'software_lifecycle': {}})

        self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_phase_is_missing_sentinel(self):
        """phase as Missing sentinel should default to NOT honoring"""
        runtime = MagicMock()
        runtime.group_config = MagicMock()
        runtime.group_config.software_lifecycle = MagicMock()
        runtime.group_config.software_lifecycle.phase = Missing

        self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_invalid_phase_name_defaults_to_not_honor(self):
        """Invalid phase name should default to NOT honoring (fail-safe)"""
        runtime = MagicMock()
        runtime.group_config = Model({'software_lifecycle': {'phase': 'invalid-phase'}})

        self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_api_failure_defaults_to_not_honor(self):
        """Product Pages API failure should default to NOT honoring (fail-safe)"""
        runtime = MagicMock(spec=['group', 'group_config'])
        runtime.group = 'openshift-4.17'
        runtime.group_config = Model({'software_lifecycle': {'phase': 'release'}})

        with patch('artcommonlib.util.is_release_next_week', side_effect=Exception("API down")):
            self.assertFalse(should_honor_ignorable_repos(runtime, force_ignore=False))

    def test_caching_on_runtime_works(self):
        """Release schedule query should be cached on runtime to avoid repeated API calls"""
        runtime = MagicMock(spec=['group', 'group_config'])
        runtime.group = 'openshift-4.17'
        runtime.group_config = Model({'software_lifecycle': {'phase': 'release'}})

        with patch('artcommonlib.util.is_release_next_week', return_value=True) as mock_api:
            # First call
            result1 = should_honor_ignorable_repos(runtime, force_ignore=False)
            self.assertFalse(result1)
            self.assertEqual(mock_api.call_count, 1)

            # Second call should use cache
            result2 = should_honor_ignorable_repos(runtime, force_ignore=False)
            self.assertFalse(result2)
            self.assertEqual(mock_api.call_count, 1)  # Still 1, not 2

            # Verify cache attribute exists
            self.assertTrue(hasattr(runtime, '_cached_is_release_next_week'))
            self.assertTrue(runtime._cached_is_release_next_week)

    def test_caching_stores_api_failure_result(self):
        """Even API failures should be cached to avoid hammering broken endpoint"""
        runtime = MagicMock(spec=['group', 'group_config'])
        runtime.group = 'openshift-4.17'
        runtime.group_config = Model({'software_lifecycle': {'phase': 'release'}})

        with patch('artcommonlib.util.is_release_next_week', side_effect=Exception("API down")) as mock_api:
            # First call
            result1 = should_honor_ignorable_repos(runtime, force_ignore=False)
            self.assertFalse(result1)
            self.assertEqual(mock_api.call_count, 1)

            # Second call should not retry API
            result2 = should_honor_ignorable_repos(runtime, force_ignore=False)
            self.assertFalse(result2)
            self.assertEqual(mock_api.call_count, 1)  # Still 1, not 2

            # Verify cache stores the fail-safe value (True)
            self.assertTrue(hasattr(runtime, '_cached_is_release_next_week'))
            self.assertTrue(runtime._cached_is_release_next_week)

    def test_group_with_commitish_is_stripped(self):
        """Group name with @commitish should be stripped before API call"""
        runtime = MagicMock(spec=['group', 'group_config'])
        runtime.group = 'openshift-4.17@abc123def'
        runtime.group_config = Model({'software_lifecycle': {'phase': 'release'}})

        with patch('artcommonlib.util.is_release_next_week', return_value=False) as mock_api:
            should_honor_ignorable_repos(runtime, force_ignore=False)

            # Verify API was called with stripped group name
            mock_api.assert_called_once_with('openshift-4.17')


if __name__ == '__main__':
    unittest.main()
