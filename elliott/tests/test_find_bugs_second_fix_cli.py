import unittest
from unittest.mock import MagicMock, PropertyMock, patch

from click.testing import CliRunner
from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.common import Runtime, cli
from elliottlib.cli.find_bugs_second_fix_cli import (
    FindBugsSecondFix,
    find_bugs_second_fix,
)
from flexmock import flexmock


class TestFindBugsSecondFix(unittest.TestCase):
    def test_find_bugs_second_fix_class_initialization(self):
        """Test that FindBugsSecondFix is initialized with correct parameters"""
        find_bugs = FindBugsSecondFix()
        self.assertEqual(find_bugs.status, {"MODIFIED", "VERIFIED", "ON_QA"})
        self.assertTrue(find_bugs.cve_only)


class FindBugsSecondFixCLITestCase(unittest.TestCase):
    def setUp(self):
        """Set up common test fixtures"""
        self.runner = CliRunner()
        self.major_version = 4
        self.minor_version = 20

    def _setup_common_mocks(self, phase="pre-release"):
        """Setup common mocks used across tests"""
        # Group config with software lifecycle
        group_config = flexmock(software_lifecycle=flexmock(phase=phase))

        # Runtime mocks
        flexmock(Runtime).should_receive("initialize").with_args(mode="images").and_return(None)
        flexmock(Runtime).should_receive("get_major_minor").and_return(self.major_version, self.minor_version)

        # Mock group_config property using PropertyMock
        group_config_patch = patch.object(Runtime, "group_config", new_callable=PropertyMock, return_value=group_config)

        # JIRA bug tracker mocks
        flexmock(JIRABugTracker).should_receive("get_config").and_return({"target_release": ["4.20.z"]})
        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)

        return group_config_patch

    def test_find_bugs_second_fix_cli_report_mode(self):
        """Test find-bugs:second-fix command in report mode (no --close flag)"""
        group_config_patch = self._setup_common_mocks()

        # Create mock tracker bugs
        tracker1 = flexmock(id="OCPBUGS-1", is_tracker_bug=lambda: True)
        tracker2 = flexmock(id="OCPBUGS-2", is_tracker_bug=lambda: True)
        all_bugs = [tracker1, tracker2, flexmock(id="OCPBUGS-3", is_tracker_bug=lambda: False)]

        # Mock the search to return bugs
        flexmock(FindBugsSecondFix).should_receive("search").and_return(all_bugs)

        # Mock get_second_fix_trackers to return one second-fix tracker
        with (
            group_config_patch,
            patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix,
        ):
            mock_second_fix.return_value = [tracker1]

            result = self.runner.invoke(cli, ["-g", "openshift-4.20", "find-bugs:second-fix"])

            # Verify command executed successfully
            self.assertEqual(result.exit_code, 0)
            # Note: LOGGER.info() output is not captured by CliRunner, so we only verify exit code

    def test_find_bugs_second_fix_cli_with_close_flag(self):
        """Test find-bugs:second-fix command with --close flag"""
        group_config_patch = self._setup_common_mocks()

        # Create mock tracker bugs
        tracker1 = flexmock(id="OCPBUGS-1", is_tracker_bug=lambda: True)
        tracker2 = flexmock(id="OCPBUGS-2", is_tracker_bug=lambda: True)
        all_bugs = [tracker1, tracker2]

        # Mock the search to return bugs
        flexmock(FindBugsSecondFix).should_receive("search").and_return(all_bugs)

        # Mock get_second_fix_trackers
        with (
            group_config_patch,
            patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix,
        ):
            mock_second_fix.return_value = [tracker1]

            # Mock bug tracker update
            bug_tracker = flexmock()
            flexmock(Runtime).should_receive("get_bug_tracker").with_args("jira").and_return(bug_tracker)
            bug_tracker.should_receive("update_bug_status").with_args(
                bug=tracker1, target_status="CLOSED", comment=str, log_comment=True, noop=False
            ).once()

            result = self.runner.invoke(cli, ["-g", "openshift-4.20", "find-bugs:second-fix", "--close"])

            # Verify command executed successfully
            self.assertEqual(result.exit_code, 0)

    def test_find_bugs_second_fix_cli_with_noop_flag(self):
        """Test find-bugs:second-fix command with --noop flag"""
        group_config_patch = self._setup_common_mocks()

        # Create mock tracker bugs
        tracker1 = flexmock(id="OCPBUGS-1", is_tracker_bug=lambda: True)
        all_bugs = [tracker1]

        # Mock the search to return bugs
        flexmock(FindBugsSecondFix).should_receive("search").and_return(all_bugs)

        # Mock get_second_fix_trackers
        with (
            group_config_patch,
            patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix,
        ):
            mock_second_fix.return_value = [tracker1]

            # Mock bug tracker update with noop=True
            bug_tracker = flexmock()
            flexmock(Runtime).should_receive("get_bug_tracker").with_args("jira").and_return(bug_tracker)
            bug_tracker.should_receive("update_bug_status").with_args(
                bug=tracker1, target_status="CLOSED", comment=str, log_comment=True, noop=True
            ).once()

            result = self.runner.invoke(cli, ["-g", "openshift-4.20", "find-bugs:second-fix", "--close", "--noop"])

            self.assertEqual(result.exit_code, 0)

    def test_find_bugs_second_fix_cli_wrong_lifecycle_phase(self):
        """Test find-bugs:second-fix when software lifecycle is not in allowed phases"""
        group_config_patch = self._setup_common_mocks(phase="release")

        # Mock the search (should not be called)
        flexmock(FindBugsSecondFix).should_receive("search").never()

        with group_config_patch:
            result = self.runner.invoke(cli, ["-g", "openshift-4.20", "find-bugs:second-fix"])

            # Verify command executed successfully
            self.assertEqual(result.exit_code, 0)

    def test_find_bugs_second_fix_cli_signing_phase(self):
        """Test find-bugs:second-fix works in signing phase"""
        group_config_patch = self._setup_common_mocks(phase="signing")

        # Create mock tracker bugs
        tracker1 = flexmock(id="OCPBUGS-1", is_tracker_bug=lambda: True)
        all_bugs = [tracker1]

        # Mock the search to return bugs
        flexmock(FindBugsSecondFix).should_receive("search").and_return(all_bugs)

        # Mock get_second_fix_trackers
        with (
            group_config_patch,
            patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix,
        ):
            mock_second_fix.return_value = []

            result = self.runner.invoke(cli, ["-g", "openshift-4.20", "find-bugs:second-fix"])

            # Verify command executed successfully
            self.assertEqual(result.exit_code, 0)

    def test_find_bugs_second_fix_no_trackers_found(self):
        """Test find-bugs:second-fix when no tracker bugs are found"""
        group_config_patch = self._setup_common_mocks()

        # Mock the search to return no bugs
        flexmock(FindBugsSecondFix).should_receive("search").and_return([])

        # Mock get_second_fix_trackers (will be called with empty list)
        with (
            group_config_patch,
            patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix,
        ):
            mock_second_fix.return_value = []

            result = self.runner.invoke(cli, ["-g", "openshift-4.20", "find-bugs:second-fix"])

            # Verify command executed successfully
            self.assertEqual(result.exit_code, 0)

    def test_find_bugs_second_fix_no_second_fix_trackers(self):
        """Test find-bugs:second-fix when no second-fix trackers are found"""
        group_config_patch = self._setup_common_mocks()

        # Create mock tracker bugs
        tracker1 = flexmock(id="OCPBUGS-1", is_tracker_bug=lambda: True)
        all_bugs = [tracker1]

        # Mock the search to return bugs
        flexmock(FindBugsSecondFix).should_receive("search").and_return(all_bugs)

        # Mock get_second_fix_trackers to return empty list
        with (
            group_config_patch,
            patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix,
        ):
            mock_second_fix.return_value = []

            result = self.runner.invoke(cli, ["-g", "openshift-4.20", "find-bugs:second-fix"])

            # Verify command executed successfully
            self.assertEqual(result.exit_code, 0)


class TestFindBugsSecondFixFunction(unittest.TestCase):
    def setUp(self):
        """Set up common test fixtures"""
        self.runtime = MagicMock()
        self.runtime.get_major_minor.return_value = (4, 17)
        self.runtime.debug = False

        self.find_bugs_obj = MagicMock()
        self.bug_tracker = MagicMock()

    def test_find_bugs_second_fix_allowed_phase_no_close(self):
        """Test find_bugs_second_fix function in allowed phase without closing"""
        self.runtime.group_config.software_lifecycle.phase = "pre-release"

        # Create mock tracker bugs
        tracker1 = MagicMock()
        tracker1.id = "OCPBUGS-1"
        tracker1.is_tracker_bug.return_value = True

        tracker2 = MagicMock()
        tracker2.id = "OCPBUGS-2"
        tracker2.is_tracker_bug.return_value = False

        all_bugs = [tracker1, tracker2]
        self.find_bugs_obj.search.return_value = all_bugs

        # Mock get_second_fix_trackers
        with patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix:
            mock_second_fix.return_value = [tracker1]

            find_bugs_second_fix(
                runtime=self.runtime,
                find_bugs_obj=self.find_bugs_obj,
                close=False,
                noop=False,
                bug_tracker=self.bug_tracker,
            )

            # Verify search was called
            self.find_bugs_obj.search.assert_called_once()
            # Verify update_bug_status was NOT called
            self.bug_tracker.update_bug_status.assert_not_called()

    def test_find_bugs_second_fix_with_close(self):
        """Test find_bugs_second_fix function with close=True"""
        self.runtime.group_config.software_lifecycle.phase = "pre-release"

        # Create mock tracker bugs
        tracker1 = MagicMock()
        tracker1.id = "OCPBUGS-1"
        tracker1.is_tracker_bug.return_value = True

        all_bugs = [tracker1]
        self.find_bugs_obj.search.return_value = all_bugs

        # Mock get_bug_tracker to return our mock
        self.runtime.get_bug_tracker.return_value = self.bug_tracker

        # Mock get_second_fix_trackers
        with patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix:
            mock_second_fix.return_value = [tracker1]

            find_bugs_second_fix(
                runtime=self.runtime,
                find_bugs_obj=self.find_bugs_obj,
                close=True,
                noop=False,
                bug_tracker=self.bug_tracker,
            )

            # Verify update_bug_status was called
            self.bug_tracker.update_bug_status.assert_called_once()
            call_args = self.bug_tracker.update_bug_status.call_args
            self.assertEqual(call_args[1]["bug"], tracker1)
            self.assertEqual(call_args[1]["target_status"], "CLOSED")
            self.assertIn("4.16", call_args[1]["comment"])
            self.assertIn("pre-release", call_args[1]["comment"])
            self.assertTrue(call_args[1]["log_comment"])
            self.assertFalse(call_args[1]["noop"])

    def test_find_bugs_second_fix_comment_version_calculation(self):
        """Test that the comment contains the correct previous version"""
        self.runtime.group_config.software_lifecycle.phase = "pre-release"
        self.runtime.get_major_minor.return_value = (4, 18)

        # Create mock tracker bug
        tracker1 = MagicMock()
        tracker1.id = "OCPBUGS-1"
        tracker1.is_tracker_bug.return_value = True

        all_bugs = [tracker1]
        self.find_bugs_obj.search.return_value = all_bugs

        # Mock get_bug_tracker
        self.runtime.get_bug_tracker.return_value = self.bug_tracker

        # Mock get_second_fix_trackers
        with patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix:
            mock_second_fix.return_value = [tracker1]

            find_bugs_second_fix(
                runtime=self.runtime,
                find_bugs_obj=self.find_bugs_obj,
                close=True,
                noop=False,
                bug_tracker=self.bug_tracker,
            )

            # Verify the comment mentions 4.17 (18 - 1)
            call_args = self.bug_tracker.update_bug_status.call_args
            self.assertIn("4.17", call_args[1]["comment"])

    def test_find_bugs_second_fix_exception_handling(self):
        """Test exception handling when closing bugs fails"""
        self.runtime.group_config.software_lifecycle.phase = "pre-release"

        # Create mock tracker bug
        tracker1 = MagicMock()
        tracker1.id = "OCPBUGS-1"
        tracker1.is_tracker_bug.return_value = True

        all_bugs = [tracker1]
        self.find_bugs_obj.search.return_value = all_bugs

        # Mock get_bug_tracker
        self.runtime.get_bug_tracker.return_value = self.bug_tracker

        # Make update_bug_status raise an exception
        self.bug_tracker.update_bug_status.side_effect = Exception("API Error")

        # Mock get_second_fix_trackers
        with patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix:
            mock_second_fix.return_value = [tracker1]

            # Should not raise exception, should handle it gracefully
            find_bugs_second_fix(
                runtime=self.runtime,
                find_bugs_obj=self.find_bugs_obj,
                close=True,
                noop=False,
                bug_tracker=self.bug_tracker,
            )

            # Verify update_bug_status was called despite exception
            self.bug_tracker.update_bug_status.assert_called_once()

    def test_find_bugs_second_fix_disallowed_phase(self):
        """Test find_bugs_second_fix with disallowed lifecycle phase"""
        self.runtime.group_config.software_lifecycle.phase = "release"

        find_bugs_second_fix(
            runtime=self.runtime,
            find_bugs_obj=self.find_bugs_obj,
            close=False,
            noop=False,
            bug_tracker=self.bug_tracker,
        )

        # Verify search was NOT called
        self.find_bugs_obj.search.assert_not_called()
        # Verify update_bug_status was NOT called
        self.bug_tracker.update_bug_status.assert_not_called()

    def test_find_bugs_second_fix_filters_non_trackers(self):
        """Test that non-tracker bugs are filtered out"""
        self.runtime.group_config.software_lifecycle.phase = "pre-release"

        # Create mixed bugs
        tracker1 = MagicMock()
        tracker1.id = "OCPBUGS-1"
        tracker1.is_tracker_bug.return_value = True

        non_tracker = MagicMock()
        non_tracker.id = "OCPBUGS-2"
        non_tracker.is_tracker_bug.return_value = False

        all_bugs = [tracker1, non_tracker]
        self.find_bugs_obj.search.return_value = all_bugs

        # Mock get_second_fix_trackers - should only receive tracker bugs
        with patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix:
            mock_second_fix.return_value = []

            find_bugs_second_fix(
                runtime=self.runtime,
                find_bugs_obj=self.find_bugs_obj,
                close=False,
                noop=False,
                bug_tracker=self.bug_tracker,
            )

            # Verify get_second_fix_trackers was called with only tracker bugs
            called_trackers = mock_second_fix.call_args[0][1]
            self.assertEqual(len(called_trackers), 1)
            self.assertEqual(called_trackers[0].id, "OCPBUGS-1")

    def test_find_bugs_second_fix_with_noop(self):
        """Test find_bugs_second_fix function with noop=True"""
        self.runtime.group_config.software_lifecycle.phase = "pre-release"

        # Create mock tracker bug
        tracker1 = MagicMock()
        tracker1.id = "OCPBUGS-1"
        tracker1.is_tracker_bug.return_value = True

        all_bugs = [tracker1]
        self.find_bugs_obj.search.return_value = all_bugs

        # Mock get_bug_tracker
        self.runtime.get_bug_tracker.return_value = self.bug_tracker

        # Mock get_second_fix_trackers
        with patch("elliottlib.cli.find_bugs_second_fix_cli.get_second_fix_trackers") as mock_second_fix:
            mock_second_fix.return_value = [tracker1]

            find_bugs_second_fix(
                runtime=self.runtime,
                find_bugs_obj=self.find_bugs_obj,
                close=True,
                noop=True,
                bug_tracker=self.bug_tracker,
            )

            # Verify noop flag was passed
            call_args = self.bug_tracker.update_bug_status.call_args
            self.assertTrue(call_args[1]["noop"])


if __name__ == "__main__":
    unittest.main()
