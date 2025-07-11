import unittest
from types import SimpleNamespace

from click.testing import CliRunner
from elliottlib.bzutil import BugzillaBugTracker, JIRABugTracker
from elliottlib.cli.common import Runtime, cli
from elliottlib.cli.find_bugs_second_fix_cli import find_bugs_second_fix_cli
from flexmock import flexmock


class FindBugsSecondFixTestCase(unittest.TestCase):
    def test_find_bugs_second_fix(self):
        runner = CliRunner()
        jira_bug = flexmock(id='OCPBUGS-123', status="MODIFIED")
        bz_bug = flexmock(id='BZ-123', status="MODIFIED")

        mock_runtime = flexmock()
        mock_runtime.group_config = SimpleNamespace(software_lifecycle=SimpleNamespace(phase='pre-release'))
        mock_runtime.should_receive("initialize").and_return(None)
        mock_runtime.should_receive("get_major_minor").and_return(4, 20)
        flexmock(JIRABugTracker).should_receive("get_config").and_return(
            {
                'target_release': ['4.20.0'],
                'server': "server",
            }
        )

        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        client.should_receive("search_issues").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        expected_comment = (
            "This CVE tracker was closed since it was not declared as first-fix for the"
            "recent OCP 4.20 version in pre-release"
        )
        flexmock(JIRABugTracker).should_receive("update_bug_status").with_args(
            jira_bug,
            target_status='CLOSED',
            comment=expected_comment,
            log_comment=True,
            noop=True,
        )

        flexmock(BugzillaBugTracker).should_receive("get_config").and_return(
            {
                'target_release': ['4.20.0'],
                'server': "bugzilla.redhat.com",
            }
        )
        flexmock(BugzillaBugTracker).should_receive("login").and_return(None)
        flexmock(BugzillaBugTracker).should_receive("search").and_return([bz_bug])

        result = runner.invoke(cli, ['-g', 'openshift-4.20', 'find-bugs:second-fix', '--noop'])
        self.assertEqual(result.exit_code, 0)


if __name__ == '__main__':
    unittest.main()
