import unittest

from click.testing import CliRunner
from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.common import Runtime, cli
from elliottlib.cli.find_bugs_qe_cli import FindBugsQE
from flexmock import flexmock


class FindBugsQETestCase(unittest.TestCase):
    def test_find_bugs_qe(self):
        runner = CliRunner()
        jira_bug = flexmock(id='OCPBUGS-123', status="MODIFIED")

        flexmock(Runtime).should_receive("initialize").and_return(None)
        flexmock(Runtime).should_receive("get_major_minor").and_return(4, 6)
        flexmock(JIRABugTracker).should_receive("get_config").and_return(
            {
                'target_release': ['4.6.z'],
                'server': "server",
            }
        )
        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        flexmock(JIRABugTracker).should_receive("search").and_return([jira_bug])
        expected_comment = (
            "An ART build cycle completed after this fix was made, which usually means it can be"
            " expected in the next created 4.6 nightly and release."
        )
        flexmock(JIRABugTracker).should_receive("update_bug_status").with_args(
            jira_bug,
            'ON_QA',
            comment=expected_comment,
            noop=True,
        )

        result = runner.invoke(cli, ['-g', 'openshift-4.6', 'find-bugs:qe', '--noop'])
        self.assertEqual(result.exit_code, 0)


if __name__ == '__main__':
    unittest.main()
