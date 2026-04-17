import unittest
from unittest.mock import patch

import elliottlib.cli.find_bugs_qe_cli as qe_cli
from click.testing import CliRunner
from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.common import Runtime, cli
from elliottlib.cli.find_bugs_qe_cli import FindBugsQE, close_reconciliation_bugs, find_bugs_qe
from flexmock import flexmock


class FindBugsQETestCase(unittest.TestCase):
    def test_find_bugs_qe(self):
        runner = CliRunner()
        jira_bug = flexmock(id='OCPBUGS-123', status="MODIFIED", is_tracker_bug=lambda: False, security_level=None)
        reconciliation_bug = flexmock(id='OCPBUGS-456', status="Verified", is_tracker_bug=lambda: False)

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

        # Mock for MODIFIED bugs search
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

        # Mock for reconciliation bugs search
        flexmock(JIRABugTracker).should_receive("_query").with_args(
            status=['Verified'],
            include_labels=['art:reconciliation'],
        ).and_return("mocked query").once()

        flexmock(JIRABugTracker).should_receive("_search").with_args("mocked query", verbose=False).and_return(
            [reconciliation_bug]
        ).once()

        result = runner.invoke(cli, ['-g', 'openshift-4.6', 'find-bugs:qe', '--noop'])
        self.assertEqual(result.exit_code, 0)

    def test_find_bugs_qe_opt_out_skips_filter(self):
        runtime = flexmock(debug=False, get_major_minor=lambda: (4, 6))
        bug = flexmock(id='OCPBUGS-123', status="MODIFIED", is_tracker_bug=lambda: False, security_level=None)
        bug_tracker = flexmock(type='jira')
        find_bugs_obj = FindBugsQE(art_managed_trackers_only=False)
        flexmock(find_bugs_obj).should_receive("search").and_return([bug]).once()
        flexmock(bug_tracker).should_receive("target_release").and_return(["4.6.z"])
        flexmock(bug_tracker).should_receive("update_bug_status").with_args(
            bug,
            'ON_QA',
            comment=(
                "An ART build cycle completed after this fix was made, which usually means it can be"
                " expected in the next created 4.6 nightly and release."
            ),
            noop=True,
        ).once()

        with patch.object(qe_cli, "filter_art_managed_jira_trackers") as filter_mock:
            find_bugs_qe(runtime, find_bugs_obj, True, bug_tracker)

        filter_mock.assert_not_called()

    def test_close_reconciliation_bugs_unchanged(self):
        runtime = flexmock(debug=False, get_major_minor=lambda: (4, 6))
        bug1 = flexmock(id='OCPBUGS-456', status="Verified")
        bug2 = flexmock(id='OCPBUGS-789', status="Verified")
        client = flexmock()
        bug_tracker = flexmock(type='jira', _client=client)
        flexmock(bug_tracker).should_receive("target_release").and_return(["4.6.z"])
        flexmock(bug_tracker).should_receive("_query").with_args(
            status=['Verified'],
            include_labels=['art:reconciliation'],
        ).and_return("mocked query").once()
        flexmock(bug_tracker).should_receive("_search").with_args("mocked query", verbose=False).and_return(
            [bug1, bug2]
        ).once()
        for bug in [bug1, bug2]:
            flexmock(client).should_receive("transition_issue").with_args(
                bug.id, 'Closed', fields={'resolution': {'name': 'Done'}}
            ).once()
            flexmock(bug_tracker).should_receive("add_comment").with_args(
                bug.id,
                "This bug has been identified as having no customer value and will be closed without shipping. "
                "It was part of an ART reconciliation process and does not require further action.",
                private=False,
                noop=False,
            ).once()

        with patch.object(qe_cli, "filter_art_managed_jira_trackers") as filter_mock:
            close_reconciliation_bugs(runtime, False, bug_tracker)

        filter_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
