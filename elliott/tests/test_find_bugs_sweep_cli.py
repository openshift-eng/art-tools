import traceback
import unittest
from unittest.mock import Mock, patch

import elliottlib.cli.find_bugs_sweep_cli as sweep_cli
from click.testing import CliRunner
from elliottlib import errata
from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.common import Runtime, cli
from elliottlib.cli.find_bugs_sweep_cli import FindBugsMode, categorize_bugs_by_type, extras_bugs, get_assembly_bug_ids
from elliottlib.exceptions import ElliottFatalError
from flexmock import flexmock


class TestFindBugsMode(unittest.TestCase):
    def test_find_bugs_mode_search(self):
        config = {
            'target_release': ['4.3.0', '4.3.z'],
            'product': "product",
            'server': "server",
        }
        bug_ids = [1, 2]

        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        flexmock(JIRABugTracker).should_receive("search").and_return(bug_ids)

        bug_tracker = JIRABugTracker(config)
        find_bugs = FindBugsMode(status=['foo', 'bar'], cve_only=False)
        find_bugs.include_status(['alpha'])
        find_bugs.exclude_status(['foo'])
        bugs = find_bugs.search(bug_tracker_obj=bug_tracker)
        self.assertEqual(bug_ids, bugs)


class FindBugsSweepTestCase(unittest.IsolatedAsyncioTestCase):
    @patch('elliottlib.bzutil.JIRABugTracker.filter_attached_bugs')
    @patch('elliottlib.cli.find_bugs_sweep_cli.FindBugsSweep.search')
    def test_find_bugs_sweep_report_jira(self, search_mock, jira_filter_mock):
        # jira mocks
        jira_bug = flexmock(
            id='OCPBUGS-1',
            component='OLM',
            status='ON_QA',
            summary='summary',
            created_days_ago=lambda: 7,
        )

        runner = CliRunner()

        # common mocks
        flexmock(Runtime).should_receive("initialize").and_return(None)
        flexmock(Runtime).should_receive("get_major_minor").and_return(4, 6)
        flexmock(sweep_cli).should_receive("get_assembly_bug_ids").and_return(set(), set())
        flexmock(Runtime).should_receive("get_default_advisories").and_return({})
        flexmock(sweep_cli).should_receive("categorize_bugs_by_type").and_return({"image": {jira_bug}}, [])

        flexmock(JIRABugTracker).should_receive("get_config").and_return({'target_release': ['4.6.z']})
        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        search_mock.return_value = [jira_bug]
        jira_filter_mock.return_value = []

        result = runner.invoke(cli, ['-g', 'openshift-4.6', 'find-bugs:sweep', '--report'])
        if result.exit_code != 0:
            exc_type, exc_value, exc_traceback = result.exc_info
            t = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.fail(t)
        self.assertIn('OCPBUGS-1', result.output)

    @patch('elliottlib.bzutil.JIRABugTracker.filter_attached_bugs')
    @patch('elliottlib.cli.find_bugs_sweep_cli.get_sweep_cutoff_timestamp')
    def test_find_bugs_sweep_brew_event(self, *_):
        runner = CliRunner()
        bugs = [flexmock(id='BZ1', status='ON_QA')]
        flexmock(Runtime).should_receive("initialize").and_return(None)
        flexmock(Runtime).should_receive("get_major_minor").and_return(4, 6)

        # jira mocks
        flexmock(JIRABugTracker).should_receive("get_config").and_return({'target_release': ['4.6.z']})
        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        flexmock(JIRABugTracker).should_receive("search").and_return(bugs)
        flexmock(JIRABugTracker).should_receive("filter_bugs_by_cutoff_event").and_return([])

        # common mocks
        flexmock(sweep_cli).should_receive("get_assembly_bug_ids").and_return(set(), set())
        flexmock(Runtime).should_receive("get_default_advisories").and_return({})

        result = runner.invoke(cli, ['-g', 'openshift-4.6', '--assembly', '4.6.52', 'find-bugs:sweep'])
        if result.exit_code != 0:
            exc_type, exc_value, exc_traceback = result.exc_info
            t = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.fail(t)

    @patch('elliottlib.bzutil.JIRABugTracker.filter_attached_bugs')
    def test_find_bugs_sweep_advisory_jira(self, jira_filter_mock):
        runner = CliRunner()
        bugs = [
            flexmock(
                id='BZ1',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='whatever',
                sub_component='whatever',
            )
        ]
        advisory_id = 123

        # common mocks
        flexmock(Runtime).should_receive("initialize")
        flexmock(Runtime).should_receive("get_major_minor").and_return(4, 6)
        flexmock(sweep_cli).should_receive("get_assembly_bug_ids").and_return(set(), set())
        flexmock(Runtime).should_receive("get_default_advisories").and_return({})

        # jira mocks
        flexmock(JIRABugTracker).should_receive("get_config").and_return({'target_release': ['4.6.z']})
        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        flexmock(JIRABugTracker).should_receive("search").and_return(bugs)
        flexmock(JIRABugTracker).should_receive("attach_bugs").with_args(
            [b.id for b in bugs], advisory_id=advisory_id, noop=False, verbose=False
        )
        jira_filter_mock.return_value = []

        result = runner.invoke(cli, ['-g', 'openshift-4.6', 'find-bugs:sweep', '--add', str(advisory_id)])
        if result.exit_code != 0:
            exc_type, exc_value, exc_traceback = result.exc_info
            t = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.fail(t)

    @patch('elliottlib.bzutil.JIRABugTracker.filter_attached_bugs')
    def test_find_bugs_sweep_advisory_type(self, jira_filter_mock):
        runner = CliRunner()
        bugs = [flexmock(id='OCPBUGS-1')]

        # common mocks
        flexmock(Runtime).should_receive("initialize")
        flexmock(Runtime).should_receive("get_major_minor").and_return(4, 6)
        flexmock(sweep_cli).should_receive("get_assembly_bug_ids").and_return(set(), set())
        flexmock(Runtime).should_receive("get_default_advisories").and_return({'image': 123})
        flexmock(sweep_cli).should_receive("categorize_bugs_by_type").and_return({"image": set(bugs)}, [])

        # jira mocks
        flexmock(JIRABugTracker).should_receive("get_config").and_return({'target_release': ['4.6.z']})
        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        flexmock(JIRABugTracker).should_receive("search").and_return(bugs)
        flexmock(JIRABugTracker).should_receive("advisory_bug_ids").and_return([])
        jira_filter_mock.return_value = []
        flexmock(JIRABugTracker).should_receive("attach_bugs").with_args(
            [b.id for b in bugs], advisory_id=123, noop=False, verbose=False
        )

        result = runner.invoke(cli, ['-g', 'openshift-4.6', 'find-bugs:sweep', '--use-default-advisory', 'image'])
        if result.exit_code != 0:
            exc_type, exc_value, exc_traceback = result.exc_info
            t = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.fail(t)

    @patch('elliottlib.bzutil.JIRABugTracker.filter_attached_bugs')
    def test_find_bugs_sweep_default_advisories(self, jira_filter_mock):
        runner = CliRunner()
        image_bugs = [flexmock(id=1), flexmock(id=2)]
        rpm_bugs = [flexmock(id=3), flexmock(id=4)]
        extras_bugs = [flexmock(id=5), flexmock(id=6)]

        # common mocks
        flexmock(Runtime).should_receive("initialize").and_return(None)
        flexmock(Runtime).should_receive("get_major_minor").and_return(4, 6)
        flexmock(sweep_cli).should_receive("get_assembly_bug_ids").and_return(set(), set())
        flexmock(sweep_cli).should_receive("categorize_bugs_by_type").and_return(
            {
                "image": set(image_bugs),
                "rpm": set(rpm_bugs),
                "extras": set(extras_bugs),
            },
            [],
        )
        flexmock(Runtime).should_receive("get_default_advisories").and_return(
            {'image': 123, 'rpm': 123, 'extras': 123, 'metadata': 123}
        )

        # jira mocks
        flexmock(JIRABugTracker).should_receive("get_config").and_return({'target_release': ['4.6.z']})
        client = flexmock()
        flexmock(client).should_receive("fields").and_return([])
        flexmock(JIRABugTracker).should_receive("login").and_return(client)
        flexmock(JIRABugTracker).should_receive("search").and_return(image_bugs + rpm_bugs + extras_bugs)
        flexmock(JIRABugTracker).should_receive("attach_bugs").times(3).and_return()
        jira_filter_mock.return_value = []

        result = runner.invoke(cli, ['-g', 'openshift-4.6', 'find-bugs:sweep', '--into-default-advisories'])
        if result.exit_code != 0:
            exc_type, exc_value, exc_traceback = result.exc_info
            t = "\n".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.fail(t)


class TestCategorizeBugsByType(unittest.TestCase):
    def test_categorize_bugs_by_type(self):
        major_version = 4
        minor_version = 11
        advisory_id_map = {'image': 1, 'rpm': 2, 'extras': 3, 'microshift': 4}
        bugs = [
            flexmock(
                id='OCPBUGS-0',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_summary_suffix=lambda *_: True,
                whiteboard_component='foo',
                component='',
                summary='',
            ),
            flexmock(
                id='OCPBUGS-1',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_summary_suffix=lambda *_: True,
                whiteboard_component='bar',
                component='',
                summary='',
            ),
            flexmock(
                id='OCPBUGS-2',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_summary_suffix=lambda *_: True,
                whiteboard_component='buzz',
                component='',
                summary='',
            ),
            flexmock(
                id='OCPBUGS-3',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='',
                summary='',
            ),
            flexmock(
                id='OCPBUGS-4',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='MicroShift',
                summary='',
            ),
        ]
        builds_map = {
            'image': {bugs[2].whiteboard_component: None},
            'rpm': {bugs[1].whiteboard_component: None},
            'extras': {bugs[0].whiteboard_component: None},
            'microshift': dict(),
        }

        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[3]})
        for kind in advisory_id_map.keys():
            flexmock(errata).should_receive("get_advisory_nvrs").with_args(advisory_id_map[kind]).and_return(
                builds_map[kind]
            )
        expected = {
            'rpm': {bugs[1].id},
            'image': {bugs[2].id},
            'extras': {bugs[0].id, bugs[3].id},
            'metadata': set(),
            'microshift': {bugs[4].id},
        }

        queried, issues = categorize_bugs_by_type(
            bugs=bugs,
            advisory_id_map=advisory_id_map,
            permitted_bug_ids=4,
            major_version=major_version,
            minor_version=minor_version,
            operator_bundle_advisory="metadata",
        )
        self.assertEqual(issues, [])
        for adv in queried:
            actual = set()
            for bug in queried[adv]:
                actual.add(bug.id)
            self.assertEqual(actual, expected[adv])

    def test_raise_fake_trackers(self):
        bugs = [
            flexmock(id='OCPBUGS-5', is_tracker_bug=lambda: False, is_invalid_tracker_bug=lambda: True, component='')
        ]
        advisory_id_map = {'image': 1, 'rpm': 2, 'extras': 3, 'microshift': 4}
        major_version = 4
        minor_version = 11
        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[0]})
        with self.assertRaisesRegex(ElliottFatalError, 'look like CVE trackers'):
            categorize_bugs_by_type(
                bugs=bugs,
                advisory_id_map=advisory_id_map,
                permitted_bug_ids=4,
                major_version=major_version,
                minor_version=minor_version,
                operator_bundle_advisory="metadata",
            )


class TestGenAssemblyBugIDs(unittest.TestCase):
    @patch("elliottlib.cli.find_bugs_sweep_cli.assembly_issues_config")
    def test_gen_assembly_bug_ids_jira(self, assembly_issues_config: Mock):
        assembly_issues_config.return_value = flexmock(
            include=[{"id": 1}, {"id": 'OCPBUGS-2'}], exclude=[{"id": "2"}, {"id": 'OCPBUGS-3'}]
        )
        runtime = flexmock(get_releases_config=lambda: None, assembly='foo')
        expected = ({"OCPBUGS-2"}, {"OCPBUGS-3"})
        actual = get_assembly_bug_ids(runtime, 'jira')
        self.assertEqual(actual, expected)

    @patch("elliottlib.cli.find_bugs_sweep_cli.assembly_issues_config")
    def test_gen_assembly_bug_ids_bz(self, assembly_issues_config: Mock):
        assembly_issues_config.return_value = flexmock(
            include=[{"id": 1}, {"id": 'OCPBUGS-2'}], exclude=[{"id": "2"}, {"id": 'OCPBUGS-3'}]
        )
        runtime = flexmock(get_releases_config=lambda: None, assembly='foo')
        expected = ({1}, {"2"})
        actual = get_assembly_bug_ids(runtime, 'bugzilla')
        self.assertEqual(actual, expected)


class TestExtrasBugs(unittest.TestCase):
    def test_payload_bug(self):
        bugs = [flexmock(id='123', component='Payload Component', sub_component='Subcomponent')]
        self.assertEqual(len(extras_bugs(bugs)), 0)

    def test_extras_bug(self):
        bugs = [flexmock(id='123', component='Metering Operator', sub_component='Subcomponent')]
        self.assertEqual(len(extras_bugs(bugs)), 1)

    def test_subcomponent_bug(self):
        bugs = [flexmock(id='123', component='Networking', sub_component='SR-IOV')]
        self.assertEqual(len(extras_bugs(bugs)), 1)

    def test_nonsubcomponent_bug(self):
        bugs = [flexmock(id='123', component='Networking', sub_component='Not SR-IOV')]
        self.assertEqual(len(extras_bugs(bugs)), 0)


if __name__ == '__main__':
    unittest.main()
