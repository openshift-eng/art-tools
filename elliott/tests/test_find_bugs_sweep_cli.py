import traceback
import unittest
from unittest.mock import MagicMock, Mock, patch

import elliottlib.cli.find_bugs_sweep_cli as sweep_cli
from click.testing import CliRunner
from elliottlib import errata
from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.common import Runtime, cli
from elliottlib.cli.find_bugs_sweep_cli import (
    FindBugsMode,
    categorize_bugs_by_type,
    extras_bugs,
    get_assembly_bug_ids,
    get_builds_by_advisory_kind,
)
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
        flexmock(sweep_cli).should_receive("get_builds_by_advisory_kind")
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
        flexmock(sweep_cli).should_receive("get_builds_by_advisory_kind")
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
        flexmock(sweep_cli).should_receive("get_builds_by_advisory_kind")
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
    def setUp(self):
        self.major_version = 4
        self.minor_version = 11
        self.runtime = MagicMock(get_default_advisories=MagicMock(return_value={"rpm": -1, "image": -1}))

    def test_categorize_no_trackers(self):
        bugs = [
            # extras bug
            flexmock(
                id='OCPBUGS-0',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='',
            ),
            # image bug
            flexmock(
                id='OCPBUGS-1',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='',
            ),
            # image bug
            flexmock(
                id='OCPBUGS-2',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='',
            ),
            # microshift bug
            flexmock(
                id='OCPBUGS-3',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='MicroShift',
            ),
        ]

        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[0]})

        expected = {
            'rpm': set(),
            'image': {bugs[1].id, bugs[2].id},
            'extras': {bugs[0].id},
            'metadata': set(),
            'microshift': {bugs[3].id},
        }

        bugs_by_kind, issues = categorize_bugs_by_type(
            runtime=self.runtime,
            bugs=bugs,
            builds_by_advisory_kind=None,
            major_version=self.major_version,
            minor_version=self.minor_version,
        )
        self.assertEqual(issues, [])
        for kind in expected:
            self.assertEqual(expected[kind], set(b.id for b in bugs_by_kind[kind]))

    def test_categorize_with_trackers_no_builds(self):
        bugs = [
            # valid tracker
            flexmock(
                id='OCPBUGS-0',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: True,
                whiteboard_component='foo',
                component='',
                summary='',
            ),
            # valid tracker
            flexmock(
                id='OCPBUGS-1',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: True,
                whiteboard_component='bar',
                component='',
                summary='',
            ),
            # valid tracker
            flexmock(
                id='OCPBUGS-2',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: True,
                whiteboard_component='buzz',
                component='',
                summary='',
            ),
            # valid extras non-tracker bug
            flexmock(
                id='OCPBUGS-3',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='',
                summary='',
            ),
            # valid microshift non-tracker bug
            flexmock(
                id='OCPBUGS-4',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='MicroShift',
                summary='',
            ),
        ]

        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[3]})

        expected = {
            'rpm': set(),
            'image': {bugs[0].id, bugs[1].id, bugs[2].id},
            'extras': {bugs[3].id},
            'metadata': set(),
            'microshift': {bugs[4].id},
        }

        bugs_by_kind, issues = categorize_bugs_by_type(
            runtime=self.runtime,
            bugs=bugs,
            builds_by_advisory_kind=None,
            major_version=self.major_version,
            minor_version=self.minor_version,
        )
        self.assertEqual(issues, [])
        for kind in expected:
            self.assertEqual(expected[kind], set(b.id for b in bugs_by_kind[kind]))

    def test_categorize_with_trackers_and_builds(self):
        bugs = [
            # valid extras tracker
            flexmock(
                id='OCPBUGS-0',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: True,
                whiteboard_component='foo',
                component='',
                summary='',
            ),
            # valid rpm tracker
            flexmock(
                id='OCPBUGS-1',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: True,
                whiteboard_component='bar',
                component='',
                summary='',
            ),
            # valid image tracker
            flexmock(
                id='OCPBUGS-2',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: True,
                whiteboard_component='buzz',
                component='',
                summary='',
            ),
            # valid extras non-tracker bug
            flexmock(
                id='OCPBUGS-3',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='',
                summary='',
            ),
            # valid microshift non-tracker bug
            flexmock(
                id='OCPBUGS-4',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='MicroShift',
                summary='',
            ),
        ]

        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[0], bugs[3]})

        builds_by_advisory_kind = {
            'image': {f"{bugs[2].whiteboard_component}-version-release.el9"},
            'rpm': {f"{bugs[1].whiteboard_component}-version-release.el9"},
            'extras': {f"{bugs[0].whiteboard_component}-version-release.el9"},
            'microshift': {},
        }

        expected = {
            'rpm': {bugs[1].id},
            'image': {bugs[2].id},
            'extras': {bugs[0].id, bugs[3].id},
            'metadata': set(),
            'microshift': {bugs[4].id},
        }

        bugs_by_kind, issues = categorize_bugs_by_type(
            runtime=self.runtime,
            bugs=bugs,
            builds_by_advisory_kind=builds_by_advisory_kind,
            major_version=self.major_version,
            minor_version=self.minor_version,
        )
        self.assertEqual(issues, [])
        for kind in expected:
            self.assertEqual(expected[kind], set(b.id for b in bugs_by_kind[kind]))

    def test_raise_fake_trackers(self):
        bugs = [
            flexmock(
                id='OCPBUGS-5',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: True,
                component='',
            ),
            flexmock(
                id='OCPBUGS-6',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: False,
                component='',
            ),
        ]
        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[0]})
        with self.assertRaisesRegex(ElliottFatalError, 'look like CVE trackers'):
            categorize_bugs_by_type(
                runtime=self.runtime,
                bugs=bugs,
                builds_by_advisory_kind=None,
                major_version=self.major_version,
                minor_version=self.minor_version,
            )

    def test_raise_tracker_bad_summary(self):
        bugs = [
            flexmock(
                id='OCPBUGS-5',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: False,
                whiteboard_component='foo',
                component='',
            )
        ]
        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[0]})
        with self.assertRaisesRegex(ElliottFatalError, 'invalid summary'):
            categorize_bugs_by_type(
                runtime=self.runtime,
                bugs=bugs,
                builds_by_advisory_kind=None,
                major_version=self.major_version,
                minor_version=self.minor_version,
            )

    def test_ignore_bad_trackers(self):
        bugs = [
            # valid tracker
            flexmock(
                id='OCPBUGS-2',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: True,
                whiteboard_component='buzz',
                component='',
                summary='',
            ),
            # bad summary tracker
            flexmock(
                id='OCPBUGS-4',
                is_tracker_bug=lambda: True,
                is_invalid_tracker_bug=lambda: False,
                has_valid_target_version_in_summary=lambda *_: False,
                whiteboard_component='foo',
                component='',
            ),
            # invalid tracker
            flexmock(
                id='OCPBUGS-5',
                is_tracker_bug=lambda: False,
                is_invalid_tracker_bug=lambda: True,
                component='',
            ),
        ]
        builds_by_advisory_kind = {
            'image': {},
            'rpm': {f"{bugs[1].whiteboard_component}-version-release.el9"},
            'extras': {f"{bugs[0].whiteboard_component}-version-release.el9"},
            'microshift': {},
        }

        flexmock(sweep_cli).should_receive("extras_bugs").and_return({bugs[0]})

        expected = {
            'image': set(),
            'rpm': set(),
            'extras': {bugs[0].id},
            'metadata': set(),
            'microshift': set(),
        }

        bugs_by_kind, issues = categorize_bugs_by_type(
            runtime=self.runtime,
            bugs=bugs,
            builds_by_advisory_kind=builds_by_advisory_kind,
            major_version=self.major_version,
            minor_version=self.minor_version,
            permissive=True,
        )
        for kind in expected:
            self.assertEqual(expected[kind], set(b.id for b in bugs_by_kind[kind]))

        self.assertEqual(
            issues,
            [
                "Bug(s) ['OCPBUGS-5'] look like CVE trackers, but really are not.",
                "Tracker Bug(s) ['OCPBUGS-4'] have invalid summary.",
            ],
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


class TestGetBuildsByAdvisoryKind(unittest.TestCase):
    def test_get_builds_brew_system(self):
        """Test get_builds_by_advisory_kind for brew build system"""
        runtime = flexmock(build_system='brew')
        runtime.should_receive("get_default_advisories").and_return(
            {'rpm': 12345, 'image': 12346, 'extras': 12347, 'metadata': 12348}
        )

        # Mock errata.get_advisory_nvrs_flattened for each advisory
        flexmock(errata).should_receive("get_advisory_nvrs_flattened").with_args(12345).and_return(
            ['rpm-package-1-1.0.0-1.el9']
        )
        flexmock(errata).should_receive("get_advisory_nvrs_flattened").with_args(12346).and_return(
            ['image1-container-v1.0.0-1', 'image2-container-v1.0.0-1']
        )
        flexmock(errata).should_receive("get_advisory_nvrs_flattened").with_args(12347).and_return(
            ['extras-package-1.0.0-1.el9']
        )
        flexmock(errata).should_receive("get_advisory_nvrs_flattened").with_args(12348).and_return(
            ['metadata-package-1.0.0-1.el9']
        )

        expected = {
            'rpm': ['rpm-package-1-1.0.0-1.el9'],
            'image': ['image1-container-v1.0.0-1', 'image2-container-v1.0.0-1'],
            'extras': ['extras-package-1.0.0-1.el9'],
            'metadata': ['metadata-package-1.0.0-1.el9'],
        }

        result = get_builds_by_advisory_kind(runtime)
        self.assertEqual(result, expected)

    def test_get_builds_brew_system_empty_advisories(self):
        """Test get_builds_by_advisory_kind for brew system with no advisories"""
        runtime = flexmock(build_system='brew')
        runtime.should_receive("get_default_advisories").and_return({})

        expected = {}
        result = get_builds_by_advisory_kind(runtime)
        self.assertEqual(result, expected)

    @patch('elliottlib.cli.find_bugs_sweep_cli.assembly_config_struct')
    @patch('elliottlib.cli.find_bugs_sweep_cli.get_builds_from_mr')
    def test_get_builds_konflux_system_with_shipment(self, mock_get_builds, mock_assembly_config):
        """Test get_builds_by_advisory_kind for konflux build system with shipment URL"""
        runtime = flexmock(build_system='konflux', assembly='4.16.0')
        runtime.should_receive("get_releases_config").and_return({})

        mock_assembly_config.return_value = {
            'shipment': {'url': 'https://gitlab.com/example/shipment/-/merge_requests/123'}
        }

        mock_get_builds.return_value = {
            'rpm': ['konflux-rpm-1.0.0-1.el9'],
            'image': ['konflux-image-container-v1.0.0-1'],
            'extras': ['konflux-extras-1.0.0-1.el9'],
        }

        expected = {
            'rpm': ['konflux-rpm-1.0.0-1.el9'],
            'image': ['konflux-image-container-v1.0.0-1'],
            'extras': ['konflux-extras-1.0.0-1.el9'],
        }

        result = get_builds_by_advisory_kind(runtime)
        self.assertEqual(result, expected)

        # Verify mocks were called correctly
        mock_assembly_config.assert_called_once_with({}, '4.16.0', "group", {})
        mock_get_builds.assert_called_once_with('https://gitlab.com/example/shipment/-/merge_requests/123')

    @patch('elliottlib.cli.find_bugs_sweep_cli.assembly_config_struct')
    @patch('elliottlib.cli.find_bugs_sweep_cli.get_builds_from_mr')
    def test_get_builds_konflux_system_no_shipment_url(self, mock_get_builds, mock_assembly_config):
        """Test get_builds_by_advisory_kind for konflux system with no shipment URL"""
        runtime = flexmock(build_system='konflux', assembly='4.16.0')
        runtime.should_receive("get_releases_config").and_return({})

        mock_assembly_config.return_value = {'shipment': {}}

        expected = {}
        result = get_builds_by_advisory_kind(runtime)
        self.assertEqual(result, expected)

        # Verify get_builds_from_mr was not called
        mock_get_builds.assert_not_called()


if __name__ == '__main__':
    unittest.main()
