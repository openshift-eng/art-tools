import logging
import unittest
import xmlrpc.client
from datetime import datetime, timezone
from unittest import mock

import requests
from artcommonlib.jira_config import JIRA_SERVER_URL, get_jira_browse_url
from elliottlib import bzutil, constants, exceptions
from elliottlib.bzutil import Bug, BugTracker, BugzillaBug, BugzillaBugTracker, JIRABug, JIRABugTracker
from flexmock import flexmock
from parameterized import parameterized

hostname = "bugzilla.redhat.com"


class TestBug(unittest.TestCase):
    def test_bug(self):
        bug_obj = flexmock(id=2)
        self.assertEqual(Bug(bug_obj).bug.id, bug_obj.id)

    def test_is_invalid_tracker_bug(self):
        bug_true = flexmock(id=1, summary="CVE-2022-0001", keywords=[], whiteboard_component=None)
        self.assertEqual(BugzillaBug(bug_true).is_invalid_tracker_bug(), True)

    @parameterized.expand(
        [
            ("Bug is fine [openshift-4.12]", (4, 12), "Bug is fine [openshift-4.12]"),
            ("Trailing .z [openshift-4.19.z]", (4, 19), "Trailing .z [openshift-4.19.z]"),
            ("Wrong in brackets [openshift-whatever]", (4, 11), "Wrong in brackets [openshift-4.11]"),
            ("Append here", (4, 13), "Append here [openshift-4.13]"),
            ("Wrong version [openshift-4.19]", (4, 20), "Wrong version [openshift-4.20]"),
        ]
    )
    def test_make_summary_with_target_version(self, summary, version, expected):
        major, minor = version
        bug = JIRABug(flexmock(fields=flexmock(summary=summary)))
        result = bug.make_summary_with_target_version(major, minor)
        self.assertEqual(result, expected)

    @parameterized.expand(
        [
            ("Some bug summary [openshift-whatever]", (4, 11), False),
            ("Bug is fine [openshift-4.12]", (4, 12), True),
            ("Different summary [openshift-4.15]", (4, 20), False),
            ("Append here", (4, 13), False),
            ("New style .z suffix [openshift-4.15.z]", (4, 15), True),
        ]
    )
    def test_has_valid_summary_suffix(self, summary, version, expected):
        major, minor = version
        bug = JIRABug(flexmock(fields=flexmock(summary=summary)))
        result = bug.has_valid_target_version_in_summary(major, minor)
        self.assertEqual(result, expected)


class TestBugTracker(unittest.TestCase):
    def test_get_corresponding_flaw_bugs(self):
        flaw_a = flexmock(id=1)
        flaw_b = flexmock(id=2)
        flaw_c = flexmock(id=3)
        valid_flaw_bugs = [flaw_a, flaw_b]

        tracker_bugs = [
            flexmock(corresponding_flaw_bug_ids=[flaw_a.id, flaw_b.id], id=10, whiteboard_component='component:foo'),
            flexmock(corresponding_flaw_bug_ids=[flaw_b.id, flaw_c.id], id=11, whiteboard_component='component:bar'),
            flexmock(corresponding_flaw_bug_ids=[flaw_b.id], id=12, whiteboard_component=None),
            flexmock(corresponding_flaw_bug_ids=[flaw_c.id], id=13, whiteboard_component='component:foobar'),
        ]

        flexmock(BugzillaBugTracker).should_receive("login")
        flexmock(BugzillaBugTracker).should_receive("get_flaw_bugs").and_return(valid_flaw_bugs)
        expected = (
            {10: [flaw_a.id, flaw_b.id], 11: [flaw_b.id]},
            {
                flaw_a.id: {'bug': flaw_a, 'trackers': [tracker_bugs[0]]},
                flaw_b.id: {'bug': flaw_b, 'trackers': [tracker_bugs[0], tracker_bugs[1]]},
            },
        )
        brew_api = flexmock()
        brew_api.should_receive("getPackageID").and_return(True)
        actual = BugTracker.get_corresponding_flaw_bugs(tracker_bugs, BugzillaBugTracker({}), strict=False)
        self.assertEqual(expected, actual)

    def test_get_corresponding_flaw_bugs_strict(self):
        flaw_a = flexmock(id=1)
        flaw_b = flexmock(id=2)
        flaw_c = flexmock(id=3)
        valid_flaw_bugs = [flaw_a, flaw_b]

        tracker_bugs = [
            flexmock(corresponding_flaw_bug_ids=[flaw_a.id, flaw_b.id], id=10, whiteboard_component='component:foo'),
            flexmock(corresponding_flaw_bug_ids=[flaw_b.id, flaw_c.id], id=11, whiteboard_component='component:bar'),
            flexmock(corresponding_flaw_bug_ids=[flaw_b.id], id=12, whiteboard_component=None),
            flexmock(corresponding_flaw_bug_ids=[flaw_c.id], id=13, whiteboard_component='component:foobar'),
        ]

        flexmock(BugzillaBugTracker).should_receive("login")
        flexmock(BugzillaBugTracker).should_receive("get_flaw_bugs").and_return(valid_flaw_bugs)

        brew_api = flexmock()
        brew_api.should_receive("getPackageID").and_return(True)
        self.assertRaises(
            exceptions.ElliottFatalError,
            BugTracker.get_corresponding_flaw_bugs,
            tracker_bugs,
            BugzillaBugTracker({}),
            strict=True,
        )


class TestJIRABugTracker(unittest.TestCase):
    def test_get_config(self):
        config = {'foo': 1, 'jira_config': {'bar': 2}}
        vars_mock = flexmock(MAJOR="4", MINOR="9")
        runtime = flexmock(
            group_config=flexmock(vars=vars_mock),
            gitdata=flexmock(),
        )
        runtime.gitdata.should_receive("load_data").with_args(key='bug', replace_vars=vars_mock).and_return(
            flexmock(data=config)
        )

        actual = JIRABugTracker.get_config(runtime)
        expected = {'foo': 1, 'bar': 2}
        self.assertEqual(actual, expected)

    def test_security_filtering_in_query(self):
        """Test that security filtering is included in JQL query when enabled"""
        # Create a minimal tracker for testing
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL}
        mock_jira_client = flexmock()
        flexmock(JIRABugTracker).should_receive("login").and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive("_init_fields")

        tracker = JIRABugTracker(config)

        # Test with security filtering enabled
        JIRABugTracker.ENABLE_SECURITY_LEVEL_FILTERING = True
        query = tracker._query(status=["NEW"], search_filter="default")
        self.assertIn(' and ("level" in', query)
        self.assertIn(' or "level" is EMPTY)', query)

        # Test with security filtering disabled
        JIRABugTracker.ENABLE_SECURITY_LEVEL_FILTERING = False
        query = tracker._query(status=["NEW"], search_filter="default")
        self.assertNotIn(' and ("level" in', query)
        self.assertNotIn(' or "level" is EMPTY)', query)

    def test_search_with_security_filtering(self):
        """Test that search results respect security filtering"""
        # Mock configuration
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL, 'token_auth': 'mock_token'}

        # Create mock issues with different security levels
        mock_security_allowed = flexmock(name="Red Hat Employee")
        mock_security_disallowed = flexmock(name="Special Public")

        mock_issue_allowed = flexmock(
            key='OCPBUGS-11111',
            fields=flexmock(
                summary='Allowed security bug',
                status=flexmock(name='NEW'),
                components=[flexmock(name='Test Component')],
                labels=['Security', 'SecurityTracking'],
                security=mock_security_allowed,
                project=flexmock(key='OCPBUGS'),
            ),
            permalink=lambda: get_jira_browse_url('OCPBUGS-11111'),
        )

        mock_issue_disallowed = flexmock(
            key='OCPBUGS-22222',
            fields=flexmock(
                summary='Disallowed security bug',
                status=flexmock(name='NEW'),
                components=[flexmock(name='Test Component')],
                labels=['Security', 'SecurityTracking'],
                security=mock_security_disallowed,
                project=flexmock(key='OCPBUGS'),
            ),
            permalink=lambda: get_jira_browse_url('OCPBUGS-22222'),
        )

        # Mock JIRA client search - should only return allowed bugs when filtering is enabled
        mock_jira_client = flexmock()

        # When security filtering is enabled, only return the allowed bug
        JIRABugTracker.ENABLE_SECURITY_LEVEL_FILTERING = True
        mock_jira_client.should_receive("search_issues").and_return([mock_issue_allowed])

        # Mock the login method
        flexmock(JIRABugTracker).should_receive("login").and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive("_init_fields")

        # Create tracker and perform search
        tracker = JIRABugTracker(config)
        bugs = tracker.search(['NEW'], 'default')

        # Verify we only got the allowed bug
        self.assertEqual(len(bugs), 1)
        self.assertEqual(bugs[0].id, 'OCPBUGS-11111')
        self.assertEqual(bugs[0].security_level.name, 'Red Hat Employee')
        self.assertIn(bugs[0].security_level.name, constants.JIRA_SECURITY_ALLOWLIST)

        # Test with security filtering disabled - should return both bugs
        JIRABugTracker.ENABLE_SECURITY_LEVEL_FILTERING = False
        mock_jira_client.should_receive("search_issues").and_return([mock_issue_allowed, mock_issue_disallowed])

        bugs = tracker.search(['NEW'], 'default')

        # Verify we got both bugs when filtering is disabled
        self.assertEqual(len(bugs), 2)
        bug_ids = [bug.id for bug in bugs]
        self.assertIn('OCPBUGS-11111', bug_ids)
        self.assertIn('OCPBUGS-22222', bug_ids)

    def test_get_available_target_versions(self):
        """Test fetching available target versions from JIRA"""
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL}

        # Mock issue types response (JIRA returns objects, not dicts)
        bug_type = flexmock(id='1', name='Bug')
        story_type = flexmock(id='2', name='Story')
        mock_issue_types = [bug_type, story_type]

        # Mock fields response with Target Version field
        version1 = flexmock(name='4.17.0')
        version2 = flexmock(name='4.17.z')
        version3 = flexmock(name='4.18.0')
        version4 = flexmock(name='4.18.z')

        field = flexmock(fieldId='customfield_12319940', allowedValues=[version1, version2, version3, version4])
        mock_fields = [field]

        mock_jira_client = flexmock()
        mock_jira_client.should_receive('project_issue_types').with_args('OCPBUGS').and_return(mock_issue_types)
        mock_jira_client.should_receive('project_issue_fields').with_args('OCPBUGS', '1').and_return(mock_fields)

        flexmock(JIRABugTracker).should_receive('login').and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive('_init_fields')

        tracker = JIRABugTracker(config)
        available_versions = tracker._get_available_target_versions()

        self.assertEqual(len(available_versions), 4)
        self.assertIn('4.17.0', available_versions)
        self.assertIn('4.17.z', available_versions)
        self.assertIn('4.18.0', available_versions)
        self.assertIn('4.18.z', available_versions)

        # Test caching - should not call API methods again
        available_versions_2 = tracker._get_available_target_versions()
        self.assertEqual(available_versions, available_versions_2)

    def test_get_available_target_versions_error_handling(self):
        """Test error handling when fetching target versions fails"""
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL}

        mock_jira_client = flexmock()
        mock_jira_client.should_receive('project_issue_types').and_raise(Exception('API error'))

        flexmock(JIRABugTracker).should_receive('login').and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive('_init_fields')

        tracker = JIRABugTracker(config)
        available_versions = tracker._get_available_target_versions()

        # Should return empty list on error
        self.assertEqual(available_versions, [])

    def test_query_with_valid_target_versions(self):
        """Test _query filters target versions correctly when all are valid"""
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL, 'target_release': ['4.17.0', '4.17.z']}

        mock_jira_client = flexmock()
        flexmock(JIRABugTracker).should_receive('login').and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive('_init_fields')

        tracker = JIRABugTracker(config)

        # Mock available versions
        flexmock(tracker).should_receive('_get_available_target_versions').and_return(['4.17.0', '4.17.z', '4.18.0'])

        query = tracker._query(status=['NEW'], target_release=['4.17.0', '4.17.z'], with_target_release=False)

        # Both versions should be included in the query
        self.assertIn('4.17.0', query)
        self.assertIn('4.17.z', query)
        self.assertIn('"Target Version" in', query)

    def test_query_with_invalid_target_versions(self):
        """Test _query filters out non-existent target versions"""
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL, 'target_release': ['4.17.0', '4.20.0']}

        mock_jira_client = flexmock()
        flexmock(JIRABugTracker).should_receive('login').and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive('_init_fields')

        tracker = JIRABugTracker(config)

        # Mock available versions - 4.20.0 does not exist
        flexmock(tracker).should_receive('_get_available_target_versions').and_return(['4.17.0', '4.17.z', '4.18.0'])

        query = tracker._query(status=['NEW'], target_release=['4.17.0', '4.20.0'], with_target_release=False)

        # Only 4.17.0 should be included, 4.20.0 should be filtered out
        self.assertIn('4.17.0', query)
        self.assertNotIn('4.20.0', query)
        self.assertIn('"Target Version" in', query)

    def test_query_with_all_invalid_target_versions(self):
        """Test _query behavior when all target versions are invalid"""
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL, 'target_release': ['4.20.0', '4.21.0']}

        mock_jira_client = flexmock()
        flexmock(JIRABugTracker).should_receive('login').and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive('_init_fields')

        tracker = JIRABugTracker(config)

        # Mock available versions - neither 4.20.0 nor 4.21.0 exist
        flexmock(tracker).should_receive('_get_available_target_versions').and_return(['4.17.0', '4.17.z', '4.18.0'])

        query = tracker._query(status=['NEW'], target_release=['4.20.0', '4.21.0'], with_target_release=False)

        # Should return None when all target versions are filtered out
        self.assertIsNone(query)

    def test_query_when_available_versions_fetch_fails(self):
        """Test _query proceeds with original query when fetching available versions fails"""
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL, 'target_release': ['4.17.0']}

        mock_jira_client = flexmock()
        flexmock(JIRABugTracker).should_receive('login').and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive('_init_fields')

        tracker = JIRABugTracker(config)

        # Mock available versions returning empty list (error case)
        flexmock(tracker).should_receive('_get_available_target_versions').and_return([])

        query = tracker._query(status=['NEW'], target_release=['4.17.0'], with_target_release=False)

        # Should proceed with original query including the target version
        self.assertIn('4.17.0', query)
        self.assertIn('"Target Version" in', query)

    def test_search_returns_empty_when_all_versions_filtered(self):
        """Test that search methods return empty list when all target versions are filtered out"""
        config = {'project': 'OCPBUGS', 'server': JIRA_SERVER_URL, 'target_release': ['4.99.0']}

        mock_jira_client = flexmock()
        flexmock(JIRABugTracker).should_receive('login').and_return(mock_jira_client)
        flexmock(JIRABugTracker).should_receive('_init_fields')

        tracker = JIRABugTracker(config)

        # Mock available versions - 4.99.0 doesn't exist
        flexmock(tracker).should_receive('_get_available_target_versions').and_return(['4.17.0', '4.17.z'])

        # Test search method
        result = tracker.search(['NEW'])
        self.assertEqual(result, [])

        # Test blocker_search method
        result = tracker.blocker_search(['NEW'])
        self.assertEqual(result, [])

        # Test cve_tracker_search method
        result = tracker.cve_tracker_search(['NEW'])
        self.assertEqual(result, [])


class TestBugzillaBugTracker(unittest.TestCase):
    def test_get_config(self):
        config = {'foo': 1, 'bugzilla_config': {'bar': 2}}
        vars_mock = flexmock(MAJOR="4", MINOR="9")
        runtime = flexmock(
            group_config=flexmock(vars=vars_mock),
            gitdata=flexmock(),
        )
        runtime.gitdata.should_receive("load_data").with_args(key='bug', replace_vars=vars_mock).and_return(
            flexmock(data=config)
        )

        actual = BugzillaBugTracker.get_config(runtime)
        expected = {'foo': 1, 'bar': 2}
        self.assertEqual(actual, expected)


class TestJIRABug(unittest.TestCase):
    def test_depends_on(self):
        bug = flexmock(key='OCPBUGS-1')
        flexmock(JIRABug).should_receive("_get_depends").and_return(['foo'])
        self.assertEqual(JIRABug(bug).depends_on, ['foo'])

    def test_is_placeholder_bug(self):
        bug1 = flexmock(
            key='OCPBUGS-1',
            fields=flexmock(summary='Placeholder', components=[flexmock(name='Release')], labels=['Automation']),
        )
        self.assertEqual(JIRABug(bug1).is_placeholder_bug(), True)

        bug2 = flexmock(
            key='OCPBUGS-2', fields=flexmock(summary='Placeholder', components=[flexmock(name='Foo')], labels=['Bar'])
        )
        self.assertEqual(JIRABug(bug2).is_placeholder_bug(), False)

    def test_is_ocp_bug(self):
        bug1 = flexmock(key='OCPBUGS-1', fields=flexmock(project=flexmock(key='foo')))
        self.assertEqual(JIRABug(bug1).is_ocp_bug(), False)

        bug2 = flexmock(key='OCPBUGS-1', fields=flexmock(project=flexmock(key='OCPBUGS')))
        flexmock(JIRABug).should_receive("is_placeholder_bug").and_return(True)
        self.assertEqual(JIRABug(bug2).is_ocp_bug(), False)

        bug2 = flexmock(key='OCPBUGS-1', fields=flexmock(project=flexmock(key='OCPBUGS')))
        flexmock(JIRABug).should_receive("is_placeholder_bug").and_return(False)
        self.assertEqual(JIRABug(bug2).is_ocp_bug(), True)

    def test_is_tracker_bug(self):
        bug = flexmock(
            key='OCPBUGS1',
            fields=flexmock(
                labels=constants.TRACKER_BUG_KEYWORDS + ['somethingelse', 'pscomponent:my-image', 'flaw:bz#123'],
                issuetype=flexmock(name='Bug'),
            ),
        )
        expected = True
        actual = JIRABug(bug).is_tracker_bug()
        self.assertEqual(expected, actual)

    def test_is_tracker_bug_missing_keywords(self):
        bug = flexmock(
            key='OCPBUGS1',
            fields=flexmock(
                labels=['somethingelse', 'pscomponent:my-image', 'flaw:bz#123'], issuetype=flexmock(name='Bug')
            ),
        )
        expected = False
        actual = JIRABug(bug).is_tracker_bug()
        self.assertEqual(expected, actual)

    def test_is_tracker_bug_missing_pscomponent(self):
        bug = flexmock(
            key='OCPBUGS1',
            fields=flexmock(
                labels=constants.TRACKER_BUG_KEYWORDS + ['somethingelse', 'flaw:bz#123'], issuetype=flexmock(name='Bug')
            ),
        )
        expected = False
        actual = JIRABug(bug).is_tracker_bug()
        self.assertEqual(expected, actual)

    def test_is_tracker_bug_missing_flaw(self):
        bug = flexmock(
            key='OCPBUGS1',
            fields=flexmock(
                labels=constants.TRACKER_BUG_KEYWORDS + ['somethingelse', 'pscomponent:my-image'],
                issuetype=flexmock(name='Bug'),
            ),
        )
        expected = False
        actual = JIRABug(bug).is_tracker_bug()
        self.assertEqual(expected, actual)

    def test_component_sub_component(self):
        bug = JIRABug(
            flexmock(key="OCPBUGS-43", fields=flexmock(components=[flexmock(name="foo / bar")])),
        )
        actual = (bug.component, bug.sub_component)
        expected = ("foo", "bar")
        self.assertEqual(actual, expected)

    def test_component_sub_component_no_whitespace(self):
        bug = JIRABug(
            flexmock(key="OCPBUGS-43", fields=flexmock(components=[flexmock(name="foo/bar")])),
        )
        actual = (bug.component, bug.sub_component)
        expected = ("foo", "bar")
        self.assertEqual(actual, expected)

    def test_corresponding_flaw_bug_ids(self):
        bug = JIRABug(
            flexmock(key="OCPBUGS-43", fields=flexmock(labels=["foo", "flaw:123", "flaw:bz#456"])),
        )
        actual = bug.corresponding_flaw_bug_ids
        expected = [456]
        self.assertEqual(actual, expected)

    def test_whiteboard_component(self):
        bug = JIRABug(flexmock(key=1, fields=flexmock(labels=["foo"], issuetype=flexmock(name='Bug'))))
        self.assertIsNone(bug.whiteboard_component)

        bug = JIRABug(flexmock(key=1, fields=flexmock(labels=["pscomponent: "], issuetype=flexmock(name='Bug'))))
        self.assertIsNone(bug.whiteboard_component)

        for expected in ["something", "openvswitch2.15", "trailing_blank 	"]:
            bug = JIRABug(
                flexmock(key=1, fields=flexmock(labels=[f"pscomponent: {expected}"], issuetype=flexmock(name='Bug')))
            )
            actual = bug.whiteboard_component
            self.assertEqual(actual, expected.strip())


class TestBugzillaBug(unittest.TestCase):
    def test_is_tracker_bug(self):
        bug = flexmock(id='1', keywords=constants.TRACKER_BUG_KEYWORDS, whiteboard_component='my-image')
        expected = True
        actual = BugzillaBug(bug).is_tracker_bug()
        self.assertEqual(expected, actual)

    def test_is_tracker_bug_fail(self):
        bug = flexmock(id='1', keywords=['SomeOtherKeyword'], whiteboard_component='my-image')
        expected = False
        actual = BugzillaBug(bug).is_tracker_bug()
        self.assertEqual(expected, actual)

    def test_whiteboard_component(self):
        bug = BugzillaBug(flexmock(id=1, whiteboard="foo"))
        self.assertIsNone(bug.whiteboard_component)

        bug = BugzillaBug(flexmock(id=2, whiteboard="component: "))
        self.assertIsNone(bug.whiteboard_component)

        for expected in ["something", "openvswitch2.15", "trailing_blank 	"]:
            bug = BugzillaBug(flexmock(id=2, whiteboard=f"component: {expected}"))
            actual = bug.whiteboard_component
            self.assertEqual(actual, expected.strip())

    def test_filter_bugs_by_cutoff_event(self):
        bzapi = mock.MagicMock()
        with mock.patch("elliottlib.bzutil.BugzillaBugTracker.login") as mock_login:
            mock_login.return_value = bzapi
            bug_tracker = bzutil.BugzillaBugTracker({})
        desired_statuses = ["MODIFIED", "ON_QA", "VERIFIED"]
        sweep_cutoff_timestamp = datetime(2021, 6, 30, 12, 30, 00, 0, tzinfo=timezone.utc).timestamp()
        bugs = [
            mock.MagicMock(id=1, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T12:29:00")),
            mock.MagicMock(id=2, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T12:30:00")),
            mock.MagicMock(id=3, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T12:31:00")),
            mock.MagicMock(id=4, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T00:00:00")),
            mock.MagicMock(id=5, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T00:00:00")),
            mock.MagicMock(id=6, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T00:00:00")),
            mock.MagicMock(id=7, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T00:00:00")),
            mock.MagicMock(id=8, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T00:00:00")),
            mock.MagicMock(id=9, status="ON_QA", creation_time=xmlrpc.client.DateTime("20210630T00:00:00")),
        ]
        bzapi.bugs_history_raw.return_value = {
            "bugs": [
                {
                    "id": 1,
                    "history": [],
                },
                {
                    "id": 2,
                    "history": [],
                },
                {
                    "id": 4,
                    "history": [
                        {
                            "when": xmlrpc.client.DateTime("20210630T01:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T23:59:59"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                            ],
                        },
                    ],
                },
                {
                    "id": 5,
                    "history": [
                        {
                            "when": xmlrpc.client.DateTime("20210630T01:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "NEW", "added": "MODIFIED"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T23:59:59"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "MODIFIED", "added": "ON_QA"},
                            ],
                        },
                    ],
                },
                {
                    "id": 6,
                    "history": [
                        {
                            "when": xmlrpc.client.DateTime("20210630T01:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "NEW", "added": "ASSIGNED"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T23:59:59"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "ASSIGNED", "added": "ON_QA"},
                            ],
                        },
                    ],
                },
                {
                    "id": 7,
                    "history": [
                        {
                            "when": xmlrpc.client.DateTime("20210630T01:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "NEW", "added": "MODIFIED"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T23:59:59"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "MODIFIED", "added": "ON_QA"},
                            ],
                        },
                    ],
                },
                {
                    "id": 8,
                    "history": [
                        {
                            "when": xmlrpc.client.DateTime("20210630T01:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "NEW", "added": "MODIFIED"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T13:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "MODIFIED", "added": "ON_QA"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T23:59:59"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "ON_QA", "added": "VERIFIED"},
                            ],
                        },
                    ],
                },
                {
                    "id": 9,
                    "history": [
                        {
                            "when": xmlrpc.client.DateTime("20210630T01:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "NEW", "added": "MODIFIED"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T13:00:00"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "MODIFIED", "added": "ON_QA"},
                            ],
                        },
                        {
                            "when": xmlrpc.client.DateTime("20210630T23:59:59"),
                            "changes": [
                                {"field_name": "irelevant1", "removed": "foo", "added": "bar"},
                                {"field_name": "irelevant2", "removed": "bar", "added": "foo"},
                                {"field_name": "status", "removed": "ON_QA", "added": "ASSIGNED"},
                            ],
                        },
                    ],
                },
            ],
        }
        actual = bug_tracker.filter_bugs_by_cutoff_event(bugs, desired_statuses, sweep_cutoff_timestamp)
        self.assertListEqual([1, 2, 4, 5, 7, 8], [bug.id for bug in actual])


class TestBZUtil(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_is_viable_bug(self):
        bug = mock.MagicMock()
        bug.status = "VERIFIED"
        self.assertTrue(bzutil.is_viable_bug(bug))
        bug.status = "MODIFIED"
        self.assertFalse(bzutil.is_viable_bug(bug))

    def test_to_timestamp(self):
        dt = xmlrpc.client.DateTime("20210615T18:23:22")
        actual = bzutil.to_timestamp(dt)
        self.assertEqual(actual, 1623781402.0)

    async def test_approximate_cutoff_timestamp(self):
        koji_api = mock.MagicMock()
        koji_api.getEvent.return_value = {"ts": datetime(2021, 7, 3, 0, 0, 0, 0, tzinfo=timezone.utc).timestamp()}
        metas = [
            mock.MagicMock(),
            mock.MagicMock(),
            mock.MagicMock(),
        ]
        metas[0].get_latest_build = mock.AsyncMock(return_value={"nvr": "a-4.9.0-202107020000.p0"})
        metas[1].get_latest_build = mock.AsyncMock(return_value={"nvr": "b-4.9.0-202107020100.p0"})
        metas[2].get_latest_build = mock.AsyncMock(return_value={"nvr": "c-4.9.0-202107020200.p0"})
        actual = await bzutil.approximate_cutoff_timestamp(mock.ANY, koji_api, metas)
        self.assertEqual(datetime(2021, 7, 2, 2, 0, 0, 0, tzinfo=timezone.utc).timestamp(), actual)

        koji_api.getEvent.return_value = {"ts": datetime(2021, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc).timestamp()}
        actual = await bzutil.approximate_cutoff_timestamp(mock.ANY, koji_api, metas)
        self.assertEqual(datetime(2021, 7, 1, 0, 0, 0, 0, tzinfo=timezone.utc).timestamp(), actual)

        koji_api.getEvent.return_value = {"ts": datetime(2021, 7, 4, 0, 0, 0, 0, tzinfo=timezone.utc).timestamp()}
        actual = await bzutil.approximate_cutoff_timestamp(mock.ANY, koji_api, [])
        self.assertEqual(datetime(2021, 7, 4, 0, 0, 0, 0, tzinfo=timezone.utc).timestamp(), actual)

    def test_sort_cve_bugs(self):
        flaw_bugs = [
            flexmock(alias=['CVE-2022-123'], severity='Low'),
            flexmock(alias=['CVE-2022-9'], severity='urgent'),
            flexmock(alias=['CVE-2022-10'], severity='urgent'),
            flexmock(alias=['CVE-2021-789'], severity='medium'),
            flexmock(alias=['CVE-2021-100'], severity='medium'),
        ]
        sort_list = [b.alias[0] for b in bzutil.sort_cve_bugs(flaw_bugs)]

        self.assertEqual('CVE-2022-9', sort_list[0])
        self.assertEqual('CVE-2022-10', sort_list[1])
        self.assertEqual('CVE-2021-100', sort_list[2])
        self.assertEqual('CVE-2021-789', sort_list[3])
        self.assertEqual('CVE-2022-123', sort_list[4])

    def test_is_first_fix_any_validate(self):
        # Mock runtime object
        mock_runtime = flexmock()
        mock_runtime.should_receive('get_major_minor').and_return((4, 8))

        # should raise error when no tracker bugs are found
        self.assertRaisesRegex(
            ValueError,
            r'does not seem to have trackers',
            bzutil.is_first_fix_any,
            mock_runtime,
            BugzillaBug(flexmock(id=1)),
            [],
        )

        # should raise error when flaw alias isn't present
        self.assertRaisesRegex(
            ValueError,
            r'does not have a CVE alias',
            bzutil.is_first_fix_any,
            mock_runtime,
            BugzillaBug(flexmock(id=1)),
            [JIRABug(flexmock(key="OCPBUGS-foo"))],
        )

    def test_is_first_fix_any_image_delivery_repo(self):
        # Mock runtime object
        mock_runtime = flexmock()
        mock_runtime.should_receive('get_major_minor').and_return((4, 8))

        # Mock image meta for openshift4/some-image delivery repo matching
        mock_meta = flexmock()
        mock_config = flexmock()
        mock_delivery = flexmock()
        mock_delivery.delivery_repo_names = ['openshift4/some-image']
        mock_config.delivery = mock_delivery
        mock_meta.config = mock_config
        mock_meta.should_receive('get_component_name').and_return('some-image-component')

        mock_runtime.should_receive('image_metas').and_return([mock_meta])

        hydra_data = {
            'package_state': [
                {
                    'product_name': "Red Hat OpenShift Container Platform 4",
                    'fix_state': "Some other status",
                    'package_name': "openshift4/some-image",
                },
            ],
        }
        flexmock(requests).should_receive('get').and_return(
            flexmock(json=lambda: hydra_data, raise_for_status=lambda: None)
        )

        flaw_bug = BugzillaBug(flexmock(id=1, alias=['CVE-123']))
        tracker_bugs = [flexmock(id=2, whiteboard_component='some-image-component')]
        expected = True
        actual = bzutil.is_first_fix_any(mock_runtime, flaw_bug, tracker_bugs)
        self.assertEqual(expected, actual)

    def test_is_first_fix_any_component_name(self):
        # Mock runtime object
        mock_runtime = flexmock()
        mock_runtime.should_receive('get_major_minor').and_return((4, 8))
        mock_runtime.should_receive('image_metas').and_return([])

        hydra_data = {
            'package_state': [
                {
                    'product_name': "Red Hat OpenShift Container Platform 4",
                    'fix_state': "Some other status",
                    'package_name': "some-component-name",
                },
            ],
        }
        flexmock(requests).should_receive('get').and_return(
            flexmock(json=lambda: hydra_data, raise_for_status=lambda: None)
        )

        flaw_bug = BugzillaBug(flexmock(id=1, alias=['CVE-123']))
        tracker_bugs = [flexmock(id=2, whiteboard_component='some-component-name')]
        expected = True
        actual = bzutil.is_first_fix_any(mock_runtime, flaw_bug, tracker_bugs)
        self.assertEqual(expected, actual)

    def test_is_first_fix_any_false(self):
        # Mock runtime object
        mock_runtime = flexmock()
        mock_runtime.should_receive('get_major_minor').and_return((4, 8))
        mock_runtime.should_receive('image_metas').and_return([])

        hydra_data = {
            'package_state': [
                {
                    'product_name': "Red Hat OpenShift Container Platform 4",
                    'fix_state': "Affected",
                    'package_name': "unrelated-component",
                },
                # will not be considered since it's not OCP 4
                {
                    'product_name': "Red Hat OpenShift Container Platform 3",
                    'fix_state': "Affected",
                    'package_name': "some-component-name",
                },
            ],
        }
        flexmock(requests).should_receive('get').and_return(
            flexmock(json=lambda: hydra_data, raise_for_status=lambda: None)
        )

        flaw_bug = BugzillaBug(flexmock(id=1, alias=['CVE-123']))
        tracker_bugs = [flexmock(id=2, whiteboard_component='some-component-name')]
        expected = False
        actual = bzutil.is_first_fix_any(mock_runtime, flaw_bug, tracker_bugs)
        self.assertEqual(expected, actual)


class TestSearchFilter(unittest.TestCase):
    def test_search_filter(self):
        """Verify the bugzilla SearchFilter works as expected"""
        field_name = "component"
        operator = "notequals"
        value = "RFE"
        expected = "&f1=component&o1=notequals&v1=RFE"

        sf = bzutil.SearchFilter(field_name, operator, value)
        self.assertEqual(sf.tostring(1), expected)


class TestGetHigestImpact(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_lowest_to_highest_impact(self):
        trackers = [
            flexmock(id=index, severity=severity)
            for index, severity in enumerate(constants.BUG_SEVERITY_NUMBER_MAP.keys())
        ]
        tracker_flaws_map = {tracker.id: [] for tracker in trackers}
        impact = bzutil.get_highest_impact(trackers, tracker_flaws_map)
        self.assertEqual(impact, constants.SECURITY_IMPACT[4])

    def test_single_impact(self):
        bugs = []
        severity = "high"
        bugs.append(flexmock(severity=severity))
        impact = bzutil.get_highest_impact(bugs, None)
        self.assertEqual(impact, constants.SECURITY_IMPACT[constants.BUG_SEVERITY_NUMBER_MAP[severity]])

    def test_impact_for_tracker_with_unspecified_severity(self):
        bugs = []
        severity = "unspecified"
        bugs.append(flexmock(id=123, severity=severity))
        tracker_flaws_map = {
            123: [flexmock(id=123, severity="medium")],
        }
        impact = bzutil.get_highest_impact(bugs, tracker_flaws_map)
        self.assertEqual(impact, "Moderate")
        tracker_flaws_map = {
            123: [flexmock(id=123, severity="unspecified")],
        }
        impact = bzutil.get_highest_impact(bugs, tracker_flaws_map)
        self.assertEqual(impact, "Low")


if __name__ == "__main__":
    unittest.main()
