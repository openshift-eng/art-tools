import unittest
from unittest.mock import MagicMock, Mock, PropertyMock, patch

from artcommonlib.jira_config import JIRA_DOMAIN_NAME
from elliottlib.cli.process_release_from_fbc_bugs_cli import process_bugs

PATCH_CREATE_TRACKER = "elliottlib.cli.process_release_from_fbc_bugs_cli._create_jira_tracker"
PATCH_GET_COMPONENT = "elliottlib.cli.process_release_from_fbc_bugs_cli.get_konflux_component_by_component"


class TestProcessReleaseFromFbcBugs(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = Mock()
        self.mock_runtime.image_metas.return_value = []

    def _make_jira_bug(self, key, is_vulnerability=False, cve_id=None, whiteboard_component=None, flaw_bug_ids=None):
        """Helper to create a mocked JIRABug."""
        bug = MagicMock()
        bug.id = key
        bug.is_type_vulnerability.return_value = is_vulnerability
        type(bug).cve_id = PropertyMock(return_value=cve_id)
        type(bug).whiteboard_component = PropertyMock(return_value=whiteboard_component)
        type(bug).corresponding_flaw_bug_ids = PropertyMock(return_value=flaw_bug_ids or [])
        return bug

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_regular_bugs_produce_rhba(self, mock_create_tracker, mock_get_component):
        """Non-vulnerability JIRAs should produce an RHBA advisory."""
        bug1 = self._make_jira_bug("OADP-1111")
        bug2 = self._make_jira_bug("OADP-2222")

        mock_tracker = Mock()
        mock_tracker.get_bug.side_effect = lambda k: {"OADP-1111": bug1, "OADP-2222": bug2}[k]
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["OADP-1111", "OADP-2222"])

        self.assertEqual(result.type, "RHBA")
        self.assertIsNone(result.cves)
        self.assertIsNotNone(result.issues)
        fixed_ids = [i.id for i in result.issues.fixed]
        self.assertIn("OADP-1111", fixed_ids)
        self.assertIn("OADP-2222", fixed_ids)
        for issue in result.issues.fixed:
            self.assertEqual(issue.source, JIRA_DOMAIN_NAME)
        mock_get_component.assert_not_called()

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_cve_bug_produces_rhsa(self, mock_create_tracker, mock_get_component):
        """A Vulnerability JIRA with CVE should produce RHSA with CVE associations and flaw bugs."""
        mock_get_component.return_value = "oadp-1-4-oadp-velero"

        cve_bug = self._make_jira_bug(
            "OADP-7223",
            is_vulnerability=True,
            cve_id="CVE-2025-12345",
            whiteboard_component="oadp-velero-container",
            flaw_bug_ids=[98765],
        )

        mock_tracker = Mock()
        mock_tracker.get_bug.return_value = cve_bug
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["OADP-7223"])

        self.assertEqual(result.type, "RHSA")

        # CVE associations
        self.assertIsNotNone(result.cves)
        self.assertEqual(len(result.cves), 1)
        self.assertEqual(result.cves[0].key, "CVE-2025-12345")
        self.assertEqual(result.cves[0].component, "oadp-1-4-oadp-velero")

        # Issues: should contain both the flaw bug (bugzilla) and JIRA
        self.assertIsNotNone(result.issues)
        fixed_by_source = {(i.id, i.source) for i in result.issues.fixed}
        self.assertIn(("98765", "bugzilla.redhat.com"), fixed_by_source)
        self.assertIn(("OADP-7223", JIRA_DOMAIN_NAME), fixed_by_source)

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_mixed_cve_and_regular_bugs(self, mock_create_tracker, mock_get_component):
        """A mix of CVE and regular bugs should produce RHSA."""
        mock_get_component.return_value = "oadp-1-4-oadp-velero"

        cve_bug = self._make_jira_bug(
            "OADP-7223",
            is_vulnerability=True,
            cve_id="CVE-2025-12345",
            whiteboard_component="oadp-velero-container",
            flaw_bug_ids=[98765],
        )
        regular_bug = self._make_jira_bug("OADP-6707")

        mock_tracker = Mock()
        mock_tracker.get_bug.side_effect = lambda k: {"OADP-7223": cve_bug, "OADP-6707": regular_bug}[k]
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["OADP-7223", "OADP-6707"])

        self.assertEqual(result.type, "RHSA")
        self.assertEqual(len(result.cves), 1)

        fixed_ids = {i.id for i in result.issues.fixed}
        self.assertIn("OADP-7223", fixed_ids)
        self.assertIn("OADP-6707", fixed_ids)
        self.assertIn("98765", fixed_ids)

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_cve_without_cve_id_skipped(self, mock_create_tracker, mock_get_component):
        """A Vulnerability JIRA without a CVE ID should be skipped for CVE association but still listed as fixed."""
        bug = self._make_jira_bug(
            "OADP-9999",
            is_vulnerability=True,
            cve_id=None,
            whiteboard_component="oadp-velero-container",
        )

        mock_tracker = Mock()
        mock_tracker.get_bug.return_value = bug
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["OADP-9999"])

        self.assertEqual(result.type, "RHBA")
        self.assertIsNone(result.cves)
        fixed_ids = [i.id for i in result.issues.fixed]
        self.assertIn("OADP-9999", fixed_ids)
        mock_get_component.assert_not_called()

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_cve_without_pscomponent_skipped(self, mock_create_tracker, mock_get_component):
        """A Vulnerability with CVE but no pscomponent should skip CVE association."""
        bug = self._make_jira_bug(
            "OADP-8888",
            is_vulnerability=True,
            cve_id="CVE-2025-99999",
            whiteboard_component=None,
        )

        mock_tracker = Mock()
        mock_tracker.get_bug.return_value = bug
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["OADP-8888"])

        self.assertEqual(result.type, "RHBA")
        self.assertIsNone(result.cves)
        mock_get_component.assert_not_called()

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_unmapped_component_skipped(self, mock_create_tracker, mock_get_component):
        """When pscomponent can't be mapped to a Konflux component, CVE association is skipped."""
        mock_get_component.return_value = None

        bug = self._make_jira_bug(
            "OADP-7777",
            is_vulnerability=True,
            cve_id="CVE-2025-11111",
            whiteboard_component="unknown-container",
        )

        mock_tracker = Mock()
        mock_tracker.get_bug.return_value = bug
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["OADP-7777"])

        self.assertEqual(result.type, "RHBA")
        self.assertIsNone(result.cves)

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_empty_jira_list(self, mock_create_tracker, mock_get_component):
        """Empty/whitespace JIRA IDs should produce RHBA with no issues."""
        mock_tracker = Mock()
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["", "  ", ""])

        self.assertEqual(result.type, "RHBA")
        self.assertIsNone(result.issues)
        self.assertIsNone(result.cves)

    @patch(PATCH_GET_COMPONENT)
    @patch(PATCH_CREATE_TRACKER)
    async def test_multiple_cves(self, mock_create_tracker, mock_get_component):
        """Multiple CVE bugs should all appear in the CVE associations list."""
        mock_get_component.side_effect = lambda _rt, comp: {
            "oadp-velero-container": "oadp-1-4-oadp-velero",
            "oadp-operator-container": "oadp-1-4-oadp-operator",
        }.get(comp)

        cve1 = self._make_jira_bug(
            "OADP-1001",
            is_vulnerability=True,
            cve_id="CVE-2025-00001",
            whiteboard_component="oadp-velero-container",
            flaw_bug_ids=[11111],
        )
        cve2 = self._make_jira_bug(
            "OADP-1002",
            is_vulnerability=True,
            cve_id="CVE-2025-00002",
            whiteboard_component="oadp-operator-container",
            flaw_bug_ids=[22222],
        )

        mock_tracker = Mock()
        mock_tracker.get_bug.side_effect = lambda k: {"OADP-1001": cve1, "OADP-1002": cve2}[k]
        mock_create_tracker.return_value = mock_tracker

        result = await process_bugs(self.mock_runtime, ["OADP-1001", "OADP-1002"])

        self.assertEqual(result.type, "RHSA")
        self.assertEqual(len(result.cves), 2)
        cve_keys = {c.key for c in result.cves}
        self.assertEqual(cve_keys, {"CVE-2025-00001", "CVE-2025-00002"})

        flaw_ids = {i.id for i in result.issues.fixed if i.source == "bugzilla.redhat.com"}
        self.assertEqual(flaw_ids, {"11111", "22222"})


if __name__ == "__main__":
    unittest.main()
