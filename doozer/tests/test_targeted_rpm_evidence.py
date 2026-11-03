import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from artcommonlib.model import Model
from doozerlib.targeted_rpm_evidence import TargetedRpmEvidence


def _bug(key: str, cve: str | None, status: str = "Verified", component: str = "kernel") -> SimpleNamespace:
    """
    Construct a Jira issue discovered by the release-scoped query.
    """
    return SimpleNamespace(id=key, cve_id=cve, status=status, whiteboard_component=component)


class TestTargetedRpmEvidence(TestCase):
    def setUp(self):
        """
        Create a basis with an already claimed issue and a supplied release trigger.
        """
        self.runtime = MagicMock()
        self.runtime.get_releases_config.return_value = Model(
            {
                "releases": {
                    "5.0.0": {
                        "assembly": {
                            "issues": {"include": [{"id": "OCPBUGS-OLD"}], "exclude": [{"id": "OCPBUGS-EXCLUDED"}]},
                        }
                    }
                }
            }
        )
        self.session = self.runtime.pooled_koji_client_session.return_value.__enter__.return_value
        self.session.getChangelogEntries.side_effect = lambda nvr: (
            [
                {"text": "fix CVE-2025-1000 OCPBUGS-100"},
            ]
            + ([{"text": "fix CVE-2026-2000 and OCPBUGS-200"}] if "-new." in nvr else [])
        )
        self.review = TargetedRpmEvidence(self.runtime, "5.0.0", ("OCPBUGS-TRIGGER",), ("CVE-2026-98432",))

    @patch("doozerlib.targeted_rpm_evidence.JIRABugTracker")
    def test_pin_explains_trigger_and_its_own_new_references(self, tracker_cls):
        """
        A kernel CVE trigger is distinct from the bugs newly mentioned by CRI-O.
        """
        tracker_cls.return_value.target_release.return_value = ["5.0.z"]
        tracker_cls.return_value._get_available_target_versions.return_value = {"5.0.z"}
        tracker_cls.return_value.search_bugs.return_value = [_bug("OCPBUGS-200", "CVE-2026-2000", component="cri-o")]
        reason = self.review.record("cri-o", 9, "cri-o-5.0.0-old.el9", "cri-o-5.0.0-new.el9")
        self.assertIn("triggered by OCPBUGS-TRIGGER, CVE-2026-98432", reason)
        self.assertIn("CVE-2026-2000", reason)
        self.assertIn("OCPBUGS-200", reason)
        self.assertNotIn("CVE-2025-1000", reason)
        self.assertNotIn("OCPBUGS-100", reason)
        self.assertEqual(self.review.records[0]["jira_matches"][0]["disposition"], "proposed_addition")
        query = tracker_cls.return_value.search_bugs.call_args.kwargs["custom_query"]
        self.assertIn("CVE-2026-2000", query)
        self.assertIn("OCPBUGS-200", query)

    @patch("doozerlib.targeted_rpm_evidence.JIRABugTracker")
    def test_kernel_claims_require_review_and_respect_existing_issues(self, tracker_cls):
        """
        Propose eligible kernel trackers while retaining included, claimed and excluded states.
        """
        tracker_cls.return_value.target_release.return_value = ["5.0.z"]
        tracker_cls.return_value._get_available_target_versions.return_value = {"5.0.z"}
        tracker_cls.return_value.search_bugs.return_value = [
            _bug("OCPBUGS-300", "CVE-2026-2000"),
            _bug("OCPBUGS-301", "CVE-2026-2000", status="New"),
            _bug("OCPBUGS-TRIGGER", "CVE-2026-2000"),
            _bug("OCPBUGS-OLD", "CVE-2026-2000"),
            _bug("OCPBUGS-EXCLUDED", "CVE-2026-2000"),
            _bug("OCPBUGS-OTHER", "CVE-2026-2000", component="openssl"),
            _bug("OCPBUGS-OTHER-RHEL", "CVE-2026-2000", component="openshift/ose-rhel-coreos-10"),
        ]
        self.review.record("kernel", 9, "kernel-5.14.0-old.el9_8", "kernel-5.14.0-new.el9_8")
        matches = {item["id"]: item["disposition"] for item in self.review.records[0]["jira_matches"]}
        self.assertEqual(
            matches,
            {
                "OCPBUGS-300": "proposed_addition",
                "OCPBUGS-301": "needs_review",
                "OCPBUGS-TRIGGER": "already_included",
                "OCPBUGS-OLD": "included_in_basis",
                "OCPBUGS-EXCLUDED": "excluded_in_basis",
            },
        )
        with TemporaryDirectory() as directory:
            self.review.write(Path(directory))
            markdown = (Path(directory) / "targeted-rpm-review.log").read_text()
            proposal = markdown.split("```yaml", 1)[1]
            self.assertIn("OCPBUGS-300", proposal)
            self.assertNotIn("OCPBUGS-301", proposal)
            self.assertNotIn("OCPBUGS-TRIGGER", proposal)
            details = json.loads((Path(directory) / "targeted-rpm-evidence.log").read_text())
            self.assertIn("fix CVE-2026-2000", details["records"][0]["changelog_evidence"][0])

    @patch("doozerlib.targeted_rpm_evidence.JIRABugTracker", side_effect=ValueError("Set JIRA_TOKEN"))
    def test_jira_failure_preserves_changelog_evidence_without_false_claim(self, tracker_cls):
        """
        Missing Jira access leaves CVEs discoverable and marks mapping incomplete.
        """
        reason = self.review.record("kernel", 9, "kernel-5.14.0-old.el9_8", "kernel-5.14.0-new.el9_8")
        self.assertIn("Issue evidence incomplete", reason)
        self.assertEqual(self.review.records[0]["new_cves"], ["CVE-2026-2000"])
        self.assertEqual(self.review.records[0]["jira_matches"], [])
        self.runtime.logger.warning.assert_called_once()

    def test_missing_basis_changelog_does_not_report_entire_history_as_new(self):
        """
        Unknown baseline evidence must not become an apparent list of newly fixed CVEs.
        """
        self.session.getChangelogEntries.return_value = None
        self.session.getChangelogEntries.side_effect = None
        self.review.record("kernel", 9, "kernel-5.14.0-old.el9_8", "kernel-5.14.0-new.el9_8")
        self.assertEqual(self.review.records[0]["new_cves"], [])
        self.assertTrue(self.review.warnings)

    def test_kernel_branch_change_does_not_claim_a_cve_delta(self):
        """
        Different RHEL minor branches cannot establish newly fixed kernel CVEs.
        """
        self.review.record("kernel", 9, "kernel-5.14.0-old.el9_7", "kernel-5.14.0-new.el9_8")
        self.assertTrue(self.review.warnings)
        self.assertEqual(self.review.records[0]["new_cves"], [])
        self.session.getChangelogEntries.assert_not_called()

    def test_changelog_only_existing_references_produces_no_proposals(self):
        """
        Unchanged historical references do not need another CVE claim.
        """
        self.review.record("kernel", 9, "kernel-5.14.0-old.el9_8", "kernel-5.14.0-another.el9_8")
        self.assertEqual(self.review.records[0]["new_cves"], [])
        self.assertEqual(self.review.records[0]["new_ocpbugs"], [])
        self.assertEqual(self.review.records[0]["jira_matches"], [])
