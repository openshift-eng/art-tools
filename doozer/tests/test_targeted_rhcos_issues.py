import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from artcommonlib.model import Model
from doozerlib.targeted_rhcos_issues import requirements_installed, verified_rpm_requirements
from doozerlib.targeted_rpm_evidence import TargetedRpmEvidence


def _comment(text: str, created: str = "2026-11-01") -> SimpleNamespace:
    """
    Construct a Jira comment containing RHCOS CVE verification evidence.
    """
    return SimpleNamespace(body=text, created=created)


class TestRhcosIssueDiscovery(TestCase):
    def setUp(self):
        """
        Use selected RHEL 9 and 10 images with kernel and Intel microcode RPMs.
        """
        self.entries = [
            ["kernel-core", "0", "5.14.0", "687.65.1.el9_8", "x86_64", "rhel-coreos"],
            ["microcode_ctl", "4", "20260210", "1.20260812.1.el9_8", "noarch", "rhel-coreos"],
            ["kernel-core", "0", "6.12.0", "211.73.1.el10_2", "x86_64", "rhel-coreos-10"],
        ]
        self.runtime = MagicMock()
        self.runtime.get_releases_config.return_value = Model({"releases": {"5.0.0": {"assembly": {}}}})
        self.review = TargetedRpmEvidence(self.runtime, "5.0.0", ("OCPBUGS-135021",), ())
        self.review.add_inventory("x86_64", self.entries)

    @patch("doozerlib.targeted_rpm_evidence.JIRABugTracker")
    def test_discovers_all_seven_verified_fixes_without_changelog_delta(self, tracker_cls):
        """
        Discover the 5.0.1 examples, including microcode and a manually verified kernel fix.
        """
        pairs = {
            "OCPBUGS-128369": "CVE-2026-89777",
            "OCPBUGS-109653": "CVE-2025-31936",
            "OCPBUGS-122827": "CVE-2026-80824",
            "OCPBUGS-126719": "CVE-2026-68376",
            "OCPBUGS-130466": "CVE-2026-98222",
            "OCPBUGS-130469": "CVE-2026-98320",
            "OCPBUGS-134361": "CVE-2026-90003",
        }
        tracker = tracker_cls.return_value
        tracker.target_release.return_value = ["5.0.z"]
        tracker._get_available_target_versions.return_value = {"5.0.z"}
        tracker.cve_tracker_search.return_value = [
            SimpleNamespace(id=key, cve_id=cve, status="Verified", whiteboard_component="openshift/ose-rhel-coreos-9")
            for key, cve in pairs.items()
        ]
        comments = {}
        for key, cve in pairs.items():
            requirement = "kernel-0:5.14.0-687.61.1.el9_8.x86_64"
            if key == "OCPBUGS-109653":
                requirement = "microcode_ctl-4:20260210-1.20260812.1.el9_8.noarch"
            text = f"{cve} verified in RHCOS build nightly. Required: {requirement}. Result: PASS"
            if key == "OCPBUGS-134361":
                text = (
                    f"{cve} manually reviewed for RHCOS build nightly. "
                    "Installed kernel-5.14.0-687.61.1.el9_8 >= required kernel-0:5.14.0-687.54.1.el9_8. "
                    "Result: PASS. Manual correction by the RHCOS CVE verifier maintainer."
                )
            comments[key] = [_comment(text)]
        tracker._client.comments.side_effect = comments.__getitem__
        self.assertEqual(self.review.discover_rhcos_issues(), sorted(pairs))
        tracker.cve_tracker_search.assert_called_once_with(status=["Verified"], search_filter=None)
        self.runtime.pooled_koji_client_session.assert_not_called()
        with TemporaryDirectory() as directory:
            self.review.write(Path(directory))
            details = json.loads((Path(directory) / "targeted-rpm-evidence.log").read_text())
            self.assertEqual(len(details["rhcos_matches"]), 7)
            self.assertEqual(details["selected_rhcos_rpms"]["x86_64"], self.entries)
            markdown = (Path(directory) / "targeted-rpm-review.log").read_text()
            self.assertIn("Automatically included", markdown)
            for key, cve in pairs.items():
                self.assertIn(key, markdown)
                self.assertIn(cve, markdown)

    def test_atlassian_document_comments_preserve_required_versions(self):
        """
        Jira Cloud rich text must produce the same requirements as plain comments.
        """
        comment = SimpleNamespace(
            created="2026-11-01",
            body={
                "type": "doc",
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {
                                "type": "text",
                                "text": (
                                    "CVE-2026-1234 verified in RHCOS build nightly. "
                                    "Required: microcode_ctl-4:20260210-1.20260812.1.el9_8.noarch. Result: PASS"
                                ),
                            }
                        ],
                    }
                ],
            },
        )
        requirements = verified_rpm_requirements([comment], "CVE-2026-1234")
        self.assertEqual(requirements[0]["epoch"], "4")
        self.assertTrue(requirements_installed(requirements, {"x86_64": self.entries}, 9))

    def test_epoch_branch_and_all_installed_versions_must_match(self):
        """
        Never include a tracker based on a newer RPM in another stream or architecture.
        """
        cve = "CVE-2026-1234"
        required = verified_rpm_requirements(
            [
                _comment(
                    f"{cve} verified in RHCOS build nightly. Required: kernel-0:5.14.0-687.61.1.el9_8. Result: PASS"
                )
            ],
            cve,
        )
        inventories = {"x86_64": self.entries}
        self.assertTrue(requirements_installed(required, inventories, 9))
        self.assertFalse(requirements_installed(required, inventories, 10))
        inventories["aarch64"] = [["kernel-core", "0", "5.14.0", "687.47.1.el9_8", "aarch64", "rhel-coreos"]]
        self.assertFalse(requirements_installed(required, inventories, 9))
        self.entries[0][3] = "687.99.1.el9_7"
        self.assertFalse(requirements_installed(required, {"x86_64": self.entries}, 9))
        required[0].update(name="microcode_ctl", epoch="5", version="20260210", release="1.20260812.1.el9_8")
        self.assertFalse(requirements_installed(required, {"x86_64": self.entries}, 9))

    def test_latest_failed_or_unparseable_verification_cannot_reuse_old_pass(self):
        """
        Unsupported or failing latest verification remains a review candidate.
        """
        cve = "CVE-2026-1234"
        old = _comment(f"{cve} verified in RHCOS build nightly. Required: kernel-0:5.14.0-687.61.1.el9_8. Result: PASS")
        for latest in ["Result: FAIL", "Result: PASS", "Required: unknown. Result: PASS"]:
            comments = [old, _comment(f"{cve} verified in RHCOS build newer. {latest}", "2026-11-02")]
            self.assertEqual(verified_rpm_requirements(comments, cve), [])

    @patch("doozerlib.targeted_rpm_evidence.JIRABugTracker")
    def test_existing_excluded_unverified_and_unrelated_trackers_are_not_added(self, tracker_cls):
        """
        Respect assembly issue choices, release query scope and RHCOS components.
        """
        self.review._basis_included = {"OCPBUGS-OLD"}
        self.review._excluded = {"OCPBUGS-EXCLUDED"}
        tracker = tracker_cls.return_value
        tracker.target_release.return_value = ["5.0.z"]
        tracker._get_available_target_versions.return_value = {"5.0.z"}
        tracker.cve_tracker_search.return_value = [
            SimpleNamespace(id=key, cve_id="CVE-2026-1234", status=status, whiteboard_component=component)
            for key, status, component in [
                ("OCPBUGS-OLD", "Verified", "rhcos"),
                ("OCPBUGS-EXCLUDED", "Verified", "rhcos"),
                ("OCPBUGS-135021", "Verified", "rhcos"),
                ("OCPBUGS-NEW", "New", "rhcos"),
                ("OCPBUGS-OTHER", "Verified", "openssl"),
                ("OCPBUGS-UNKNOWN", "Verified", "rhcos"),
            ]
        ]
        tracker._client.comments.return_value = []
        self.assertEqual(self.review.discover_rhcos_issues(), [])
        tracker._client.comments.assert_called_once_with("OCPBUGS-UNKNOWN")
        self.assertEqual(self.review.rhcos_matches[0]["disposition"], "needs_review")

    @patch("doozerlib.targeted_rpm_evidence.JIRABugTracker", side_effect=ValueError("Set JIRA_TOKEN"))
    def test_missing_jira_access_is_reported_as_incomplete(self, tracker_cls):
        """
        Missing credentials must not look like a successful search with no fixes.
        """
        self.assertEqual(self.review.discover_rhcos_issues(), [])
        self.assertIn("Set JIRA_TOKEN", self.review.warnings[0])
