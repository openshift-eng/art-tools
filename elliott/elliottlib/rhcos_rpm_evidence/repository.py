"""Jira adapter for release-scoped RHCOS CVE tracker evidence."""

from dataclasses import dataclass

from elliottlib.bzutil import JIRABug, JIRABugTracker, is_rhcos_pscomponent
from elliottlib.rhcos_rpm_evidence.models import VerificationComment
from elliottlib.rhcos_rpm_evidence.ports import RHCOSTracker


@dataclass
class JiraRHCOSTrackerRepository:
    """
    JIRABugTracker adapter implementing the RHCOS evidence repository port.
    """

    tracker: JIRABugTracker

    def target_releases(self) -> tuple[str, ...]:
        """
        Return configured target releases and reject an unscoped Jira query.
        """
        target_releases = tuple(self.tracker.target_release() or ())
        if not target_releases:
            raise ValueError("No Jira target releases configured; refusing an unscoped RHCOS tracker search")

        available = self.tracker.available_target_versions()
        if available:
            unavailable = set(target_releases) - set(available)
            if unavailable:
                raise ValueError(f"Configured Jira target releases are unavailable: {sorted(unavailable)}")
        return target_releases

    def find_trackers(self) -> list[RHCOSTracker]:
        """
        Find non-closed RHCOS CVE trackers within configured target releases.
        """
        self.target_releases()
        bugs = self.tracker.search_bugs(
            status=None,
            search_filter=None,
            include_labels=["SecurityTracking"],
            custom_query="and status != \"Closed\"",
        )
        return [bug for bug in bugs if bug.cve_id and is_rhcos_pscomponent(bug.whiteboard_component or "")]

    def comments(self, issue_key: str) -> list[VerificationComment]:
        """
        Fetch Jira comments through the tracker's public client accessor.
        """
        return [
            VerificationComment(created=str(comment.created), body=comment.body)
            for comment in self.tracker.client().comments(issue_key)
        ]

    def mark_verified(self, bug: RHCOSTracker, comment: str, dry_run: bool) -> bool:
        """
        Transition a tracker to VERIFIED and attach its evidence comment.
        """
        if dry_run:
            return self.tracker.update_bug_status(
                bug,
                "VERIFIED",
                comment=comment,
                log_comment=True,
                noop=True,
            )

        current_bug = JIRABug(self.tracker.client().issue(bug.id))
        if current_bug.status.strip().upper() == "VERIFIED":
            return False
        updated = self.tracker.update_bug_status(
            current_bug,
            "VERIFIED",
            log_comment=False,
            noop=False,
        )
        if not updated:
            return False

        self.tracker.add_comment(bug.id, comment, private=True)
        return True
