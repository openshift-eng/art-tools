"""Jira adapter for release-scoped RHCOS CVE tracker evidence."""

import json
from dataclasses import dataclass, field

from elliottlib.bzutil import JIRABug, JIRABugTracker, is_rhcos_pscomponent
from elliottlib.rhcos_rpm_evidence.models import (
    TRANSITIONABLE_TRACKER_STATUSES,
)
from elliottlib.rhcos_rpm_evidence.ports import RHCOSTracker


@dataclass
class JiraRHCOSTrackerRepository:
    """
    JIRABugTracker adapter implementing the RHCOS evidence repository port.
    """

    tracker: JIRABugTracker
    issues_by_key: dict[str, RHCOSTracker] = field(default_factory=dict, init=False)

    def target_releases(self) -> tuple[str, ...]:
        """
        Return configured target releases and reject an unscoped Jira query.
        """
        target_releases = tuple(self.tracker.target_release() or ())
        if not target_releases:
            raise ValueError("No Jira target releases configured; refusing an unscoped RHCOS tracker search")

        available = self.tracker.available_target_versions()
        if not available:
            raise ValueError("Unable to validate Jira target release metadata")
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
            require_complete=True,
        )
        selected = [bug for bug in bugs if bug.cve_id and is_rhcos_pscomponent(bug.whiteboard_component or "")]
        self.issues_by_key = {str(bug.id): bug for bug in selected}
        return selected

    def mark_verified(self, bug: RHCOSTracker, comment: str, dry_run: bool) -> bool:
        """
        Transition a tracker to VERIFIED and attach its evidence comment.
        """
        if dry_run:
            updated = self.tracker.update_bug_status(
                bug,
                "VERIFIED",
                comment=comment,
                log_comment=True,
                noop=True,
            )
            if not updated:
                raise RuntimeError("Jira dry-run transition failed")
            return True

        current_bug = JIRABug(self.tracker.client().issue(bug.id))
        status = current_bug.status.strip().upper()
        if status == "VERIFIED":
            return False
        if status not in TRANSITIONABLE_TRACKER_STATUSES:
            return False
        client = self.tracker.client()
        transition_id = client.find_transitionid_by_name(bug.id, "VERIFIED")
        if transition_id is None:
            raise RuntimeError("Jira VERIFIED transition is unavailable")
        payload = {
            "transition": {"id": str(transition_id)},
            "update": {
                "comment": [
                    {
                        "add": {
                            "body": comment,
                            "visibility": {"type": "group", "value": "Red Hat Employee"},
                        }
                    }
                ]
            },
        }
        client._session.post(client._get_url(f"issue/{bug.id}/transitions"), data=json.dumps(payload))
        return True
