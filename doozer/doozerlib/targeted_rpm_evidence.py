"""
Explain targeted RPM pins and discover additional kernel CVE claim candidates.

Reports distinguish the release's triggering issues from references newly mentioned
in each RPM's changelog. Discovery is read-only and never claims a CVE fixed or
adds a Jira issue to the assembly automatically.
"""

import json
import re
from pathlib import Path
from typing import Any

from artcommonlib.assembly import assembly_issues_config
from artcommonlib.jira_config import get_jira_browse_url
from elliottlib.bzutil import JIRABugTracker, is_rhcos_pscomponent


class TargetedRpmEvidence:
    """
    Collect source-build changes, changelog references, and release-scoped Jira matches.
    """

    def __init__(self, runtime: Any, basis: str, bug_ids: tuple[str, ...], cve_ids: tuple[str, ...]):
        self._runtime = runtime
        self._bugs = bug_ids
        self._cves = cve_ids
        self._changelogs: dict[str, list[str]] = {}
        self._tracker = None
        self._jira_error: str | None = None
        self.records: list[dict] = []
        self.warnings: list[str] = []
        basis_issues = assembly_issues_config(runtime.get_releases_config(), basis)
        self._included = set(bug_ids)
        self._basis_included = {str(item.id) for item in basis_issues.include or []}
        self._excluded = {str(item.id) for item in basis_issues.exclude or []}

    def _changelog(self, nvr: str) -> list[str]:
        """
        Cache exact source-build changelogs and retain entry text for review evidence.
        """
        if nvr not in self._changelogs:
            with self._runtime.pooled_koji_client_session() as session:
                entries = session.getChangelogEntries(nvr)
            if entries is None:
                raise ValueError(f"No changelog returned for {nvr}")
            self._changelogs[nvr] = [entry.get("text") or entry.get("changelog") or "" for entry in entries]
        return self._changelogs[nvr]

    def _jira_matches(self, package: str, el_ver: int, cves: set[str], bugs: set[str]) -> list[dict]:
        """
        Map changelog references to the configured release without changing Jira.
        """
        if not cves and not bugs:
            return []
        if self._jira_error:
            raise ValueError(self._jira_error)
        try:
            if self._tracker is None:
                self._tracker = JIRABugTracker(JIRABugTracker.get_config(self._runtime))
                releases = self._tracker.target_release()
                if not releases:
                    raise ValueError("No Jira target releases configured")
                available = self._tracker._get_available_target_versions()
                if available and set(releases) - set(available):
                    raise ValueError("Configured Jira target releases are unavailable")
            clauses = [f'key = "{key}"' for key in sorted(bugs)]
            clauses += [f'summary ~ "{cve}"' for cve in sorted(cves)]
            matches = {}
            for offset in range(0, len(clauses), 25):
                query = " and (" + " OR ".join(clauses[offset : offset + 25]) + ")"
                for bug in self._tracker.search_bugs(status=None, search_filter=None, custom_query=query):
                    component = bug.whiteboard_component or ""
                    rhcos_match = is_rhcos_pscomponent(component) and component in {
                        "rhcos",
                        f"openshift/ose-rhel-coreos-{el_ver}",
                    }
                    if bug.id not in bugs and not (component == package or rhcos_match):
                        continue
                    cve = bug.cve_id
                    if bug.id not in bugs and cve not in cves:
                        continue
                    if bug.id in self._included:
                        disposition = "already_included"
                    elif bug.id in self._basis_included:
                        disposition = "included_in_basis"
                    elif bug.id in self._excluded:
                        disposition = "excluded_in_basis"
                    elif str(bug.status).strip().upper() == "VERIFIED":
                        disposition = "proposed_addition"
                    else:
                        disposition = "needs_review"
                    matches[bug.id] = {
                        "id": bug.id,
                        "cve": cve,
                        "status": bug.status,
                        "disposition": disposition,
                        "url": get_jira_browse_url(bug.id),
                    }
            return [matches[key] for key in sorted(matches)]
        except Exception as exc:
            self._jira_error = str(exc)
            raise

    def record(self, package: str, el_ver: int, before: str | None, selected: str) -> str:
        """
        Record a source-build delta and return an explanation suitable for a pin's why field.
        """
        row = {
            "package": package,
            "el_target": f"el{el_ver}",
            "before": before,
            "selected": selected,
            "new_cves": [],
            "new_ocpbugs": [],
            "changelog_evidence": [],
            "jira_matches": [],
            "warnings": [],
            "trigger_ocpbugs": list(self._bugs),
            "trigger_cves": list(self._cves),
        }
        try:
            if before is None:
                raise ValueError("No basis source build available; cannot establish RPM-specific changes")
            if package == "kernel":
                old_branch = re.search(r"\.el\d+(?:_\d+)?", before)
                new_branch = re.search(r"\.el\d+(?:_\d+)?", selected)
                if not old_branch or not new_branch or old_branch.group() != new_branch.group():
                    raise ValueError("Kernel builds are on different or unknown RHEL branches; CVE delta needs review")
            old = set(self._changelog(before))
            added = [text for text in self._changelog(selected) if text not in old]
            references = "\n".join(added)
            cves = set(re.findall(r"CVE-\d{4}-\d+", references, re.IGNORECASE))
            bugs = set(re.findall(r"OCPBUGS-\d+", references, re.IGNORECASE))
            cves, bugs = {value.upper() for value in cves}, {value.upper() for value in bugs}
            row["new_cves"], row["new_ocpbugs"] = sorted(cves), sorted(bugs)
            row["changelog_evidence"] = [
                text for text in added if re.search(r"CVE-\d{4}-\d+|OCPBUGS-\d+", text, re.IGNORECASE)
            ]
            row["jira_matches"] = self._jira_matches(package, el_ver, cves, bugs)
        except Exception as exc:
            warning = f"{package}/el{el_ver}: issue evidence incomplete: {exc}"
            row["warnings"].append(warning)
            self.warnings.append(warning)
            self._runtime.logger.warning(warning)
        self.records.append(row)
        trigger = ", ".join((*self._bugs, *self._cves)) or "targeted RHCOS update (no issue IDs supplied)"
        reason = [f"RHCOS update triggered by {trigger}.", f"el{el_ver}: {before or 'not installed'} -> {selected}."]
        refs = sorted(set(row["new_cves"]) | set(row["new_ocpbugs"]))
        reason.append(
            "RPM-specific new changelog references: " + (", ".join(refs) if refs else "none identified") + "."
        )
        if row["jira_matches"]:
            reason.append("Release Jira matches: " + ", ".join(item["id"] for item in row["jira_matches"]) + ".")
        elif refs and not row["warnings"]:
            reason.append("Release Jira matches: none found.")
        if row["warnings"]:
            reason.append("Issue evidence incomplete; see targeted-rpm-review.log.")
        return "\n".join(reason)

    def write(self, directory: Path) -> None:
        """
        Write a concise review and JSON evidence as log files archived by existing jobs.
        """
        directory.mkdir(parents=True, exist_ok=True)
        report = {
            "trigger_ocpbugs": list(self._bugs),
            "trigger_cves": list(self._cves),
            "records": self.records,
            "warnings": self.warnings,
            "complete": not self.warnings,
        }
        (directory / "targeted-rpm-evidence.log").write_text(json.dumps(report, indent=2) + "\n")
        lines = [
            "# Targeted RPM issue review",
            "",
            "Trigger: " + (", ".join((*self._bugs, *self._cves)) or "No issue IDs supplied"),
            "",
            "New changelog references and Jira matches are candidates for review, not automatic fixed-CVE claims.",
            "",
        ]
        proposals = set()
        for row in self.records:
            lines += [
                f"## {row['package']} / {row['el_target']}",
                "",
                f"Basis: `{row['before']}`",
                f"Selected: `{row['selected']}`",
                "",
                "New CVE mentions: " + (", ".join(row["new_cves"]) or "none identified"),
                "New OCPBUGS references: " + (", ".join(row["new_ocpbugs"]) or "none identified"),
                "",
            ]
            for match in row["jira_matches"]:
                lines.append(f"- [{match['id']}]({match['url']}): {match['status']} — {match['disposition']}")
                if match["disposition"] == "proposed_addition":
                    proposals.add(match["id"])
            lines += [f"- Incomplete evidence: {warning}" for warning in row["warnings"]] + [""]
        lines += [
            "## Proposed issues.include additions",
            "",
            "Preserve existing includes; review applicability before adding these issues.",
            "",
            "```yaml",
            "issues:",
        ]
        if proposals:
            lines += ["  include:"] + [f"    - id: {key}" for key in sorted(proposals)]
        else:
            lines += ["  include: []"]
        lines += ["```", "", "Full changelog evidence: targeted-rpm-evidence.log", ""]
        (directory / "targeted-rpm-review.log").write_text("\n".join(lines))
