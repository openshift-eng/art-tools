import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import List, Set

import click
from artcommonlib.assembly import assembly_config_struct
from artcommonlib.jira_config import JIRA_DOMAIN_NAME

from elliottlib import errata
from elliottlib.bzutil import Bug
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.find_bugs_sweep_cli import (
    FindBugsSweep,
    categorize_bugs_by_type,
    get_bugs_sweep,
    get_builds_by_advisory_kind,
)
from elliottlib.shipment_utils import get_shipment_configs_from_mr
from elliottlib.verify_common import get_assembly_advisory_ids, get_assembly_shipment_url

LOGGER = logging.getLogger(__name__)


@dataclass
class MissedTracker:
    bug_id: str
    kind: str
    source: str


@dataclass
class VerifyCVETrackersResult:
    missed_trackers: list[MissedTracker] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.missed_trackers) == 0

    @property
    def failed(self) -> bool:
        return len(self.missed_trackers) > 0


def render_result(result: VerifyCVETrackersResult, output: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "ok": result.ok,
                "missed_trackers": [
                    {
                        "bug_id": t.bug_id,
                        "kind": t.kind,
                        "source": t.source,
                    }
                    for t in result.missed_trackers
                ],
            },
            indent=2,
        )

    lines = ["CVE tracker bug check", ""]
    if result.ok:
        lines.append("  No missed CVE tracker bugs found.")
    else:
        lines.append(f"  Found {len(result.missed_trackers)} missed CVE tracker bug(s):")
        for t in result.missed_trackers:
            lines.append(f"    {t.bug_id} (kind={t.kind}, not found in {t.source})")
    lines.append("")
    overall = "OK" if result.ok else "FAIL"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)


async def get_advisory_jira_issues(advisory_id: int) -> set[str]:
    """Get all jira issue IDs attached to an advisory."""
    bug_ids = await asyncio.to_thread(errata.get_bug_ids, advisory_id)
    return set(bug_ids.get("jira", []))


def get_shipment_jira_issues(mr_url: str, group: str) -> set[str]:
    """Get all jira issue IDs from shipment YAML files in a merge request."""
    issues: set[str] = set()
    shipment_configs = get_shipment_configs_from_mr(mr_url, group=group)
    for config in shipment_configs.values():
        release_notes = config.shipment.data.releaseNotes if config.shipment.data else None
        if not release_notes or not release_notes.issues or not release_notes.issues.fixed:
            continue
        for issue in release_notes.issues.fixed:
            if issue.source == JIRA_DOMAIN_NAME:
                issues.add(issue.id)
    return issues


def get_shipment_kinds(runtime) -> set[str]:
    """Get advisory kinds that go through the shipment flow (not brew advisory)."""
    releases_config = runtime.get_releases_config()
    group_config = assembly_config_struct(releases_config, runtime.assembly, "group", {})
    shipment = group_config.get("shipment", {})
    shipment_advisories = shipment.get("advisories", [])
    return {sa.get("kind") for sa in shipment_advisories if sa.get("kind")}


async def find_cve_tracker_bugs(runtime, permissive: bool = True) -> dict[str, List[Bug]]:
    """Run find-bugs --cve-only logic and return tracker Bug objects by advisory kind."""
    find_bugs_obj = FindBugsSweep(cve_only=True, art_managed_trackers_only=True)
    bug_tracker = runtime.get_bug_tracker("jira")

    bugs = await get_bugs_sweep(runtime, find_bugs_obj, bug_tracker, filter_attached_bugs=True)
    major_version, minor_version = runtime.get_major_minor()
    builds_by_advisory_kind = get_builds_by_advisory_kind(runtime)
    bugs_by_type, _ = categorize_bugs_by_type(
        runtime=runtime,
        bugs=bugs,
        builds_by_advisory_kind=builds_by_advisory_kind,
        major_version=major_version,
        minor_version=minor_version,
        operator_bundle_advisory="metadata",
        permissive=permissive,
        exclude_trackers=False,
    )

    return {kind: list(kind_bugs) for kind, kind_bugs in bugs_by_type.items()}


async def verify_cve_trackers(runtime, permissive: bool = True) -> VerifyCVETrackersResult:
    result = VerifyCVETrackersResult()

    LOGGER.info("Finding CVE tracker bugs...")
    cve_trackers_by_kind = await find_cve_tracker_bugs(runtime, permissive=permissive)

    total_trackers = sum(len(bugs) for bugs in cve_trackers_by_kind.values())
    if total_trackers == 0:
        LOGGER.info("No CVE tracker bugs found for this assembly")
        return result

    LOGGER.info("Found %d CVE tracker bug(s) across %d kind(s)", total_trackers, len(cve_trackers_by_kind))
    for kind, bugs in cve_trackers_by_kind.items():
        LOGGER.info("  %s: %s", kind, [b.id for b in bugs])

    # Get advisory IDs and determine which are RHSA
    advisories = get_assembly_advisory_ids(runtime)
    if not advisories:
        LOGGER.warning("No advisory IDs found in assembly config")

    # Collect jira issues from RHSA advisories, per kind
    rhsa_jira_issues_by_kind: dict[str, set[str]] = {}
    for impetus, advisory_id in advisories.items():
        raw = await asyncio.to_thread(errata.get_raw_erratum, advisory_id)
        if "rhsa" not in raw.get("errata", {}):
            LOGGER.info("Advisory %s (%s): not RHSA, skipping", advisory_id, impetus)
            continue
        issues = await get_advisory_jira_issues(advisory_id)
        LOGGER.info("Advisory %s (%s): RHSA, found %d jira issues", advisory_id, impetus, len(issues))
        rhsa_jira_issues_by_kind[impetus] = issues

    # Build set of CVE IDs covered per kind from RHSA advisory bugs
    rhsa_covered_cves_by_kind: dict[str, Set[str]] = {}
    bug_tracker = runtime.get_bug_tracker("jira")
    for kind, jira_issues in rhsa_jira_issues_by_kind.items():
        if not jira_issues:
            continue
        advisory_bugs = await asyncio.to_thread(bug_tracker.get_bugs, list(jira_issues), True)
        covered_cves = {b.cve_id for b in advisory_bugs if b.cve_id}
        rhsa_covered_cves_by_kind[kind] = covered_cves
        LOGGER.info("RHSA advisory for %s covers %d unique CVEs", kind, len(covered_cves))

    # Cross-check CVE trackers against RHSA advisories (for rpm and rhcos kinds)
    advisory_kinds = ("rpm", "rhcos")
    for kind in advisory_kinds:
        trackers = cve_trackers_by_kind.get(kind, [])
        kind_jira_issues = rhsa_jira_issues_by_kind.get(kind, set())
        kind_covered_cves = rhsa_covered_cves_by_kind.get(kind, set())
        for bug in trackers:
            if bug.id in kind_jira_issues:
                continue
            if bug.cve_id and bug.cve_id in kind_covered_cves:
                LOGGER.info(
                    "CVE tracker %s (kind=%s) not on advisory, but %s is covered by another tracker on the same advisory — skipping",
                    bug.id,
                    kind,
                    bug.cve_id,
                )
                continue
            LOGGER.warning("CVE tracker %s (kind=%s) not found in RHSA advisories", bug.id, kind)
            result.missed_trackers.append(MissedTracker(bug_id=bug.id, kind=kind, source="RHSA advisories"))

    # Check shipment MR (Konflux flow) if available
    mr_url = get_assembly_shipment_url(runtime)
    if mr_url:
        LOGGER.info("Checking shipment MR for CVE tracker coverage")
        shipment_jira_issues = await asyncio.to_thread(get_shipment_jira_issues, mr_url, runtime.group)
        LOGGER.info("Found %d jira issues in shipment MR", len(shipment_jira_issues))

        # Only check kinds that go through the shipment flow, not advisory-only kinds (e.g. rhcos, rpm)
        shipment_advisory_kinds = get_shipment_kinds(runtime)
        if not shipment_advisory_kinds:
            # Shipment MR exists but no kinds configured — fall back to all non-advisory kinds
            LOGGER.warning(
                "Shipment MR exists but no shipment advisory kinds configured — falling back to all non-advisory kinds"
            )
            shipment_advisory_kinds = {k for k in cve_trackers_by_kind if k not in advisory_kinds}
        LOGGER.info("Shipment advisory kinds: %s", sorted(shipment_advisory_kinds))

        for kind, trackers in cve_trackers_by_kind.items():
            if kind not in shipment_advisory_kinds:
                LOGGER.info("Skipping shipment check for kind=%s (goes through brew advisory, not shipment)", kind)
                continue
            for bug in trackers:
                if bug.id not in shipment_jira_issues:
                    LOGGER.warning("CVE tracker %s (kind=%s) not found in shipment MR", bug.id, kind)
                    result.missed_trackers.append(MissedTracker(bug_id=bug.id, kind=kind, source="shipment MR"))
    else:
        LOGGER.info("No shipment MR URL found in assembly config, skipping shipment check")

    return result


@cli.command("verify-cve-trackers", short_help="Check that CVE tracker bugs are attached to advisories/shipment")
@click.option(
    "-o",
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--permissive/--no-permissive",
    default=True,
    show_default=True,
    help="Ignore bugs that are determined to be invalid and continue.",
)
@click.pass_obj
@click_coroutine
async def verify_cve_trackers_cli(runtime, output, permissive):
    """Check that all CVE tracker bugs are properly attached to RHSA advisories
    and/or shipment merge requests.

    Finds CVE tracker bugs for the assembly, then cross-checks them against:
    - RHSA advisories: rpm and rhcos tracker bugs must be in RHSA advisory jira issues
    - Shipment MR (Konflux flow): all tracker bugs must be in shipment YAML files

    Exits with code 1 if any CVE tracker bug is missing.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.51 verify-cve-trackers
    """
    runtime.initialize(mode="both")
    result = await verify_cve_trackers(runtime, permissive=permissive)
    click.echo(render_result(result, output))
    if not result.ok:
        raise SystemExit(1)
