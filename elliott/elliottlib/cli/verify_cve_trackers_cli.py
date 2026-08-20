import json
import logging
from dataclasses import dataclass, field
from typing import Optional

import click
from artcommonlib.assembly import assembly_config_struct
from artcommonlib.jira_config import JIRA_DOMAIN_NAME

from elliottlib import errata
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.find_bugs_sweep_cli import (
    FindBugsSweep,
    categorize_bugs_by_type,
    get_bugs_sweep,
    get_builds_by_advisory_kind,
)
from elliottlib.shipment_utils import get_shipment_configs_from_mr

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
    bug_ids = errata.get_bug_ids(advisory_id)
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


async def find_cve_tracker_bugs(runtime, permissive: bool = True) -> dict[str, list[str]]:
    """Run find-bugs --cve-only logic and return tracker bug IDs by advisory kind."""
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

    return {kind: [b.id for b in kind_bugs] for kind, kind_bugs in bugs_by_type.items()}


def get_advisory_ids(runtime) -> dict[str, int]:
    """Get advisory IDs from assembly config, similar to verify_security_alerts_cli."""
    releases_config = runtime.get_releases_config()
    group_config = assembly_config_struct(releases_config, runtime.assembly, "group", {})
    advisories = group_config.get("advisories", {})
    result = {}
    for impetus, ad_id in advisories.items():
        if ad_id:
            result[impetus] = int(ad_id)
    return result


def get_shipment_mr_url(runtime) -> Optional[str]:
    """Get shipment MR URL from assembly config."""
    releases_config = runtime.get_releases_config()
    group_config = assembly_config_struct(releases_config, runtime.assembly, "group", {})
    shipment = group_config.get("shipment", {})
    return shipment.get("url")


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
        LOGGER.info("  %s: %s", kind, bugs)

    # Get advisory IDs and determine which are RHSA
    advisories = get_advisory_ids(runtime)
    if not advisories:
        LOGGER.warning("No advisory IDs found in assembly config")

    # Collect jira issues from RHSA advisories
    rhsa_jira_issues: set[str] = set()
    for impetus, advisory_id in advisories.items():
        raw = errata.get_raw_erratum(advisory_id)
        errata_type = next(iter(raw.get("errata", {}).keys()), "")
        if errata_type != "rhsa":
            LOGGER.info("Advisory %s (%s): type %s, skipping (not RHSA)", advisory_id, impetus, errata_type.upper())
            continue
        issues = await get_advisory_jira_issues(advisory_id)
        LOGGER.info("Advisory %s (%s): RHSA, found %d jira issues", advisory_id, impetus, len(issues))
        rhsa_jira_issues.update(issues)

    # Cross-check CVE trackers against RHSA advisories (for rpm and rhcos kinds)
    advisory_kinds = ("rpm", "rhcos")
    for kind in advisory_kinds:
        trackers = cve_trackers_by_kind.get(kind, [])
        for tracker_id in trackers:
            if tracker_id not in rhsa_jira_issues:
                LOGGER.warning("CVE tracker %s (kind=%s) not found in RHSA advisories", tracker_id, kind)
                result.missed_trackers.append(MissedTracker(bug_id=tracker_id, kind=kind, source="RHSA advisories"))

    # Check shipment MR (Konflux flow) if available
    mr_url = get_shipment_mr_url(runtime)
    if mr_url:
        LOGGER.info("Checking shipment MR for CVE tracker coverage: %s", mr_url)
        shipment_jira_issues = get_shipment_jira_issues(mr_url, group=runtime.group)
        LOGGER.info("Found %d jira issues in shipment MR", len(shipment_jira_issues))

        # Cross-check all CVE trackers against shipment data
        for kind, trackers in cve_trackers_by_kind.items():
            for tracker_id in trackers:
                if tracker_id not in shipment_jira_issues:
                    LOGGER.warning("CVE tracker %s (kind=%s) not found in shipment MR", tracker_id, kind)
                    result.missed_trackers.append(MissedTracker(bug_id=tracker_id, kind=kind, source="shipment MR"))
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
    "--permissive",
    is_flag=True,
    default=True,
    show_default=True,
    help="Ignore bugs that are determined to be invalid and continue",
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
