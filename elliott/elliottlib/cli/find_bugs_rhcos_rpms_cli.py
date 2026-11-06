"""
Analyze release-scoped RHCOS CVE trackers against selected image RPM inventories.

Examples:
    elliott --group openshift-4.20 --assembly 4.20.1 find-bugs:rhcos-rpms
    elliott --group openshift-4.20 --assembly 4.20.1 find-bugs:rhcos-rpms --update-tracker --dry-run
"""

import json
import logging
from pathlib import Path

import click
from artcommonlib.assembly import assembly_issues_config

from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.common import cli
from elliottlib.rhcos_rpm_evidence.build_evidence import KojiErrataRpmBuildEvidenceSource
from elliottlib.rhcos_rpm_evidence.inventory import RHCOSInventoryProvider
from elliottlib.rhcos_rpm_evidence.report import render_report, write_report
from elliottlib.rhcos_rpm_evidence.repository import JiraRHCOSTrackerRepository
from elliottlib.rhcos_rpm_evidence.service import RHCOSRpmEvidenceService
from elliottlib.runtime import Runtime

LOGGER = logging.getLogger(__name__)


@cli.command("find-bugs:rhcos-rpms", short_help="Discover and verify RHCOS CVE trackers using RPM evidence")
@click.option("--update-tracker", is_flag=True, help="Move evidence-qualified trackers to VERIFIED")
@click.option("--dry-run", is_flag=True, help="Report status changes without updating Jira")
@click.option("--output", type=click.Choice(["text", "json"]), default="text", help="Output format")
@click.pass_obj
def find_bugs_rhcos_rpms_cli(runtime: Runtime, update_tracker: bool, dry_run: bool, output: str) -> None:
    """Analyze release-scoped RHCOS CVE trackers against selected and basis RHCOS RPMs.

    A tracker is eligible for VERIFIED when its latest passing RHCOS verification
    requirements are installed, or when a changed selected RPM's changelog newly
    names the CVE relative to the exact assembly basis. Advisory-only, baseline, and
    collection gaps are reported without automatic status changes.

    Usage:
        $ elliott --group openshift-4.20 --assembly 4.20.1 find-bugs:rhcos-rpms
        $ elliott --group openshift-4.20 --assembly 4.20.1 find-bugs:rhcos-rpms --update-tracker --dry-run
    """
    runtime.initialize(mode="both")
    tracker = runtime.get_bug_tracker("jira")
    if not isinstance(tracker, JIRABugTracker):
        raise click.ClickException("RHCOS RPM evidence requires the Jira bug tracker")

    issue_config = assembly_issues_config(runtime.get_releases_config(), runtime.assembly)
    excluded_issue_keys = {str(item["id"]) for item in issue_config.exclude or []}
    service = RHCOSRpmEvidenceService(
        assembly=runtime.assembly,
        inventory_source=RHCOSInventoryProvider(runtime),
        build_evidence_source=KojiErrataRpmBuildEvidenceSource(runtime),
        tracker_repository=JiraRHCOSTrackerRepository(tracker),
        excluded_issue_keys=excluded_issue_keys,
    )
    report = service.run(update_trackers=update_tracker, dry_run=dry_run)
    json_path, text_path = write_report(Path(runtime.working_dir), report)
    LOGGER.info("RHCOS RPM evidence saved to %s and %s", json_path, text_path)
    if report.warnings:
        LOGGER.warning("RHCOS RPM evidence collection completed with warnings: %s", "; ".join(report.warnings))

    if output == "json":
        click.echo(json.dumps(report.to_dict(), indent=2))
    else:
        click.echo(render_report(report))
