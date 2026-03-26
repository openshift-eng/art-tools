import logging
import sys
from typing import List

import click
from artcommonlib.util import new_roundtrip_yaml_handler

from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.attach_cve_flaws_cli import get_konflux_component_by_component
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import CveAssociation, ReleaseNotes
from elliottlib.shipment_utils import set_bugzilla_bug_ids, set_jira_bug_ids

YAML = new_roundtrip_yaml_handler()
logger = logging.getLogger(__name__)


async def process_bugs(runtime: Runtime, jira_ids: List[str]) -> ReleaseNotes:
    """Process a list of JIRA IDs and produce a ReleaseNotes object.

    For each JIRA:
      - If it is a Vulnerability type, extract CVE ID, flaw bug IDs, and pscomponent
        to produce CveAssociation entries and flaw bug references.
      - All JIRAs are added to the fixed issues list.

    The advisory type is RHSA if any CVE is found, RHBA otherwise.
    """
    bug_tracker: JIRABugTracker = runtime.get_bug_tracker("jira")

    cve_associations: list[CveAssociation] = []
    flaw_bug_ids: list[int] = []
    all_jira_ids: list[str] = []

    for jira_id in jira_ids:
        jira_id = jira_id.strip()
        if not jira_id:
            continue

        logger.info("Processing JIRA %s", jira_id)
        jira_bug = bug_tracker.get_bug(jira_id)
        all_jira_ids.append(jira_id)

        if jira_bug.is_type_vulnerability():
            logger.info("JIRA %s is a Vulnerability type", jira_id)

            cve_id = jira_bug.cve_id
            if not cve_id:
                logger.warning("JIRA %s is Vulnerability but has no CVE ID, skipping CVE association", jira_id)
                continue

            whiteboard_component = jira_bug.whiteboard_component
            if not whiteboard_component:
                logger.warning("JIRA %s (CVE %s) has no pscomponent, skipping CVE association", jira_id, cve_id)
                continue

            konflux_component = get_konflux_component_by_component(runtime, whiteboard_component)
            if not konflux_component:
                logger.warning(
                    "JIRA %s (CVE %s): component '%s' could not be mapped to a Konflux component, skipping",
                    jira_id,
                    cve_id,
                    whiteboard_component,
                )
                continue

            logger.info(
                "JIRA %s: CVE %s mapped to component %s (from %s)",
                jira_id,
                cve_id,
                konflux_component,
                whiteboard_component,
            )
            cve_associations.append(CveAssociation(key=cve_id, component=konflux_component))

            bug_flaw_ids = jira_bug.corresponding_flaw_bug_ids
            if bug_flaw_ids:
                logger.info("JIRA %s: found flaw bug IDs %s", jira_id, bug_flaw_ids)
                flaw_bug_ids.extend(bug_flaw_ids)
            else:
                logger.warning("JIRA %s (CVE %s) has no flaw bug IDs", jira_id, cve_id)

    advisory_type = "RHSA" if cve_associations else "RHBA"
    logger.info("Advisory type determined: %s (CVEs found: %d)", advisory_type, len(cve_associations))

    release_notes = ReleaseNotes(type=advisory_type)

    # Set all JIRAs as fixed issues
    if all_jira_ids:
        set_jira_bug_ids(release_notes, all_jira_ids)

    # Set flaw bugs from Bugzilla (if RHSA)
    if flaw_bug_ids:
        set_bugzilla_bug_ids(release_notes, flaw_bug_ids)

    # Set CVE associations
    if cve_associations:
        cve_associations.sort(key=lambda x: x.key)
        release_notes.cves = cve_associations

    return release_notes


@cli.command("process-release-from-fbc-bugs", short_help="Process JIRA bugs for release-from-fbc advisory")
@click.option(
    "--jira-bugs",
    required=True,
    help="Comma-separated list of JIRA issue IDs (e.g., OADP-7223,OADP-7222,OADP-6707)",
)
@click.pass_obj
@click_coroutine
async def process_release_from_fbc_bugs_cli(runtime: Runtime, jira_bugs: str):
    """Process JIRA bugs for a release-from-fbc advisory and output ReleaseNotes YAML.

    This command fetches each JIRA issue, detects CVEs (Vulnerability type issues),
    extracts flaw bug IDs and pscomponent for component mapping, and determines
    the advisory type (RHSA if CVEs present, RHBA otherwise).

    The JIRA list is treated as source of truth -- no verification is performed.

    Output is a YAML block suitable for shipment data.releaseNotes.

    Example:

    \b
        $ elliott --group=oadp-1.4 process-release-from-fbc-bugs --jira-bugs OADP-7223,OADP-7222
    """
    runtime.initialize(mode="images")

    jira_ids = [j.strip() for j in jira_bugs.split(",") if j.strip()]
    if not jira_ids:
        raise click.UsageError("At least one JIRA ID must be provided via --jira-bugs")

    release_notes = await process_bugs(runtime, jira_ids)

    YAML.dump(release_notes.model_dump(mode="python", exclude_unset=True, exclude_none=True), sys.stdout)
