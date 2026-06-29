import logging
import re
import sys
from typing import List, Optional

import click
from artcommonlib.jira_config import JIRA_SERVER_URL
from artcommonlib.util import new_roundtrip_yaml_handler

from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.attach_cve_flaws_cli import get_konflux_component_by_component
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import CveAssociation, ReleaseNotes
from elliottlib.shipment_utils import set_bugzilla_bug_ids, set_jira_bug_ids
from elliottlib.util import get_component_by_delivery_repo

YAML = new_roundtrip_yaml_handler()
logger = logging.getLogger(__name__)


def _create_jira_tracker() -> JIRABugTracker:
    """Create a JIRABugTracker with minimal config, bypassing bug.yml from ocp-build-data.

    Non-OpenShift products (OADP, MTA, MTC, etc.) may not have bug.yml in their
    ocp-build-data group, so we construct the tracker directly.
    """
    config = {'server': JIRA_SERVER_URL}
    return JIRABugTracker(config)


def _extract_cve_id_from_labels(labels: List[str]) -> Optional[str]:
    """Extract CVE ID from JIRA labels (e.g., 'CVE-2025-12345')."""
    for label in labels:
        if label.startswith('CVE-'):
            return label
    return None


def _extract_flaw_bug_ids_from_labels(labels: List[str]) -> List[int]:
    """Extract Bugzilla flaw bug IDs from JIRA labels (e.g., 'flaw:bz#12345')."""
    flaw_ids = []
    for label in labels:
        match = re.match(r'flaw:bz#(\d+)', label)
        if match:
            flaw_ids.append(int(match.group(1)))
    return flaw_ids


def _extract_pscomponent_from_labels(labels: List[str]) -> Optional[str]:
    """Extract pscomponent from JIRA labels (e.g., 'pscomponent:oadp/oadp-velero-rhel9')."""
    for label in labels:
        if label.startswith('pscomponent:'):
            return label[len('pscomponent:') :]
    return None


async def process_bugs(runtime: Runtime, jira_ids: List[str]) -> ReleaseNotes:
    """Process a list of JIRA IDs and produce a ReleaseNotes object.

    For each JIRA:
      - If it is a Vulnerability type, extract CVE ID, flaw bug IDs, and pscomponent
        from labels to produce CveAssociation entries and flaw bug references.
      - All JIRAs are added to the fixed issues list.

    CVE data is extracted from JIRA labels (not custom fields) because non-OpenShift
    products store this information in labels like 'CVE-xxxx', 'flaw:bz#xxxxx',
    and 'pscomponent:xxx'.

    The advisory type is RHSA if any CVE is found, RHBA otherwise.
    """
    bug_tracker: JIRABugTracker = _create_jira_tracker()

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

        if not jira_bug.is_type_vulnerability():
            logger.info("JIRA %s is not a Vulnerability type, adding as regular bug", jira_id)
            continue

        logger.info("JIRA %s is a Vulnerability type", jira_id)
        labels = getattr(jira_bug.bug.fields, 'labels', []) or []
        logger.info("JIRA %s labels: %s", jira_id, labels)

        cve_id = _extract_cve_id_from_labels(labels)
        if not cve_id:
            logger.warning("JIRA %s is Vulnerability but has no CVE label, skipping CVE association", jira_id)
            continue

        pscomponent = _extract_pscomponent_from_labels(labels)
        if not pscomponent:
            logger.warning("JIRA %s (CVE %s) has no pscomponent label, skipping CVE association", jira_id, cve_id)
            continue

        distgit_component = get_component_by_delivery_repo(runtime, pscomponent)
        if not distgit_component:
            logger.warning(
                "JIRA %s (CVE %s): pscomponent '%s' not found in delivery_repo_names, skipping",
                jira_id,
                cve_id,
                pscomponent,
            )
            continue

        konflux_component = get_konflux_component_by_component(runtime, distgit_component)
        if not konflux_component:
            logger.warning(
                "JIRA %s (CVE %s): distgit component '%s' could not be mapped to a Konflux component, skipping",
                jira_id,
                cve_id,
                distgit_component,
            )
            continue

        logger.info(
            "JIRA %s: CVE %s -> pscomponent %s -> distgit %s -> konflux %s",
            jira_id,
            cve_id,
            pscomponent,
            distgit_component,
            konflux_component,
        )
        cve_associations.append(CveAssociation(key=cve_id, component=konflux_component))

        bug_flaw_ids = _extract_flaw_bug_ids_from_labels(labels)
        if bug_flaw_ids:
            logger.info("JIRA %s: found flaw bug IDs %s", jira_id, bug_flaw_ids)
            flaw_bug_ids.extend(bug_flaw_ids)
        else:
            logger.warning("JIRA %s (CVE %s) has no flaw:bz# labels", jira_id, cve_id)

    advisory_type = "RHSA" if cve_associations else "RHBA"
    logger.info("Advisory type determined: %s (CVEs found: %d)", advisory_type, len(cve_associations))

    release_notes = ReleaseNotes(type=advisory_type)

    if all_jira_ids:
        set_jira_bug_ids(release_notes, all_jira_ids)

    if flaw_bug_ids:
        set_bugzilla_bug_ids(release_notes, flaw_bug_ids)

    if cve_associations:
        cve_associations.sort(key=lambda x: (x.key, x.component))
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
