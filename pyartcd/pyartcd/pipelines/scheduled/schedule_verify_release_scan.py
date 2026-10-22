"""Find released shipment MRs that still need verify-release."""

import logging
import re

from artcommonlib.constants import REDHAT_GITLAB_URL, SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.gitlab import GitLabClient

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

STAGE_RELEASE_SUCCESS_LABEL = "stage-release-success"
STAGE_TESTING_SUCCESS_LABEL = "stage-testing-success"
VERIFY_RELEASE_SUCCESS_LABEL = "verify-release-success"
RELEASE_NAME_PATTERN = re.compile(r"Shipment for (\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.]+))?")


async def scan_and_trigger(runtime: Runtime):
    """Queue verify-release for stage-tested OCP shipment MRs not yet verified."""
    gitlab_client = GitLabClient.from_url(SHIPMENT_DATA_URL_TEMPLATE)
    project_path = SHIPMENT_DATA_URL_TEMPLATE.split(REDHAT_GITLAB_URL + "/")[1]
    project = gitlab_client.get_project(project_path)
    merge_requests = project.mergerequests.list(
        state="opened",
        labels=[STAGE_RELEASE_SUCCESS_LABEL, STAGE_TESTING_SUCCESS_LABEL],
        per_page=100,
    )

    for mr in merge_requests:
        labels = set(mr.labels or [])
        if VERIFY_RELEASE_SUCCESS_LABEL in labels:
            LOGGER.info("MR !%s (%s) — verify-release already passed, skipping", mr.iid, mr.title)
            continue

        if STAGE_TESTING_SUCCESS_LABEL not in labels:
            LOGGER.info("MR !%s (%s) — stage testing has not passed, skipping", mr.iid, mr.title)
            continue

        match = RELEASE_NAME_PATTERN.search(mr.title)
        if not match:
            LOGGER.info("MR !%s (%s) — not an OCP assembly, skipping", mr.iid, mr.title)
            continue

        major, minor, patch, suffix = match.groups()
        version = f"{major}.{minor}"
        assembly = suffix if suffix else f"{major}.{minor}.{patch}"

        if runtime.dry_run:
            LOGGER.info("[DRY-RUN] MR !%s — would queue verify-release for %s/%s", mr.iid, version, assembly)
            continue

        LOGGER.info("MR !%s — queueing verify-release for %s/%s", mr.iid, version, assembly)
        try:
            # The scheduled scan does not wait for the verification job to start.
            jenkins.start_verify_release(version=version, assembly=assembly, block_until_building=False)
        except Exception:
            LOGGER.exception("MR !%s — failed to trigger verify-release for %s", mr.iid, assembly)


@cli.command("schedule-verify-release-scan", short_help="Scan shipment MRs and trigger verify-release")
@pass_runtime
@click_coroutine
async def schedule_verify_release_scan(runtime: Runtime):
    """Queue verify-release for open shipment MRs that passed stage testing.

    Only MRs with stage-release-success and stage-testing-success are eligible.
    Successful verify-release builds add ``verify-release-success`` to the MR.
    Failed builds remain eligible for a later scan.

    Requires GITLAB_TOKEN to be set in the environment.

    Example:
        artcd schedule-verify-release-scan
    """
    jenkins.init_jenkins()
    await scan_and_trigger(runtime)
