import logging
import re

from artcommonlib.constants import REDHAT_GITLAB_URL, SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.gitlab import GitLabClient

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

STAGE_RELEASE_SUCCESS_LABEL = "stage-release-success"
STAGE_TESTING_LABELS = {"stage-testing-triggered", "stage-testing-success", "stage-testing-fail"}
RELEASE_NAME_PATTERN = re.compile(r"Shipment for (\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.]+))?")


async def scan_and_trigger(runtime: Runtime):
    gitlab_client = GitLabClient.from_url(SHIPMENT_DATA_URL_TEMPLATE)
    project_path = SHIPMENT_DATA_URL_TEMPLATE.split(REDHAT_GITLAB_URL + "/")[1]
    project = gitlab_client.get_project(project_path)

    mrs = project.mergerequests.list(
        state="opened",
        labels=[STAGE_RELEASE_SUCCESS_LABEL],
        per_page=100,
    )

    for mr in mrs:
        labels = set(mr.labels or [])
        if labels & STAGE_TESTING_LABELS:
            found = labels & STAGE_TESTING_LABELS
            LOGGER.info("MR !%s (%s) — has %s, skipping", mr.iid, mr.title, found)
            continue

        match = RELEASE_NAME_PATTERN.search(mr.title)
        if not match:
            LOGGER.info("MR !%s (%s) — not an OCP assembly, skipping", mr.iid, mr.title)
            continue

        major, minor, patch, suffix = match.groups()
        version = f"{major}.{minor}"
        # RC/EC: assembly is the suffix (e.g. "rc.5"), standard: full "X.Y.Z"
        assembly = suffix if suffix else f"{major}.{minor}.{patch}"

        LOGGER.info("MR !%s — queueing verify-stage-testing for %s/%s", mr.iid, version, assembly)
        try:
            # The verify job waits for Prow and runs serially, so this scan must not wait for it to start.
            if not runtime.dry_run:
                jenkins.start_verify_stage_testing(version=version, assembly=assembly, block_until_building=False)
            else:
                LOGGER.warning('[DRY RUN] Would trigger %s job for %s', 'verify-stage-testing', version)
        except Exception:
            LOGGER.exception("MR !%s — failed to trigger verify-stage-testing for %s", mr.iid, assembly)


@cli.command("schedule-stage-testing-scan", short_help="Scan shipment MRs and trigger stage testing")
@pass_runtime
@click_coroutine
async def schedule_stage_testing_scan(runtime: Runtime):
    """Scan GitLab shipment MRs for assemblies needing stage testing.

    Lists open MRs with 'stage-release-success' label that have no
    stage-testing-* label yet, and triggers the verify-stage-testing
    Jenkins job for each.

    Requires GITLAB_TOKEN to be set in the environment.

    \b
    Example:
        artcd schedule-stage-testing-scan
    """
    jenkins.init_jenkins()
    await scan_and_trigger(runtime)
