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
ASSEMBLY_PATTERN = re.compile(r"Shipment for (\d+\.\d+\.\d+)")


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

        match = ASSEMBLY_PATTERN.search(mr.title)
        if not match:
            LOGGER.info("MR !%s (%s) — not an OCP assembly, skipping", mr.iid, mr.title)
            continue

        assembly = match.group(1)
        parts = assembly.split(".")
        version = f"{parts[0]}.{parts[1]}"

        LOGGER.info("MR !%s — triggering verify-stage-testing for %s/%s", mr.iid, version, assembly)
        jenkins.start_verify_stage_testing(version=version, assembly=assembly, block_until_building=True)


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
