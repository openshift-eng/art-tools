import logging
import re

from artcommonlib.constants import REDHAT_GITLAB_URL, SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.gitlab import GitLabClient

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.pipelines.verify_advisory_transition import ADVISORY_TRANSITION_SUCCESS_LABEL, VERIFY_RELEASE_SUCCESS_LABEL
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)
RELEASE_NAME_PATTERN = re.compile(r"Shipment for (\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.]+))?")


async def scan_and_trigger(runtime: Runtime):
    gitlab_client = GitLabClient.from_url(SHIPMENT_DATA_URL_TEMPLATE)
    project_path = SHIPMENT_DATA_URL_TEMPLATE.split(REDHAT_GITLAB_URL + "/")[1]
    # GitLabClient lists every page. MRs may already be closed or merged.
    merge_requests = gitlab_client.list_merge_requests(
        project_path, state="all", labels=[VERIFY_RELEASE_SUCCESS_LABEL], per_page=100
    )
    releases = set()
    for mr in merge_requests:
        labels = set(mr.labels or [])
        if VERIFY_RELEASE_SUCCESS_LABEL not in labels or ADVISORY_TRANSITION_SUCCESS_LABEL in labels:
            continue
        match = RELEASE_NAME_PATTERN.search(mr.title)
        if not match:
            LOGGER.info("MR !%s (%s) is not an OCP release, skipping", mr.iid, mr.title)
            continue
        major, minor, patch, suffix = match.groups()
        version = f"{major}.{minor}"
        assembly = suffix if suffix else f"{major}.{minor}.{patch}"
        if (version, assembly) in releases:
            continue
        releases.add((version, assembly))
        if runtime.dry_run:
            LOGGER.info("[DRY-RUN] MR !%s would queue verify-advisory-transition for %s/%s", mr.iid, version, assembly)
            continue
        try:
            jenkins.start_verify_advisory_transition(version=version, assembly=assembly, block_until_building=False)
        except Exception:
            LOGGER.exception("MR !%s failed to trigger verify-advisory-transition for %s", mr.iid, assembly)


@cli.command(
    "schedule-verify-advisory-transition-scan", short_help="Queue advisory transitions for verified shipment MRs"
)
@pass_runtime
@click_coroutine
async def schedule_verify_advisory_transition_scan(runtime: Runtime):
    """Scan all MR states for verify-release-success without advisory-transition-success.

    Failures remain eligible for the next scheduled scan. The per-release
    Jenkins job locks each release and rechecks labels before doing work.
    Requires GITLAB_TOKEN and Jenkins credentials. Cadence is set in Jenkins.
    """
    if not runtime.dry_run:
        jenkins.init_jenkins()
    await scan_and_trigger(runtime)
