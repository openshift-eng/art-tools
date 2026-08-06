import json
import sys
from datetime import datetime, timezone
from typing import List, Optional, Set

import click
from artcommonlib import logutil
from artcommonlib.rpm_utils import parse_nvr

from elliottlib import Runtime, constants
from elliottlib.bzutil import Bug
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.find_bugs_sweep_cli import FindBugsSweep
from elliottlib.util import chunk, isolate_timestamp_in_release, normalize_component_by_ocp_delivery_repo

logger = logutil.get_logger(__name__)


def get_timestamp_from_nvr(nvr: str) -> float:
    """Extract a UTC unix timestamp from the embedded YYYYMMddHHmm in an NVR release field."""
    parsed = parse_nvr(nvr)
    ts_str = isolate_timestamp_in_release(parsed['release'])
    if not ts_str:
        raise click.BadParameter(
            f"Cannot extract timestamp from NVR release field: {nvr}. "
            "The release field must contain a YYYYMMddHHmm timestamp."
        )
    dt = datetime.strptime(ts_str, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    return dt.timestamp()


def parse_cutoff_time(time_str: str) -> float:
    """Parse an ISO datetime string into a UTC unix timestamp."""
    if time_str.endswith('Z'):
        time_str = time_str[:-1] + '+00:00'
    dt = datetime.fromisoformat(time_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def get_component_names_from_nvrs(nvrs: List[str]) -> Set[str]:
    """Extract package names from NVRs for component filtering."""
    names = set()
    for nvr in nvrs:
        if nvr:
            parsed = parse_nvr(nvr)
            names.add(parsed['name'])
    return names


def filter_bugs_by_cutoff_chunked(bug_tracker, bugs, statuses, cutoff_ts):
    """Filter bugs by cutoff in chunks to avoid Jira URL length limits."""
    result = []
    for bug_chunk in chunk(bugs, constants.BUG_LOOKUP_CHUNK_SIZE):
        result.extend(bug_tracker.filter_bugs_by_cutoff_event(bug_chunk, statuses, cutoff_ts))
    return result


def get_delivery_repo_names(runtime: Runtime, component_name: str) -> Set[str]:
    """Map a build component name to its delivery repo names using ocp-build-data metadata.

    Tracker bugs use delivery repo names (e.g. openshift4/ose-kube-rbac-proxy-rhel9) in their
    pscomponent labels. This function finds the matching image metadata and returns all
    delivery repo names so we can query Jira directly.
    """
    for image_meta in runtime.image_metas():
        if image_meta.get_component_name() == component_name:
            repo_names = image_meta.config.delivery.get('delivery_repo_names', [])
            if repo_names:
                return set(repo_names)
    return set()


def search_tracker_bugs_at_cutoff(bug_tracker, component_names, statuses, cutoff_ts, runtime):
    """Search for tracker bugs matching components that were in a qualifying status at cutoff time.

    Builds a JQL query using pscomponent labels derived from delivery repo names in ocp-build-data.
    """
    dt = datetime.utcfromtimestamp(cutoff_ts).strftime("%Y/%m/%d %H:%M")
    status_val = ','.join(f'"{s}"' for s in statuses)

    # Build pscomponent label conditions from both component names and delivery repo names
    label_conditions = []
    for comp in component_names:
        label_conditions.append(f'labels = "pscomponent:{comp}"')
        label_conditions.append(f'labels = "art:pscomponent:{comp}"')
        for repo_name in get_delivery_repo_names(runtime, comp):
            label_conditions.append(f'labels = "pscomponent:{repo_name}"')
            label_conditions.append(f'labels = "art:pscomponent:{repo_name}"')

    if not label_conditions:
        logger.warning(f"No pscomponent labels found for components: {component_names}")
        return []

    label_filter = f' and ({" or ".join(label_conditions)})'
    cutoff_filter = f' and status was in ({status_val}) on("{dt}")'
    query = bug_tracker._query(status=None, custom_query=label_filter + cutoff_filter)
    if query is None:
        return []
    logger.info(f"JQL: {query}")
    return bug_tracker._search(query)


def filter_by_component(bugs: List[Bug], component_names: Set[str], runtime: Runtime) -> List[Bug]:
    """Filter tracker bugs by whiteboard_component. Non-tracker bugs pass through unfiltered."""
    filtered = []
    for bug in bugs:
        if not bug.is_tracker_bug():
            filtered.append(bug)
            continue
        wb_component = bug.whiteboard_component
        if wb_component:
            wb_component = normalize_component_by_ocp_delivery_repo(runtime, wb_component)
        if wb_component in component_names:
            filtered.append(bug)
    return filtered


def validate_options(nvr, cutoff_time, from_nvr, to_nvr, from_time, to_time):
    """Validate mutual exclusivity of single vs range mode options."""
    single_opts = {'--nvr': nvr, '--cutoff-time': cutoff_time}
    range_opts = {'--from-nvr': from_nvr, '--to-nvr': to_nvr, '--from-time': from_time, '--to-time': to_time}

    has_single = any(single_opts.values())
    has_range = any(range_opts.values())

    if has_single and has_range:
        raise click.BadParameter(
            "Cannot mix single-mode options (--nvr, --cutoff-time) with "
            "range-mode options (--from-nvr, --to-nvr, --from-time, --to-time)."
        )

    if not has_single and not has_range:
        raise click.BadParameter(
            "Must provide either single-mode (--nvr or --cutoff-time) or "
            "range-mode (--from-nvr/--from-time + --to-nvr/--to-time) options."
        )

    if has_single:
        if nvr and cutoff_time:
            raise click.BadParameter("Cannot use both --nvr and --cutoff-time. Pick one.")
        return 'single'

    # Range mode validation
    if from_nvr and from_time:
        raise click.BadParameter("Cannot use both --from-nvr and --from-time. Pick one.")
    if to_nvr and to_time:
        raise click.BadParameter("Cannot use both --to-nvr and --to-time. Pick one.")
    if not (from_nvr or from_time):
        raise click.BadParameter("Range mode requires a 'from' source: --from-nvr or --from-time.")
    if not (to_nvr or to_time):
        raise click.BadParameter("Range mode requires a 'to' source: --to-nvr or --to-time.")

    return 'range'


@cli.command("find-bugs:cutoff", short_help="Find bugs by cutoff time or NVR")
@click.option("--nvr", default=None, help="Image NVR to derive a single cutoff timestamp from")
@click.option("--cutoff-time", default=None, help="ISO datetime for single cutoff (e.g. 2025-04-15T13:28:00Z)")
@click.option("--from-nvr", default=None, help="Start NVR for range mode")
@click.option("--to-nvr", default=None, help="End NVR for range mode")
@click.option("--from-time", default=None, help="Start ISO datetime for range mode")
@click.option("--to-time", default=None, help="End ISO datetime for range mode")
@click.option(
    "--component", default=None, help="Filter tracker bugs by whiteboard component (overrides NVR-derived component)"
)
@click.option(
    "--include-status",
    multiple=True,
    default=None,
    required=False,
    type=click.Choice(constants.VALID_BUG_STATES),
    help="Include bugs of this status (in addition to the default VERIFIED)",
)
@click.option(
    "--exclude-status",
    multiple=True,
    default=None,
    required=False,
    type=click.Choice(constants.VALID_BUG_STATES),
    help="Exclude bugs of this status",
)
@click.option("--output", "-o", type=click.Choice(["json", "text"]), default="text", help="Output format")
@click.pass_obj
@click_coroutine
async def find_bugs_cutoff_cli(
    runtime: Runtime,
    nvr,
    cutoff_time,
    from_nvr,
    to_nvr,
    from_time,
    to_time,
    component,
    include_status,
    exclude_status,
    output,
):
    """Find OCP bugs by cutoff time or NVR.

    Supports two modes:

    \b
    Single cutoff: find bugs that were in qualifying status at a given time.
        $ elliott -g openshift-4.18 find-bugs:cutoff --nvr <NVR> -o json
        $ elliott -g openshift-4.18 find-bugs:cutoff --cutoff-time 2025-04-15T13:28:00Z

    \b
    Range: find bugs that entered qualifying status between two cutoff times.
        $ elliott -g openshift-4.18 find-bugs:cutoff --from-nvr <NVR1> --to-nvr <NVR2>
        $ elliott -g openshift-4.18 find-bugs:cutoff --from-time <T1> --to-time <T2>

    When NVRs are provided, tracker bugs are automatically filtered to those
    whose whiteboard_component matches the NVR package name. Use --component
    to override this or to filter when using --cutoff-time/--from-time/--to-time.
    """

    mode = validate_options(nvr, cutoff_time, from_nvr, to_nvr, from_time, to_time)

    runtime.initialize(mode="both")
    bug_tracker = runtime.get_bug_tracker('jira')

    find_bugs_obj = FindBugsSweep()
    find_bugs_obj.include_status(include_status)
    find_bugs_obj.exclude_status(exclude_status)

    # Resolve timestamps
    if mode == 'single':
        cutoff_ts = get_timestamp_from_nvr(nvr) if nvr else parse_cutoff_time(cutoff_time)
        nvrs_provided = [nvr] if nvr else []
    else:
        t1 = get_timestamp_from_nvr(from_nvr) if from_nvr else parse_cutoff_time(from_time)
        t2 = get_timestamp_from_nvr(to_nvr) if to_nvr else parse_cutoff_time(to_time)
        if t1 >= t2:
            raise click.BadParameter(
                f"'from' cutoff ({datetime.utcfromtimestamp(t1)}) must be before "
                f"'to' cutoff ({datetime.utcfromtimestamp(t2)})."
            )
        nvrs_provided = [n for n in [from_nvr, to_nvr] if n]

    # Determine component filter
    component_names: Optional[Set[str]] = None
    if component:
        component_names = {component}
    elif nvrs_provided:
        component_names = get_component_names_from_nvrs(nvrs_provided)

    statuses = sorted(find_bugs_obj.status)

    if component_names:
        # Fast path: query Jira directly for tracker bugs by pscomponent label + cutoff
        logger.info(f"Searching for tracker bugs with component(s) {component_names} and status {statuses}")
        if mode == 'single':
            utc_ts = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
            logger.info(f"Finding tracker bugs in {statuses} at {utc_ts}...")
            result_bugs = search_tracker_bugs_at_cutoff(
                bug_tracker, component_names, find_bugs_obj.status, cutoff_ts, runtime
            )
        else:
            utc_t1 = datetime.fromtimestamp(t1, tz=timezone.utc)
            utc_t2 = datetime.fromtimestamp(t2, tz=timezone.utc)
            logger.info(f"Finding tracker bugs that entered {statuses} between {utc_t1} and {utc_t2}...")
            at_t2 = search_tracker_bugs_at_cutoff(bug_tracker, component_names, find_bugs_obj.status, t2, runtime)
            at_t1 = search_tracker_bugs_at_cutoff(bug_tracker, component_names, find_bugs_obj.status, t1, runtime)
            at_t1_ids = {b.id for b in at_t1}
            result_bugs = [b for b in at_t2 if b.id not in at_t1_ids]
    else:
        # No component filter: broad search + chunked cutoff filtering
        tr = bug_tracker.target_release()
        logger.info(f"Searching for bugs currently in {statuses} with target releases: {tr}")
        bugs = find_bugs_obj.search(bug_tracker_obj=bug_tracker, verbose=runtime.debug)

        if not bugs:
            logger.info("No bugs found matching current status criteria.")
            if output == 'json':
                click.echo(json.dumps([], indent=4))
            else:
                click.echo("No bugs found.")
            sys.exit(0)

        logger.info(f"Found {len(bugs)} bugs currently in qualifying status.")

        if mode == 'single':
            utc_ts = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
            logger.info(f"Filtering to bugs that were in {statuses} at {utc_ts}...")
            result_bugs = filter_bugs_by_cutoff_chunked(bug_tracker, bugs, find_bugs_obj.status, cutoff_ts)
        else:
            utc_t1 = datetime.fromtimestamp(t1, tz=timezone.utc)
            utc_t2 = datetime.fromtimestamp(t2, tz=timezone.utc)
            logger.info(f"Finding bugs that entered {statuses} between {utc_t1} and {utc_t2}...")
            at_t2 = filter_bugs_by_cutoff_chunked(bug_tracker, bugs, find_bugs_obj.status, t2)
            at_t1 = filter_bugs_by_cutoff_chunked(bug_tracker, bugs, find_bugs_obj.status, t1)
            at_t1_ids = {b.id for b in at_t1}
            result_bugs = [b for b in at_t2 if b.id not in at_t1_ids]

    logger.info(f"{len(result_bugs)} bugs found.")

    # Output results
    bug_ids = sorted([b.id for b in result_bugs])
    if output == 'json':
        click.echo(json.dumps(bug_ids, indent=4))
    else:
        if bug_ids:
            click.echo(f"Found {len(bug_ids)} bugs:")
            click.echo(", ".join(bug_ids))
        else:
            click.echo("No bugs found.")

    sys.exit(0)
