import json
import sys
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Set

import click
from artcommonlib import arch_util, logutil
from artcommonlib.assembly import assembly_issues_config
from artcommonlib.format_util import green_prefix, green_print

from elliottlib import Runtime, bzutil, constants, errata
from elliottlib.bzutil import Bug, BugTracker, JIRABug
from elliottlib.cli import common
from elliottlib.cli.common import click_coroutine
from elliottlib.exceptions import ElliottFatalError
from elliottlib.util import chunk, fix_summary_suffix

logger = logutil.get_logger(__name__)
type_bug_list = List[Bug]
type_bug_set = Set[Bug]


class FindBugsMode:
    def __init__(self, status: List, cve_only: bool):
        self.status = set(status)
        self.cve_only = cve_only

    def include_status(self, status: List):
        self.status |= set(status)

    def exclude_status(self, status: List):
        self.status -= set(status)

    def search(self, bug_tracker_obj: BugTracker, verbose: bool = False):
        func = bug_tracker_obj.cve_tracker_search if self.cve_only else bug_tracker_obj.search
        return func(
            self.status,
            verbose=verbose,
        )


class FindBugsSweep(FindBugsMode):
    def __init__(self, cve_only: bool):
        super().__init__(status={'MODIFIED', 'ON_QA', 'VERIFIED'}, cve_only=cve_only)


@common.cli.command("find-bugs:sweep", short_help="Sweep qualified bugs into advisories")
@common.use_default_advisory_option
@click.option(
    "--include-status",
    'include_status',
    multiple=True,
    default=None,
    required=False,
    type=click.Choice(constants.VALID_BUG_STATES),
    help="Include bugs of this status",
)
@click.option(
    "--exclude-status",
    'exclude_status',
    multiple=True,
    default=None,
    required=False,
    type=click.Choice(constants.VALID_BUG_STATES),
    help="Exclude bugs of this status",
)
@click.option("--report", required=False, is_flag=True, help="Output a detailed report of found bugs")
@click.option(
    '--output',
    '-o',
    required=False,
    type=click.Choice(['text', 'json', 'slack']),
    default='text',
    help='Applies chosen format to --report output',
)
@click.option(
    "--into-default-advisories",
    is_flag=True,
    help='Attaches bugs found to their correct default advisories, e.g. operator-related bugs go to '
    '"extras" instead of the default "image", bugs filtered into "none" are not attached at all.',
)
@click.option("--cve-only", is_flag=True, help="Only find CVE trackers")
@click.option("--advance-release", is_flag=True, help="If the release contains an advance advisory")
@click.option(
    "--permissive",
    is_flag=True,
    default=False,
    required=False,
    help="Ignore bugs that are determined to be invalid and continue",
)
@click.option("--noop", "--dry-run", is_flag=True, default=False, help="Don't change anything")
@click.pass_obj
@click_coroutine
async def find_bugs_sweep_cli(
    runtime: Runtime,
    default_advisory_type,
    include_status,
    exclude_status,
    report,
    output,
    into_default_advisories,
    cve_only,
    advance_release,
    permissive,
    noop,
):
    """Find OCP bugs and (optional) add them to ADVISORY.

     The --group automatically determines the correct target-releases to search
    for bugs claimed to be fixed, but not yet attached to advisories.
    Security Tracker Bugs are validated with attached builds to advisories.
    If expected builds are not found then tracker bugs are not attached.
    default statuses: ['MODIFIED', 'ON_QA', 'VERIFIED']

    Using --use-default-advisory without a value set for the matching key
    in the build-data will cause an error and elliott will exit in a
    non-zero state. Use of this option silently overrides providing an
    advisory with the --add option.

        List bugs that WOULD be swept into advisories (NOOP):

    \b
        $ elliott -g openshift-4.8 --assembly 4.8.32 find-bugs:sweep

        Sweep bugs for an assembly into the advisories defined

    \b
        $ elliott -g openshift-4.8 --assembly 4.8.32 find-bugs:sweep --into-default-advisories

        Sweep rpm bugs into the rpm advisory defined

    \b
        $ elliott -g openshift-4.8 --assembly 4.8.32 find-bugs:sweep --use-default-advisory rpm

    """
    count_advisory_attach_flags = sum(map(bool, [default_advisory_type, into_default_advisories]))
    if count_advisory_attach_flags > 1:
        raise click.BadParameter("Use only one of --use-default-advisory <KIND> or --into-default-advisories")

    if runtime.build_system == 'konflux' and count_advisory_attach_flags:
        raise click.BadParameter("Attaching bugs with --build-system=konflux is not supported. ")

    runtime.initialize(mode="both")
    find_bugs_obj = FindBugsSweep(cve_only=cve_only)
    find_bugs_obj.include_status(include_status)
    find_bugs_obj.exclude_status(exclude_status)

    bug_tracker = runtime.get_bug_tracker('jira')

    bugs_dict: type_bug_list = await find_bugs(
        runtime,
        find_bugs_obj,
        bug_tracker,
        advance_release=advance_release,
        noop=noop,
        permissive=permissive,
    )

    if count_advisory_attach_flags:
        advisories_by_kind = [default_advisory_type] if default_advisory_type else runtime.get_default_advisories()
        bugs_dict = await attach_bugs(
            bugs_dict,
            advisories_by_kind,
            bug_tracker,
            noop=noop,
            debug=runtime.debug,
        )

    bugs = []
    for bug_set in bugs_dict.values():
        bugs.extend([b.id for b in bug_set])

    if output == 'text':
        click.echo(f"Found {len(bugs)} bugs")
        if bugs:
            click.echo(", ".join(sorted(bugs)))
    elif output == 'json':
        bugs_dict_formatted = {key: sorted([b.id for b in bug_set]) for key, bug_set in bugs_dict.items()}
        click.echo(json.dumps(bugs_dict_formatted, indent=4))

    if report:
        print_report(bugs, output)

    sys.exit(0)


async def get_bugs_sweep(runtime: Runtime, find_bugs_obj, bug_tracker):
    bugs = find_bugs_obj.search(bug_tracker_obj=bug_tracker, verbose=runtime.debug)
    if bugs:
        sweep_cutoff_timestamp = await get_sweep_cutoff_timestamp(runtime)
        if sweep_cutoff_timestamp:
            utc_ts = datetime.fromtimestamp(sweep_cutoff_timestamp, tz=timezone.utc)
            logger.info(
                f"Filtering bugs that have changed ({len(bugs)}) to one of the desired statuses before the "
                f"cutoff time {utc_ts}..."
            )
            qualified_bugs = []
            unqualified_bugs = []
            for chunk_of_bugs in chunk(bugs, constants.BUG_LOOKUP_CHUNK_SIZE):
                qualified_bugs_chunk = bug_tracker.filter_bugs_by_cutoff_event(
                    chunk_of_bugs, find_bugs_obj.status, sweep_cutoff_timestamp, verbose=runtime.debug
                )
                qualified_bugs.extend(qualified_bugs_chunk)
                not_qualified = {b.id for b in chunk_of_bugs} - {b.id for b in qualified_bugs_chunk}
                unqualified_bugs.extend(list(not_qualified))
            if unqualified_bugs:
                logger.debug(f"These bugs did not qualify cutoff time {utc_ts}: {sorted(unqualified_bugs)}")
            logger.info(f"{len(qualified_bugs)} of {len(bugs)} bugs are qualified for the cutoff time {utc_ts}")
            bugs = qualified_bugs

        # filter bugs that have been swept into other advisories
        logger.info("Filtering bugs that haven't been attached to any advisories...")
        attached_bugs = await bug_tracker.filter_attached_bugs(bugs)
        if attached_bugs:
            attached_bug_ids = {b.id for b in attached_bugs}
            logger.debug(f"Bugs attached to other advisories: {sorted(attached_bug_ids)}")
            bugs = [b for b in bugs if b.id not in attached_bug_ids]
            logger.info(f"Filtered {len(attached_bugs)} bugs since they are attached to other advisories")

    included_bug_ids, excluded_bug_ids = get_assembly_bug_ids(runtime, bug_tracker_type=bug_tracker.type)
    if included_bug_ids & excluded_bug_ids:
        raise ValueError(
            f"The following {bug_tracker.type} bugs are defined in both 'include' and 'exclude': "
            f"{included_bug_ids & excluded_bug_ids}"
        )
    if included_bug_ids:
        logger.warning(
            f"The following {bug_tracker.type} bugs will be additionally included because they are "
            f"explicitly defined in the assembly config: {included_bug_ids}"
        )
        # filter out bugs that are already swept in
        # so that we don't double add them
        bug_ids = {b.id for b in bugs}
        included_bug_ids = included_bug_ids - bug_ids
        included_bugs = bug_tracker.get_bugs(included_bug_ids)
        if find_bugs_obj.cve_only:
            logger.info("checking if cve tracker bug found in included bug list")
            included_bugs = [ib for ib in included_bugs if ib.is_tracker_bug()]
            logger.info(
                f"filtered cve tracker bug from included bug list: {[getattr(ib, 'id') for ib in included_bugs]}"
            )
        bugs.extend(included_bugs)
    if excluded_bug_ids:
        logger.warning(
            f"The following {bug_tracker.type} bugs will be excluded because they are explicitly "
            f"defined in the assembly config: {excluded_bug_ids}"
        )
        bugs = [bug for bug in bugs if bug.id not in excluded_bug_ids]

    return bugs


async def find_bugs(
    runtime: Runtime,
    find_bugs_obj: FindBugsMode,
    bug_tracker: BugTracker,
    advance_release: bool = False,
    noop: bool = False,
    permissive: bool = False,
) -> Dict[str | int, type_bug_set]:
    """Find bugs based on the given find_bugs_obj and bug_tracker.
    Attach them to the advisory if specified.

    returns a dict of
    - {kind: bugs} if bugs are not requested to be attached, where kind is the advisory type for which bugs were found e.g "rpm", "image", "extras", "microshift"
    - {advisory_id: bugs} if bugs are requested to be attached, where advisory_id is the advisory for which bugs were found and attached
    """

    statuses = sorted(find_bugs_obj.status)
    tr = bug_tracker.target_release()
    logger.info(f"Searching {bug_tracker.type} for bugs with status {statuses} and target releases: {tr}\n")

    bugs = await get_bugs_sweep(runtime, find_bugs_obj, bug_tracker)
    bugs_by_type = init_bugs_by_kind_dict(advance_release)
    if not bugs:
        logger.info(f"No qualified {bug_tracker.type} bugs found")
        return bugs_by_type

    advisory_ids = runtime.get_default_advisories()
    included_bug_ids, _ = get_assembly_bug_ids(runtime, bug_tracker_type=bug_tracker.type)
    major_version, minor_version = runtime.get_major_minor()
    categorize_bugs_by_type(
        bugs,
        bugs_by_type,
        advisory_ids,
        included_bug_ids,
        noop,
        permissive=permissive,
        major_version=major_version,
        minor_version=minor_version,
    )
    for kind, kind_bugs in bugs_by_type.items():
        logger.info(f'{kind} bugs: {[b.id for b in kind_bugs]}')

    return bugs_by_type


async def attach_bugs(
    bugs_by_kind: Dict[str, type_bug_set],
    advisories_by_kind: Dict[str, int],
    bug_tracker: BugTracker,
    noop: bool = False,
    debug: bool = False,
) -> Dict[str | int, type_bug_set]:
    """Attach bugs to advisories based on the given bugs_by_kind and advisories_by_kind.
    returns a dict of {advisory_id: bugs} where advisory_id is the advisory for which bugs were found and attached
    """

    attached_bugs_by_advisory = {}
    for kind, advisory_id in advisories_by_kind.items():
        kind_bugs = bugs_by_kind.get(kind, set())
        if kind_bugs:
            bug_tracker.attach_bugs([b.id for b in kind_bugs], advisory_id=advisory_id, noop=noop, verbose=debug)
        attached_bugs_by_advisory[advisory_id] = kind_bugs
    return attached_bugs_by_advisory


def get_assembly_bug_ids(runtime, bug_tracker_type):
    # Loads included/excluded bugs from assembly config
    issues_config = assembly_issues_config(runtime.get_releases_config(), runtime.assembly)
    included_bug_ids = {i["id"] for i in issues_config.include}
    excluded_bug_ids = {i["id"] for i in issues_config.exclude}

    if bug_tracker_type == 'jira':
        included_bug_ids = {i for i in included_bug_ids if JIRABug.looks_like_a_jira_bug(i)}
        excluded_bug_ids = {i for i in excluded_bug_ids if JIRABug.looks_like_a_jira_bug(i)}
    elif bug_tracker_type == 'bugzilla':
        included_bug_ids = {i for i in included_bug_ids if not JIRABug.looks_like_a_jira_bug(i)}
        excluded_bug_ids = {i for i in excluded_bug_ids if not JIRABug.looks_like_a_jira_bug(i)}
    return included_bug_ids, excluded_bug_ids


def init_bugs_by_kind_dict(advance_release: bool = False) -> Dict[str, type_bug_set]:
    """Returns a dict of {advisory_type: bugs}"""
    operator_bundle_advisory = "advance" if advance_release else "metadata"
    return {
        "rpm": set(),
        "image": set(),
        "extras": set(),
        # Metadata advisory will not have Bugs for z-stream releases
        # But at GA time it can have operator builds for the early operator release
        # and thus related extras bugs (including trackers and flaws) will need to be attached to it
        # If operator_bundle_advisory is set to 'advance' we consider the advance advisory as the metadata advisory
        # advance advisory will have bugs while metadata advisory will not, until GA.
        operator_bundle_advisory: set(),
        "microshift": set(),
    }


def categorize_bugs_by_type(
    bugs: List[Bug],
    bugs_by_type: Dict[str, type_bug_set],
    advisory_id_map: Dict[str, int],
    permitted_bug_ids,
    noop,
    major_version: int,
    minor_version: int,
    permissive=False,
):
    """Categorize bugs into different types of advisories
    :return: (bugs_by_type, issues) where bugs_by_type is a dict of {advisory_type: bugs} and issues is a list of issues
    """
    issues = []

    # for 3.x, all bugs should go to the rpm advisory
    if int(major_version) < 4:
        bugs_by_type["rpm"] = set(bugs)
        return bugs_by_type, issues

    # for 4.x, first sort all non_tracker_bugs
    tracker_bugs: type_bug_set = set()
    non_tracker_bugs: type_bug_set = set()
    fake_trackers: type_bug_set = set()

    for b in bugs:
        if b.is_tracker_bug():
            tracker_bugs.add(b)
        else:
            non_tracker_bugs.add(b)
            if b.is_invalid_tracker_bug():
                fake_trackers.add(b)

    bugs_by_type["extras"] = extras_bugs(non_tracker_bugs)
    remaining = non_tracker_bugs - bugs_by_type["extras"]
    bugs_by_type["microshift"] = {b for b in remaining if b.component and b.component.startswith('MicroShift')}
    remaining = remaining - bugs_by_type["microshift"]
    bugs_by_type["image"] = remaining

    if fake_trackers:
        message = f"Bug(s) {[t.id for t in fake_trackers]} look like CVE trackers, but really are not."
        if permissive:
            logger.warning(f"{message} Ignoring them.")
            issues.append(message)
        else:
            raise ElliottFatalError(f"{message} Please fix.")

    if not tracker_bugs:
        return bugs_by_type, issues

    logger.info(f"Tracker Bugs found: {len(tracker_bugs)}")

    for b in tracker_bugs:
        logger.info(f'Tracker bug, component: {(b.id, b.whiteboard_component)}')
        # get summary of tracker-bug and update it if needed
        summary_suffix = f"[openshift-{major_version}.{minor_version}]"
        if not b.summary.endswith(summary_suffix):
            new_s = fix_summary_suffix(b.summary, summary_suffix)
            try:
                b.update_summary(new_s, noop=noop)
            except Exception as e:
                logger.warning("Failed to fix summary: %s", str(e))

    if not advisory_id_map:
        logger.warning(
            "Skipping categorizing Tracker Bugs; advisories with attached builds must be given for this operation."
        )
        return bugs_by_type, issues

    logger.info("Validating tracker bugs with builds in advisories..")
    found = set()
    for kind in bugs_by_type.keys():
        if len(found) == len(tracker_bugs):
            break
        advisory = advisory_id_map.get(kind)
        if not advisory:
            continue
        attached_builds = errata.get_advisory_nvrs(advisory)
        packages = set(attached_builds.keys())
        exception_packages = []
        if kind == 'image':
            # golang builder is a special tracker component
            # which applies to all our golang images
            exception_packages.append(constants.GOLANG_BUILDER_CVE_COMPONENT)

        for bug in tracker_bugs:
            package_name = bug.whiteboard_component
            if kind == "microshift" and package_name == "microshift" and len(packages) == 0:
                # microshift is special since it has a separate advisory, and it's build is attached
                # after payload is promoted. So do not pre-emptively complain
                logger.info(
                    f"skip attach microshift bug {bug.id} to {advisory} because this advisory has no builds attached"
                )
                found.add(bug)
            elif (package_name in packages) or (package_name in exception_packages):
                if package_name in packages:
                    logger.info(f"{kind} build found for #{bug.id}, {package_name} ")
                if package_name in exception_packages:
                    logger.info(f"{package_name} bugs included by default")
                found.add(bug)
                bugs_by_type[kind].add(bug)
            elif package_name == "rhcos" and packages & arch_util.RHCOS_BREW_COMPONENTS:
                # rhcos trackers are special, since they have per-architecture component names
                # (rhcos-x86_64, rhcos-aarch64, ...) in Brew,
                # but the tracker bug has a generic "rhcos" component name
                logger.info(f"{kind} build found for #{bug.id}, {package_name} ")
                found.add(bug)
                bugs_by_type[kind].add(bug)

    not_found = set(tracker_bugs) - found
    if not_found:
        still_not_found = not_found
        if permitted_bug_ids:
            logger.info(
                'The following bugs will be included because they are '
                f'explicitly included in the assembly config: {permitted_bug_ids}'
            )
            still_not_found = {b for b in not_found if b.id not in permitted_bug_ids}

        if still_not_found:
            still_not_found_with_component = [(b.id, b.whiteboard_component) for b in still_not_found]
            message = (
                'No attached builds found in advisories for tracker bugs (bug, package): '
                f'{still_not_found_with_component}. Either attach builds or explicitly include/exclude the bug '
                f'ids in the assembly definition'
            )
            if permissive:
                logger.warning(f"{message} Ignoring them because --permissive.")
                issues.append(message)
            else:
                raise ValueError(message)

    return bugs_by_type, issues


def extras_bugs(bugs: type_bug_set) -> type_bug_set:
    # optional operators bugs should be swept to the "extras" advisory
    # a way to identify operator-related bugs is by its "Component" value.
    # temporarily hardcode here until we need to move it to ocp-build-data.
    extras_components = {
        "Logging",
        "Service Brokers",
        "Metering Operator",
        "Node Feature Discovery Operator",
        "Cloud Native Events",
        "Telco Edge",
    }  # we will probably find more
    extras_subcomponents = {
        ("Networking", "SR-IOV"),
        ("Storage", "Local Storage Operator"),
        ("Cloud Native Events", "Hardware Event Proxy"),
        ("Cloud Native Events", "Hardware Event Proxy Operator"),
        ("Telco Edge", "TALO"),
    }
    extra_bugs = set()
    for bug in bugs:
        if bug.component in extras_components:
            extra_bugs.add(bug)
        elif bug.sub_component and (bug.component, bug.sub_component) in extras_subcomponents:
            extra_bugs.add(bug)
    return extra_bugs


def print_report(bugs: type_bug_list, output: str = 'text') -> None:
    approved_url = 'https://source.redhat.com/groups/public/openshift/openshift_wiki/openshift_bugzilla_process'
    if output == 'slack':
        for bug in bugs:
            if bug.release_blocker:
                click.echo(
                    "<{}|_Release blocker: Approved_> bug for <{}|{}> - {:<25s} ".format(
                        approved_url, bug.weburl, bug.id, bug.component
                    )
                )
            else:
                click.echo("<{}|{}> - {:<25s} ".format(bug.weburl, bug.id, bug.component))

    elif output == 'json':
        print(
            json.dumps(
                [
                    {
                        "id": bug.id,
                        "component": bug.component,
                        "status": bug.status,
                        "date": str(bug.creation_time_parsed()),
                        "summary": bug.summary[:60],
                        "url": bug.weburl,
                    }
                    for bug in bugs
                ],
                indent=4,
            )
        )

    else:  # output == 'text'
        green_print(
            "{:<13s} {:<25s} {:<12s} {:<7s} {:<10s} {:60s}".format(
                "ID", "COMPONENT", "STATUS", "SCORE", "AGE", "SUMMARY"
            )
        )
        for bug in bugs:
            days_ago = bug.created_days_ago()
            cf_pm_score = bug.cf_pm_score if hasattr(bug, "cf_pm_score") else '?'
            click.echo(
                "{:<13s} {:<25s} {:<12s} {:<7s} {:<3d} days   {:60s} ".format(
                    str(bug.id), bug.component, bug.status, cf_pm_score, days_ago, bug.summary[:60]
                )
            )


async def get_sweep_cutoff_timestamp(runtime):
    sweep_cutoff_timestamp = 0
    if runtime.build_system == 'brew' and runtime.assembly_basis_event:
        logger.info(f"Determining approximate cutoff timestamp from basis event {runtime.assembly_basis_event}...")
        brew_api = runtime.build_retrying_koji_client()
        sweep_cutoff_timestamp = await bzutil.approximate_cutoff_timestamp(
            runtime.assembly_basis_event, brew_api, runtime.rpm_metas() + runtime.image_metas()
        )
    elif runtime.build_system == 'konflux':
        sweep_cutoff_timestamp = runtime.assembly_basis_event.timestamp()

    return sweep_cutoff_timestamp
