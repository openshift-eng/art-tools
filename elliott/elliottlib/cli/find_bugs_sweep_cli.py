import json
import re
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import click
from artcommonlib import arch_util, logutil
from artcommonlib.assembly import assembly_config_struct, assembly_issues_config
from artcommonlib.format_util import green_print
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder

from elliottlib import Runtime, bzutil, constants, errata
from elliottlib.bzutil import Bug, BugTracker, JIRABug
from elliottlib.cli import common
from elliottlib.cli.common import click_coroutine
from elliottlib.exceptions import ElliottFatalError
from elliottlib.shipment_utils import get_builds_from_mr
from elliottlib.util import chunk, get_component_by_delivery_repo

logger = logutil.get_logger(__name__)
type_bug_list = List[Bug]
type_bug_set = Set[Bug]
yaml = new_roundtrip_yaml_handler()


class FindBugsMode:
    def __init__(self, status: List, cve_only: bool = False):
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
    def __init__(self, cve_only: bool = False):
        # Policy document: https://docs.google.com/document/d/1RkyN1hp1_mUqeks0kVzPscDeZVo-RDCYCTq6VLNB9i4/edit?tab=t.0#heading=h.m52kbuqe0dss
        # Accordingly, ART only sweeps VERIFIED by default.
        super().__init__(status={'VERIFIED'}, cve_only=cve_only)


@common.cli.command("find-bugs:sweep", short_help="Sweep qualified bugs into advisories")
@click.option("--add", "-a", 'advisory_id', type=int, metavar='ADVISORY', help="Add found bugs to ADVISORY")
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
    advisory_id,
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
    operator_bundle_advisory = "advance" if advance_release else "metadata"

    count_advisory_attach_flags = sum(map(bool, [advisory_id, default_advisory_type, into_default_advisories]))
    if count_advisory_attach_flags > 1:
        raise click.BadParameter("Use only one of --use-default-advisory, --add, or --into-default-advisories")

    if runtime.build_system == 'konflux':
        raise click.BadParameter("Do not use find-bugs:sweep with --build-system=konflux, instead use find-bugs")

    runtime.initialize(mode="both")
    find_bugs_obj = FindBugsSweep(cve_only=cve_only)
    find_bugs_obj.include_status(include_status)
    find_bugs_obj.exclude_status(exclude_status)

    bugs: type_bug_list = await find_and_attach_bugs(
        runtime,
        advisory_id,
        default_advisory_type,
        find_bugs_obj,
        noop=noop,
        permissive=permissive,
        bug_tracker=runtime.get_bug_tracker('jira'),
        operator_bundle_advisory=operator_bundle_advisory,
    )

    if not bugs:
        logger.info('No bugs found')
        sys.exit(0)

    if output == 'text':
        click.echo(f"Found {len(bugs)} bugs")
        click.echo(", ".join(sorted(str(b.id) for b in bugs)))

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


async def find_and_attach_bugs(
    runtime: Runtime,
    advisory_id,
    default_advisory_type,
    find_bugs_obj,
    noop,
    permissive,
    bug_tracker,
    operator_bundle_advisory,
):
    statuses = sorted(find_bugs_obj.status)
    tr = bug_tracker.target_release()
    logger.info(f"Searching {bug_tracker.type} for bugs with status {statuses} and target releases: {tr}\n")

    bugs = await get_bugs_sweep(runtime, find_bugs_obj, bug_tracker)
    if not bugs:
        logger.info(f"No qualified {bug_tracker.type} bugs found")
        return []

    advisory_ids = runtime.get_default_advisories()
    included_bug_ids, _ = get_assembly_bug_ids(runtime, bug_tracker_type=bug_tracker.type)
    major_version, minor_version = runtime.get_major_minor()

    builds_by_advisory_kind = get_builds_by_advisory_kind(runtime)

    bugs_by_type, _ = categorize_bugs_by_type(
        runtime=runtime,
        bugs=bugs,
        builds_by_advisory_kind=builds_by_advisory_kind,
        permitted_bug_ids=included_bug_ids,
        major_version=major_version,
        minor_version=minor_version,
        operator_bundle_advisory=operator_bundle_advisory,
        permissive=permissive,
    )
    for kind, kind_bugs in bugs_by_type.items():
        logger.info(f'{kind} bugs: {[b.id for b in kind_bugs]}')

    if runtime.build_system == 'konflux':
        return bugs

    if not any([advisory_id, default_advisory_type, advisory_ids]):
        return bugs
    # `--add ADVISORY_NUMBER` should respect the user's wish
    # and attach all available bugs to whatever advisory is specified.
    if advisory_id and not default_advisory_type:
        bug_tracker.attach_bugs([b.id for b in bugs], advisory_id=advisory_id, noop=noop, verbose=runtime.debug)
        return bugs

    if not advisory_ids:
        logger.info("No advisories to attach to")
        return bugs

    advisory_types_to_attach = [default_advisory_type] if default_advisory_type else bugs_by_type.keys()
    for advisory_type in sorted(advisory_types_to_attach):
        kind_bugs = bugs_by_type.get(advisory_type)
        if kind_bugs:
            if advisory_type not in advisory_ids:
                logger.warning(
                    f"Bugs were found for {advisory_type} but not attached because {advisory_type} advisory "
                    "does not exist"
                )
                continue
            bug_tracker.attach_bugs(
                [b.id for b in kind_bugs], advisory_id=advisory_ids[advisory_type], noop=noop, verbose=runtime.debug
            )
    return bugs


def get_builds_by_advisory_kind(runtime: Runtime) -> Dict[str, List[str]]:
    """Get builds attached to advisories by advisory kind, based on the build system.

    For brew:
    - fetch advisories from assembly.group.advisories
    - for each advisory, fetch attached builds from Errata Tool

    For konflux:
    - fetch advisories from assembly.group.shipment.advisories
    - for each shipment yaml file for an advisory in shipment MR, fetch builds from shipment spec

    :param runtime: Runtime object
    :return: Dict of {advisory_kind: [builds]} where builds is a list of NVRs (str) and kind is e.g. "rpm", "image", "extras", "microshift", "metadata"
    """
    # fetch builds from ET advisories
    builds_by_kind: Dict[str, List[str]] = {}
    if runtime.build_system == 'brew':
        advisory_ids = runtime.get_default_advisories()
        for kind, kind_advisory_id in advisory_ids.items():
            if int(kind_advisory_id) <= 0:
                logger.info(f"{kind} advisory is not initialized: {kind_advisory_id}")
                continue
            builds_by_kind[kind] = errata.get_advisory_nvrs_flattened(kind_advisory_id)
    elif runtime.build_system == 'konflux':
        # fetch builds from shipments
        assembly_group_config = assembly_config_struct(runtime.get_releases_config(), runtime.assembly, "group", {})
        shipment = assembly_group_config.get("shipment", {})
        mr_url = shipment.get("url")
        if not mr_url:
            logger.warning("No shipment URL found in assembly config, cannot fetch builds for advisories.")
        else:
            logger.info(f"Fetching builds from shipment URL: {mr_url}")
            builds_by_kind = get_builds_from_mr(mr_url)
    return builds_by_kind


def get_assembly_bug_ids(runtime, bug_tracker_type) -> tuple[Set[str], Set[str]]:
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


def categorize_bugs_by_type(
    runtime: Runtime,
    bugs: List[Bug],
    builds_by_advisory_kind: Dict[str, List[str]],
    major_version: int,
    minor_version: int,
    permitted_bug_ids: Optional[set] = None,
    operator_bundle_advisory: Optional[str] = "metadata",
    permissive: bool = False,
    exclude_trackers: bool = False,
) -> tuple[Dict[str, type_bug_set], List[str]]:
    """Categorize bugs into different types of advisories
    :param bugs: List of Bug objects to categorize
    :param builds_by_advisory_kind: Dict of {advisory_kind: [builds]} where builds are the NVRs attached to advisories
    :param major_version: Major version of the release
    :param minor_version: Minor version of the release
    :param permitted_bug_ids: Set of bug IDs that are explicitly permitted for inclusion
    :param operator_bundle_advisory: Type of advisory for operator bundles, defaults to "metadata"
    :param permissive: If True, ignore invalid bugs instead of raising an error
    :param exclude_trackers: If True, exclude tracker bugs from the categorization
    :return: (bugs_by_type, issues) where bugs_by_type is a dict of {advisory_kind: bug_ids} and issues is a list of problems found
    """

    # record problems with bugs found during categorization
    issues: List[str] = []

    # initialize dict to hold bug ids by advisory kind
    bugs_by_type: Dict[str, type_bug_set] = {
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

    # for 3.x, all bugs should go to the rpm advisory
    if int(major_version) < 4:
        bugs_by_type["rpm"] = set(bugs)
        return bugs_by_type, issues

    # for 4.x, first sort all non_tracker_bugs
    tracker_bugs: type_bug_set = set()
    non_tracker_bugs: type_bug_set = set()
    fake_trackers: type_bug_set = set()

    # Categorize into tracker and non-tracker bugs
    # while also collecting fake trackers
    for b in bugs:
        if b.is_tracker_bug():
            tracker_bugs.add(b)
        else:
            if b.is_invalid_tracker_bug():
                fake_trackers.add(b)
            else:
                non_tracker_bugs.add(b)

    # Categorize non-tracker bugs into different types
    # extras bugs go to extras advisory
    non_tracker_extras = extras_bugs(non_tracker_bugs)
    if non_tracker_extras:
        bugs_by_type["extras"].update(non_tracker_extras)
    non_tracker_bugs -= bugs_by_type["extras"]

    # If there is a distinct RHCOS advisory, RHCOS bugs should go there instead of the image advisory
    if "rhcos" in runtime.get_default_advisories():
        bugs_by_type["rhcos"] = rhcos_bugs(non_tracker_bugs)
        non_tracker_bugs -= bugs_by_type["rhcos"]

    # microshift bugs go to microshift advisory
    bugs_by_type["microshift"] = {b for b in non_tracker_bugs if b.component and b.component.startswith('MicroShift')}
    non_tracker_bugs -= bugs_by_type["microshift"]

    # remaining non-tracker bugs go to image advisory
    bugs_by_type["image"] = non_tracker_bugs

    # Complain about fake trackers
    if fake_trackers:
        sorted_ids = sorted([t.id for t in fake_trackers])
        message = f"Bug(s) {sorted_ids} look like CVE trackers, but really are not."
        if permissive:
            logger.warning(f"{message} Ignoring them.")
            issues.append(message)
        else:
            raise ElliottFatalError(f"{message} Please fix.")

    if exclude_trackers:
        logger.info("Excluding tracker bugs because --exclude-trackers is set")
        tracker_bugs = set()

    # Return early if there are no tracker bugs to process
    if not tracker_bugs:
        return bugs_by_type, issues

    # Process tracker bugs
    logger.info(f"Tracker Bugs found: {len(tracker_bugs)}")

    # Validate tracker bugs' summary suffixes
    invalid_summary_trackers: type_bug_set = set()
    for b in tracker_bugs:
        logger.info(f'Tracker bug, component: {(b.id, b.whiteboard_component)}')
        if not b.has_valid_target_version_in_summary(major_version, minor_version):
            invalid_summary_trackers.add(b)

    if invalid_summary_trackers:
        sorted_ids = sorted([t.id for t in invalid_summary_trackers])
        message = f"Tracker Bug(s) {sorted_ids} have invalid summary."
        if permissive:
            logger.warning(f"{message} Ignoring them.")
            issues.append(message)
        else:
            raise ElliottFatalError(f"{message} Please fix.")

        tracker_bugs -= invalid_summary_trackers

    # If advisories are not provided, we cannot categorize tracker bugs
    if not builds_by_advisory_kind:
        logger.warning(
            "Skipping categorizing Tracker Bugs; builds attached to advisories must be given for this operation. All "
            "tracker bugs are assumed to belong to `image` advisory"
        )
        bugs_by_type["image"].update(tracker_bugs)
        return bugs_by_type, issues

    logger.info("Validating tracker bugs with builds in advisories..")
    found = set()
    for kind in bugs_by_type.keys():
        if len(found) == len(tracker_bugs):
            break
        attached_nvrs = builds_by_advisory_kind.get(kind, [])
        packages = {parse_nvr(nvr)["name"] for nvr in attached_nvrs}
        exception_packages = []
        if kind == 'image':
            # golang builder is a special tracker component
            # which applies to all our golang images
            exception_packages.append(constants.GOLANG_BUILDER_CVE_COMPONENT)

        for bug in tracker_bugs:
            package_name = bug.whiteboard_component
            if "openshift4/" in package_name:
                package_name = get_component_by_delivery_repo(runtime, package_name)
            if kind == "microshift" and package_name == "microshift" and len(packages) == 0:
                # microshift is special since it has a separate advisory, and it's build is attached
                # after payload is promoted. So do not pre-emptively complain
                logger.info(
                    f"skip categorizing microshift bug {bug.id} to microshift advisory because advisory has no builds attached"
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

    def _message(kind: str, bug_data: list[tuple[str, str]]) -> str:
        advisory_type = "errata" if runtime.build_system == "brew" else "shipment"
        return (
            f'No attached builds found in {advisory_type} advisories for {kind} tracker bugs (bug, package): '
            f'{bug_data}. Either attach builds or explicitly include/exclude the bug ids in the assembly definition'
        )

    not_found = set(tracker_bugs) - found
    if not_found:
        still_not_found = not_found
        if permitted_bug_ids:
            logger.info(
                'The following tracker bugs will be included in image advisory because they are '
                f'explicitly included in the assembly config: {permitted_bug_ids}'
            )
            bugs_by_type["image"] = {b for b in not_found if b.id in permitted_bug_ids}
            still_not_found = {b for b in not_found if b.id not in permitted_bug_ids}

        if still_not_found:
            # We want to separate image and rpm tracker bug-build-validation since they are handled differently
            # builds from errata/rpm advisories are only fetched if --build-system=brew
            # builds from shipment/image advisories are only fetched if --build-system=konflux
            # so only complain about bugs for which builds were fetched

            # whiteboard value could be component or delivery repo name
            not_found_image_bugs = [
                b
                for b in still_not_found
                if b.whiteboard_component.endswith("-container") or "openshift4/" in b.whiteboard_component
            ]
            not_found_rpm_bugs = [b for b in still_not_found if b not in not_found_image_bugs]

            if runtime.build_system == "brew":
                message = _message("rpm", [(b.id, b.whiteboard_component) for b in not_found_rpm_bugs])
            else:
                message = _message("image", [(b.id, b.whiteboard_component) for b in not_found_image_bugs])

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


def rhcos_bugs(bugs: type_bug_set) -> type_bug_set:
    # RHCOS bugs should be swept to the "rhcos" advisory until RHCOS is moved to Konflux.
    # A way to identify RHCOS-related bugs is by its "Component" value.
    rhcos_components = {
        "RHCOS",
    }
    rhcos_bugs = {bug for bug in bugs if bug.component in rhcos_components}
    return rhcos_bugs


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

    # get timestamp from whatever basis event is available
    basis_event = runtime.assembly_basis_event
    if isinstance(basis_event, int):
        logger.info(f"Determining approximate cutoff timestamp from basis event {basis_event}...")
        brew_api = runtime.build_retrying_koji_client()
        sweep_cutoff_timestamp = await bzutil.approximate_cutoff_timestamp(
            runtime.assembly_basis_event, brew_api, runtime.rpm_metas() + runtime.image_metas()
        )
    elif isinstance(basis_event, datetime):
        sweep_cutoff_timestamp = basis_event.timestamp()

    return sweep_cutoff_timestamp
