import asyncio
import re
import json
import yaml
from typing import Any, Dict, Iterable, List, Set, Tuple, Union
import click
import logging

from errata_tool import ErrataException

from artcommonlib import logutil, arch_util
from artcommonlib.assembly import assembly_issues_config
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import is_release_next_week
from elliottlib import bzutil, constants
from elliottlib.cli.common import cli, click_coroutine, pass_runtime
from elliottlib.errata_async import AsyncErrataAPI, AsyncErrataUtils
from elliottlib.runtime import Runtime
from elliottlib.util import minor_version_tuple
from elliottlib.bzutil import Bug
from elliottlib.errata import get_bug_ids, sync_jira_issue
from elliottlib.cli.attach_cve_flaws_cli import get_flaws
from elliottlib.cli.find_bugs_sweep_cli import FindBugsSweep, categorize_bugs_by_type
from elliottlib.errata import is_advisory_editable
from elliottlib.verify_issue import VerifyIssueCode, VerifyIssue, stringify

logger = logging.getLogger(__name__)

output_option = click.option('--output', '-o',
                             required=False,
                             type=click.Choice(['text', 'json', 'slack']),
                             default='text',
                             help='Applies chosen format to command output')


@cli.command("verify-attached-bugs",
             short_help="Run validations on bugs attached to release advisories and report results")
@click.option("--verify-bug-status/--no-verify-bug-status", is_flag=True,
              help="Check that bugs of advisories are all ON_QA or more",
              type=bool, default=True)
@click.option("--verify-flaws", is_flag=True,
              help="Check that flaw bugs are attached and associated with respective builds",
              type=bool, default=False)
@click.option("--no-verify-blocking-bugs", is_flag=True,
              help="Don't check if there are open bugs for the next minor version blocking bugs for this minor version",
              type=bool, default=False)
@click.option("--skip-multiple-advisories-check", is_flag=True,
              help="Do not check if bugs are attached to multiple advisories",
              type=bool, default=False)
@click.argument("advisories", nargs=-1, type=click.IntRange(1), required=False)
@output_option
@pass_runtime
@click_coroutine
async def verify_attached_bugs_cli(runtime: Runtime, verify_bug_status: bool, advisories: Tuple[int, ...], verify_flaws: bool,
                                   no_verify_blocking_bugs: bool, skip_multiple_advisories_check: bool, output: str):
    """
    Validate bugs in release advisories (specified in arguments or as part of an assembly).
    Requires group param (-g openshift-X.Y)

    Default validations:
    - Target Release: All bugs belong to the group's target release.
    - Bug Sorting: Bugs are attached to the correct advisory type (image/rpm/extras) using
    find-bugs:sweep logic. This only runs with --assembly. To skip manually specify advisories.
    - Improper Tracker Bugs: Bugs that have CVE in their title but do not have Tracker Bug labels set.
    - Regression Check: If there are any backport bugs, make sure they don't get ahead of their original bug.
    This is to make sure we don't regress when upgrading from OCP X.Y to X.Y+1. To skip use --no-verify-blocking-bugs.
    - Bugs aren't attached to multiple advisories. To skip use --skip-multiple-advisories-check.

    Additional validations:
    - Bug Status (--verify-bug-status): All bugs are in at least in ON_QA state
    - Flaw Bugs (--verify-flaws): All flaw bugs are attached to advisories (using attach-cve-flaws logic) and
    associated with respective builds. Verify advisory type is RHSA when applicable, and CVE names field is correct.

    If any verification fails, a text explanation is given and the return code is 1.
    Otherwise, prints the number of bugs in the advisories and exits with success.
    """
    runtime.initialize()
    if advisories:
        logger.warning("Cannot verify advisory bug sorting. To verify that bugs are attached to the "
                       "correct release advisories, run with --assembly=<release>")
        advisory_id_map = {'?': a for a in advisories}
    else:
        advisory_id_map = runtime.get_default_advisories()
    if not advisory_id_map:
        logger.error("No advisories specified on command line or in [group.yml|releases.yml]")
        exit(1)
    await verify_attached_bugs(runtime, verify_bug_status, advisory_id_map, verify_flaws, no_verify_blocking_bugs,
                               skip_multiple_advisories_check, output)


async def verify_attached_bugs(runtime: Runtime, verify_bug_status: bool, advisory_id_map: Dict[str, int], verify_flaws:
                               bool, no_verify_blocking_bugs: bool, skip_multiple_advisories_check: bool, output: str):
    validator = BugValidator(runtime, output=output)
    advisory_bug_map = validator.get_attached_bugs(list(advisory_id_map.values()))
    bugs = {b for bugs in advisory_bug_map.values() for b in bugs}

    # bug.is_ocp_bug() filters by product/project, so we don't get flaw bugs or bugs of other products or
    # placeholder
    non_flaw_bugs = [b for b in bugs if b.is_ocp_bug()]

    try:
        validator.validate(non_flaw_bugs, verify_bug_status, no_verify_blocking_bugs, is_attached=True)

        # skip advisory type check if advisories are
        # manually passed in, and we don't know their type
        if '?' not in advisory_id_map.keys():
            included_bug_ids = set()
            if runtime.assembly:
                issues_config = assembly_issues_config(runtime.get_releases_config(), runtime.assembly)
                included_bug_ids = {issue["id"] for issue in issues_config.include}
            validator.verify_bugs_advisory_type(non_flaw_bugs, advisory_id_map, advisory_bug_map, included_bug_ids)

        if not skip_multiple_advisories_check:
            await validator.verify_bugs_multiple_advisories(non_flaw_bugs)

        if verify_flaws:
            await validator.verify_attached_flaws(advisory_bug_map)
    except Exception as e:
        validator._complain(VerifyIssue(
            code=VerifyIssueCode.VALIDATION_ERROR,
            message=f"Error validating attached bugs: {type(e)}: {e}"
        ))
    finally:
        await validator.close()
        validator.report()


@cli.command("verify-bugs", short_help="Verify bugs included in an assembly (default --assembly=stream)")
@click.option("--verify-bug-status", is_flag=True, help="Check that bugs of advisories are all ON_QA or more",
              type=bool, default=False)
@click.option("--no-verify-blocking-bugs", is_flag=True,
              help="Don't check if there are open bugs for the next minor version blocking bugs for this minor version",
              type=bool, default=False)
@output_option
@pass_runtime
@click_coroutine
async def verify_bugs_cli(runtime, verify_bug_status, output, no_verify_blocking_bugs: bool):
    """
    Validate bugs that qualify as being part of an assembly (specified as --assembly)
    Requires group param (-g openshift-X.Y). By default runs for --assembly=stream

    Default validations:
    - Target Release: All bugs belong to the group's target release.
    - Improper Tracker Bugs: Bugs that have CVE in their title but do not have Tracker Bug labels set.
    - Regression Check: If there are any backport bugs, make sure they don't get ahead of their original bug.
    This is to make sure we don't regress when upgrading from OCP X.Y to X.Y+1. To skip use --no-verify-blocking-bugs.

    Additional validations:
    - Bug Status (--verify-bug-status): All bugs are at least in VERIFIED state

    """
    runtime.initialize()
    await verify_bugs(runtime, verify_bug_status, output, no_verify_blocking_bugs)


async def verify_bugs(runtime, verify_bug_status, output, no_verify_blocking_bugs):
    validator = BugValidator(runtime, output)
    find_bugs_obj = FindBugsSweep(cve_only=False)
    ocp_bugs = []
    logger.info(f'Using {runtime.assembly} assembly to search bugs')
    for b in [runtime.get_bug_tracker('jira'), runtime.get_bug_tracker('bugzilla')]:
        bugs = find_bugs_obj.search(bug_tracker_obj=b, verbose=runtime.debug)
        logger.info(f"Found {len(bugs)} {b.type} bugs: {[b.id for b in bugs]}")
        ocp_bugs.extend(bugs)

    try:
        validator.validate(ocp_bugs, verify_bug_status, no_verify_blocking_bugs)
    except Exception as e:
        validator._complain(VerifyIssue(
            code=VerifyIssueCode.VALIDATION_ERROR,
            message=f"Error validating bugs: {type(e)}: {e}"
        ))
    finally:
        await validator.close()
        validator.report()


class BugValidator:

    def __init__(self, runtime: Runtime, output: str = 'text'):
        self.runtime = runtime
        self.target_releases: List[str] = runtime.get_bug_tracker('jira').config['target_release']
        self.et_data: Dict[str, Any] = runtime.get_errata_config()
        self.errata_api = AsyncErrataAPI(self.et_data.get("server", constants.errata_url))
        self.problems: List[VerifyIssue] = []
        self.output = output

    async def close(self):
        await self.errata_api.close()

    def report(self):
        if self.problems:
            self.problems = [p.to_dict() for p in self.problems]
            if self.output == 'text':
                print("Found the following problems, please investigate")
                print(yaml.dump(self.problems, indent=2, sort_keys=False, width=float("inf")))
            elif self.output == 'json':
                print(json.dumps(self.problems, indent=2, sort_keys=False))
            elif self.output == 'slack':
                for problem in self.problems:
                    print(problem)
            exit(1)

    def validate(self, non_flaw_bugs: List[Bug], verify_bug_status: bool, no_verify_blocking_bugs: bool,
                 is_attached: bool = False):
        if verify_bug_status:
            self._verify_bug_status(non_flaw_bugs)

        non_flaw_bugs = self.filter_bugs_by_release(non_flaw_bugs, complain=True)
        self._find_invalid_trackers(non_flaw_bugs)

        # Make sure the next version is GA before regression check
        major, minor = self.runtime.get_major_minor()
        version = f"{major}.{minor + 1}"
        try:
            next_is_ga = self.runtime.is_version_in_lifecycle_phase("release", version)
        except Exception as e:
            logger.warning(f"Failed to determine if {version} is GA: {e}. Assuming it is not GA.")
            next_is_ga = False
        if not next_is_ga:
            no_verify_blocking_bugs = True
            logger.info(f"Skipping regression check because {version} is not GA")

        if not no_verify_blocking_bugs:
            blocking_bugs_for = self._get_blocking_bugs_for(non_flaw_bugs)
            self._verify_blocking_bugs(blocking_bugs_for, is_attached=is_attached)

    def verify_bugs_advisory_type(self, non_flaw_bugs, advisory_id_map, advisory_bug_map, permitted_bug_ids):
        advance_release = False
        if "advance" in advisory_id_map and is_advisory_editable(advisory_id_map["advance"]):
            advance_release = True
        operator_bundle_advisory = "advance" if advance_release else "metadata"

        bugs_by_type: Dict[str, Set]
        issues: List[VerifyIssue]
        bugs_by_type, issues = categorize_bugs_by_type(non_flaw_bugs, advisory_id_map,
                                                       permitted_bug_ids=permitted_bug_ids,
                                                       permissive=True,
                                                       operator_bundle_advisory=operator_bundle_advisory)

        # ignore this issue since we have our own dedicated check for this
        issues = [i for i in issues if i.code != VerifyIssueCode.INVALID_TRACKER_BUGS]

        for kind, advisory_id in advisory_id_map.items():
            actual = {b for b in advisory_bug_map[advisory_id] if b.is_ocp_bug()}

            # If impetus is not present, assume no bugs need to be attached
            expected = bugs_by_type.get(kind, set())
            if not expected:
                # Looks like impetus is not part of the ones we care about. Skipping bug attachment
                continue
            if actual != expected:
                bugs_not_found = expected - actual
                if bugs_not_found:
                    bugs = [b.id for b in bugs_not_found]
                    message = f'Expected Bugs not found in {kind} advisory ({advisory_id}): {stringify(bugs)}'
                    issue = VerifyIssue(
                        code=VerifyIssueCode.MISSING_BUGS_IN_ADVISORY,
                        message=message,
                    )
                    issues.append(issue)

                extra_bugs = actual - expected
                if extra_bugs:
                    bugs = [b.id for b in extra_bugs]
                    message = f'Unexpected Bugs found in {kind} advisory ({advisory_id}): {stringify(bugs)}'
                    issue = VerifyIssue(
                        code=VerifyIssueCode.EXTRA_BUGS_IN_ADVISORY,
                        message=message,
                    )
                    issues.append(issue)
        self._complain(issues)

    async def verify_bugs_multiple_advisories(self, non_flaw_bugs: List[Bug]):
        logger.info(f'Checking {len(non_flaw_bugs)} bugs, if any bug is attached to multiple advisories')

        async def get_all_advisory_ids(bug):
            try:
                all_advisories_id = bug.all_advisory_ids()
            except ErrataException:
                try:
                    sync_jira_issue(bug.id)
                except Exception as e:
                    return f'Failed to sync bug {bug.id}: {e}'
            if len(all_advisories_id) > 1:
                return bug.id, all_advisories_id
            return None

        results = await asyncio.gather(*[asyncio.create_task(get_all_advisory_ids(bug)) for bug in non_flaw_bugs])
        filtered_results = [r for r in results if r]
        if not filtered_results:
            return
        issue = VerifyIssue(
            code=VerifyIssueCode.BUGS_MULTIPLE_ADVISORIES,
            message=f"Bugs are attached to multiple advisories: {stringify(filtered_results)}",
        )
        self._complain(issue)

    async def verify_attached_flaws(self, advisory_bugs: Dict[int, List[Bug]]):
        futures = []
        for advisory_id, attached_bugs in advisory_bugs.items():
            attached_trackers = [b for b in attached_bugs if b.is_tracker_bug()]
            attached_flaws = [b for b in attached_bugs if b.is_flaw_bug()]
            logger.info(f"Verifying advisory {advisory_id}: attached-trackers: "
                        f"{[b.id for b in attached_trackers]} "
                        f"attached-flaws: {[b.id for b in attached_flaws]}")
            futures.append(self._verify_attached_flaws_for(advisory_id, attached_trackers, attached_flaws))
        await asyncio.gather(*futures)

    async def _verify_attached_flaws_for(self, advisory_id: int, attached_trackers: Iterable[Bug], attached_flaws: Iterable[Bug]):
        flaw_bug_tracker = self.runtime.get_bug_tracker('bugzilla')
        brew_api = self.runtime.build_retrying_koji_client()
        tracker_flaws, first_fix_flaw_bugs = get_flaws(flaw_bug_tracker, attached_trackers, brew_api)

        # Check if attached flaws match expected flaws
        first_fix_flaw_ids = {b.id for b in first_fix_flaw_bugs}
        attached_flaw_ids = {b.id for b in attached_flaws}
        missing_flaw_ids = first_fix_flaw_ids - attached_flaw_ids
        issues: List[VerifyIssue] = []
        if missing_flaw_ids:
            message = (f"On advisory {advisory_id}, these flaw bugs are not attached but they are referenced by "
                       "attached tracker bugs. You need to attach those flaw bugs or drop corresponding tracker bugs: "
                       f"{stringify(missing_flaw_ids)}")
            issue = VerifyIssue(
                code=VerifyIssueCode.MISSING_FLAW_BUGS,
                message=message,
            )
            issues.append(issue)
        extra_flaw_ids = attached_flaw_ids - first_fix_flaw_ids
        if extra_flaw_ids:
            message = (f"On advisory {advisory_id}, these flaw bugs are attached but there are no tracker bugs "
                       "referencing them. You need to drop those flaw bugs or attach corresponding tracker bugs: "
                       f"{stringify(extra_flaw_ids)}")
            issue = VerifyIssue(
                code=VerifyIssueCode.EXTRA_FLAW_BUGS,
                message=message,
            )
            issues.append(issue)

        # Check if advisory is of the expected type
        advisory_info = await self.errata_api.get_advisory(advisory_id)
        advisory_type = next(iter(advisory_info["errata"].keys())).upper()  # should be one of [RHBA, RHSA, RHEA]
        if not first_fix_flaw_ids:
            if advisory_type == "RHSA":
                msg = (f"Advisory {advisory_id} is of type {advisory_type} but has no first-fix flaw bugs. "
                       "It should be converted to RHBA or RHEA.")
                issue = VerifyIssue(
                    code=VerifyIssueCode.WRONG_ADVISORY_TYPE,
                    message=msg
                )
                issues.append(issue)
            self._complain(issues)
            return  # The remaining checks are not needed for a non-RHSA.
        if advisory_type != "RHSA":
            msg = (f"Advisory {advisory_id} is of type {advisory_type} but has first-fix flaw bugs attached: "
                   f"{stringify(first_fix_flaw_ids)}. It should be converted to RHSA.")
            issue = VerifyIssue(
                code=VerifyIssueCode.WRONG_ADVISORY_TYPE,
                message=msg
            )
            issues.append(issue)

        # Get attached builds
        attached_builds = await self.errata_api.get_builds_flattened(advisory_id)
        attached_components = {parse_nvr(build)["name"] for build in attached_builds}

        # Validate CVE mappings (of CVEs and builds)
        flaw_id_bugs = {f.id: f for f in first_fix_flaw_bugs}
        cve_components_mapping: Dict[str, Set[str]] = {}
        for tracker in attached_trackers:
            whiteboard_component = tracker.whiteboard_component
            if not whiteboard_component:
                raise ValueError(f"Bug {tracker.id} doesn't have a valid whiteboard component.")
            if whiteboard_component == "rhcos":
                # rhcos trackers are special, since they have per-architecture component names
                # (rhcos-x86_64, rhcos-aarch64, ...) in Brew,
                # but the tracker bug has a generic "rhcos" component name
                # so we need to associate this CVE with all per-architecture component names
                component_names = attached_components & arch_util.RHCOS_BREW_COMPONENTS
            else:
                component_names = {whiteboard_component}
            flaw_ids = tracker_flaws[tracker.id]
            for flaw_id in flaw_ids:
                if flaw_id not in flaw_id_bugs:  # This means associated flaw wasn't considered a first fix
                    continue
                alias = [k for k in flaw_id_bugs[flaw_id].alias if k.startswith('CVE-')]
                if len(alias) != 1:
                    raise ValueError(f"Bug {flaw_id} should have exactly 1 CVE alias.")
                cve = alias[0]
                cve_components_mapping.setdefault(cve, set()).update(component_names)

        try:
            extra_exclusions, missing_exclusions = await AsyncErrataUtils.validate_cves_and_get_exclusions_diff(
                self.errata_api,
                advisory_id, attached_builds,
                cve_components_mapping)
            for cve, cve_package_exclusions in extra_exclusions.items():
                if cve_package_exclusions:
                    msg = (f"On advisory {advisory_id}, {cve} is not associated with Brew components "
                           f"{stringify(cve_package_exclusions)}."
                           " You may need to associate the CVE with the components "
                           "in the CVE mapping or drop the tracker bugs.")
                    issue = VerifyIssue(
                        code=VerifyIssueCode.EXTRA_CVE_EXCLUSIONS,
                        message=msg
                    )
                    issues.append(issue)
            for cve, cve_package_exclusions in missing_exclusions.items():
                if cve_package_exclusions:
                    msg = (f"On advisory {advisory_id}, {cve} is associated with Brew components "
                           f"{stringify(cve_package_exclusions)} without a tracker bug."
                           " You may need to explicitly exclude those Brew components from the CVE "
                           "mapping or attach the corresponding tracker bugs.")
                    issue = VerifyIssue(
                        code=VerifyIssueCode.MISSING_CVE_EXCLUSIONS,
                        message=msg
                    )
                    issues.append(issue)
        except ValueError as e:
            issues.append(VerifyIssue(
                code=VerifyIssueCode.VALIDATION_ERROR,
                message=f"Error validating cve exclusions on advisory {advisory_id}: {type(e)}: {e}"
            ))

        # Validate `CVE Names` field of the advisory
        advisory_cves = advisory_info["content"]["content"]["cve"].split()
        extra_cves = cve_components_mapping.keys() - advisory_cves
        if extra_cves:
            msg = (f"On advisory {advisory_id}, bugs for the following CVEs are already attached "
                   f"but they are not listed in advisory's `CVE Names` field: {stringify(extra_cves)}")
            issue = VerifyIssue(
                code=VerifyIssueCode.EXTRA_CVE_NAMES_IN_ADVISORY,
                message=msg
            )
            issues.append(issue)
        missing_cves = advisory_cves - cve_components_mapping.keys()
        if missing_cves:
            msg = (f"On advisory {advisory_id}, bugs for the following CVEs are not attached but listed in "
                   f"advisory's `CVE Names` field: {stringify(missing_cves)}")
            issue = VerifyIssue(
                code=VerifyIssueCode.MISSING_CVE_NAMES_IN_ADVISORY,
                message=msg
            )
            issues.append(issue)

        self._complain(issues)

    def get_attached_bugs(self, advisory_ids: List[str]) -> Dict[int, Set[Bug]]:
        """ Get bugs attached to specified advisories
        :return: a dict with advisory id as key and set of bug objects as value
        """
        logger.info(f"Retrieving bugs for advisories: {advisory_ids}")

        # {12345: {'bugzilla' -> [..], 'jira' -> [..]} .. }
        advisory_bug_id_map: Dict[int, Dict] = {advisory_id: get_bug_ids(advisory_id) for advisory_id in advisory_ids}

        # we do this to gather bug ids from all advisories of a bug type
        # and fetch them in one go to avoid multiple requests
        attached_bug_map: Dict[int, Set[Bug]] = {advisory_id: set() for advisory_id in advisory_ids}
        for bug_tracker_type in ['jira', 'bugzilla']:
            bug_tracker = self.runtime.get_bug_tracker(bug_tracker_type)
            all_bug_ids = {bug_id for bug_dict in advisory_bug_id_map.values() for bug_id in bug_dict[bug_tracker_type]}
            bug_map: Dict[Bug] = bug_tracker.get_bugs_map(all_bug_ids)
            for advisory_id in advisory_ids:
                set_of_bugs: Set[Bug] = {bug_map[bid] for bid in advisory_bug_id_map[advisory_id][bug_tracker_type]
                                         if bid in bug_map}
                attached_bug_map[advisory_id] = attached_bug_map[advisory_id] | set_of_bugs
        return attached_bug_map

    def filter_bugs_by_release(self, bugs: List[Bug], complain: bool = False) -> List[Bug]:
        # filter out bugs with an invalid target release
        invalid_bugs = []
        valid_bugs = []
        for b in bugs:
            # b.target_release is a list
            if any(target in self.target_releases for target in b.target_release):
                valid_bugs.append(b)
            else:
                invalid_bugs.append((b.id, ', '.join(b.target_release)))
        if invalid_bugs and complain:
            issue = VerifyIssue(
                code=VerifyIssueCode.INVALID_TARGET_RELEASE,
                message=f"Bugs have invalid target release: {stringify(invalid_bugs)}",
            )
            self._complain(issue)
        return valid_bugs

    def _get_blocking_bugs_for(self, bugs):
        # get blocker bugs in the next version for all bugs we are examining
        candidate_blockers = []
        for b in bugs:
            if b.depends_on:
                candidate_blockers.extend(b.depends_on)
        jira_ids, bz_ids = bzutil.get_jira_bz_bug_ids(set(candidate_blockers))

        v = minor_version_tuple(self.target_releases[0])
        next_version = (v[0], v[1] + 1)

        def is_next_target(target_v):
            pattern = re.compile(r'^\d+\.\d+\.([0z])$')
            return pattern.match(target_v) and minor_version_tuple(target_v) == next_version

        def managed_by_art(b: Bug):
            components_not_managed_by_art = self.runtime.get_bug_tracker('jira').component_filter()
            return b.component not in components_not_managed_by_art

        # retrieve blockers and filter to those with correct product and target version
        blockers = []
        if jira_ids:
            blockers.extend(self.runtime.get_bug_tracker('jira')
                            .get_bugs(jira_ids))
        if bz_ids:
            blockers.extend(self.runtime.get_bug_tracker('bugzilla')
                            .get_bugs(bz_ids))
        logger.debug(f"Candidate Blocker bugs found: {[b.id for b in blockers]}")
        blocking_bugs = {}
        for bug in blockers:
            if not bug.is_ocp_bug() or not managed_by_art(bug):
                continue
            target_release = []
            # A bug without `Target Version` shouldn't be considered as a blocking bug
            try:
                target_release = bug.target_release
            except ValueError as err:  # bug.target_release will raise ValueError if Target Version is not set
                if "does not have `Target Version` field set" not in str(err):
                    raise
            next_target = any(is_next_target(target) for target in target_release)
            if next_target:
                blocking_bugs[bug.id] = bug
        logger.info(f"Blocking bugs for next target release ({next_version[0]}.{next_version[1]}): "
                    f"{list(blocking_bugs.keys())}")

        k = {bug: [blocking_bugs[b] for b in bug.depends_on if b in blocking_bugs] for bug in bugs}
        return k

    def _verify_blocking_bugs(self, blocking_bugs_for, is_attached=False):
        def has_valid_status(blocker, bug):
            """Check if blocker has invalid status in relation to bug"""
            pending_statuses = ['VERIFIED', 'RELEASE_PENDING', 'Release Pending', 'Verified']
            bugs_are_on_qa = bug.status == blocker.status == "ON_QA"
            closed_statuses = ['CLOSED', 'Closed']
            valid_closed_resolutions = ['CURRENTRELEASE', 'NEXTRELEASE', 'ERRATA', 'DUPLICATE', 'NOTABUG', 'WONTFIX',
                                        'Done', 'Fixed', 'Done-Errata', 'Obsolete', 'Current Release', 'Errata',
                                        'Next Release', "Won't Do", "Won't Fix", 'Duplicate', 'Duplicate Issue',
                                        'Not a Bug']

            if blocker.status in pending_statuses or bugs_are_on_qa:
                return True
            if blocker.status in closed_statuses and blocker.resolution in valid_closed_resolutions:
                return True
            return False

        parent_wrong_status_bugs = []
        parent_not_shipping_bugs = []

        # complain about blocking bugs that aren't verified or shipped
        major, minor = self.runtime.get_major_minor()
        for bug, blockers in blocking_bugs_for.items():
            for blocker in blockers:
                if not has_valid_status(blocker, bug):
                    message = f"{bug.id} status={bug.status} is a backport of bug {blocker.id} status={blocker.status}"
                    if blocker.resolution:
                        message += f":{blocker.resolution}"
                    parent_wrong_status_bugs.append((bug.id, message))
                if is_attached and blocker.status in ['ON_QA', 'Verified', 'VERIFIED'] and is_release_next_week(f"openshift-{major}.{minor + 1}"):
                    try:
                        blocker_advisories = blocker.all_advisory_ids()
                    except ErrataException:
                        try:
                            sync_jira_issue(blocker.id)
                            blocker_advisories = blocker.all_advisory_ids()
                        except Exception as e:
                            message = f"Failed to fetch advisories for bug {blocker.id}: {e}"
                            self._complain(VerifyIssue(
                                code=VerifyIssueCode.VALIDATION_ERROR,
                                message=message
                            ))
                            continue
                    if not blocker_advisories:
                        message = f"{bug.id} is a backport of bug {blocker.id} status={blocker.status}"
                        parent_not_shipping_bugs.append((bug.id, message))

        issues: List[VerifyIssue] = []

        if parent_wrong_status_bugs:
            parent_wrong_status = VerifyIssue(
                code=VerifyIssueCode.PARENT_BUG_WRONG_STATUS,
                message=f"The parent bugs of these bugs have invalid status: {stringify(parent_wrong_status_bugs)}",
            )
            issues.append(parent_wrong_status)

        if parent_not_shipping_bugs:
            parent_not_shipping = VerifyIssue(
                code=VerifyIssueCode.PARENT_BUG_NOT_SHIPPING,
                message=f"The parent bugs of these bugs are not shipping: {stringify(parent_not_shipping_bugs)}",
            )
            issues.append(parent_not_shipping)
        self._complain(issues)

    def _verify_bug_status(self, bugs):
        bugs_with_status = []
        # complain about bugs that are not yet ON_QA or more.
        good_resolution = ['CURRENTRELEASE', 'ERRATA',
                           'Done', 'Fixed', 'Done-Errata', 'Current Release', 'Errata']
        for bug in bugs:
            if bug.is_flaw_bug():
                continue
            if bug.status in ["VERIFIED", "RELEASE_PENDING", "Verified", "Release Pending", "ON_QA"]:
                continue
            if bug.status in ["CLOSED", "Closed"] and bug.resolution in good_resolution:
                continue
            status = bug.status
            if bug.status in ['CLOSED', 'Closed']:
                status = f"{bug.status}: {bug.resolution}"
            bugs_with_status.append((bug.id, status))
        if bugs_with_status:
            issue = VerifyIssue(
                code=VerifyIssueCode.INVALID_BUG_STATUS,
                message=f"Bugs have invalid status: {stringify(bugs_with_status)}",
            )
            self._complain(issue)

    def _find_invalid_trackers(self, bugs):
        logger.info("Checking for invalid tracker bugs")
        invalid_bugs = [b for b in bugs if b.is_invalid_tracker_bug()]
        if invalid_bugs:
            bugs = [t.id for t in invalid_bugs]
            issue = VerifyIssue(
                code=VerifyIssueCode.INVALID_TRACKER_BUGS,
                message=f"Bugs look like CVE trackers but do not have proper metadata: {stringify(bugs)}",
            )
            self._complain(issue)

    def _complain(self, issues: Union[List[VerifyIssue], VerifyIssue]):
        if isinstance(issues, VerifyIssue):
            issues = [issues]
        self.problems.extend(issues)
