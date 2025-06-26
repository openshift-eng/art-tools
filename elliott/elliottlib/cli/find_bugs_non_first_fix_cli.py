import sys
import traceback
from typing import Dict, Iterable, List

import click
from artcommonlib import logutil

from elliottlib import Runtime
from elliottlib.bzutil import Bug, BugTracker, is_first_fix_any
from elliottlib.cli.common import cli
from elliottlib.cli.find_bugs_sweep_cli import FindBugsMode

LOGGER = logutil.get_logger(__name__)


class FindBugsNonFirstFix(FindBugsMode):
    def __init__(self):
        super().__init__(
            status={'MODIFIED', 'VERIFIED', 'ON_QA'},
            cve_only=True,
        )


@cli.command(
    "find-bugs:non-first-fix", short_help="Closes trackers if they are NOT first fix when branch is pre-release"
)
@click.option("--noop", "--dry-run", is_flag=True, default=False, help="Don't change anything")
@click.pass_obj
def find_bugs_non_first_fix_cli(runtime: Runtime, noop):
    """Checks if CVE trackers in a pre-release branch [before GA] are not 'first fix',
    and if so, they will be closed
    \b

        $ elliott -g openshift-4.Y find-bugs:non-first-fix
    """
    runtime.initialize()
    find_bugs_obj = FindBugsNonFirstFix()
    exit_code = 0
    for b in [runtime.get_bug_tracker('jira')]:
        try:
            find_bugs_non_first_fix(runtime, find_bugs_obj, noop, b)
        except Exception as e:
            LOGGER.error(traceback.format_exc())
            LOGGER.error(f'exception with {b.type} bug tracker: {e}')
            exit_code = 1
    sys.exit(exit_code)


def find_bugs_non_first_fix(runtime, find_bugs_obj, noop, bug_tracker):
    if runtime.group_config.software_lifecycle.phase == 'pre-release':
        LOGGER.info(f"Software lifecycle is {runtime.group_config.software_lifecycle.phase}. Performing action...")
        trackers = find_bugs_obj.search(bug_tracker_obj=bug_tracker, verbose=runtime.debug)
        flaw_bug_tracker = runtime.get_bug_tracker('bugzilla')
        brew_api = runtime.build_retrying_koji_client()
        tracker_flaws, not_first_fix_flaw_bugs = get_flaws(flaw_bug_tracker, trackers, brew_api, strict=False)
        LOGGER.info(f"not-first-fix: {tracker_flaws}, not-first-flaw bugs: {not_first_fix_flaw_bugs}")
        # Extract Bugzilla IDs from not_first_flaw_bugs into a set for efficient lookup
        not_first_flaw_bug_ids_set = {bug.id for bug in not_first_fix_flaw_bugs}
        print(f"Bugzilla IDs to match against: {not_first_flaw_bug_ids_set}\n")
        # Filter tracker_flaws based on these Bugzilla IDs
        matching_tracker_keys_list = []
        for tracker_id, bugzilla_ids_list in tracker_flaws.items():
            if any(bz_id in not_first_flaw_bug_ids_set for bz_id in bugzilla_ids_list):
                matching_tracker_keys_list.append(tracker_id)
        print("\nList of matching OCPBUGSkeys:")
        print(matching_tracker_keys_list)

        new_bug_tracker = runtime.get_bug_tracker('jira')
        for k in trackers:
            if k.id in matching_tracker_keys_list:
                LOGGER.info(k)
                comment = '''This CVE tracker is not a firts-fix'''
                target = "CLOSED"
                try:
                    new_bug_tracker.update_bug_status(
                        bug=k,
                        target_status=target,
                        comment=comment,
                        log_comment=True,
                        noop=noop
                    )
                except Exception as e:
                    LOGGER.error(traceback.format_exc())
                    LOGGER.error(f'exception with OCPBUGS: {k} bug tracker: {e}')
    else:
        LOGGER.info("Software lifecycle is not in 'pre-release'")


def get_flaws(flaw_bug_tracker: BugTracker, tracker_bugs: Iterable[Bug], brew_api, strict=True) -> (Dict, List):
    # validate and get target_release
    if not tracker_bugs:
        return {}, []  # Bug.get_target_release will panic on empty array
    current_target_release = Bug.get_target_release(tracker_bugs)
    tracker_flaws, flaw_tracker_map = BugTracker.get_corresponding_flaw_bugs(
        tracker_bugs, flaw_bug_tracker, brew_api, strict=strict
    )
    LOGGER.info(
        f'Found {len(flaw_tracker_map)} {flaw_bug_tracker.type} corresponding flaw bugs:'
        f' {sorted(flaw_tracker_map.keys())}'
    )

    # current_target_release can be digit.digit.([z|0])?
    # if current_target_release is GA then run first-fix bug filtering
    # for GA not every flaw bug is considered first-fix
    # for z-stream every flaw bug is considered first-fix
    # https://docs.engineering.redhat.com/display/PRODSEC/Security+errata+-+First+fix
    if current_target_release[-1] == 'z':
        LOGGER.info("Detected z-stream target release, every flaw bug is considered first-fix")
    else:
        LOGGER.info("Detected pre-release, applying first-fix filtering..")
        # we are interested here in CVE trackers that are NOT fist-fix
        not_first_fix_flaw_bugs = [
            flaw_bug_info['bug']
            for flaw_bug_info in flaw_tracker_map.values()
            if not is_first_fix_any(flaw_bug_info['bug'], flaw_bug_info['trackers'], current_target_release)
        ]

    LOGGER.info(f'{len(not_first_fix_flaw_bugs)} out of {len(flaw_tracker_map)} flaw bugs considered "not-first-fix"')
    return tracker_flaws, not_first_fix_flaw_bugs
