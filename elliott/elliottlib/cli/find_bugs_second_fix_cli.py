import traceback

import click
from artcommonlib import logutil

from elliottlib import Runtime
from elliottlib.bzutil import get_flaws
from elliottlib.cli.common import cli
from elliottlib.cli.find_bugs_sweep_cli import FindBugsMode

LOGGER = logutil.get_logger(__name__)


class FindBugsSecondFix(FindBugsMode):
    def __init__(self):
        super().__init__(
            status={'MODIFIED', 'VERIFIED', 'ON_QA'},
            cve_only=True,
        )


@cli.command("find-bugs:second-fix", short_help="Closes trackers if they are NOT first fix when branch is pre-release")
@click.option("--close", is_flag=True, default=False, help="Close the CVE trackers that are not first fix")
@click.option("--noop", "--dry-run", is_flag=True, default=False, help="Don't change anything")
@click.pass_obj
def find_bugs_second_fix_cli(runtime: Runtime, close, noop):
    """Checks if CVE trackers in a pre-release branch [before GA] are not 'first fix',
    and if so, they will be closed
    \b

        $ elliott -g openshift-4.Y find-bugs:second-fix
    """
    runtime.initialize()
    find_bugs_obj = FindBugsSecondFix()
    find_bugs_second_fix(runtime, find_bugs_obj, close, noop, runtime.get_bug_tracker('jira'))


def find_bugs_second_fix(runtime, find_bugs_obj, close, noop, bug_tracker):
    allowed_phases = ['pre-release','signing']
    phase_value = runtime.group_config.software_lifecycle.phase
    if phase_value in allowed_phases:
        LOGGER.info(
            f"Software lifecycle is {runtime.group_config.software_lifecycle.phase}. Performing second-fix action ..."
        )
        major_version, minor_version = runtime.get_major_minor()
        trackers = find_bugs_obj.search(bug_tracker_obj=bug_tracker, verbose=runtime.debug)
        flaw_bug_tracker = runtime.get_bug_tracker('bugzilla')
        tracker_flaws, not_first_fix_flaw_bugs = get_flaws(
            flaw_bug_tracker, trackers, runtime.assembly_type, runtime.assembly
        )
        # Extract Bugzilla IDs from not_first_flaw_bugs into a set for efficient lookup
        not_first_flaw_bug_ids_set = {bug.id for bug in not_first_fix_flaw_bugs}
        # Filter tracker_flaws based on these Bugzilla IDs
        matching_tracker_keys_list = []

        for tracker_id, bugzilla_ids_list in tracker_flaws.items():
            if any(bz_id in not_first_flaw_bug_ids_set for bz_id in bugzilla_ids_list):
                matching_tracker_keys_list.append(tracker_id)

        if close:
            bug_tracker = runtime.get_bug_tracker('jira')
            for k in trackers:
                if k.id in matching_tracker_keys_list:
                    LOGGER.info(
                        f" These CVE trackers {k} were computed as second fixes and therefore they will be closed"
                    )
                    comment = f'''
    This CVE tracker was closed since it was not declared as first-fix for the recent OCP {major_version}.{minor_version} version
    in pre-release
    '''
                    target = "CLOSED"
                    try:
                        bug_tracker.update_bug_status(
                            bug=k, target_status=target, comment=comment, log_comment=True, noop=noop
                        )
                    except Exception as e:
                        LOGGER.error(traceback.format_exc())
                        LOGGER.error(f'exception with OCPBUGS: {k} bug tracker: {e}')
        else:
            LOGGER.info(
                f"Found {len(matching_tracker_keys_list)} CVE trackers that are not first-fix for OCP {major_version}.{minor_version} pre-release"
            )
            for k in matching_tracker_keys_list:
                LOGGER.info(k)
            LOGGER.info("Use --close to close these CVE trackers.")
    else:
        LOGGER.info(f"Software lifecycle is not in {phase_value} . The first-fix logic doesn't apply here.")
