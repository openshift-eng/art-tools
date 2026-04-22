import traceback

import click
from artcommonlib import logutil

from elliottlib import Runtime
from elliottlib.bzutil import get_second_fix_trackers
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
    runtime.initialize(mode='images')
    find_bugs_obj = FindBugsSecondFix()
    find_bugs_second_fix(runtime, find_bugs_obj, close, noop, runtime.get_bug_tracker('jira'))


def find_bugs_second_fix(runtime, find_bugs_obj, close, noop, bug_tracker):
    allowed_phases = ['pre-release', 'signing']
    phase_value = runtime.group_config.software_lifecycle.phase
    if phase_value in allowed_phases:
        LOGGER.info(
            f"Software lifecycle is {runtime.group_config.software_lifecycle.phase}. Performing second-fix action ..."
        )
        major_version, minor_version = runtime.get_major_minor()

        LOGGER.info("Fetching tracker bugs .. ")
        trackers = find_bugs_obj.search(bug_tracker_obj=bug_tracker, verbose=runtime.debug)
        LOGGER.info(f"{len(trackers)} trackers found: {[b.id for b in trackers]}")

        trackers = [b for b in trackers if b.is_tracker_bug()]
        LOGGER.info(f"{len(trackers)} valid trackers found: {[b.id for b in trackers]}")

        LOGGER.info("Computing second-fix tracker ids .. ")
        second_fix_trackers = get_second_fix_trackers(runtime, trackers)
        LOGGER.info(
            f"{len(second_fix_trackers)} CVE trackers that are second-fix: {sorted([b.id for b in second_fix_trackers])}"
        )

        if close:
            bug_tracker = runtime.get_bug_tracker('jira')
            for k in second_fix_trackers:
                LOGGER.info(f"Tracker {k.id} is a second fix for it's associated CVE and therefore it will be closed")
                comment = f'''
Closing this CVE tracker, as the CVE has been declared fixed for this component for {major_version}.{int(minor_version) - 1}.
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
                f"Found {len(second_fix_trackers)} CVE trackers that are not first-fix for OCP {major_version}.{minor_version} pre-release"
            )
            for k in second_fix_trackers:
                LOGGER.info(k.id)
            LOGGER.info("Use --close to close these CVE trackers.")
    else:
        LOGGER.info(f"Software lifecycle is not in {phase_value} . The first-fix logic doesn't apply here.")
