import sys
import traceback

import click
from artcommonlib import logutil

from elliottlib import Runtime
from elliottlib.cli.common import cli
from elliottlib.cli.find_bugs_sweep_cli import FindBugsMode, filter_art_managed_jira_trackers

LOGGER = logutil.get_logger(__name__)


class FindBugsQE(FindBugsMode):
    def __init__(self, art_managed_trackers_only: bool = True):
        super().__init__(
            status={'MODIFIED'},
            cve_only=False,
            art_managed_trackers_only=art_managed_trackers_only,
        )


@cli.command("find-bugs:qe", short_help="Change MODIFIED bugs to ON_QA and close reconciliation bugs")
@click.option("--noop", "--dry-run", is_flag=True, default=False, help="Don't change anything")
@click.option(
    "--art-managed-trackers-only/--no-art-managed-trackers-only",
    default=True,
    help="Filter image component CVE trackers to ART-managed images only",
)
@click.pass_obj
def find_bugs_qe_cli(runtime: Runtime, noop, art_managed_trackers_only):
    """Find MODIFIED bugs for the target-releases, and set them to ON_QA.
        with a release comment on each bug.

        Also finds and closes bugs with art:reconciliation label.

    \b
        $ elliott -g openshift-4.6 find-bugs:qe

    """
    runtime.initialize(mode="images")
    find_bugs_obj = FindBugsQE(art_managed_trackers_only=art_managed_trackers_only)
    exit_code = 0
    bug_tracker = runtime.get_bug_tracker('jira')

    # First, handle MODIFIED bugs -> ON_QA
    try:
        find_bugs_qe(runtime, find_bugs_obj, noop, bug_tracker)
    except Exception as e:
        LOGGER.error(traceback.format_exc())
        LOGGER.error(f'exception with JIRA bug tracker (find_bugs_qe): {e}')
        exit_code = 1

    # Second, close reconciliation bugs with art:reconciliation label
    try:
        close_reconciliation_bugs(runtime, noop, bug_tracker)
    except Exception as e:
        LOGGER.error(traceback.format_exc())
        LOGGER.error(f'exception with JIRA bug tracker (close_reconciliation_bugs): {e}')
        exit_code = 1

    sys.exit(exit_code)


def close_reconciliation_bugs(runtime, noop: bool, bug_tracker):
    """Find and close bugs with art:reconciliation label.

    These bugs track PRs for upstream/downstream reconciliation. They are closed
    when the discrepancy is resolved (e.g. after a golang builder update).
    """
    statuses = ['MODIFIED', 'ON_QA', 'Verified']
    tr = bug_tracker.target_release()
    LOGGER.info(
        f"Searching {bug_tracker.type} for bugs with art:reconciliation label "
        f"in statuses {statuses} and target releases: {tr}"
    )

    query = bug_tracker._query(
        status=statuses,
        include_labels=['art:reconciliation'],
    )
    bugs = bug_tracker._search(query, verbose=runtime.debug)
    LOGGER.info(f"Found {len(bugs)} bugs to close: {', '.join(sorted(str(b.id) for b in bugs))}")

    close_comment = (
        "Base image updates are what production build config expects. "
        "Closing this issue as Done, no need to inform customers about this change."
    )

    for bug in bugs:
        LOGGER.info(f"Closing bug {bug.id} (status: {bug.status}) with resolution Done")
        if noop:
            LOGGER.info(f"Would have closed {bug.id} from {bug.status} to Closed with resolution Done")
            LOGGER.info(f"Would have added comment to {bug.id}: {close_comment}")
        else:
            try:
                bug_tracker._client.transition_issue(bug.id, 'Closed', fields={'resolution': {'name': 'Done'}})
                LOGGER.info(f"Closed {bug.id} with resolution Done")

                bug_tracker.add_comment(bug.id, close_comment, private=False, noop=False)
                LOGGER.info(f"Added closure comment to {bug.id}")
            except Exception as e:
                LOGGER.error(f"Failed to close bug {bug.id}: {e}")


def find_bugs_qe(runtime, find_bugs_obj, noop, bug_tracker):
    major_version, minor_version = runtime.get_major_minor()
    statuses = sorted(find_bugs_obj.status)
    tr = bug_tracker.target_release()
    LOGGER.info(f"Searching {bug_tracker.type} for bugs with status {statuses} and target releases: {tr}")

    bugs = find_bugs_obj.search(bug_tracker_obj=bug_tracker, verbose=runtime.debug)
    if find_bugs_obj.art_managed_trackers_only:
        bugs = filter_art_managed_jira_trackers(runtime, bugs, bug_tracker_type=bug_tracker.type)
    LOGGER.info(f"Found {len(bugs)} bugs: {', '.join(sorted(str(b.id) for b in bugs))}")

    release_comment = (
        "An ART build cycle completed after this fix was made, which usually means it can be"
        f" expected in the next created {major_version}.{minor_version} nightly and release."
    )
    for bug in bugs:
        updated = bug_tracker.update_bug_status(bug, 'ON_QA', comment=release_comment, noop=noop)
        if updated:
            if bug.is_tracker_bug():
                # leave a special comment for QE
                comment = """Note for QE:
    This is a CVE bug. Please plan on verifying this bug ASAP.
    A CVE bug shouldn't be dropped from an advisory if QE doesn't have enough time to verify.
    Contact ProdSec if you have questions.
    """
                bug_tracker.add_comment(bug.id, comment, private=True, noop=noop)

                # get summary of tracker bug and update it if needed
                if not bug.has_valid_target_version_in_summary(major_version, minor_version):
                    new_s = bug.make_summary_with_target_version(major_version, minor_version)
                    LOGGER.info(f"Updating summary for bug {bug.id} from '{bug.summary}' to '{new_s}'")
                    try:
                        bug.update_summary(new_s, noop=noop)
                    except Exception as e:
                        LOGGER.warning("Failed to fix summary: %s", str(e))

            elif bug_tracker.type == 'jira':
                # If a security level is specified, the bug won't be visible on advisories
                # Make this explicit in the bug comment. Not applicable for security trackers/flaw bugs
                security_level = bug.security_level
                if security_level:
                    comment = (
                        "This is not a public issue, the customer visible advisory will not link the fix."
                        "Setting the Security Level to public before the advisory ships will have it included"
                    )
                    bug_tracker.add_comment(bug.id, comment, private=True, noop=noop)
