import click
from artcommonlib import logutil
from errata_tool import Erratum

from elliottlib.cli.common import cli, find_default_advisory, use_default_advisory_option
from elliottlib.exceptions import ElliottFatalError

LOGGER = logutil.get_logger(__name__)


@cli.command("create-placeholder", short_help="Create a placeholder bug")
@click.option("--attach", "-a", "advisory_id", type=int, metavar="ADVISORY", help="Attach the bug to ADVISORY")
@use_default_advisory_option
@click.option(
    "--noop",
    "--dry-run",
    required=False,
    default=False,
    is_flag=True,
    help="Print what would change, but don't change anything",
)
@click.pass_obj
def create_placeholder_cli(runtime, kind, advisory_id, default_advisory_type, noop):
    """Create a placeholder bug for attaching to an advisory.

    ADVISORY - Optional. The advisory to attach the bug to.

    $ elliott --group openshift-4.1 create-placeholder --attach 12345
    """
    if advisory_id and default_advisory_type:
        raise click.BadParameter("Use only one of --use-default-advisory or --advisory")

    runtime.initialize()
    if default_advisory_type is not None:
        advisory_id = find_default_advisory(runtime, default_advisory_type)

    create_placeholder(advisory_id, runtime.get_bug_tracker("jira"), noop)


def create_placeholder(advisory_id, bug_tracker, noop):
    bug = bug_tracker.create_placeholder(noop)
    if noop:
        return

    click.echo(f"Created Bug: {bug.id} {bug.weburl}")

    if not advisory_id:
        return

    advisory = Erratum(errata_id=advisory_id)

    if advisory is False:
        raise ElliottFatalError(f"Error: Could not locate advisory {advisory_id}")

    click.echo("Attaching bug to advisory...")
    bug_tracker.attach_bugs([bug.id], advisory_obj=advisory)
