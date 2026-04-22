from datetime import datetime

import click
from artcommonlib import logutil
from errata_tool import ErrataException

from elliottlib.cli.common import cli, find_default_advisory, use_default_advisory_option
from elliottlib.errata import get_raw_erratum, update_erratum

LOGGER = logutil.get_logger(__name__)


@cli.command("advisory-date", short_help="Show or update the publish date for a group of advisories")
@use_default_advisory_option
@click.argument("advisories", nargs=-1, type=click.IntRange(1), required=False)
@click.option("--new", help="New publish date to update advisories with.")
@click.option(
    '--yes',
    '-y',
    is_flag=True,
    default=False,
    type=bool,
    help="Update the advisories (by default only a preview is displayed)",
)
@click.pass_obj
def advisory_date_cli(runtime, default_advisory_type, advisories, new, yes):
    """Display or Change the publish date for a group of advisories.

    NOTE: The two advisory input options (--assembly and --advisories)
    are mutually exclusive and can not be used together.

    Show the publish date for all advisories for an assembly/group/advisories

    $ elliott -g openshift-4.8 --assembly 4.8.8 advisory-date

    $ elliott -g openshift-3.11 advisory-date

    $ elliott advisory-date 80825 80824

    (Preview) update publish date for all advisories for an assembly

    $ elliott -g openshift-4.8 --assembly 4.8.8 advisory-date --new "2021-Aug-31"

    (Commit) update publish date for all advisories for an assembly

    $ elliott -g openshift-4.8 --assembly 4.8.8 advisory-date --new "2021-Aug-31" --yes
    """
    noop = not yes
    count_flags = sum(map(bool, [runtime.group, advisories]))
    if count_flags > 1:
        raise click.BadParameter("Use only one of --group or advisories param")

    if not advisories:
        runtime.initialize()
        advisories = runtime.group_config.advisories.values()
        if default_advisory_type:
            advisories = [find_default_advisory(runtime, default_advisory_type)]

    errors = []
    for advisory_id in advisories:
        try:
            click.echo(f"Fetching {advisory_id} ... ")
            erratum = get_raw_erratum(advisory_id)['errata']
            advisory_type_key = list(erratum.keys())[0]
            advisory = erratum[advisory_type_key]

            # Format the publish date as 'YYYY-Mon-DD', e.g., '2025-Oct-21'
            current = advisory['publish_date']
            dt = datetime.strptime(current, "%Y-%m-%dT%H:%M:%S%z")
            current = dt.strftime("%Y-%b-%d")
            click.echo(f"publish_date = {current}")

            if new:
                if new == current:
                    click.echo("No change. New value is same as current value")
                else:
                    click.echo(f"Preparing update to publish_date: {current} ➔ {new}")
                    if not noop:
                        advisory = update_erratum(advisory_id, {'advisory': {'publish_date_override': new}})
                        click.echo("Committed change")
                    else:
                        click.echo("Would have committed change")
        except ErrataException as ex:
            click.echo(f'Error fetching/changing {advisory_id}: {ex}')
            errors.append(ex)
    if errors:
        raise Exception(errors)
