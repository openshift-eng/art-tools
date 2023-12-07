import click
import datetime
import elliottlib
from elliottlib.cli.common import cli
from elliottlib.cli.create_placeholder_cli import create_placeholder_cli
from elliottlib.exectools import cmd_assert
from elliottlib.exceptions import ElliottFatalError
from elliottlib.util import YMD, validate_release_date, \
    validate_email_address, exit_unauthorized, green_prefix

LOGGER = elliottlib.logutil.getLogger(__name__)


@cli.command("create", short_help="Create a new advisory")
@click.option("--type", '-t', 'errata_type',
              type=click.Choice(['RHBA', 'RHEA']),
              default='RHBA',
              help="Type of Advisory to create.")
@click.option("--kind", '-k', required=True,
              type=click.Choice(['rpm', 'image']),
              help="Kind of artifacts that will be attached to Advisory. Affects boilerplate text.")
@click.option("--impetus",
              type=click.Choice(elliottlib.constants.errata_valid_impetus),
              default='standard',
              help="Impetus for the advisory creation. 'standard' by default")
@click.option("--art-advisory-key",
              type=click.Choice(['rpm', 'microshift', 'metadata', 'image', 'extras']),
              help="Class of the advisory. Should be one of [rpm, microshift, metadata, image, extras].")
@click.option("--date", required=True,
              callback=validate_release_date,
              help="Release date for the advisory. Format: YYYY-Mon-DD.")
@click.option('--assigned-to', metavar="EMAIL_ADDR", required=True,
              envvar="ELLIOTT_ASSIGNED_TO_EMAIL",
              callback=validate_email_address,
              help="The email address group to review and approve the advisory.")
@click.option('--manager', metavar="EMAIL_ADDR", required=True,
              envvar="ELLIOTT_MANAGER_EMAIL",
              callback=validate_email_address,
              help="The email address of the manager monitoring the advisory status.")
@click.option('--package-owner', metavar="EMAIL_ADDR", required=True,
              envvar="ELLIOTT_PACKAGE_OWNER_EMAIL",
              callback=validate_email_address,
              help="The email address of the person responsible managing the advisory.")
@click.option('--with-placeholder', is_flag=True,
              default=False, type=bool,
              help="Create a placeholder bug and attach it to the advisory. Only valid if also using --yes.")
@click.option('--with-liveid', is_flag=True,
              default=True, type=bool,
              help="Request a Live ID for the advisory. Only valid if also using --yes.")
@click.option('--yes', '-y', is_flag=True,
              default=False, type=bool,
              help="Create the advisory (by default only a preview is displayed)")
@click.pass_obj
@click.pass_context
def create_cli(ctx, runtime, errata_type, kind, impetus, art_advisory_key, date, assigned_to, manager, package_owner, with_placeholder, with_liveid, yes):
    """Create a new advisory. The kind of advisory must be specified with
'--kind'. Valid choices are 'rpm' and 'image'.

    You MUST specify a group (ex: "openshift-3.9") manually using the
    --group option. See examples below.

You must set a Release Date by providing a YYYY-Mon-DD formatted string to the
--date option.

The default behavior for this command is to show what the generated
advisory would look like. The raw JSON used to create the advisory
will be printed to the screen instead of posted to the Errata Tool
API.

The impetus option only affects the metadata added to the new
advisory and its synopsis.

The --assigned-to, --manager and --package-owner options are required.
They are the email addresses of the parties responsible for managing and
approving the advisory.

Provide the '--yes' or '-y' option to confirm creation of the
advisory.

    PREVIEW an RPM Advisory 21 days from now (the default release date) for OSE 3.9:

    $ elliott --group openshift-3.9 create

    CREATE Image Advisory for the 3.5 series on the first Monday in March:

\b
    $ elliott --group openshift-3.5 create --yes -k image --date 2018-Mar-05
"""
    runtime.initialize()

    et_data = runtime.get_errata_config()

    # User entered a valid value for --date, set the release date
    release_date = datetime.datetime.strptime(date, YMD)

    try:
        erratum = elliottlib.errata.new_erratum(
            et_data,
            errata_type=errata_type,
            boilerplate_name=(art_advisory_key if art_advisory_key else kind),
            release_date=release_date.strftime(YMD),
            assigned_to=assigned_to,
            manager=manager,
            package_owner=package_owner
        )
    except elliottlib.exceptions.ErrataToolUnauthorizedException:
        exit_unauthorized()
    except elliottlib.exceptions.ErrataToolError as ex:
        raise ElliottFatalError(getattr(ex, 'message', repr(ex)))

    if yes:
        erratum.commit()
        green_prefix("Created new advisory: ")
        click.echo(str(erratum))

        if with_placeholder:
            click.echo("Creating and attaching placeholder bug...")
            ctx.invoke(create_placeholder_cli, advisory=erratum.errata_id)

        if with_liveid:
            click.echo("Requesting Live ID...")
            base_url = "https://errata.devel.redhat.com/errata/set_live_advisory_name"
            cmd_assert(
                f"curl -X POST --fail --negotiate -u : {base_url}/{erratum.errata_id}",
                retries=3,
                pollrate=10,
            )
    else:
        green_prefix("Would have created advisory: ")
        click.echo("")
        click.echo(erratum)
