import click
import datetime
import elliottlib
from artcommonlib import logutil
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.format_util import green_prefix
from elliottlib.cli.common import cli
from elliottlib.cli.create_placeholder_cli import create_placeholder_cli
from elliottlib.exectools import cmd_assert
from elliottlib.exceptions import ElliottFatalError, ErrataToolUnauthorizedException, ErrataToolError
from elliottlib.util import YMD, validate_release_date, \
    validate_email_address, exit_unauthorized
from elliottlib import errata


LOGGER = logutil.get_logger(__name__)


@cli.command("create", short_help="Create a new advisory")
@click.option("--type", '-t', 'errata_type', required=True,
              type=click.Choice(['RHBA', 'RHEA']),
              help="Type of Advisory to create.")
@click.option("--art-advisory-key", required=True,
              help="Boilerplate for the advisory. This will be looked up from erratatool.yml")
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
def create_cli(ctx, runtime, errata_type, art_advisory_key, date, assigned_to, manager, package_owner, with_placeholder, with_liveid, yes):
    """Create a new advisory. The boilerplate to use for the advisory must be specified with
'--art-advisory-key'. This will be looked up from erratatool.yml.

    You MUST specify a group (ex: "openshift-3.9") manually using the
    --group option. See examples below.

You must set a Release Date by providing a YYYY-Mon-DD formatted string to the
--date option.

The default behavior for this command is to show what the generated
advisory would look like. The raw JSON used to create the advisory
will be printed to the screen instead of posted to the Errata Tool
API.

The --assigned-to, --manager and --package-owner options are required.
They are the email addresses of the parties responsible for managing and
approving the advisory.

Provide the '--yes' or '-y' option to confirm creation of the
advisory.

    PREVIEW RPM Advisory:

    $ elliott -g openshift-4.14 create --art-advisory-key rpm --date 2018-Mar-05

    CREATE Image Advisory:

\b
    $ elliott -g openshift-4.14 create --art-advisory-key image --date 2018-Mar-05 --yes
"""
    runtime.initialize()

    et_data = runtime.get_errata_config()

    # User entered a valid value for --date, set the release date
    release_date = datetime.datetime.strptime(date, YMD)

    if "boilerplates" not in et_data:
        raise ValueError("`boilerplates` is required in erratatool.yml")

    if art_advisory_key not in et_data["boilerplates"]:
        raise ValueError(f"Boilerplate {art_advisory_key} not found in erratatool.yml")

    boilerplate = et_data["boilerplates"][art_advisory_key]

    try:
        erratum = errata.new_erratum(
            et_data,
            errata_type=errata_type,
            boilerplate_name=art_advisory_key,
            release_date=release_date.strftime(YMD),
            assigned_to=assigned_to,
            manager=manager,
            package_owner=package_owner
        )
    except ErrataToolUnauthorizedException:
        exit_unauthorized()
    except ErrataToolError as ex:
        raise ElliottFatalError(getattr(ex, 'message', repr(ex)))

    if yes:
        erratum.commit()
        green_prefix("Created new advisory: ")
        click.echo(str(erratum))

        if with_placeholder:
            click.echo("Creating and attaching placeholder bug...")
            ctx.invoke(create_placeholder_cli, advisory=erratum.errata_id)

        # This is to enable leaving legacy style comment when we create an advisory
        # It is relied on by rel-eng automation to trigger binary releases to Customer Portal
        # It is really only required for our main payload advisory "image"
        # https://gitlab.cee.redhat.com/releng/g-chat-notifier/notifier/-/blob/3d71698a45de9f847cb272d18a5a27dccf9521a0/notifier/etera_controller.py#L128
        # https://issues.redhat.com/browse/ART-8758
        # Do not leave a comment for a custom assembly type
        if boilerplate.get("advisory_type_comment", False) in ["yes", "True", True] and runtime.assembly_type is not AssemblyTypes.CUSTOM:
            major, minor = runtime.get_major_minor()
            comment = {"release": f"{major}.{minor}", "kind": art_advisory_key, "impetus": "standard"}
            errata.add_comment(erratum.errata_id, comment)

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
