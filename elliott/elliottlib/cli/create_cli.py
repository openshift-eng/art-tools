from typing import Optional
import click
from datetime import datetime
from artcommonlib import logutil
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.exectools import cmd_assert
from artcommonlib.format_util import green_prefix
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.create_placeholder_cli import create_placeholder_cli
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
@click.option('--with-liveid/--no-liveid', is_flag=True,
              default=True, type=bool,
              help="Request a Live ID for the advisory. Only valid if also using --yes.")
@click.option('--batch-id', metavar="BATCH_ID", type=int,
              help="Batch ID to use for the advisory.")
@click.option('--yes', '-y', is_flag=True,
              default=False, type=bool,
              help="Create the advisory (by default only a preview is displayed)")
@click.pass_obj
@click.pass_context
@click_coroutine
async def create_cli(ctx, runtime, errata_type, art_advisory_key, date, assigned_to, manager, package_owner,
                     with_placeholder: bool, with_liveid: bool, batch_id: Optional[int], yes: bool):
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
    release_date = datetime.strptime(date, YMD)

    if "boilerplates" not in et_data:
        raise ValueError("`boilerplates` is required in erratatool.yml")

    if art_advisory_key not in et_data["boilerplates"]:
        raise ValueError(f"Boilerplate {art_advisory_key} not found in erratatool.yml")

    boilerplate = et_data["boilerplates"][art_advisory_key]

    errata_api = AsyncErrataAPI()
    try:
        if yes:
            created_advisory = await errata_api.create_advisory(
                product=et_data['product'],
                release=boilerplate.get('release', et_data['release']),
                errata_type=errata_type,
                advisory_synopsis=boilerplate['synopsis'],
                advisory_topic=boilerplate['topic'],
                advisory_description=boilerplate['description'],
                advisory_solution=boilerplate['solution'],
                advisory_quality_responsibility_name=et_data['quality_responsibility_name'],
                advisory_package_owner_email=package_owner,
                advisory_manager_email=manager,
                advisory_assigned_to_email=assigned_to,
                advisory_publish_date_override=release_date.strftime(YMD),
                batch_id=batch_id
            )
            advisory_info = next(iter(created_advisory["errata"].values()))
            advisory_name = advisory_info["fulladvisory"].rsplit("-", 1)[0]
            advisory_id = advisory_info["id"]
            green_prefix("Created new advisory: ")
            print_advisory(advisory_name, boilerplate['synopsis'], package_owner, assigned_to, et_data['quality_responsibility_name'], release_date, batch_id)

            if with_placeholder:
                click.echo("Creating and attaching placeholder bug...")
                ctx.invoke(create_placeholder_cli, advisory=advisory_id)

            # This is to enable leaving legacy style comment when we create an advisory
            # It is relied on by rel-eng automation to trigger binary releases to Customer Portal
            # It is really only required for our main payload advisory "image"
            # https://gitlab.cee.redhat.com/releng/g-chat-notifier/notifier/-/blob/3d71698a45de9f847cb272d18a5a27dccf9521a0/notifier/etera_controller.py#L128
            # https://issues.redhat.com/browse/ART-8758
            # Do not leave a comment for a custom assembly type
            if boilerplate.get("advisory_type_comment", False) in ["yes", "True", True] and runtime.assembly_type is not AssemblyTypes.CUSTOM:
                major, minor = runtime.get_major_minor()
                comment = {"release": f"{major}.{minor}", "kind": art_advisory_key, "impetus": "standard"}
                errata.add_comment(advisory_id, comment)

            if with_liveid:
                click.echo("Requesting Live ID...")
                resp = await errata_api.request_liveid(advisory_id)
                live_advisory_name = next(iter(resp.values()))["fulladvisory"].rsplit("-", 1)[0]
                print(f"Live ID requested for advisory: {live_advisory_name}")
        else:
            green_prefix("Would have created advisory: ")
            click.echo("")
            print_advisory("(unassigned)", boilerplate['synopsis'], package_owner, assigned_to, et_data['quality_responsibility_name'], release_date, batch_id)
    finally:
        await errata_api.close()


def print_advisory(errata_name: str, synopsis: str, package_owner: str, assigned_to: str, qe_group: str, release_date: datetime, batch_id: Optional[int] = None):
    click.echo(f"""{errata_name}: {synopsis}
  package owner: {package_owner}  qe: {assigned_to} qe_group: {qe_group}
  url:   https://errata.devel.redhat.com/advisory/0
  state: NEW_FILES
  created:     None
  ship target: {release_date.strftime(YMD)}
  batch_id:    {batch_id}
  ship date:   None
  age:         0 days
  bugs:        []
  jira issues: []
  builds:
""")
