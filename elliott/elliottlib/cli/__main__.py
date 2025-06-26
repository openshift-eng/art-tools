# -*- coding: utf-8 -*-
"""
Elliott is a CLI tool for managing Red Hat release advisories using the Erratatool
web service.
"""

# -----------------------------------------------------------------------------
# Module dependencies
# -----------------------------------------------------------------------------

# Prepare for Python 3
# stdlib
import datetime
import json
import logging
import sys
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool

# 3rd party
import click
from artcommonlib.format_util import green_prefix, red_prefix, red_print, yellow_prefix
from errata_tool import ErrataException

import elliottlib.brew
import elliottlib.bzutil
import elliottlib.constants
import elliottlib.errata
import elliottlib.exceptions

# ours
from elliottlib import Runtime
from elliottlib.cli.advisory_commons_cli import advisory_commons_cli
from elliottlib.cli.advisory_drop_cli import advisory_drop_cli
from elliottlib.cli.advisory_images_cli import advisory_images_cli
from elliottlib.cli.attach_bugs_cli import attach_bugs_cli
from elliottlib.cli.attach_cve_flaws_cli import attach_cve_flaws_cli
from elliottlib.cli.change_state_cli import change_state_cli
from elliottlib.cli.common import cli, find_default_advisory, use_default_advisory_option
from elliottlib.cli.conforma_cli import verify_conforma_cli
from elliottlib.cli.create_cli import create_cli
from elliottlib.cli.create_placeholder_cli import create_placeholder_cli
from elliottlib.cli.create_textonly_cli import create_textonly_cli
from elliottlib.cli.find_bugs_blocker_cli import find_bugs_blocker_cli
from elliottlib.cli.find_bugs_cli import find_bugs_cli
from elliottlib.cli.find_bugs_golang_cli import find_bugs_golang_cli
from elliottlib.cli.find_bugs_kernel_cli import find_bugs_kernel_cli
from elliottlib.cli.find_bugs_kernel_clones_cli import find_bugs_kernel_clones_cli
from elliottlib.cli.find_bugs_qe_cli import find_bugs_qe_cli
from elliottlib.cli.find_bugs_second_fix_cli import find_bugs_second_fix_cli
from elliottlib.cli.find_bugs_sweep_cli import find_bugs_sweep_cli
from elliottlib.cli.find_builds_cli import find_builds_cli
from elliottlib.cli.find_konflux_builds_cli import find_k_builds_cli
from elliottlib.cli.find_unconsumed_rpms import find_unconsumed_rpms_cli
from elliottlib.cli.get_golang_report_cli import get_golang_report_cli
from elliottlib.cli.get_golang_versions_cli import get_golang_versions_cli
from elliottlib.cli.konflux_release_cli import konflux_release_cli
from elliottlib.cli.konflux_release_watch_cli import watch_release_cli
from elliottlib.cli.move_builds_cli import move_builds_cli
from elliottlib.cli.pin_builds_cli import assembly_pin_builds_cli
from elliottlib.cli.remove_bugs_cli import remove_bugs_cli
from elliottlib.cli.remove_builds_cli import remove_builds_cli
from elliottlib.cli.repair_bugs_cli import repair_bugs_cli
from elliottlib.cli.rhcos_cli import rhcos_cli
from elliottlib.cli.shipment_cli import shipment_cli
from elliottlib.cli.snapshot_cli import snapshot_cli
from elliottlib.cli.tag_builds_cli import tag_builds_cli

# cli commands
from elliottlib.cli.tarball_sources_cli import tarball_sources_cli
from elliottlib.cli.validate_rhsa import validate_rhsa_cli
from elliottlib.cli.verify_attached_bugs_cli import verify_attached_bugs_cli
from elliottlib.cli.verify_attached_operators_cli import verify_attached_operators_cli
from elliottlib.cli.verify_cvp_cli import verify_cvp_cli
from elliottlib.cli.verify_payload import verify_payload
from elliottlib.exceptions import ElliottFatalError
from elliottlib.util import pbar_header, progress_func

# -----------------------------------------------------------------------------
# Constants and defaults
# -----------------------------------------------------------------------------
pass_runtime = click.make_pass_decorator(Runtime)
LOGGER = logging.getLogger(__name__)


#
# Get an Advisory
# advisory:get
#
@cli.command("get", short_help="Get information for an ADVISORY")
@click.argument('advisory', type=int, required=False)
@use_default_advisory_option
@click.option('--details', is_flag=True, default=False, help="Print the full object of the advisory")
@click.option('--id-only', is_flag=True, default=False, help="Print only the ID of the default advisory")
@click.option('--json', 'as_json', metavar="FILE_NAME", help="Dump the advisory as JSON to a file (or '-' for stdout)")
@pass_runtime
@click.pass_context
def get(ctx, runtime, default_advisory_type, details, id_only, as_json, advisory):
    """Get details about a specific advisory from the Errata Tool. By
    default a brief one-line informational string is printed. Use the
    --details option to fetch and print the full details of the advisory.

    Use of --id-only will override all other printing options. Requires
    using --use-default-advisory. Only the ID of the advisory is printed
    to standard out.

    Fields for the short format: Release date, State, Synopsys, URL

        Basic one-line output for advisory 123456:

    \b
        $ elliott get 123456
        2018-02-23T18:34:40 NEW_FILES OpenShift Container Platform 3.9 bug fix and enhancement update - https://errata.devel.redhat.com/advisory/123456

        Get the full JSON advisory object, use `jq` to print just the
        errata portion of the advisory:

    \b
        $ elliott get --json - 123456 | jq '.errata'
        {
          "rhba": {
            "actual_ship_date": null,
            "assigned_to_id": 3002255,
            "batch_id": null,
            ...
    """

    runtime.initialize(no_group=default_advisory_type is None)

    if bool(advisory) == bool(default_advisory_type):
        raise click.BadParameter("Specify exactly one of --use-default-advisory or advisory arg")
    if default_advisory_type is not None:
        advisory = find_default_advisory(runtime, default_advisory_type, quiet=True)

    if id_only:
        click.echo(advisory)
        return

    advisory = elliottlib.errata.Advisory(errata_id=advisory)

    if details:
        click.echo(advisory)
        return

    if not as_json:
        advisory_string = "{date} {state} {synopsis} {url}".format(
            date=advisory.publish_date_override,
            state=advisory.errata_state,
            synopsis=advisory.synopsis,
            url=advisory.url(),
        )
        click.echo(advisory_string)
        return

    json_data = advisory.get_erratum_data()

    json_data['bugs'] = advisory.errata_bugs
    json_data['jira_issues'] = advisory.jira_issues
    json_data['current_flags'] = advisory.current_flags
    json_data['errata_builds'] = advisory.errata_builds
    json_data['rpmdiffs'] = advisory.externalTests(test_type='rpmdiff')

    if as_json == "-":
        click.echo(json.dumps(json_data, indent=4, sort_keys=True))
        return

    with open(as_json, "w") as json_file:
        json.dump(json_data, json_file, indent=4, sort_keys=True)


#
# Poll for rpm-signed state change
# poll-signed
#
@cli.command("poll-signed", short_help="Poll for RPM build 'signed' status")
@click.option("--minutes", "-m", required=False, default=15, type=int, help="How long to poll before quitting")
@click.option("--advisory", "-a", type=int, metavar='ADVISORY', help="Advisory to watch")
@use_default_advisory_option
@click.option(
    "--noop",
    "--dry-run",
    required=False,
    default=False,
    is_flag=True,
    help="Don't actually poll, just print the signed status of each build",
)
@pass_runtime
def poll_signed(runtime, minutes, advisory, default_advisory_type, noop):
    """Poll for the signed-status of RPM builds attached to
    ADVISORY. Returns rc=0 when all builds have been signed. Returns non-0
    after MINUTES have passed and all builds have not been signed. This
    non-0 return code is the number of unsigned builds remaining. All
    builds must show 'signed' for this command to succeed.

        NOTE: The two advisory options are mutually exclusive.

    For testing in pipeline scripts this sub-command accepts a --noop
    option. When --noop is used the value of --minutes is irrelevant. This
    command will print out the signed state of all attached builds and
    then exit with rc=0 if all builds are signed and non-0 if builds are
    still unsigned. In the non-0 case the return code is the number of
    unsigned builds.

        Wait 15 minutes for the default 4.2 advisory to show all RPMS have
        been signed:

        $ elliott -g openshift-4.2 poll-signed --use-default-advisory rpm

        Wait 5 mintes for the provided 4.2 advisory to show all RPMs have
        been signed:

        $ elliott -g openshift-4.2 poll-signed -m 5 --advisory 123456

        Print the signed status of all attached builds, exit
        immediately. Return code is the number of unsigned builds.

    \b
        $ elliott -g openshift-4.2 poll-signed --noop --use-default-advisory rpm
    """
    if not (bool(advisory) ^ bool(default_advisory_type)):
        raise click.BadParameter("Use only one of --use-default-advisory or --advisory")

    runtime.initialize(no_group=default_advisory_type is None)

    if default_advisory_type is not None:
        advisory = find_default_advisory(runtime, default_advisory_type)

    if not noop:
        click.echo("Polling up to {} minutes for all RPMs to be signed".format(minutes))

    try:
        e = elliottlib.errata.Advisory(errata_id=advisory)
        all_builds = set([])
        all_signed = False
        # `errata_builds` is a dict with brew tags as keys, values are
        # lists of builds on the advisory with that tag
        for k, v in e.errata_builds.items():
            all_builds = all_builds.union(set(v))
        green_prefix("Fetching initial states: ")
        click.echo("{} builds to check".format(len(all_builds)))
        start_time = datetime.datetime.now()
        while datetime.datetime.now() - start_time < datetime.timedelta(minutes=minutes):
            pbar_header("Getting build signatures: ", "Should be pretty quick", all_builds)
            pool = ThreadPool(cpu_count())
            # Look up builds concurrently
            click.secho("[", nl=False)

            build_sigs = pool.map(
                lambda build: progress_func(lambda: elliottlib.errata.build_signed(build), '*'), all_builds
            )
            # Wait for results
            pool.close()
            pool.join()
            click.echo(']')

            if all(build_sigs):
                all_signed = True
                break
            elif noop:
                # Escape the time-loop
                break
            else:
                yellow_prefix("Not all builds signed: ")
                click.echo("re-checking")
                continue

        if not all_signed:
            red_prefix("Signing incomplete: ")
            if noop:
                click.echo("All builds not signed. ")
            else:
                click.echo("All builds not signed in given window ({} minutes). ".format(minutes))
                exit(1)
        else:
            green_prefix("All builds signed: ")
            click.echo("Enjoy!")
    except ErrataException as ex:
        raise ElliottFatalError(getattr(ex, 'message', repr(ex)))


# Register additional commands / groups
cli.add_command(advisory_images_cli)
cli.add_command(create_placeholder_cli)
cli.add_command(create_cli)
cli.add_command(change_state_cli)
cli.add_command(verify_conforma_cli)
cli.add_command(find_bugs_sweep_cli)
cli.add_command(find_bugs_cli)
cli.add_command(find_builds_cli)
cli.add_command(find_k_builds_cli)
cli.add_command(tag_builds_cli)
cli.add_command(tarball_sources_cli)
cli.add_command(verify_cvp_cli)
cli.add_command(advisory_drop_cli)
cli.add_command(verify_attached_operators_cli)
cli.add_command(verify_payload)
cli.add_command(verify_attached_bugs_cli)
cli.add_command(attach_cve_flaws_cli)
cli.add_command(create_textonly_cli)
cli.add_command(attach_bugs_cli)
cli.add_command(find_bugs_qe_cli)
cli.add_command(get_golang_versions_cli)
cli.add_command(validate_rhsa_cli)
cli.add_command(rhcos_cli)
cli.add_command(advisory_commons_cli)
cli.add_command(find_bugs_blocker_cli)
cli.add_command(remove_bugs_cli)
cli.add_command(repair_bugs_cli)
cli.add_command(find_unconsumed_rpms_cli)
cli.add_command(find_bugs_kernel_cli)
cli.add_command(find_bugs_kernel_clones_cli)
cli.add_command(move_builds_cli)
cli.add_command(find_bugs_golang_cli)
cli.add_command(remove_builds_cli)
cli.add_command(get_golang_report_cli)
cli.add_command(snapshot_cli)
cli.add_command(konflux_release_cli)
cli.add_command(assembly_pin_builds_cli)
cli.add_command(shipment_cli)
cli.add_command(watch_release_cli)
cli.add_command(find_bugs_second_fix_cli)

# -----------------------------------------------------------------------------
# CLI Entry point
# -----------------------------------------------------------------------------


def main():
    try:
        cli(obj={})
    except ElliottFatalError as ex:
        # Allow capturing actual tool errors and print them
        # nicely instead of a gross stack-trace.
        # All internal errors that should simply cause the app
        # to exit with an error code should use ElliottFatalError
        red_print(getattr(ex, 'message', repr(ex)))
        sys.exit(1)


if __name__ == '__main__':
    main()
