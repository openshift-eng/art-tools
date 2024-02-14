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
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool
import datetime
import json
import sys
from typing import Dict, List

from artcommonlib.format_util import red_print, red_prefix, green_prefix, green_print, yellow_prefix, yellow_print
# ours
from elliottlib import util
from elliottlib import Runtime
from artcommonlib import rhcos
import elliottlib.constants
import elliottlib.bzutil
import elliottlib.brew
import elliottlib.errata
import elliottlib.exceptions

from elliottlib.exceptions import ElliottFatalError
from elliottlib.util import progress_func, pbar_header
from elliottlib.cli.common import cli, use_default_advisory_option, find_default_advisory, click_coroutine

# cli commands
from elliottlib.cli.tarball_sources_cli import tarball_sources_cli
from elliottlib.cli.find_builds_cli import find_builds_cli
from elliottlib.cli.find_bugs_sweep_cli import find_bugs_sweep_cli
from elliottlib.cli.create_cli import create_cli
from elliottlib.cli.create_placeholder_cli import create_placeholder_cli
from elliottlib.cli.change_state_cli import change_state_cli
from elliottlib.cli.advisory_images_cli import advisory_images_cli
from elliottlib.cli.tag_builds_cli import tag_builds_cli
from elliottlib.cli.verify_cvp_cli import verify_cvp_cli
from elliottlib.cli.advisory_drop_cli import advisory_drop_cli
from elliottlib.cli.verify_attached_operators_cli import verify_attached_operators_cli
from elliottlib.cli.verify_attached_bugs_cli import verify_attached_bugs_cli
from elliottlib.cli.attach_cve_flaws_cli import attach_cve_flaws_cli
from elliottlib.cli.attach_bugs_cli import attach_bugs_cli
from elliottlib.cli.find_bugs_qe_cli import find_bugs_qe_cli
from elliottlib.cli.get_golang_versions_cli import get_golang_versions_cli
from elliottlib.cli.validate_rhsa import validate_rhsa_cli
from elliottlib.cli.rhcos_cli import rhcos_cli
from elliottlib.cli.create_textonly_cli import create_textonly_cli
from elliottlib.cli.advisory_commons_cli import advisory_commons_cli
from elliottlib.cli.find_bugs_blocker_cli import find_bugs_blocker_cli
from elliottlib.cli.remove_bugs_cli import remove_bugs_cli
from elliottlib.cli.repair_bugs_cli import repair_bugs_cli
from elliottlib.cli.find_unconsumed_rpms import find_unconsumed_rpms_cli
from elliottlib.cli.find_bugs_kernel_cli import find_bugs_kernel_cli
from elliottlib.cli.find_bugs_kernel_clones_cli import find_bugs_kernel_clones_cli
from elliottlib.cli.move_builds_cli import move_builds_cli
from elliottlib.cli.find_bugs_golang_cli import find_bugs_golang_cli
from elliottlib.cli.remove_builds_cli import remove_builds_cli
from elliottlib.cli.get_golang_report_cli import get_golang_report_cli

# 3rd party
import click
from errata_tool import ErrataException


# -----------------------------------------------------------------------------
# Constants and defaults
# -----------------------------------------------------------------------------
pass_runtime = click.make_pass_decorator(Runtime)


#
# Get an Advisory
# advisory:get
#
@cli.command("get", short_help="Get information for an ADVISORY")
@click.argument('advisory', type=int, required=False)
@use_default_advisory_option
@click.option('--details', is_flag=True, default=False,
              help="Print the full object of the advisory")
@click.option('--id-only', is_flag=True, default=False,
              help="Print only the ID of the default advisory")
@click.option('--json', 'as_json', metavar="FILE_NAME",
              help="Dump the advisory as JSON to a file (or '-' for stdout)")
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
            url=advisory.url())
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
# Verify images in a payload match the corresponding advisory
# verify-payload
#


@cli.command("verify-payload", short_help="Verify payload contents match advisory builds")
@click.argument("payload")
@click.argument('advisory', type=int)
@click.pass_obj
@click_coroutine
async def verify_payload(runtime, payload, advisory):
    """Cross-check that the builds present in PAYLOAD match the builds
attached to ADVISORY. The payload is treated as the source of
truth. If something is absent or different in the advisory it is
treated as an error with the advisory.

\b
    PAYLOAD - Full pullspec of the payload to verify
    ADVISORY - Numerical ID of the advisory

Report on:

\b
 1. Missing in Advisory - Payload nvrs that are missing from the given advisory
 2. Extra in Advisory - Nvrs that are not in payload but are attached to advisory
 3. Attached but Shipped Or Pending - Nvrs that are in payload and attached, but are also attached more advisory(s)
 which might be pending release or shipped

Results are summarily printed at the end of the run. They are also
written out to summary_results.json.

     Verify builds in the given payload match the builds attached to
     advisory 41567

 \b
    $ elliott -g openshift-1 verify-payload quay.io/openshift-release-dev/ocp-release:4.1.0-rc.6 41567

    """
    runtime.initialize()
    rhcos_images = {c['name'] for c in rhcos.get_container_configs(runtime)}

    runtime.logger.info("Fetching nvrs from payload..")
    all_payload_nvrs = await util.get_nvrs_from_payload(payload, rhcos_images, runtime.logger)
    all_payload_nvrs = set(all_payload_nvrs)
    runtime.logger.info(f"Found {len(all_payload_nvrs)} nvrs in payload")

    runtime.logger.info("Fetching nvrs from advisory..")
    all_advisory_nvrs = elliottlib.errata.get_all_advisory_nvrs(advisory)

    # filter out rhcos nvrs since they are special and not auto-stitched and fetched by util.get_nvrs_from_payload
    all_advisory_nvrs = [n for n in all_advisory_nvrs if "rhcos-" not in n[0]]

    all_advisory_nvrs = set(all_advisory_nvrs)
    runtime.logger.info(f"Found {len(all_advisory_nvrs)} nvrs in advisory")

    missing_in_advisory = all_payload_nvrs - all_advisory_nvrs
    extra_in_advisory = all_advisory_nvrs - all_payload_nvrs
    runtime.logger.info(f"Extra in advisory {len(extra_in_advisory)}")

    in_pending_advisory = set()
    in_shipped_advisory = set()
    attached_but_shipped_or_pending = set()

    runtime.logger.info("Checking if any payload nvrs that are attached have been shipped...")
    for nvr_tuple in all_payload_nvrs & all_advisory_nvrs:
        nvr = "-".join(nvr_tuple)
        build = elliottlib.errata.get_brew_build(nvr)
        for ad in build.all_errata:
            if ad['id'] == advisory:
                continue
            if ad["status"] == "DROPPED_NO_SHIP":
                continue
            attached_but_shipped_or_pending.add(nvr_tuple)
    if attached_but_shipped_or_pending:
        runtime.logger.info(f"Nvrs that are attached but shipped or pending {len(attached_but_shipped_or_pending)}")

    if missing_in_advisory:  # check if missing images are already shipped or pending to ship
        runtime.logger.info(f"Checking if {len(missing_in_advisory)} missing images are shipped...")
        for nvr_tuple in missing_in_advisory:
            nvr = "-".join(nvr_tuple)
            build = elliottlib.errata.get_brew_build(nvr)
            for ad in build.all_errata:
                if ad["status"] == "DROPPED_NO_SHIP":
                    continue
                if ad["status"] == "SHIPPED_LIVE":
                    in_shipped_advisory.add(nvr_tuple)
                else:
                    in_pending_advisory.add(nvr_tuple)
        missing_in_advisory = missing_in_advisory - in_pending_advisory - in_shipped_advisory
    runtime.logger.info(f"Missing in advisory {len(missing_in_advisory)}")

    output = {
        "missing_in_advisory": sorted("-".join(t) for t in missing_in_advisory),
        "extra_in_advisory": sorted("-".join(t) for t in extra_in_advisory),
        "in_pending_advisory": sorted("-".join(t) for t in in_pending_advisory),
        "in_shipped_advisory": sorted("-".join(t) for t in in_shipped_advisory),
        "attached_but_shipped_or_pending": sorted("-".join(t) for t in attached_but_shipped_or_pending)
    }

    green_print("Summary results:")
    click.echo(json.dumps(output, indent=4))
    with open('summary_results.json', 'w') as fp:
        json.dump(output, fp, indent=4)
    green_prefix("Wrote out summary results: ")
    click.echo("summary_results.json")

#
# Poll for rpm-signed state change
# poll-signed
#


@cli.command("poll-signed", short_help="Poll for RPM build 'signed' status")
@click.option("--minutes", "-m", required=False,
              default=15, type=int,
              help="How long to poll before quitting")
@click.option("--advisory", "-a",
              type=int, metavar='ADVISORY',
              help="Advisory to watch")
@use_default_advisory_option
@click.option("--noop", "--dry-run",
              required=False,
              default=False, is_flag=True,
              help="Don't actually poll, just print the signed status of each build")
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
            pbar_header("Getting build signatures: ",
                        "Should be pretty quick",
                        all_builds)
            pool = ThreadPool(cpu_count())
            # Look up builds concurrently
            click.secho("[", nl=False)

            build_sigs = pool.map(
                lambda build: progress_func(
                    lambda: elliottlib.errata.build_signed(build),
                    '*'),
                all_builds)
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
cli.add_command(find_bugs_sweep_cli)
cli.add_command(find_builds_cli)
cli.add_command(tag_builds_cli)
cli.add_command(tarball_sources_cli)
cli.add_command(verify_cvp_cli)
cli.add_command(advisory_drop_cli)
cli.add_command(verify_attached_operators_cli)
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
