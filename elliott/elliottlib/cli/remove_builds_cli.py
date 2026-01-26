import sys

import click
from artcommonlib import logutil
from errata_tool import ErrataException

from elliottlib import Runtime, errata
from elliottlib.cli.common import cli, find_default_advisory, use_default_advisory_option
from elliottlib.util import ensure_erratatool_auth

LOGGER = logutil.get_logger(__name__)


@cli.command("remove-builds", short_help="Remove builds from ADVISORY")
@click.argument("builds", metavar="<NVR_OR_ID>", nargs=-1, required=False, default=None)
@click.option("--advisory", "-a", "advisory_id", type=int, metavar="ADVISORY", help="Remove found builds from ADVISORY")
@use_default_advisory_option
@click.option(
    "--all", "clean", required=False, default=False, is_flag=True, help="Remove all builds attached to the Advisory"
)
@click.option("--noop", "--dry-run", is_flag=True, default=False, help="Don't change anything")
@click.option(
    "--builds-file",
    "-f",
    "builds_file",
    help="File to read builds from, `-` to read from STDIN.",
    type=click.File("rt"),
)
@click.pass_obj
def remove_builds_cli(runtime: Runtime, builds, advisory_id, default_advisory_type, clean, noop, builds_file):
    """Remove builds (image or rpm) from ADVISORY.

        Remove builds that are attached to an advisory:

    \b
        $ elliott --group openshift-4.14 remove-builds
        openshift-enterprise-deployer-container-v4.14.0-202401121302.p0.g286cfa5.assembly.stream
        ose-installer-container-v4.14.0-202401121302.p0.ga57f47f.assembly.stream --advisory 1234123

        Remove build from default rpm advisory

    \b
        $ elliott --group openshift-4.14 --assembly 4.14.9 remove-builds fmt-10.2.1-1.el9 --use-default-advisory rpm

        Remove all builds from default image advisory

    \b
        $ elliott --group openshift-4.14 --assembly 4.14.9 remove-builds --all --use-default-advisory image

    """
    if bool(builds) and bool(builds_file):
        raise click.BadParameter("Use only one of --build or --builds-file")

    if builds_file:
        if builds_file == "-":
            builds_file = sys.stdin
        builds = [line.strip() for line in builds_file.readlines()]

    if bool(clean) == bool(builds):
        raise click.BadParameter("Specify either <NVRs> or --all param")
    if bool(advisory_id) == bool(default_advisory_type):
        raise click.BadParameter("Specify either --advisory <ADVISORY_ID> or --use-default-advisory")

    runtime.initialize()
    if default_advisory_type is not None:
        advisory_id = find_default_advisory(runtime, default_advisory_type)

    ensure_erratatool_auth()

    builds_to_remove = []
    advisory_builds = errata.get_brew_builds(advisory_id)

    if builds:
        builds_to_remove = [b.nvr for b in advisory_builds if b.nvr in builds]
    elif clean:
        builds_to_remove = [b.nvr for b in advisory_builds]

    LOGGER.info(f"Found {len(builds_to_remove)} build(s) attached to advisory")
    if not builds_to_remove:
        return
    if len(builds_to_remove) < 10:
        LOGGER.info(builds_to_remove)

    LOGGER.info(f"Removing build(s) from advisory {advisory_id} ..")

    if noop:
        LOGGER.info("[NOOP] Would've removed build(s) from advisory")
        return

    try:
        erratum = errata.Advisory(errata_id=advisory_id)
        erratum.ensure_state("NEW_FILES")
        erratum.remove_builds(builds_to_remove)
    except ErrataException as e:
        LOGGER.error(f"Cannot change advisory {advisory_id}: {e}")
        exit(1)
