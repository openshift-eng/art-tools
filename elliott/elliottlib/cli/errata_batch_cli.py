import click

from artcommonlib import logutil
from elliottlib.cli.common import cli
from elliottlib.errata import get_advisory_batch, set_advisory_batch, unset_advisory_batch, find_batch_for_release

LOGGER = logutil.get_logger(__name__)


@cli.group("errata", short_help="Manage errata in Errata Tool")
def errata():
    pass


@errata.group("batch", short_help="Manage errata batch")
def batch():
    pass


@batch.command("set", short_help="Set errata batch for one or more advisories")
@click.argument("advisories", nargs=-1, type=click.IntRange(1), required=False)
@click.option("--batch-id", required=False, type=int, help="Batch id to set")
@click.option("--auto", is_flag=True, help="Find batch id by assembly name. Only works with --assembly")
@click.option("--noop", "--dry-run", is_flag=True, help="Dry run")
@click.pass_obj
def errata_batch_set_cli(runtime, advisories, batch_id, auto, noop):
    """Set batch for one or multiple advisories.

Advisories created for an OCP version have common fields, that sometimes
will need updating. This command helps with that.

    NOTE: The two advisory input options (--assembly and [advisories])
    are mutually exclusive and can not be used together.

    Set batch for all advisories for an assembly/group/advisories

    $ elliott -g openshift-4.15 --assembly 4.15.12 errata batch set --batch-id 12345

    Set batch for a specific advisory

    $ elliott -g openshift-4.15 errata batch set 80825 --batch-id 12345
"""
    named_assembly = runtime.assembly not in ['test', 'stream']
    if sum(map(bool, [named_assembly, advisories])) > 1:
        raise click.BadParameter("Use only one of --assembly or [advisories]")
    if sum(map(bool, [auto, batch_id])) > 1:
        raise click.BadParameter("Use only one of --auto or --batch-id")

    if auto:
        if not named_assembly:
            raise click.BadParameter("--auto can be used only with a named --assembly")
        batch_id = find_batch_for_release(runtime.assembly)
        if not batch_id:
            raise click.BadParameter(f"Batch not found for {runtime.assembly}. Create it first")

    if not advisories:
        runtime.initialize()
        advisories = runtime.group_config.advisories.values()

    errors = []
    for advisory_id in advisories:
        try:
            current_batch = get_advisory_batch(advisory_id)

            if current_batch == batch_id:
                LOGGER.info(f"Advisory {advisory_id} - No change. Given batch is already set")
            else:
                LOGGER.info(f"Advisory {advisory_id} - changing batch from {current_batch} ➔ {batch_id}")
                if not noop:
                    set_advisory_batch(advisory_id, batch_id)
        except Exception as ex:
            LOGGER.error(f'Error fetching/changing {advisory_id}: {ex}')
            errors.append(ex)
    if errors:
        raise Exception(errors)
