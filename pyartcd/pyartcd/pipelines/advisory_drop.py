import logging
import os
from typing import Optional, Union

import click
from artcommonlib import exectools

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

logger = logging.getLogger(__name__)

DEFAULT_DROP_COMMENT = (
    "This bug will be dropped from current advisory because the advisory "
    "will also be dropped and not going to be shipped."
)


async def drop_advisory(
    group: str,
    advisory: Union[int, str],
    comment: str = DEFAULT_DROP_COMMENT,
    env: Optional[dict] = None,
    dry_run: bool = False,
):
    """Repair bugs and drop an advisory.

    Runs ``elliott repair-bugs`` to move bugs back to VERIFIED and close
    placeholders, then runs ``elliott advisory-drop`` to drop the advisory.

    :param group: OCP group name (e.g. ``openshift-4.17``).
    :param advisory: Advisory number (int or str).
    :param comment: Comment to attach to repaired bugs.
    :param env: Environment variables for the elliott commands.
        Falls back to ``os.environ`` when *None*.
    :param dry_run: When *True*, log what would happen but do not execute.
    """
    if env is None:
        env = os.environ.copy()

    advisory = str(advisory)

    logger.info("Repairing bugs on advisory %s before dropping...", advisory)

    if dry_run:
        logger.warning("[DRY RUN] Would repair bugs and drop advisory %s", advisory)
        return

    # repair-bugs: move bugs back and close placeholders
    repair_cmd = [
        'elliott',
        '--group',
        group,
        'repair-bugs',
        '--advisory',
        advisory,
        '--auto',
        '--comment',
        comment,
        '--close-placeholder',
        '--from',
        'RELEASE_PENDING',
        '--to',
        'VERIFIED',
    ]
    await exectools.cmd_assert_async(repair_cmd, env=env)

    # drop the advisory
    drop_cmd = [
        'elliott',
        '--group',
        group,
        'advisory-drop',
        advisory,
    ]
    await exectools.cmd_assert_async(drop_cmd, env=env)

    logger.info("Successfully dropped advisory %s", advisory)


@cli.command('advisory-drop')
@click.option('--group', required=True, help='OCP group')
@click.option('--advisory', required=True, help='Advisory number')
@click.option(
    '--comment',
    required=False,
    default=DEFAULT_DROP_COMMENT,
    help='Comment will add to the bug attached on the advisory to explain the reason',
)
@pass_runtime
@click_coroutine
async def advisory_drop_cli(runtime: Runtime, group: str, advisory: str, comment: str):
    await drop_advisory(group=group, advisory=advisory, comment=comment, dry_run=runtime.dry_run)
