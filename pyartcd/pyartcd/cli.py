import asyncio
import logging
import sys
from functools import update_wrapper
from pathlib import Path
from typing import Optional

import click

from pyartcd import __version__
from pyartcd.runtime import Runtime

pass_runtime = click.make_pass_decorator(Runtime)


def click_coroutine(f):
    """ A wrapper to allow to use asyncio with click.
    https://github.com/pallets/click/issues/85
    """
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))
    return update_wrapper(wrapper, f)


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('artcd v{}'.format(__version__))
    click.echo('Python v{}'.format(sys.version))
    ctx.exit()


# ============================================================================
# GLOBAL OPTIONS: parameters for all commands
# ============================================================================
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True,
              help="Print version information and quit")
@click.option("--config", "-c", metavar='PATH',
              help="Configuration file ('~/.config/artcd.toml' by default)")
@click.option("--working-dir", "-C", metavar='PATH', default=None,
              help="Existing directory in which file operations should be performed (current directory by default)")
@click.option("--dry-run", is_flag=True,
              help="don't actually execute the pipeline; just print what would be done")
@click.option("--verbosity", "-v", count=True,
              help="[MULTIPLE] increase output verbosity")
@click.pass_context
def cli(ctx: click.Context, config: Optional[str], working_dir: Optional[str], dry_run: bool, verbosity: int):

    config_filename = config or Path("~/.config/artcd.toml").expanduser()
    working_dir = working_dir or Path.cwd()
    # configure logging
    if not verbosity:
        logging.basicConfig(level=logging.WARNING)
    elif verbosity == 1:
        logging.basicConfig(level=logging.INFO)
    elif verbosity >= 2:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger("requests_kerberos").setLevel(logging.INFO)
    else:
        raise ValueError(f"Invalid verbosity {verbosity}")
    ctx.obj = Runtime.from_config_file(config_filename, working_dir=Path(working_dir), dry_run=dry_run)
