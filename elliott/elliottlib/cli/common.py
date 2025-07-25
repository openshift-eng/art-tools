import asyncio
import sys
from functools import update_wrapper

import click
from artcommonlib import dotconfig
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.format_util import green_prefix, red_prefix, yellow_print

from elliottlib import Runtime, __version__, constants, errata
from elliottlib.cli import cli_opts


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(f'Elliott v{__version__}')
    click.echo(f'Python v{sys.version}')
    ctx.exit()


context_settings = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=context_settings)
@click.option('--version', is_flag=True, callback=print_version, expose_value=False, is_eager=True)
@click.option(
    '--working-dir',
    metavar='PATH',
    envvar='ELLIOTT_WORKING_DIR',
    default=None,
    help='Existing directory in which file operations should be performed.',
)
@click.option('--data-path', metavar='PATH', default=None, help='Git repo or directory containing groups metadata')
@click.option(
    '--shipment-path',
    metavar='PATH',
    default=None,
    help="Git repo or directory containing group metadata for shipping a konflux release e.g."
    f" {SHIPMENT_DATA_URL_TEMPLATE.format('ocp')}. Defaults to `main` branch for a repo - to point to a "
    "different branch/commit use repo@commitish",
)
@click.option(
    '--disable-gssapi', default=False, is_flag=True, help='Disable gssapi for requests that do not require keytab'
)
@click.option('--group', '-g', default=None, metavar='NAME', help='The group of images on which to operate.')
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    default='stream',
    help="The name of an assembly to rebase & build for. Assemblies must be enabled in group.yml or with --enable-assemblies.",
)
@click.option(
    '--enable-assemblies',
    default=False,
    is_flag=True,
    help='Enable assemblies even if not enabled in group.yml. Primarily for testing purposes.',
)
@click.option('--branch', default=None, metavar='BRANCH', help='Branch to override any default in group.yml.')
@click.option(
    '-i',
    '--images',
    default=[],
    metavar='NAME',
    multiple=True,
    help='Name of group image member to include in operation (all by default). Can be comma delimited list.',
)
@click.option(
    "-r",
    "--rpms",
    default=[],
    metavar='NAME',
    multiple=True,
    help="Name of group rpm member to include in operation (all by default). Can be comma delimited list.",
)
@click.option(
    '-x',
    '--exclude',
    default=[],
    metavar='NAME',
    multiple=True,
    help='Name of group image or rpm member to exclude in operation (none by default). Can be comma delimited list.',
)
@click.option('--quiet', '-q', default=False, is_flag=True, help='Suppress non-critical output')
@click.option('--debug', default=False, is_flag=True, help='Show debug output on console.')
@click.option(
    "--brew-event",
    metavar='EVENT',
    type=click.INT,
    default=None,
    help="Lock koji clients from runtime to this brew event.",
)
@click.option(
    "--build-system",
    default='brew',
    envvar='BUILD_SYSTEM',
    help="Which build system (Brew/Konflux) to consider when searching for builds.",
)
@click.pass_context
def cli(ctx, **kwargs):
    cfg = dotconfig.Config(
        'elliott', 'settings', template=cli_opts.CLI_CONFIG_TEMPLATE, envvars=cli_opts.CLI_ENV_VARS, cli_args=kwargs
    )
    ctx.obj = Runtime(cfg_obj=cfg, **cfg.to_dict())


#
# Look up a default advisory specified for the branch in ocp-build-data
# Advisory types are in elliottlib.constants.standard_advisory_types
# but this doesn't validate against that.
#
def find_default_advisory(runtime, default_advisory_type, quiet=False):
    '''The `quiet` parameter will disable printing the informational message'''
    default_advisory = runtime.group_config.advisories.get(default_advisory_type, None)
    if default_advisory is None:
        red_prefix('No value defined for default advisory:')
        click.echo(
            ' The key advisories.{} is not defined for group {} in [group|releases].yml'.format(
                default_advisory_type, runtime.group
            )
        )
        exit(1)
    if not quiet:
        green_prefix('Default advisory detected: ')
        click.echo(default_advisory)
    return default_advisory


use_default_advisory_option = click.option(
    '--use-default-advisory',
    'default_advisory_type',
    metavar='ADVISORY_TYPE',
    type=click.Choice(constants.standard_advisory_types),
    help='Use the default value from [group|releases].yml for ADVISORY_TYPE [{}]'.format(
        ', '.join(constants.standard_advisory_types)
    ),
)

pass_runtime = click.make_pass_decorator(Runtime)


def click_coroutine(f):
    """A wrapper to allow to use asyncio with click.
    https://github.com/pallets/click/issues/85
    """

    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))

    return update_wrapper(wrapper, f)


def move_builds(attached_builds, kind, from_advisory, to_advisory):
    # remove builds
    from_erratum = errata.Advisory(errata_id=from_advisory)
    from_erratum.ensure_state('NEW_FILES')
    from_erratum.remove_builds(list(b.nvr for b in attached_builds))
    # will not attempt moving advisory to old state; if empty, ET will not allow

    # add builds
    to_erratum = errata.Advisory(errata_id=to_advisory)
    old_state = to_erratum.errata_state
    to_erratum.ensure_state('NEW_FILES')
    to_erratum.attach_builds(attached_builds, kind)
    if old_state != 'NEW_FILES':
        try:
            to_erratum.ensure_state(old_state)
        except errata.ErrataException as ex:
            yellow_print(f"Unable to move advisory {to_advisory} to {old_state}: {ex}. Please move it manually.")
