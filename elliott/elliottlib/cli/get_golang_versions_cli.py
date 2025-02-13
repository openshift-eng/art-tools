import click

from artcommonlib import rhcos, format_util, logutil
from artcommonlib.format_util import green_print
from artcommonlib.rpm_utils import parse_nvr
from elliottlib import util, errata
from elliottlib.cli.common import (cli, find_default_advisory,
                                   use_default_advisory_option)
from elliottlib.cli.common import click_coroutine

_LOGGER = logutil.get_logger(__name__)


@cli.command("go", short_help="Get version of Go for advisory builds")
@click.option("--release", "-r",
              help="Release/nightly pullspec to inspect builds for")
@click.option('--advisory', '-a', 'advisory_id', type=int,
              help="The advisory ID to fetch builds from")
@use_default_advisory_option
@click.option('--nvrs', '-n',
              help="Brew nvrs to show go version for. Comma separated")
@click.option('--components', '-c',
              help="Only show go versions for these components (rpms/images) in advisory. Comma separated")
@click.option('-o', '--output',
              type=click.Choice(['json', 'text']),
              default='text', help='Output format')
@click.option('--report', '-R', 'report', is_flag=True, default=False)
@click.pass_obj
@click_coroutine
async def get_golang_versions_cli(runtime, release, advisory_id, default_advisory_type, nvrs, components, output,
                                  report):
    """
    Get the Go version for brew builds specified via nvrs / advisory / release

    Usage:

\b
    $ elliott go -a 76557

    List go version for all golang builds in the given advisory

\b
    $ elliott go -a 79683 -c ironic-container,ose-ovirt-csi-driver-container

    List go version for the given brew components in the advisory

\b
    $ elliott -g openshift-4.12 --assembly 4.12.13 go --use-default-advisory image

    Use default advisory for a group/assembly

\b
    $ elliott go -n podman-3.0.1-6.el8,podman-1.9.3-3.rhaos4.6.el8

    List go version for given brew nvrs

\b
    $ elliott go -r registry.ci.openshift.org/ocp/release:4.14.0-0.nightly-2023-04-24-145153

    List go version for given release pullspec
"""
    count_options = sum(map(bool, [advisory_id, nvrs, default_advisory_type, release]))
    if count_options > 1:
        raise click.BadParameter("Use only one of --release, --advisory, --nvrs, --use-default-advisory")

    advisory_id, rhcos_images = None, None
    if default_advisory_type or release:
        runtime.initialize()
        if default_advisory_type:
            advisory_id = find_default_advisory(runtime, default_advisory_type)
        elif release:
            rhcos_images = {c['name'] for c in rhcos.get_container_configs(runtime)}
    else:
        runtime.initialize(no_group=True)

    if components:
        components = [c.strip() for c in components.split(',')]
        if release and not all(c.endswith('-container') for c in components):
            raise click.BadParameter('components for payload should end with -container')

    if release:
        return await print_release_golang(release, rhcos_images, components, output, report)
    elif advisory_id:
        return print_advisory_golang(advisory_id, components, output, report)
    elif nvrs:
        nvrs = [n.strip() for n in nvrs.split(',')]
        return print_nvrs_golang(nvrs, output, report)
    else:
        format_util.red_print('The input value is not valid.')


def print_nvrs_golang(nvrs, output, report):
    container_nvrs, rpm_nvrs = [], []
    for n in nvrs:
        parsed_nvr = parse_nvr(n)
        nvr_tuple = (parsed_nvr['name'], parsed_nvr['version'], parsed_nvr['release'])
        if 'container' in parsed_nvr['name']:
            container_nvrs.append(nvr_tuple)
        else:
            rpm_nvrs.append(nvr_tuple)

    go_nvr_map = {}
    if rpm_nvrs:
        go_nvr_map.update(util.get_golang_rpm_nvrs(rpm_nvrs, _LOGGER))
    if container_nvrs:
        go_nvr_map.update(util.get_golang_container_nvrs(container_nvrs, _LOGGER))

    if not go_nvr_map:
        green_print('No golang builds detected')

    if output == 'json':
        util.pretty_print_nvrs_go_json(go_nvr_map, report)
    elif output == 'text':
        util.pretty_print_nvrs_go(go_nvr_map, report)


def print_advisory_golang(advisory_id, components, output, report):
    nvrs = errata.get_all_advisory_nvrs(advisory_id)
    _LOGGER.debug(f'{len(nvrs)} builds found in advisory')
    if not nvrs:
        _LOGGER.debug('No golang builds found. exiting')
        green_print(f'No golang builds found in advisory {advisory_id}')
        return
    if components:
        if 'openshift' in components:
            components.remove('openshift')
            components.append('openshift-hyperkube')
        nvrs = [p for p in nvrs if p[0] in components]

    content_type = errata.get_erratum_content_type(advisory_id)
    if content_type == 'docker':
        go_nvr_map = util.get_golang_container_nvrs(nvrs, _LOGGER)
    else:
        go_nvr_map = util.get_golang_rpm_nvrs(nvrs, _LOGGER)

    if output == 'json':
        util.pretty_print_nvrs_go_json(go_nvr_map, report)
    elif output == 'text':
        util.pretty_print_nvrs_go(go_nvr_map, report)


async def print_release_golang(pullspec, rhcos_images, components, output, report):
    nvr_map = await util.get_nvrs_from_release(pullspec, rhcos_images, _LOGGER)
    nvrs = [(n, vr_tuple[0], vr_tuple[1]) for n, vr_tuple in nvr_map.items()]
    _LOGGER.debug(f'{len(nvrs)} builds found in {pullspec}')
    if not nvrs:
        _LOGGER.debug('No golang builds found. exiting')
        green_print(f'No golang builds found in release {pullspec}')
        return
    if components:
        nvrs = [p for p in nvrs if p[0] in components]

    go_nvr_map = util.get_golang_container_nvrs(nvrs, _LOGGER)
    if output == 'json':
        util.pretty_print_nvrs_go_json(go_nvr_map, report)
    elif output == 'text':
        util.pretty_print_nvrs_go(go_nvr_map, report)
