import base64
import click
import os
import yaml
import sys
import io
import pathlib
import re
from typing import Dict
from ghapi.core import GhApi

from artcommonlib.format_util import red_print, green_print, yellow_print, color_print
from doozerlib import metadata, gitdata, Runtime
from doozerlib.cli import cli, pass_runtime
from doozerlib.config import MetaDataConfig as mdc
from doozerlib.rhcos import RHCOSBuildInspector
from doozerlib.util import dict_get
from doozerlib.exceptions import DoozerFatalError


# config:* commands are a special beast and
# requires the same non-standard runtime options
CONFIG_RUNTIME_OPTS = {
    'mode': 'both',           # config wants it all
    'clone_distgits': False,  # no need, just doing config
    'clone_source': False,    # no need, just doing config
    'disabled': True,         # show all, including disabled/wip
    'prevent_cloning': True,  # raise exception is somehow we try to clone
    'config_only': True,      # only initialize config and nothing else
    'group_only': False       # only initialize group, logging and nothing else
}

option_config_commit_msg = click.option("--message", "-m", metavar='MSG', help="Commit message for config change.", default=None)


# Normally runtime only runs in one mode as you never do
# rpm AND image operations at once. This is not so with config
# functions. This intelligently chooses modes for these only
def _fix_runtime_mode(runtime):
    mode = 'both'
    if runtime.rpms and not runtime.images:
        mode = 'rpms'
    elif runtime.images and not runtime.rpms:
        mode = 'images'

    CONFIG_RUNTIME_OPTS['mode'] = mode


@cli.command("config:commit", help="Commit pending changes from config:new")
@option_config_commit_msg
@click.option('--push/--no-push', default=False, is_flag=True,
              help='Push changes back to config repo. --no-push is default')
@pass_runtime
def config_commit(runtime, message, push):
    """
    Commit outstanding metadata config changes
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(no_group=False, **CONFIG_RUNTIME_OPTS)

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    config = mdc(runtime)
    config.commit(message)
    if push:
        config.push()


@cli.command("config:push", help="Push all pending changes to config repo")
@pass_runtime
def config_push(runtime):
    """
    Push changes back to config repo.
    Will of course fail if user does not have write access.
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(no_group=False, **CONFIG_RUNTIME_OPTS)
    config = mdc(runtime)
    config.push()


@cli.command("config:get", short_help="Pull latest config data into working directory")
@pass_runtime
def config_get(runtime):
    """
    Pull latest config data into working directory.
    This function exists as a convenience for working with the
    config manually.
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(no_group=False, **CONFIG_RUNTIME_OPTS)


@cli.command("config:read-group", short_help="Output aspects of the group.yml")
@click.argument("key", nargs=1, metavar="KEY", type=click.STRING, default=None, required=False)
@click.option("--length", "as_len", default=False, is_flag=True, help='Print length of dict/list specified by key')
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@click.option("--permit-missing-group", default=False, is_flag=True, help='Show default if group is missing')
@click.option("--default", help="Value to print if key cannot be found", default=None)
@click.option("--out-file", help="Specific key in config to print", default=None)
@pass_runtime
def config_read_group(runtime, key, as_len, as_yaml, permit_missing_group, default, out_file):
    """
    Read data from group.yaml for given group and key, If key is not specified,
    the entire group data structure will be output.

    Usage:

    $ doozer --group=openshift-4.0 config:read-group [KEY] --yaml

    Where [KEY] is a key inside group.yaml that you would like to read.
    Key dot-notation is supported, such as: sources.ose.branch.fallback

    Examples:
    $ doozer --group=openshift-4.3 config:read-group sources.ose.url

    # Print yaml formatted list of non_release images (print empty list if not present).
    $ doozer --group=openshift-4.3 config:read-group --default '[]' --yaml non_release.images

    # How many images are in the non_release.images list (print 0 list if not present)
    $ doozer --group=openshift-4.3 config:read-group --default 0 --len non_release.images

    """
    _fix_runtime_mode(runtime)
    try:
        runtime.initialize(**CONFIG_RUNTIME_OPTS)
    except gitdata.GitDataException:
        # This may happen if someone if trying to get data for a branch that does not exist.
        # This may be perfectly OK if they are just trying to check the next minor's branch,
        # but that branch does not exist yet. Caller must specify --permit-missing to allow
        # this behavior.
        if permit_missing_group and default:
            red_print(default)
            exit(0)
        raise

    group_primitive = runtime.get_group_config().primitive()
    if key is None:
        value = group_primitive
    else:
        value = dict_get(group_primitive, key, None)
        if value is None:
            if default is not None:
                red_print(default)
                exit(0)
            raise DoozerFatalError('No default specified and unable to find key: {}'.format(key))

    if as_len:
        if hasattr(value, '__len__'):
            value = len(value)
        else:
            raise DoozerFatalError('Extracted element has no length: {}'.format(key))
    elif as_yaml:
        value = yaml.safe_dump(value, indent=2, default_flow_style=False)

    if out_file:
        with io.open(out_file, 'w', encoding="utf-8") as f:
            f.write(value)

    print(str(value))


def get_releases(runtime) -> dict:
    """
    Uses GitHub API to fetch releases.yaml from openshift-eng/ocp-build-data for a given group

    Parses the file and returns it as a dictionary
    """

    if not runtime.data_path.startswith('https://'):
        # assume data_path is a local path; GhApi couldn't handle other sources anyway
        with open(f'{runtime.data_path}/releases.yml', 'r') as file:
            return yaml.safe_load(file)

    if not (github_token := os.environ.get('GITHUB_TOKEN')):
        raise DoozerFatalError('A GITHUB_TOKEN environment variable must be defined!')

    owner = runtime.data_path.split('/')[-2]
    api = GhApi(owner=owner, repo='ocp-build-data', token=github_token)
    blob = api.repos.get_content('releases.yml', ref=runtime.group_commitish)
    return yaml.safe_load(base64.b64decode(blob['content']))


@cli.command("config:read-releases", short_help="Output aspects of releases.yml")
@click.option("--length", "as_len", default=False, is_flag=True, help='Print number of assemblies defined for group')
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@click.option("--out-file", help="Output contents to a file instead of stdout", default=None)
@pass_runtime
def config_read_releases(runtime, as_len, as_yaml, out_file):
    """
    Read data from releases.yaml for given group and key.
    If key is not specified, the entire release data structure will be output.

    Usage:

    $ doozer --group=openshift-4.14 config:read-releases

    $ doozer --group=openshift-4.14 config:read-releases --yaml

    $ doozer --group=openshift-4.13 config:read-releases --length

    $ doozer --group=openshift-4.13 config:read-releases --yaml --out-file /tmp/out.yaml
    """

    CONFIG_RUNTIME_OPTS['group_only'] = True
    runtime.initialize(**CONFIG_RUNTIME_OPTS)
    content = get_releases(runtime)

    if as_len:
        output = len(content['releases'])
    elif as_yaml:
        output = yaml.safe_dump(content)
    else:
        output = content

    if out_file:
        try:
            with io.open(out_file, 'w', encoding="utf-8") as f:
                f.write(str(output))
        except PermissionError:
            click.echo(f'Permission denied: could not write to {out_file}')
            sys.exit(1)

    else:
        click.echo(output)


@cli.command("config:read-assembly", short_help="Output aspects of a specific assembly defined in releases.yml")
@click.option("--default", help="Value to print if key cannot be found", default=None)
@click.option("--length", "as_len", default=False, is_flag=True, help='Print length of dict/list specified by key')
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Format output as YAML')
@click.option("--out-file", help="Output contents to a file instead of stdout", default=None)
@click.argument("key", nargs=1, metavar="KEY", type=click.STRING, default=None, required=False)
@pass_runtime
def config_read_assemblies(runtime, default, as_len, as_yaml, out_file, key):
    """
    Read data from releases.yaml for given group, assembly and key.
    An assembly must be specified. To get a global representation of release.yaml,
    use doozer config:read-releases instead

    Usage:

    $ doozer --group=openshift-4.13 --assembly 4.13.1 config:read-assembly

    $ doozer --group=openshift-4.13 --assembly 4.13.1 config:read-assembly --yaml

    $ doozer --group=openshift-4.13 --assembly 4.13.1 config:read-assembly --yaml --out-file /tmp/out.yaml

    $ doozer --group=openshift-4.13 --assembly 4.13.1 config:read-assembly --yaml assembly.issues.exclude --length

    $ doozer --group=openshift-4.13 --assembly 4.13.1 config:read-assembly --yaml assembly.promotion_permits

    $ doozer --group=openshift-4.13 --assembly 4.13.1 config:read-assembly --yaml assembly.promotion_permits --default []
    """

    CONFIG_RUNTIME_OPTS['group_only'] = True
    runtime.initialize(**CONFIG_RUNTIME_OPTS)
    releases = get_releases(runtime)['releases']
    try:
        assembly_data = releases[runtime.assembly]
    except KeyError:
        raise DoozerFatalError(f'No assembly data found for assembly "{runtime.assembly}" in group {runtime.group}')

    if key is not None:
        assembly_data = dict_get(assembly_data, key, None)
        if assembly_data is None:
            if default is not None:
                click.echo(default)
                sys.exit(0)
            raise DoozerFatalError('No default specified and unable to find key: {}'.format(key))

    if as_len:
        output = len(assembly_data)

    elif as_yaml:
        output = yaml.safe_dump(assembly_data)

    else:
        output = assembly_data

    if out_file:
        try:
            with io.open(out_file, 'w', encoding="utf-8") as f:
                f.write(str(output))
        except PermissionError:
            click.echo(f'Permission denied: could not write to {out_file}')
            sys.exit(1)

    else:
        click.echo(output)


@cli.command("config:update-mode", short_help="Update config(s) mode. enabled|disabled|wip")
@click.argument("mode", nargs=1, metavar="MODE", type=click.Choice(metadata.CONFIG_MODES))  # new mode value
@click.option('--push/--no-push', default=False, is_flag=True,
              help='Push changes back to config repo. --no-push is default')
@option_config_commit_msg
@pass_runtime
def config_mode(runtime, mode, push, message):
    """Update [MODE] of given config(s) to one of:
    - enable: Normal operation
    - disable: Will not be used unless explicitly specified
    - wip: Same as `disable` plus affected by --wip flag

    Filtering of configs is based on usage of the following global options:
    --group, --images/-i, --rpms/-r

    See `doozer --help` for more.

    Usage:

    $ doozer --group=openshift-4.0 -i aos3-installation config:mode [MODE]

    Where [MODE] is one of enable, disable, or wip.

    Multiple configs may be specified and updated at once.

    Commit message will default to stating mode change unless --message given.
    If --push not given must use config:push after.
    """
    _fix_runtime_mode(runtime)
    if not runtime.load_wip and CONFIG_RUNTIME_OPTS['mode'] == 'both':
        red_print('Updating all mode for all configs in group is not allowed! Please specifiy configs directly.')
        sys.exit(1)
    runtime.initialize(**CONFIG_RUNTIME_OPTS)
    config = mdc(runtime)
    config.update('mode', mode)
    if not message:
        message = 'Updating [mode] to "{}"'.format(mode)
    config.commit(message)

    if push:
        config.push()


@cli.command("config:print", short_help="View config for given images / rpms")
@click.option("-n", "--name-only", default=False, is_flag=True,
              help="Just print name of matched configs. Overrides --key")
@click.option("--key", help="Specific key in config to print", default=None)
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@pass_runtime
def config_print(runtime, key, name_only, as_yaml):
    """Print name, sub-key, or entire config

    Filtering of configs is based on usage of the following global options:
    --group, --images/-i, --rpms/-r

    See `doozer --help` for more.

    Examples:

    Print all configs in group:

        $ doozer --group=openshift-4.0 config:print

    Print single config in group:

        $ doozer --group=openshift-4.0 -i aos3-installation config:print

    Print `owners` key from all configs in group:

        $ doozer --group=openshift-4.0 config:print --key owners

    Print only names of configs in group:

        $ doozer --group=openshift-4.0 config:print --name-only
    """
    _fix_runtime_mode(runtime)
    opts = dict(CONFIG_RUNTIME_OPTS)
    opts['config_only'] = False  # This verb must load image & rpm data
    runtime.initialize(**opts)
    config = mdc(runtime)
    config.config_print(key, name_only, as_yaml)


@cli.command("config:gen-csv", short_help="Generate .csv file for given images/rpms")
@click.option("--keys", help="Specific key in config to print, separated by commas: --keys key,name,owners",
              default=None)
@click.option("--type", "as_type", default=None, help='Write content type: image or rpm')
@click.option("--output", "-o", default=None, help='Write csv data to FILE instead of STDOUT')
@pass_runtime
def config_gencsv(runtime, keys, as_type, output):
    """Generate .csv file for given --keys and --type

    By default print out with STDOUT, you can use --output/-o to specify an output file

    Filtering of configs is based on usage of the following global options:
    --group, --images/-i, --rpms/-r

    See `doozer --help` for more.

    Examples:

    Generate a CSV where each row is distgit key, image name, and owners list:

        $ doozer --group=openshift-4.0 config:gen-csv --type image --keys key,name,owners

    Generate a CSV only include image aos3-installation each row is distgit key, image name, output to file ./image.csv:

        $ doozer --group=openshift-4.0 -i aos3-installation config:gen-csv --type image --keys key,name --output ./image.csv

    """
    _fix_runtime_mode(runtime)
    runtime.initialize(**CONFIG_RUNTIME_OPTS)
    config = mdc(runtime)
    config.config_gen_csv(keys, as_type, output)


@cli.command('config:rhcos-srpms')
@click.option("--version", metavar="RHCOS_VER", help="RHCOS version for which to collection SRPMS (e.g. 413.92.202303212039-0).", required=True)
@click.option("-o", "--output", metavar="DIR", help="Output directory to sync to", required=True)
@click.option("--brew-root", metavar="DIR", default='/mnt/redhat/brewroot', help="Brewroot directory from which to source RPMs.", required=True)
@click.option("-a", "--arch",
              metavar='ARCH',
              help="Arch for which the repo should be generated (if not specified, use all runtime arches).",
              default=None, required=False)
@pass_runtime
def config_rhcos_src(runtime: Runtime, version, output, brew_root, arch):
    runtime.initialize(clone_distgits=False)

    package_build_objects: Dict[str, Dict] = dict()
    if arch:
        arches = [arch]
    else:
        arches = runtime.arches

    for arch_entry in arches:
        runtime.logger.info(f'Pulling RHCOS package information for {version} and arch={arch_entry}')
        inspector = RHCOSBuildInspector(runtime, pullspec_for_tag={}, brew_arch=arch_entry, build_id=version)
        package_build_objects.update(inspector.get_package_build_objects())

    brew_root_path = pathlib.Path(brew_root)
    brew_packages_path = brew_root_path.joinpath('packages')

    if not brew_packages_path.is_dir():
        red_print(f'Brewroot packages must be a directory: {str(brew_packages_path)}')
        exit(1)

    output_path = pathlib.Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    for package_name, build_obj in package_build_objects.items():
        package_nvr = build_obj['nvr']

        src_dir_path = brew_packages_path.joinpath(package_name, build_obj['version'], build_obj['release'], 'src')
        out_base_dir_path = output_path.joinpath(package_name, build_obj['version'], build_obj['release'])
        out_base_dir_path.mkdir(parents=True, exist_ok=True)
        out_src_dir = out_base_dir_path.joinpath('src')
        if not src_dir_path.exists():
            runtime.logger.warning(f'Failed to find RPM brewroot directory {str(src_dir_path.absolute())}')
            continue

        if out_src_dir.exists():
            if out_src_dir.is_symlink():
                runtime.logger.info(f'Output directory already contains a symlink for {package_nvr}. Skipping.')
                continue
            else:
                red_print(f'File already exists; cannot replace with brewroot content: {str(out_src_dir)}')
                exit(1)

        out_src_dir.symlink_to(str(src_dir_path.absolute()))
        runtime.logger.info(f'Populated {str(out_src_dir)}')


@cli.command("config:update-required", short_help="Update images that are required")
@click.option("--image-list", help="File with list of images, one per line.", required=True)
@pass_runtime
def config_update_required(runtime, image_list):
    """Ingest list of images and update data repo
    with which images are required and which are not.
    """
    _fix_runtime_mode(runtime)
    runtime.initialize(**CONFIG_RUNTIME_OPTS)

    with io.open(image_list, 'r', encoding="utf-8") as il:
        image_list = [i.strip() for i in il.readlines() if i.strip()]

    resolved = []
    required = []
    optional = []
    for img in runtime.image_metas():
        name = img.image_name
        slash = img.image_name.find('/')
        if slash >= 0:
            name = name[slash + 1:]
        found = False
        for i in image_list:
            if i == name or i == name.replace('ose-', ''):
                required.append(img)
                resolved.append(i)
                found = True
                green_print('{} -> {}'.format(img.distgit_key, i))
                break
        if not found:
            optional.append(img)

    missing = list(set(image_list) - set(resolved))
    if missing:
        yellow_print('\nThe following images in the data set could not be resolved:')
        yellow_print('\n'.join(missing))

    for img in required:
        msg = 'Updating {} to be required'.format(img.distgit_key)
        color_print(msg, color='blue')

        data_obj = runtime.gitdata.load_data(path='images', key=img.distgit_key)
        data_obj.data['required'] = True
        data_obj.save()

    for img in optional:
        msg = 'Updating {} to be optional'.format(img.distgit_key)
        color_print(msg, color='blue')

        data_obj = runtime.gitdata.load_data(path='images', key=img.distgit_key)
        data_obj.data.pop('required', None)
        data_obj.save()

    green_print('\nComplete! Remember to commit and push the changes!')
