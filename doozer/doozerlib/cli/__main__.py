# -*- coding: utf-8 -*-
import os
import shutil
import sys
import tempfile

import click
import yaml
from artcommonlib import exectools
from artcommonlib.format_util import color_print, green_print, red_print, yellow_print
from artcommonlib.pushd import Dir
from future import standard_library

from doozerlib import cli as cli_package
from doozerlib import state
from doozerlib.cli import cli, pass_runtime
from doozerlib.cli.config import (
    config_commit,
    config_gencsv,
    config_get,
    config_mode,
    config_print,
    config_push,
    config_read_assemblies,
    config_read_group,
    config_read_releases,
    config_rhcos_src,
    config_update_required,
)
from doozerlib.cli.config_plashet import config_plashet
from doozerlib.cli.config_tag_rpms import config_tag_rpms
from doozerlib.cli.detect_embargo import detect_embargo
from doozerlib.cli.fbc import fbc_import, fbc_rebase_and_build
from doozerlib.cli.get_nightlies import get_nightlies
from doozerlib.cli.images import (
    distgit_config_template,
    images_build_image,
    images_clone,
    images_covscan,
    images_foreach,
    images_list,
    images_merge,
    images_print,
    images_pull_image,
    images_push,
    images_push_distgit,
    images_rebase,
    images_revert,
    images_show_ancestors,
    images_show_tree,
    query_rpm_version,
)
from doozerlib.cli.images_health import images_health
from doozerlib.cli.images_konflux import images_konflux_build, images_konflux_rebase
from doozerlib.cli.images_okd import images_okd, images_okd_prs
from doozerlib.cli.images_streams import images_streams, images_streams_gen_buildconfigs, images_streams_mirror
from doozerlib.cli.inspect_stream import inspect_stream
from doozerlib.cli.olm_bundle import list_olm_operators, olm_bundles_print, rebase_and_build_olm_bundle
from doozerlib.cli.release_calc_upgrade_tests import release_calc_upgrade_tests
from doozerlib.cli.release_gen_assembly import gen_assembly_from_releases, releases_gen_assembly
from doozerlib.cli.release_gen_payload import release_gen_payload
from doozerlib.cli.rpms import (
    rpms_build,
    rpms_clone,
    rpms_clone_sources,
    rpms_print,
    rpms_rebase,
    rpms_rebase_and_build,
)
from doozerlib.cli.rpms_read_config import config_read_rpms
from doozerlib.cli.scan_fips import scan_fips
from doozerlib.cli.scan_osh import scan_osh
from doozerlib.cli.scan_sources import config_scan_source_changes
from doozerlib.cli.scan_sources_konflux import config_scan_source_changes_konflux
from doozerlib.exceptions import DoozerFatalError
from doozerlib.util import analyze_debug_timing, get_release_calc_previous

standard_library.install_aliases()

# =============================================================================
#
# CLI Commands
#
# =============================================================================


@cli.command("db:query", short_help="Query database records")
@click.option("-p", "--operation", required=True, metavar='NAME', help="Which operation to query (e.g. 'build')")
@click.option("-a", "--attribute", default=[], metavar='NAME', multiple=True, help="Attribute name to output")
@click.option("-m", "--match", default=[], metavar='NAME=VALUE', multiple=True, help="name=value matches (AND logic)")
@click.option(
    "-l", "--like", default=[], metavar='NAME=%VALUE%', multiple=True, help="name LIKE value matches (AND logic)"
)
@click.option(
    "-w",
    "--where",
    metavar='EXPR',
    default='',
    help="Complex expression instead of matching. e.g. ( `NAME`=\"VALUE\" and `NAME2` LIKE \"VALUE2%\" ) or NOT `NAME3` < \"VALUE3\"",
)
@click.option(
    "-s",
    "--sort-by",
    '--order-by',
    default=None,
    metavar='NAME',
    help="Attribute name to sort by. Note: The sort attribute must be present in at least one of the predicates of the expression.",
)
@click.option("--limit", default=0, metavar='NUM', help="Limit records returned to specified number")
@click.option("-o", "--output", metavar='format', default="human", help='Output format: human, csv, or yaml')
@pass_runtime
def db_select(runtime, operation, attribute, match, like, where, sort_by, limit, output):
    """
    Select records from the datastore.

    \b
    For simpledb UI in a browser:
    https://chrome.google.com/webstore/detail/sdbnavigator/ddhigekdfabonefhiildaiccafacphgg/related?hl=en-US

    \b
    Select syntax:
    https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/UsingSelect.html

    \b
    When using --sort-by:
    https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SortingDataSelect.html
    """

    runtime.initialize(clone_distgits=False, no_group=True)
    if not runtime.datastore:
        red_print('--datastore must be specified')
        exit(1)

    if not attribute:  # No attribute names identified? return everything.
        names = '*'
    else:
        names = ','.join([f'`{a}`' for a in attribute])

    where_str = ''
    if where:
        where_str = ' WHERE ' + f'{where}'
    elif match or like:
        where_str = ' WHERE '
        if match:
            quoted_match = ['`{}`="{}"'.format(*a_match.split('=')) for a_match in match]
            where_str += ' AND '.join(quoted_match)
        if like:
            quoted_like = ['`{}` LIKE "{}"'.format(*a_like.split('=')) for a_like in like]
            where_str += ' AND '.join(quoted_like)

    sort_by_str = ''
    if sort_by:
        # https://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SortingDataSelect.html
        sort_by_str = f' ORDER BY `{sort_by}` ASC'

    domain = f'`{runtime.datastore}_{operation}`'

    result = runtime.db.select(f'SELECT {names} FROM {domain}{where_str}{sort_by_str}', limit=int(limit))

    if output.lower() == 'yaml':
        print(yaml.safe_dump(result))

    elif output.lower == 'csv':
        include_name = 'name' in attribute
        for item in result:
            if include_name:
                print('Name', end=',')
            for a in item['Attributes']:
                print(a['Name'], end=',')
            print()

            if include_name:
                print(item['Name'], end=',')
            for a in item['Attributes']:
                print(a['Value'], end=',')
            print()
    else:
        # human
        for item in result:
            print(item['Name'] + ':')

            for a in item['Attributes']:
                print('\t' + a['Name'], end='=')
                print(a['Value'])
            print()


@cli.command("cleanup", short_help="Cleanup the Doozer environment")
@pass_runtime
def cleanup(runtime):
    """
    Cleanup the OIT working environment.
    Currently this just clears out the working dir content
    """

    runtime.initialize(no_group=True)

    runtime.logger.info('Clearing out {}'.format(runtime.working_dir))

    ignore_list = ['settings.yaml']
    with Dir(runtime.working_dir):
        for ent in os.listdir("."):
            if ent in ignore_list:
                continue

            yellow_print(ent)
            # Otherwise, remove
            if os.path.isfile(ent) or os.path.islink(ent):
                os.remove(ent)
            else:
                shutil.rmtree(ent)


@cli.command("release:calc-previous", short_help="Returns a list of releases to include in a release's previous field")
@click.option(
    "--version",
    metavar='NEW_VER',
    required=True,
    help="The release to calculate previous for (e.g. 4.5.4 or 4.6.0..hotfix)",
)
@click.option(
    "-a", "--arch", metavar='ARCH', help="Arch for which the repo should be generated", default='x86_64', required=False
)
@click.option(
    "--graph-url",
    metavar='GRAPH_URL',
    required=False,
    default='https://api.openshift.com/api/upgrades_info/v1/graph',
    help="Cincinnati graph URL to query",
)
@click.option(
    "--graph-content-stable",
    metavar='JSON_FILE',
    required=False,
    help="Override content from stable channel - primarily for testing",
)
@click.option(
    "--graph-content-candidate",
    metavar='JSON_FILE',
    required=False,
    help="Override content from candidate channel - primarily for testing",
)
@click.option(
    "--suggestions-url",
    metavar='SUGGESTIONS_URL',
    required=False,
    default="https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/",
    help="Suggestions URL, load from {major}-{minor}-{arch}.yaml",
)
def release_calc_previous(version, arch, graph_url, graph_content_stable, graph_content_candidate, suggestions_url):
    # Refer to https://docs.google.com/document/d/16eGVikCYARd6nUUtAIHFRKXa7R_rU5Exc9jUPcQoG8A/edit
    # for information on channels & edges
    results = get_release_calc_previous(
        version, arch, graph_url, graph_content_stable, graph_content_candidate, suggestions_url
    )
    print(','.join(results))


@cli.command("beta:reposync", short_help="Sync yum repos listed in group.yaml to local directory.")
@click.option("-o", "--output", metavar="DIR", help="Output directory to sync to", required=True)
@click.option("-c", "--cachedir", metavar="DIR", help="Cache directory for yum", required=True)
@click.option(
    "-a", "--arch", metavar='ARCH', help="Arch for which the repo should be generated", default='x86_64', required=False
)
@click.option(
    "--repo-type",
    metavar="REPO_TYPE",
    envvar="OIT_IMAGES_REPO_TYPE",
    default="unsigned",
    help="Repo group type to use for repo file generation (e.g. signed, unsigned).",
)
@click.option(
    "-n",
    "--name",
    "names",
    default=[],
    metavar='NAME',
    multiple=True,
    help="Only sync the specified repository names; if not specified all will be synced.",
)
@click.option(
    '--dry-run', default=False, is_flag=True, help='Print derived yum configuration for sync operation and exit'
)
@pass_runtime
def beta_reposync(runtime, output, cachedir, arch, repo_type, names, dry_run):
    """Sync yum repos listed in group.yaml to local directory.
    See `doozer --help` for more.

    Examples:

    Perform reposync operation for ppc64le architecture, preferring unsigned repos:

        $ doozer --group=openshift-4.0 beta:reposync -o /tmp/repo_sync -c /tmp/cache/ --repo-type unsigned --arch ppc64le

    Synchronize signed, x86_64, rpms from rhel-server-ose-rpms repository.

        $ doozer --group=openshift-4.0 beta:reposync -o /tmp/repo_sync -c /tmp/cache/ --repo-type signed -r rhel-server-ose-rpms

    """
    runtime.initialize(clone_distgits=False)
    repos = runtime.repos

    enabled_repos = []
    for name, cfg in repos.items():
        if cfg.is_reposync_enabled():
            enabled_repos.append(name)

    yum_conf = """
[main]
cachedir={}/$basearch/$releasever
keepcache=0
debuglevel=2
logfile={}/yum.log
obsoletes=1
gpgcheck=1
plugins=1
installonly_limit=3
""".format(cachedir, runtime.working_dir)

    optional_fails = []

    repos_content = repos.repo_file(repo_type, enabled_repos=enabled_repos, arch=arch)
    content = "{}\n\n{}".format(yum_conf, repos_content)

    print("repo config:\n", content)

    if not os.path.isdir(output):
        yellow_print('Creating outputdir: {}'.format(output))
        exectools.cmd_assert('mkdir -p {}'.format(output))

    if not os.path.isdir(cachedir):
        yellow_print('Creating cachedir: {}'.format(cachedir))
        exectools.cmd_assert('mkdir -p {}'.format(cachedir))

    metadata_dir = None
    yc_file = None
    try:
        # If corrupted, reposync metadata can interfere with subsequent runs.
        # Ensure we have a clean space for each invocation.
        metadata_dir = tempfile.mkdtemp(prefix='reposync-metadata.', dir=cachedir)
        yc_file = tempfile.NamedTemporaryFile()
        yc_file.write(content.encode('utf-8'))

        # must flush so it can be read
        yc_file.flush()

        exectools.cmd_assert('yum clean all')  # clean the cache first to avoid outdated repomd.xml files

        for repo in repos.values():
            # If specific names were specified, only synchronize them.
            if names and repo.name not in names:
                continue

            if not repo.is_reposync_enabled():
                runtime.logger.info('Skipping repo {} because reposync is disabled in group.yml'.format(repo.name))
                continue

            color_print('Syncing repo {}'.format(repo.name), 'blue')
            cmd = (
                'dnf '
                f'--config {yc_file.name} '
                f'--repoid {repo.name} '
                'reposync '
                f'--arch {arch} '
                '--arch noarch '
                '--delete '
                '--download-metadata '
                f'--download-path {output} '
            )
            if repo.is_reposync_latest_only():
                cmd += '--newest-only '

            if dry_run:
                cmd += '--urls '

            rc, out, err = exectools.cmd_gather(cmd, realtime=True)
            if rc != 0:
                if not repo.cs_optional:
                    raise DoozerFatalError(err)
                else:
                    runtime.logger.warning('Failed to sync repo {} but marked as optional: {}'.format(repo.name, err))
                    optional_fails.append(repo.name)
    finally:
        yc_file.close()
        shutil.rmtree(metadata_dir, ignore_errors=True)

    if optional_fails:
        yellow_print(
            'Completed with the following optional repos skipped or partial due to failure, see log.:\n{}'.format(
                '\n'.join(optional_fails)
            )
        )
    else:
        green_print('Repos synced to {}'.format(output))


@cli.command("analyze:debug-log", short_help="Output an analysis of the debug log")
@click.argument("debug_log", nargs=1)
def analyze_debug_log(debug_log):
    """
    Specify a doozer working directory debug.log as the argument. A table with be printed to stdout.
    The top row of the table will be all the threads that existed during the doozer runtime.
    T1, T2, T3, ... each represent an independent thread which was created and performed some action.

    The first column of the graph indicates a time interval. Each interval is 10 seconds. Actions
    performed by each thread are grouped into the interval in which those actions occurred. The
    action performed by a thread is aligned and printed under the thread's column.

    Example 1:
    *     T1    T2    T3
    0     loading metadata A                          # During the first 10 seconds, T1 is perform actions.
     0    loading metaddata B                         # Multiple events can happen during the same interval.
     0    loading metaddata C

    Example 2:
    *     T1    T2    T3
    0     loading metadata A                          # During the first 10 seconds, T1 is perform actions.
     0    loading metaddata B                         # Multiple events can happen during the same interval.
    1           cloning                               # During seconds 10-20, three threads start cloning.
     1    cloning
     1                cloning
    """
    f = os.path.abspath(debug_log)
    analyze_debug_timing(f)


def main():
    try:
        # pylint: disable=no-value-for-parameter
        cli(obj={})
    except DoozerFatalError as ex:
        # Allow capturing actual tool errors and print them
        # nicely instead of a gross stack-trace.
        # All internal errors that should simply cause the app
        # to exit with an error code should use DoozerFatalError
        red_print('\nDoozer Failed With Error:\n' + str(ex))

        if cli_package.CTX_GLOBAL and cli_package.CTX_GLOBAL.obj:
            cli_package.CTX_GLOBAL.obj.state['status'] = state.STATE_FAIL
            cli_package.CTX_GLOBAL.obj.state['msg'] = str(ex)
        sys.exit(1)
    finally:
        if cli_package.CTX_GLOBAL and cli_package.CTX_GLOBAL.obj and cli_package.CTX_GLOBAL.obj.initialized:
            cli_package.CTX_GLOBAL.obj.save_state()


if __name__ == '__main__':
    main()
