import copy
import functools
import json
import os
import pathlib
import re
import urllib.parse
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from inspect import getframeinfo, stack
from itertools import chain
from os.path import abspath
from pathlib import Path
from sys import getsizeof, stderr
from typing import Dict, List, Optional, Tuple, Union

import semver
import yaml

import artcommonlib
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch, GO_ARCHES
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.format_util import red_print
from artcommonlib.model import Model, Missing
from artcommonlib.util import isolate_major_minor_in_group

try:
    from reprlib import repr
except ImportError:
    pass

from doozerlib import constants
from functools import lru_cache

DICT_EMPTY = object()


def dict_get(dct, path, default=DICT_EMPTY):
    dct = copy.deepcopy(dct)  # copy to not modify original
    for key in path.split('.'):
        try:
            dct = dct[key]
        except KeyError:
            if default is DICT_EMPTY:
                raise Exception('Unable to follow key path {}'.format(path))
            return default
    return dct


def setup_and_fetch_public_upstream_source(public_source_url: str, public_upstream_branch: str, source_dir: str):
    """
    Fetch public upstream source for specified Git repository. Set up public_upstream remote if needed.

    :param public_source_url: HTTPS Git URL of the public upstream source
    :param public_upstream_branch: Git branch of the public upstream source
    :param source_dir: Path to the local Git repository
    """
    out, err = exectools.cmd_assert(["git", "-C", source_dir, "remote"])
    if 'public_upstream' not in out.strip().split():
        exectools.cmd_assert(["git", "-C", source_dir, "remote", "add", "--", "public_upstream", public_source_url])
    else:
        exectools.cmd_assert(["git", "-C", source_dir, "remote", "set-url", "--", "public_upstream", public_source_url])
    exectools.cmd_assert(["git", "-C", source_dir, "fetch", "--", "public_upstream", public_upstream_branch], retries=3,
                         set_env=constants.GIT_NO_PROMPTS)


def is_commit_in_public_upstream(revision: str, public_upstream_branch: str, source_dir: str):
    """
    Determine if the public upstream branch includes the specified commit.

    :param revision: Git commit hash or reference
    :param public_upstream_branch: Git branch of the public upstream source
    :param source_dir: Path to the local Git repository
    """
    cmd = ["git", "merge-base", "--is-ancestor", "--", revision, "public_upstream/" + public_upstream_branch]
    # The command exits with status 0 if true, or with status 1 if not. Errors are signaled by a non-zero status that is not 1.
    # https://git-scm.com/docs/git-merge-base#Documentation/git-merge-base.txt---is-ancestor
    rc, out, err = exectools.cmd_gather(cmd)
    if rc == 0:
        return True
    if rc == 1:
        return False
    raise IOError(
        f"Couldn't determine if the commit {revision} is in the public upstream source repo. `git merge-base` exited with {rc}, stdout={out}, stderr={err}")


def is_in_directory(path: os.PathLike, directory: os.PathLike):
    """check whether a path is in another directory
    """
    a = Path(path).parent.resolve()
    b = Path(directory).resolve()
    try:
        a.relative_to(b)
        return True
    except ValueError:
        return False


def mkdirs(path, mode=0o755):
    """
    Make sure a directory exists. Similar to shell command `mkdir -p`.
    :param path: Str path
    :param mode: create directories with mode
    """
    pathlib.Path(str(path)).mkdir(mode=mode, parents=True, exist_ok=True)


def analyze_debug_timing(file):
    peal = re.compile(r'^(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d),\d\d\d \w+ [(](\d+)[)] (.*)')

    thread_names = {}
    event_timings = {}  # maps internal to { <thread_name> => [event list] }
    first_interval = None

    def get_thread_name(thread):
        if thread in thread_names:
            return thread_names[thread]
        c = f'T{len(thread_names)}'
        thread_names[thread] = c
        return c

    def get_interval_map(interval):
        nonlocal first_interval
        if first_interval is None:
            first_interval = interval
        interval = interval - first_interval
        if interval in event_timings:
            return event_timings[interval]
        mapping = {}
        event_timings[interval] = mapping
        return mapping

    def get_thread_event_list(interval, thread):
        thread_name = get_thread_name(thread)
        interval_map = get_interval_map(interval)
        if thread_name in interval_map:
            return interval_map[thread_name]
        event_list = []
        interval_map[thread_name] = event_list
        return event_list

    def add_thread_event(interval, thread, event):
        get_thread_event_list(int(interval), thread).append(event)

    with open(file, 'r') as f:
        for line in f:
            m = peal.match(line.strip())
            if m:
                thread = m.group(2)  # thread id (e.g. 139770552305472)
                datestr = m.group(1)  # 2020-04-09 10:17:03,092
                event = m.group(3)
                date_time_obj = datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S')
                minute_mark = int(int(date_time_obj.strftime("%s")) / 10)  # ten second intervals
                add_thread_event(minute_mark, thread, event)

    def print_em(*args):
        for a in args:
            print(str(a).ljust(5), end="")
        print('')

    print('Thread timelines')
    names = sorted(list(thread_names.values()), key=lambda e: int(e[1:]))  # sorts as T1, T2, T3, .... by removing 'T'
    print_em('*', *names)

    sorted_intervals = sorted(list(event_timings.keys()))
    for interval in range(0, sorted_intervals[-1] + 1):
        print_em(interval, *names)
        if interval in event_timings:
            interval_map = event_timings[interval]
            for i, thread_name in enumerate(names):
                events = interval_map.get(thread_name, [])
                for event in events:
                    with_event = list(names)
                    with_event[i] = thread_name + ': ' + event
                    print_em(f' {interval}', *with_event[:i + 1])


def what_is_in_master() -> str:
    """
    :return: Returns a string like "4.6" to identify which release currently resides in master branch.
    """
    # The promotion target of the openshift/images master branch defines this release master is associated with.
    ci_config_url = 'https://raw.githubusercontent.com/openshift/release/master/ci-operator/config/openshift/images/openshift-images-master.yaml'
    content = exectools.urlopen_assert(ci_config_url).read()
    ci_config = yaml.safe_load(content)
    # Look for something like: https://github.com/openshift/release/blob/251cb12e913dcde7be7a2b36a211650ed91c45c4/ci-operator/config/openshift/images/openshift-images-master.yaml#L64
    promotion_to = ci_config.get('promotion', {}).get('to', [])
    if promotion_to:
        target_release = promotion_to[0].get('name', None)
    else:
        target_release = None
    if not target_release:
        red_print(content)
        raise IOError('Unable to find which openshift release resides in master')
    return target_release


def extract_version_fields(version, at_least=0):
    """
    For a specified version, return a list with major, minor, patch.. isolated
    as integers.
    :param version: A version to parse
    :param at_least: The minimum number of fields to find (else raise an error)
    """
    fields = [int(f) for f in version.strip().split('-')[0].lstrip('v').split('.')]  # v1.17.1 => [ '1', '17', '1' ]
    if len(fields) < at_least:
        raise IOError(f'Unable to find required {at_least} fields in {version}')
    return fields


def get_cincinnati_channels(major, minor):
    """
    :param major: Major for release
    :param minor: Minor version for release.
    :return: Returns the Cincinnati graph channels associated with a release
             in promotion order (e.g. candidate -> stable)
    """
    major = int(major)
    minor = int(minor)

    if major != 4:
        raise IOError('Unable to derive previous for non v4 major')

    prefixes = ['candidate', 'fast', 'stable']
    if major == 4 and minor == 1:
        prefixes = ['prerelease', 'stable']

    return [f'{prefix}-{major}.{minor}' for prefix in prefixes]


def get_docker_config_json(config_dir):
    flist = os.listdir(abspath(config_dir))
    if 'config.json' in flist:
        return abspath(os.path.join(config_dir, 'config.json'))
    else:
        raise FileNotFoundError("Can not find the registry config file in {}".format(config_dir))


def isolate_git_commit_in_release(release: str) -> Optional[str]:
    """
    Given a release field, determines whether is contains
    .git.<commit> information or .g<commit> (new style). If it does, it returns the value
    of <commit>. If it is not found, None is returned.
    """
    match = re.match(r'.*\.git\.([a-f0-9]+)(?:\.+|$)', release)
    if match:
        return match.group(1)

    match = re.match(r'.*\.g([a-f0-9]+)(?:\.+|$)', release)
    if match:
        return match.group(1)

    return None


def isolate_pflag_in_release(release: str) -> Optional[str]:
    """
    Given a release field, determines whether is contains
    .p0/.p1 information. If it does, it returns the value
    'p0' or 'p1'. If it is not found, None is returned.
    """
    match = re.match(r'.*\.(p[?01])(?:\.+|$)', release)

    if match:
        return match.group(1)

    return None


def isolate_nightly_name_components(nightly_name: str) -> (str, str, bool):
    """
    Given a release name (e.g. 4.8.0-0.nightly-s390x-2021-07-02-143555, 4.1.0-0.nightly-priv-2019-11-08-213727),
    return:
     - The major.minor of the release (e.g. 4.8)
     - The brew CPU architecture name associated with the nightly (e.g. s390x, x86_64)
     - Whether the release is from a private release controller.
    :param nightly_name: The name of the nightly to analyze
    :return: (major_minor, brew_arch, is_private)
    """
    major_minor = '.'.join(nightly_name.split('.')[:2])
    nightly_name = nightly_name[nightly_name.find('.nightly') + 1:]  # strip off versioning info (e.g.  4.8.0-0.)
    components = nightly_name.split('-')
    is_private = ('priv' in components)
    pos = components.index('nightly')
    possible_arch = components[pos + 1]
    if possible_arch not in GO_ARCHES:
        go_arch = 'x86_64'  # for historical reasons, amd64 is not included in the release name
    else:
        go_arch = possible_arch
    brew_arch = brew_arch_for_go_arch(go_arch)
    return major_minor, brew_arch, is_private


def isolate_el_version_in_brew_tag(tag: Union[str, int]) -> Optional[int]:
    """
    Given a brew tag (target) name, determines whether it contains
    a RHEL version. If it does, it returns the version value.
    If it is not found, None is returned. If an int is passed in,
    the int is just returned.
    """
    if isinstance(tag, int):
        # If this is already an int, just use it.
        return tag
    else:
        try:
            return int(str(tag))  # int as a str?
        except ValueError:
            pass
    el_version_match = re.search(r"rhel-(\d+)", tag)
    return int(el_version_match[1]) if el_version_match else None


# https://code.activestate.com/recipes/577504/
def total_size(o, handlers=None, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    if handlers is None:
        handlers = dict()

    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: dict_handler,
        set: iter,
        frozenset: iter,
    }
    all_handlers.update(handlers)  # user handlers take precedence
    seen = set()  # track which object id's have already been seen
    default_size = getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:  # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def to_nvre(build_record: Dict):
    """
    From a build record object (such as an entry returned by listTagged),
    returns the full nvre in the form n-v-r:E.
    """
    nvr = build_record['nvr']
    if 'epoch' in build_record and build_record["epoch"] and build_record["epoch"] != 'None':
        return f'{nvr}:{build_record["epoch"]}'
    return nvr


def strip_epoch(nvr: str):
    """
    If an NVR string is N-V-R:E, returns only the NVR portion. Otherwise
    returns NVR exactly as-is.
    """
    return nvr.split(':')[0]


def isolate_timestamp_in_release(release: str) -> Optional[str]:
    """
    Given a release field, determines whether is contains
    a timestamp. If it does, it returns the timestamp.
    If it is not found, None is returned.
    """
    match = re.search(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})", release)  # yyyyMMddHHmm
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5))
        if year >= 2000 and month >= 1 and month <= 12 and day >= 1 and day <= 31 and hour <= 23 and minute <= 59:
            return match.group(0)
    return None


def get_release_tag_datetime(release: str) -> Optional[str]:
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})-(\d{6})", release)  # yyyy-MM-dd-HHmmss
    if match:
        return datetime.strptime(match.group(0), "%Y-%m-%d-%H%M%S")
    return None


def sort_semver(versions):
    return sorted(versions, key=functools.cmp_to_key(semver.compare), reverse=True)


def get_channel_versions(channel, arch,
                         graph_url='https://api.openshift.com/api/upgrades_info/v1/graph',
                         graph_content_stable=None,
                         graph_content_candidate=None):
    """
    Queries Cincinnati and returns a tuple containing:
    1. All of the versions in the specified channel in decending order (e.g. 4.6.26, ... ,4.6.1)
    2. A map of the edges associated with each version (e.g. map['4.6.1'] -> [ '4.6.2', '4.6.3', ... ]
    :param channel: The name of the channel to inspect
    :param arch: Arch for the channel
    :param graph_url: Cincinnati graph URL to query
    :param graph_content_candidate: Override content from candidate channel - primarily for testing
    :param graph_content_stable: Override content from stable channel - primarily for testing
    :return: (versions, edge_map)
    """
    content = None
    if (channel == 'stable') and graph_content_stable:
        # permit override
        with open(graph_content_stable, 'r') as f:
            content = f.read()

    if (channel != 'stable') and graph_content_candidate:
        # permit override
        with open(graph_content_candidate, 'r') as f:
            content = f.read()

    if not content:
        url = f'{graph_url}?arch={arch}&channel={channel}'
        req = urllib.request.Request(url)
        req.add_header('Accept', 'application/json')
        content = exectools.urlopen_assert(req).read()

    graph = json.loads(content)
    versions = [node['version'] for node in graph['nodes']]
    descending_versions = sort_semver(versions)

    edges: Dict[str, List] = dict()
    for v in versions:
        # Ensure there is at least an empty list for all versions.
        edges[v] = []

    for edge_def in graph['edges']:
        # edge_def example [22, 20] where is number is an offset into versions
        from_ver = versions[edge_def[0]]
        to_ver = versions[edge_def[1]]
        edges[from_ver].append(to_ver)

    return descending_versions, edges


def get_build_suggestions(major, minor, arch,
                          suggestions_url='https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/'):
    """
    Loads suggestions_url/major.minor.yaml and returns minor_min, minor_max,
    minor_block_list, z_min, z_max, and z_block_list
    :param suggestions_url: Base url to /{major}.{minor}.yaml
    :param major: Major version
    :param minor: Minor version
    :param arch: Architecture to lookup
    :return: {minor_min, minor_max, minor_block_list, z_min, z_max, z_block_list}
    """
    url = f'{suggestions_url}/{major}.{minor}.yaml'
    req = urllib.request.Request(url)
    req.add_header('Accept', 'application/yaml')
    suggestions = yaml.safe_load(exectools.urlopen_assert(req))
    if arch in suggestions:
        return suggestions[arch]
    else:
        return suggestions['default']


def get_release_calc_previous(version, arch,
                              graph_url='https://api.openshift.com/api/upgrades_info/v1/graph',
                              graph_content_stable=None,
                              graph_content_candidate=None,
                              suggestions_url='https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/'):
    major, minor = extract_version_fields(version, at_least=2)[:2]
    arch = go_arch_for_brew_arch(arch)  # Cincinnati is go code, and uses a different arch name than brew
    # Get the names of channels we need to analyze
    candidate_channel = get_cincinnati_channels(major, minor)[0]
    prev_candidate_channel = get_cincinnati_channels(major, minor - 1)[0]

    upgrade_from = set()
    prev_versions, prev_edges = get_channel_versions(prev_candidate_channel, arch, graph_url,
                                                     graph_content_stable, graph_content_candidate)
    curr_versions, current_edges = get_channel_versions(candidate_channel, arch, graph_url, graph_content_stable,
                                                        graph_content_candidate)
    suggestions = get_build_suggestions(major, minor, arch, suggestions_url)
    for v in prev_versions:
        if (semver.VersionInfo.parse(v) >= semver.VersionInfo.parse(suggestions['minor_min'])
                and semver.VersionInfo.parse(v) < semver.VersionInfo.parse(suggestions['minor_max'])
                and v not in suggestions['minor_block_list']):
            upgrade_from.add(v)
    for v in curr_versions:
        if (semver.VersionInfo.parse(v) >= semver.VersionInfo.parse(suggestions['z_min'])
                and semver.VersionInfo.parse(v) < semver.VersionInfo.parse(suggestions['z_max'])
                and v not in suggestions['z_block_list']):
            upgrade_from.add(v)

    candidate_channel_versions, candidate_edges = curr_versions, current_edges
    # 'nightly' was an older convention. This nightly variant check can be removed by Oct 2020.
    if 'nightly' not in version and 'hotfix' not in version:
        # If we are not calculating a previous list for standard release, we want edges from previously
        # released hotfixes to be valid for this node IF and only if that hotfix does not
        # have an edge to TWO previous standard releases.
        # ref: https://docs.google.com/document/d/16eGVikCYARd6nUUtAIHFRKXa7R_rU5Exc9jUPcQoG8A/edit

        # If a release name in candidate contains 'hotfix', it was promoted as a hotfix for a customer.
        previous_hotfixes = list(filter(lambda release: 'nightly' in release or 'hotfix' in release, candidate_channel_versions))
        # For each hotfix that doesn't have 2 outgoing edges, and it as an incoming edge to this release
        for hotfix_version in previous_hotfixes:
            if len(candidate_edges[hotfix_version]) < 2:
                upgrade_from.add(hotfix_version)

    return sort_semver(list(upgrade_from))


async def find_manifest_list_sha(pull_spec):
    image_data = oc_image_info__caching(pull_spec)
    if 'listDigest' not in image_data:
        raise ValueError('Specified image is not a manifest-list.')
    return image_data['listDigest']


def get_release_name(assembly_type: artcommonlib.assembly.AssemblyTypes, group_name: str, assembly_name: str,
                     release_offset: Optional[int]):
    major, minor = isolate_major_minor_in_group(group_name)
    if major is None or minor is None:
        raise ValueError(f"Invalid group name: {group_name}")
    if assembly_type == AssemblyTypes.CUSTOM:
        if release_offset is None:
            raise ValueError("release_offset is required for a CUSTOM release.")
        release_name = f"{major}.{minor}.{release_offset}-assembly.{assembly_name}"
    elif assembly_type in [AssemblyTypes.CANDIDATE, AssemblyTypes.PREVIEW]:
        if release_offset is not None:
            raise ValueError(f"release_offset can't be set for a {assembly_type.value} release.")
        release_name = f"{major}.{minor}.0-{assembly_name}"
    elif assembly_type == AssemblyTypes.STANDARD:
        if release_offset is not None:
            raise ValueError("release_offset can't be set for a STANDARD release.")
        release_name = f"{assembly_name}"
    else:
        raise ValueError(f"Assembly type {assembly_type} is not supported.")
    return release_name


def get_release_name_for_assembly(group_name: str, releases_config: Model, assembly_name: str):
    """ Get release name for an assembly.
    """
    assembly_type = artcommonlib.assembly.assembly_type(releases_config, assembly_name)
    patch_version = artcommonlib.assembly.assembly_basis(releases_config, assembly_name).get('patch_version')
    if assembly_type is AssemblyTypes.CUSTOM:
        patch_version = artcommonlib.assembly.assembly_basis(releases_config, assembly_name).get('patch_version')
        # If patch_version is not set, go through the chain of assembly inheritance and determine one
        current_assembly = assembly_name
        while patch_version is None:
            parent_assembly = releases_config.releases[current_assembly].assembly.basis.assembly
            if parent_assembly is Missing:
                break
            if artcommonlib.assembly.assembly_type(releases_config, parent_assembly) is AssemblyTypes.STANDARD:
                patch_version = int(parent_assembly.rsplit('.', 1)[-1])
                break
            current_assembly = parent_assembly
        if patch_version is None:
            raise ValueError("patch_version is not set in assembly definition and can't be auto-determined through the chain of inheritance.")
    return get_release_name(assembly_type, group_name, assembly_name, patch_version)


def oc_image_info(pull_spec: str, go_arch: str = 'amd64') -> Dict:
    """
    Returns a Dict of the parsed JSON output of `oc image info` for the specified
    pullspec. Use oc_image_info__caching if you do not believe the image will change
    during the course of doozer's execution.
    """
    # Filter by os because images can be multi-arch manifest lists (which cause oc image info to throw an error if not filtered).
    cmd = ['oc', 'image', 'info', f'--filter-by-os={go_arch}', '-o', 'json', pull_spec]
    out, _ = exectools.cmd_assert(cmd, retries=3)
    return json.loads(out)


@lru_cache(maxsize=1000)
def oc_image_info__caching(pull_spec: str, go_arch: str = 'amd64') -> Dict:
    """
    Returns a Dict of the parsed JSON output of `oc image info` for the specified
    pullspec. This function will cache that output per pullspec, so do not use it
    if you expect the image to change during the course of doozer's execution.
    """
    return oc_image_info(pull_spec, go_arch)
