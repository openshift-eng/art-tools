from functools import lru_cache
import logging
from typing import OrderedDict, Optional, Tuple, Iterable, List, Union, Dict
from datetime import datetime, timezone, timedelta, date
import re
import asyncio

import aiohttp
import requests
from pathlib import Path
from semver import VersionInfo
from tenacity import retry, wait_fixed, stop_after_attempt
from ruamel.yaml import YAML
from artcommonlib.constants import RELEASE_SCHEDULES
from artcommonlib.model import ListModel, Missing
from urllib.parse import quote

LOGGER = logging.getLogger(__name__)


def remove_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    else:
        return s[:]


def remove_prefixes(s: str, *args) -> str:
    for prefix in args:
        s = remove_prefix(s, prefix)
    return s


def remove_suffix(s: str, suffix: str) -> str:
    # suffix='' should not call self[:-0].
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    else:
        return s[:]


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


def new_roundtrip_yaml_handler():
    """
    Creates and returns a configured YAML handler with specific formatting settings.
    Returns:
        YAML: A YAML handler configured with:
            - round-trip (rt) mode for preserving comments and formatting
            - disabled flow style for better readability
            - preserved quotes
            - 4096 character line width
            - custom indentation (2 spaces for mappings, 4 for sequences)
    """
    yaml = YAML(typ="rt")
    yaml.default_flow_style = False
    yaml.preserve_quotes = True
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


@lru_cache(maxsize=512)
def convert_remote_git_to_https(source_url: str):
    """
    Accepts a source git URL in ssh or https format and return it in a normalized
    https format (:port on servers is not supported):
        - https protocol
        - no trailing /
    :param source_url: Git remote
    :return: Normalized https git URL
    """
    url = source_url.strip().rstrip('/')
    url = remove_prefixes(url, 'http://', 'https://', 'git://', 'git@', 'ssh://')
    url = remove_suffix(url, '.git')
    url = url.split('@', 1)[-1]  # Strip username@

    if url.find(':') > -1:
        server, org_repo = url.rsplit(':', 1)
    elif url.rfind('/') > -1:
        server, org_repo = url.rsplit('/', 1)
    else:
        return f'https://{url}'  # weird..

    return f'https://{server}/{org_repo}'


@lru_cache(maxsize=512)
def convert_remote_git_to_ssh(url):
    """
    Accepts a remote git URL and turns it into a git@
    ssh form.
    :param url: The initial URL
    :return: A url in git@server:repo.git
    """
    server, org, repo_name = split_git_url(url)
    return f'git@{server}:{org}/{repo_name}.git'


def split_git_url(url) -> (str, str, str):
    """
    :param url: A remote ssh or https github url
    :return: Splits a github url into the server name, org, and repo name
    """
    https_normalized = convert_remote_git_to_https(url)
    url = https_normalized[8:]  # strip https://
    server, repo = url.split('/', 1)  # e.g. 'github.com', 'openshift/origin'
    org, repo_name = repo.split('/', 1)
    return server, org, repo_name


@retry(reraise=True, wait=wait_fixed(10), stop=stop_after_attempt(3))
async def download_file_from_github(repository, branch, path, token: str, destination):
    server, org, repo_name = split_git_url(repository)
    url = f'https://raw.githubusercontent.com/{org}/{repo_name}/{branch}/{path}'
    headers = {"Authorization": f'token {token}'}

    LOGGER.info('Downloading %s...', url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            with open(str(destination), "wb") as f:
                f.write((await resp.text()).encode())


def merge_objects(a, b):
    """ Merges two, potentially deep, objects into a new one and returns the result.
    'a' is layered over 'b' and is dominant when necessary. The output is 'c'.
    """
    if not isinstance(a, dict) or not isinstance(b, dict):
        return a
    c: OrderedDict = b.copy()
    for k, v in a.items():
        c[k] = merge_objects(v, b.get(k))
        if k not in b:
            # move new entry to the beginning
            c.move_to_end(k, last=False)
    return c


def is_future_release_date(date_str):
    """
    If the input date is in future then return True elase False
    """
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    current_date = datetime.now(tz=timezone.utc)
    if target_date > current_date:
        return True
    else:
        return False


def get_assembly_release_date(assembly, group):
    """
    Get assembly release release date from release schedule API.

    :raises ValueError: If the assembly release date is not found
    """
    release_schedules = requests.get(f'{RELEASE_SCHEDULES}/{group}.z/?fields=all_ga_tasks', headers={'Accept': 'application/json'})
    try:
        for release in release_schedules.json()['all_ga_tasks']:
            if assembly in release['name']:
                # convert date format for advisory usage, 2024-02-13 -> 2024-Feb-13
                return datetime.strptime(release['date_start'], "%Y-%m-%d").strftime("%Y-%b-%d")
    except KeyError:
        pass
    raise ValueError(f'Assembly release date not found for {assembly}')


async def get_assembly_release_date_async(release_name: str):
    """
    Get assembly release release date from release schedule API.

    :raises ValueError: If the assembly release date is not found
    """
    version = VersionInfo.parse(release_name)
    release_train = f'openshift-{version.major}.{version.minor}.z'
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{RELEASE_SCHEDULES}/{release_train}/?fields=all_ga_tasks', headers={'Accept': 'application/json'}) as response:
            response.raise_for_status()
            data = await response.json()
            for release in data['all_ga_tasks']:
                if release_name in release['name']:
                    # convert date format for advisory usage, 2024-02-13 -> 2024-Feb-13
                    return datetime.strptime(release['date_start'], "%Y-%m-%d").strftime("%Y-%b-%d")
    raise ValueError(f'Assembly release date not found for {release_name}')


def is_release_next_week(group):
    """
    Check if there release of group need to release in the near week
    """
    release_schedules = requests.get(f'{RELEASE_SCHEDULES}/{group}.z/?fields=all_ga_tasks', headers={'Accept': 'application/json'})
    for release in release_schedules.json()['all_ga_tasks']:
        release_date = datetime.strptime(release['date_finish'], "%Y-%m-%d").date()
        if release_date > date.today() and release_date <= date.today() + timedelta(days=7):
            return True
    return False


def get_inflight(assembly, group):
    """
    Get inflight release name from current assembly release
    """
    inflight_release = None
    assembly_release_date = get_assembly_release_date(assembly, group)
    major, minor = get_ocp_version_from_group(group)
    release_schedules = requests.get(f'{RELEASE_SCHEDULES}/openshift-{major}.{minor-1}.z/?fields=all_ga_tasks', headers={'Accept': 'application/json'})
    for release in release_schedules.json()['all_ga_tasks']:
        is_future = is_future_release_date(release['date_start'])
        if is_future:
            days_diff = abs((datetime.strptime(assembly_release_date, "%Y-%b-%d") - datetime.strptime(release['date_start'], "%Y-%m-%d")).days)
            if days_diff <= 5:  # if next Y-1 release and assembly release in the same week
                match = re.search(r'\d+\.\d+\.\d+', release['name'])
                if match:
                    inflight_release = match.group()
                    break
                else:
                    raise ValueError(f"Didn't find in_inflight release in {release['name']}")
    return inflight_release


def isolate_rhel_major_from_version(version: str) -> Optional[int]:
    """
    E.g. '9.2' => 9
    """

    match = re.fullmatch(r"(\d+).(\d+)", version)
    if match:
        return int(match[1])
    return None


def isolate_rhel_major_from_distgit_branch(branch: str) -> Optional[int]:
    """
    E.g. 'rhaos-4.16-rhel-9' => 9
    """

    match = re.fullmatch(r"^rhaos-\d+\.\d+-rhel-(\d+)", branch)
    if match:
        return int(match[1])
    return None


def get_ocp_version_from_group(group):
    """
    Extract ocp version from group value openshift-4.15 --> 4, 15
    """
    match = re.fullmatch(r"openshift-(\d+).(\d+)", group)
    if not match:
        raise ValueError(f"Invalid group name: {group}")
    return int(match[1]), int(match[2])


def deep_merge(dict1, dict2):
    """
    Recursively merge two dictionaries.

    Returns:
    A new dictionary with merged values.
    """

    merged = dict1.copy()

    for key, value in dict2.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            # If both values are dictionaries, merge them recursively
            merged[key] = deep_merge(merged[key], value)
        else:
            # Otherwise, simply update the value
            merged[key] = value

    return merged


def isolate_major_minor_in_group(group_name: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Given a group name, determines whether it contains an OCP {major}.{minor} version.
    If it does, it returns the version value as (int, int).
    If it is not found, (None, None) is returned.
    """

    match = re.fullmatch(r"openshift-(\d+).(\d+)", group_name)
    if not match:
        return None, None
    return int(match[1]), int(match[2])


async def run_limited_unordered(func, args: Iterable, limit: int) -> List:
    """
    limit the concurrency of asyncio tasks - adapted from https://death.andgravity.com/limit-concurrency
    :param func: async function to run against the args
    :param args: collection of args to be run (each arg is a list of parameters to func)
    :param limit: max number of tasks to run concurrently
    :return: an iterator of the task results (not necessarily in the order of args given)

    example usage:
      async def foo(bar, baz):
        await asyncio.sleep(1)
        return bar + baz

      print(await run_limited_unordered(foo, {(1, 2), (3, 4), ...}, limit=2))
      -> [3, 7, ...]  # (after a wait of len(args)/2)
    """
    return [it async for it in run_limited_generator(func, args, limit)]


async def run_limited_generator(func, args: Iterable, limit: int) -> Iterable:
    tasks = map(lambda params: func(*params), args)
    async for task in _limit_concurrency(tasks, limit):
        yield await task


async def _limit_concurrency(tasks: List, limit: int):
    tasks = iter(tasks)
    complete = False
    pending = set()

    while pending or not complete:
        while len(pending) < limit and not complete:
            try:
                task = next(tasks)
            except StopIteration:
                complete = True
            else:
                pending.add(asyncio.ensure_future(task))

        if not pending:
            return

        done, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )
        while done:
            yield done.pop()


class KubeCondition:
    def __init__(self, condition_obj: Dict):
        self.type = condition_obj.get('type')
        self.message = condition_obj.get('message')
        self.reason = condition_obj.get('reason')
        self.status = condition_obj.get('status')
        self.last_transition_time = None
        if condition_obj.get('lastTransitionTime'):
            self.last_transition_time = datetime.fromisoformat(condition_obj.get('lastTransitionTime').rstrip("Z"))

    def is_status_true(self) -> bool:
        return str(self.status).lower() == 'true'

    def is_status_false(self) -> bool:
        return str(self.status).lower() == 'false'

    @staticmethod
    def find_condition(obj, condition_type: str, _default: Optional["KubeCondition"] = None) -> "KubeCondition":
        """
        Searches a kube object's status.conditions for a specified condition type. Returns the
        condition entry if found. Otherwise, returns _default value.
        """
        try:
            for condition in obj.get('status', {}).get('conditions', []):
                if condition['type'] == condition_type:
                    return KubeCondition(condition)
        except AttributeError:
            pass
        return _default


def is_cachito_enabled(metadata, group_config, logger):
    """
    Cachito will be configured if `cachito.enabled` is True in image metadata or `cachito.enabled` is True in group config.
    https://osbs.readthedocs.io/en/latest/users.html#remote-sources
    """
    cachito_enabled = False
    if metadata.config.cachito.enabled:
        cachito_enabled = True
        logger.info("cachito/cachi2 enabled from metadata config")
    elif metadata.config.cachito.enabled is Missing:
        if group_config.cachito.enabled:
            cachito_enabled = True
            logger.info("cachito/cachi2 enabled from group config")
        elif isinstance(metadata.config.content.source.pkg_managers, ListModel):
            logger.warning(
                f"pkg_managers directive for {metadata.name} has no effect since cachito/cachi2 is not enabled in "
                "image metadata or group config.")
    if cachito_enabled and not metadata.has_source():
        logger.warning("Cachito integration for distgit-only image %s is not supported.", metadata.name)
        cachito_enabled = False
    return cachito_enabled


def detect_package_managers(metadata, dest_dir: Path):
    """ Detect and return package managers used by the source
    :return: a list of package managers
    """
    if not dest_dir or not dest_dir.is_dir():
        raise FileNotFoundError(f"Distgit directory for image {metadata.distgit_key} hasn't been cloned.")
    pkg_manager_files = {
        "gomod": ["go.mod"],
        "npm": ["npm-shrinkwrap.json", "package-lock.json"],
        "pip": ["requirements.txt", "requirements-build.txt"],
        "yarn": ["yarn.lock"],
    }
    pkg_managers: List[str] = []
    for pkg_manager, files in pkg_manager_files.items():
        if any(dest_dir.joinpath(file).is_file() for file in files):
            pkg_managers.append(pkg_manager)
    return pkg_managers
