import re
import requests
from typing import Optional, Tuple
from datetime import datetime
from artcommonlib.constants import RELEASE_SCHEDULES


def split_el_suffix_in_release(release: str) -> Tuple[str, Optional[str]]:
    """
    Given a release field, this will method will split out any
    .el### or +el### suffix and return (prefix, el_suffix) where el_suffix
    is None if there .el### is not detected.
    """

    el_suffix_match = re.match(r'(.*)[.+](el\d+)(?:.*|$)', release)
    if el_suffix_match:
        prefix = el_suffix_match.group(1)
        el_suffix = el_suffix_match.group(2)
        return prefix, el_suffix
    else:
        return release, None


def isolate_assembly_in_release(release: str) -> Optional[str]:
    """
    Given a release field, determines whether is contains
    an assembly name. If it does, it returns the assembly
    name. If it is not found, None is returned.
    """
    assembly_prefix = '.assembly.'
    asm_pos = release.rfind(assembly_prefix)
    if asm_pos == -1:
        return None

    # Our rpm release fields will usually have ".el?" after ".assembly.name"
    # But some of our base images can have ".el?" before ".assembly.name"
    # If ".el?" appears after assembly name, then strip it off
    el_pos = release.rfind('.el')
    if el_pos > asm_pos:
        release, _ = split_el_suffix_in_release(release)

    return release[asm_pos + len(assembly_prefix):]


def isolate_el_version_in_release(release: str) -> Optional[int]:
    """
    Given a release field, determines whether is contains
    a RHEL version. If it does, it returns the version value as an int.
    If it is not found, None is returned.
    """
    _, el_suffix = split_el_suffix_in_release(release)
    if el_suffix:
        return int(el_suffix[2:])
    return None


def get_feature_freeze_release_date(major, minor):
    """
    Get feature freeze release release date from release schedule API
    """
    release_date = None
    release_schedules = requests.get(
        f'{RELEASE_SCHEDULES}/openshift-{major}.{minor}/schedule-tasks/?name=Feature%20Development%20for%20{major}.{minor}',
        headers={'Accept': 'application/json'}
    )
    if release_schedules.status_code == 200:
        release_date = datetime.strptime(release_schedules.json()[0]['date_finish'], "%Y-%m-%d")
    return release_date


def get_ga_release_date(major, minor):
    """
    Get ga release release date from release schedule API
    """
    release_date = None
    release_schedules = requests.get(
        f'{RELEASE_SCHEDULES}/openshift-{major}.{minor}/schedule-tasks/?name=OpenShift%20Container%20Platform%20GA%20Release%20Schedule',
        headers={'Accept': 'application/json'}
    )
    if release_schedules.status_code == 200:
        release_date = datetime.strptime(release_schedules.json()[0]['date_finish'], "%Y-%m-%d")
    return release_date


def get_assembly_release_date(assembly, group):
    """
    Get assembly release release date from release schedule API
    """
    assembly_release_date = None
    release_schedules = requests.get(
        f'{RELEASE_SCHEDULES}/{group}.z/?fields=all_ga_tasks',
        headers={'Accept': 'application/json'}
    )
    for release in release_schedules.json()['all_ga_tasks']:
        if assembly in release['name']:
            # convert date format for advisory usage, 2024-02-13 -> 2024-Feb-13
            assembly_release_date = datetime.strptime(release['date_start'], "%Y-%m-%d").strftime("%Y-%b-%d")
            break
    return assembly_release_date


def get_inflight(assembly, group):
    """
    Get inflight release name from current assembly release
    """
    inflight_release = None
    assembly_release_date = get_assembly_release_date(assembly, group)
    if not assembly_release_date:
        raise ValueError(f'Assembly release date not found for {assembly}')
    major, minor = get_ocp_version_from_group(group)
    release_schedules = requests.get(
        f'{RELEASE_SCHEDULES}/openshift-{major}.{minor-1}.z/?fields=all_ga_tasks',
        headers={'Accept': 'application/json'}
    )
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
