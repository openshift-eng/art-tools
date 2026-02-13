from typing import Dict, Iterable, List, Optional

from artcommonlib import logutil
from artcommonlib.release_util import isolate_assembly_in_release

LOGGER = logutil.get_logger(__name__)


def find_latest_build(builds: List[Dict], assembly: Optional[str]) -> Optional[Dict]:
    """Find the latest build specific to the assembly in a list of builds belonging to the same component and brew tag
    :param builds: a list of build dicts sorted by tagging event in descending order
    :param assembly: the name of assembly; None if assemblies support is disabled
    :return: a brew build dict or None
    """

    if not assembly:  # if assembly is not enabled, choose the true latest tagged
        chosen_build = builds[0] if builds else None
    else:  # assembly is enabled
        # find the newest build containing ".assembly.<assembly-name>" in its RELEASE field
        chosen_build = next(
            (build for build in builds if isolate_assembly_in_release(build["release"]) == assembly), None
        )
        if not chosen_build and assembly != "stream":
            # If no such build, fall back to the newest build containing ".assembly.stream"
            chosen_build = next(
                (build for build in builds if isolate_assembly_in_release(build["release"]) == "stream"), None
            )
        if not chosen_build:
            # If none of the builds have .assembly.stream in the RELEASE field,
            # fall back to the latest build without .assembly in the RELEASE field
            chosen_build = next(
                (build for build in builds if isolate_assembly_in_release(build["release"]) is None), None
            )
    return chosen_build


def find_latest_builds(brew_builds: Iterable[Dict], assembly: Optional[str]) -> Iterable[Dict]:
    """Find latest builds specific to the assembly in a list of brew builds.
    :param brew_builds: a list of build dicts sorted by tagging event in descending order
    :param assembly: the name of assembly; None if assemblies support is disabled
    :return: an iterator of latest brew build dicts
    """
    # group builds by component name
    grouped_builds = {}  # key is component_name, value is a list of Brew build dicts
    for build in brew_builds:
        grouped_builds.setdefault(build["name"], []).append(build)

    for builds in grouped_builds.values():  # builds are ordered from newest tagged to oldest tagged
        chosen_build = find_latest_build(builds, assembly)
        if chosen_build:
            yield chosen_build
