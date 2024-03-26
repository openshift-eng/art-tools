from typing import List, Optional, Iterable, Dict
from artcommonlib import logutil
from artcommonlib.model import Missing
from artcommonlib.release_util import isolate_assembly_in_release

LOGGER = logutil.get_logger(__name__)


def find_latest_build(builds: List[Dict], assembly: Optional[str]) -> Optional[Dict]:
    """ Find the latest build specific to the assembly in a list of builds belonging to the same component and brew tag
    :param builds: a list of build dicts sorted by tagging event in descending order
    :param assembly: the name of assembly; None if assemblies support is disabled
    :return: a brew build dict or None
    """

    if not assembly:  # if assembly is not enabled, choose the true latest tagged
        chosen_build = builds[0] if builds else None
    else:  # assembly is enabled
        # find the newest build containing ".assembly.<assembly-name>" in its RELEASE field
        chosen_build = next((build for build in builds if isolate_assembly_in_release(build["release"]) == assembly),
                            None)
        if not chosen_build and assembly != "stream":
            # If no such build, fall back to the newest build containing ".assembly.stream"
            chosen_build = next(
                (build for build in builds if isolate_assembly_in_release(build["release"]) == "stream"), None)
        if not chosen_build:
            # If none of the builds have .assembly.stream in the RELEASE field,
            # fall back to the latest build without .assembly in the RELEASE field
            chosen_build = next((build for build in builds if isolate_assembly_in_release(build["release"]) is None),
                                None)
    return chosen_build


def find_latest_builds(brew_builds: Iterable[Dict], assembly: Optional[str]) -> Iterable[Dict]:
    """ Find latest builds specific to the assembly in a list of brew builds.
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


def canonical_builders_enabled(canonical_builders_from_upstream, runtime) -> bool:
    """
    canonical_builders_from_upstream can be set globally, or overridden by single components; this function will take
    either a global or a local flag, and check if canonical builders apply within this context
    """

    if canonical_builders_from_upstream is Missing:
        # Default case: override using ART's config
        return False

    elif canonical_builders_from_upstream in ['on', True]:
        # yaml parser converts bare 'on' to True, same for 'off' and False
        return True

    elif canonical_builders_from_upstream in ['off', False]:
        return False

    else:
        # Invalid value: fallback to default
        LOGGER.warning(
            'Invalid value provided for "canonical_builders_from_upstream": %s', canonical_builders_from_upstream)
        return False
