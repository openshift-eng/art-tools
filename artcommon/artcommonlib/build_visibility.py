import re
from enum import Enum
from typing import List, Optional

# from doozerlib.util import isolate_pflag_in_release


class BuildVisibility(Enum):
    PUBLIC = 0
    PRIVATE = 1


VISIBILITY_SUFFIX = {
    "brew": {
        BuildVisibility.PUBLIC: "p0",
        BuildVisibility.PRIVATE: "p1",
    },
    "konflux": {
        BuildVisibility.PUBLIC: "p2",
        BuildVisibility.PRIVATE: "p3",
    },
}


def get_build_system(visibility_suffix: str) -> str:
    """
    Get the build system from the visibility suffix.
    E.g. 'p0' -> 'brew', 'p3' -> 'konflux'
    """
    for build_system, visibility_map in VISIBILITY_SUFFIX.items():
        if visibility_suffix in visibility_map.values():
            return build_system
    raise ValueError(f"Unknown visibility suffix: {visibility_suffix}")


def is_release_embargoed(release: str, build_system: str, default=True) -> bool:
    """
    Get the embargo status from the visibility suffix.
    E.g. 'p0' -> False, 'p3' -> True
    """
    pflag = isolate_pflag_in_release(release)
    if pflag == VISIBILITY_SUFFIX[build_system][BuildVisibility.PRIVATE]:
        return True
    elif pflag == VISIBILITY_SUFFIX[build_system][BuildVisibility.PUBLIC]:
        return False

    # If in doubt (e.g. the p? information is missing) mark the build as embargoed
    return default


def get_visibility_suffix(build_system: str, visibility: BuildVisibility) -> str:
    """
    Get the p? flag based on the visibility status and the build system. E.g.:
    - 'brew', PUBLIC -> 'p0'
    - 'konflux', PRIVATE -> 'p3'
    """
    return VISIBILITY_SUFFIX[build_system][visibility]


def get_all_visibility_suffixes() -> List[str]:
    """
    Return all valid visibility suffixes, for all build systems and visibility statuses
    """
    return [suffix for d in VISIBILITY_SUFFIX.values() for suffix in d.values()]


def isolate_pflag_in_release(release: str) -> Optional[str]:
    """
    Given a release field, determines whether it contains any of the visibility suffixes.
    If it does, it returns the matched suffix (e.g., 'p0', 'p1', 'p2', 'p3').
    If not found, returns None.
    """
    suffixes = get_all_visibility_suffixes() + ["p?"]
    suffix_pattern = "|".join(re.escape(suffix) for suffix in suffixes)
    pattern = rf"\.({suffix_pattern})(?:\.|$)"
    match = re.search(pattern, release)
    if match:
        return match.group(1)
    return None
