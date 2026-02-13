import re
from enum import Enum
from typing import Optional, Tuple


def split_el_suffix_in_release(release: str) -> Tuple[str, Optional[str]]:
    """
    Given a release field, this will method will split out any
    .el### or +el### or .scos### or +scos### suffix and return (prefix, el_suffix) where el_suffix
    is None if there .el### or .scos### is not detected.
    For OKD builds, this returns the scos suffix (e.g., 'scos9').
    For OCP builds, this returns the el suffix (e.g., 'el9').
    """

    el_suffix_match = re.match(r"(.*)[.+]((?:el|scos)\d+)(?:.*|$)", release)
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
    assembly_prefix = ".assembly."
    asm_pos = release.rfind(assembly_prefix)
    if asm_pos == -1:
        return None

    # Our rpm release fields will usually have ".el?" or ".scos?" after ".assembly.name"
    # But some of our base images can have ".el?" or ".scos?" before ".assembly.name"
    # If ".el?" or ".scos?" appears after assembly name, then strip it off
    el_pos = max(release.rfind(".el"), release.rfind(".scos"))
    if el_pos > asm_pos:
        release, _ = split_el_suffix_in_release(release)

    return release[asm_pos + len(assembly_prefix) :]


def isolate_el_version_in_release(release: str) -> Optional[int]:
    """
    Given a release field, determines whether it contains
    a RHEL version (el### or scos###). If it does, it returns the version value as an int.
    If it is not found, None is returned.
    """
    _, el_suffix = split_el_suffix_in_release(release)
    if el_suffix:
        # Strip the prefix ('el' or 'scos') and return the numeric version
        if el_suffix.startswith("scos"):
            return int(el_suffix[4:])
        else:  # starts with 'el'
            return int(el_suffix[2:])
    return None


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


class SoftwareLifecyclePhase(Enum):
    PRE_RELEASE = 0
    SIGNING = 1
    RELEASE = 2
    EOL = 100

    @classmethod
    def from_name(cls, phase_name):
        try:
            return cls[phase_name.upper().replace("-", "_")]
        except KeyError:
            raise ValueError(f"{phase_name} is not a valid phase name")

    def __lt__(self, other):
        if isinstance(other, SoftwareLifecyclePhase):
            return self.value < other.value
        return self.value < other

    def __gt__(self, other):
        if isinstance(other, SoftwareLifecyclePhase):
            return self.value > other.value
        return self.value > other

    def __le__(self, other):
        return not self > other

    def __ge__(self, other):
        return not self < other

    def __eq__(self, other):
        if isinstance(other, SoftwareLifecyclePhase):
            return self.value == other.value
        return self.value == other
