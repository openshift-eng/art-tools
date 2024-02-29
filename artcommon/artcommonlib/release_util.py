import re
from typing import Optional, Tuple


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
