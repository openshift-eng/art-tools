"""
Shared utilities for RPM lockfile generation.
"""

import os

from artcommonlib.rpm_utils import label_compare, split_nvr_epoch


def build_env() -> dict:
    """
    Build environment for subprocess with registry auth if available.
    """
    env = os.environ.copy()
    registry_auth = os.environ.get("QUAY_AUTH_FILE") or os.environ.get("REGISTRY_AUTH_FILE")
    if registry_auth:
        env["REGISTRY_AUTH_FILE"] = registry_auth
    return env


def parse_evr(evr: str) -> tuple[str, str, str]:
    """
    Parse an EVR string into (epoch, version, release).

    Uses artcommonlib.rpm_utils.split_nvr_epoch for epoch extraction.

    Arg(s):
        evr (str): EVR string in format "version-release" or
            "epoch:version-release".
    Return Value(s):
        tuple[str, str, str]: (epoch, version, release).
    """
    vr, epoch = split_nvr_epoch(evr)
    if not epoch:
        epoch = "0"
    version, release = vr.rsplit("-", 1)
    return epoch, version, release


def compare_evr(evr1: str, evr2: str) -> int:
    """
    Compare two EVR strings using RPM version comparison.

    Arg(s):
        evr1 (str): First EVR string.
        evr2 (str): Second EVR string.
    Return Value(s):
        int: 1 if evr1 is newer, -1 if evr2 is newer, 0 if equal.
    """
    return label_compare(parse_evr(evr1), parse_evr(evr2))


def pick_minimum_evr(evrs: list[str]) -> str:
    """
    Return the oldest EVR from a list using RPM version comparison.

    Arg(s):
        evrs (list[str]): EVR strings to compare.
    Return Value(s):
        str: The minimum (oldest) EVR.
    """
    result = evrs[0]
    for evr in evrs[1:]:
        if compare_evr(evr, result) < 0:
            result = evr
    return result


def format_version_pin(name: str, evr: str) -> str:
    """
    Format a version-pinned DNF package spec from name and EVR.

    Arg(s):
        name (str): Package name (e.g., "libeconf").
        evr (str): EVR string (e.g., "0.4.1-5.el9" or "1:2.0-3.el9").
    Return Value(s):
        str: DNF install spec (e.g., "libeconf-0.4.1-5.el9").
    """
    epoch, version, release = parse_evr(evr)
    if epoch != "0":
        return f"{name}-{epoch}:{version}-{release}"
    return f"{name}-{version}-{release}"
