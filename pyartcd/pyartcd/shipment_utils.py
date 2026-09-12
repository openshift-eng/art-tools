"""
Shared helpers for Konflux shipment pipelines.

The helpers in this module keep RHEL-version handling consistent between
standalone binary releases and MicroShift bootc shipments.
"""

from pathlib import Path

import yaml as stdlib_yaml
from artcommonlib.release_util import isolate_el_version_in_release


def group_nvrs_by_rhel_version(nvrs: list[str]) -> dict[str, list[str]]:
    """
    Groups NVRs by their RHEL version suffix.

    Args:
        nvrs: Build NVRs whose final release segment may contain an ``elN`` suffix.
    Returns:
        Dictionary keyed by ``elN`` or ``default``, sorted by key for deterministic output.
    """
    groups: dict[str, list[str]] = {}
    for nvr in nvrs:
        # The release field is the last hyphen-delimited segment of an NVR.
        release = nvr.rsplit("-", 1)[-1] if "-" in nvr else nvr
        el_version = isolate_el_version_in_release(release)
        key = f"el{el_version}" if el_version is not None else "default"
        groups.setdefault(key, []).append(nvr)

    return dict(
        sorted(
            groups.items(),
            key=lambda item: -1 if item[0] == "default" else int(item[0].removeprefix("el")),
        )
    )


def get_release_plan_names(
    config_path: Path,
    application: str,
    rhel_suffix: str | None = None,
) -> tuple[str, str]:
    """
    Loads stage and production ReleasePlan names from shipment config.yaml.

    When a RHEL suffix is provided, the RHEL-specific application key is tried
    first and the plain application key is used as a backwards-compatible fallback.

    Args:
        config_path: Path to the shipment-data config.yaml file.
        application: Base Konflux application name.
        rhel_suffix: Optional suffix such as ``el9`` or ``el10``.
    Returns:
        Tuple containing the stage and production ReleasePlan names. Missing
        values retain the existing ``n/a`` behavior.
    """
    stage_release_plan = "n/a"
    prod_release_plan = "n/a"

    if not config_path.exists():
        return stage_release_plan, prod_release_plan

    with config_path.open("r") as config_file:
        shipment_config = stdlib_yaml.safe_load(config_file) or {}

    applications = shipment_config.get("applications", {})
    lookup_keys = [application]
    if rhel_suffix:
        rhel_number = rhel_suffix.removeprefix("el").removeprefix("rhel")
        lookup_keys = [
            f"{application}-rhel{rhel_number}",
            f"{application}-{rhel_suffix}",
            application,
        ]

    application_config = {}
    for lookup_key in lookup_keys:
        application_config = applications.get(lookup_key, {})
        if application_config:
            break

    application_config = application_config.get("environments", {})
    stage_release_plan = application_config.get("stage", {}).get("releasePlan", "n/a")
    prod_release_plan = application_config.get("prod", {}).get("releasePlan", "n/a")

    return stage_release_plan, prod_release_plan
