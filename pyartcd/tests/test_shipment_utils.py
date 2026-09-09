"""
Tests for shared Konflux shipment helpers.
"""

from pathlib import Path

from pyartcd.shipment_utils import get_release_plan_names, group_nvrs_by_rhel_version


def test_group_nvrs_by_rhel_version_sorts_rhel_groups() -> None:
    """Groups NVRs by their RHEL suffix in deterministic order."""
    nvrs = [
        "microshift-bootc-rhel10-container-v5.0-1.el10",
        "microshift-bootc-container-v5.0-1.el9",
    ]

    assert group_nvrs_by_rhel_version(nvrs) == {
        "el10": ["microshift-bootc-rhel10-container-v5.0-1.el10"],
        "el9": ["microshift-bootc-container-v5.0-1.el9"],
    }


def test_group_nvrs_by_rhel_version_uses_default_without_suffix() -> None:
    """Places NVRs without a detectable RHEL suffix in the default group."""
    assert group_nvrs_by_rhel_version(["microshift-bootc-container-v5.0-1"]) == {
        "default": ["microshift-bootc-container-v5.0-1"]
    }


def test_get_release_plan_names_prefers_rhel_specific_application(tmp_path: Path) -> None:
    """Uses the RHEL-specific application entry when it exists."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "applications:\n"
        "  openshift-5-0:\n"
        "    environments:\n"
        "      stage:\n"
        "        releasePlan: old-stage\n"
        "      prod:\n"
        "        releasePlan: old-prod\n"
        "  openshift-5-0-rhel10:\n"
        "    environments:\n"
        "      stage:\n"
        "        releasePlan: rhel10-stage\n"
        "      prod:\n"
        "        releasePlan: rhel10-prod\n"
    )

    assert get_release_plan_names(config_path, "openshift-5-0", "el10") == (
        "rhel10-stage",
        "rhel10-prod",
    )


def test_get_release_plan_names_falls_back_to_plain_application(tmp_path: Path) -> None:
    """Falls back to the plain application entry for older shipment data."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "applications:\n"
        "  openshift-4-22:\n"
        "    environments:\n"
        "      stage:\n"
        "        releasePlan: stage\n"
        "      prod:\n"
        "        releasePlan: prod\n"
    )

    assert get_release_plan_names(config_path, "openshift-4-22", "el9") == ("stage", "prod")


def test_get_release_plan_names_returns_na_for_missing_config(tmp_path: Path) -> None:
    """Returns the existing n/a defaults when no application is configured."""
    assert get_release_plan_names(tmp_path / "missing.yaml", "openshift-5-0", "el9") == ("n/a", "n/a")
