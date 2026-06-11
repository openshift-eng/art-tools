"""
Scoped OCP version relationship helpers.

Different features need different "next/previous" semantics (standard train,
bridge siblings, release schedules, bug workflow). Use the feature-specific
helpers in this module rather than generic version arithmetic.
"""

from artcommonlib.constants import LAST_OCP_MINOR_VERSION, OCP5_BRIDGE_MINOR_BASE


def is_ocp5_bridge_group(major: int, minor: int) -> bool:
    """Return True for OCP 5.x compat bridge groups (e.g. openshift-4.23)."""
    return major == 4 and minor >= OCP5_BRIDGE_MINOR_BASE


def get_ocp5_basis_release(bridge_major: int, bridge_minor: int) -> tuple[int, int]:
    """Return the OCP 5.x basis release for a bridge group (e.g. 4.23 -> 5.0)."""
    if not is_ocp5_bridge_group(bridge_major, bridge_minor):
        raise ValueError(
            f"OCP {bridge_major}.{bridge_minor} is not an OCP 5.x bridge release "
            f"(bridge minors start at 4.{OCP5_BRIDGE_MINOR_BASE})"
        )
    return 5, bridge_minor - OCP5_BRIDGE_MINOR_BASE


def group_from_version(major: int, minor: int) -> str:
    return f"openshift-{major}.{minor}"


def get_standard_train_previous(major: int, minor: int) -> tuple[int, int]:
    """
    Previous release on the standard OCP train (excludes bridge groups).

    Examples: 5.0 -> 4.22, 4.22 -> 4.21, 5.1 -> 5.0.
    """
    if is_ocp5_bridge_group(major, minor):
        raise ValueError(
            f"OCP {major}.{minor} is a bridge release; use a scoped helper instead of get_standard_train_previous"
        )

    if minor > 0:
        return major, minor - 1

    prev_major = major - 1
    prev_max = LAST_OCP_MINOR_VERSION.get(prev_major)
    if prev_max is None:
        raise ValueError(
            f"Cannot determine previous version of OCP {major}.0: "
            f"OCP {prev_major}.x maximum minor version is not set in LAST_OCP_MINOR_VERSION."
        )
    return prev_major, prev_max


def get_standard_train_next(major: int, minor: int) -> tuple[int, int]:
    """
    Next release on the standard OCP train (excludes bridge groups).

    Examples: 4.22 -> 5.0, 4.21 -> 4.22, 5.0 -> 5.1.
    """
    if is_ocp5_bridge_group(major, minor):
        raise ValueError(
            f"OCP {major}.{minor} is a bridge release; use a scoped helper instead of get_standard_train_next"
        )

    max_minor = LAST_OCP_MINOR_VERSION.get(major)
    if max_minor is None:
        return major, minor + 1

    if minor >= max_minor:
        next_major = major + 1
        if next_major not in LAST_OCP_MINOR_VERSION:
            raise ValueError(
                f"OCP {major}.{minor} is at or beyond maximum known version ({major}.{max_minor}). "
                f"Cannot determine next version: OCP {next_major}.x is not defined in LAST_OCP_MINOR_VERSION."
            )
        return next_major, 0

    return major, minor + 1


def resolve_inflight_schedule_group(major: int, minor: int) -> str:
    """
    Group whose Product Pages schedule is used to detect concurrent in-flight releases.

    Bridge groups use the last standard 4.x schedule for the first bridge (4.23 -> 4.22),
    then the previous bridge minor for later bridge releases (4.24 -> 4.23).
    """
    if is_ocp5_bridge_group(major, minor):
        if minor == OCP5_BRIDGE_MINOR_BASE:
            last_standard = LAST_OCP_MINOR_VERSION[4]
            if last_standard is None:
                raise ValueError("LAST_OCP_MINOR_VERSION[4] is not set")
            return group_from_version(4, last_standard)
        return group_from_version(major, minor - 1)

    prev_major, prev_minor = get_standard_train_previous(major, minor)
    return group_from_version(prev_major, prev_minor)


def get_second_fix_reference_version(major: int, minor: int) -> tuple[int, int]:
    """Prior release cited when closing a non-first-fix CVE tracker in pre-release."""
    if is_ocp5_bridge_group(major, minor):
        last_standard = LAST_OCP_MINOR_VERSION[4]
        if last_standard is None:
            raise ValueError("LAST_OCP_MINOR_VERSION[4] is not set")
        return 4, last_standard
    return get_standard_train_previous(major, minor)


def get_regression_check_gate_version(major: int, minor: int) -> str:
    """
    Next GA lifecycle gate for blocking-bug regression checks.

    Bridge groups gate on the basis release's train-next (4.23 -> 5.1).
    """
    if is_ocp5_bridge_group(major, minor):
        basis_major, basis_minor = get_ocp5_basis_release(major, minor)
        next_major, next_minor = get_standard_train_next(basis_major, basis_minor)
    else:
        next_major, next_minor = get_standard_train_next(major, minor)
    return f"{next_major}.{next_minor}"


def get_blocking_bug_target_version(major: int, minor: int) -> tuple[int, int]:
    """Target major.minor for blocking bugs (X.Y.z target release matching)."""
    if is_ocp5_bridge_group(major, minor):
        return get_ocp5_basis_release(major, minor)
    return get_standard_train_next(major, minor)


def get_next_scheduled_release_group(major: int, minor: int) -> str:
    """Group used for is_release_next_week schedule checks on blocker advisories."""
    if is_ocp5_bridge_group(major, minor):
        basis_major, basis_minor = get_ocp5_basis_release(major, minor)
        return group_from_version(basis_major, basis_minor)
    next_major, next_minor = get_standard_train_next(major, minor)
    return group_from_version(next_major, next_minor)


def get_reconciliation_depend_version(major: int, minor: int) -> tuple[int, int]:
    """Next minor for ART reconciliation JIRA depend links (Update X.Y {image}...)."""
    if is_ocp5_bridge_group(major, minor):
        basis_major, basis_minor = get_ocp5_basis_release(major, minor)
        return get_standard_train_next(basis_major, basis_minor)
    return get_standard_train_next(major, minor)
