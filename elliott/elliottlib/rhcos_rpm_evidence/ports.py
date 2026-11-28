"""Ports consumed by the RHCOS RPM evidence use case."""

from typing import Protocol

from elliottlib.rhcos_rpm_evidence.models import (
    InstalledRpm,
    RHCOSImageInventory,
    RpmBuildEvidence,
)


class RHCOSTracker(Protocol):
    """
    Tracker fields needed by RHCOS evidence evaluation.
    """

    @property
    def id(self) -> str:
        """
        Jira issue key.
        """
        ...

    @property
    def cve_id(self) -> str | None:
        """
        Tracked CVE identifier.
        """
        ...

    @property
    def status(self) -> str:
        """
        Current Jira workflow status.
        """
        ...

    @property
    def whiteboard_component(self) -> str | None:
        """
        Downstream component identifying the RHCOS stream.
        """
        ...


class RHCOSTrackerRepository(Protocol):
    """
    Port for release-scoped tracker lookup, comments, and status changes.
    """

    def target_releases(self) -> tuple[str, ...]:
        """
        Return configured Jira target releases after validating their availability.
        """
        ...

    def find_trackers(self) -> list[RHCOSTracker]:
        """
        Find release-scoped RHCOS CVE trackers in all workflow states.
        """
        ...

    def mark_verified(self, bug: RHCOSTracker, comment: str, dry_run: bool) -> bool:
        """
        Move the tracker to VERIFIED and add the evidence comment.
        """
        ...


class RHCOSInventorySource(Protocol):
    """
    Port for loading selected RHCOS images and their installed RPMs.
    """

    def collect(self) -> tuple[RHCOSImageInventory, ...]:
        """
        Return selected images and their RPM inventories.
        """
        ...

    def collect_basis(self) -> tuple[RHCOSImageInventory, ...]:
        """
        Return the exact RHCOS inventories represented by the assembly basis.
        """
        ...


class RpmBuildEvidenceSource(Protocol):
    """
    Port for collecting changelog and Errata evidence for exact RPM builds.
    """

    def collect(self, nvrs: set[str]) -> tuple[RpmBuildEvidence, ...]:
        """
        Return CVE evidence for the requested RPM NVRs, including partial failures.
        """
        ...

    def resolve_build_nvrs(self, rpms: set[InstalledRpm]) -> dict[InstalledRpm, str]:
        """
        Resolve installed binary RPMs to their exact Brew source build NVRs.
        """
        ...
