"""Domain models for evaluating RHCOS RPM verification evidence."""

import re
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import Any


class VerificationResult(StrEnum):
    """
    Result recorded by an RHCOS CVE verification comment.
    """

    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"
    UNSUPPORTED = "unsupported"


class TrackerDisposition(StrEnum):
    """
    Action or outcome for an RHCOS tracker in the selected release.
    """

    ALREADY_VERIFIED = "already_verified"
    EXCLUDED = "excluded"
    STATUS_NOT_ELIGIBLE = "status_not_eligible"
    NO_EVIDENCE = "no_evidence"
    OBSERVED_IN_BASIS = "observed_in_basis"
    NEEDS_REVIEW = "needs_review"
    EVIDENCE_INCOMPLETE = "evidence_incomplete"
    READY_TO_VERIFY = "ready_to_verify"
    WOULD_VERIFY = "would_verify"
    VERIFIED_FROM_EVIDENCE = "verified_from_evidence"
    STATUS_UPDATE_FAILED = "status_update_failed"


@dataclass(frozen=True)
class VerificationComment:
    """
    A Jira comment containing a possible RHCOS verification result.
    """

    created: str
    body: Any


@dataclass(frozen=True)
class RpmNevra:
    """
    An RPM name, epoch, version, release, and optional architecture.
    """

    name: str
    epoch: str
    version: str
    release: str
    arch: str | None

    @property
    def rhel_branch(self) -> str | None:
        """
        Return the exact RHEL branch embedded in this RPM release.
        """
        match = re.search(r"\.el\d+(?:_\d+|[a-z]+)?(?=$|[._])", self.release)
        return match.group() if match else None

    def to_dict(self) -> dict[str, str | None]:
        """
        Serialize the RPM in a stable report shape.
        """
        return {
            "name": self.name,
            "epoch": self.epoch,
            "version": self.version,
            "release": self.release,
            "arch": self.arch,
        }

    def __str__(self) -> str:
        """
        Format the RPM as NEVRA, omitting architecture when unspecified.
        """
        nevra = f"{self.name}-{self.epoch}:{self.version}-{self.release}"
        return f"{nevra}.{self.arch}" if self.arch else nevra


@dataclass(frozen=True)
class InstalledRpm(RpmNevra):
    """
    An RPM read from a selected RHCOS image.
    """

    stream: str


class RHCOSRpm(Enum):
    """
    RPM families that RHCOS evidence discovery evaluates.
    """

    KERNEL = ("kernel", ("kernel-core",), ("extensions",), True)

    def __init__(
        self,
        source_package_name: str,
        installed_package_names: tuple[str, ...],
        excluded_stream_fragments: tuple[str, ...],
        required_per_architecture: bool,
    ) -> None:
        """
        Store the source build name and its installed-image RPM selectors.
        """
        self.source_package_name = source_package_name
        self.installed_package_names = installed_package_names
        self.excluded_stream_fragments = excluded_stream_fragments
        self.required_per_architecture = required_per_architecture

    def matches_inventory_rpm(self, rpm: InstalledRpm) -> bool:
        """
        Return whether an installed RPM belongs to this family in the base OS.
        """
        return rpm.name in self.installed_package_names and not any(
            fragment in rpm.stream for fragment in self.excluded_stream_fragments
        )

    def build_nvr(self, rpm: InstalledRpm) -> str:
        """
        Format the source package NVR corresponding to an installed RPM.
        """
        return f"{self.source_package_name}-{rpm.version}-{rpm.release}"


@dataclass(frozen=True)
class RHCOSImageInventory:
    """
    One RHCOS image's identity and installed RPM inventory.
    """

    arch: str
    build_id: str
    pullspecs: tuple[tuple[str, str], ...]
    rpms: tuple[InstalledRpm, ...]

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize image identity without duplicating the full RPM inventory.
        """
        return {
            "arch": self.arch,
            "build_id": self.build_id,
            "pullspecs": dict(self.pullspecs),
            "rpm_count": len(self.rpms),
        }


@dataclass(frozen=True)
class RpmChange:
    """
    Difference for one RPM package, stream, RHEL branch, and image architecture.
    """

    image_arch: str
    rpm_type: RHCOSRpm
    stream: str
    change: str
    before: InstalledRpm | None
    after: InstalledRpm | None

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize the RPM inventory change and both exact package versions.
        """
        return {
            "image_arch": self.image_arch,
            "rpm_type": self.rpm_type.name.lower(),
            "stream": self.stream,
            "change": self.change,
            "before": self.before.to_dict() if self.before else None,
            "after": self.after.to_dict() if self.after else None,
        }


@dataclass(frozen=True)
class RpmAdvisoryEvidence:
    """
    CVEs and metadata associated with one Errata advisory containing an RPM build.
    """

    advisory_id: str
    url: str
    state: str
    cves: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize the advisory metadata used during CVE candidate discovery.
        """
        return {
            "id": self.advisory_id,
            "url": self.url,
            "state": self.state,
            "cves": list(self.cves),
        }


@dataclass(frozen=True)
class RpmBuildEvidence:
    """
    CVE evidence collected for one exact Brew RPM build NVR.
    """

    nvr: str
    changelog_cves: tuple[str, ...]
    changelog_complete: bool
    advisories: tuple[RpmAdvisoryEvidence, ...]
    advisory_lookup_complete: bool
    warnings: tuple[str, ...] = ()

    @property
    def advisory_cves(self) -> set[str]:
        """
        Return CVEs listed by advisories associated with this build.
        """
        return {cve for advisory in self.advisories for cve in advisory.cves}

    @property
    def all_cves(self) -> set[str]:
        """
        Return CVEs mentioned by either the build changelog or its advisories.
        """
        return set(self.changelog_cves) | self.advisory_cves

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize build-level evidence and its collection completeness.
        """
        return {
            "nvr": self.nvr,
            "changelog_cves": list(self.changelog_cves),
            "changelog_complete": self.changelog_complete,
            "advisories": [advisory.to_dict() for advisory in self.advisories],
            "advisory_lookup_complete": self.advisory_lookup_complete,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class RpmCveEvidence:
    """
    Package-specific evidence connecting a selected RHCOS RPM build to a CVE.
    """

    cve_id: str
    rpm_type: RHCOSRpm
    rpm_name: str
    stream: str
    image_arch: str
    rhel_branch: str | None
    selected_nvr: str
    basis_nvr: str | None
    changelog_match: bool
    advisory_ids: tuple[str, ...]
    advisory_urls: tuple[str, ...]
    observed_in_basis: bool
    new_changelog_match: bool
    baseline_complete: bool

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize exact RPM, baseline, changelog, and advisory evidence for a CVE.
        """
        return {
            "cve_id": self.cve_id,
            "rpm_type": self.rpm_type.name.lower(),
            "rpm_name": self.rpm_name,
            "stream": self.stream,
            "image_arch": self.image_arch,
            "rhel_branch": self.rhel_branch,
            "selected_nvr": self.selected_nvr,
            "basis_nvr": self.basis_nvr,
            "changelog_match": self.changelog_match,
            "advisory_ids": list(self.advisory_ids),
            "advisory_urls": list(self.advisory_urls),
            "observed_in_basis": self.observed_in_basis,
            "new_changelog_match": self.new_changelog_match,
            "baseline_complete": self.baseline_complete,
        }


@dataclass(frozen=True)
class RpmRequirementCheck:
    """
    Comparison of one required RPM against the selected image inventory.
    """

    required: RpmNevra
    installed: tuple[InstalledRpm, ...]
    passed: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize the requirement and the selected RPMs checked against it.
        """
        return {
            "required": self.required.to_dict(),
            "installed": [rpm.to_dict() | {"stream": rpm.stream} for rpm in self.installed],
            "passed": self.passed,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class TrackerFinding:
    """
    Evidence outcome for one release-scoped RHCOS CVE tracker.
    """

    issue_key: str
    cve_id: str
    component: str
    status: str
    disposition: TrackerDisposition
    reason: str
    verification_comment_created: str | None = None
    checks: tuple[RpmRequirementCheck, ...] = ()
    rpm_evidence: tuple[RpmCveEvidence, ...] = ()
    warning: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize the finding for logs and archived evidence.
        """
        return {
            "issue_key": self.issue_key,
            "cve_id": self.cve_id,
            "component": self.component,
            "status": self.status,
            "disposition": self.disposition.value,
            "reason": self.reason,
            "verification_comment_created": self.verification_comment_created,
            "rpm_checks": [check.to_dict() for check in self.checks],
            "rpm_evidence": [evidence.to_dict() for evidence in self.rpm_evidence],
            "warning": self.warning,
        }


@dataclass(frozen=True)
class AnalysisReport:
    """
    Complete result of an RHCOS RPM evidence run.
    """

    assembly: str
    target_releases: tuple[str, ...]
    complete: bool
    selected_images: tuple[RHCOSImageInventory, ...]
    findings: tuple[TrackerFinding, ...]
    tracker_search_complete: bool
    basis_images: tuple[RHCOSImageInventory, ...] = ()
    rpm_changes: tuple[RpmChange, ...] = ()
    rpm_build_evidence: tuple[RpmBuildEvidence, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize the report as JSON-compatible values.
        """
        return {
            "provider": "rhcos_rpm",
            "assembly": self.assembly,
            "target_releases": list(self.target_releases),
            "complete": self.complete,
            "tracker_search_complete": self.tracker_search_complete,
            "architecture_scope": [image.arch for image in self.selected_images],
            "selected_images": [image.to_dict() for image in self.selected_images],
            "basis_images": [image.to_dict() for image in self.basis_images],
            "rpm_changes": [change.to_dict() for change in self.rpm_changes],
            "rpm_build_evidence": [evidence.to_dict() for evidence in self.rpm_build_evidence],
            "findings": [finding.to_dict() for finding in self.findings],
            "warnings": list(self.warnings),
        }
