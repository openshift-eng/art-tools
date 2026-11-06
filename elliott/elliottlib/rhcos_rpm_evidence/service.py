"""Use case for discovering and verifying RHCOS CVE trackers from RPM evidence."""

import logging
import re
from collections.abc import Iterable

from elliottlib.rhcos_rpm_evidence.discovery import compare_rpm_inventories, match_changed_rpm_cves, required_build_nvrs
from elliottlib.rhcos_rpm_evidence.models import (
    AnalysisReport,
    RHCOSImageInventory,
    RHCOSRpm,
    RpmBuildEvidence,
    RpmChange,
    RpmCveEvidence,
    RpmRequirementCheck,
    TrackerDisposition,
    TrackerFinding,
    VerificationResult,
)
from elliottlib.rhcos_rpm_evidence.ports import (
    RHCOSInventorySource,
    RHCOSTracker,
    RHCOSTrackerRepository,
    RpmBuildEvidenceSource,
)
from elliottlib.rhcos_rpm_evidence.requirements import (
    ParsedVerification,
    check_required_rpms,
    parse_latest_verification,
)

LOGGER = logging.getLogger(__name__)
_VERIFIED_STATUS = "VERIFIED"
_TRANSITIONABLE_STATUSES = {"NEW", "ASSIGNED", "POST", "MODIFIED", "ON_QA"}
_RHEL_COMPONENT_RE = re.compile(r"^openshift/ose-rhel-coreos-(\d+)$")


class RHCOSRpmEvidenceService:
    """
    Compare selected RHCOS RPMs with their basis and evaluate CVE tracker evidence.
    """

    def __init__(
        self,
        assembly: str,
        inventory_source: RHCOSInventorySource,
        build_evidence_source: RpmBuildEvidenceSource,
        tracker_repository: RHCOSTrackerRepository,
        excluded_issue_keys: Iterable[str] = (),
        rhcos_rpms: Iterable[RHCOSRpm] = (RHCOSRpm.KERNEL,),
    ) -> None:
        """
        Configure the use case with inventory, build evidence, Jira, and exclusions.
        """
        self._assembly = assembly
        self._inventory_source = inventory_source
        self._build_evidence_source = build_evidence_source
        self._tracker_repository = tracker_repository
        self._excluded_issue_keys = {str(issue_key) for issue_key in excluded_issue_keys}
        self._rhcos_rpms = tuple(rhcos_rpms)
        if not self._rhcos_rpms:
            raise ValueError("At least one RHCOS RPM family must be selected for evidence discovery")

    def run(self, update_trackers: bool = False, dry_run: bool = False) -> AnalysisReport:
        """
        Collect release evidence, evaluate trackers, and optionally transition qualified issues.
        """
        warnings: list[str] = []
        selected_images: tuple[RHCOSImageInventory, ...] = ()
        basis_images: tuple[RHCOSImageInventory, ...] = ()
        rpm_changes: tuple[RpmChange, ...] = ()
        build_evidence: tuple[RpmBuildEvidence, ...] = ()
        rpm_cve_evidence: tuple[RpmCveEvidence, ...] = ()
        rpm_discovery_complete = False

        try:
            target_releases = self._tracker_repository.target_releases()
            trackers = self._tracker_repository.find_trackers()
        except Exception as exc:
            warning = f"RHCOS Jira tracker search incomplete: {exc}"
            LOGGER.warning(warning)
            return self._report(warnings=[warning], tracker_search_complete=False)

        try:
            selected_images = self._inventory_source.collect()
            if not selected_images:
                raise ValueError("No selected RHCOS image inventories were returned")
        except Exception as exc:
            warning = f"Selected RHCOS RPM inventory unavailable: {exc}"
            LOGGER.warning(warning)
            findings = tuple(
                self._finding(
                    bug,
                    TrackerDisposition.EVIDENCE_INCOMPLETE,
                    warning,
                    warning=warning,
                )
                for bug in sorted(trackers, key=lambda item: item.id)
            )
            return self._report(
                target_releases=target_releases,
                findings=findings,
                warnings=[warning],
                tracker_search_complete=True,
            )

        baseline_inventory_complete = False
        try:
            basis_images = self._inventory_source.collect_basis()
            if not basis_images:
                raise ValueError("No basis RHCOS image inventories were returned")
            baseline_inventory_complete = True
        except Exception as exc:
            warning = f"Basis RHCOS RPM inventory unavailable: {exc}"
            LOGGER.warning(warning)
            warnings.append(warning)

        if baseline_inventory_complete:
            try:
                rpm_changes = compare_rpm_inventories(basis_images, selected_images, self._rhcos_rpms)
                rpm_discovery_complete = True
            except Exception as exc:
                warning = f"RHCOS RPM comparison incomplete: {exc}"
                LOGGER.warning(warning)
                warnings.append(warning)
                baseline_inventory_complete = False

        if baseline_inventory_complete:
            try:
                required_nvrs = required_build_nvrs(rpm_changes)
                if required_nvrs:
                    build_evidence = self._build_evidence_source.collect(required_nvrs)
                    rpm_cve_evidence = match_changed_rpm_cves(rpm_changes, build_evidence, baseline_inventory_complete)
                    warnings.extend(warning for evidence in build_evidence for warning in evidence.warnings)
                    evidence_by_nvr = {evidence.nvr: evidence for evidence in build_evidence}
                    rpm_discovery_complete = (
                        rpm_discovery_complete
                        and set(evidence_by_nvr) == required_nvrs
                        and all(
                            evidence.changelog_complete and evidence.advisory_lookup_complete
                            for evidence in build_evidence
                        )
                    )
            except Exception as exc:
                warning = f"RHCOS RPM changelog and advisory evidence collection failed: {exc}"
                LOGGER.warning(warning)
                warnings.append(warning)
                rpm_discovery_complete = False

        findings: list[TrackerFinding] = []
        for bug in sorted(trackers, key=lambda item: item.id):
            matches = self._tracker_evidence(bug, rpm_cve_evidence)
            finding = self._evaluate_tracker(
                bug, selected_images, matches, rpm_discovery_complete, update_trackers, dry_run
            )
            findings.append(finding)
            if finding.warning:
                warnings.append(f"{finding.issue_key}: {finding.warning}")

        return self._report(
            target_releases=target_releases,
            complete=not warnings,
            selected_images=selected_images,
            basis_images=basis_images,
            rpm_changes=rpm_changes,
            build_evidence=build_evidence,
            findings=tuple(findings),
            warnings=warnings,
            tracker_search_complete=True,
        )

    def _tracker_evidence(self, bug: RHCOSTracker, evidence: tuple[RpmCveEvidence, ...]) -> tuple[RpmCveEvidence, ...]:
        """
        Select package evidence for one CVE and its RHEL-specific Jira component.
        """
        cve_id = str(bug.cve_id or "").upper()
        component = str(bug.whiteboard_component or "")
        component_match = _RHEL_COMPONENT_RE.fullmatch(component)
        component_major = component_match.group(1) if component_match else None

        def _matches_component_branch(item: RpmCveEvidence) -> bool:
            """
            Match an RPM's RHEL major to an explicitly versioned tracker component.
            """
            if component_major is None:
                return True
            branch_match = re.search(r"\d+", item.rhel_branch or "")
            return bool(branch_match and branch_match.group() == component_major)

        return tuple(item for item in evidence if item.cve_id == cve_id and _matches_component_branch(item))

    def _evaluate_tracker(
        self,
        bug: RHCOSTracker,
        selected_images: tuple[RHCOSImageInventory, ...],
        rpm_evidence: tuple[RpmCveEvidence, ...],
        rpm_discovery_complete: bool,
        update_trackers: bool,
        dry_run: bool,
    ) -> TrackerFinding:
        """
        Evaluate one tracker and apply its permitted evidence-based status transition.
        """
        issue_key = str(bug.id)
        cve_id = str(bug.cve_id or "")
        component = str(bug.whiteboard_component or "")
        status = str(bug.status or "")
        normalized_status = status.strip().upper()

        if issue_key in self._excluded_issue_keys:
            return self._finding(bug, TrackerDisposition.EXCLUDED, "Issue is explicitly excluded by the assembly")
        if update_trackers and normalized_status == _VERIFIED_STATUS:
            return self._finding(bug, TrackerDisposition.ALREADY_VERIFIED, "Tracker is already VERIFIED")
        if update_trackers and normalized_status not in _TRANSITIONABLE_STATUSES:
            return self._finding(
                bug,
                TrackerDisposition.STATUS_NOT_ELIGIBLE,
                f"Tracker status {status!r} is not eligible for automatic verification",
            )
        if not cve_id:
            return self._finding(bug, TrackerDisposition.NEEDS_REVIEW, "Tracker has no CVE ID")

        try:
            comments = self._tracker_repository.comments(issue_key)
        except Exception as exc:
            warning = f"Unable to read verification comments: {exc}"
            return self._finding(
                bug,
                TrackerDisposition.EVIDENCE_INCOMPLETE,
                warning,
                rpm_evidence=rpm_evidence,
                warning=warning,
            )

        verification = parse_latest_verification(comments, cve_id)
        if verification and verification.result is VerificationResult.PASS:
            checks = check_required_rpms(verification.requirements, selected_images, component)
            if checks and all(check.passed for check in checks):
                return self._verify_from_evidence(
                    bug,
                    selected_images,
                    verification,
                    checks,
                    rpm_evidence,
                    update_trackers,
                    dry_run,
                    "The latest RHCOS verification passed; the selected image contains every required RPM at the "
                    "required version or newer",
                )
            failed = "; ".join(check.reason for check in checks if not check.passed)
            return self._finding(
                bug,
                TrackerDisposition.NEEDS_REVIEW,
                failed or "No RPM requirements could be checked",
                verification=verification,
                checks=checks,
                rpm_evidence=rpm_evidence,
            )

        if verification and verification.result in {VerificationResult.FAIL, VerificationResult.UNSUPPORTED}:
            return self._finding(
                bug,
                TrackerDisposition.NEEDS_REVIEW,
                verification.reason,
                verification=verification,
                rpm_evidence=rpm_evidence,
            )

        if any(evidence.new_changelog_match for evidence in rpm_evidence):
            reason = "A newly selected RPM changelog names this CVE; the exact RPM was added or upgraded from basis"
            return self._verify_from_evidence(
                bug,
                selected_images,
                verification,
                (),
                rpm_evidence,
                update_trackers,
                dry_run,
                reason,
            )

        if verification and verification.result is VerificationResult.UNKNOWN:
            return self._finding(
                bug,
                TrackerDisposition.NEEDS_REVIEW,
                verification.reason,
                verification=verification,
                rpm_evidence=rpm_evidence,
            )

        if rpm_evidence and any(not evidence.baseline_complete for evidence in rpm_evidence):
            disposition = TrackerDisposition.EVIDENCE_INCOMPLETE
            reason = "RPM CVE evidence was found, but the corresponding basis evidence is incomplete"
        elif rpm_evidence and all(evidence.observed_in_basis for evidence in rpm_evidence):
            disposition = TrackerDisposition.OBSERVED_IN_BASIS
            reason = "The matching CVE evidence is already present in the basis RPMs"
        elif rpm_evidence:
            disposition = TrackerDisposition.NEEDS_REVIEW
            reason = "The selected RPM is associated with this CVE, but its changelog does not establish a new fix"
        elif not rpm_discovery_complete:
            disposition = TrackerDisposition.EVIDENCE_INCOMPLETE
            reason = "RHCOS RPM delta, changelog, or advisory evidence could not be collected completely"
        else:
            disposition = TrackerDisposition.NO_EVIDENCE
            reason = "No changed selected RHCOS RPM evidence matched this CVE"

        return self._finding(
            bug,
            disposition,
            reason,
            verification=verification,
            rpm_evidence=rpm_evidence,
        )

    def _verify_from_evidence(
        self,
        bug: RHCOSTracker,
        selected_images: tuple[RHCOSImageInventory, ...],
        verification: ParsedVerification | None,
        checks: tuple[RpmRequirementCheck, ...],
        rpm_evidence: tuple[RpmCveEvidence, ...],
        update_trackers: bool,
        dry_run: bool,
        reason: str,
    ) -> TrackerFinding:
        """
        Report or perform a VERIFIED transition using the evidence attached to a finding.
        """
        if not update_trackers:
            return self._finding(
                bug,
                TrackerDisposition.READY_TO_VERIFY,
                reason,
                verification=verification,
                checks=checks,
                rpm_evidence=rpm_evidence,
            )

        try:
            updated = self._transition(bug, verification, checks, selected_images, rpm_evidence, dry_run)
        except Exception as exc:
            warning = f"Evidence passed but Jira status update failed: {exc}"
            return self._finding(
                bug,
                TrackerDisposition.STATUS_UPDATE_FAILED,
                warning,
                verification=verification,
                checks=checks,
                rpm_evidence=rpm_evidence,
                warning=warning,
            )

        if dry_run:
            disposition = TrackerDisposition.WOULD_VERIFY
            result_reason = f"{reason}; tracker would move to VERIFIED"
        elif updated:
            disposition = TrackerDisposition.VERIFIED_FROM_EVIDENCE
            result_reason = f"{reason}; tracker moved to VERIFIED"
        else:
            disposition = TrackerDisposition.ALREADY_VERIFIED
            result_reason = "Tracker was verified concurrently"
        return self._finding(
            bug,
            disposition,
            result_reason,
            verification=verification,
            checks=checks,
            rpm_evidence=rpm_evidence,
        )

    def _transition(
        self,
        bug: RHCOSTracker,
        verification: ParsedVerification | None,
        checks: tuple[RpmRequirementCheck, ...],
        selected_images: tuple[RHCOSImageInventory, ...],
        rpm_evidence: tuple[RpmCveEvidence, ...],
        dry_run: bool,
    ) -> bool:
        """
        Write an auditable Jira comment and request the VERIFIED transition.
        """
        selected_builds = ", ".join(f"{image.arch}={image.build_id}" for image in selected_images)
        comment_parts = [f"RHCOS RPM evidence for {bug.cve_id} in assembly {self._assembly}."]
        if verification:
            comment_parts.append(f"Latest verification comment created {verification.comment_created}.")
        if checks:
            rpm_checks = "; ".join(
                f"{check.required} -> {', '.join(str(rpm) for rpm in check.installed)}" for check in checks
            )
            comment_parts.append(f"Required-to-installed RPM matches: {rpm_checks}.")
        if rpm_evidence:
            rpm_matches = "; ".join(
                f"{item.rpm_type.name.lower()} ({item.rpm_name}) "
                f"{item.basis_nvr or '(absent from basis)'} -> {item.selected_nvr}; "
                f"new_changelog_match={item.new_changelog_match}; advisories="
                f"{','.join(item.advisory_urls) or 'none'}"
                for item in rpm_evidence
            )
            comment_parts.append(f"Changed RPM CVE evidence: {rpm_matches}.")
        comment_parts.append(f"Selected RHCOS image builds: {selected_builds}.")
        return self._tracker_repository.mark_verified(bug, " ".join(comment_parts), dry_run=dry_run)

    def _finding(
        self,
        bug: RHCOSTracker,
        disposition: TrackerDisposition,
        reason: str,
        verification: ParsedVerification | None = None,
        checks: tuple[RpmRequirementCheck, ...] = (),
        rpm_evidence: tuple[RpmCveEvidence, ...] = (),
        warning: str | None = None,
    ) -> TrackerFinding:
        """
        Create a domain finding for one tracker.
        """
        return TrackerFinding(
            issue_key=str(bug.id),
            cve_id=str(bug.cve_id or ""),
            component=str(bug.whiteboard_component or ""),
            status=str(bug.status or ""),
            disposition=disposition,
            reason=reason,
            verification_comment_created=verification.comment_created if verification else None,
            checks=checks,
            rpm_evidence=rpm_evidence,
            warning=warning,
        )

    def _report(
        self,
        target_releases: tuple[str, ...] = (),
        complete: bool = False,
        selected_images: tuple[RHCOSImageInventory, ...] = (),
        basis_images: tuple[RHCOSImageInventory, ...] = (),
        rpm_changes: tuple[RpmChange, ...] = (),
        build_evidence: tuple[RpmBuildEvidence, ...] = (),
        findings: tuple[TrackerFinding, ...] = (),
        warnings: list[str] | tuple[str, ...] = (),
        tracker_search_complete: bool = False,
    ) -> AnalysisReport:
        """
        Build the serialized result from the evidence gathered by this run.
        """
        return AnalysisReport(
            assembly=self._assembly,
            target_releases=target_releases,
            complete=complete,
            selected_images=selected_images,
            findings=findings,
            tracker_search_complete=tracker_search_complete,
            basis_images=basis_images,
            rpm_changes=rpm_changes,
            rpm_build_evidence=build_evidence,
            warnings=tuple(warnings),
        )
