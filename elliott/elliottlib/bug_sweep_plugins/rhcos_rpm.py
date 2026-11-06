"""RHCOS RPM evidence eligibility plugin for release bug sweeps."""

import logging

from elliottlib.bug_sweep_plugins.contracts import (
    PluginContext,
    PluginDecision,
    PluginDisposition,
    PluginResult,
)
from elliottlib.bzutil import JIRABugTracker
from elliottlib.rhcos_rpm_evidence.build_evidence import KojiErrataRpmBuildEvidenceSource
from elliottlib.rhcos_rpm_evidence.inventory import RHCOSInventoryProvider
from elliottlib.rhcos_rpm_evidence.models import TrackerDisposition
from elliottlib.rhcos_rpm_evidence.repository import JiraRHCOSTrackerRepository
from elliottlib.rhcos_rpm_evidence.service import RHCOSRpmEvidenceService

LOGGER = logging.getLogger(__name__)


class RHCOSRpmEvidencePlugin:
    """Claim RHCOS CVE trackers and classify them using selected-image RPM evidence."""

    @property
    def name(self) -> str:
        """
        Return the stable plugin identifier accepted by find-bugs.
        """
        return "rhcos-rpm"

    def evaluate(self, context: PluginContext) -> PluginResult:
        """
        Run RHCOS evidence analysis without changing Jira tracker status.
        """
        if not isinstance(context.bug_tracker, JIRABugTracker):
            raise TypeError("The rhcos-rpm plugin requires the Jira bug tracker")

        LOGGER.info("Starting RHCOS RPM evidence discovery for assembly %s", context.runtime.assembly)
        service = RHCOSRpmEvidenceService(
            assembly=context.runtime.assembly,
            inventory_source=RHCOSInventoryProvider(context.runtime),
            build_evidence_source=KojiErrataRpmBuildEvidenceSource(context.runtime),
            tracker_repository=JiraRHCOSTrackerRepository(context.bug_tracker),
            excluded_issue_keys=context.excluded_issue_keys,
        )
        report = service.run()

        decisions = tuple(
            PluginDecision(
                issue_key=finding.issue_key,
                disposition=self._disposition_for(finding.disposition),
                reason=finding.reason,
            )
            for finding in report.findings
        )
        return PluginResult(
            plugin_name=self.name,
            decisions=decisions,
            domain_search_complete=report.tracker_search_complete,
            warnings=report.warnings,
        )

    @staticmethod
    def _disposition_for(disposition: TrackerDisposition) -> PluginDisposition:
        """
        Map RHCOS analysis outcomes onto the shared issue ownership contract.
        """
        if disposition in {
            TrackerDisposition.READY_TO_VERIFY,
            TrackerDisposition.WOULD_VERIFY,
            TrackerDisposition.VERIFIED_FROM_EVIDENCE,
            TrackerDisposition.ALREADY_VERIFIED,
        }:
            return PluginDisposition.INCLUDE
        if disposition in {
            TrackerDisposition.NEEDS_REVIEW,
            TrackerDisposition.EVIDENCE_INCOMPLETE,
            TrackerDisposition.STATUS_NOT_ELIGIBLE,
            TrackerDisposition.STATUS_UPDATE_FAILED,
        }:
            return PluginDisposition.REVIEW
        if disposition in {
            TrackerDisposition.EXCLUDED,
            TrackerDisposition.NO_EVIDENCE,
            TrackerDisposition.OBSERVED_IN_BASIS,
        }:
            return PluginDisposition.SUPPRESS
        raise ValueError(f"Unhandled RHCOS tracker disposition: {disposition}")
