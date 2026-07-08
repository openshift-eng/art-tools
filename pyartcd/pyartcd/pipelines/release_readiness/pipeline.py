"""
Release readiness pipeline orchestrator.

Wires together independent check modules and produces a ReadinessReport.
"""

import logging
from datetime import datetime, timezone

from artcommonlib.util import isolate_major_minor_in_group

from pyartcd.pipelines.release_readiness.checks import (
    check_blocker_bugs,
    check_build_failures,
    check_build_sync,
    check_bundle_fbc_coverage,
    check_nightly_status,
)
from pyartcd.pipelines.release_readiness.dev_cut_off import get_next_dev_cut_off
from pyartcd.pipelines.release_readiness.helpers import worst_status
from pyartcd.pipelines.release_readiness.models import ReadinessReport
from pyartcd.runtime import Runtime

_LOGGER = logging.getLogger(__name__)


class ReleaseReadinessPipeline:
    """
    Runs release readiness checks for a single OCP version.
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        build_system: str,
    ):
        self.runtime = runtime
        self.group = group
        self.build_system = build_system
        self._doozer_working = str(runtime.working_dir / f"doozer_working-{group}")

        major, minor = isolate_major_minor_in_group(group)
        self._ocp_version = f"{major}.{minor}"
        self._major = major
        self._minor = minor

    async def run(self) -> ReadinessReport:
        """
        Run all readiness checks and return the report.
        """

        _LOGGER.info("Checking release readiness for %s", self.group)

        dev_cut_off = await get_next_dev_cut_off(self.group, self._ocp_version)

        checks = [
            await check_nightly_status(self._major, self._minor, self.build_system),
            await check_blocker_bugs(self.group, self._doozer_working),
            await check_build_failures(self.group, self.build_system, self._doozer_working),
            await check_build_sync(self._ocp_version),
            await check_bundle_fbc_coverage(self.group, self.build_system, self._doozer_working),
        ]

        overall = worst_status([c.status for c in checks])

        return ReadinessReport(
            group=self.group,
            timestamp=datetime.now(timezone.utc).isoformat(),
            overall_status=overall,
            overall_emoji=overall.overall_emoji,
            checks=checks,
            dev_cut_off=dev_cut_off,
        )
