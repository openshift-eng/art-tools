"""Verify Errata Tool advisory states and optionally label a shipment MR."""

import logging
from dataclasses import asdict, dataclass, field

import click
from artcommonlib.gitlab import GitLabClient

from elliottlib import constants
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.verify_common import (
    VerifyResultBase,
    get_assembly_shipment_url,
    get_ocp_release_advisory_ids,
    handle_verify_result,
    verify_output_option,
)

LOGGER = logging.getLogger(__name__)
ADVISORY_TRANSITION_SUCCESS_LABEL = "advisory-transition-success"
POST_QE_STATES = ("REL_PREP", "PUSH_READY", "IN_PUSH", "SHIPPED_LIVE")


@dataclass
class AdvisoryStatusResult:
    advisory_id: int
    status: str | None = None
    skipped: bool = False
    error: str | None = None

    @property
    def passed(self) -> bool:
        return (self.skipped or self.status in POST_QE_STATES) and not self.error


@dataclass
class VerifyAdvisoryStatusResult(VerifyResultBase):
    advisories: list[AdvisoryStatusResult] = field(default_factory=list)
    mr_url: str | None = None
    label: str | None = None
    label_added: bool = False
    dry_run: bool = False
    error: str | None = None

    @property
    def passed(self) -> bool:
        return bool(self.advisories) and all(a.passed for a in self.advisories) and not self.error

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "advisories": [dict(asdict(a), passed=a.passed) for a in self.advisories],
            "mr_url": self.mr_url,
            "label": self.label,
            "label_added": self.label_added,
            "dry_run": self.dry_run,
            "error": self.error,
        }

    def render_text(self) -> str:
        lines = ["Advisory status verification"]
        for a in self.advisories:
            outcome = "SKIP" if a.skipped else ("PASS" if a.passed else "FAIL")
            lines.append(f"  {a.advisory_id}: {outcome} ({a.status or 'unknown'})")
            if a.error:
                lines.append(f"    {a.error}")
        if self.label:
            action = "added" if self.label_added else ("would add" if self.dry_run and self.passed else "not added")
            lines.append(f"  Label {self.label}: {action} ({self.mr_url})")
        if self.error:
            lines.append(f"  Error: {self.error}")
        lines.append(f"Overall: {'PASS' if self.passed else 'FAIL'}")
        return "\n".join(lines)


async def verify_advisory_status(advisory_ids: list[int]) -> VerifyAdvisoryStatusResult:
    result = VerifyAdvisoryStatusResult()
    async with AsyncErrataAPI() as api:
        for advisory_id in dict.fromkeys(advisory_ids):
            advisory = AdvisoryStatusResult(advisory_id=advisory_id)
            try:
                advisory.status = await api.get_advisory_state(advisory_id)
                if not advisory.status:
                    advisory.error = "Unable to determine advisory status"
                elif advisory.status == constants.errata_dropped_advisory_label:
                    advisory.skipped = True
                elif advisory.status not in constants.errata_states:
                    advisory.error = f"Unknown advisory status: {advisory.status}"
            except Exception as exc:
                advisory.error = str(exc)
            result.advisories.append(advisory)
    return result


@cli.command("verify-advisory-status", short_help="Verify advisory states and optionally label a shipment MR")
@click.option(
    "--mark",
    is_flag=True,
    help="Add advisory-transition-success after all active advisories have passed QE.",
)
@click.option("--dry-run", is_flag=True, help="Check actual advisory states without adding the label.")
@verify_output_option
@click.pass_obj
@click_coroutine
async def verify_advisory_status_cli(runtime, mark, dry_run, output):
    """Verify that assembly advisories have progressed beyond QE.

    Resolve advisory IDs from the assembly build-data configuration, excluding
    MicroShift through the shared verify helper. REL_PREP, PUSH_READY, IN_PUSH
    and SHIPPED_LIVE pass. DROPPED_NO_SHIP is skipped. NEW_FILES, QE, unknown
    states and API errors fail. With --mark, add advisory-transition-success only
    after all checks pass. Resolve the MR URL from the assembly configuration;
    closed and merged MRs are supported. Dry runs still verify actual states.

    Examples:

    \b
        elliott -g openshift-4.22 --assembly 4.22.9 verify-advisory-status
        elliott -g openshift-4.22 --assembly 4.22.9 verify-advisory-status --mark
    """
    if not runtime.group:
        raise click.UsageError("A group/assembly configuration is required")
    runtime.dry_run = dry_run
    runtime.initialize(no_group=False)
    advisory_ids = tuple(get_ocp_release_advisory_ids(runtime).values())
    if not advisory_ids or any(a <= 0 for a in advisory_ids):
        raise click.UsageError("No valid advisory IDs selected")
    # Resolve the label destination before any checks, so configuration errors
    # cannot leave callers guessing whether verification or marking failed.
    shipment_url = get_assembly_shipment_url(runtime, required=True) if mark else None
    result = await verify_advisory_status(list(advisory_ids))
    if mark:
        result.mr_url = shipment_url
        result.label = ADVISORY_TRANSITION_SUCCESS_LABEL
        result.dry_run = dry_run
        if result.passed:
            if dry_run:
                LOGGER.info("[DRY-RUN] Would add %s to %s", ADVISORY_TRANSITION_SUCCESS_LABEL, shipment_url)
            else:
                try:
                    client = GitLabClient.from_url(shipment_url)
                    client.add_mr_labels(shipment_url, [ADVISORY_TRANSITION_SUCCESS_LABEL])
                    result.label_added = True
                except Exception as exc:
                    result.error = str(exc)
    handle_verify_result(result, output)
