"""Check or update release verification outcome labels on a shipment MR."""

import logging
from dataclasses import dataclass
from typing import Optional

import click
from artcommonlib.gitlab import GitLabClient

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.verify_common import (
    VerifyResultBase,
    get_assembly_shipment_url,
    handle_verify_result,
    verify_output_option,
)

LOGGER = logging.getLogger(__name__)

VERIFY_RELEASE_SUCCESS_LABEL = "verify-release-success"
VERIFY_RELEASE_FAIL_LABEL = "verify-release-fail"


@dataclass
class LabelCheckResult(VerifyResultBase):
    state: str = "not_verified"
    mr_url: str = ""

    @property
    def passed(self) -> bool:
        return self.state == "verified"

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "state": self.state,
            "mr_url": self.mr_url,
        }

    def render_text(self) -> str:
        status = "PASS" if self.passed else ("FAIL" if self.state == "failed" else "NOT VERIFIED")
        return f"Release verification: {status}\n  MR: {self.mr_url}"


@dataclass
class MarkResult(VerifyResultBase):
    mr_url: str = ""
    label_added: bool = False
    label: str = VERIFY_RELEASE_SUCCESS_LABEL
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.label_added

    def to_dict(self) -> dict:
        return {
            "mr_url": self.mr_url,
            "label": self.label,
            "label_added": self.label_added,
            "passed": self.passed,
            "error": self.error,
        }

    def render_text(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        line = f"Mark release verification ({self.label}): {status}"
        if self.error:
            line += f" — {self.error}"
        if self.mr_url:
            line += f"\n  MR: {self.mr_url}"
        return line


# ---------------------------------------------------------------------------
# CLI: verify-release
# ---------------------------------------------------------------------------


@cli.command("verify-release", short_help="Check or mark release verification on shipment MR")
@click.option(
    "--mark",
    is_flag=True,
    default=False,
    help="Add verify-release-success label to the shipment MR.",
)
@click.option(
    "--mark-failed", is_flag=True, help="Record a failed verify-release attempt and remove the success label."
)
@click.option("--dry-run", is_flag=True, default=False, help="Do not update the shipment MR.")
@verify_output_option
@click.pass_obj
@click_coroutine
async def verify_release_cli(runtime, mark, mark_failed, dry_run, output):
    """Check or update release verification outcome labels on a shipment MR.

    Without options, report verified, failed or not_verified from the MR labels.

    With --mark, adds the 'verify-release-success' label to the MR.
    Intended to run after all verify-release checks have passed; removes fail.
    With --mark-failed, adds verify-release-fail and removes success. The two
    marking options are mutually exclusive. Marking reports the label operation,
    while calling without flags only reads the existing outcome.

    Requires GITLAB_TOKEN for GitLab API operations.

    Examples:

    \b
        # Check label status
        elliott -g openshift-4.18 --assembly 4.18.51 verify-release

    \b
        # Mark as verified
        elliott -g openshift-4.18 --assembly 4.18.51 verify-release --mark

    \b
        # Record an unsuccessful attempt
        elliott -g openshift-4.18 --assembly 4.18.51 verify-release --mark-failed
    """
    if mark and mark_failed:
        raise click.UsageError("--mark and --mark-failed are mutually exclusive")
    runtime.dry_run = dry_run
    runtime.initialize(no_group=False)

    mr_url = get_assembly_shipment_url(runtime, required=True)

    if mark or mark_failed:
        _handle_mark(runtime, mr_url, output, failed=mark_failed)
    else:
        _handle_label_check(mr_url, output)


def _handle_label_check(mr_url: str, output: str) -> None:
    gitlab_client = GitLabClient.from_url(mr_url)
    mr = gitlab_client.get_mr_from_url(mr_url)
    labels = set(mr.labels or [])
    state = (
        "verified"
        if VERIFY_RELEASE_SUCCESS_LABEL in labels
        else ("failed" if VERIFY_RELEASE_FAIL_LABEL in labels else "not_verified")
    )
    result = LabelCheckResult(state=state, mr_url=mr_url)
    handle_verify_result(result, output)


def _handle_mark(runtime, mr_url: str, output: str, *, failed: bool = False) -> None:
    label = VERIFY_RELEASE_FAIL_LABEL if failed else VERIFY_RELEASE_SUCCESS_LABEL
    previous_label = VERIFY_RELEASE_SUCCESS_LABEL if failed else VERIFY_RELEASE_FAIL_LABEL
    result = MarkResult(mr_url=mr_url, label=label)

    if runtime.dry_run:
        LOGGER.info("[DRY-RUN] Would add %s and remove %s", label, previous_label)
        result.label_added = True
    else:
        try:
            gitlab_client = GitLabClient.from_url(mr_url)
            gitlab_client.add_mr_labels(mr_url, [label], remove_labels=[previous_label])
            LOGGER.info("Added label %s and removed %s", label, previous_label)
            result.label_added = True
        except Exception as exc:
            result.error = str(exc)
            LOGGER.error("Failed to add label to shipment MR: %s", exc)

    handle_verify_result(result, output)
