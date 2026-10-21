"""Check or update the verify-release-success label on a shipment MR."""

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
        status = "PASS" if self.passed else "NOT VERIFIED"
        return f"Release verification: {status}\n  MR: {self.mr_url}"


@dataclass
class MarkResult(VerifyResultBase):
    mr_url: str = ""
    label_added: bool = False
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.label_added

    def to_dict(self) -> dict:
        return {
            "mr_url": self.mr_url,
            "label": VERIFY_RELEASE_SUCCESS_LABEL,
            "label_added": self.label_added,
            "passed": self.passed,
            "error": self.error,
        }

    def render_text(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        line = f"Mark release verified: {status}"
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
@verify_output_option
@click.pass_obj
@click_coroutine
async def verify_release_cli(runtime, mark, output):
    """Check or update the verify-release-success label on a shipment MR.

    Without options, checks whether the shipment MR has the
    'verify-release-success' label and reports the state.

    With --mark, adds the 'verify-release-success' label to the MR.
    Intended to run after all verify-release checks have passed.

    Requires GITLAB_TOKEN for GitLab API operations.

    Examples:

    \b
        # Check label status
        elliott -g openshift-4.18 --assembly 4.18.51 verify-release

    \b
        # Mark as verified
        elliott -g openshift-4.18 --assembly 4.18.51 verify-release --mark
    """
    runtime.initialize(no_group=False)

    mr_url = get_assembly_shipment_url(runtime, required=True)

    if mark:
        _handle_mark(runtime, mr_url, output)
    else:
        _handle_label_check(mr_url, output)


def _handle_label_check(mr_url: str, output: str) -> None:
    gitlab_client = GitLabClient.from_url(mr_url)
    mr = gitlab_client.get_mr_from_url(mr_url)
    state = "verified" if VERIFY_RELEASE_SUCCESS_LABEL in (mr.labels or []) else "not_verified"
    result = LabelCheckResult(state=state, mr_url=mr_url)
    handle_verify_result(result, output)


def _handle_mark(runtime, mr_url: str, output: str) -> None:
    result = MarkResult(mr_url=mr_url)

    if runtime.dry_run:
        LOGGER.info("[DRY-RUN] Would add label '%s' to %s", VERIFY_RELEASE_SUCCESS_LABEL, mr_url)
        result.label_added = True
    else:
        try:
            gitlab_client = GitLabClient.from_url(mr_url)
            project_path, mr_iid = GitLabClient._parse_mr_url(mr_url)
            project = gitlab_client.get_project(project_path)
            project.mergerequests.update(int(mr_iid), add_labels=VERIFY_RELEASE_SUCCESS_LABEL)
            LOGGER.info("Added label '%s' to MR %s", VERIFY_RELEASE_SUCCESS_LABEL, mr_url)
            result.label_added = True
        except Exception as exc:
            result.error = str(exc)
            LOGGER.error("Failed to add label to shipment MR: %s", exc)

    handle_verify_result(result, output)
