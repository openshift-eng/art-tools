import logging
import os
import re
import ssl
from dataclasses import dataclass
from typing import Optional

import click
import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from elliottlib.cli.common import cli
from elliottlib.verify_common import (
    VerifyResultBase,
    get_assembly_shipment_url,
    handle_verify_result,
    render_verify_result,
    verify_output_option,
)

LOGGER = logging.getLogger(__name__)

PROW_JOB_URL = "https://prow.ci.openshift.org/prowjob?prowjob={job_id}"
GANGWAY_URL = "https://gangway-ci.apps.ci.l2s4.p1.openshiftapps.com/v1/executions/"
JOB_NAME_TEMPLATE = "periodic-ci-openshift-openshift-tests-private-release-{version}-stage-testing-e2e-aws-ipi"
PAYLOAD_TEMPLATE = "quay.io/openshift-release-dev/ocp-release:{assembly}-x86_64"

STAGE_TESTING_LABEL = "stage-testing-success"
TERMINAL_STATES = {"success", "failure", "aborted", "error"}
REQUEST_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class StageTestingResult(VerifyResultBase):
    job_id: str = ""
    job_name: str = ""
    state: str = ""
    url: Optional[str] = None
    start_time: Optional[str] = None
    completion_time: Optional[str] = None
    label_added: bool = False
    label_error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.state == "success" and self.label_error is None

    @property
    def failed(self) -> bool:
        return self.label_error is not None or (self.state in TERMINAL_STATES and self.state != "success")

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def pending(self) -> bool:
        return not self.terminal

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "state": self.state,
            "passed": self.passed,
            "terminal": self.terminal,
            "url": self.url,
            "start_time": self.start_time,
            "completion_time": self.completion_time,
            "label_added": self.label_added,
            **({"label_error": self.label_error} if self.label_error else {}),
        }

    def render_text(self) -> str:
        lines = [
            f"Stage testing: {self.state.upper()}",
            f"  Job ID: {self.job_id}",
            f"  Job name: {self.job_name}",
        ]
        if self.url:
            lines.append(f"  URL: {self.url}")
        if self.start_time:
            lines.append(f"  Started: {self.start_time}")
        if self.completion_time:
            lines.append(f"  Completed: {self.completion_time}")
        if self.label_added:
            lines.append(f"  Label '{STAGE_TESTING_LABEL}' added to MR")
        if self.label_error:
            lines.append(f"  Label update failed: {self.label_error}")
        return "\n".join(lines)


@dataclass
class TriggerResult(VerifyResultBase):
    job_id: str = ""
    job_name: str = ""

    @property
    def passed(self) -> bool:
        return bool(self.job_id)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
        }

    def render_text(self) -> str:
        return f"Stage testing: TRIGGERED\n  Job ID: {self.job_id}\n  Job name: {self.job_name}"


@dataclass
class LabelCheckResult(VerifyResultBase):
    label_found: bool = False
    mr_url: str = ""

    @property
    def passed(self) -> bool:
        return self.label_found

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "label": STAGE_TESTING_LABEL,
            "label_found": self.label_found,
            "mr_url": self.mr_url,
        }

    def render_text(self) -> str:
        status = "PASS" if self.label_found else "FAIL"
        return f"Stage testing label check: {status}\n  Label: {STAGE_TESTING_LABEL}\n  MR: {self.mr_url}"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _get_session() -> requests.Session:
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_gitlab_headers() -> dict:
    token = os.environ.get("GITLAB_TOKEN")
    if not token:
        raise click.ClickException("GITLAB_TOKEN environment variable is required for GitLab API")
    return {"PRIVATE-TOKEN": token.strip()}


# ---------------------------------------------------------------------------
# GitLab MR helpers
# ---------------------------------------------------------------------------


def _parse_gitlab_mr_url(mr_url: str) -> tuple[str, str, str]:
    match = re.match(r"(https?://[^/]+)/(.+?)/-/merge_requests/(\d+)", mr_url)
    if not match:
        raise click.ClickException(f"Cannot parse GitLab MR URL: {mr_url}")
    return match.group(1), match.group(2), match.group(3)


def _check_mr_label(mr_url: str) -> bool:
    base_url, project_path, mr_iid = _parse_gitlab_mr_url(mr_url)
    encoded_project = requests.utils.quote(project_path, safe="")
    api_url = f"{base_url}/api/v4/projects/{encoded_project}/merge_requests/{mr_iid}"

    response = _get_session().get(
        url=api_url,
        headers=_get_gitlab_headers(),
        timeout=REQUEST_TIMEOUT,
        verify=ssl.get_default_verify_paths().openssl_cafile,
    )
    if response.status_code != 200:
        raise click.ClickException(f"Failed to get MR info: HTTP {response.status_code} {response.reason}")

    labels = response.json().get("labels", [])
    return STAGE_TESTING_LABEL in labels


def _add_mr_label(mr_url: str) -> None:
    base_url, project_path, mr_iid = _parse_gitlab_mr_url(mr_url)
    encoded_project = requests.utils.quote(project_path, safe="")
    api_url = f"{base_url}/api/v4/projects/{encoded_project}/merge_requests/{mr_iid}"

    response = _get_session().put(
        url=api_url,
        headers=_get_gitlab_headers(),
        json={"add_labels": STAGE_TESTING_LABEL},
        timeout=REQUEST_TIMEOUT,
        verify=ssl.get_default_verify_paths().openssl_cafile,
    )
    if response.status_code != 200:
        raise click.ClickException(f"Failed to add label to MR: HTTP {response.status_code} {response.reason}")
    LOGGER.info("Added label '%s' to MR", STAGE_TESTING_LABEL)


# ---------------------------------------------------------------------------
# Prow job operations
# ---------------------------------------------------------------------------


def trigger_prow_job(version: str, assembly: str) -> tuple[str, str]:
    """Trigger a stage testing Prow job via Gangway API. Returns (job_id, job_name)."""
    job_name = JOB_NAME_TEMPLATE.format(version=version)
    payload_url = PAYLOAD_TEMPLATE.format(assembly=assembly)

    LOGGER.info("Triggering Prow job %s with payload %s", job_name, payload_url)

    token = os.environ.get("GANGWAY_TOKEN")
    if not token:
        raise click.ClickException("GANGWAY_TOKEN environment variable is required for Gangway API authentication")

    url = GANGWAY_URL + job_name
    data = {
        "job_execution_type": "1",
        "pod_spec_options": {
            "envs": {
                "RELEASE_IMAGE_LATEST": payload_url,
            }
        },
    }

    response = _get_session().post(
        url=url,
        json=data,
        headers={"Authorization": f"Bearer {token.strip()}"},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        raise click.ClickException(
            f"Failed to trigger Prow job '{job_name}': HTTP {response.status_code} {response.reason}"
        )

    job_id = response.json()["id"]
    LOGGER.info("Triggered Prow job, job ID: %s", job_id)
    return job_id, job_name


def get_job_status(job_id: str) -> StageTestingResult:
    """Check the status of a Prow job. Point-in-time, no polling.

    The Prow prowjob API is public and does not require authentication.
    """
    url = PROW_JOB_URL.format(job_id=job_id.strip())
    response = _get_session().get(url=url, timeout=REQUEST_TIMEOUT)

    if response.status_code != 200:
        raise click.ClickException(
            f"Failed to get Prow job status for {job_id}: HTTP {response.status_code} {response.reason}"
        )

    job_data = yaml.safe_load(response.text)
    if not job_data:
        raise click.ClickException(f"Empty response from Prow API for job {job_id}")

    status = job_data["status"]
    spec = job_data["spec"]

    return StageTestingResult(
        job_id=job_id,
        job_name=spec["job"],
        state=status.get("state", "unknown"),
        url=status.get("url"),
        start_time=status.get("startTime"),
        completion_time=status.get("completionTime"),
    )


# ---------------------------------------------------------------------------
# CLI: verify-stage-testing
# ---------------------------------------------------------------------------


@cli.command("verify-stage-testing", short_help="Verify stage testing status")
@click.option("--job-id", default=None, help="Check status of an existing Prow job. Adds label to MR on success.")
@click.option(
    "--trigger",
    is_flag=True,
    default=False,
    help="Trigger a new stage testing Prow job via Gangway API. Requires GANGWAY_TOKEN.",
)
@verify_output_option
@click.pass_obj
def verify_stage_testing_cli(runtime, job_id, trigger, output):
    """Verify stage testing status for an assembly.

    Without options, checks if the 'stage-testing-success' label exists
    on the shipment MR (resolved from assembly config in releases.yml).

    With --job-id, checks the Prow job status (public API, no auth needed).
    If the job succeeded, adds the 'stage-testing-success' label to the
    shipment MR.

    With --trigger, triggers a new stage testing Prow job via Gangway
    API and returns the job ID.

    Requires GITLAB_TOKEN for MR label operations.
    Requires GANGWAY_TOKEN for --trigger (Gangway API).

    Examples:

    \b
        # Check label on MR
        elliott -g openshift-4.22 --assembly 4.22.9 verify-stage-testing

    \b
        # Trigger a new job
        elliott -g openshift-4.22 --assembly 4.22.9 verify-stage-testing --trigger

    \b
        # Check existing job status
        elliott -g openshift-4.22 --assembly 4.22.9 verify-stage-testing --job-id abc123
    """
    if job_id and trigger:
        raise click.ClickException("--job-id and --trigger are mutually exclusive")

    runtime.initialize(no_group=False)

    if trigger:
        _handle_trigger(runtime, output)
    elif job_id:
        _handle_job_check(runtime, job_id, output)
    else:
        _handle_label_check(runtime, output)


def _handle_trigger(runtime, output):
    version = runtime.group.split("-")[-1]
    assembly = runtime.assembly
    job_id, job_name = trigger_prow_job(version, assembly)
    result = TriggerResult(job_id=job_id, job_name=job_name)
    handle_verify_result(result, output)


def _handle_job_check(runtime, job_id, output):
    result = get_job_status(job_id)

    if result.state == "success":
        try:
            mr_url = get_assembly_shipment_url(runtime, required=True)
            _add_mr_label(mr_url)
            result.label_added = True
        except Exception as exc:
            result.label_error = str(exc)
            LOGGER.error("Failed to add stage-testing label: %s", exc)

    click.echo(render_verify_result(result, output))

    # Exit on failed only, not on pending — artcd relies on rc=0 to continue polling
    if result.failed:
        raise SystemExit(1)


def _handle_label_check(runtime, output):
    mr_url = get_assembly_shipment_url(runtime, required=True)
    label_found = _check_mr_label(mr_url)
    result = LabelCheckResult(label_found=label_found, mr_url=mr_url)
    handle_verify_result(result, output)
