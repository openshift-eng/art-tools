import asyncio
import json
import logging

import click
from artcommonlib import exectools

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 300
DEFAULT_TIMEOUT = 9000


class StageTestingPipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.group = f"openshift-{version}"
        self.data_path = constants.OCP_BUILD_DATA_URL
        self.poll_interval = poll_interval
        self.timeout = timeout

        self.working_dir = self.runtime.working_dir / "verify_stage_testing"
        self.working_dir.mkdir(parents=True, exist_ok=True)

    @property
    def _elliott_base(self) -> list[str]:
        return [
            "elliott",
            f"--group={self.group}",
            f"--assembly={self.assembly}",
            f"--data-path={self.data_path}",
            f"--working-dir={self.working_dir / 'elliott-working'}",
        ]

    async def run(self) -> dict:
        """Check label, trigger if needed, and poll until completion."""
        label_result = await self._check_label()

        if label_result["passed"]:
            click.echo("[verify-stage-testing] Stage testing already passed — label found on MR", err=True)
            return {"passed": True, "state": "success", "already_passed": True}

        trigger_result = await self._trigger()
        job_id = trigger_result["job_id"]
        click.echo(f"[verify-stage-testing] Triggered Prow job: {job_id}", err=True)

        result = await self._poll_job(job_id)

        if result["passed"]:
            click.echo("[verify-stage-testing] PASS — stage testing succeeded", err=True)
        else:
            click.echo(f"[verify-stage-testing] FAIL — state: {result['state']}", err=True)

        return result

    async def _check_label(self) -> dict:
        """Call elliott verify-stage-testing (label check) and parse JSON output."""
        cmd = self._elliott_base + [
            "verify-stage-testing",
            "-o",
            "json",
        ]

        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)

        if not stdout.strip():
            raise RuntimeError(f"elliott verify-stage-testing failed with rc={rc}: {stderr}")

        return json.loads(stdout)

    async def _trigger(self) -> dict:
        """Call elliott verify-stage-testing --trigger to trigger the Prow job."""
        cmd = self._elliott_base + [
            "verify-stage-testing",
            "--trigger",
            "-o",
            "json",
        ]

        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)

        if rc != 0 and not stdout.strip():
            raise RuntimeError(f"elliott verify-stage-testing --trigger failed with rc={rc}: {stderr}")

        return json.loads(stdout)

    async def _poll_job(self, job_id: str) -> dict:
        """Poll elliott verify-stage-testing --job-id until terminal state."""
        elapsed = 0

        while elapsed < self.timeout:
            result = await self._check_job(job_id)

            if result["terminal"]:
                return result

            url_info = f" url: {result['url']}" if result.get("url") else ""
            click.echo(
                f"[verify-stage-testing] Job {job_id} state: {result['state']}{url_info} "
                f"— polling again in {self.poll_interval}s ({elapsed}s/{self.timeout}s elapsed)",
                err=True,
            )
            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

        raise TimeoutError(f"Stage testing job {job_id} did not complete within {self.timeout}s")

    async def _check_job(self, job_id: str) -> dict:
        """Run elliott verify-stage-testing --job-id and parse JSON output."""
        cmd = self._elliott_base + [
            "verify-stage-testing",
            f"--job-id={job_id}",
            "-o",
            "json",
        ]

        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)

        if rc != 0 and not stdout.strip():
            raise RuntimeError(f"elliott verify-stage-testing failed with rc={rc}: {stderr}")

        return json.loads(stdout)


@cli.command("verify-stage-testing", short_help="Trigger and poll stage testing Prow job")
@click.option("--version", required=True, help="OCP version (e.g. 4.22)")
@click.option("--assembly", required=True, help="Assembly name (e.g. 4.22.9)")
@click.option(
    "--poll-interval",
    type=int,
    default=DEFAULT_POLL_INTERVAL,
    show_default=True,
    help="Seconds between status checks.",
)
@click.option(
    "--timeout",
    type=int,
    default=DEFAULT_TIMEOUT,
    show_default=True,
    help="Maximum seconds to wait for job completion.",
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@pass_runtime
@click_coroutine
async def verify_stage_testing_cli(
    runtime: Runtime,
    version: str,
    assembly: str,
    poll_interval: int,
    timeout: int,
    output: str,
):
    """Trigger a stage testing Prow job and poll until completion.

    Calls 'elliott verify-stage-testing --trigger' to trigger the job
    (skips if the stage-testing-success label is already present),
    then polls with 'elliott verify-stage-testing --job-id' until
    the job reaches a terminal state.

    Requires GANGWAY_TOKEN (Gangway) and GITLAB_TOKEN (label management)
    to be set in the environment.

    \b
    Example:
        artcd verify-stage-testing --version 4.22 --assembly 4.22.9
    """
    pipeline = StageTestingPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        poll_interval=poll_interval,
        timeout=timeout,
    )
    result = await pipeline.run()

    if output == "json":
        click.echo(json.dumps(result, indent=2))
    else:
        state = result.get("state", "unknown").upper()
        if result.get("already_passed"):
            click.echo("Stage testing: ALREADY PASSED (label found)")
        else:
            job_id = result.get("job_id", "unknown")
            click.echo(f"Stage testing: {state}")
            click.echo(f"  Job ID: {job_id}")
            if result.get("url"):
                click.echo(f"  URL: {result['url']}")
            if result.get("label_added"):
                click.echo("  Label 'stage-testing-success' added to MR")

    if not result.get("passed"):
        raise SystemExit(1)
