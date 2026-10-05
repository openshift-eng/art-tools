import asyncio
import json
import logging

import click
from artcommonlib import exectools

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 120
DEFAULT_TIMEOUT = 7200


class VerifyCdnPushPipeline:
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

        self.working_dir = self.runtime.working_dir / "verify_cdn_push"
        self.working_dir.mkdir(parents=True, exist_ok=True)

    @property
    def _elliott_cmd(self) -> list[str]:
        return [
            "elliott",
            f"--group={self.group}",
            f"--assembly={self.assembly}",
            f"--data-path={self.data_path}",
            f"--working-dir={self.working_dir / 'elliott-working'}",
            "verify-cdn-push",
            "--push",
            "-o",
            "json",
        ]

    async def run(self) -> dict:
        elapsed = 0

        while True:
            if elapsed >= self.timeout:
                LOGGER.error("Timeout reached (%ds) waiting for CDN staging pushes", self.timeout)
                raise TimeoutError(f"CDN staging push did not complete within {self.timeout}s")

            LOGGER.info("Running elliott verify-cdn-push (elapsed %ds)", elapsed)
            result = await self._run_elliott()

            if result["passed"]:
                LOGGER.info("All CDN staging pushes complete")
                return result

            if result.get("failed"):
                LOGGER.error("CDN staging push failed")
                return result

            advisories = result.get("advisories", [])
            if not any(a.get("pending") for a in advisories):
                LOGGER.error("No pending advisories but result is not passed or failed: %s", result)
                raise RuntimeError("Unexpected state: no pending advisories")

            LOGGER.info(
                "CDN staging pushes still pending, waiting %ds before next check",
                self.poll_interval,
            )
            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

    async def _run_elliott(self) -> dict:
        cmd = self._elliott_cmd
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)
        if rc != 0:
            LOGGER.warning("elliott verify-cdn-push exited with rc=%s", rc)

        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            LOGGER.error("Failed to parse elliott output: %s", stdout[:500])
            raise


@cli.command("verify-cdn-push", short_help="Push advisories to CDN staging and poll until complete")
@click.option("--version", required=True, help="OCP version (e.g. 4.19)")
@click.option("--assembly", required=True, help="Assembly name (e.g. 4.19.42)")
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
    help="Maximum seconds to wait for all pushes to complete.",
)
@pass_runtime
@click_coroutine
async def verify_cdn_push_cli(
    runtime: Runtime,
    version: str,
    assembly: str,
    poll_interval: int,
    timeout: int,
):
    """Push advisories to CDN staging and poll until all pushes complete.

    Wraps `elliott verify-cdn-push --push` in a polling loop. Triggers CDN
    staging push for release advisories (rpm, rhcos types) and waits until
    all push jobs reach COMPLETE status or the timeout is reached.

    Handles blocking/dependency advisories automatically — advisories that
    depend on others being pushed first are retried on each poll iteration.
    """
    pipeline = VerifyCdnPushPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        poll_interval=poll_interval,
        timeout=timeout,
    )
    result = await pipeline.run()
    click.echo(json.dumps(result, indent=2))
    if not result.get("passed"):
        raise SystemExit(1)
