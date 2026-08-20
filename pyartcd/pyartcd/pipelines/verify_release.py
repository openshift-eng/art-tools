import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import click
from artcommonlib import exectools

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)


class StepStatus(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class StepResult:
    name: str
    status: StepStatus
    message: str = ""


@dataclass
class VerifyReleaseResult:
    steps: list[StepResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(s.status in (StepStatus.PASS, StepStatus.SKIP) for s in self.steps)

    def summary(self) -> str:
        lines = []
        for s in self.steps:
            line = f"{s.name}: {s.status.value}"
            if s.message:
                line += f" — {s.message}"
            lines.append(line)
        return "\n".join(lines)


def render_result(result: VerifyReleaseResult, output: str, version: str, assembly: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "version": version,
                "assembly": assembly,
                "passed": result.passed,
                "steps": [
                    {
                        "name": s.name,
                        "status": s.status.value,
                        "message": s.message,
                    }
                    for s in result.steps
                ],
            },
            indent=2,
        )

    lines = [f"Verify release {assembly}", ""]
    for s in result.steps:
        lines.append(f"  {s.name}: {s.status.value}")
    lines.append("")
    overall = "PASS" if result.passed else "FAIL"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)


class VerifyReleasePipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        skip_steps: Optional[list[str]] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.group = f"openshift-{version}"
        self.data_path = constants.OCP_BUILD_DATA_URL
        self.skip_steps = set(skip_steps or [])

        self.working_dir = self.runtime.working_dir / "verify_release"
        self.working_dir.mkdir(parents=True, exist_ok=True)

        self._elliott_env = os.environ.copy()

    @property
    def _elliott_base(self) -> list[str]:
        return [
            "elliott",
            f"--group={self.group}",
            f"--assembly={self.assembly}",
            f"--data-path={self.data_path}",
            f"--working-dir={self.working_dir / 'elliott-working'}",
        ]

    async def run(self) -> VerifyReleaseResult:
        result = VerifyReleaseResult()

        # All steps can run in parallel
        steps = [
            ("cdn-push", self._verify_cdn_push),
            ("signatures", self._verify_signatures),
            ("image-grades", self._verify_image_grades),
            ("payload", self._verify_payload),
            ("qe-qualifier", self._verify_qe_qualifier),
            ("security-alerts", self._verify_security_alerts),
            ("kernel-tag", self._verify_kernel_tag),
            ("cve-trackers", self._verify_cve_trackers),
        ]

        # Build task list (skip user-requested skips)
        tasks = []
        for step_name, step_fn in steps:
            if step_name in self.skip_steps:
                result.steps.append(StepResult(step_name, StepStatus.SKIP, "skipped by user"))
                click.echo(f"[verify-release: {step_name}] SKIP (skipped by user)", err=True)
            else:
                tasks.append(self._safe_run_step(step_name, step_fn))

        # Run all tasks in parallel
        if tasks:
            click.echo(f"[verify-release] Running {len(tasks)} steps in parallel", err=True)
            step_results = await asyncio.gather(*tasks)

            # Process results (individual steps already logged their status)
            for step_result in step_results:
                result.steps.append(step_result)

        return result

    async def _safe_run_step(self, step_name: str, step_fn):
        """Wrap step execution to catch all exceptions and return FAIL instead."""
        try:
            return await step_fn()
        except Exception as e:
            click.echo(f"[verify-release: {step_name}] ✗ FAIL: {e}", err=True)
            return StepResult(step_name, StepStatus.FAIL, str(e))

    async def _run_elliott_cmd(self, step_name: str, cmd: list[str]) -> StepResult:
        """Run elliott command with live output streaming."""
        click.echo(f"[verify-release: {step_name}] Starting...", err=True)

        # Use step-specific working directory to avoid race conditions in parallel execution
        step_working_dir = self.working_dir / f"elliott-working-{step_name}"
        step_env = self._elliott_env.copy()
        step_env["ELLIOTT_WORKING_DIR"] = str(step_working_dir)

        # Replace --working-dir argument in command
        cmd_with_step_dir = []
        for i, arg in enumerate(cmd):
            if arg.startswith("--working-dir="):
                cmd_with_step_dir.append(f"--working-dir={step_working_dir}")
            else:
                cmd_with_step_dir.append(arg)

        try:
            await exectools.cmd_assert_async(cmd_with_step_dir, env=step_env)
            click.echo(f"[verify-release: {step_name}] ✓ PASS", err=True)
            return StepResult(step_name, StepStatus.PASS)
        except Exception as e:
            click.echo(f"[verify-release: {step_name}] ✗ FAIL", err=True)
            return StepResult(step_name, StepStatus.FAIL, str(e))

    async def _verify_cdn_push(self) -> StepResult:
        cmd = self._elliott_base + ["verify-cdn-push", "--no-push"]
        return await self._run_elliott_cmd("cdn-push", cmd)

    async def _verify_signatures(self) -> StepResult:
        cmd = self._elliott_base + ["verify-signatures"]
        return await self._run_elliott_cmd("signatures", cmd)

    async def _verify_image_grades(self) -> StepResult:
        cmd = self._elliott_base + ["verify-image-grades"]
        return await self._run_elliott_cmd("image-grades", cmd)

    async def _verify_payload(self) -> StepResult:
        imagestream = f"ocp/{self.version}-art-assembly-{self.assembly}"
        cmd = self._elliott_base + ["verify-payload", imagestream]
        return await self._run_elliott_cmd("payload", cmd)

    async def _verify_qe_qualifier(self) -> StepResult:
        cmd = self._elliott_base + ["verify-qe-qualifier"]
        return await self._run_elliott_cmd("qe-qualifier", cmd)

    async def _verify_security_alerts(self) -> StepResult:
        cmd = self._elliott_base + ["verify-security-alerts"]
        return await self._run_elliott_cmd("security-alerts", cmd)

    async def _verify_kernel_tag(self) -> StepResult:
        cmd = self._elliott_base + ["verify-kernel-tag"]
        return await self._run_elliott_cmd("kernel-tag", cmd)

    async def _verify_cve_trackers(self) -> StepResult:
        cmd = self._elliott_base + ["verify-cve-trackers"]
        return await self._run_elliott_cmd("cve-trackers", cmd)


@cli.command("verify-release", short_help="Run post-release verification checks")
@click.option("--version", required=True, help="OCP version (e.g. 4.19)")
@click.option("--assembly", required=True, help="Assembly name (e.g. 4.19.42)")
@click.option(
    "--skip",
    "skip_steps",
    multiple=True,
    help="Steps to skip (cdn-push, signatures, image-grades, payload, qe-qualifier, security-alerts, kernel-tag, cve-trackers)",
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
async def verify_release_cli(
    runtime: Runtime,
    version: str,
    assembly: str,
    skip_steps: tuple[str, ...],
    output: str,
):
    pipeline = VerifyReleasePipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        skip_steps=list(skip_steps),
    )
    result = await pipeline.run()
    click.echo(render_result(result, output, version, assembly))
    if not result.passed:
        sys.exit(1)
