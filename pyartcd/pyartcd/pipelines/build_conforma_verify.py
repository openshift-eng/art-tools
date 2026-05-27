import json
import os
import sys
from pathlib import Path
from typing import List, Optional

import click
from artcommonlib import exectools, logutil

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logutil.get_logger(__name__)


class BuildConformaVerifyPipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        builds: Optional[List[str]],
        env: str,
        batch_size: int,
        fail_fast: bool,
        data_path: Optional[str] = None,
        data_gitref: Optional[str] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.builds = builds or []
        self.env = env
        self.batch_size = batch_size
        self.fail_fast = fail_fast
        self.data_path = data_path or constants.OCP_BUILD_DATA_URL
        self.data_gitref = data_gitref
        self.dry_run = runtime.dry_run
        self.logger = LOGGER

        self.working_dir = self.runtime.working_dir / "conforma_verify"
        self.working_dir.mkdir(parents=True, exist_ok=True)

        self.group = f"openshift-{version}"

    @property
    def _elliott_base_command(self) -> list[str]:
        group_param = f'--group={self.group}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'

        return [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            f'--working-dir={self.working_dir}',
            f'--data-path={self.data_path}',
        ]

    async def run(self):
        nvrs = self.builds

        if not nvrs:
            self.logger.info("No builds provided; fetching latest image builds for assembly '%s'...", self.assembly)
            nvrs = await self._find_latest_builds()
            self.logger.info("Found %d image builds", len(nvrs))

        if not nvrs:
            raise RuntimeError("No builds found to verify")

        self.logger.info("Creating Snapshot from %d NVRs...", len(nvrs))
        await self._create_snapshot(nvrs)

        self.logger.info("Running Conforma verification on %d NVRs...", len(nvrs))
        await self._run_conforma(nvrs)

    async def _find_latest_builds(self) -> List[str]:
        cmd = self._elliott_base_command + [
            "find-builds",
            "--kind=image",
            "--all-image-types",
            "--json=-",
        ]
        _, stdout, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        if not stdout:
            self.logger.warning("No output from find-builds")
            return []

        data = json.loads(stdout)
        payload = data.get("payload", [])
        non_payload = data.get("non_payload", [])
        nvrs = sorted(set(payload + non_payload))
        self.logger.info("Found %d payload and %d non-payload builds", len(payload), len(non_payload))
        return nvrs

    async def _create_snapshot(self, nvrs: List[str]):
        nvrs_file = Path(self.working_dir) / "snapshot_nvrs.txt"
        nvrs_file.write_text("\n".join(nvrs) + "\n")

        cmd = self._elliott_base_command + [
            "snapshot",
            "new",
            f"--builds-file={nvrs_file}",
            "--apply",
        ]

        quay_auth_file = os.getenv("QUAY_AUTH_FILE")
        if quay_auth_file:
            cmd.append(f"--pull-secret={quay_auth_file}")

        job_url = os.getenv("BUILD_URL")
        if job_url:
            cmd.append(f"--job-url={job_url}")

        _, stdout, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        if stdout:
            self.logger.info("Snapshot created:\n%s", stdout)

    async def _run_conforma(self, nvrs: List[str]):
        nvrs_file = Path(self.working_dir) / "conforma_nvrs.txt"
        nvrs_file.write_text("\n".join(nvrs) + "\n")

        cmd = self._elliott_base_command + [
            "verify-conforma",
            f"--nvrs-file={nvrs_file}",
            f"--env={self.env}",
            f"--batch-size={self.batch_size}",
        ]
        if self.fail_fast:
            cmd.append("--fail-fast")

        konflux_kubeconfig = os.getenv("KONFLUX_SA_KUBECONFIG")
        if konflux_kubeconfig:
            cmd.append(f"--konflux-kubeconfig={konflux_kubeconfig}")

        pull_secret = os.getenv("QUAY_AUTH_FILE")
        if pull_secret:
            cmd.append(f"--pull-secret={pull_secret}")

        rc, stdout, _ = await exectools.cmd_gather_async(cmd, stderr=None, check=False)
        if stdout:
            self.logger.info("Conforma output:\n%s", stdout)

        if rc != 0:
            self.logger.error("Conforma verification failed (exit code %d)", rc)
            sys.exit(1)

        self.logger.info("Conforma verification passed")


@cli.command("build-conforma-verify", short_help="Run Conforma (EC) verification on OCP image builds")
@click.option("--version", required=True, help="OCP version (e.g. 4.18)")
@click.option("--assembly", required=True, default="stream", help="Assembly name")
@click.option("--data-path", default=None, help="ocp-build-data repo URL or path")
@click.option("--data-gitref", default=None, help="ocp-build-data git ref")
@click.option("--builds", default="", help="Comma-separated NVRs to verify (empty = fetch latest)")
@click.option("--env", type=click.Choice(["stage", "prod"]), default="prod", help="EC policy environment")
@click.option("--batch-size", type=int, default=10, help="NVRs per verification batch")
@click.option("--fail-fast", is_flag=True, default=False, help="Stop on first failure")
@pass_runtime
@click_coroutine
async def build_conforma_verify(
    runtime: Runtime,
    version: str,
    assembly: str,
    data_path: Optional[str],
    data_gitref: Optional[str],
    builds: str,
    env: str,
    batch_size: int,
    fail_fast: bool,
):
    builds_list = [b.strip() for b in builds.split(",") if b.strip()] if builds else []

    pipeline = BuildConformaVerifyPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        builds=builds_list,
        env=env,
        batch_size=batch_size,
        fail_fast=fail_fast,
        data_path=data_path,
        data_gitref=data_gitref,
    )
    await pipeline.run()
