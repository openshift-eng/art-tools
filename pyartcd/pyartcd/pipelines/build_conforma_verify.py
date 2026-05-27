import asyncio
import json
import os
import sys
from typing import List, Optional

import click
from artcommonlib import exectools, logutil
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord, KonfluxRecord
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.constants import KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION
from elliottlib.cli.snapshot_cli import get_build_records_by_nvrs

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logutil.get_logger(__name__)

MAX_CONCURRENCY = 10


class BuildConformaVerifyPipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        builds: Optional[List[str]],
        fail_fast: bool,
        data_path: Optional[str] = None,
        data_gitref: Optional[str] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.builds = builds or []
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

        self.logger.info("Looking up build records for %d NVRs...", len(nvrs))
        elliott_runtime = self._init_elliott_runtime()
        nvr_record_map = await get_build_records_by_nvrs(elliott_runtime, nvrs)

        self.logger.info("Running EC verification for %d builds...", len(nvr_record_map))
        await self._verify_builds(nvr_record_map)

    def _init_elliott_runtime(self):
        """Initialize a lightweight Elliott Runtime for DB access."""
        from elliottlib.runtime import Runtime as ElliottRuntime

        elliott_runtime = ElliottRuntime()
        group_param = self.group
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'
        elliott_runtime.group = group_param
        elliott_runtime.assembly = self.assembly
        elliott_runtime.initialize(mode='images', build_system='konflux', data_path=self.data_path)
        return elliott_runtime

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

    async def _verify_builds(self, nvr_record_map: dict[str, KonfluxRecord]):
        kubeconfig = os.getenv("KONFLUX_SA_KUBECONFIG")
        konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=KONFLUX_DEFAULT_NAMESPACE,
            config_file=kubeconfig,
            context=None,
            dry_run=self.dry_run,
        )

        ec_policy = KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION
        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        results: dict[str, dict] = {}

        async def _verify_one(nvr: str, record: KonfluxRecord):
            async with semaphore:
                app_name = record.get_konflux_application_name()
                comp_name = record.get_konflux_component_name()
                self.logger.info("Verifying %s (component=%s)...", nvr, comp_name)

                ec_result = await konflux_client.verify_enterprise_contract(
                    namespace=KONFLUX_DEFAULT_NAMESPACE,
                    application_name=app_name,
                    component_name=comp_name,
                    image_pullspec=record.image_pullspec,
                    source_url=record.rebase_repo_url,
                    commit_sha=record.rebase_commitish,
                    ec_policy=ec_policy,
                    logger=self.logger.getChild(nvr),
                )

                status = "PASS" if not ec_result.ec_failed else "FAIL"
                results[nvr] = {
                    "status": status,
                    "plr_url": ec_result.ec_pipeline_url,
                }
                self.logger.info("%s: %s (PLR: %s)", nvr, status, ec_result.ec_pipeline_url)
                return ec_result

        if self.fail_fast:
            for nvr, record in nvr_record_map.items():
                ec_result = await _verify_one(nvr, record)
                if ec_result.ec_failed:
                    self.logger.error("Stopping early due to --fail-fast: %s failed", nvr)
                    break
        else:
            await asyncio.gather(*(_verify_one(nvr, record) for nvr, record in nvr_record_map.items()))

        failed = [nvr for nvr, r in results.items() if r["status"] == "FAIL"]
        passed = [nvr for nvr, r in results.items() if r["status"] == "PASS"]

        self.logger.info("=== EC Verification Summary ===")
        self.logger.info("Passed: %d / %d", len(passed), len(results))
        if failed:
            self.logger.error("Failed: %d / %d", len(failed), len(results))
            for nvr in failed:
                self.logger.error("  FAIL: %s  PLR: %s", nvr, results[nvr]["plr_url"])
            sys.exit(1)

        self.logger.info("All %d builds passed EC verification", len(passed))


@cli.command("build-conforma-verify", short_help="Run Conforma (EC) verification on OCP image builds")
@click.option("--version", required=True, help="OCP version (e.g. 4.18)")
@click.option("--assembly", required=True, default="stream", help="Assembly name")
@click.option("--data-path", default=None, help="ocp-build-data repo URL or path")
@click.option("--data-gitref", default=None, help="ocp-build-data git ref")
@click.option("--builds", default="", help="Comma-separated NVRs to verify (empty = fetch latest)")
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
    fail_fast: bool,
):
    builds_list = [b.strip() for b in builds.split(",") if b.strip()] if builds else []

    pipeline = BuildConformaVerifyPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        builds=builds_list,
        fail_fast=fail_fast,
        data_path=data_path,
        data_gitref=data_gitref,
    )
    await pipeline.run()
