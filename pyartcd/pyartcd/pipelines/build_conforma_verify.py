import asyncio
import json
import os
import re
import sys
from collections import defaultdict
from typing import List, Optional

import click
from artcommonlib import exectools, logutil
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildRecord, KonfluxRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from doozerlib.backend.konflux_client import KonfluxClient, get_common_runtime_watcher_labels
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo
from doozerlib.constants import (
    KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION,
    KONFLUX_EC_PIPELINE_GIT_URL,
    KONFLUX_EC_PIPELINE_PATH,
    KONFLUX_EC_PIPELINE_REVISION,
)

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logutil.get_logger(__name__)

BATCH_SIZE = 10


class BuildConformaVerifyPipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        builds: Optional[List[str]],
        data_path: Optional[str] = None,
        data_gitref: Optional[str] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.builds = builds or []
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
        nvr_record_map = await self._lookup_build_records(nvrs)

        self.logger.info("Running EC verification for %d builds...", len(nvr_record_map))
        await self._verify_builds(nvr_record_map)

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

    async def _lookup_build_records(self, nvrs: List[str]) -> dict[str, KonfluxRecord]:
        db = KonfluxDb()
        db.bind(KonfluxBuildRecord)

        where = {"group": self.group, "engine": Engine.KONFLUX.value}
        records = await db.get_build_records_by_nvrs(
            nvrs,
            where=where,
            strict=True,
            exclude_large_columns=True,
        )
        return {str(record.nvr): record for record in records if record is not None}

    async def _verify_builds(self, nvr_record_map: dict[str, KonfluxRecord]):
        from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome

        kubeconfig = os.getenv("KONFLUX_SA_KUBECONFIG")
        konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=KONFLUX_DEFAULT_NAMESPACE,
            config_file=kubeconfig,
            context=None,
            dry_run=self.dry_run,
        )

        ec_policy = KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION
        records = list(nvr_record_map.values())
        application_name = records[0].get_konflux_application_name()

        components = [
            {
                "name": record.get_konflux_component_name(),
                "containerImage": record.image_pullspec,
                "source": {
                    "git": {
                        "url": record.rebase_repo_url,
                        "revision": record.rebase_commitish,
                    }
                },
            }
            for record in records
        ]

        # Ensure IntegrationTestScenario exists
        policy_suffix = ec_policy.split('/')[-1]
        its_name = f"{application_name}-ec-{policy_suffix}"
        self.logger.info("Ensuring IntegrationTestScenario %s exists...", its_name)
        await konflux_client.ensure_integration_test_scenario(
            name=its_name,
            application_name=application_name,
            policy_configuration=ec_policy,
        )

        # Split into batches
        batches = [components[i : i + BATCH_SIZE] for i in range(0, len(components), BATCH_SIZE)]
        total_batches = len(batches)
        self.logger.info("Split %d components into %d batches of up to %d", len(components), total_batches, BATCH_SIZE)

        if self.dry_run:
            for batch_idx in range(1, total_batches + 1):
                self.logger.warning("[DRY RUN] Would have created EC PipelineRun for batch %d", batch_idx)
            self.logger.info("All %d components passed EC verification (dry run)", len(components))
            return

        # Create all PipelineRuns up front
        batch_plrs: list[tuple[int, list[dict], str]] = []  # (batch_idx, batch, plr_name)
        for batch_idx, batch in enumerate(batches, start=1):
            self.logger.info("=== Creating batch %d/%d (%d components) ===", batch_idx, total_batches, len(batch))

            snapshot_spec = {"application": application_name, "components": batch}
            snapshot_json = json.dumps(snapshot_spec)

            generate_name = f"{application_name}-ec-verify-{batch_idx}-"
            if len(generate_name) > 248:
                generate_name = generate_name[:248]

            watch_labels = get_common_runtime_watcher_labels()
            labels = {
                "appstudio.openshift.io/application": application_name,
                "test.appstudio.openshift.io/scenario": its_name,
                "kueue.x-k8s.io/priority-class": "build-priority-2",
            }
            labels.update(watch_labels)

            manifest = {
                "apiVersion": "tekton.dev/v1",
                "kind": "PipelineRun",
                "metadata": {
                    "generateName": generate_name,
                    "namespace": KONFLUX_DEFAULT_NAMESPACE,
                    "labels": labels,
                    "annotations": {
                        "test.appstudio.openshift.io/kind": "enterprise-contract",
                        "art-jenkins-job-url": os.getenv("BUILD_URL", "n/a"),
                    },
                },
                "spec": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {"name": "url", "value": KONFLUX_EC_PIPELINE_GIT_URL},
                            {"name": "revision", "value": KONFLUX_EC_PIPELINE_REVISION},
                            {"name": "pathInRepo", "value": KONFLUX_EC_PIPELINE_PATH},
                        ],
                    },
                    "params": [
                        {"name": "POLICY_CONFIGURATION", "value": ec_policy},
                        {"name": "SINGLE_COMPONENT", "value": "false"},
                        {"name": "SNAPSHOT", "value": snapshot_json},
                    ],
                    "taskRunTemplate": {
                        "serviceAccountName": f"build-pipeline-{batch[0]['name']}",
                    },
                    "timeouts": {
                        "pipeline": "1h",
                    },
                },
            }

            plr = await konflux_client._create(manifest)
            plr_name = plr.metadata.name
            plr_url = KonfluxClient.resource_url(plr.to_dict())
            self.logger.info("Created EC PipelineRun for batch %d: %s", batch_idx, plr_url)
            batch_plrs.append((batch_idx, batch, plr_name))

        self.logger.info("All %d batches submitted, waiting for completion in parallel...", total_batches)

        # Wait for all batches in parallel
        async def _wait_for_batch(batch_idx: int, batch: list[dict], plr_name: str) -> dict:
            plr_info = await konflux_client.wait_for_pipelinerun(plr_name, namespace=KONFLUX_DEFAULT_NAMESPACE)
            plr_url = KonfluxClient.resource_url(plr_info.to_dict())
            condition = plr_info.find_condition("Succeeded")
            outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(condition)
            if outcome is not KonfluxBuildOutcome.SUCCESS:
                self.logger.error("Batch %d FAILED. PLR: %s", batch_idx, plr_url)
                violations = self._extract_violations_from_plr(plr_info, batch, konflux_client)
                return {"batch": batch_idx, "plr_url": plr_url, "passed": False, "violations": violations}
            self.logger.info("Batch %d PASSED. PLR: %s", batch_idx, plr_url)
            return {"batch": batch_idx, "plr_url": plr_url, "passed": True, "count": len(batch)}

        results = await asyncio.gather(*[_wait_for_batch(idx, batch, name) for idx, batch, name in batch_plrs])

        failed_batches = [r for r in results if not r["passed"]]
        passed_count = sum(r["count"] for r in results if r["passed"])

        self.logger.info("=== EC Verification Summary ===")
        self.logger.info(
            "Passed: %d / %d components (%d / %d batches)",
            passed_count,
            len(components),
            total_batches - len(failed_batches),
            total_batches,
        )
        if failed_batches:
            all_violations = self._aggregate_violations(failed_batches)
            self._log_violation_summary(all_violations, failed_batches)
            sys.exit(1)

        self.logger.info("All %d components passed EC verification", len(components))

    def _extract_violations_from_plr(
        self, plr_info: PipelineRunInfo, batch: list[dict], konflux_client: KonfluxClient
    ) -> list[dict]:
        """Parse violation details from the verify pod logs of a failed PipelineRun.

        Returns a list of dicts with keys: component_name, image_ref, rule, title, reason
        """
        digest_to_name = {}
        for comp in batch:
            image_ref = comp.get("containerImage", "")
            digest = image_ref.split("@")[-1] if "@" in image_ref else image_ref
            digest_to_name[digest] = comp["name"]

        log_text = self._get_verify_pod_log(plr_info, konflux_client)
        if not log_text:
            self.logger.warning("No verify pod log available for violation extraction")
            return []

        return self._parse_violations_from_log(log_text, digest_to_name)

    def _get_verify_pod_log(self, plr_info: PipelineRunInfo, konflux_client: KonfluxClient) -> Optional[str]:
        """Fetch the report log from the verify pod of a failed PipelineRun.

        The watcher only caches logs for failed containers, but the report step
        typically exits 0 even when violations are found. We fetch it directly
        from the Kubernetes API instead.
        """
        for pod_info in plr_info.get_pods():
            pod_name = pod_info.name or ""
            if "verify" not in pod_name:
                continue

            namespace = pod_info.namespace or KONFLUX_DEFAULT_NAMESPACE
            for step_name in ("step-report", "step-detailed-report"):
                try:
                    log = konflux_client.corev1_client.read_namespaced_pod_log(
                        name=pod_name,
                        namespace=namespace,
                        container=step_name,
                        _request_timeout=120,
                    )
                    if log:
                        return log
                except Exception as e:
                    self.logger.debug("Could not fetch log for %s/%s: %s", pod_name, step_name, e)

            # Fall back to any pre-fetched failed container log
            for container in pod_info.get_all_containers():
                if container.is_failed:
                    log = container.get_log_content()
                    if log:
                        return log
        return None

    @staticmethod
    def _parse_violations_from_log(log_text: str, digest_to_name: dict[str, str]) -> list[dict]:
        """Parse the EC detailed-report log and extract violation entries.

        Each violation block in the log looks like:
            ✕ [Violation] rule.name
              ImageRef: quay.io/...@sha256:abc123
              Reason: <multiline reason text>
              Term: ...
              Title: Human readable title
              Description: ...
              Solution: ...
        """
        violations: list[dict] = []

        block_pattern = re.compile(r'✕ \[Violation\] (\S+)(.*?)(?=✕ \[Violation\]|\Z)', re.DOTALL)
        image_ref_pattern = re.compile(r'ImageRef:\s*(\S+)')
        reason_pattern = re.compile(r'Reason:\s*(.*?)(?=\n\s*(?:Term|Title):)', re.DOTALL)
        title_pattern = re.compile(r'Title:\s*(.*)')

        for block_match in block_pattern.finditer(log_text):
            rule = block_match.group(1)
            block_body = block_match.group(2)

            ref_match = image_ref_pattern.search(block_body)
            image_ref = ref_match.group(1) if ref_match else ""

            reason = ""
            reason_match = reason_pattern.search(block_body)
            if reason_match:
                reason = " ".join(reason_match.group(1).strip().split())

            title = ""
            title_match = title_pattern.search(block_body)
            if title_match:
                title = title_match.group(1).strip()

            digest = image_ref.split("@")[-1] if "@" in image_ref else image_ref
            component_name = digest_to_name.get(digest, image_ref)

            violations.append(
                {
                    "component_name": component_name,
                    "image_ref": image_ref,
                    "rule": rule,
                    "title": title,
                    "reason": reason,
                }
            )

        return violations

    def _aggregate_violations(self, failed_batches: list[dict]) -> dict[str, list[dict]]:
        """Aggregate violations across all failed batches, keyed by component name."""
        by_component: dict[str, list[dict]] = defaultdict(list)
        for fb in failed_batches:
            for v in fb.get("violations", []):
                by_component[v["component_name"]].append(v)
        return dict(by_component)

    def _log_violation_summary(self, all_violations: dict[str, list[dict]], failed_batches: list[dict]):
        """Log a human-readable summary of all EC violations."""
        self.logger.error("=== EC Violation Details ===")
        if not all_violations:
            self.logger.error("Failed batches:")
            for fb in failed_batches:
                self.logger.error("  Batch %d: %s (no violation details available)", fb["batch"], fb["plr_url"])
            return

        unique_rules: set[str] = set()
        for component_name, violations in sorted(all_violations.items()):
            self.logger.error("  Component: %s", component_name)
            self.logger.error("    ImageRef: %s", violations[0]["image_ref"])
            seen_rules: set[str] = set()
            for v in violations:
                if v["rule"] not in seen_rules:
                    seen_rules.add(v["rule"])
                    unique_rules.add(v["rule"])
                    self.logger.error("    - [%s] %s", v["rule"], v["title"])
                    if v["reason"]:
                        self.logger.error("      Reason: %s", v["reason"])

        self.logger.error("---")
        self.logger.error(
            "Total: %d unique component(s) with violations, %d unique rule(s) violated",
            len(all_violations),
            len(unique_rules),
        )
        self.logger.error("Failed batches:")
        for fb in failed_batches:
            self.logger.error("  Batch %d: %s", fb["batch"], fb["plr_url"])


@cli.command("build-conforma-verify", short_help="Run Conforma (EC) verification on OCP image builds")
@click.option("--version", required=True, help="OCP version (e.g. 4.18)")
@click.option("--assembly", required=True, default="stream", help="Assembly name")
@click.option("--data-path", default=None, help="ocp-build-data repo URL or path")
@click.option("--data-gitref", default=None, help="ocp-build-data git ref")
@click.option("--builds", default="", help="Comma-separated NVRs to verify (empty = fetch latest)")
@pass_runtime
@click_coroutine
async def build_conforma_verify(
    runtime: Runtime,
    version: str,
    assembly: str,
    data_path: Optional[str],
    data_gitref: Optional[str],
    builds: str,
):
    builds_list = [b.strip() for b in builds.split(",") if b.strip()] if builds else []

    pipeline = BuildConformaVerifyPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        builds=builds_list,
        data_path=data_path,
        data_gitref=data_gitref,
    )
    await pipeline.run()
