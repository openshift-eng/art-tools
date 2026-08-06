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
from artcommonlib.konflux.konflux_build_record import (
    Engine,
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
    KonfluxRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import oc_image_info_async
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
from pyartcd.slack import SlackClient

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
        ec_policy: Optional[str] = None,
        fbc_ec_policy: Optional[str] = None,
        effective_time: Optional[str] = None,
        include_bundles: bool = False,
        include_fbcs: bool = False,
        include_corresponding_bundles: bool = False,
        include_corresponding_fbcs: bool = False,
        slack_client: Optional[SlackClient] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.builds = builds or []
        self.data_path = data_path or constants.OCP_BUILD_DATA_URL
        self.data_gitref = data_gitref
        self.ec_policy = ec_policy
        self.fbc_ec_policy = fbc_ec_policy
        self.effective_time = effective_time or "now"
        self.include_bundles = include_bundles
        self.include_fbcs = include_fbcs
        self.include_corresponding_bundles = include_corresponding_bundles
        self.include_corresponding_fbcs = include_corresponding_fbcs
        self.slack_client = slack_client
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

    @property
    def _doozer_base_command(self) -> list[str]:
        group_param = f'--group={self.group}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'

        return [
            'doozer',
            group_param,
            f'--assembly={self.assembly}',
            f'--working-dir={self.working_dir}',
            f'--data-path={self.data_path}',
        ]

    async def run(self):
        any_failed = False
        image_failed = 0
        bundle_failed = 0
        fbc_failed = 0
        all_violations: dict[str, list[dict]] = {}

        nvrs = self.builds
        if not nvrs:
            self.logger.info("No builds provided; fetching latest image builds for assembly '%s'...", self.assembly)
            nvrs = await self._find_latest_builds()
            self.logger.info("Found %d image builds", len(nvrs))

        if not nvrs and not self.include_bundles and not self.include_fbcs:
            raise RuntimeError("No builds found to verify")

        if nvrs:
            self.logger.info("Looking up build records for %d image NVRs...", len(nvrs))
            nvr_record_map = await self._lookup_build_records(nvrs)
            records = list(nvr_record_map.values())
            self.logger.info("Running EC verification for %d image builds...", len(records))
            passed, violations = await self._verify_records(records, build_type="image")
            if not passed:
                any_failed = True
                image_failed = len(violations)
                all_violations.update(violations)

        bundle_records_list: list[KonfluxRecord] = []
        fbc_records_list: list[KonfluxRecord] = []

        if nvrs and (self.include_corresponding_bundles or self.include_corresponding_fbcs):
            self.logger.info("Finding corresponding operator bundles and FBCs...")
            corresponding_bundles, corresponding_fbcs = await self._find_corresponding_builds(
                nvr_record_map, lookup_fbcs=self.include_corresponding_fbcs
            )
            if self.include_corresponding_bundles and corresponding_bundles:
                self.logger.info("Found %d corresponding bundle builds", len(corresponding_bundles))
                bundle_records_list.extend(corresponding_bundles)
            if self.include_corresponding_fbcs and corresponding_fbcs:
                self.logger.info("Found %d corresponding FBC builds", len(corresponding_fbcs))
                fbc_records_list.extend(corresponding_fbcs)

        if self.include_bundles:
            self.logger.info(
                "Fetching latest bundle builds for group '%s', assembly '%s'...", self.group, self.assembly
            )
            bundle_records = await self._find_latest_bundles()
            if bundle_records:
                existing_nvrs = {r.nvr for r in bundle_records_list}
                for r in bundle_records:
                    if r.nvr not in existing_nvrs:
                        bundle_records_list.append(r)

        if self.include_fbcs:
            self.logger.info("Fetching latest FBC builds for group '%s', assembly '%s'...", self.group, self.assembly)
            fbc_records = await self._find_latest_fbcs()
            if fbc_records:
                existing_nvrs = {r.nvr for r in fbc_records_list}
                for r in fbc_records:
                    if r.nvr not in existing_nvrs:
                        fbc_records_list.append(r)

        if bundle_records_list:
            self.logger.info("Running EC verification for %d bundle builds...", len(bundle_records_list))
            passed, violations = await self._verify_records(bundle_records_list, build_type="bundle")
            if not passed:
                any_failed = True
                bundle_failed = len(violations)
                all_violations.update(violations)
        elif self.include_bundles or self.include_corresponding_bundles:
            self.logger.warning("No bundle builds found for verification")

        if fbc_records_list:
            self.logger.info("Running EC verification for %d FBC builds...", len(fbc_records_list))
            passed, violations = await self._verify_records(fbc_records_list, build_type="fbc")
            if not passed:
                any_failed = True
                fbc_failed = len(violations)
                all_violations.update(violations)
        elif self.include_fbcs or self.include_corresponding_fbcs:
            self.logger.warning("No FBC builds found for verification")

        await self._report_to_slack(
            any_failed=any_failed,
            image_failed=image_failed,
            bundle_failed=bundle_failed,
            fbc_failed=fbc_failed,
            all_violations=all_violations,
        )

        if any_failed:
            sys.exit(1)

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

    async def _find_latest_bundles(self) -> list[KonfluxRecord]:
        db = KonfluxDb()
        db.bind(KonfluxBundleBuildRecord)

        records_by_name: dict[str, KonfluxRecord] = {}
        async for record in db.search_builds_by_fields(
            where={"group": self.group, "assembly": self.assembly, "outcome": "success"},
            order_by="start_time",
            sorting="DESC",
        ):
            if record.name not in records_by_name:
                records_by_name[record.name] = record
        return list(records_by_name.values())

    async def _find_latest_fbcs(self) -> list[KonfluxRecord]:
        db = KonfluxDb()
        db.bind(KonfluxFbcBuildRecord)

        records_by_name: dict[str, KonfluxRecord] = {}
        async for record in db.search_builds_by_fields(
            where={"group": self.group, "assembly": self.assembly, "outcome": "success"},
            order_by="start_time",
            sorting="DESC",
        ):
            if record.name not in records_by_name:
                records_by_name[record.name] = record
        return list(records_by_name.values())

    async def _find_operator_bundle_names(self) -> dict[str, str]:
        """Returns a mapping of operator distgit_key -> bundle short name.

        Uses doozer's "bundle-name" output format, which honors any per-image
        `bundle_name_override` config rather than assuming '{operator_name}-bundle'.
        """
        cmd = self._doozer_base_command + ['olm-bundle:list-olm-operators', '--output-format', 'bundle-name']
        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        bundle_names_by_operator: dict[str, str] = {}
        for line in out.strip().split('\n') if out.strip() else []:
            distgit_key, _, bundle_name = line.partition('\t')
            if distgit_key and bundle_name:
                bundle_names_by_operator[distgit_key] = bundle_name
        return bundle_names_by_operator

    async def _find_corresponding_builds(
        self, nvr_record_map: dict[str, KonfluxRecord], lookup_fbcs: bool = True
    ) -> tuple[list[KonfluxRecord], list[KonfluxRecord]]:
        """Find corresponding bundle and FBC builds for operator images.

        Returns (bundle_records, fbc_records). Set lookup_fbcs=False to skip FBC queries.
        """
        bundle_names_by_operator = await self._find_operator_bundle_names()
        if not bundle_names_by_operator:
            self.logger.warning("No operator names found")
            return [], []

        self.logger.info("Found %d operator names, cross-referencing with image NVRs...", len(bundle_names_by_operator))

        bundle_db = KonfluxDb()
        bundle_db.bind(KonfluxBundleBuildRecord)

        operator_nvrs = []
        for nvr, record in nvr_record_map.items():
            if record.name in bundle_names_by_operator:
                operator_nvrs.append((record.name, nvr))

        self.logger.info("Found %d operator image builds to look up", len(operator_nvrs))

        bundle_records: list[KonfluxRecord] = []
        # Bundle nvr -> operator distgit_key, so the FBC lookup below doesn't need to
        # reverse-derive the operator name from the bundle name (which breaks under
        # bundle_name_override).
        operator_name_by_bundle_nvr: dict[str, str] = {}
        for operator_name, nvr in operator_nvrs:
            bundle_name = bundle_names_by_operator[operator_name]
            bundle = await bundle_db.get_latest_build(
                name=bundle_name,
                group=self.group,
                outcome=KonfluxBuildOutcome.SUCCESS,
                assembly=self.assembly,
                extra_patterns={'operator_nvr': nvr},
            )
            if bundle:
                self.logger.info("  Found bundle for %s: %s", nvr, bundle.nvr)
                bundle_records.append(bundle)
                operator_name_by_bundle_nvr[bundle.nvr] = operator_name
            else:
                self.logger.warning("  No bundle found for operator %s (%s)", operator_name, nvr)

        fbc_records: list[KonfluxRecord] = []
        if lookup_fbcs and bundle_records:
            fbc_db = KonfluxDb()
            fbc_db.bind(KonfluxFbcBuildRecord)
            for bundle in bundle_records:
                operator_name = operator_name_by_bundle_nvr[bundle.nvr]
                fbc_name = f'{operator_name}-fbc'
                async for fbc in fbc_db.search_builds_by_fields(
                    where={
                        'name': fbc_name,
                        'group': self.group,
                        'outcome': KonfluxBuildOutcome.SUCCESS,
                        'assembly': self.assembly,
                    },
                    array_contains={'bundle_nvrs': bundle.nvr},
                    limit=1,
                    order_by='start_time',
                    sorting='DESC',
                ):
                    self.logger.info("  Found FBC for %s: %s", bundle.nvr, fbc.nvr)
                    fbc_records.append(fbc)
                    break
                else:
                    self.logger.warning("  No FBC found for bundle %s", bundle.nvr)

        return bundle_records, fbc_records

    async def _verify_records(
        self, records: list[KonfluxRecord], build_type: str = "image"
    ) -> tuple[bool, dict[str, list[dict]]]:
        """Run EC verification on a list of build records.

        Returns (passed, violations_by_component).
        """
        kubeconfig = os.getenv("KONFLUX_SA_KUBECONFIG")
        konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=KONFLUX_DEFAULT_NAMESPACE,
            config_file=kubeconfig,
            context=None,
            dry_run=self.dry_run,
        )

        if build_type == "fbc":
            ec_policy = self.fbc_ec_policy or self.ec_policy or KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION
        else:
            ec_policy = self.ec_policy or KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION

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

        policy_suffix = ec_policy.split('/')[-1]
        its_name = f"{application_name}-ec-{policy_suffix}"
        self.logger.info("[%s] Ensuring IntegrationTestScenario %s exists...", build_type, its_name)
        await konflux_client.ensure_integration_test_scenario(
            name=its_name,
            application_name=application_name,
            policy_configuration=ec_policy,
        )

        batches = [components[i : i + BATCH_SIZE] for i in range(0, len(components), BATCH_SIZE)]
        total_batches = len(batches)
        self.logger.info(
            "[%s] Split %d components into %d batches of up to %d",
            build_type,
            len(components),
            total_batches,
            BATCH_SIZE,
        )

        if self.dry_run:
            for batch_idx in range(1, total_batches + 1):
                self.logger.warning(
                    "[%s] [DRY RUN] Would have created EC PipelineRun for batch %d", build_type, batch_idx
                )
            self.logger.info("[%s] All %d components passed EC verification (dry run)", build_type, len(components))
            return True, {}

        batch_plrs: list[tuple[int, list[dict], str]] = []
        for batch_idx, batch in enumerate(batches, start=1):
            self.logger.info(
                "[%s] === Creating batch %d/%d (%d components) ===", build_type, batch_idx, total_batches, len(batch)
            )

            snapshot_spec = {"application": application_name, "components": batch}
            snapshot_json = json.dumps(snapshot_spec)

            generate_name = f"{application_name}-ec-{build_type}-{batch_idx}-"
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
                        {"name": "EFFECTIVE_TIME", "value": self.effective_time},
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
            self.logger.info("[%s] Created EC PipelineRun for batch %d: %s", build_type, batch_idx, plr_url)
            batch_plrs.append((batch_idx, batch, plr_name))

        self.logger.info(
            "[%s] All %d batches submitted, waiting for completion in parallel...", build_type, total_batches
        )

        async def _wait_for_batch(batch_idx: int, batch: list[dict], plr_name: str) -> dict:
            try:
                plr_info = await konflux_client.wait_for_pipelinerun(plr_name, namespace=KONFLUX_DEFAULT_NAMESPACE)
                plr_url = KonfluxClient.resource_url(plr_info.to_dict())
                condition = plr_info.find_condition("Succeeded")
                outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(condition)
                if outcome is not KonfluxBuildOutcome.SUCCESS:
                    self.logger.error("[%s] Batch %d FAILED. PLR: %s", build_type, batch_idx, plr_url)
                    violations = await self._extract_violations_from_plr(plr_info, batch, konflux_client)
                    return {"batch": batch_idx, "plr_url": plr_url, "passed": False, "violations": violations}
                self.logger.info("[%s] Batch %d PASSED. PLR: %s", build_type, batch_idx, plr_url)
                return {"batch": batch_idx, "plr_url": plr_url, "passed": True, "count": len(batch)}
            except Exception:
                self.logger.exception(
                    "[%s] Batch %d ERROR: failed to wait for PipelineRun %s", build_type, batch_idx, plr_name
                )
                return {"batch": batch_idx, "plr_url": None, "passed": False, "violations": []}

        results = await asyncio.gather(*[_wait_for_batch(idx, batch, name) for idx, batch, name in batch_plrs])

        failed_batches = [r for r in results if not r["passed"]]
        passed_count = sum(r["count"] for r in results if r["passed"])

        self.logger.info("=== [%s] EC Verification Summary ===", build_type)
        self.logger.info(
            "[%s] Passed: %d / %d components (%d / %d batches)",
            build_type,
            passed_count,
            len(components),
            total_batches - len(failed_batches),
            total_batches,
        )
        if failed_batches:
            all_violations = self._aggregate_violations(failed_batches)
            self._log_violation_summary(all_violations, failed_batches)
            return False, all_violations

        self.logger.info("[%s] All %d components passed EC verification", build_type, len(components))
        return True, {}

    async def _extract_violations_from_plr(
        self, plr_info: PipelineRunInfo, batch: list[dict], konflux_client: KonfluxClient
    ) -> list[dict]:
        """Parse violation details from the verify pod logs of a failed PipelineRun.

        Returns a list of dicts with keys: component_name, image_ref, rule, title, reason
        """
        registry_config = os.environ.get('QUAY_AUTH_FILE')
        digest_to_name = {}
        for comp in batch:
            image_ref = comp.get("containerImage", "")
            digest = image_ref.split("@")[-1] if "@" in image_ref else image_ref
            digest_to_name[digest] = comp["name"]
            try:
                arch_infos = await oc_image_info_async(image_ref, '--show-multiarch', registry_config=registry_config)
                if isinstance(arch_infos, list):
                    for arch_info in arch_infos:
                        arch_digest = arch_info.get("digest", "")
                        if arch_digest:
                            digest_to_name[arch_digest] = comp["name"]
            except Exception:
                self.logger.warning("Failed to resolve per-arch digests for %s", comp["name"], exc_info=True)

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

    async def _report_to_slack(
        self,
        any_failed: bool,
        image_failed: int,
        bundle_failed: int,
        fbc_failed: int,
        all_violations: Optional[dict[str, list[dict]]] = None,
    ):
        if not self.slack_client:
            return

        if not any_failed:
            message = (
                f":white_check_mark: build-conforma-verify in {self.version} "
                f"(assembly=`{self.assembly}`) passed (effective_time=`{self.effective_time}`)"
            )
        else:
            failed_parts = []
            if image_failed:
                failed_parts.append(f"{image_failed} image(s)")
            if bundle_failed:
                failed_parts.append(f"{bundle_failed} bundle(s)")
            if fbc_failed:
                failed_parts.append(f"{fbc_failed} FBC(s)")
            if failed_parts:
                detail = f"failed for: {', '.join(failed_parts)}"
            else:
                detail = "failed (no violation details available)"
            message = (
                f":warning: build-conforma-verify in {self.version} "
                f"(assembly=`{self.assembly}`) {detail} "
                f"(effective_time=`{self.effective_time}`)"
            )

        await self.slack_client.say_in_thread(message)

        if any_failed and all_violations:
            unique_rules: dict[str, str] = {}
            for violations in all_violations.values():
                for v in violations:
                    if v["rule"] not in unique_rules:
                        unique_rules[v["rule"]] = v.get("title", "")
            lines = [f"Unique rules violated ({len(unique_rules)}):"]
            for rule, title in sorted(unique_rules.items()):
                lines.append(f"- `{rule}` — {title}" if title else f"- `{rule}`")
            await self.slack_client.say_in_thread("\n".join(lines))


@cli.command("build-conforma-verify", short_help="Run Conforma (EC) verification on OCP builds")
@click.option("--version", required=True, help="OCP version (e.g. 4.18)")
@click.option("--assembly", required=True, default="stream", help="Assembly name")
@click.option("--data-path", default=None, help="ocp-build-data repo URL or path")
@click.option("--data-gitref", default=None, help="ocp-build-data git ref")
@click.option("--builds", default="", help="Comma-separated image NVRs to verify (empty = fetch latest)")
@click.option(
    "--ec-policy",
    default=None,
    help="EnterpriseContractPolicy CR reference (namespace/name). Defaults to ocp-art-tenant/conforma-build-stage",
)
@click.option(
    "--fbc-ec-policy",
    default=None,
    help="EnterpriseContractPolicy CR reference for FBC builds (namespace/name). Falls back to --ec-policy if unset",
)
@click.option(
    "--effective-time",
    default=None,
    help="RFC 3339 timestamp for EC policy effective_on evaluation (e.g. 2026-08-05T00:00:00Z). Defaults to 'now'",
)
@click.option("--include-bundles", is_flag=True, default=False, help="Also verify latest OLM bundle builds")
@click.option("--include-fbcs", is_flag=True, default=False, help="Also verify latest FBC builds")
@click.option(
    "--include-corresponding-bundles",
    is_flag=True,
    default=False,
    help="For each operator build in --builds, find and verify its corresponding bundle build",
)
@click.option(
    "--include-corresponding-fbcs",
    is_flag=True,
    default=False,
    help="For each operator build in --builds, find and verify its corresponding FBC build",
)
@click.option(
    "--report-to-slack",
    is_flag=True,
    default=False,
    help="Post results to #art-release Slack channel",
)
@pass_runtime
@click_coroutine
async def build_conforma_verify(
    runtime: Runtime,
    version: str,
    assembly: str,
    data_path: Optional[str],
    data_gitref: Optional[str],
    builds: str,
    ec_policy: Optional[str],
    fbc_ec_policy: Optional[str],
    effective_time: Optional[str],
    include_bundles: bool,
    include_fbcs: bool,
    include_corresponding_bundles: bool,
    include_corresponding_fbcs: bool,
    report_to_slack: bool,
):
    builds_list = [b.strip() for b in builds.split(",") if b.strip()] if builds else []

    slack_client = None
    if report_to_slack:
        slack_token = os.environ.get("SLACK_BOT_TOKEN")
        if not slack_token:
            raise RuntimeError("SLACK_BOT_TOKEN is required with --report-to-slack")
        slack_client = runtime.new_slack_client(token=slack_token)
        slack_client.bind_channel("#art-release")

    pipeline = BuildConformaVerifyPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        builds=builds_list,
        data_path=data_path,
        data_gitref=data_gitref,
        ec_policy=ec_policy,
        fbc_ec_policy=fbc_ec_policy,
        effective_time=effective_time,
        include_bundles=include_bundles,
        include_fbcs=include_fbcs,
        include_corresponding_bundles=include_corresponding_bundles,
        include_corresponding_fbcs=include_corresponding_fbcs,
        slack_client=slack_client,
    )
    await pipeline.run()
