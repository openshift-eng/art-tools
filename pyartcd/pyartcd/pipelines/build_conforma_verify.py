import json
import os
import sys
from typing import List, Optional

import click
from artcommonlib import exectools, logutil
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildRecord, KonfluxRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from doozerlib.backend.konflux_client import KonfluxClient, get_common_runtime_watcher_labels
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

EC_WORKERS = "35"


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

        # Build a single multi-component Snapshot JSON
        components = []
        for record in records:
            components.append(
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
            )
        snapshot_spec = {"application": application_name, "components": components}
        snapshot_json = json.dumps(snapshot_spec)
        self.logger.info("Built snapshot with %d components for application %s", len(components), application_name)

        # Ensure IntegrationTestScenario exists
        policy_suffix = ec_policy.split('/')[-1]
        its_name = f"{application_name}-ec-{policy_suffix}"
        self.logger.info("Ensuring IntegrationTestScenario %s exists...", its_name)
        await konflux_client.ensure_integration_test_scenario(
            name=its_name,
            application_name=application_name,
            policy_configuration=ec_policy,
        )

        # Create and submit a single EC PipelineRun for all components
        generate_name = f"{application_name}-ec-conforma-verify-"
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
                    {"name": "WORKERS", "value": EC_WORKERS},
                ],
                "taskRunTemplate": {
                    "serviceAccountName": f"build-pipeline-{components[0]['name']}",
                },
                "taskRunSpecs": [
                    {
                        "pipelineTaskName": "verify",
                        "stepSpecs": [
                            {
                                "name": "validate",
                                "computeResources": {
                                    "limits": {"cpu": "16", "memory": "16Gi"},
                                    "requests": {"cpu": "10", "memory": "10Gi"},
                                },
                            }
                        ],
                    }
                ],
                "timeouts": {
                    "pipeline": "4h",
                },
            },
        }

        if self.dry_run:
            self.logger.warning("[DRY RUN] Would have created EC PipelineRun for %d components", len(components))
            return

        self.logger.info("Creating EC PipelineRun for %d components...", len(components))
        plr = await konflux_client._create(manifest)
        plr_name = plr.metadata.name
        plr_url = KonfluxClient.resource_url(plr.to_dict())
        self.logger.info("Created EC PipelineRun: %s", plr_url)

        self.logger.info("Waiting for EC PipelineRun %s to complete...", plr_name)
        plr_info = await konflux_client.wait_for_pipelinerun(plr_name, namespace=KONFLUX_DEFAULT_NAMESPACE)
        plr_url = KonfluxClient.resource_url(plr_info.to_dict())

        from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome

        condition = plr_info.find_condition('Succeeded')
        outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(condition)

        if outcome is not KonfluxBuildOutcome.SUCCESS:
            self.logger.error("EC verification FAILED. PLR: %s", plr_url)
            sys.exit(1)

        self.logger.info("EC verification PASSED for all %d components. PLR: %s", len(components), plr_url)


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
