"""Utilities for running artcd pipelines inside Tekton on the artc2023 cluster."""

import logging
import os
from typing import Optional

from artcommonlib import exectools

logger = logging.getLogger(__name__)

ARTC_CONSOLE_BASE = "https://console-openshift-console.apps.artc2023.pc3z.p1.openshiftapps.com"
DEFAULT_NAMESPACE = "layered-products"


def is_tekton_context() -> bool:
    """Return True when running inside a Tekton PipelineRun."""
    return bool(os.environ.get("TEKTON_PIPELINERUN_NAME"))


def get_current_pipelinerun_name() -> Optional[str]:
    """Return the name of the PipelineRun we are executing inside, or None."""
    return os.environ.get("TEKTON_PIPELINERUN_NAME") or None


def pipelinerun_url(name: str, namespace: str = DEFAULT_NAMESPACE) -> str:
    """Build the OpenShift console URL for a PipelineRun."""
    return f"{ARTC_CONSOLE_BASE}/k8s/ns/{namespace}/tekton.dev~v1~PipelineRun/{name}/logs"


async def start_pipeline(
    pipeline_name: str,
    params: dict,
    namespace: str = DEFAULT_NAMESPACE,
    pipeline_timeout: str = "4h",
) -> str:
    """Fire-and-forget: start a Tekton pipeline and return the PipelineRun name.

    Uses the in-cluster service account — no kubeconfig needed when running on-cluster.
    Does NOT stream logs or wait for completion (matches the Jenkins fire-and-forget model).
    """
    cmd = [
        "tkn",
        "pipeline",
        "start",
        pipeline_name,
        "--namespace",
        namespace,
        "--pipeline-timeout",
        pipeline_timeout,
        "--output",
        "name",
    ]
    for key, value in params.items():
        cmd.extend(["--param", f"{key}={value}"])

    logger.info("Starting Tekton pipeline %s in namespace %s", pipeline_name, namespace)
    rc, stdout, stderr = await exectools.cmd_gather_async(cmd)
    if rc != 0:
        raise ChildProcessError(f"Failed to start pipeline {pipeline_name} (exit {rc}): {stderr.strip()}")

    plr_name = stdout.strip()
    if not plr_name:
        raise RuntimeError(f"tkn pipeline start {pipeline_name} returned empty PipelineRun name")

    url = pipelinerun_url(plr_name, namespace)
    logger.info("Started PipelineRun: %s (%s)", plr_name, url)
    return plr_name
