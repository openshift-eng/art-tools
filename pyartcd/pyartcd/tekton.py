import json
import logging
import os
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)


def is_tekton_context() -> bool:
    """Detect if currently running inside a Tekton TaskRun.

    The artcd Task sets TASKRUN_NAME via stepTemplate when running in Tekton.
    """
    return bool(os.environ.get("TASKRUN_NAME"))


def _get_propagatable_params() -> dict:
    """Collect parameters that should automatically propagate to downstream pipelines."""
    propagatable = {}
    art_tools_commit = os.environ.get("ART_TOOLS_COMMIT", "").strip()
    if art_tools_commit:
        propagatable["art-tools-commit"] = art_tools_commit
    return propagatable


def start_pipeline_run(
    pipeline_name: str,
    params: dict,
    namespace: Optional[str] = None,
) -> None:
    """Create a Tekton PipelineRun to trigger a downstream pipeline.

    Uses ``oc create -f -`` to submit a PipelineRun resource. Parameters with
    None or empty-string values are omitted. Automatically propagates
    ``ART_TOOLS_COMMIT`` from the environment as ``art-tools-commit``.
    """
    if namespace is None:
        namespace = os.environ.get("TASKRUN_NAMESPACE", "art-cd")

    merged_params = {**_get_propagatable_params(), **params}

    pipeline_run = {
        "apiVersion": "tekton.dev/v1",
        "kind": "PipelineRun",
        "metadata": {
            "generateName": f"{pipeline_name}-",
            "namespace": namespace,
        },
        "spec": {
            "pipelineRef": {"name": pipeline_name},
            "params": [
                {"name": k, "value": str(v)} for k, v in merged_params.items() if v is not None and str(v) != ""
            ],
        },
    }

    pr_json = json.dumps(pipeline_run)
    logger.info("Creating PipelineRun for pipeline %s in namespace %s", pipeline_name, namespace)
    logger.debug("PipelineRun spec: %s", pr_json)

    result = subprocess.run(
        ["oc", "create", "-f", "-"],
        input=pr_json,
        capture_output=True,
        text=True,
        check=True,
    )
    logger.info("PipelineRun created: %s", result.stdout.strip())
