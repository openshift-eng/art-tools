import json
import logging
import os
import re
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)

ARTC2023_CONSOLE_URL = "https://console-openshift-console.apps.artc2023.pc3z.p1.openshiftapps.com"


def is_tekton_context() -> bool:
    """Detect if currently running inside a Tekton TaskRun.

    The artcd Task sets TASKRUN_NAME via stepTemplate when running in Tekton.
    """
    return bool(os.environ.get("TASKRUN_NAME"))


def get_current_pipelinerun_name() -> Optional[str]:
    """Return the name of the current PipelineRun, or None if unavailable.

    Reads from TEKTON_PIPELINERUN_NAME which must be set via
    ``$(context.pipelineRun.name)`` in the Pipeline definition.
    """
    return os.environ.get("TEKTON_PIPELINERUN_NAME") or None


def _get_namespace() -> str:
    return os.environ.get("TASKRUN_NAMESPACE", "art-cd")


def _get_console_url() -> str:
    return os.environ.get("TEKTON_CONSOLE_URL", ARTC2023_CONSOLE_URL)


def _build_console_pipelinerun_url(pipelinerun_name: str, namespace: Optional[str] = None) -> str:
    ns = namespace or _get_namespace()
    return f"{_get_console_url()}/k8s/ns/{ns}/tekton.dev~v1~PipelineRun/{pipelinerun_name}"


def _get_propagatable_params() -> dict:
    """Collect parameters that should automatically propagate to downstream pipelines."""
    propagatable = {}
    art_tools_commit = os.environ.get("ART_TOOLS_COMMIT", "").strip()
    if art_tools_commit:
        propagatable["art-tools-commit"] = art_tools_commit
    return propagatable


def _parse_created_name(oc_stdout: str) -> Optional[str]:
    """Extract the resource name from ``oc create`` output like ``pipelinerun.tekton.dev/build-fbc-xyz created``."""
    match = re.search(r"(?:pipelinerun\.tekton\.dev/)?(\S+)\s+created", oc_stdout)
    return match.group(1) if match else None


def annotate_current_pipelinerun(annotations: dict) -> None:
    """Annotate the current PipelineRun with the given key-value pairs.

    Silently skips if not running inside a Tekton context or if the
    PipelineRun name is unavailable.
    """
    pr_name = get_current_pipelinerun_name()
    if not pr_name:
        return
    ns = _get_namespace()
    for key, value in annotations.items():
        try:
            subprocess.run(
                ["oc", "annotate", "pipelinerun", pr_name, f"{key}={value}", "-n", ns, "--overwrite"],
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception:
            logger.debug("Failed to annotate pipelinerun %s with %s=%s", pr_name, key, value, exc_info=True)


def start_pipeline_run(
    pipeline_name: str,
    params: dict,
    namespace: Optional[str] = None,
) -> Optional[str]:
    """Create a Tekton PipelineRun to trigger a downstream pipeline.

    Uses ``oc create -f -`` to submit a PipelineRun resource. Parameters with
    None or empty-string values are omitted. Automatically propagates
    ``ART_TOOLS_COMMIT`` from the environment as ``art-tools-commit``.

    When running inside a Tekton PipelineRun, the child PipelineRun is
    labelled with the parent pipeline/pipelinerun name and annotated with
    a console URL back to the parent for build-chain traceability.

    Returns the created PipelineRun name, or None if it could not be parsed.
    """
    if namespace is None:
        namespace = _get_namespace()

    merged_params = {**_get_propagatable_params(), **params}

    labels = {}
    annotations = {}
    parent_pr_name = get_current_pipelinerun_name()
    if parent_pr_name:
        parent_pipeline = os.environ.get("TEKTON_PIPELINE_NAME", "")
        if parent_pipeline:
            labels["art.openshift.io/parent-pipeline"] = parent_pipeline
        labels["art.openshift.io/parent-pipelinerun"] = parent_pr_name
        annotations["art.openshift.io/parent-pipelinerun-console-url"] = _build_console_pipelinerun_url(
            parent_pr_name, namespace
        )

    pipeline_run = {
        "apiVersion": "tekton.dev/v1",
        "kind": "PipelineRun",
        "metadata": {
            "generateName": f"{pipeline_name}-",
            "namespace": namespace,
            "labels": labels,
            "annotations": annotations,
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
    stdout = result.stdout.strip()
    logger.info("PipelineRun created: %s", stdout)

    created_name = _parse_created_name(stdout)
    if created_name:
        logger.info("Child PipelineRun name: %s", created_name)
    return created_name
