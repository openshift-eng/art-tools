"""Format status messages for the pipeline UI."""

import re

from .rebuild import pipeline_name

TASK_COUNTS = re.compile(r"\bTasks Completed:\s*\d+\s*\(Failed:\s*\d+,\s*Cancelled:?\s*\d+\),\s*Skipped:\s*\d+\b")


def status_message(value):
    """Omit Tekton task counts while retaining other status details."""
    return TASK_COUNTS.sub("", str(value or "")).strip()


def lp_rebase_warning(run):
    """Expose partial LP rebase failures only after a successful execution."""
    if pipeline_name(run) != "build-layered-products":
        return ""
    succeeded = any(
        condition.get("type") == "Succeeded" and condition.get("status") == "True"
        for condition in run.get("status", {}).get("conditions", [])
    )
    annotations = run.get("metadata", {}).get("annotations") or {}
    warning = annotations.get("art.openshift.io/rebase-warning")
    return warning.strip() if succeeded and isinstance(warning, str) else ""
