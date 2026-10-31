"""Format status messages for the pipeline UI."""

import re

TASK_COUNTS = re.compile(r"\bTasks Completed:\s*\d+\s*\(Failed:\s*\d+,\s*Cancelled:?\s*\d+\),\s*Skipped:\s*\d+\b")


def status_message(value):
    """Omit Tekton task counts while retaining other status details."""
    return TASK_COUNTS.sub("", str(value or "")).strip()
