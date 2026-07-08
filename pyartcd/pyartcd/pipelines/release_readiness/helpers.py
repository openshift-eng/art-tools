"""
Pure helper functions for release readiness checks.

No I/O, no side effects — only data transformation and formatting.
"""

import re
from datetime import datetime, timezone

from pyartcd.pipelines.release_readiness.models import (
    MAX_ERROR_LENGTH,
    CheckResult,
    Status,
)


def worst_status(statuses: list[Status]) -> Status:
    """
    Return the most severe status from a list.
    """

    for severity in (Status.RED, Status.YELLOW):
        if severity in statuses:
            return severity
    return Status.GREEN


def parse_nightly_timestamp(nightly_name: str) -> datetime | None:
    """
    Parse UTC timestamp from a nightly name like "4.21.0-0.nightly-2026-07-07-031500".
    """

    match = re.search(r"(\d{4})-(\d{2})-(\d{2})-(\d{6})$", nightly_name)
    if not match:
        return None
    try:
        return datetime(
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3)),
            int(match.group(4)[0:2]),
            int(match.group(4)[2:4]),
            int(match.group(4)[4:6]),
            tzinfo=timezone.utc,
        )
    except (ValueError, IndexError):
        return None


def format_age(total_minutes: float) -> str:
    """
    Format a duration in minutes as "Xh Ym ago" or "Ym ago".
    """

    hours = int(total_minutes // 60)
    mins = int(total_minutes % 60)
    if hours > 0:
        return f"{hours}h{mins}m ago"
    return f"{mins}m ago"


def format_relative_days(days_diff: int) -> str:
    """
    Format a day difference as a human-readable relative string.
    """

    if days_diff < 0:
        abs_days = abs(days_diff)
        if abs_days == 1:
            return "yesterday ⚠️"
        return f"{abs_days} days ago ⚠️"
    elif days_diff == 0:
        return "today"
    elif days_diff == 1:
        return "tomorrow"
    return f"{days_diff} days"


def truncate_error(error: Exception) -> str:
    """
    Extract first line of an error message, truncated.
    """

    return str(error).split("\n")[0][:MAX_ERROR_LENGTH]


def error_result(name: str, status: Status, prefix: str, error: Exception) -> CheckResult:
    """
    Build a CheckResult for an error case.
    """

    return CheckResult(
        name=name,
        status=status,
        summary=f"{prefix}: {truncate_error(error)} {status.emoji}",
    )
