"""
Blocker bugs check — runs elliott to find blocker bugs and categorizes them.
"""

import json
import logging
import re

from artcommonlib import exectools

from pyartcd.pipelines.release_readiness.helpers import error_result
from pyartcd.pipelines.release_readiness.models import MAX_DETAIL_ITEMS, CheckResult, Status

_LOGGER = logging.getLogger(__name__)


async def check_blocker_bugs(group: str, doozer_working: str) -> CheckResult:
    """
    Check for blocker bugs using elliott. Partitions into open/on_qa/verified.

    Arg(s):
        group (str): OCP group (e.g. "openshift-4.21").
        doozer_working (str): Working directory for doozer/elliott.

    Return Value(s):
        CheckResult: Blocker bugs check result.
    """

    _LOGGER.info("Checking blocker bugs for %s", group)

    cmd = [
        "elliott",
        "--group",
        group,
        "--assembly",
        "stream",
        f"--working-dir={doozer_working}",
        "find-bugs:blocker",
        "--include-status",
        "VERIFIED",
        "--output",
        "json",
    ]

    try:
        _, stdout, _ = await exectools.cmd_gather_async(cmd)
        bugs = json.loads(stdout) if stdout.strip() else []
    except json.JSONDecodeError as e:
        match = re.search(r"Found ([0-9]+) bugs", str(stdout))
        count = int(match[1]) if match else None
        if count and count > 0:
            return CheckResult(
                name="blocker_bugs", status=Status.RED, summary=f"{count} blocker bug(s) (details unavailable) ❌"
            )
        if count == 0:
            bugs = []
        else:
            _LOGGER.warning("Could not parse blocker bugs output for %s: %s", group, e)
            return error_result("blocker_bugs", Status.YELLOW, "Could not parse blocker bugs output", e)
    except Exception as e:
        _LOGGER.warning("Error checking blocker bugs for %s: %s", group, e)
        return error_result("blocker_bugs", Status.YELLOW, "Could not check blocker bugs", e)

    open_bugs, on_qa_bugs, verified_bugs = _categorize_bugs(bugs)
    details = _format_bug_details(open_bugs, on_qa_bugs, verified_bugs)
    summary, status = _summarize_bugs(open_bugs, on_qa_bugs)

    return CheckResult(name="blocker_bugs", status=status, summary=summary, details=details)


def _categorize_bugs(bugs: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Split bugs into (open, on_qa, verified) buckets.
    """

    open_bugs, on_qa_bugs, verified_bugs = [], [], []
    for bug in bugs:
        status = bug.get("status", "").upper()
        if status == "ON_QA":
            on_qa_bugs.append(bug)
        elif status == "VERIFIED":
            verified_bugs.append(bug)
        else:
            open_bugs.append(bug)
    return open_bugs, on_qa_bugs, verified_bugs


def _format_bug_details(
    open_bugs: list[dict],
    on_qa_bugs: list[dict],
    verified_bugs: list[dict],
) -> list[str]:
    """
    Format bug details for the report.
    """

    details: list[str] = []

    if open_bugs:
        details.append(f"  Open blockers: {len(open_bugs)} 🔴")
        for bug in open_bugs[:MAX_DETAIL_ITEMS]:
            details.append(f"    {bug.get('id', '?')} ({bug.get('component', '?')})")
    else:
        details.append("  Open blockers: 0 ✅")

    if on_qa_bugs:
        bug_ids = ", ".join(b.get("id", "?") for b in on_qa_bugs[:MAX_DETAIL_ITEMS])
        details.append(f"  On QA: {len(on_qa_bugs)} ({bug_ids})")

    if verified_bugs:
        details.append(f"  Verified: {len(verified_bugs)} ✅")

    return details


def _summarize_bugs(open_bugs: list[dict], on_qa_bugs: list[dict]) -> tuple[str, Status]:
    """
    Generate summary string and status for blocker bugs.

    Return Value(s):
        tuple: (summary, status).
    """

    if open_bugs:
        summary = f"{len(open_bugs)} open blocker(s)"
        if on_qa_bugs:
            summary += f", {len(on_qa_bugs)} on QA"
        return summary + " ❌", Status.RED

    if on_qa_bugs:
        return f"No open blockers, {len(on_qa_bugs)} on QA ✅", Status.GREEN

    return "No blocker bugs ✅", Status.GREEN
