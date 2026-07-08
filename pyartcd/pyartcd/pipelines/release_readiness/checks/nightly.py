"""
Nightly status check — queries release controller for accepted nightlies (amd64).
"""

import logging
from datetime import datetime, timezone

import aiohttp
from doozerlib.cli.get_nightlies import get_nightly_tag_base
from doozerlib.util import rc_api_url

from pyartcd.pipelines.release_readiness.helpers import error_result, parse_nightly_timestamp
from pyartcd.pipelines.release_readiness.models import (
    NIGHTLY_AGE_RED_HOURS,
    NIGHTLY_AGE_YELLOW_HOURS,
    NIGHTLY_HISTORY_COUNT,
    CheckResult,
    NightlyPhase,
    Status,
)

_LOGGER = logging.getLogger(__name__)


async def check_nightly_status(major: int, minor: int, build_system: str) -> CheckResult:
    """
    Check latest accepted nightly age and recent history (amd64 only).

    Arg(s):
        major (int): OCP major version.
        minor (int): OCP minor version.
        build_system (str): Build system ("brew" or "konflux").

    Return Value(s):
        CheckResult: Nightly status check result.
    """

    group = f"openshift-{major}.{minor}"
    _LOGGER.info("Checking nightly status for %s", group)
    tag_base = get_nightly_tag_base(major, minor, build_system)
    now = datetime.now(timezone.utc)

    try:
        rc_endpoint = f"{rc_api_url(tag_base, 'amd64', private_nightly=False)}/tags"
        async with aiohttp.ClientSession() as session:
            async with session.get(rc_endpoint) as resp:
                if resp.status != 200:
                    return error_result(
                        "nightly_status",
                        Status.RED,
                        "Failed to query release controller",
                        Exception(f"HTTP {resp.status}"),
                    )
                data = await resp.json()

        valid_tags = [t for t in data.get("tags", []) if isinstance(t, dict) and t.get("phase")]
        accepted = [t for t in valid_tags if t["phase"] == "Accepted"]

        history = " ".join(NightlyPhase.emoji_for(t.get("phase", "")) for t in valid_tags[:NIGHTLY_HISTORY_COUNT])
        details = [f"  Last {NIGHTLY_HISTORY_COUNT}: {history}"] if history else []

        if not accepted:
            return CheckResult(
                name="nightly_status", status=Status.RED, summary="No accepted nightly ❌", details=details
            )

        nightly_name = accepted[0].get("name", "")
        nightly_time = parse_nightly_timestamp(nightly_name)
        if not nightly_time:
            return CheckResult(
                name="nightly_status", status=Status.YELLOW, summary=f"{nightly_name} (unparseable timestamp) ⚠️"
            )

        age_hours = (now - nightly_time).total_seconds() / 3600
        if age_hours >= NIGHTLY_AGE_RED_HOURS:
            status = Status.RED
        elif age_hours >= NIGHTLY_AGE_YELLOW_HOURS:
            status = Status.YELLOW
        else:
            status = Status.GREEN

        return CheckResult(
            name="nightly_status",
            status=status,
            summary=f"`{nightly_name}` ({age_hours:.0f}h ago) {status.emoji}",
            details=details,
        )

    except Exception as e:
        _LOGGER.warning("Error checking nightly for %s: %s", group, e)
        return error_result("nightly_status", Status.RED, "Error", e)
