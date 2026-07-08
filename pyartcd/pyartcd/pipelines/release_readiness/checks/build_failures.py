"""
Build failures check — runs doozer images:health to find failing image builds.
"""

import json
import logging

from artcommonlib import exectools

from pyartcd.pipelines.release_readiness.helpers import error_result
from pyartcd.pipelines.release_readiness.models import MAX_DETAIL_ITEMS, CheckResult, Status

_LOGGER = logging.getLogger(__name__)


async def check_build_failures(group: str, build_system: str, doozer_working: str) -> CheckResult:
    """
    Check for failing image builds. Informational only (always GREEN status).

    Arg(s):
        group (str): OCP group (e.g. "openshift-4.21").
        build_system (str): Build system ("brew" or "konflux").
        doozer_working (str): Working directory for doozer.

    Return Value(s):
        CheckResult: Build failures check result.
    """

    _LOGGER.info("Checking build failures for %s", group)

    cmd = [
        "doozer",
        "--group",
        group,
        "--assembly",
        "stream",
        "--build-system",
        build_system,
        f"--working-dir={doozer_working}",
        "images:health",
    ]

    try:
        _, stdout, _ = await exectools.cmd_gather_async(cmd)
        concerns = json.loads(stdout)
    except Exception as e:
        _LOGGER.warning("Error checking build failures for %s: %s", group, e)
        return error_result("build_failures", Status.GREEN, "Could not check build health", e)

    failure_codes = {"FAILING_AT_LEAST_FOR", "LATEST_ATTEMPT_FAILED"}
    failing = list(dict.fromkeys(c.get("image_name", "unknown") for c in concerns if c.get("code") in failure_codes))

    if not failing:
        return CheckResult(
            name="build_failures",
            status=Status.GREEN,
            summary="No build failures ✅",
            details=["  All images healthy ✅"],
        )

    names_str = ", ".join(failing[:MAX_DETAIL_ITEMS])
    if len(failing) > MAX_DETAIL_ITEMS:
        names_str += f", ... (+{len(failing) - MAX_DETAIL_ITEMS} more)"

    return CheckResult(
        name="build_failures",
        status=Status.GREEN,
        summary=f"{len(failing)} image(s) failing ⚠️",
        details=[f"  Failing: {len(failing)} [{names_str}]"],
    )
