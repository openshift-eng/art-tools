"""
Build-sync check — queries Jenkins for the latest build-sync-konflux job status.
"""

import logging
from datetime import datetime, timezone

from pyartcd import constants as pyartcd_constants
from pyartcd import jenkins
from pyartcd.pipelines.release_readiness.helpers import error_result, format_age
from pyartcd.pipelines.release_readiness.models import CheckResult, Status

_LOGGER = logging.getLogger(__name__)

BUILD_SYNC_JOB_URL = f"{pyartcd_constants.JENKINS_UI_URL}/job/aos-cd-builds/job/build%252Fbuild-sync-konflux"


async def check_build_sync(ocp_version: str) -> CheckResult:
    """
    Check last build-sync-konflux job status. Informational only.

    Arg(s):
        ocp_version (str): OCP version string (e.g. "4.21").

    Return Value(s):
        CheckResult: Build-sync check result.
    """

    _LOGGER.info("Checking build-sync status for %s", ocp_version)

    try:
        try:
            jenkins.init_jenkins()
        except Exception:
            return CheckResult(name="build_sync", status=Status.GREEN, summary="Skipped (no Jenkins credentials) ⏭️")

        data = _query_jenkins_build_sync()
        if data is None:
            return CheckResult(name="build_sync", status=Status.GREEN, summary="Could not query Jenkins ⚠️")

        return _parse_build_sync_result(data, ocp_version)

    except Exception as e:
        _LOGGER.warning("Error checking build-sync for %s: %s", ocp_version, e)
        return error_result("build_sync", Status.YELLOW, "Could not check build-sync", e)


def _query_jenkins_build_sync() -> dict | None:
    """
    Query Jenkins for recent build-sync-konflux builds.
    """

    job_path = "job/aos-cd-builds/job/build%252Fbuild-sync-konflux"
    api_url = (
        f"{pyartcd_constants.JENKINS_SERVER_URL}/{job_path}/api/json"
        f"?tree=builds[number,result,timestamp,displayName,actions[parameters[name,value]]]{{0,100}}"
    )
    resp = jenkins.jenkins_client.requester.get_url(api_url)
    if resp.status_code != 200:
        return None
    return resp.json()


def _parse_build_sync_result(data: dict, ocp_version: str) -> CheckResult:
    """
    Find the latest build-sync for this version and return its status.
    """

    now = datetime.now(timezone.utc)

    for build in data.get("builds", []):
        params = _extract_jenkins_params(build)
        if params.get("BUILD_VERSION") != ocp_version:
            continue

        age_minutes = (now - datetime.fromtimestamp(build["timestamp"] / 1000, tz=timezone.utc)).total_seconds() / 60
        age_str = format_age(age_minutes)
        build_num = build.get("number", "?")
        result = build.get("result")
        is_unviable = "[UNVIABLE]" in build.get("displayName", "")

        build_link = f"[#{build_num}]({BUILD_SYNC_JOB_URL}/{build_num})"

        if result == "SUCCESS" and not is_unviable:
            return CheckResult(
                name="build_sync",
                status=Status.GREEN,
                summary=f"{build_link} ({age_str}) ✅",
                details=["  Assembly: viable ✅"],
            )
        elif result is None:
            return CheckResult(
                name="build_sync",
                status=Status.GREEN,
                summary=f"{build_link} in progress ({age_str}) ⏳",
            )
        elif is_unviable:
            return CheckResult(
                name="build_sync",
                status=Status.YELLOW,
                summary=f"{build_link} UNVIABLE ({age_str}) ⚠️",
                details=["  Assembly: UNVIABLE (no new nightly will be produced) ⚠️"],
            )
        else:
            return CheckResult(
                name="build_sync",
                status=Status.YELLOW,
                summary=f"{build_link} {result} ({age_str}) ❌",
                details=[f"  Result: {result} ❌"],
            )

    return CheckResult(name="build_sync", status=Status.YELLOW, summary=f"No recent build-sync for {ocp_version} ⚠️")


def _extract_jenkins_params(build: dict) -> dict[str, str]:
    """
    Extract parameters from a Jenkins build API response.
    """

    for action in build.get("actions", []):
        if action and action.get("_class") == "hudson.model.ParametersAction":
            return {p.get("name", ""): p.get("value", "") for p in action.get("parameters", [])}
    return {}
