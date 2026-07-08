"""
Dev cut-off date resolution via Product Pages (PP).

Determines the next z-stream assembly name from releases.yml, then looks up
its dev cut-off date from the PP schedule API.
"""

import logging
import re
from datetime import date, datetime

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from pyartcd import util as pyartcd_util
from pyartcd.pipelines.release_readiness.helpers import format_relative_days

_LOGGER = logging.getLogger(__name__)

PP_BASE_URL = "https://pp.engineering.redhat.com"


async def get_next_dev_cut_off(group: str, ocp_version: str) -> str | None:
    """
    Determine the next z-stream assembly from releases.yml, then look up
    its dev cut off date from Product Pages.

    Arg(s):
        group (str): OCP group (e.g. "openshift-4.21").
        ocp_version (str): OCP version string (e.g. "4.21").

    Return Value(s):
        str | None: Formatted dev cut off string, or None if unavailable.
    """

    try:
        next_assembly = await _get_next_assembly_name(group, ocp_version)
        if not next_assembly:
            return None

        cut_date = await _get_dev_cut_off_from_pp(next_assembly, ocp_version)
        if not cut_date:
            return f"{next_assembly} — dev cut off not found in PP"

        today = datetime.now().date()
        relative = format_relative_days((cut_date - today).days)
        return f"{next_assembly} — {cut_date} ({relative})"

    except Exception as e:
        _LOGGER.warning("Could not fetch dev cut off: %s", e)
        return None


async def _get_next_assembly_name(group: str, ocp_version: str) -> str | None:
    """
    Read releases.yml to find the highest existing z-stream assembly,
    then return the next one (e.g. 4.20.8 exists -> return 4.20.9).
    """

    releases_config = await pyartcd_util.load_releases_config(group)
    if not releases_config:
        return None

    pattern = re.compile(rf"^{re.escape(ocp_version)}\.(\d+)$")
    max_z = max(
        (int(m.group(1)) for name in releases_config.get("releases", {}) if (m := pattern.match(name))),
        default=-1,
    )
    return f"{ocp_version}.{max_z + 1}"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
    before_sleep=lambda rs: _LOGGER.warning("PP request failed, retrying (attempt %d)...", rs.attempt_number),
)
async def _get_dev_cut_off_from_pp(assembly_name: str, ocp_version: str) -> date | None:
    """
    Query Product Pages for the dev cut off date of a specific assembly.
    Retries the entire flow (auth + queries) on transient failures.

    Arg(s):
        assembly_name (str): Assembly name (e.g. "4.21.9").
        ocp_version (str): OCP version string (e.g. "4.21").

    Return Value(s):
        date | None: Dev cut off date, or None if not found.
    """

    try:
        import requests_gssapi
    except ImportError:
        _LOGGER.warning("requests_gssapi not available, skipping PP query")
        return None

    session = requests.Session()
    auth = requests_gssapi.HTTPSPNEGOAuth(mutual_authentication=requests_gssapi.OPTIONAL)
    session.post(f"{PP_BASE_URL}/oidc/authenticate", auth=auth, verify=True, timeout=30)

    schedule_id = _find_schedule_id(session, ocp_version)
    if not schedule_id:
        return None

    resp = session.get(f"{PP_BASE_URL}/api/v7/schedules/{schedule_id}/tasks", timeout=30)
    resp.raise_for_status()

    for task in resp.json():
        if "dev" not in task.get("flags", []):
            continue
        if assembly_name not in task.get("name", ""):
            continue
        date_str = task.get("date_finish")
        if date_str:
            return datetime.strptime(date_str, "%Y-%m-%d").date()

    return None


def _find_schedule_id(session: requests.Session, ocp_version: str) -> int | None:
    """
    Find the PP schedule ID for this version's z-stream.
    """

    resp = session.get(f"{PP_BASE_URL}/api/v7/schedules/", timeout=30)
    resp.raise_for_status()

    for sched in resp.json():
        name = sched.get("name", "").lower()
        if sched.get("is_active") and f"{ocp_version}.z" in name:
            return sched["id"]
    return None
