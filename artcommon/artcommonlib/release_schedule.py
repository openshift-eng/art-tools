"""
Release schedule API client for fetching OCP release schedule data.
"""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
import requests_gssapi
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.util import get_ocp_version_from_group, is_future_release_date

LOGGER = logging.getLogger(__name__)


class ReleaseScheduleClient:
    """Client for the release schedule API."""

    RELEASE_SCHEDULES_BASE = "https://pp.engineering.redhat.com/api/v7/releases"
    AUTH_URL = "https://pp.engineering.redhat.com/oidc/authenticate"
    GA_TASKS_FIELD = "all_ga_tasks"
    INFLIGHT_DAYS_THRESHOLD = 5
    RELEASE_NEXT_WEEK_DAYS = 7

    def _fetch_ga_tasks(
        self,
        group: str,
        assembly_type: AssemblyTypes = AssemblyTypes.STANDARD,
        assembly_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all_ga_tasks from release schedule API for a group."""
        s = requests.Session()
        auth = requests_gssapi.HTTPSPNEGOAuth(mutual_authentication=requests_gssapi.OPTIONAL)
        s.post(self.AUTH_URL, auth=auth)

        pre_ga_release = assembly_type in (AssemblyTypes.CANDIDATE, AssemblyTypes.PREVIEW)
        standard_ga_release = assembly_type == AssemblyTypes.STANDARD and assembly_name and assembly_name.endswith('.0')

        if pre_ga_release or standard_ga_release:
            path = f'releases/{group}/'
        else:
            path = f'{group}.z/'
        response = s.get(
            f'{self.RELEASE_SCHEDULES_BASE}/{path}?fields={self.GA_TASKS_FIELD}',
            headers={'Accept': 'application/json'},
        )
        response.raise_for_status()
        data = response.json()
        return data.get(self.GA_TASKS_FIELD, [])

    def get_assembly_release_date(
        self,
        assembly_name: str,
        group: str,
        assembly_type: AssemblyTypes = AssemblyTypes.STANDARD,
    ) -> str:
        """
        Get assembly release date from release schedule API.

        :raises ValueError: If the assembly release date is not found
        """
        for release in self._fetch_ga_tasks(group, assembly_type, assembly_name):
            if assembly_name in release['name']:
                # convert date format for advisory usage, 2024-02-13 -> 2024-Feb-13
                return datetime.strptime(release['date_start'], "%Y-%m-%d").strftime("%Y-%b-%d")
        raise ValueError(f'Assembly release date not found for {assembly_name}')

    def is_release_next_week(self, group: str) -> bool:
        """Check if release of group is scheduled for the near week."""
        for release in self._fetch_ga_tasks(group, AssemblyTypes.STANDARD):
            release_date = datetime.strptime(release['date_finish'], "%Y-%m-%d").date()
            if release_date > date.today() and release_date <= date.today() + timedelta(
                days=self.RELEASE_NEXT_WEEK_DAYS
            ):
                return True
        return False

    def get_inflight(
        self,
        assembly_name: str,
        group: str,
        assembly_type: AssemblyTypes = AssemblyTypes.STANDARD,
    ) -> Optional[str]:
        """Get inflight release name from current assembly release."""
        inflight_release = None
        assembly_release_date = self.get_assembly_release_date(assembly_name, group, assembly_type)
        major, minor = get_ocp_version_from_group(group)

        # Only look for previous group if minor > 0 to avoid negative minor versions
        if minor > 0:
            prev_group = f'openshift-{major}.{minor - 1}'
            try:
                for release in self._fetch_ga_tasks(prev_group, assembly_type, assembly_name):
                    if is_future_release_date(release['date_start']):
                        days_diff = abs(
                            (
                                datetime.strptime(assembly_release_date, "%Y-%b-%d")
                                - datetime.strptime(release['date_start'], "%Y-%m-%d")
                            ).days
                        )
                        if days_diff <= self.INFLIGHT_DAYS_THRESHOLD:  # next Y-1 and assembly in same week
                            match = re.search(r'\d+\.\d+\.\d+', release['name'])
                            if match:
                                inflight_release = match.group()
                                break
                            raise ValueError(f"Didn't find in_inflight release in {release['name']}")
            except ValueError as e:
                if "time data" in str(e) or "does not match format" in str(e):
                    raise ValueError(
                        f"Invalid date format when comparing assembly_release_date with "
                        f"release['date_start'] for {prev_group}: {e}"
                    ) from e
                raise
            except KeyError as e:
                raise ValueError(f'Failed to parse release schedule data for {prev_group}: {e}') from e

            if not inflight_release:
                LOGGER.info(
                    'Did not find a %s release that is releasing ~ in the same week as %s %s',
                    prev_group,
                    assembly_name,
                    assembly_release_date,
                )
            else:
                LOGGER.info(
                    'Found %s as in-flight release for %s %s',
                    inflight_release,
                    assembly_name,
                    assembly_release_date,
                )
        else:
            LOGGER.info('No previous group available for %s (minor version is 0)', group)

        return inflight_release
