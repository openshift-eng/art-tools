from datetime import datetime, timezone
from typing import Optional

import aiohttp
import click
from artcommonlib import redis
from artcommonlib.util import isolate_major_minor_in_group
from doozerlib.cli.get_nightlies import get_nightly_tag_base
from doozerlib.util import rc_api_url

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime


class MonitorNightlyDelaysPipeline:
    """
    Monitor for nightly build delays and alert appropriate channels.
    - Alert release-artists in version-specific channel after 6 hours of no new nightly (regardless of status)
    - Alert TRT channel after 12 hours of no new nightly (only for pre-release versions)

    Note: TRT alerts are only sent when the OCP version is in 'pre-release' lifecycle phase
    (as defined in group.yml software_lifecycle.phase). For GA or post-GA versions, only
    the release-artists channel is alerted.
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        build_system: str,
        release_artists_channel: Optional[str],
        trt_channel: str,
    ):
        self.runtime = runtime
        self.group = group
        self.build_system = build_system
        self.trt_channel = trt_channel
        self._logger = runtime.logger
        self._slack_client = self.runtime.new_slack_client()

        # Parse OCP version from group
        major, minor = isolate_major_minor_in_group(group)
        self._ocp_version = f"{major}.{minor}"
        self._major = major
        self._minor = minor

        # Default to version-specific release artists channel if not provided
        self.release_artists_channel = release_artists_channel or f"#art-release-{major}-{minor}"

        # Redis keys for tracking alert state
        key_scope = f"{self.group}:{self.build_system}"
        self._redis_key_6h = f"nightly_delay_alert_6h:{key_scope}"
        self._redis_key_12h = f"nightly_delay_alert_12h:{key_scope}"

    async def run(self):
        """Main execution method for the pipeline"""
        self._logger.info(f"Monitoring nightly delays for {self.group}")

        # Load group config to check software lifecycle
        group_config = await util.load_group_config(self.group, assembly="stream")
        software_lifecycle = group_config.get('software_lifecycle', {})
        lifecycle_phase = software_lifecycle.get('phase', 'unknown')

        self._logger.info(f"Software lifecycle phase: {lifecycle_phase}")

        # Determine if we should alert TRT (only for pre-release versions)
        alert_trt = lifecycle_phase == 'pre-release'

        if not alert_trt:
            self._logger.info(
                f"Version {self._ocp_version} is in '{lifecycle_phase}' phase - will only alert "
                f"{self.release_artists_channel}, not TRT"
            )

        # Get latest nightly (regardless of status)
        latest_nightly = await self._get_latest_nightly()

        if not latest_nightly:
            self._logger.warning(f"Could not retrieve latest nightly for {self.group}")
            return

        nightly_name = latest_nightly.get('name')
        nightly_phase = latest_nightly.get('phase', 'Unknown')

        # Extract timestamp from nightly name (format: X.Y.Z-0.nightly-YYYY-MM-DD-HHMMSS)
        nightly_time = self._parse_nightly_timestamp(nightly_name)

        if not nightly_time:
            self._logger.warning(f"Could not parse timestamp from nightly name: {nightly_name}")
            return

        # Calculate delay
        current_time = datetime.now(timezone.utc)
        time_since_last = current_time - nightly_time
        hours_since_last = time_since_last.total_seconds() / 3600

        self._logger.info(
            f"Latest nightly: {nightly_name} (phase: {nightly_phase}) created {hours_since_last:.1f} hours ago"
        )

        # Check if we need to alert
        await self._check_and_alert(nightly_name, nightly_phase, nightly_time, hours_since_last, alert_trt)

    def _parse_nightly_timestamp(self, nightly_name: str) -> Optional[datetime]:
        """
        Parse timestamp from nightly name.

        Nightly names follow format: X.Y.Z-0.nightly-YYYY-MM-DD-HHMMSS
        or X.Y.Z-0.konflux-nightly-YYYY-MM-DD-HHMMSS

        Args:
            nightly_name: Name of the nightly build

        Returns:
            datetime object in UTC, or None if parsing fails
        """
        import re

        # Match the timestamp pattern in nightly names
        # Examples:
        # 4.22.0-0.nightly-2026-03-13-124748
        # 4.22.0-0.konflux-nightly-2026-03-13-124748
        pattern = r'(\d{4})-(\d{2})-(\d{2})-(\d{6})$'
        match = re.search(pattern, nightly_name)

        if not match:
            return None

        try:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            time_str = match.group(4)

            # Parse HHMMSS
            hour = int(time_str[0:2])
            minute = int(time_str[2:4])
            second = int(time_str[4:6])

            return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        except (ValueError, IndexError) as e:
            self._logger.error(f"Failed to parse timestamp from {nightly_name}: {e}")
            return None

    async def _get_latest_nightly(self) -> Optional[dict]:
        """
        Retrieve the latest nightly from the release controller (regardless of status).

        Returns:
            Dictionary containing nightly info including name, phase, and created timestamp,
            or None if unable to retrieve.
        """
        self._logger.info('Retrieving latest nightly from release controller...')

        tag_base = get_nightly_tag_base(self._major, self._minor, self.build_system)
        # Use x86_64/amd64 as the canonical arch for checking
        rc_endpoint = f"{rc_api_url(tag_base, 'amd64', private_nightly=False)}/tags"

        self._logger.info(f"Querying release controller: {rc_endpoint}")

        async with aiohttp.ClientSession() as session:
            async with session.get(rc_endpoint) as response:
                if response.status != 200:
                    self._logger.error(f'Failed retrieving nightlies from {rc_endpoint}: status {response.status}')
                    return None

                data = await response.json()
                tags = data.get('tags', [])

                if not tags:
                    self._logger.warning(f"No nightlies found in response from {rc_endpoint}")
                    return None

                # Return the most recent nightly (first in the list)
                return tags[0]

    async def _check_and_alert(
        self, nightly_name: str, nightly_phase: str, nightly_time: datetime, hours_since_last: float, alert_trt: bool
    ):
        """
        Check delay thresholds and send alerts to appropriate channels.

        Args:
            nightly_name: Name of the latest nightly
            nightly_phase: Phase/status of the nightly (Accepted, Rejected, Ready, etc.)
            nightly_time: Timestamp when the nightly was created
            hours_since_last: Hours elapsed since the nightly was created
            alert_trt: Whether to alert TRT (only for pre-release versions)
        """
        # 12-hour threshold (TRT alert) - only for pre-release versions
        if hours_since_last >= 12 and alert_trt:
            await self._alert_12h(nightly_name, nightly_phase, nightly_time, hours_since_last)

        # 6-hour threshold (release-artists alert)
        elif hours_since_last >= 6:
            await self._alert_6h(nightly_name, nightly_phase, nightly_time, hours_since_last)

        else:
            # No delay - clear any previous alerts
            await self._clear_alerts()

    async def _alert_6h(
        self,
        nightly_name: str,
        nightly_phase: str,
        nightly_time: datetime,
        hours_since_last: float,
    ):
        """Send 6-hour delay alert to release-artists channel"""

        # Check if we already alerted for this nightly at 6h threshold
        if not self.runtime.dry_run:
            last_alerted = await redis.get_value(self._redis_key_6h)
            if last_alerted == nightly_name:
                self._logger.info(f"Already alerted for 6h delay on {nightly_name}")
                return

        self._logger.warning(f"6-hour delay detected for {self.group}")

        # Send alert to release-artists channel
        self._slack_client.bind_channel(self.release_artists_channel)

        message = (
            f":warning: *Nightly Build Delay Alert for {self._ocp_version}* :warning:\n\n"
            f"No new nightly has been produced in the past *6 hours*.\n\n"
            f"Last nightly: `{nightly_name}` (Status: {nightly_phase})\n"
            f"Created: {nightly_time.strftime('%Y-%m-%d %H:%M:%S UTC')} "
            f"(*{hours_since_last:.1f} hours ago*)\n\n"
            # f"@release-artists please investigate."
        )

        await self._slack_client.say(message)

        # Mark this nightly as alerted (store with 48h expiry to avoid stale data)
        if not self.runtime.dry_run:
            await redis.set_value(self._redis_key_6h, nightly_name, expiry=172800)

    async def _alert_12h(
        self,
        nightly_name: str,
        nightly_phase: str,
        nightly_time: datetime,
        hours_since_last: float,
    ):
        """Send 12-hour delay alert to TRT channel"""

        # Check if we already alerted for this nightly at 12h threshold
        if not self.runtime.dry_run:
            last_alerted = await redis.get_value(self._redis_key_12h)
            if last_alerted == nightly_name:
                self._logger.info(f"Already alerted for 12h delay on {nightly_name}")
                return

        self._logger.error(f"12-hour delay detected for {self.group}")

        # Send alert to TRT channel
        self._slack_client.bind_channel(self.trt_channel)

        message = (
            f":rotating_light: *CRITICAL: Nightly Build Delay Alert for {self._ocp_version}* :rotating_light:\n\n"
            f"No new nightly has been produced in the past *12 hours*.\n\n"
            f"Last nightly: `{nightly_name}` (Status: {nightly_phase})\n"
            f"Created: {nightly_time.strftime('%Y-%m-%d %H:%M:%S UTC')} "
            f"(*{hours_since_last:.1f} hours ago*)\n\n"
            # f"@release-artists please investigate."
        )

        await self._slack_client.say(message)

        # Also send to release-artists if we haven't already
        if not self.runtime.dry_run:
            last_6h_alert = await redis.get_value(self._redis_key_6h)
            if last_6h_alert != nightly_name:
                self._slack_client.bind_channel(self.release_artists_channel)
                await self._slack_client.say(
                    f":rotating_light: *CRITICAL: 12-hour nightly delay for {self._ocp_version}* - "
                    f"see alert in {self.trt_channel}"
                )

        # Mark this nightly as alerted (store with 48h expiry)
        if not self.runtime.dry_run:
            await redis.set_value(self._redis_key_12h, nightly_name, expiry=172800)
            # Also mark 6h as alerted since 12h includes 6h
            await redis.set_value(self._redis_key_6h, nightly_name, expiry=172800)

    async def _clear_alerts(self):
        """Clear alert state when delay is resolved"""
        if not self.runtime.dry_run:
            await redis.delete_key(self._redis_key_6h)
            await redis.delete_key(self._redis_key_12h)
        self._logger.info("No delays detected, alerts cleared")


@cli.command('monitor-nightly-delays')
@click.option(
    '--group',
    required=True,
    help='OCP version group to monitor (e.g., openshift-4.22)',
)
@click.option(
    '--build-system',
    type=click.Choice(['brew', 'konflux']),
    default='konflux',
    help='Build system used for nightlies (default: konflux)',
)
@click.option(
    '--release-artists-channel',
    default=None,
    help='Slack channel to ping release-artists (default: #art-release-X.Y based on group)',
)
@click.option(
    '--trt-channel',
    default='#forum-ocp-release-oversight',
    help='Slack channel to ping TRT team (default: #forum-ocp-release-oversight)',
)
@pass_runtime
@click_coroutine
async def monitor_nightly_delays(
    runtime: Runtime,
    group: str,
    build_system: str,
    release_artists_channel: Optional[str],
    trt_channel: str,
):
    """
    Monitor for nightly build delays and alert appropriate channels.

    This command checks when the latest nightly build was produced (regardless of success/failure)
    for the specified OCP version and sends alerts when there are extended delays:
    - After 6 hours: Alert release-artists in version-specific channel (#art-release-X.Y)
    - After 12 hours: Alert TRT team in their channel (ONLY for pre-release versions)

    The monitoring checks for ANY nightly production, not just successful ones, since the
    important metric is whether the nightly build pipeline is running at all.

    TRT alerts are only sent when the OCP version's software_lifecycle.phase (from group.yml)
    is set to 'pre-release'. For GA or post-GA versions in other lifecycle phases, only the
    release-artists channel will be alerted.

    Designed to be run on a scheduled basis (e.g., hourly via cron) to monitor
    nightly build health.

    Example:
        artcd monitor-nightly-delays --group openshift-4.22
    """
    await MonitorNightlyDelaysPipeline(
        runtime,
        group,
        build_system,
        release_artists_channel,
        trt_channel,
    ).run()
