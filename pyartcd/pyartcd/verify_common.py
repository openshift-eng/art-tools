"""Shared helpers for verify-* artcd commands."""

import logging

from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)


async def notify_verify_result(runtime: Runtime, version: str, assembly: str, passed: bool) -> None:
    """Use the standard Slack job attachment without changing the verification result."""
    if runtime.dry_run:
        LOGGER.info("[DRY-RUN] Skipping verification Slack notification for %s/%s", version, assembly)
        return

    try:
        slack_client = runtime.new_slack_client()
        if not slack_client.build_url:
            raise ValueError("BUILD_URL is required for verification Slack notifications")
        slack_client.bind_channel(version)
        await slack_client.say(
            ":white_check_mark: PASS" if passed else ":x: FAIL", unfurl_links=False, unfurl_media=False
        )
    except Exception:
        LOGGER.warning("Unable to send verification Slack result for %s/%s", version, assembly, exc_info=True)
