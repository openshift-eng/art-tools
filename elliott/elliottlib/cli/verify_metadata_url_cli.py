import json
import logging
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlsplit

import aiohttp
import click
from artcommonlib import exectools

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.verify_common import VerifyResultBase, handle_verify_result, verify_output_option

LOGGER = logging.getLogger(__name__)

PAYLOAD_TEMPLATE = "quay.io/openshift-release-dev/ocp-release:{assembly}-x86_64"


@dataclass
class VerifyMetadataUrlResult(VerifyResultBase):
    release: str = ""
    pullspec: str = ""
    metadata_url: str = ""
    accessible: bool = False
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.accessible and not self.error

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "failed": self.failed,
            "release": self.release,
            "pullspec": self.pullspec,
            "metadata_url": self.metadata_url,
            "accessible": self.accessible,
            "error": self.error,
        }

    def render_text(self) -> str:
        lines = ["Metadata URL check", ""]
        lines.append(f"  Release: {self.release}")
        if self.pullspec:
            lines.append(f"  Pullspec: {self.pullspec}")
        if self.metadata_url:
            lines.append(f"  Metadata URL: {self.metadata_url}")
            lines.append(f"  Accessible: {'yes' if self.accessible else 'no'}")
        if self.error:
            lines.append(f"  Error: {self.error}")
        lines.append("")
        overall = "PASS" if self.passed else "FAIL"
        lines.append(f"Overall: {overall}")
        return "\n".join(lines)


async def extract_metadata_url(pullspec: str, registry_config: str | None = None) -> str:
    cmd = ["oc", "adm", "release", "info", pullspec, "-o", "json"]
    if registry_config:
        cmd.append(f"--registry-config={registry_config}")
    rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)
    if rc != 0:
        raise RuntimeError(f"oc adm release info failed: {stderr.strip()}")
    release_info = json.loads(stdout)
    try:
        url = release_info["metadata"]["metadata"]["url"]
    except (KeyError, TypeError):
        raise RuntimeError("metadata.metadata.url not found in release info")
    if not url:
        raise RuntimeError("metadata.metadata.url is empty")
    return url


ALLOWED_METADATA_HOST = "access.redhat.com"


def validate_metadata_url(url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "https":
        raise ValueError(f"Metadata URL must use https, got {parsed.scheme}")
    if parsed.hostname != ALLOWED_METADATA_HOST:
        raise ValueError(f"Metadata URL host must be {ALLOWED_METADATA_HOST}, got {parsed.hostname}")
    if parsed.port is not None and parsed.port != 443:
        raise ValueError(f"Metadata URL must not specify a non-standard port, got {parsed.port}")
    if parsed.username or parsed.password:
        raise ValueError("Metadata URL must not contain credentials")


async def check_url_accessible(url: str) -> bool:
    # Redirects disabled to prevent SSRF via redirect chains
    validate_metadata_url(url)
    LOGGER.info("Checking accessibility of metadata URL")
    session = aiohttp.ClientSession()
    try:
        resp = await session.get(url, timeout=aiohttp.ClientTimeout(total=30), allow_redirects=False)
        try:
            LOGGER.info("HTTP %s for metadata URL", resp.status)
            return resp.status == 200
        finally:
            resp.release()
    finally:
        await session.close()


async def verify_metadata_url(release: str, registry_config: str | None = None) -> VerifyMetadataUrlResult:
    result = VerifyMetadataUrlResult(release=release, pullspec=PAYLOAD_TEMPLATE.format(assembly=release))
    try:
        LOGGER.info("Release %s pullspec: %s", release, result.pullspec)

        result.metadata_url = await extract_metadata_url(result.pullspec, registry_config=registry_config)
        result.accessible = await check_url_accessible(result.metadata_url)

    except Exception as e:
        LOGGER.error("Error verifying metadata URL for %s: %s", release, e)
        result.error = str(e)

    return result


@cli.command("verify-metadata-url", short_help="Check release payload metadata URL accessibility")
@verify_output_option
@click.pass_obj
@click_coroutine
async def verify_metadata_url_cli(runtime, output):
    """Check that the release payload metadata URL is accessible.

    Uses the exact release payload tag, extracts the metadata URL via 'oc adm release info', and verifies
    it returns HTTP 200. The metadata URL points to the customer-facing
    errata page on access.redhat.com.

    Requires --assembly with a production release version in X.Y.Z format.
    RC and EC releases are not supported.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.50 verify-metadata-url
    """
    release = runtime.assembly
    if not release or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", release):
        raise click.BadParameter(
            "Specify a release version in X.Y.Z format, for example 4.18.50.", param_hint="--assembly"
        )

    LOGGER.info("Verifying metadata URL for release %s", release)
    result = await verify_metadata_url(release=release, registry_config=runtime.registry_config)
    handle_verify_result(result, output)
