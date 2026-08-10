import json
import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp
import click
from artcommonlib import exectools

from elliottlib.cli.common import cli, click_coroutine

LOGGER = logging.getLogger(__name__)

RELEASE_STREAM_API = "https://amd64.ocp.releases.ci.openshift.org/api/v1/releasestream"


@dataclass
class VerifyMetadataUrlResult:
    release: str
    pullspec: str = ""
    metadata_url: str = ""
    accessible: bool = False
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.accessible and not self.error

    @property
    def failed(self) -> bool:
        return not self.accessible or bool(self.error)


async def get_release_pullspec(release: str) -> str:
    major = release.split(".")[0]
    url = f"{RELEASE_STREAM_API}/{major}-stable/latest?prefix={release}"
    LOGGER.info("Fetching pullspec from %s", url)
    session = aiohttp.ClientSession()
    try:
        resp = await session.get(url, timeout=aiohttp.ClientTimeout(total=30))
        try:
            if not resp.ok:
                raise RuntimeError(f"Release stream API returned {resp.status} for {url}")
            data = await resp.json()
            pullspec = data.get("pullSpec")
            if not pullspec:
                raise RuntimeError(f"No pullSpec in release stream response for {release}")
            return pullspec
        finally:
            resp.release()
    finally:
        await session.close()


async def extract_metadata_url(pullspec: str) -> str:
    cmd = ["oc", "adm", "release", "info", pullspec, "-o", "json"]
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


async def check_url_accessible(url: str) -> bool:
    LOGGER.info("Checking accessibility of %s", url)
    session = aiohttp.ClientSession()
    try:
        resp = await session.get(url, timeout=aiohttp.ClientTimeout(total=30))
        try:
            LOGGER.info("HTTP %s for %s", resp.status, url)
            return resp.ok
        finally:
            resp.release()
    finally:
        await session.close()


async def verify_metadata_url(release: str) -> VerifyMetadataUrlResult:
    result = VerifyMetadataUrlResult(release=release)
    try:
        result.pullspec = await get_release_pullspec(release)
        LOGGER.info("Release %s pullspec: %s", release, result.pullspec)

        result.metadata_url = await extract_metadata_url(result.pullspec)
        LOGGER.info("Metadata URL: %s", result.metadata_url)

        result.accessible = await check_url_accessible(result.metadata_url)

    except Exception as e:
        LOGGER.error("Error verifying metadata URL for %s: %s", release, e)
        result.error = str(e)

    return result


def render_result(result: VerifyMetadataUrlResult, output: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "passed": result.passed,
                "failed": result.failed,
                "release": result.release,
                "pullspec": result.pullspec,
                "metadata_url": result.metadata_url,
                "accessible": result.accessible,
                "error": result.error,
            },
            indent=2,
        )

    lines = ["Metadata URL check", ""]
    lines.append(f"  Release: {result.release}")
    if result.pullspec:
        lines.append(f"  Pullspec: {result.pullspec}")
    if result.metadata_url:
        lines.append(f"  Metadata URL: {result.metadata_url}")
        lines.append(f"  Accessible: {'yes' if result.accessible else 'no'}")
    if result.error:
        lines.append(f"  Error: {result.error}")
    lines.append("")

    overall = "PASS" if result.passed else "FAIL"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)


@cli.command("verify-metadata-url", short_help="Check release payload metadata URL accessibility")
@click.option(
    "-o",
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.pass_obj
@click_coroutine
async def verify_metadata_url_cli(runtime, output):
    """Check that the release payload metadata URL is accessible.

    Fetches the release pullspec from the release stream API,
    extracts the metadata URL via 'oc adm release info', and verifies
    it returns HTTP 200. The metadata URL points to the customer-facing
    errata page on access.redhat.com.

    Requires --group and --assembly global options. Uses the assembly name
    as the release version prefix for the release stream API query.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.50 verify-metadata-url
    """
    runtime.initialize()
    release = runtime.assembly

    LOGGER.info("Verifying metadata URL for release %s", release)
    result = await verify_metadata_url(release=release)
    click.echo(render_result(result, output))
    if not result.passed:
        raise SystemExit(1)
