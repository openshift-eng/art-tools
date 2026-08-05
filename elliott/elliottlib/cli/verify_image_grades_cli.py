import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse

import aiohttp
import click
import requests
import yaml
from artcommonlib.assembly import assembly_config_struct
from artcommonlib.gitlab import GitLabClient

from elliottlib.cli.common import cli, click_coroutine

LOGGER = logging.getLogger(__name__)

PYXIS_API_URL = "https://catalog.stage.redhat.com/api/containers/v1/images"
PYXIS_PROXY = "http://squid.corp.redhat.com:3128"
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
ADVISORY_ALLOWED_ORIGINS = {
    "https://errata.devel.redhat.com",
    "https://errata.stage.engineering.redhat.com",
    "https://gitlab.cee.redhat.com/rhtap-release/advisories",
    "https://gitlab.cee.redhat.com/releng/advisories",
}


@dataclass
class ImageGradeResult:
    name: str
    pullspec: str
    digest: str
    grade: str = "Unknown"
    available: bool = True

    @property
    def healthy(self) -> bool:
        return self.grade in ("A", "B")


@dataclass
class VerifyImageGradesResult:
    shipment_mr_url: str
    shipment_version: str = ""
    results: list[ImageGradeResult] = field(default_factory=list)

    @property
    def total_scanned(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> bool:
        return bool(self.results) and all(r.healthy for r in self.results)

    @property
    def unhealthy_images(self) -> list[ImageGradeResult]:
        return [r for r in self.results if not r.healthy]

    @property
    def unknown_count(self) -> int:
        return sum(1 for r in self.results if r.grade == "Unknown")

    @property
    def unavailable_images(self) -> list[ImageGradeResult]:
        return [r for r in self.results if not r.available]


def extract_digest(pullspec: str) -> str:
    if "@sha256:" in pullspec:
        return pullspec.split("@sha256:", 1)[1]
    return ""


def _parse_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_current_grade(grades: list[dict]) -> str:
    if not grades:
        return "Unknown"

    now = datetime.now(timezone.utc)
    valid = []
    for g in grades:
        start = g.get("start_date")
        if not start:
            LOGGER.warning("Skipping grade entry with no start_date: %s", g)
            continue
        try:
            if _parse_timestamp(start) <= now:
                valid.append(g)
        except (ValueError, TypeError):
            LOGGER.warning("Skipping grade entry with unparseable start_date: %s", start)
            continue

    if not valid:
        return "Unknown"

    valid.sort(key=lambda g: _parse_timestamp(g["start_date"]), reverse=True)
    return valid[0].get("grade", "Unknown")


def resolve_shipment_mr_url(runtime) -> str:
    releases_config = runtime.get_releases_config()
    assembly_group_config = assembly_config_struct(releases_config, runtime.assembly, "group", {})
    shipment = assembly_group_config.get("shipment", {})
    url = shipment.get("url")
    if not url:
        raise RuntimeError(
            f"No shipment URL found in assembly '{runtime.assembly}' group config. "
            f"Ensure releases.yml has releases.{runtime.assembly}.assembly.group.shipment.url set."
        )
    return url


def fetch_shipment_components(mr_url: str) -> tuple[list[tuple[str, str]], str]:
    gl = GitLabClient.from_url(mr_url)
    mr = gl.get_mr_from_url(mr_url)
    source_project = gl.get_project(mr.source_project_id)

    title = mr.title or ""
    match = re.search(r"Shipment for (\d+\.\d+\.\d+(?:-\S+)?)", title, re.IGNORECASE)
    version = match.group(1) if match else ""

    diff_versions = mr.diffs.list(all=True)
    if not diff_versions:
        raise RuntimeError(f"No diff versions found for MR {mr_url}")
    diff_info = diff_versions[0]
    diff = mr.diffs.get(diff_info.id)

    components: list[tuple[str, str]] = []
    failures: list[str] = []
    for file_diff in diff.diffs:
        file_path = file_diff.get("new_path") or file_diff.get("old_path")
        if not file_path or not file_path.endswith((".yaml", ".yml")):
            continue
        if ".fbc." in file_path.lower():
            continue

        try:
            file_content = source_project.files.get(file_path, mr.source_branch)
            content = file_content.decode().decode("utf-8")
            data = yaml.safe_load(content)
            if not isinstance(data, dict):
                LOGGER.warning("Shipment file %s is not a YAML mapping; skipping", file_path)
                continue

            shipment = data.get("shipment") or {}
            environments = shipment.get("environments") or {}
            stage = environments.get("stage") or {}
            advisory = stage.get("advisory") or {}
            advisory_url = advisory.get("internal_url")
            if not advisory_url:
                LOGGER.debug("No advisory internal_url in %s, skipping", file_path)
                continue

            if not any(advisory_url.startswith(origin) for origin in ADVISORY_ALLOWED_ORIGINS):
                parsed = urlparse(advisory_url)
                raise RuntimeError(f"Advisory URL not allowed: {parsed.hostname}")

            advisory_resp = requests.get(advisory_url, timeout=30, allow_redirects=False)
            advisory_resp.raise_for_status()
            advisory_data = yaml.safe_load(advisory_resp.text)

            spec = (advisory_data or {}).get("spec") or {}
            for image in spec.get("content", {}).get("images", []):
                name = image.get("component", "")
                pullspec = image.get("containerImage", "")
                if pullspec:
                    components.append((name, pullspec))
        except Exception:
            LOGGER.warning("Failed to process shipment file %s in MR %s", file_path, mr_url, exc_info=True)
            failures.append(file_path)
            continue

    if failures and not components:
        raise RuntimeError(f"Failed to read any shipment file from MR {mr_url}: {failures}")

    return components, version


async def query_freshness_grades(session: aiohttp.ClientSession, digest: str) -> tuple[list[dict], bool]:
    """Returns (grades, available). available=False means the lookup itself failed."""
    if not DIGEST_RE.match(digest):
        LOGGER.warning("Rejecting malformed image digest: %s", digest)
        return [], False
    params = {
        "filter": f"image_id==sha256:{digest}",
        "page_size": "1",
        "page": "0",
        "include": "data.freshness_grades",
    }
    try:
        async with session.get(PYXIS_API_URL, params=params, proxy=PYXIS_PROXY) as resp:
            if resp.status != 200:
                LOGGER.warning("Pyxis API returned status %s for digest %s", resp.status, digest)
                return [], False
            data = await resp.json()
            if not data.get("data"):
                return [], True
            return data["data"][0].get("freshness_grades", []), True
    except Exception:
        LOGGER.warning("Failed to query Pyxis API for digest %s", digest, exc_info=True)
        return [], False


async def verify_image_grades(shipment_mr_url: str) -> VerifyImageGradesResult:
    components, version = await asyncio.to_thread(fetch_shipment_components, shipment_mr_url)

    result = VerifyImageGradesResult(
        shipment_mr_url=shipment_mr_url,
        shipment_version=version,
    )

    LOGGER.info("Checking freshness grades for %d shipment components", len(components))

    semaphore = asyncio.Semaphore(20)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:

        async def _check_grade(name: str, pullspec: str) -> ImageGradeResult:
            digest = extract_digest(pullspec)
            if not digest:
                LOGGER.warning("No digest found in pullspec %s", pullspec)
                return ImageGradeResult(name=name, pullspec=pullspec, digest="", available=False)

            async with semaphore:
                grades, available = await query_freshness_grades(session, digest)

            grade = get_current_grade(grades)
            LOGGER.debug("Grade for %s: %s (available=%s)", name, grade, available)
            return ImageGradeResult(name=name, pullspec=pullspec, digest=digest, grade=grade, available=available)

        results = await asyncio.gather(*(_check_grade(n, ps) for n, ps in components))

    result.results = list(results)
    return result


def render_result(result: VerifyImageGradesResult, output: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "shipment_mr_url": result.shipment_mr_url,
                "shipment_version": result.shipment_version,
                "total_scanned": result.total_scanned,
                "passed": result.passed,
                "unhealthy_count": len(result.unhealthy_images),
                "unknown_count": result.unknown_count,
                "unavailable_count": len(result.unavailable_images),
                "unhealthy_images": [
                    {"name": r.name, "pullspec": r.pullspec, "grade": r.grade} for r in result.unhealthy_images
                ],
                "unavailable_images": [{"name": r.name, "pullspec": r.pullspec} for r in result.unavailable_images],
                "results": [
                    {
                        "name": r.name,
                        "pullspec": r.pullspec,
                        "digest": r.digest,
                        "grade": r.grade,
                        "healthy": r.healthy,
                        "available": r.available,
                    }
                    for r in result.results
                ],
            },
            indent=2,
        )

    lines = [
        f"Image freshness grade check for shipment {result.shipment_version or result.shipment_mr_url}",
        f"Shipment MR: {result.shipment_mr_url}",
        f"Images scanned: {result.total_scanned}",
    ]

    if result.unavailable_images:
        lines.append("")
        lines.append("UNAVAILABLE (grade lookup failed):")
        for r in result.unavailable_images:
            lines.append(f"  - {r.name}: {r.pullspec}")

    graded_unhealthy = [r for r in result.unhealthy_images if r.available]
    if graded_unhealthy:
        lines.append("")
        lines.append("UNHEALTHY IMAGES:")
        for r in graded_unhealthy:
            lines.append(f"  - {r.name} (grade {r.grade}): {r.pullspec}")

    lines.append("")
    overall = "PASS" if result.passed else "FAIL"
    healthy_count = result.total_scanned - len(result.unhealthy_images)
    lines.append(f"Overall: {overall} ({healthy_count}/{result.total_scanned} healthy)")
    return "\n".join(lines)


@cli.command("verify-image-grades", short_help="Check Pyxis freshness grades for shipment images")
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
async def verify_image_grades_cli(runtime, output):
    """Check Pyxis freshness grades for container images in a shipment MR.

    Requires --group and --assembly global options. Resolves the shipment
    MR URL from the assembly config in releases.yml.

    Queries the Red Hat Catalog (Pyxis) API for each image's freshness
    grade. Images with grade worse than B or Unknown are flagged as
    unhealthy. Exits with code 1 if any unhealthy images are found.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.51 verify-image-grades
    """
    runtime.initialize()
    shipment_mr_url = resolve_shipment_mr_url(runtime)
    LOGGER.info("Resolved shipment MR URL: %s", shipment_mr_url)
    result = await verify_image_grades(shipment_mr_url=shipment_mr_url)
    click.echo(render_result(result, output))
    if not result.passed:
        raise SystemExit(1)
