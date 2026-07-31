import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import click
import yaml
from artcommonlib import exectools
from artcommonlib.assembly import assembly_config_struct
from artcommonlib.gitlab import GitLabClient
from artcommonlib.oc_image_info import oc_image_info__cached_async

from doozerlib.cli import cli, click_coroutine, pass_runtime

LOGGER = logging.getLogger(__name__)

SKIPPED_IMAGES_PATTERNS = [
    re.compile(r"machine-os-content"),
    re.compile(r"rhel-coreos(?:-\d+)?"),
    re.compile(r"rhel-coreos(?:-\d+)?-extensions"),
]

CATALOG_API_URL = "https://catalog.redhat.com/api/containers/v1/images"
RELEASE_IMAGE_REPO = "quay.io/openshift-release-dev/ocp-release"


@dataclass
class ImageCheckResult:
    name: str
    pullspec: str
    found_in: Optional[str] = None
    match_details: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.found_in is not None


@dataclass
class VerifyImageConsistencyResult:
    payload_url: str
    shipment_mr_url: str
    payload_version: Optional[str] = None
    shipment_version: Optional[str] = None
    payload_image_count: int = 0
    shipment_component_count: int = 0
    skipped_images: list[str] = field(default_factory=list)
    results: list[ImageCheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failed_images(self) -> list[ImageCheckResult]:
        return [r for r in self.results if not r.passed]


def _is_skipped_image(name: str) -> bool:
    return any(p.fullmatch(name) for p in SKIPPED_IMAGES_PATTERNS)


@dataclass
class ImageIdentifiers:
    pullspec: str
    digest: str = ""
    list_digest: str = ""
    vcs_ref: str = ""
    name: str = ""


def identifiers_match(a: ImageIdentifiers, b: ImageIdentifiers) -> bool:
    if a.list_digest and a.list_digest == b.list_digest:
        return True
    if a.digest and a.digest == b.digest:
        return True
    if a.vcs_ref and a.vcs_ref == b.vcs_ref:
        return True
    return False


async def fetch_payload_images(payload_url: str) -> tuple[list[tuple[str, str]], str]:
    LOGGER.info("Fetching payload data from %s", payload_url)
    cmd = ["oc", "adm", "release", "info", "--pullspecs", payload_url, "-o", "json"]
    rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)
    if rc:
        raise RuntimeError(f"oc adm release info failed (rc={rc}): {stderr.strip()}")

    data = json.loads(stdout)
    version = data.get("metadata", {}).get("version", "")
    tags = data.get("references", {}).get("spec", {}).get("tags", [])

    images = []
    for tag in tags:
        name = tag.get("name", "")
        pullspec = tag.get("from", {}).get("name", "")
        if name and pullspec:
            images.append((name, pullspec))

    return images, version


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
    for file_diff in diff.diffs:
        file_path = file_diff.get("new_path") or file_diff.get("old_path")
        if not file_path or not file_path.endswith((".yaml", ".yml")):
            continue

        try:
            file_content = source_project.files.get(file_path, mr.source_branch)
            content = file_content.decode().decode("utf-8")
            data = yaml.safe_load(content)

            shipment = data.get("shipment") or {}
            snapshot = shipment.get("snapshot") or {}
            spec = snapshot.get("spec") or {}
            for comp in spec.get("components") or []:
                name = comp.get("name", "")
                pullspec = comp.get("containerImage", "")
                if pullspec:
                    components.append((name, pullspec))
        except Exception:
            LOGGER.warning("Failed to process shipment file %s in MR %s", file_path, mr_url, exc_info=True)
            continue

    return components, version


async def fetch_image_identifiers(pullspec: str) -> ImageIdentifiers:
    try:
        stdout = await oc_image_info__cached_async(pullspec, "--filter-by-os=linux/amd64", "--insecure=true")
        data = json.loads(stdout)
    except Exception:
        LOGGER.warning("Failed to fetch image metadata for %s", pullspec, exc_info=True)
        return ImageIdentifiers(pullspec=pullspec)

    labels = data.get("config", {}).get("config", {}).get("Labels", {})
    return ImageIdentifiers(
        pullspec=pullspec,
        digest=data.get("digest", ""),
        list_digest=data.get("listDigest", ""),
        vcs_ref=labels.get("vcs-ref", ""),
        name=labels.get("name", ""),
    )


async def check_catalog(digest: str) -> bool:
    if not digest:
        return False

    url = f"{CATALOG_API_URL}?filter=docker_image_digest=={digest}"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    LOGGER.warning("Red Hat Catalog API returned status %s", resp.status)
                    return False
                data = await resp.json()
                return data.get("total", 0) > 0
    except Exception:
        LOGGER.warning("Failed to query Red Hat Catalog API for digest %s", digest, exc_info=True)
        return False


async def verify_image_consistency(payload_url: str, shipment_mr_url: str) -> VerifyImageConsistencyResult:
    (payload_images, payload_version), (shipment_components, shipment_version) = await asyncio.gather(
        fetch_payload_images(payload_url),
        asyncio.to_thread(fetch_shipment_components, shipment_mr_url),
    )

    result = VerifyImageConsistencyResult(
        payload_url=payload_url,
        shipment_mr_url=shipment_mr_url,
        payload_version=payload_version,
        shipment_version=shipment_version,
        payload_image_count=len(payload_images),
        shipment_component_count=len(shipment_components),
    )

    images_to_check = []
    for name, pullspec in payload_images:
        if _is_skipped_image(name):
            result.skipped_images.append(name)
            LOGGER.info("Skipping RHCOS image: %s", name)
            continue
        images_to_check.append((name, pullspec))

    LOGGER.info(
        "Checking %d payload images against %d shipment components (%d skipped)",
        len(images_to_check),
        len(shipment_components),
        len(result.skipped_images),
    )

    all_pullspecs = set()
    for _, pullspec in images_to_check:
        all_pullspecs.add(pullspec)
    for _, pullspec in shipment_components:
        all_pullspecs.add(pullspec)

    pullspec_list = list(all_pullspecs)
    semaphore = asyncio.Semaphore(50)

    async def _throttled_fetch(ps: str) -> ImageIdentifiers:
        async with semaphore:
            return await fetch_image_identifiers(ps)

    fetched = await asyncio.gather(*(_throttled_fetch(ps) for ps in pullspec_list))
    identifiers: dict[str, ImageIdentifiers] = dict(zip(pullspec_list, fetched, strict=True))

    shipment_identifiers = [identifiers[ps] for _, ps in shipment_components if ps in identifiers]

    for name, pullspec in images_to_check:
        payload_id = identifiers.get(pullspec, ImageIdentifiers(pullspec=pullspec))
        check = ImageCheckResult(name=name, pullspec=pullspec)

        for ship_id in shipment_identifiers:
            if identifiers_match(payload_id, ship_id):
                check.found_in = "shipment"
                check.match_details = ship_id.pullspec
                break

        if not check.found_in:
            if await check_catalog(payload_id.digest):
                check.found_in = "catalog"

        if not check.found_in:
            LOGGER.error("Image %s (%s) not found in shipment or catalog", name, pullspec)

        result.results.append(check)

    return result


def render_result(result: VerifyImageConsistencyResult, output: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "payload_url": result.payload_url,
                "shipment_mr_url": result.shipment_mr_url,
                "payload_version": result.payload_version,
                "shipment_version": result.shipment_version,
                "payload_image_count": result.payload_image_count,
                "shipment_component_count": result.shipment_component_count,
                "skipped_images": result.skipped_images,
                "passed": result.passed,
                "failed_images": [{"name": r.name, "pullspec": r.pullspec} for r in result.failed_images],
                "results": [
                    {
                        "name": r.name,
                        "pullspec": r.pullspec,
                        "passed": r.passed,
                        "found_in": r.found_in,
                    }
                    for r in result.results
                ],
            },
            indent=2,
        )

    lines = [
        f"Image consistency check for payload {result.payload_version or result.payload_url}",
        f"Shipment MR: {result.shipment_mr_url}",
        f"Payload images: {result.payload_image_count} ({len(result.skipped_images)} RHCOS skipped)",
        f"Shipment components: {result.shipment_component_count}",
    ]

    if result.failed_images:
        lines.append("")
        lines.append("IMAGES NOT FOUND IN SHIPMENT OR CATALOG:")
        for r in result.failed_images:
            lines.append(f"  - {r.name}: {r.pullspec}")

    lines.append("")
    overall = "PASS" if result.passed else "FAIL"
    lines.append(f"Overall: {overall} ({len(result.results) - len(result.failed_images)}/{len(result.results)} passed)")
    return "\n".join(lines)


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


@cli.command("verify-image-consistency", short_help="Verify payload images match shipment MR components")
@click.option(
    "--arch",
    default="x86_64",
    show_default=True,
    help="Architecture for the release payload.",
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@pass_runtime
@click_coroutine
async def verify_image_consistency_cli(runtime, arch, output):
    """Verify that every image in the release payload is present in the
    shipment MR or has already been released in the Red Hat catalog.

    Requires --group and --assembly global options. Resolves the shipment
    MR URL from the assembly config and constructs the payload pullspec.

    Example:
        doozer --group openshift-4.18 --assembly 4.18.51 verify-image-consistency
    """
    runtime.initialize(config_only=True)
    shipment_mr_url = resolve_shipment_mr_url(runtime)
    payload_url = f"{RELEASE_IMAGE_REPO}:{runtime.assembly}-{arch}"
    LOGGER.info("Resolved shipment MR URL: %s", shipment_mr_url)
    LOGGER.info("Constructed payload URL: %s", payload_url)
    result = await verify_image_consistency(payload_url=payload_url, shipment_mr_url=shipment_mr_url)
    click.echo(render_result(result, output))
    if not result.passed:
        raise SystemExit(1)
