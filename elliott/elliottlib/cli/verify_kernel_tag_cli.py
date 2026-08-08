import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

import click
import koji
import requests
from artcommonlib.assembly import assembly_config_struct
from artcommonlib.constants import BREW_DOWNLOAD_URL, BREW_HUB

from elliottlib import brew
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.errata_async import AsyncErrataAPI

LOGGER = logging.getLogger(__name__)

KERNEL_TAG_ADVISORY_TYPES = ("image", "rhcos")


@dataclass
class KernelBuildInfo:
    nvr: str
    has_stop_ship: bool = False


@dataclass
class AdvisoryKernelResult:
    advisory_id: int
    impetus: str
    rhcos_builds: list[str] = field(default_factory=list)
    kernel_builds: list[KernelBuildInfo] = field(default_factory=list)
    skipped: bool = False
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return not any(k.has_stop_ship for k in self.kernel_builds) and not self.error

    @property
    def failed(self) -> bool:
        return any(k.has_stop_ship for k in self.kernel_builds) or bool(self.error)


@dataclass
class VerifyKernelTagResult:
    advisories: list[AdvisoryKernelResult] = field(default_factory=list)
    stop_ship_tag: str = ""

    @property
    def passed(self) -> bool:
        return bool(self.advisories) and all(a.passed or a.skipped for a in self.advisories)

    @property
    def failed(self) -> bool:
        return any(a.failed for a in self.advisories)


def get_rpm_deliveries_config(runtime) -> list:
    rpm_deliveries = runtime.group_config.get("rpm_deliveries")
    if not rpm_deliveries:
        return []
    return rpm_deliveries.primitive() if hasattr(rpm_deliveries, "primitive") else rpm_deliveries


def get_kernel_packages_and_tag(rpm_deliveries: list) -> tuple[set[str], str]:
    packages = set()
    tags = set()
    for entry in rpm_deliveries:
        tag = entry.get("stop_ship_tag", "")
        if tag:
            packages.update(entry.get("packages", []))
            tags.add(tag)
    if len(tags) > 1:
        raise ValueError(f"Multiple different stop_ship_tag values in rpm_deliveries: {tags}")
    return packages, tags.pop() if tags else ""


def find_rhcos_nvrs(build_nvrs: set[str]) -> list[str]:
    return sorted(nvr for nvr in build_nvrs if nvr.startswith("rhcos-"))


def nvr_to_brewroot_metadata_url(rhcos_nvr: str) -> str:
    match = re.fullmatch(r"([A-Za-z0-9_.+-]+?)-([\d.]+)-(\d+)", rhcos_nvr)
    if not match:
        raise ValueError(f"Cannot parse RHCOS NVR: {rhcos_nvr}")
    name, version, release = match.groups()
    return f"{BREW_DOWNLOAD_URL}/packages/{name}/{version}/{release}/metadata.json"


def get_kernel_nvrs_from_metadata(metadata: dict, kernel_packages: set[str]) -> list[str]:
    kernel_nvrs = []
    for entry in metadata.get("output", []):
        for comp in entry.get("components", []) or []:
            if comp.get("name") in kernel_packages:
                nvr = f"{comp['name']}-{comp['version']}-{comp['release']}"
                if nvr not in kernel_nvrs:
                    kernel_nvrs.append(nvr)
    return kernel_nvrs


def get_kernel_rpms_from_rhcos(rhcos_nvr: str, kernel_packages: set[str]) -> list[str]:
    url = nvr_to_brewroot_metadata_url(rhcos_nvr)
    LOGGER.debug("Downloading RHCOS metadata from %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return get_kernel_nvrs_from_metadata(resp.json(), kernel_packages)


def check_kernel_tags(koji_api, kernel_nvrs: list[str], stop_ship_tag: str) -> list[KernelBuildInfo]:
    results = []
    if not kernel_nvrs:
        return results
    build_tags = brew.get_builds_tags(kernel_nvrs, koji_api)
    for nvr, tags in zip(kernel_nvrs, build_tags):
        tag_names = {t["name"] for t in tags}
        results.append(KernelBuildInfo(nvr=nvr, has_stop_ship=stop_ship_tag in tag_names))
    return results


async def check_advisory_kernel_tag(
    api: AsyncErrataAPI,
    advisory_id: int,
    impetus: str,
    koji_api,
    kernel_packages: set[str],
    stop_ship_tag: str,
) -> AdvisoryKernelResult:
    result = AdvisoryKernelResult(advisory_id=advisory_id, impetus=impetus)
    try:
        all_builds = await api.get_builds_flattened(advisory_id)
        rhcos_nvrs = find_rhcos_nvrs(all_builds)
        result.rhcos_builds = rhcos_nvrs

        if not rhcos_nvrs:
            LOGGER.info("Advisory %s (%s): no RHCOS builds found, skipping", advisory_id, impetus)
            result.skipped = True
            return result

        def _check():
            seen_nvrs = set()
            all_kernel_nvrs = []
            for rhcos_nvr in rhcos_nvrs:
                LOGGER.debug("Advisory %s (%s): checking RHCOS build %s", advisory_id, impetus, rhcos_nvr)
                kernel_nvrs = get_kernel_rpms_from_rhcos(rhcos_nvr, kernel_packages)
                for nvr in kernel_nvrs:
                    if nvr not in seen_nvrs:
                        seen_nvrs.add(nvr)
                        all_kernel_nvrs.append(nvr)
            return check_kernel_tags(koji_api, all_kernel_nvrs, stop_ship_tag)

        result.kernel_builds = await asyncio.to_thread(_check)

        if not result.kernel_builds:
            result.error = "no kernel RPMs found in RHCOS builds"
            LOGGER.error("Advisory %s (%s): %s", advisory_id, impetus, result.error)
            return result

        for kb in result.kernel_builds:
            if kb.has_stop_ship:
                LOGGER.error(
                    "Advisory %s (%s): kernel %s tagged %s!",
                    advisory_id,
                    impetus,
                    kb.nvr,
                    stop_ship_tag,
                )
            else:
                LOGGER.info("Advisory %s (%s): kernel %s OK", advisory_id, impetus, kb.nvr)

    except Exception as e:
        LOGGER.error("Advisory %s (%s): error checking kernel tag: %s", advisory_id, impetus, e)
        result.error = str(e)

    return result


async def verify_kernel_tag(
    advisories: dict[str, int],
    koji_api,
    kernel_packages: set[str],
    stop_ship_tag: str,
) -> VerifyKernelTagResult:
    result = VerifyKernelTagResult(stop_ship_tag=stop_ship_tag)

    async with AsyncErrataAPI() as api:
        for impetus, advisory_id in advisories.items():
            ar = await check_advisory_kernel_tag(api, advisory_id, impetus, koji_api, kernel_packages, stop_ship_tag)
            result.advisories.append(ar)

    return result


def render_result(result: VerifyKernelTagResult, output: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "passed": result.passed,
                "failed": result.failed,
                "stop_ship_tag": result.stop_ship_tag,
                "advisories": [
                    {
                        "advisory_id": a.advisory_id,
                        "impetus": a.impetus,
                        "rhcos_builds": a.rhcos_builds,
                        "kernel_builds": [{"nvr": k.nvr, "has_stop_ship": k.has_stop_ship} for k in a.kernel_builds],
                        "skipped": a.skipped,
                        "error": a.error,
                    }
                    for a in result.advisories
                ],
            },
            indent=2,
        )

    lines = [f"Kernel stop-ship tag check (tag: {result.stop_ship_tag})", ""]
    for a in result.advisories:
        if a.skipped:
            lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): SKIPPED (no RHCOS/kernel)")
        elif a.failed:
            status = "STOP-SHIP" if any(k.has_stop_ship for k in a.kernel_builds) else "ERROR"
            lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): {status}")
        else:
            lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): OK")

        if a.error:
            lines.append(f"    Error: {a.error}")
        for k in a.kernel_builds:
            tag_status = "STOP-SHIP" if k.has_stop_ship else "ok"
            lines.append(f"    {k.nvr}: {tag_status}")
    lines.append("")

    overall = "PASS" if result.passed else "FAIL"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)


def get_advisory_ids(runtime) -> dict[str, int]:
    releases_config = runtime.get_releases_config()
    group_config = assembly_config_struct(releases_config, runtime.assembly, "group", {})
    advisories = group_config.get("advisories", {})
    result = {}
    for impetus in KERNEL_TAG_ADVISORY_TYPES:
        ad_id = advisories.get(impetus)
        if ad_id:
            result[impetus] = int(ad_id)
    return result


@cli.command("verify-kernel-tag", short_help="Check RHCOS kernel builds for stop-ship tags")
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
async def verify_kernel_tag_cli(runtime, output):
    """Check RHCOS kernel builds for early-kernel-stop-ship tags.

    Inspects image and RHCOS advisories from the assembly config. For each
    advisory, finds RHCOS builds, extracts kernel RPMs, and checks whether
    any kernel build is tagged with the stop-ship tag defined in
    rpm_deliveries group config.

    Exits with code 1 if any kernel build has the stop-ship tag.

    Requires --group and --assembly global options.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.51 verify-kernel-tag
    """
    runtime.initialize()

    rpm_deliveries = get_rpm_deliveries_config(runtime)
    if not rpm_deliveries:
        raise click.UsageError("No rpm_deliveries found in group config.")

    kernel_packages, stop_ship_tag = get_kernel_packages_and_tag(rpm_deliveries)
    if not kernel_packages or not stop_ship_tag:
        raise click.UsageError("No kernel packages or stop_ship_tag found in rpm_deliveries config.")

    advisories = get_advisory_ids(runtime)
    if not advisories:
        raise click.UsageError(f"No advisory IDs found for {KERNEL_TAG_ADVISORY_TYPES} in assembly config.")

    LOGGER.info(
        "Checking kernel stop-ship tag '%s' for packages %s in advisories: %s",
        stop_ship_tag,
        kernel_packages,
        advisories,
    )

    koji_api = koji.ClientSession(BREW_HUB)

    result = await verify_kernel_tag(
        advisories=advisories,
        koji_api=koji_api,
        kernel_packages=kernel_packages,
        stop_ship_tag=stop_ship_tag,
    )
    click.echo(render_result(result, output))
    if not result.passed:
        raise SystemExit(1)
