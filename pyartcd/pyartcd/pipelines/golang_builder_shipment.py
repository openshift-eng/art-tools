"""
CLI wrapper for golang builder shipment MRs in ocp-shipment-data.

Invocation::

    artcd golang-builder-shipment \\
        --ocp-version 4.22 \\
        --golang-nvrs golang-1.25.9-1.el9

    # or with explicit Konflux image NVRs:
    artcd golang-builder-shipment \\
        --ocp-version 4.22 \\
        --golang-group rhel-9-golang-1.25 \\
        openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9
"""

import logging
from typing import List, Optional, Tuple

import aiohttp
import click
from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.release_util import SoftwareLifecyclePhase, isolate_el_version_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.backend.golang_builder_shipment import (
    GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP,
    GolangBuilderShipmentHandler,
    derive_golang_group,
)
from elliottlib.constants import GOLANG_BUILDER_CVE_COMPONENT

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

_LOGGER = logging.getLogger(__name__)
yaml = new_roundtrip_yaml_handler()


async def resolve_konflux_image_nvrs(golang_nvrs: List[str]) -> List[str]:
    """Resolve golang RPM NVRs to Konflux golang-builder image NVRs via Konflux DB."""
    image_nvrs = []
    db = KonfluxDb()

    for nvr in golang_nvrs:
        parsed = parse_nvr(nvr)
        go_version = parsed["version"]
        el_v = isolate_el_version_in_release(parsed["release"])
        if el_v is None:
            raise ValueError(f"Cannot detect RHEL version from NVR: {nvr}")

        extra_patterns = {"nvr": f"{GOLANG_BUILDER_CVE_COMPONENT}-v{go_version}"}
        record = await anext(
            db.search_builds_by_fields(
                where={
                    "name": GOLANG_BUILDER_IMAGE_NAME,
                    "el_target": f"el{el_v}",
                    "artifact_type": str(ArtifactType.IMAGE),
                    "outcome": str(KonfluxBuildOutcome.SUCCESS),
                    "engine": str(Engine.KONFLUX),
                },
                extra_patterns=extra_patterns,
                limit=1,
            ),
            None,
        )
        if not record:
            raise RuntimeError(
                f"No Konflux golang-builder image found for go {go_version} el{el_v}. "
                f"Has update-golang built one for {nvr}?"
            )
        _LOGGER.info("Resolved %s → %s", nvr, record.nvr)
        image_nvrs.append(record.nvr)

    return image_nvrs


async def resolve_lifecycle_env(ocp_version: str, data_path: Optional[str] = None) -> str:
    """Determine 'prod' or 'ec' by reading software_lifecycle.phase from ocp-build-data."""
    base_url = (data_path or constants.OCP_BUILD_DATA_URL).rstrip("/")
    branch = f"openshift-{ocp_version}"

    if "github.com" in base_url:
        raw_url = base_url.replace("github.com", "raw.githubusercontent.com") + f"/{branch}/group.yml"
    else:
        raw_url = f"{base_url}/raw/{branch}/group.yml"

    _LOGGER.info("Fetching lifecycle phase from %s", raw_url)
    async with aiohttp.ClientSession() as session:
        async with session.get(raw_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    f"Failed to fetch group.yml from {raw_url} (HTTP {resp.status}). "
                    f"Does the branch '{branch}' exist in ocp-build-data?"
                )
            content = await resp.text()

    group_config = yaml.load(content)
    phase_str = None
    if group_config and isinstance(group_config, dict):
        lifecycle = group_config.get("software_lifecycle")
        if lifecycle and isinstance(lifecycle, dict):
            phase_str = lifecycle.get("phase")

    if not phase_str:
        _LOGGER.warning("No software_lifecycle.phase in group.yml for %s; defaulting to prod", branch)
        return "prod"

    try:
        phase = SoftwareLifecyclePhase.from_name(phase_str)
    except ValueError:
        _LOGGER.warning("Unknown lifecycle phase '%s' for %s; defaulting to prod", phase_str, branch)
        return "prod"

    if phase == SoftwareLifecyclePhase.PRE_RELEASE:
        _LOGGER.info("OCP %s is pre-release → using ec ReleasePlan", ocp_version)
        return "ec"

    _LOGGER.info("OCP %s is %s → using prod ReleasePlan", ocp_version, phase_str)
    return "prod"


class _CliRuntimeAdapter:
    """Minimal runtime shim so pyartcd CLI can use GolangBuilderShipmentHandler."""

    def __init__(self, runtime: Runtime, golang_group: str):
        self.logger = runtime.logger
        self.group = golang_group
        self.group_config = getattr(runtime, "group_config", None)


@cli.command("golang-builder-shipment")
@click.option("--ocp-version", required=False, default=None, help="OCP version (e.g. 4.22)")
@click.option(
    "--golang-group",
    required=False,
    default=None,
    help="Golang builder group (e.g. rhel-9-golang-1.25). Derived from NVRs if omitted.",
)
@click.option(
    "--golang-nvrs",
    required=False,
    default=None,
    help="Golang RPM NVRs (comma-separated). Resolves to Konflux image NVRs automatically.",
)
@click.option("--art-jira", default="", help="Related ART Jira ticket (e.g. ART-20930)")
@click.option("--shipment-data-repo-url", default=None, help="Override ocp-shipment-data repo URL")
@click.option(
    "--data-path",
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help="ocp-build-data URL (used to read software_lifecycle.phase)",
)
@click.argument("nvrs", nargs=-1, required=False)
@pass_runtime
@click_coroutine
async def golang_builder_shipment(
    runtime: Runtime,
    ocp_version: Optional[str],
    golang_group: Optional[str],
    golang_nvrs: Optional[str],
    art_jira: str,
    shipment_data_repo_url: Optional[str],
    data_path: str,
    nvrs: Tuple[str, ...],
):
    """Create a shipment MR in ocp-shipment-data for golang builder images."""
    resolved_nvrs: List[str] = list(nvrs)

    if not resolved_nvrs and golang_nvrs:
        rpm_nvrs = [n.strip() for n in golang_nvrs.replace(",", " ").split() if n.strip()]
        _LOGGER.info("Resolving golang RPM NVRs to Konflux image NVRs: %s", rpm_nvrs)
        resolved_nvrs = await resolve_konflux_image_nvrs(rpm_nvrs)
        if not golang_group:
            golang_group = derive_golang_group(rpm_nvrs)

    if not resolved_nvrs:
        raise click.UsageError("Provide Konflux image NVRs as arguments or --golang-nvrs with golang RPM NVRs")

    if not golang_group:
        golang_group = derive_golang_group(resolved_nvrs)

    if not ocp_version:
        if not golang_group:
            raise click.UsageError("--ocp-version is required when --golang-group cannot be derived")
        _LOGGER.warning(
            "No --ocp-version provided; defaulting lifecycle env to prod. "
            "Pass --ocp-version to resolve prod vs ec from ocp-build-data."
        )
        env = "prod"
    else:
        env = await resolve_lifecycle_env(ocp_version, data_path)

    adapter = _CliRuntimeAdapter(runtime, golang_group)
    handler = GolangBuilderShipmentHandler(
        runtime=adapter,
        dry_run=runtime.dry_run,
        art_jira=art_jira,
        ocp_version=ocp_version,
        shipment_data_repo_pull_url=shipment_data_repo_url,
    )
    mr_url = await handler.create_shipment_from_nvrs(
        resolved_nvrs,
        golang_group=golang_group,
        env=env,
    )
    click.echo(f"Shipment MR: {mr_url}")
