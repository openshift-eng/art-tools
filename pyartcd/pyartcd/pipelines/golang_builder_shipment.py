"""
Pipeline to create shipment MRs in ocp-shipment-data for golang builder releases.

Invocation::

    artcd golang-builder-shipment \\
        --ocp-version 4.22 \\
        --golang-nvrs golang-1.25.9-1.el9

    # or with explicit Konflux image NVRs:
    artcd golang-builder-shipment \\
        --ocp-version 4.22 \\
        --golang-group rhel-9-golang-1.25 \\
        openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9

This pipeline:
1. Derives golang_group from the NVRs (or accepts it explicitly)
2. Resolves Konflux image NVRs from golang RPM NVRs if needed
3. Reads software_lifecycle.phase from ocp-build-data to pick the correct ReleasePlan
4. Builds a ShipmentConfig YAML with snapshot from the resolved NVRs
5. Creates a draft MR in ocp-shipment-data
"""

import logging
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import click
import gitlab as python_gitlab
from artcommonlib import exectools
from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME, SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.release_util import SoftwareLifecyclePhase, isolate_el_version_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.constants import ART_IMAGES_BASE_APPLICATION
from elliottlib.constants import GOLANG_BUILDER_CVE_COMPONENT
from elliottlib.shipment_model import (
    Data,
    Environments,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    SnapshotSpec,
)

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

_LOGGER = logging.getLogger(__name__)
yaml = new_roundtrip_yaml_handler()

GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP = {
    "prod": "ocp-art-golang-builder-prod-rhel9",
    "ec": "ocp-art-golang-builder-ec-rhel9",
}


def derive_golang_group(nvrs: List[str]) -> str:
    """Derive the ocp-build-data golang group from NVR patterns.

    Supports both golang RPM NVRs (golang-1.25.9-1.el9) and
    Konflux image NVRs (openshift-golang-builder-container-v1.25.9-...).
    """
    for nvr in nvrs:
        # Konflux image NVR: openshift-golang-builder-container-v1.25.9-...el9
        m = re.search(r"v(\d+)\.(\d+)\.\d+.*\.el(\d+)", nvr)
        if m:
            return f"rhel-{m.group(3)}-golang-{m.group(1)}.{m.group(2)}"

        # RPM NVR: golang-1.25.9-1.el9
        parsed = parse_nvr(nvr)
        if parsed["name"] == "golang":
            major_minor = ".".join(parsed["version"].split(".")[:2])
            el_v = isolate_el_version_in_release(parsed["release"])
            if el_v is not None:
                return f"rhel-{el_v}-golang-{major_minor}"

    raise ValueError(f"Cannot derive golang group from NVRs: {nvrs}")


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
    """Determine 'prod' or 'ec' by reading software_lifecycle.phase from ocp-build-data.

    Reads group.yml from the openshift-{ocp_version} branch. If the phase is
    ``pre-release``, returns ``'ec'``; otherwise returns ``'prod'``.
    """
    base_url = (data_path or constants.OCP_BUILD_DATA_URL).rstrip("/")
    branch = f"openshift-{ocp_version}"

    # GitHub raw URL — works for both github.com and GHE
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


class GolangBuilderShipmentPipeline:
    """Creates a shipment MR in ocp-shipment-data for golang builder images."""

    def __init__(
        self,
        runtime: Runtime,
        ocp_version: str,
        nvrs: List[str],
        golang_group: Optional[str] = None,
        art_jira: str = "",
        shipment_data_repo_url: Optional[str] = None,
        data_path: Optional[str] = None,
    ):
        self.runtime = runtime
        self.ocp_version = ocp_version
        self.golang_group = golang_group or derive_golang_group(nvrs)
        self.nvrs = sorted(nvrs)
        self.art_jira = art_jira
        self.dry_run = runtime.dry_run
        self.data_path = data_path

        self.product = "ocp"
        self.gitlab_url = runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.working_dir = runtime.working_dir.absolute()
        self._shipment_data_repo_dir = self.working_dir / "shipment-data-push"

        self.shipment_data_repo_pull_url = (
            shipment_data_repo_url
            or runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        self.shipment_data_repo_push_url = (
            runtime.config.get("shipment_config", {}).get("shipment_data_push_url") or SHIPMENT_DATA_URL_TEMPLATE
        )
        self.shipment_data_repo = GitRepository(self._shipment_data_repo_dir, self.dry_run)

        # Resolved lazily in run() via resolve_lifecycle_env
        self.env: Optional[str] = None
        self.release_plan: Optional[str] = None

    @staticmethod
    def resolve_release_plan(env: str) -> str:
        """Map lifecycle env to the correct ReleasePlan name."""
        plan = GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP.get(env)
        if not plan:
            raise ValueError(
                f"Unknown env '{env}'. Must be one of: {list(GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP.keys())}"
            )
        return plan

    @staticmethod
    def basic_auth_url(url: str, token: str) -> str:
        """Inject token into a GitLab URL for push authentication."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://oauth2:{token}@{parsed.hostname}{parsed.path}"

    async def run(self) -> str:
        """Execute the pipeline end to end.

        Returns:
            The URL of the created shipment MR.
        """
        self.env = await resolve_lifecycle_env(self.ocp_version, self.data_path)
        self.release_plan = self.resolve_release_plan(self.env)

        _LOGGER.info(
            "Starting golang-builder-shipment pipeline: ocp_version=%s golang_group=%s env=%s release_plan=%s nvrs=%s",
            self.ocp_version,
            self.golang_group,
            self.env,
            self.release_plan,
            self.nvrs,
        )

        self.setup_working_dir()
        await self.setup_repos()

        shipment_config = await self.build_shipment_config()
        mr_url = await self.create_shipment_mr(shipment_config)

        _LOGGER.info("Shipment MR created: %s", mr_url)
        return mr_url

    def setup_working_dir(self) -> None:
        self.working_dir.mkdir(parents=True, exist_ok=True)
        if self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)

    async def setup_repos(self):
        self._gitlab_token = os.getenv("GITLAB_TOKEN")
        if not self._gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required")

        await self.shipment_data_repo.setup(
            remote_url=self.basic_auth_url(self.shipment_data_repo_push_url, self._gitlab_token),
            upstream_remote_url=self.shipment_data_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

    async def build_shipment_config(self) -> ShipmentConfig:
        """Construct a ShipmentConfig from NVRs using elliott snapshot new."""

        snapshot = await self._create_snapshot()

        # Override application to match RP/RPA configuration. elliott infers the
        # Konflux application from the build pipeline URL (e.g. "rhel-9-golang-1-25"),
        # but our ReleasePlan/RPA pair is registered under "art-images-base".
        snapshot.spec.application = ART_IMAGES_BASE_APPLICATION

        metadata = Metadata(
            product=self.product,
            application=snapshot.spec.application,
            group=self.golang_group,
            assembly="stream",
        )

        # Stage and prod use the same ReleasePlan for golang builders — the
        # RPA is per-lifecycle (ec vs prod), not per-environment.
        environments = Environments(
            stage=ShipmentEnv(releasePlan=self.release_plan),
            prod=ShipmentEnv(releasePlan=self.release_plan),
        )

        release_notes = ReleaseNotes(
            type="RHBA",
            synopsis=f"Golang builder image update for OpenShift {self.ocp_version}",
            topic=(
                f"An update for the golang builder images is now available for "
                f"Red Hat OpenShift Container Platform {self.ocp_version}."
            ),
            description=(
                f"This update provides rebuilt golang builder images for "
                f"Red Hat OpenShift Container Platform {self.ocp_version}.\n\n"
                f"Golang group: {self.golang_group}"
            ),
            solution="The golang builder images are available from registry.redhat.io/openshift/golang-builder.",
        )
        if self.art_jira:
            release_notes.references = [f"https://redhat.atlassian.net/browse/{self.art_jira}"]

        shipment = Shipment(
            metadata=metadata,
            environments=environments,
            snapshot=snapshot,
            data=Data(releaseNotes=release_notes),
        )

        config = ShipmentConfig(shipment=shipment)
        _LOGGER.info("Built ShipmentConfig with %d NVRs", len(self.nvrs))
        return config

    async def _create_snapshot(self) -> Snapshot:
        """Create a Snapshot from NVRs using ``elliott snapshot new --builds-file``."""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            for nvr in self.nvrs:
                f.write(nvr + "\n")
            builds_file = f.name

        try:
            cmd = [
                "elliott",
                "--group",
                self.golang_group,
                "--assembly",
                "stream",
                "snapshot",
                "new",
                f"--builds-file={builds_file}",
            ]
            quay_auth_file = os.getenv("QUAY_AUTH_FILE")
            if quay_auth_file:
                cmd.append(f"--pull-secret={quay_auth_file}")

            rc, stdout, stderr = await exectools.cmd_gather_async(cmd, stderr=None, check=False)
            if rc != 0:
                raise RuntimeError(f"elliott snapshot new failed (rc={rc}): {stderr or stdout}")
            if stdout:
                _LOGGER.info("elliott snapshot new output:\n%s", stdout)
        finally:
            os.unlink(builds_file)

        snapshot_obj = yaml.load(stdout)
        if not snapshot_obj or not isinstance(snapshot_obj, dict):
            raise ValueError(f"elliott snapshot new returned invalid output: {stdout!r}")

        spec = snapshot_obj.get("spec")
        if not spec:
            raise ValueError(f"elliott snapshot new output missing 'spec': {snapshot_obj}")

        return Snapshot(
            spec=SnapshotSpec(**spec),
            nvrs=self.nvrs,
        )

    async def create_shipment_mr(self, shipment_config: ShipmentConfig) -> str:
        """Write the shipment YAML and open a draft MR in ocp-shipment-data."""

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        source_branch = f"golang-builder-shipment-{self.golang_group}-{timestamp}"

        await self.shipment_data_repo.create_branch(source_branch)

        # Persist the YAML
        application = shipment_config.shipment.metadata.application
        relative_target_dir = Path("shipment") / self.product / self.golang_group / application / self.env
        target_dir = self.shipment_data_repo._directory / relative_target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"stream.image.{timestamp}.yaml"
        filepath = relative_target_dir / filename
        shipment_dump = shipment_config.model_dump(exclude_unset=True, exclude_none=True)
        out = StringIO()
        yaml.dump(shipment_dump, out)
        await self.shipment_data_repo.write_file(filepath, out.getvalue())
        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()

        commit_message = f"Add golang builder shipment for {self.golang_group}"
        if self.art_jira:
            commit_message += f"\n\nRef: {self.art_jira}"
        job_url = os.getenv("BUILD_URL", "")
        if job_url:
            commit_message += f"\n{job_url}"

        pushed = await self.shipment_data_repo.commit_push(commit_message, safe=True)
        if not pushed:
            raise RuntimeError("Failed to push shipment data to remote")

        mr_title = f"Draft: Golang builder shipment for {self.golang_group}"
        mr_description = f"Golang builder shipment for OCP {self.ocp_version}\n\n"
        mr_description += f"Group: {self.golang_group}\n"
        mr_description += f"Environment: {self.env}\n"
        mr_description += f"ReleasePlan: {self.release_plan}\n"
        mr_description += f"NVRs: {len(self.nvrs)}\n"
        if self.art_jira:
            mr_description += f"\nRef: https://redhat.atlassian.net/browse/{self.art_jira}"
        if job_url:
            mr_description += f"\nCreated by: {job_url}"

        if self.dry_run:
            _LOGGER.info("[DRY-RUN] Would create MR: %s", mr_title)
            return f"{self.gitlab_url}/placeholder/-/merge_requests/placeholder"

        gl = python_gitlab.Gitlab(self.gitlab_url, private_token=self._gitlab_token)

        def _get_project(url):
            parsed = urlparse(url)
            project_path = parsed.path.strip("/").removesuffix(".git")
            return gl.projects.get(project_path)

        source_project = _get_project(self.shipment_data_repo_push_url)
        target_project = _get_project(self.shipment_data_repo_pull_url)

        mr = source_project.mergerequests.create(
            {
                "source_branch": source_branch,
                "target_project_id": target_project.id,
                "target_branch": "main",
                "title": mr_title,
                "description": mr_description,
                "remove_source_branch": True,
            }
        )
        _LOGGER.info("Created Draft MR: %s", mr.web_url)
        return mr.web_url


@cli.command("golang-builder-shipment")
@click.option("--ocp-version", required=True, help="OCP version (e.g. 4.22)")
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
    ocp_version: str,
    golang_group: Optional[str],
    golang_nvrs: Optional[str],
    art_jira: str,
    shipment_data_repo_url: Optional[str],
    data_path: str,
    nvrs: Tuple[str, ...],
):
    """Create a shipment MR in ocp-shipment-data for golang builder images.

    Builds a ShipmentConfig YAML from the provided NVRs, auto-detects whether
    to use the prod or ec ReleasePlan from ocp-build-data software_lifecycle.phase,
    and opens a draft MR in ocp-shipment-data for ERT approval.

    Accepts either explicit Konflux image NVRs as positional arguments, or
    --golang-nvrs with golang RPM NVRs (auto-resolved to Konflux image NVRs).
    The --golang-group is derived from the NVRs when not specified.
    """
    resolved_nvrs: List[str] = list(nvrs)

    if not resolved_nvrs and golang_nvrs:
        rpm_nvrs = [n.strip() for n in golang_nvrs.replace(",", " ").split() if n.strip()]
        _LOGGER.info("Resolving golang RPM NVRs to Konflux image NVRs: %s", rpm_nvrs)
        resolved_nvrs = await resolve_konflux_image_nvrs(rpm_nvrs)
        if not golang_group:
            golang_group = derive_golang_group(rpm_nvrs)

    if not resolved_nvrs:
        raise click.UsageError("Provide Konflux image NVRs as arguments or --golang-nvrs with golang RPM NVRs")

    pipeline = GolangBuilderShipmentPipeline(
        runtime=runtime,
        ocp_version=ocp_version,
        nvrs=resolved_nvrs,
        golang_group=golang_group,
        art_jira=art_jira,
        shipment_data_repo_url=shipment_data_repo_url,
        data_path=data_path,
    )
    mr_url = await pipeline.run()
    click.echo(f"Shipment MR: {mr_url}")
