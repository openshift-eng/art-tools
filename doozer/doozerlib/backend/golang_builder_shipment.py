"""
Create shipment MRs in ocp-shipment-data for golang builder releases.

:class:`GolangBuilderShipmentHandler` builds ShipmentConfig YAML and opens draft MRs in
ocp-shipment-data. Inline builds use a single-component Snapshot; the CLI path may use
``elliott snapshot new`` for multi-NVR snapshots.
"""

import logging
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

import gitlab as python_gitlab
from artcommonlib import exectools
from artcommonlib.constants import REDHAT_GITLAB_URL, SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.constants import ART_IMAGES_BASE_APPLICATION
from doozerlib.util import konflux_golang_builder_component_name
from elliottlib.shipment_model import (
    ComponentSource,
    Data,
    Environments,
    GitSource,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    SnapshotComponent,
    SnapshotSpec,
)
from pyartcd.git import GitRepository

yaml = new_roundtrip_yaml_handler()

_PRODUCT = "ocp"
_ELLIOTT_GOLANG_GROUP = "golang"
_DEFAULT_ENV = "prod"


def format_shipment_mr_title(golang_group: str) -> str:
    """Match ocp-shipment-data convention: ``Shipment for 4.21.28 (ship date: …)``."""
    return f"Shipment for {golang_group}"


GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP = {
    "stage": "ocp-art-golang-builder-ec-rhel9",
    "prod": "ocp-art-golang-builder-prod-rhel9",
}


def derive_golang_group(nvrs: List[str]) -> str:
    """Derive the ocp-build-data golang group from NVR patterns."""
    for nvr in nvrs:
        m = re.search(r"v(\d+)\.(\d+)\.\d+.*\.el(\d+)", nvr)
        if m:
            return f"rhel-{m.group(3)}-golang-{m.group(1)}.{m.group(2)}"

        parsed = parse_nvr(nvr)
        if parsed["name"] == "golang":
            major_minor = ".".join(parsed["version"].split(".")[:2])
            el_v = isolate_el_version_in_release(parsed["release"])
            if el_v is not None:
                return f"rhel-{el_v}-golang-{major_minor}"

    raise ValueError(f"Cannot derive golang group from NVRs: {nvrs}")


def basic_auth_url(url: str, token: str) -> str:
    """Inject token into a GitLab URL for push authentication."""
    parsed = urlparse(url)
    netloc = f"oauth2:{token}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


class GolangBuilderShipmentHandler:
    """Creates a shipment MR in ocp-shipment-data for golang builder images."""

    def __init__(
        self,
        runtime,
        gitlab_url: str = REDHAT_GITLAB_URL,
        shipment_data_repo_pull_url: Optional[str] = None,
        shipment_data_repo_push_url: Optional[str] = None,
        art_jira: str = "",
    ):
        self.runtime = runtime
        self.art_jira = art_jira
        self.logger = getattr(runtime, "logger", None) or logging.getLogger(__name__)
        self.gitlab_url = gitlab_url
        # Follow the established pyartcd pattern: use a known subdir of the runtime
        # working directory. The runtime manages that dir's lifecycle (atexit cleanup
        # when auto-created, or preserved when user-specified for debugging).
        self._shipment_data_repo_dir = Path(runtime.working_dir) / "golang-shipment-data"

        self.shipment_data_repo_pull_url = shipment_data_repo_pull_url or SHIPMENT_DATA_URL_TEMPLATE
        self.shipment_data_repo_push_url = shipment_data_repo_push_url or SHIPMENT_DATA_URL_TEMPLATE
        # pyartcd.git is co-installed with doozer in rh-art-tools
        self.shipment_data_repo = GitRepository(self._shipment_data_repo_dir, False)
        self._gitlab_token: Optional[str] = None

    async def create_shipment(
        self,
        nvr: str,
        container_image: str,
        rebase_repo_url: str,
        rebase_commitish: str,
        env: Optional[str] = None,
    ) -> Optional[str]:
        """Create shipment MR for one golang builder build (non-fatal inline path)."""
        try:
            golang_group = derive_golang_group([nvr])
            env = env or _DEFAULT_ENV
            release_plan = GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP[env]

            self.logger.info(
                "Starting golang builder shipment: nvr=%s golang_group=%s env=%s release_plan=%s",
                nvr,
                golang_group,
                env,
                release_plan,
            )

            await self._setup_repos()

            snapshot = self._build_inline_snapshot(
                nvr=nvr,
                container_image=container_image,
                rebase_repo_url=rebase_repo_url,
                rebase_commitish=rebase_commitish,
            )
            shipment_config = self._build_shipment_config(
                snapshot=snapshot,
                nvrs=[nvr],
                golang_group=golang_group,
            )
            mr_url = await self._create_shipment_mr(
                shipment_config,
                golang_group=golang_group,
                env=env,
                release_plan=release_plan,
                nvrs=[nvr],
            )
            self.logger.info("Golang builder shipment MR created: %s", mr_url)
            return mr_url
        except Exception:
            self.logger.exception("Golang builder shipment failed for %s", nvr)
            return None

    async def create_shipment_from_nvrs(
        self,
        nvrs: List[str],
        env: Optional[str] = None,
    ) -> Optional[str]:
        """Create shipment MR from NVR list (CLI path; may use elliott for snapshot)."""
        nvrs = sorted(nvrs)
        golang_group = derive_golang_group(nvrs)
        env = env or _DEFAULT_ENV
        release_plan = GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP[env]

        self.logger.info(
            "Starting golang-builder-shipment: golang_group=%s env=%s release_plan=%s nvrs=%s",
            golang_group,
            env,
            release_plan,
            nvrs,
        )

        await self._setup_repos()

        snapshot = await self._create_snapshot_via_elliott(nvrs)
        shipment_config = self._build_shipment_config(
            snapshot=snapshot,
            nvrs=nvrs,
            golang_group=golang_group,
        )
        mr_url = await self._create_shipment_mr(
            shipment_config,
            golang_group=golang_group,
            env=env,
            release_plan=release_plan,
            nvrs=nvrs,
        )
        self.logger.info("Shipment MR created: %s", mr_url)
        return mr_url

    async def _setup_repos(self) -> None:
        self._gitlab_token = os.getenv("GITLAB_TOKEN")
        if not self._gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required")

        if self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)

        await self.shipment_data_repo.setup(
            remote_url=basic_auth_url(self.shipment_data_repo_push_url, self._gitlab_token),
            upstream_remote_url=self.shipment_data_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

    @staticmethod
    def _apply_rpa_component_names(snapshot: Snapshot, nvrs: List[str]) -> None:
        """Rewrite snapshot component names to RPA mapping names (same as BaseImageHandler)."""
        components = snapshot.spec.components
        if len(components) != len(nvrs):
            raise ValueError(f"Cannot map {len(nvrs)} NVR(s) to {len(components)} snapshot component(s)")
        if len(nvrs) == 1:
            components[0].name = konflux_golang_builder_component_name(nvrs[0])
            return
        sorted_nvrs = sorted(nvrs, key=konflux_golang_builder_component_name)
        sorted_components = sorted(components, key=lambda c: c.name)
        for component, nvr in zip(sorted_components, sorted_nvrs):
            component.name = konflux_golang_builder_component_name(nvr)

    @staticmethod
    def _build_inline_snapshot(
        nvr: str,
        container_image: str,
        rebase_repo_url: str,
        rebase_commitish: str,
    ) -> Snapshot:
        component = SnapshotComponent(
            name=konflux_golang_builder_component_name(nvr),
            containerImage=container_image,
            source=ComponentSource(
                git=GitSource(url=rebase_repo_url, revision=rebase_commitish),
            ),
        )
        return Snapshot(
            spec=SnapshotSpec(application=ART_IMAGES_BASE_APPLICATION, components=[component]),
            nvrs=[nvr],
        )

    def _build_shipment_config(
        self,
        snapshot: Snapshot,
        nvrs: List[str],
        golang_group: str,
    ) -> ShipmentConfig:
        # group must match the ocp-build-data branch name that elliott resolves.
        # For golang builders this is "golang", not the version-specific group
        # (e.g. "rhel-9-golang-1.25"). Version specificity lives in the snapshot
        # component names (e.g. "golang-builder-v1.25-rhel9") must match RPA mapping.
        metadata = Metadata(
            product=_PRODUCT,
            application=snapshot.spec.application,
            group="golang",
            assembly="stream",
            advisory_required=False,
        )

        environments = Environments(
            stage=ShipmentEnv(releasePlan=GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP["stage"]),
            prod=ShipmentEnv(releasePlan=GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP["prod"]),
        )

        release_notes = ReleaseNotes(
            type="RHBA",
            synopsis=f"Golang builder image update ({golang_group})",
            topic=(
                f"An update for the golang builder images ({golang_group}) is now available for "
                f"Red Hat OpenShift Container Platform."
            ),
            description=(f"This update provides rebuilt golang builder images for {golang_group}."),
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
        self.logger.info("Built ShipmentConfig with %d NVRs", len(nvrs))
        return config

    async def _create_snapshot_via_elliott(self, nvrs: List[str]) -> Snapshot:
        """Create a Snapshot from NVRs using ``elliott snapshot new --builds-file``."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            for nvr in nvrs:
                f.write(nvr + "\n")
            builds_file = f.name

        try:
            cmd = [
                "elliott",
                "--group",
                _ELLIOTT_GOLANG_GROUP,
                "--assembly",
                "stream",
                "snapshot",
                "new",
                f"--builds-file={builds_file}",
            ]
            quay_auth_file = os.getenv("QUAY_AUTH_FILE")
            if quay_auth_file:
                cmd.append(f"--pull-secret={quay_auth_file}")

            rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)
            if rc != 0:
                raise RuntimeError(f"elliott snapshot new failed (rc={rc}): {stderr or stdout}")
            if stdout:
                self.logger.info("elliott snapshot new output:\n%s", stdout)
        finally:
            os.unlink(builds_file)

        snapshot_obj = yaml.load(stdout)
        if not snapshot_obj or not isinstance(snapshot_obj, dict):
            raise ValueError(f"elliott snapshot new returned invalid output: {stdout!r}")

        spec = snapshot_obj.get("spec")
        if not spec:
            raise ValueError(f"elliott snapshot new output missing 'spec': {snapshot_obj}")

        snapshot = Snapshot(
            spec=SnapshotSpec(**spec),
            nvrs=nvrs,
        )
        snapshot.spec.application = ART_IMAGES_BASE_APPLICATION
        self._apply_rpa_component_names(snapshot, nvrs)
        return snapshot

    async def _create_shipment_mr(
        self,
        shipment_config: ShipmentConfig,
        golang_group: str,
        env: str,
        release_plan: str,
        nvrs: List[str],
    ) -> str:
        """Write the shipment YAML and open a draft MR in ocp-shipment-data."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        source_branch = f"golang-builder-shipment-{golang_group}-{timestamp}"

        await self.shipment_data_repo.create_branch(source_branch)

        application = shipment_config.shipment.metadata.application
        relative_target_dir = Path("shipment") / _PRODUCT / "golang" / application / env
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

        commit_message = f"Add golang builder shipment for {golang_group}"
        if self.art_jira:
            commit_message += f"\n\nRef: {self.art_jira}"
        job_url = os.getenv("BUILD_URL", "")
        if job_url:
            commit_message += f"\n{job_url}"

        pushed = await self.shipment_data_repo.commit_push(commit_message, safe=True)
        if not pushed:
            raise RuntimeError("Failed to push shipment data to remote")

        mr_title = format_shipment_mr_title(golang_group)
        mr_description = f"Golang builder shipment for {golang_group}\n\n"
        mr_description += f"Environment: {env}\n"
        mr_description += f"ReleasePlan: {release_plan}\n"
        mr_description += f"NVRs: {len(nvrs)}\n"
        if self.art_jira:
            mr_description += f"\nRef: https://redhat.atlassian.net/browse/{self.art_jira}"
        if job_url:
            mr_description += f"\nCreated by: {job_url}"

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
        self.logger.info("Created Draft MR: %s", mr.web_url)

        # The project has default approval rules (ART + ERT + Docs).
        # Golang builder shipments only require ART sign-off — strip the rest.
        try:
            target_mr = target_project.mergerequests.get(mr.iid)
            for rule in target_mr.approval_rules.list():
                if rule.name != "ART":
                    self.logger.info("Removing approval rule '%s' (not needed for golang builder shipment)", rule.name)
                    rule.delete()
        except Exception as e:
            self.logger.warning("Could not clean up approval rules: %s", e)

        return mr.web_url
