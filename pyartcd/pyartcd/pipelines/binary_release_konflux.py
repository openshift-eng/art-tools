import asyncio
import logging
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import click
import yaml as stdlib_yaml
from artcommonlib import exectools
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.github_auth import build_git_auth_env
from artcommonlib.gitlab import GitLabClient
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import (
    new_roundtrip_yaml_handler,
    oc_image_info_for_arch_async,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from elliottlib.shipment_model import (
    Environments,
    Metadata,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    SnapshotSpec,
)
from elliottlib.shipment_utils import (
    SKIPPED_RELEASE_PLAN,
    assert_product_may_skip_stage,
    is_release_plan_skipped,
)

from pyartcd import constants, util
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.click_validators import validate_release_date
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

yaml = new_roundtrip_yaml_handler()


BINARIES_SHIPMENT_KINDS = ("binaries", "binaries-dev-preview")
SUPPORTED_SHIPMENT_KINDS = ("image",) + BINARIES_SHIPMENT_KINDS


@dataclass(frozen=True)
class AggregationProfile:
    member: str
    version: str
    arches: tuple[str, ...]
    application: str
    source_component: str
    aggregate_component: str
    source_package: str
    aggregate_package: str


AGGREGATION_PROFILES = {
    "oc-mirror-2.0": AggregationProfile(
        member="oc-mirror-aggregate",
        version="v2.0.0",
        arches=("amd64", "arm64", "ppc64le", "s390x"),
        application="oc-mirror-2-0",
        source_component="oc-mirror-2-0-oc-mirror",
        aggregate_component="oc-mirror-2-0-oc-mirror-aggregate-binaries",
        source_package="oc-mirror-container",
        aggregate_package="oc-mirror-aggregate-binaries-container",
    ),
}


class BinaryReleaseKonfluxPipeline:
    """
    Prepare Konflux shipments for standalone binaries and oc-mirror container images.

    Standalone binaries use push-artifacts-to-cdn for Customer Portal / Content Gateway
    delivery. Build NVRs resolve to snapshots and shipment files referencing stage/prod
    ReleasePlans defined in konflux-release-data and ocp-shipment-data's config.yaml.

    For oc-mirror, the container goes through rh-advisories to registry.redhat.io and the
    aggregate binaries go through push-artifacts-to-cdn. Separate shipment files share one
    MR, and both ReleasePlanAdmissions carry their release notes. Aggregation can build a
    new transport image or verify an explicitly supplied existing aggregate before release.
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        nvrs: List[str],
        create_mr: bool = False,
        shipment_data_repo_url: Optional[str] = None,
        target_release_date: Optional[str] = None,
        release_plan_key: Optional[str] = None,
        shipment_kind: Optional[str] = None,
        aggregate: bool = False,
        build_only: bool = False,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.group = group
        self.aggregation_profile = AGGREGATION_PROFILES.get(group)
        self.assembly = assembly
        self.nvrs = nvrs
        self.create_mr = create_mr
        self.dry_run = self.runtime.dry_run
        self.target_release_date = target_release_date
        self.release_plan_key = release_plan_key
        self.shipment_kind = shipment_kind
        self.aggregate = aggregate
        self.build_only = build_only
        if build_only and not aggregate:
            raise ValueError("--build-only requires --aggregate")
        if aggregate and (not self.aggregation_profile or len(nvrs) != 1):
            raise ValueError(f"--aggregate requires group {', '.join(AGGREGATION_PROFILES)} and exactly one source NVR")
        if aggregate and not build_only and (shipment_kind is not None or release_plan_key is not None):
            raise ValueError("--aggregate cannot use --shipment-kind or --release-plan-key without --build-only")
        if shipment_kind is not None and shipment_kind not in SUPPORTED_SHIPMENT_KINDS:
            raise ValueError(
                f"Unsupported --shipment-kind {shipment_kind!r}; supported: {', '.join(SUPPORTED_SHIPMENT_KINDS)}"
            )
        if group.startswith("openshift-"):
            if shipment_kind not in BINARIES_SHIPMENT_KINDS:
                raise ValueError(
                    f"Group {group} is an OCP payload group: --shipment-kind must be one of "
                    f"{', '.join(BINARIES_SHIPMENT_KINDS)} so the shipment does not use the 'image' kind."
                )
            expected_key = f"{group.replace('.', '-')}-{shipment_kind}"
            if release_plan_key != expected_key:
                raise ValueError(
                    f"Group {group} with --shipment-kind {shipment_kind} needs --release-plan-key {expected_key}, "
                    f"not {release_plan_key!r}, so the shipment does not use the OCP advisory ReleasePlans."
                )

        # Setup working directories
        self.working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self.doozer_working_dir = self.working_dir / "doozer-working"
        self._shipment_data_repo_dir = self.working_dir / "shipment-data-push"

        # Shipment repository configuration
        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.gitlab_token = None
        self.shipment_mr_url = None
        self.job_url = None
        # Set when config.yaml stage releasePlan is the skip-stage sentinel (extraordinary).
        self.stage_release_skipped = False

        # Product configuration - initialized to None, will be loaded from group config in run()
        self.product = None

        shipment_data_repo_pull_url = shipment_data_repo_url or SHIPMENT_DATA_URL_TEMPLATE
        shipment_data_repo_push_url = shipment_data_repo_url or SHIPMENT_DATA_URL_TEMPLATE
        self.shipment_data_repo_pull_url = shipment_data_repo_pull_url
        self.shipment_data_repo_push_url = shipment_data_repo_push_url
        # GitRepository expects a local filesystem path, not a URL
        self.shipment_data_repo = GitRepository(self._shipment_data_repo_dir, self.dry_run)

        # Base elliott command template
        self._elliott_base_command = [
            'elliott',
            f'--group={group}',
            f'--assembly={assembly}',
            '--build-system=konflux',
            f'--working-dir={self.elliott_working_dir}',
        ]

    @cached_property
    def _gitlab(self) -> GitLabClient:
        """
        Get authenticated GitLab instance.
        """
        return GitLabClient(self.gitlab_url, self.gitlab_token, self.dry_run)

    def _get_gitlab_project(self, url: str):
        """
        Get GitLab project from URL.
        """
        parsed_url = urlparse(url)
        project_path = parsed_url.path.strip('/').removesuffix('.git')
        return self._gitlab.get_project(project_path)

    def check_env_vars(self):
        """
        Check required environment variables for MR creation.
        """
        if not self.create_mr or self.build_only or (self.dry_run and self.aggregate):
            return

        gitlab_token = os.getenv("GITLAB_TOKEN")
        if not gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")
        self.gitlab_token = gitlab_token

        self.job_url = os.getenv('BUILD_URL')

    def setup_working_dir(self):
        """
        Setup working directories, cleaning up any existing ones.
        """
        self.working_dir.mkdir(parents=True, exist_ok=True)
        if self.elliott_working_dir.exists():
            shutil.rmtree(self.elliott_working_dir, ignore_errors=True)
        if self.doozer_working_dir.exists():
            shutil.rmtree(self.doozer_working_dir, ignore_errors=True)
        if self.create_mr and not self.build_only and self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)

    async def setup_shipment_repo(self):
        """
        Setup shipment data repository for MR creation.
        """
        if not self.create_mr:
            return

        await self.shipment_data_repo.setup(
            remote_url=self.shipment_data_repo_push_url,
            upstream_remote_url=self.shipment_data_repo_pull_url,
            remote_auth_envs={"origin": build_git_auth_env(self.gitlab_token, username="oauth2")},
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

    async def _load_product_from_group_config(self) -> str:
        """
        Load the product field from group configuration using doozer command.
        Falls back to extracting from group name if not found.
        """
        try:
            doozer_cmd = ['doozer', f'--group={self.group}', 'config:read-group', 'product']

            _, product_output, _ = await exectools.cmd_gather_async(doozer_cmd)
            product = product_output.strip()

            if product and product != 'None' and product != 'null':
                self.logger.info(f"Loaded product from group config: {product}")
                return product
            else:
                self.logger.debug("No product field found in group config, falling back to group name extraction")

        except Exception as e:
            self.logger.warning(f"Failed to load product from group config: {e}")

        # Fallback: extract product from group name (e.g., "oc-mirror-2.0" -> "oc-mirror")
        product = self.group.rsplit('-', 1)[0]
        if product == 'openshift':
            product = 'ocp'
        self.logger.info(f"Using product extracted from group name: {product}")
        return product

    async def _load_mr_approvers_from_group_config(self) -> dict[str, list[str]]:
        """
        Load the mr_approvers field from group configuration using doozer command.
        Returns a dict mapping approval group names to lists of GitLab usernames.
        Returns empty dict if not configured.
        """
        try:
            cmd = [
                'doozer',
                f'--group={self.group}',
                f'--working-dir={self.doozer_working_dir}',
                'config:read-group',
                'mr_approvers',
            ]
            _, output, _ = await exectools.cmd_gather_async(cmd)
            output = output.strip()
            if output and output not in ('None', 'null'):
                parsed = stdlib_yaml.safe_load(output)
                if not isinstance(parsed, dict):
                    self.logger.warning("mr_approvers is not a dict (got %s), ignoring", type(parsed).__name__)
                    return {}
                return parsed
        except Exception as e:
            self.logger.warning(f"Failed to load mr_approvers from group config: {e}")
        return {}

    @staticmethod
    def _group_nvrs_by_rhel_version(nvrs: List[str]) -> Dict[str, List[str]]:
        """Group NVRs by their RHEL version suffix (e.g. 'el8', 'el9').

        NVRs without a detectable .el* suffix go under the 'default' key.
        Returns an OrderedDict-like dict sorted by key for deterministic ordering.
        """
        groups: Dict[str, List[str]] = {}
        for nvr in nvrs:
            # The release field is the last hyphen-delimited segment of an NVR
            release = nvr.rsplit('-', 1)[-1] if '-' in nvr else nvr
            el_ver = isolate_el_version_in_release(release)
            key = f"el{el_ver}" if el_ver is not None else "default"
            groups.setdefault(key, []).append(nvr)
        return dict(sorted(groups.items()))

    def _group_nvrs_by_shipment_kind(self) -> Dict[str, List[str]]:
        """Route oc-mirror containers and transport images to their respective release pipelines."""
        if not self.aggregation_profile:
            return {self.shipment_kind or "image": self.nvrs}

        kinds = {
            self.aggregation_profile.source_package: "image",
            self.aggregation_profile.aggregate_package: "binaries",
        }
        groups: Dict[str, List[str]] = {}
        for nvr in self.nvrs:
            name = parse_nvr(nvr)["name"]
            if name not in kinds:
                raise ValueError(f"Unsupported oc-mirror shipment build: {nvr}")
            kind = kinds[name]
            if self.shipment_kind and self.shipment_kind != kind:
                raise ValueError(f"{nvr} requires shipment kind {kind}, not {self.shipment_kind}")
            groups.setdefault(kind, []).append(nvr)
        if len(groups) > 1 and self.release_plan_key:
            raise ValueError("Combined oc-mirror shipments require separate image and binaries release-plan keys")
        if any(len(builds) != 1 for builds in groups.values()):
            raise ValueError("An oc-mirror release requires exactly one NVR per shipment kind")
        if len(groups) == 1 and not self.shipment_kind:
            kind = next(iter(groups))
            raise ValueError(
                f"A single-kind oc-mirror release requires explicit --shipment-kind {kind}. "
                "For a combined release, use --aggregate or supply both the source and aggregate NVRs."
            )
        return dict(sorted(groups.items()))

    async def create_snapshot(self, builds: List[str]) -> Optional[Snapshot]:
        """
        Create a snapshot from a list of build NVRs using elliott.
        """
        if not builds:
            self.logger.debug("No builds provided, skipping snapshot creation")
            return None

        self.logger.info(f"Creating Konflux snapshot for {len(builds)} builds...")

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
            for nvr in builds:
                temp_file.write(nvr + '\n')
            temp_file.flush()
            temp_file_path = temp_file.name

        snapshot_cmd = self._elliott_base_command + [
            "snapshot",
            "new",
            f"--builds-file={temp_file_path}",
        ]

        quay_auth_file = os.getenv("QUAY_AUTH_FILE")
        if quay_auth_file:
            snapshot_cmd.append(f"--pull-secret={quay_auth_file}")

        try:
            self.logger.info(f"Running elliott snapshot command: {' '.join(snapshot_cmd)}")
            _, stdout, _ = await exectools.cmd_gather_async(snapshot_cmd)
        except Exception as e:
            self.logger.exception(f"Failed to create snapshot: {e}")
            raise
        finally:
            os.unlink(temp_file_path)

        try:
            new_snapshot_obj = yaml.load(stdout)
            self.logger.info("✓ Successfully created Konflux snapshot")
            return Snapshot(spec=SnapshotSpec(**new_snapshot_obj.get("spec")), nvrs=sorted(builds))
        except Exception as e:
            self.logger.exception(f"Failed to parse elliott snapshot output: {e}")
            self.logger.debug(f"Raw output was: {stdout}")
            raise

    def create_shipment_config(
        self, snapshot: Snapshot, rhel_suffix: Optional[str] = None, shipment_kind: Optional[str] = None
    ) -> ShipmentConfig:
        """
        Create a shipment configuration for the given snapshot, reading
        the stage/prod releasePlan names for the snapshot's application from config.yaml
        in the shipment data repo.

        When rhel_suffix is given (e.g. 'el8', 'el9'), the config.yaml lookup first tries
        the key '{application}-{rhel_suffix}' and falls back to '{application}'.
        When self.release_plan_key is set it replaces '{application}' in that lookup.
        Oc-mirror binaries use the separate '{application}-binaries' entry.
        """
        if self.product is None:
            raise RuntimeError(
                "Product is not initialized. Please call run() first to load the product from group "
                "configuration, or ensure self.product is set before calling create_shipment_config()."
            )

        application = snapshot.spec.application
        self.logger.info(f"Creating shipment config for application '{application}' (rhel_suffix={rhel_suffix!r})...")

        metadata_extra = (
            {"release_notes_in_rpa": True}
            if self.group.startswith("openshift-") or self.group == "oc-mirror-2.0"
            else {}
        )
        metadata = Metadata(
            product=self.product,
            application=application,
            group=self.group,
            assembly=self.assembly,
            fbc=False,
            **metadata_extra,
        )

        base_key = self.release_plan_key or application
        if self.aggregation_profile and shipment_kind == "binaries" and not self.release_plan_key:
            base_key = f"{application}-binaries"
        stage_rpa = "n/a"
        prod_rpa = "n/a"
        config_path = self.shipment_data_repo._directory / "config.yaml"
        if config_path.exists():
            with open(config_path, 'r') as f:
                shipment_config = stdlib_yaml.safe_load(f) or {}
            applications = shipment_config.get("applications", {})
            # Try RHEL-versioned key first (e.g. 'oc-mirror-2-0-el9'), then fall back to the
            # plain application name for products that don't split by RHEL version.
            lookup_key = f"{base_key}-{rhel_suffix}" if rhel_suffix else base_key
            app_env_config = (applications.get(lookup_key) or applications.get(base_key) or {}).get("environments", {})
            stage_rpa = app_env_config.get("stage", {}).get("releasePlan", "n/a")
            prod_rpa = app_env_config.get("prod", {}).get("releasePlan", "n/a")

        effective_key = f"{base_key}-{rhel_suffix}" if rhel_suffix else base_key

        # Prod must never use the skip-stage sentinel.
        if is_release_plan_skipped(prod_rpa):
            raise ValueError(
                f"prod releasePlan for '{effective_key}' is set to the skip-stage sentinel "
                f"({prod_rpa!r}). Skipping prod is forbidden; only stage may use "
                f"releasePlan: {SKIPPED_RELEASE_PLAN}."
            )

        # EXTRAORDINARY: stage releasePlan: Skipped bypasses Konflux stage release.
        # Use only with explicit team approval; leave a caution comment in config.yaml.
        # Product must also be listed in SKIP_STAGE_ALLOWED_PRODUCTS.
        if is_release_plan_skipped(stage_rpa):
            assert_product_may_skip_stage(self.product)
            stage_rpa = SKIPPED_RELEASE_PLAN
            self.stage_release_skipped = True
            self.logger.warning(
                "EXTRAORDINARY: stage releasePlan for '%s' (product %r) is %r — Konflux stage "
                "release and stage CDN publish will be skipped. Team approval required.",
                effective_key,
                self.product,
                SKIPPED_RELEASE_PLAN,
            )

        if stage_rpa == "n/a" or prod_rpa == "n/a":
            if self.create_mr:
                raise ValueError(
                    f"stage/prod releasePlan is not registered for '{effective_key}' in {config_path}. "
                    "Cannot create a shipment MR with unresolved ReleasePlans."
                )
            self.logger.warning(
                f"Could not resolve stage/prod releasePlan for '{effective_key}' from config.yaml. "
                "Please ensure it is registered there before merging the resulting MR."
            )

        environments = Environments(stage=ShipmentEnv(releasePlan=stage_rpa), prod=ShipmentEnv(releasePlan=prod_rpa))

        # Container and CDN ReleasePlanAdmissions carry their static release notes in
        # konflux-release-data, so neither shipment needs data.releaseNotes here.
        shipment = Shipment(metadata=metadata, environments=environments, snapshot=snapshot, data=None)

        return ShipmentConfig(shipment=shipment)

    async def create_shipment_mr(self, shipments_by_kind: Dict[str, ShipmentConfig]) -> str:
        """
        Create a new shipment MR with the given shipment config files (one per kind).
        """
        if not self.create_mr:
            return ""

        self.logger.info("Creating shipment MR...")

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"binary-release-{self.assembly}-{timestamp}"
        target_branch = "main"

        await self.shipment_data_repo.create_branch(source_branch)

        commit_message = f"Add shipment configuration for {self.product} {self.assembly}"
        updated = await self.update_shipment_data(shipments_by_kind, "prod", commit_message, timestamp)
        if not updated:
            raise ValueError("Failed to update shipment data repo. Please investigate.")

        source_project = self._get_gitlab_project(self.shipment_data_repo_push_url)
        target_project = self._get_gitlab_project(self.shipment_data_repo_pull_url)

        if self.target_release_date:
            mr_title = f"Draft: Shipment for {self.product} {self.assembly} (ship date: {self.target_release_date})"
        else:
            mr_title = f"Draft: Shipment for {self.product} {self.assembly}"
        mr_description = f"Created by job: {self.job_url}\n\n" if self.job_url else ""
        mr_description += f"Shipment file created for {self.assembly} using binary-release-konflux command"
        if self.stage_release_skipped:
            mr_description += (
                "\n\n**CAUTION — stage release skipped:** `config.yaml` sets stage "
                f"`releasePlan: {SKIPPED_RELEASE_PLAN}` for this application. This is an "
                "extraordinary, team-approved workflow that bypasses Konflux stage release "
                "and stage CDN publish. CI will apply the `stage-release-skipped` label "
                "instead of `stage-release-success`."
            )

        if self.dry_run:
            self.logger.info("[DRY-RUN] Would have created MR with title: %s", mr_title)
            mr_url = f"{self.gitlab_url}/placeholder/placeholder/-/merge_requests/placeholder"
        else:
            mr = source_project.mergerequests.create(
                {
                    'source_branch': source_branch,
                    'target_project_id': target_project.id,
                    'target_branch': target_branch,
                    'title': mr_title,
                    'description': mr_description,
                    'remove_source_branch': True,
                }
            )
            mr_url = mr.web_url
            self.logger.info("Created Merge Request: %s", mr_url)

        approvers_config = await self._load_mr_approvers_from_group_config()
        if approvers_config:
            if self.dry_run:
                self.logger.info("[DRY-RUN] Would set MR approval rules: %s", approvers_config)
            else:
                try:
                    await self._gitlab.set_mr_approval_rules(mr_url, approvers_config)
                except Exception as e:
                    self.logger.warning(f"Failed to set MR approval rules: {e}")
        elif self.group.startswith("openshift-"):
            self._keep_only_art_approval_rule(mr_url)

        self.shipment_mr_url = mr_url
        return mr_url

    def _keep_only_art_approval_rule(self, mr_url: str):
        """
        Remove every MR approval rule except ART, as golang builder shipments do.

        The shipment repo's default rules are ART + ERT + Docs; CDN binary shipments
        for openshift groups only need ART sign-off.

        :param mr_url: URL of the shipment merge request.
        """
        if self.dry_run:
            self.logger.info("[DRY-RUN] Would remove non-ART approval rules from %s", mr_url)
            return
        try:
            mr = self._gitlab.get_mr_from_url(mr_url)
            if not mr:
                self.logger.warning("Could not load MR %s to clean up approval rules", mr_url)
                return
            for rule in mr.approval_rules.list():
                if rule.name != "ART":
                    self.logger.info("Removing approval rule '%s' (not needed for CDN binary shipment)", rule.name)
                    rule.delete()
        except Exception as e:
            self.logger.warning("Could not clean up approval rules: %s", e)

    async def set_shipment_mr_ready(self):
        """
        Mark the shipment MR as ready by removing the Draft prefix from the title.
        """
        if self.dry_run:
            self.logger.info("[DRY-RUN] Would set shipment MR ready: %s", self.shipment_mr_url)
            return

        mr = await self._gitlab.set_mr_ready(self.shipment_mr_url)

        if mr:
            self.logger.info("Waiting for 30 seconds to ensure MR is updated...")
            await asyncio.sleep(30)

            try:
                pipeline_url = await self._gitlab.trigger_ci_pipeline(mr)
                if pipeline_url:
                    self.logger.info(f"CI pipeline triggered: {pipeline_url}")
                else:
                    self.logger.warning(f"Failed to trigger CI pipeline for MR branch {mr.source_branch}")
            except Exception as e:
                self.logger.warning(f"Failed to trigger CI MR pipeline for branch {mr.source_branch}: {e}")

    async def update_shipment_data(
        self, shipments_by_kind: Dict[str, ShipmentConfig], env: str, commit_message: str, timestamp: str
    ) -> bool:
        """
        Write all shipment config files and push in a single commit.
        """
        if not self.create_mr:
            return False

        for advisory_kind, shipment_config in shipments_by_kind.items():
            filepath = await self._write_shipment_file(advisory_kind, shipment_config, env, timestamp)
            self.logger.info("Updating shipment file: %s", filepath)

        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()
        return await self.shipment_data_repo.commit_push(commit_message, safe=True)

    async def _write_shipment_file(
        self, advisory_kind: str, shipment_config: ShipmentConfig, env: str, timestamp: str
    ) -> str:
        """
        Write the shipment file to disk under the standard shipment path convention:
        shipment/{product}/{group}/{application}/{env}/{assembly}.{advisory_kind}.{timestamp}.yaml
        """
        filename = f"{self.assembly}.{advisory_kind}.{timestamp}.yaml"

        product = shipment_config.shipment.metadata.product
        group = shipment_config.shipment.metadata.group
        application = shipment_config.shipment.metadata.application
        relative_target_dir = Path("shipment") / product / group / application / env
        target_dir = self.shipment_data_repo._directory / relative_target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        filepath = relative_target_dir / filename

        shipment_dump = shipment_config.model_dump(exclude_unset=True, exclude_none=True)
        out = StringIO()
        yaml.dump(shipment_dump, out)
        await self.shipment_data_repo.write_file(filepath, out.getvalue())

        return str(filepath)

    async def write_shipment_files_locally(
        self, shipments_by_kind: Dict[str, ShipmentConfig], env: str, timestamp: str
    ):
        """
        Write shipment files to the local repository without creating an MR.
        """
        for advisory_kind, shipment_config in shipments_by_kind.items():
            filepath = await self._write_shipment_file(advisory_kind, shipment_config, env, timestamp)
            self.logger.info(f"Created shipment file: {filepath}")

    @staticmethod
    def _aggregate_data_path() -> str:
        """Require a remote recipe repository so its metadata and build context agree."""
        data_path = os.environ.get("DOOZER_DATA_PATH", constants.OCP_BUILD_DATA_URL)
        data_url = urlparse(data_path)
        if data_url.scheme == "file" or (not data_url.scheme and ":" not in data_url.path):
            raise ValueError(
                "Aggregation requires a remote Git URL for DOOZER_DATA_PATH; local working trees are not supported. "
                "The recipe and metadata must come from the same fetched build-data checkout."
            )
        return data_path

    async def resolve_aggregate_parents(self, source_snapshot: Snapshot) -> Dict[str, str]:
        """Resolve and validate the selected source image's four Linux architecture digests."""
        profile = self.aggregation_profile
        components = source_snapshot.spec.components
        if source_snapshot.spec.application != profile.application or len(components) != 1:
            raise ValueError(f"Aggregation requires one {profile.application} source component")
        if components[0].name != profile.source_component:
            raise ValueError("Source NVR must be the oc-mirror image build, not an existing aggregate")
        source_image = components[0].containerImage
        if not re.fullmatch(r".+@sha256:[0-9a-f]{64}", source_image):
            raise ValueError("Source NVR must resolve to a digest-pinned image")
        image_repo = source_image.split("@", 1)[0]
        if ":" in image_repo.rsplit("/", 1)[-1]:
            image_repo = image_repo.rsplit(":", 1)[0]
        registry_config = os.environ.get("QUAY_AUTH_FILE")
        parents = {}
        for arch in profile.arches:
            info = await oc_image_info_for_arch_async(
                source_image, go_arch=f"linux/{arch}", registry_config=registry_config
            )
            digest = info.get("digest", "")
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError(f"No valid {arch} digest for {source_image}")
            image_config = info.get("config") or {}
            if image_config.get("os") != "linux" or image_config.get("architecture") != arch:
                raise ValueError(f"{source_image} does not provide a linux/{arch} image")
            parents[arch] = f"{image_repo}@{digest}"
        self.logger.info("Aggregate source image: %s; parents: %s", source_image, parents)
        return parents

    @staticmethod
    def verify_aggregate_parents(dockerfile_text: str, parents: Dict[str, str]):
        """Require exactly the four selected source parents in a rebased aggregate Dockerfile."""
        stages = re.findall(r"^FROM --platform=linux/(\S+) (\S+) AS (\S+)$", dockerfile_text, re.MULTILINE)
        actual = {arch: image for arch, image, alias in stages if arch == alias}
        if len(stages) != len(parents) or actual != parents:
            raise ValueError("Rebased aggregate parents do not match the selected source NVR")

    async def verify_reused_aggregate(self, source_snapshot: Snapshot, aggregate_snapshot: Snapshot):
        """Verify a reused transport image's recorded build source against the selected container."""
        component = aggregate_snapshot.spec.components[0]
        if not re.fullmatch(r".+@sha256:[0-9a-f]{64}", component.containerImage):
            raise ValueError("Reused aggregate must resolve to a digest-pinned image")
        source = component.source.git if component.source else None
        if not source or not source.url or not re.fullmatch(r"[0-9a-f]{40}", source.revision or ""):
            raise ValueError("Reused aggregate requires a recorded Git URL and full rebase commit")
        parents = await self.resolve_aggregate_parents(source_snapshot)
        with tempfile.TemporaryDirectory(prefix="aggregate-verification-", dir=self.working_dir) as scratch:
            repo = GitRepository(scratch)
            await repo.setup(source.url)
            await repo.fetch_switch_branch("aggregate-verification", upstream_ref=source.revision)
            self.verify_aggregate_parents(await repo.read_file("Dockerfile"), parents)

    async def build_aggregate(self, source_snapshot: Snapshot) -> str:
        """Build one transport image from the selected oc-mirror NVR's four architecture images."""
        profile = self.aggregation_profile
        data_path = self._aggregate_data_path()
        parents = await self.resolve_aggregate_parents(source_snapshot)
        image_repo = source_snapshot.spec.components[0].containerImage.split("@", 1)[0]
        if ":" in image_repo.rsplit("/", 1)[-1]:
            image_repo = image_repo.rsplit(":", 1)[0]

        with tempfile.TemporaryDirectory(prefix="aggregate-build-data-", dir=self.working_dir) as scratch:
            data_dir = Path(scratch, "ocp-build-data")
            data_repo = GitRepository(data_dir)
            await data_repo.setup(data_path)
            await data_repo.fetch_switch_branch(self.group)
            config_path = data_dir / f"binaries/{profile.member}.yml"
            config = stdlib_yaml.safe_load(config_path.read_text())
            source_config = config["content"]["source"]
            source_git_url = (source_config.get("git") or {}).get("url")
            if not source_git_url:
                raise ValueError("The aggregate recipe must declare content.source.git.url for the rebase destination")
            # Keep this fetched recipe checkout, but honor its declared Git destination.
            # SourceResolver derives the rebase remote from the local alias's origin.
            await data_repo.setup(source_git_url)
            config["from"]["builder"] = [{"image": image} for image in parents.values()]
            source_config.pop("git")
            source_config["alias"] = profile.member
            config_path.write_text(stdlib_yaml.safe_dump(config, sort_keys=False))

            base = [
                "doozer",
                f"--group={self.group}",
                f"--assembly={self.assembly}",
                "--build-system=konflux",
                f"--data-path={data_dir}",
                f"--working-dir={self.doozer_working_dir}",
                f"--binaries={profile.member}",
                "--source",
                profile.member,
                str(data_dir),
            ]
            await exectools.cmd_assert_async(
                base
                + [
                    "beta:binaries:konflux:rebase",
                    f"--version={profile.version}",
                    f"--release={util.default_release_suffix()}",
                    f"--image-repo={image_repo}",
                    f"--message=Aggregate {self.nvrs[0]}",
                    "--push",
                ]
            )
            dockerfile = self.doozer_working_dir / f"konflux_build_sources/containers/{profile.member}/Dockerfile"
            self.verify_aggregate_parents(dockerfile.read_text(), parents)
            build = base + [
                "beta:binaries:konflux:build",
                f"--image-repo={image_repo}",
                f"--konflux-namespace={resolve_konflux_namespace_by_product(self.product)}",
            ]
            kubeconfig = resolve_konflux_kubeconfig_by_product(self.product)
            if kubeconfig:
                build.append(f"--konflux-kubeconfig={kubeconfig}")
            await exectools.cmd_assert_async(build)

        with (self.doozer_working_dir / "record.log").open() as record_file:
            records = record_util.parse_record_log(record_file)
        builds = [r for r in records.get("binary_build_konflux", []) if r.get("name") == profile.member]
        if len(builds) != 1 or builds[0].get("status") != "0":
            raise ValueError("No successful aggregate build was recorded")
        aggregate_nvr = builds[0].get("nvrs", "")
        if not aggregate_nvr or aggregate_nvr == "n/a" or "," in aggregate_nvr:
            raise ValueError("Expected exactly one aggregate NVR")
        if is_nvr_embargoed(aggregate_nvr):
            raise ValueError("Refusing to release an embargoed aggregate build")
        self.logger.info("Aggregate NVR: %s; source NVR: %s", aggregate_nvr, self.nvrs[0])
        return aggregate_nvr

    async def run(self) -> None:
        """
        Resolve builds and prepare their container-advisory or CDN shipment files.
        """
        self.logger.info(f"Starting binary release workflow for {self.group} {self.assembly}")
        self.logger.info(f"Processing {len(self.nvrs)} NVR(s): {self.nvrs}")

        self.check_env_vars()
        if self.aggregate and self.dry_run:
            self.working_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.setup_working_dir()

        self.product = await self._load_product_from_group_config()
        self.logger.info(f"Loaded product '{self.product}' - continuing workflow for {self.product} {self.assembly}")

        # Refuse to release embargoed (private-fix) NVRs to a public CDN.
        try:
            embargoed_nvrs = [nvr for nvr in self.nvrs if is_nvr_embargoed(nvr)]
        except ValueError as e:
            raise RuntimeError(f"Refusing to create a release: unable to determine embargo status: {e}") from e
        if embargoed_nvrs:
            raise RuntimeError(
                f"Refusing to create a release referencing embargoed (private-fix) NVR(s): {embargoed_nvrs}"
            )

        if self.aggregate:
            source_snapshot = await self.create_snapshot(self.nvrs)
            if not source_snapshot:
                raise RuntimeError("No snapshot could be created for the aggregate source NVR")
            if self.dry_run:
                self._aggregate_data_path()
                await self.resolve_aggregate_parents(source_snapshot)
                self.logger.info(
                    "[DRY-RUN] Validated aggregate inputs for %s; build-only=%s", self.nvrs[0], self.build_only
                )
                return
            aggregate_nvr = await self.build_aggregate(source_snapshot)
            if self.build_only:
                self.logger.info("--build-only: not creating a shipment for %s", aggregate_nvr)
                return
            self.nvrs = self.nvrs + [aggregate_nvr]

        snapshots_by_kind = {}
        for base_kind, kind_nvrs in self._group_nvrs_by_shipment_kind().items():
            # Split RHEL variants within each artifact kind, preserving existing filenames.
            nvr_groups = self._group_nvrs_by_rhel_version(kind_nvrs)
            multi_rhel = len(nvr_groups) > 1
            for rhel_suffix, group_nvrs in nvr_groups.items():
                snapshot = await self.create_snapshot(group_nvrs)
                if not snapshot:
                    raise RuntimeError(f"No snapshot could be created for {base_kind} {rhel_suffix} NVR(s)")
                if self.aggregation_profile:
                    profile = self.aggregation_profile
                    expected = profile.source_component if base_kind == "image" else profile.aggregate_component
                    if snapshot.spec.application != profile.application or [
                        component.name for component in snapshot.spec.components
                    ] != [expected]:
                        raise ValueError(f"Unexpected oc-mirror {base_kind} snapshot components")

                advisory_kind = f"{base_kind}-{rhel_suffix}" if multi_rhel else base_kind
                snapshots_by_kind[advisory_kind] = (base_kind, snapshot, rhel_suffix if multi_rhel else None)

        if self.aggregation_profile and not self.aggregate and len(snapshots_by_kind) > 1:
            await self.verify_reused_aggregate(snapshots_by_kind["image"][1], snapshots_by_kind["binaries"][1])

        if self.create_mr:
            await self.setup_shipment_repo()

        shipments_by_kind: Dict[str, ShipmentConfig] = {}
        for advisory_kind, (base_kind, snapshot, rhel_suffix) in snapshots_by_kind.items():
            options = {"shipment_kind": base_kind} if self.aggregation_profile else {}
            shipments_by_kind[advisory_kind] = self.create_shipment_config(snapshot, rhel_suffix=rhel_suffix, **options)

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        if not self.create_mr:
            await self.write_shipment_files_locally(shipments_by_kind, "prod", timestamp)
        else:
            mr_url = await self.create_shipment_mr(shipments_by_kind)
            if mr_url:
                self.logger.info(f"Created shipment MR: {mr_url}")
                await self.set_shipment_mr_ready()

        completion_msg = (
            f"Binary release workflow completed for {self.product} {self.assembly}. "
            f"Created {len(shipments_by_kind)} shipment file(s) ({', '.join(shipments_by_kind.keys())})."
        )
        if self.shipment_mr_url:
            completion_msg += f" MR: {self.shipment_mr_url}"
        self.logger.info(completion_msg)


@cli.command("binary-release-konflux")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The ocp-build-data group to operate on, e.g. oc-mirror-2.0",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly to operate on, e.g. stream",
)
@click.option(
    "--nvrs",
    metavar="NVRS",
    required=True,
    help="Comma-separated list of build NVR(s) to release, "
    "e.g. oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9",
)
@click.option(
    "--create-mr",
    is_flag=True,
    help="Create a merge request in the shipment data repository (requires GITLAB_TOKEN environment variable)",
)
@click.option(
    '--shipment-data-repo-url',
    help='Shipment data repository URL for MR creation. If not provided, will use default based on configuration.',
)
@click.option(
    '--target-release-date',
    default=None,
    callback=validate_release_date,
    help='Target ship date for the release (e.g., 2026-Mar-31 or 2026-03-31). '
    'When provided, the date is included in the shipment MR title.',
)
@click.option(
    '--release-plan-key',
    default=None,
    help='ocp-shipment-data config.yaml applications key to read the stage/prod releasePlans from, instead of '
    'the snapshot application name. Required for openshift-* groups, e.g. openshift-4-22-binaries.',
)
@click.option(
    '--shipment-kind',
    default=None,
    help='Shipment file kind used in the file name and by shipment CI (default: image). '
    'Required for single-kind oc-mirror releases and openshift-* groups, e.g. image, binaries or binaries-dev-preview.',
)
@click.option('--aggregate', is_flag=True, help='Aggregate the four architectures of one oc-mirror-2.0 source NVR.')
@click.option('--build-only', is_flag=True, help='With --aggregate, build and verify without creating a shipment.')
@pass_runtime
@click_coroutine
async def binary_release_konflux(
    runtime: Runtime,
    group: str,
    assembly: str,
    nvrs: str,
    create_mr: bool,
    shipment_data_repo_url: Optional[str],
    target_release_date: Optional[str],
    release_plan_key: Optional[str],
    shipment_kind: Optional[str],
    aggregate: bool,
    build_only: bool,
):
    """
    Prepare container-advisory and CDN binary shipments from Konflux builds.

    This command resolves the given build NVR(s) into a Konflux snapshot and creates a
    shipment files using the stage/prod ReleasePlans in ocp-shipment-data's config.yaml.
    For oc-mirror, the container uses rh-advisories and the aggregate uses push-artifacts-to-cdn.
    Both RPAs carry their static release notes, so no per-shipment release notes are generated.

    \b
    # oc-mirror 2.0 container and binary release:
    $ artcd binary-release-konflux \\
        --group oc-mirror-2.0 \\
        --assembly stream \\
        --nvrs oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9 \\
        --aggregate --create-mr

    For oc-mirror, --aggregate builds the transport image and prepares both container
    and binaries shipment files in one MR. To reuse a successful build, pass both
    the source and aggregate NVRs without --aggregate.
    An image-only or binaries-only oc-mirror release requires --shipment-kind image or binaries.
    Add --build-only to stop after building and verifying that image.
    """
    nvrs_list = [nvr.strip() for nvr in nvrs.split(',') if nvr.strip()]
    if not nvrs_list:
        raise click.ClickException("--nvrs must contain at least one valid NVR")

    pipeline = BinaryReleaseKonfluxPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        nvrs=nvrs_list,
        create_mr=create_mr,
        shipment_data_repo_url=shipment_data_repo_url,
        target_release_date=target_release_date,
        release_plan_key=release_plan_key,
        shipment_kind=shipment_kind,
        aggregate=aggregate,
        build_only=build_only,
    )

    await pipeline.run()
