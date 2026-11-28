import asyncio
import logging
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import click
from artcommonlib import exectools
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.github_auth import build_git_auth_env
from artcommonlib.gitlab import GitLabClient
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import (
    Environments,
    Metadata,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    SnapshotSpec,
)

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

yaml = new_roundtrip_yaml_handler()

# Brew/Konflux component (package) names whose standalone-image shipments default to
# silent (non-advisory, registry-push-only) delivery instead of a normal advisory
# release. Silent delivery is a property of a *consumer* of this pipeline, not of the
# pipeline itself — most standalone images should ship through the normal advisory
# process (the default), so only add a component here when it has a documented need
# to skip the advisory pipeline. operator-registry needs this so FBC builds across RH
# can pull a given y-stream's image without an advisory (see ART-25199/ART-14308).
# The default can always be overridden per-invocation with --silent/--public.
SILENT_BY_DEFAULT_COMPONENTS = {
    "operator-registry-container",
}


class StandaloneImagesShipPipeline:
    """
    Prepare a Konflux shipment MR for one or more standalone image NVRs — images
    that are not part of the normal OCP release payload, e.g. operator-registry or
    another standalone-image project. By default the shipment is a normal advisory
    release; components listed in SILENT_BY_DEFAULT_COMPONENTS instead default to a
    silent (non-advisory, registry-push-only) release. Either can be forced per-run
    via --silent/--public.

    This pipeline only resolves the given NVRs into a Konflux snapshot and opens a
    shipment MR against ocp-shipment-data; it does not itself create any Konflux
    Snapshot or Release object. Once the MR is reviewed/merged, shipment-ci's
    generated CI jobs run `elliott release new --env=stage|prod ...` (which creates
    the real Snapshot and Release objects, independently per environment) and
    `elliott release watch ...`. Stage is gated by MR approval; prod is a
    separately-triggered pipeline run after stage succeeds.

    v1 limitation: all NVRs passed to one invocation must resolve to a single
    Konflux application (one stage/prod ReleasePlan pair). Shipping NVRs that span
    multiple applications in one MR (the way oc-mirror bundles its "image" and
    "binaries" shipment kinds) is a deliberately deferred v2 extension.
    """

    _JIRA_KEY_RE = re.compile(r"^[A-Z][A-Z0-9]+-\d+$")
    _JIRA_BROWSE_URL = "https://redhat.atlassian.net/browse"

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        nvrs: List[str],
        stage_release_plan: str,
        prod_release_plan: str,
        jiras: Optional[List[str]] = None,
        create_mr: bool = False,
        shipment_data_repo_url: Optional[str] = None,
        job_url: Optional[str] = None,
        silent: Optional[bool] = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.nvrs = nvrs
        self.stage_release_plan = stage_release_plan
        self.prod_release_plan = prod_release_plan
        self.jiras = jiras or []
        self.create_mr = create_mr
        self.dry_run = self.runtime.dry_run
        self.job_url = job_url

        invalid_jiras = [j for j in self.jiras if not self._parse_jira_key(j)]
        if invalid_jiras:
            raise ValueError(f"Invalid Jira issue key(s): {', '.join(invalid_jiras)}")

        self.silent = self._resolve_silent(silent)

        # Setup working directories
        self.working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self._shipment_data_repo_dir = self.working_dir / "shipment-data-push"

        # Shipment repository configuration
        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.gitlab_token = None
        self.shipment_mr_url: Optional[str] = None

        # Product configuration - initialized to None, will be loaded from group config in run()
        self.product: Optional[str] = None

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

    @staticmethod
    def _parse_jira_key(jira_ref: str) -> str:
        """Extract and validate a JIRA issue key from a URL or bare key.

        Accepts:
          - "ART-1234" (bare key)
          - "https://redhat.atlassian.net/browse/ART-1234" (full URL)
        Returns the issue key or empty string if invalid.
        """
        value = jira_ref.strip()
        if StandaloneImagesShipPipeline._JIRA_KEY_RE.match(value):
            return value
        parsed = urlparse(value)
        if parsed.hostname != "redhat.atlassian.net":
            return ""
        key = parsed.path.rstrip("/").split("/")[-1]
        if not StandaloneImagesShipPipeline._JIRA_KEY_RE.match(key):
            return ""
        return key

    def _resolve_silent(self, silent_override: Optional[bool]) -> bool:
        """
        Determine whether this run should be a silent (non-advisory) shipment.

        An explicit --silent/--public override always wins. Otherwise, the default is
        derived from each NVR's component name against SILENT_BY_DEFAULT_COMPONENTS.
        All NVRs in one invocation must agree on the default; mixed components without
        an explicit override is an error rather than a guess.
        """
        if silent_override is not None:
            return silent_override

        components = {parse_nvr(nvr)['name'] for nvr in self.nvrs}
        defaults = {component in SILENT_BY_DEFAULT_COMPONENTS for component in components}
        if len(defaults) > 1:
            raise ValueError(
                f"NVR components {sorted(components)} have different default silent/public "
                "classifications; pass --silent or --public explicitly to disambiguate."
            )
        return defaults.pop()

    def check_env_vars(self):
        """
        Check required environment variables for MR creation.
        """
        if not self.create_mr:
            return

        gitlab_token = os.getenv("GITLAB_TOKEN")
        if not gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")
        self.gitlab_token = gitlab_token

        if not self.job_url:
            self.job_url = os.getenv('BUILD_URL')

    def setup_working_dir(self):
        """
        Setup working directories, cleaning up any existing ones.
        """
        self.working_dir.mkdir(parents=True, exist_ok=True)
        if self.elliott_working_dir.exists():
            shutil.rmtree(self.elliott_working_dir, ignore_errors=True)
        if self.create_mr and self._shipment_data_repo_dir.exists():
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
            if product and product not in ('None', 'null'):
                self.logger.info(f"Loaded product from group config: {product}")
                return product
            self.logger.debug("No product field found in group config, falling back to group name extraction")
        except Exception as e:
            self.logger.warning(f"Failed to load product from group config: {e}")

        # Fallback: extract product from group name (e.g., "oc-mirror-2.0" -> "oc-mirror")
        product = self.group.rsplit('-', 1)[0]
        if product == 'openshift':
            product = 'ocp'
        self.logger.info(f"Using product extracted from group name: {product}")
        return product

    async def create_snapshot(self, nvrs: List[str]) -> Snapshot:
        """
        Resolve the given NVRs into a single-application Konflux snapshot spec using
        `elliott snapshot new` in dry-run mode (no `--apply`). This never touches the
        cluster; it only resolves NVRs to digest-pinned pullspecs so they can be
        embedded in the shipment YAML. The actual Snapshot object is created later by
        shipment-ci's `elliott release new --apply`.
        """
        self.logger.info(f"Resolving Konflux snapshot spec for {len(nvrs)} NVR(s)...")

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
            for nvr in nvrs:
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
            self.logger.exception(f"Failed to resolve snapshot spec: {e}")
            raise
        finally:
            os.unlink(temp_file_path)

        try:
            snapshot_objs = list(yaml.load_all(stdout))
        except Exception as e:
            self.logger.exception(f"Failed to parse elliott snapshot output: {e}")
            self.logger.debug(f"Raw output was: {stdout}")
            raise

        if len(snapshot_objs) != 1:
            applications = sorted({obj.get("spec", {}).get("application") for obj in snapshot_objs})
            raise ValueError(
                f"NVRs {nvrs} resolve to {len(snapshot_objs)} Konflux application(s) ({', '.join(applications)}); "
                "standalone-images-ship v1 only supports NVRs from a single application in one invocation. "
                "Run the pipeline once per application instead."
            )

        self.logger.info("Successfully resolved Konflux snapshot spec")
        return Snapshot(spec=SnapshotSpec(**snapshot_objs[0]["spec"]), nvrs=sorted(nvrs))

    def create_shipment_config(self, snapshot: Snapshot) -> ShipmentConfig:
        """
        Build a shipment configuration for the given snapshot, silent or public
        depending on self.silent (see SILENT_BY_DEFAULT_COMPONENTS / --silent/--public).
        """
        if self.product is None:
            raise RuntimeError("Product is not initialized. Please call run() first.")

        application = snapshot.spec.application
        kind = "silent" if self.silent else "public"
        self.logger.info(f"Creating {kind} shipment config for application '{application}'...")

        metadata = Metadata(
            product=self.product,
            application=application,
            group=self.group,
            assembly=self.assembly,
            fbc=False,
            # The real "silent" flag: shipment-ci skips its advisory-URL check for a
            # registry-push-only release when advisory_required is false. There is no
            # advisory/release-notes payload for a silent shipment, so docs approval
            # has nothing to check either. A public (advisory) shipment leaves both at
            # their normal defaults (True).
            advisory_required=not self.silent,
            docs_approval_required=not self.silent,
            # openshift-* shipments otherwise require data.releaseNotes (Shipment's
            # own "make_sure_data_is_present_unless_fbc" validator). A silent shipment
            # has no release notes to give; a public one is expected to have its
            # release notes managed in the ReleasePlanAdmission rather than typed into
            # the shipment file (matches the convention binary_release_konflux.py
            # already uses for openshift-*/oc-mirror-2.0 shipments, silent or not).
            release_notes_in_rpa=self.group.startswith("openshift-"),
        )
        environments = Environments(
            stage=ShipmentEnv(releasePlan=self.stage_release_plan),
            prod=ShipmentEnv(releasePlan=self.prod_release_plan),
        )
        shipment = Shipment(metadata=metadata, environments=environments, snapshot=snapshot, data=None)
        return ShipmentConfig(shipment=shipment)

    async def _write_shipment_file(self, shipment_config: ShipmentConfig, env: str, timestamp: str) -> str:
        """
        Write the shipment file to disk under the standard shipment path convention:
        shipment/{product}/{group}/{application}/{env}/{assembly}.image.{timestamp}.yaml
        """
        filename = f"{self.assembly}.image.{timestamp}.yaml"

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

    async def write_shipment_file_locally(self, shipment_config: ShipmentConfig, timestamp: str) -> str:
        """
        Write the shipment file to the local repository without creating an MR.
        """
        filepath = await self._write_shipment_file(shipment_config, "prod", timestamp)
        self.logger.info(f"Created shipment file: {filepath}")
        return filepath

    def _mr_description(self) -> str:
        description = f"Created by job: {self.job_url}\n\n" if self.job_url else ""
        description += f"Silent image shipment for {self.product} {self.assembly} using standalone-images-ship command"
        if self.jiras:
            description += "\n\nTracking ticket(s):"
            for jira_ref in self.jiras:
                issue_key = self._parse_jira_key(jira_ref)
                description += f"\n- {self._JIRA_BROWSE_URL}/{issue_key}"
        return description

    async def create_shipment_mr(self, shipment_config: ShipmentConfig) -> str:
        """
        Write the shipment file and open a single MR against the shipment data repo.
        """
        if not self.create_mr:
            return ""

        self.logger.info("Creating shipment MR...")

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"standalone-images-ship-{self.assembly}-{timestamp}"
        target_branch = "main"

        await self.shipment_data_repo.create_branch(source_branch)

        commit_message = f"Add silent image shipment configuration for {self.product} {self.assembly}"
        filepath = await self._write_shipment_file(shipment_config, "prod", timestamp)
        self.logger.info("Updating shipment file: %s", filepath)
        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()
        updated = await self.shipment_data_repo.commit_push(commit_message, safe=True)
        if not updated:
            raise ValueError("Failed to update shipment data repo. Please investigate.")

        source_project = self._get_gitlab_project(self.shipment_data_repo_push_url)
        target_project = self._get_gitlab_project(self.shipment_data_repo_pull_url)

        mr_title = f"Draft: Silent shipment for {self.product} {self.assembly}"
        mr_description = self._mr_description()

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

        self.shipment_mr_url = mr_url
        return mr_url

    async def set_shipment_mr_ready(self):
        """
        Mark the shipment MR as ready by removing the Draft prefix from the title,
        which triggers shipment-ci's generated CI pipeline (stage release, gated by
        MR approval; prod is a separately-triggered pipeline run after stage).
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

    async def run(self) -> None:
        """
        Resolve the given NVRs and prepare an image shipment MR (silent or public,
        see self.silent).
        """
        kind = "silent" if self.silent else "public"
        self.logger.info(f"Starting standalone-images-ship ({kind}) workflow for {self.group} {self.assembly}")
        self.logger.info(f"Processing {len(self.nvrs)} NVR(s): {self.nvrs}")

        self.check_env_vars()
        self.setup_working_dir()

        self.product = await self._load_product_from_group_config()
        self.logger.info(f"Loaded product '{self.product}' - continuing workflow for {self.product} {self.assembly}")

        # Refuse to ship embargoed (private-fix) NVRs, silent or public.
        try:
            embargoed_nvrs = [nvr for nvr in self.nvrs if is_nvr_embargoed(nvr)]
        except ValueError as e:
            raise RuntimeError(f"Refusing to create a release: unable to determine embargo status: {e}") from e
        if embargoed_nvrs:
            raise RuntimeError(
                f"Refusing to create a release referencing embargoed (private-fix) NVR(s): {embargoed_nvrs}"
            )

        snapshot = await self.create_snapshot(self.nvrs)
        shipment_config = self.create_shipment_config(snapshot)

        if self.create_mr:
            await self.setup_shipment_repo()
            mr_url = await self.create_shipment_mr(shipment_config)
            if mr_url:
                self.logger.info(f"Created shipment MR: {mr_url}")
                await self.set_shipment_mr_ready()
        else:
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            await self.write_shipment_file_locally(shipment_config, timestamp)

        completion_msg = f"standalone-images-ship workflow completed for {self.product} {self.assembly}."
        if self.shipment_mr_url:
            completion_msg += f" MR: {self.shipment_mr_url}"
        self.logger.info(completion_msg)


@cli.command("standalone-images-ship")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The ocp-build-data group to operate on, e.g. openshift-5.2",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly to operate on, e.g. stream",
)
@click.option(
    "--nvr",
    "nvrs",
    metavar="NVR",
    multiple=True,
    required=True,
    help="[MULTIPLE] Build NVR to ship. All NVRs in one invocation must resolve to a single Konflux application.",
)
@click.option(
    "--jira",
    "jiras",
    metavar="ISSUE_KEY",
    multiple=True,
    help="[MULTIPLE] Jira issue key (or browse URL) to reference in the shipment MR description for traceability.",
)
@click.option(
    "--stage-release-plan",
    metavar="RELEASE_PLAN",
    required=True,
    help="Konflux ReleasePlan name to use for the stage environment.",
)
@click.option(
    "--prod-release-plan",
    metavar="RELEASE_PLAN",
    required=True,
    help="Konflux ReleasePlan name to use for the prod environment.",
)
@click.option(
    "--silent/--public",
    "silent",
    default=None,
    help=(
        "Force a silent (non-advisory, registry-push-only) or a public (normal advisory) "
        "shipment. If neither is given, the default is chosen per-NVR's component against "
        "SILENT_BY_DEFAULT_COMPONENTS in standalone_images_ship.py (currently: "
        "operator-registry-container). All NVRs in one invocation must agree on the default "
        "if neither flag is given."
    ),
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
    '--job-url',
    metavar='URL',
    default=None,
    help='The URL of the job running this pipeline. Included in the MR description.',
)
@pass_runtime
@click_coroutine
async def standalone_images_ship(
    runtime: Runtime,
    group: str,
    assembly: str,
    nvrs: tuple,
    jiras: tuple,
    stage_release_plan: str,
    prod_release_plan: str,
    silent: Optional[bool],
    create_mr: bool,
    shipment_data_repo_url: Optional[str],
    job_url: Optional[str],
):
    """
    Prepare a Konflux shipment MR for one or more standalone image NVRs, e.g.
    operator-registry or another standalone-image project.

    This command resolves the given NVRs into a Konflux snapshot and opens a shipment
    MR using the given stage/prod ReleasePlans. By default the shipment is a normal
    advisory release; components listed in SILENT_BY_DEFAULT_COMPONENTS (e.g.
    operator-registry) instead default to a silent shipment, with
    `metadata.advisory_required` set to false so shipment-ci treats it as a
    registry-push-only release. Either can be forced with --silent/--public. This
    command does not create any Konflux Snapshot or Release object itself —
    shipment-ci's existing generated CI jobs do that once the MR is reviewed and
    merged.

    \b
    # Operator-registry silent release for 5.2 (silent is the default for this component):
    $ artcd standalone-images-ship \\
        --group openshift-5.2 --assembly stream \\
        --nvr operator-registry-container-v5.2.0-202611252026.p2.gffec6b4.assembly.stream.el9 \\
        --jira ART-25199 \\
        --stage-release-plan ocp-art-stage-silent-5.2 \\
        --prod-release-plan ocp-art-prod-silent-5.2 \\
        --create-mr

    \b
    # A different standalone-image project, shipped as a normal public advisory (the default):
    $ artcd standalone-images-ship \\
        --group <group> --assembly <assembly> \\
        --nvr <nvr> --jira <ISSUE> \\
        --stage-release-plan <stage-plan> --prod-release-plan <prod-plan> \\
        --create-mr
    """
    pipeline = StandaloneImagesShipPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        nvrs=list(nvrs),
        stage_release_plan=stage_release_plan,
        prod_release_plan=prod_release_plan,
        jiras=list(jiras),
        create_mr=create_mr,
        shipment_data_repo_url=shipment_data_repo_url,
        job_url=job_url,
        silent=silent,
    )

    await pipeline.run()
