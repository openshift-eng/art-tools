import asyncio
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import click
import yaml as stdlib_yaml
from artcommonlib import exectools
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.gitlab import GitLabClient
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
from pyartcd.click_validators import validate_release_date
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

yaml = new_roundtrip_yaml_handler()


class BinaryReleaseKonfluxPipeline:
    """
    Pipeline for shipping standalone CDN binary products (e.g. oc-mirror) built via Konflux.

    Unlike PrepareReleaseKonfluxPipeline (OCP payload) or ReleaseFromFbcPipeline (layered
    products/FBC), this pipeline is for products released directly to the Customer Portal
    Content Gateway via the `push-artifacts-to-cdn` Konflux pipeline. It takes a set of
    already-built NVR(s), resolves them to a Konflux snapshot, and opens a shipment MR
    referencing the product's CDN stage/prod ReleasePlans (defined in konflux-release-data
    and ocp-shipment-data's config.yaml).

    No release notes/advisory data is generated here: CDN ReleasePlanAdmissions carry their
    own static release notes (product_id, synopsis, etc.) in konflux-release-data.
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
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.nvrs = nvrs
        self.create_mr = create_mr
        self.dry_run = self.runtime.dry_run
        self.target_release_date = target_release_date

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

    @staticmethod
    def basic_auth_url(url: str, token: str) -> str:
        """
        Create a basic auth URL with the given token.
        """
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme or "https"
        netloc = parsed_url.netloc
        path = parsed_url.path
        params = parsed_url.params
        query = parsed_url.query
        fragment = parsed_url.fragment

        url_parts = [path]
        if params:
            url_parts.append(f";{params}")
        if query:
            url_parts.append(f"?{query}")
        if fragment:
            url_parts.append(f"#{fragment}")

        rest_of_url = "".join(url_parts)
        return f'{scheme}://oauth2:{token}@{netloc}{rest_of_url}'

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
        if not self.create_mr:
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
        if self.create_mr and self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)

    async def setup_shipment_repo(self):
        """
        Setup shipment data repository for MR creation.
        """
        if not self.create_mr:
            return

        await self.shipment_data_repo.setup(
            remote_url=self.basic_auth_url(self.shipment_data_repo_push_url, self.gitlab_token),
            upstream_remote_url=self.shipment_data_repo_pull_url,
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

    def create_shipment_config(self, snapshot: Snapshot) -> ShipmentConfig:
        """
        Create a shipment configuration (kind "image") for the given snapshot, reading
        the stage/prod releasePlan names for the snapshot's application from config.yaml
        in the shipment data repo.
        """
        if self.product is None:
            raise RuntimeError(
                "Product is not initialized. Please call run() first to load the product from group "
                "configuration, or ensure self.product is set before calling create_shipment_config()."
            )

        application = snapshot.spec.application
        self.logger.info(f"Creating shipment config for application '{application}'...")

        metadata = Metadata(
            product=self.product,
            application=application,
            group=self.group,
            assembly=self.assembly,
            fbc=False,
        )

        stage_rpa = "n/a"
        prod_rpa = "n/a"
        config_path = self.shipment_data_repo._directory / "config.yaml"
        if config_path.exists():
            with open(config_path, 'r') as f:
                shipment_config = stdlib_yaml.safe_load(f) or {}
            app_env_config = shipment_config.get("applications", {}).get(application, {}).get("environments", {})
            stage_rpa = app_env_config.get("stage", {}).get("releasePlan", "n/a")
            prod_rpa = app_env_config.get("prod", {}).get("releasePlan", "n/a")

        if stage_rpa == "n/a" or prod_rpa == "n/a":
            if self.create_mr:
                raise ValueError(
                    f"stage/prod releasePlan is not registered for application '{application}' in {config_path}. "
                    "Cannot create a shipment MR with unresolved ReleasePlans."
                )
            self.logger.warning(
                f"Could not resolve stage/prod releasePlan for application '{application}' from config.yaml. "
                "Please ensure it is registered there before merging the resulting MR."
            )

        environments = Environments(stage=ShipmentEnv(releasePlan=stage_rpa), prod=ShipmentEnv(releasePlan=prod_rpa))

        # CDN ReleasePlanAdmissions carry their own static release notes (product_id, synopsis,
        # etc. in konflux-release-data), so no data.releaseNotes is generated here.
        shipment = Shipment(metadata=metadata, environments=environments, snapshot=snapshot, data=None)

        return ShipmentConfig(shipment=shipment)

    async def create_shipment_mr(self, shipment_config: ShipmentConfig) -> str:
        """
        Create a new shipment MR with the given shipment config file.
        """
        if not self.create_mr:
            return ""

        self.logger.info("Creating shipment MR...")

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"binary-release-{self.assembly}-{timestamp}"
        target_branch = "main"

        await self.shipment_data_repo.create_branch(source_branch)

        commit_message = f"Add shipment configuration for {self.product} {self.assembly}"
        updated = await self.update_shipment_data(shipment_config, "prod", commit_message, timestamp)
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

        self.shipment_mr_url = mr_url
        return mr_url

    async def set_shipment_mr_ready(self):
        """
        Mark the shipment MR as ready by removing the Draft prefix from the title.
        """
        mr = await self._gitlab.set_mr_ready(self.shipment_mr_url)

        if mr and not self.dry_run:
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
        self, shipment_config: ShipmentConfig, env: str, commit_message: str, timestamp: str
    ) -> bool:
        """
        Update shipment data repo with the given shipment config file.
        """
        if not self.create_mr:
            return False

        filepath = await self._write_shipment_file(shipment_config, env, timestamp)
        self.logger.info("Updating shipment file: %s", filepath)

        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()
        return await self.shipment_data_repo.commit_push(commit_message, safe=True)

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

    async def write_shipment_file_locally(self, shipment_config: ShipmentConfig, env: str, timestamp: str):
        """
        Write the shipment file to the local repository without creating an MR.
        """
        filepath = await self._write_shipment_file(shipment_config, env, timestamp)
        self.logger.info(f"Created shipment file: {filepath}")

    async def run(self) -> None:
        """
        Execute the binary release workflow: resolve NVR(s) to a Konflux snapshot, build a
        shipment config referencing the product's CDN releasePlans, and optionally open a
        shipment MR.
        """
        self.logger.info(f"Starting binary release workflow for {self.group} {self.assembly}")
        self.logger.info(f"Processing {len(self.nvrs)} NVR(s): {self.nvrs}")

        self.check_env_vars()
        self.setup_working_dir()
        if self.create_mr:
            await self.setup_shipment_repo()

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

        snapshot = await self.create_snapshot(self.nvrs)
        if not snapshot:
            raise RuntimeError("No snapshot could be created from the provided NVR(s)")

        shipment_config = self.create_shipment_config(snapshot)

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        if not self.create_mr:
            await self.write_shipment_file_locally(shipment_config, "prod", timestamp)
        else:
            mr_url = await self.create_shipment_mr(shipment_config)
            if mr_url:
                self.logger.info(f"Created shipment MR: {mr_url}")
                await self.set_shipment_mr_ready()

        completion_msg = f"Binary release workflow completed for {self.product} {self.assembly}."
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
):
    """
    Create a shipment for a standalone CDN binary product built via Konflux.

    This command resolves the given build NVR(s) into a Konflux snapshot and creates a
    shipment file referencing the product's CDN stage/prod ReleasePlans (as configured in
    ocp-shipment-data's config.yaml). Unlike release-from-fbc or prepare-release-konflux,
    no advisory/release-notes data is generated - the CDN ReleasePlanAdmission already
    carries static release notes.

    \b
    # oc-mirror 2.0 binary release:
    $ artcd binary-release-konflux \\
        --group oc-mirror-2.0 \\
        --assembly stream \\
        --nvrs oc-mirror-container-2.0-202607291654.p2.g90b54b1.assembly.stream.el9 \\
        --create-mr
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
    )

    await pipeline.run()
