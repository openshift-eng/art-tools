import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import click
import yaml as stdlib_yaml
from artcommonlib import exectools
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
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
from elliottlib.util import extract_nvrs_from_fbc

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

yaml = new_roundtrip_yaml_handler()


class ReleaseFromFbcPipeline:
    """
    Pipeline for creating simplified shipment files from FBC images.

    This pipeline is designed for non-OpenShift products (e.g., OADP, MTA, MTC, logging)
    that need to create shipment files from FBC images. For OpenShift releases,
    use the PrepareReleaseKonfluxPipeline instead.
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        fbc_pullspecs: List[str],
        create_mr: bool = False,
        shipment_data_repo_url: Optional[str] = None,
        shipment_path: Optional[str] = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.fbc_pullspecs = fbc_pullspecs
        self.create_mr = create_mr
        self.dry_run = self.runtime.dry_run

        # Setup working directories
        self.working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self._shipment_data_repo_dir = self.working_dir / "shipment-data-push"

        # Shipment repository configuration
        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.gitlab_token = None
        self.shipment_mr_url = None

        # Product configuration - initialized to None, will be loaded from group config in run()
        self.product = None

        # Set default shipment_path if not provided, using same logic as elliott
        self.shipment_path_was_defaulted = not shipment_path
        if not shipment_path:
            # For non-OpenShift products, use default shipment data repository
            shipment_path = SHIPMENT_DATA_URL_TEMPLATE

        # Setup shipment repo configuration
        self.shipment_data_repo_pull_url, self.shipment_data_repo_push_url = self._shipment_data_repo_vars(
            shipment_data_repo_url
        )
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

    def _shipment_data_repo_vars(self, shipment_data_repo_url: Optional[str]):
        """
        Determine shipment data repository URLs for pull and push operations.
        """
        shipment_data_repo_pull_url = (
            shipment_data_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        shipment_data_repo_push_url = (
            shipment_data_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_push_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        return shipment_data_repo_pull_url, shipment_data_repo_push_url

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

        # Construct the URL parts
        url_parts = [path]
        if params:
            url_parts.append(f";{params}")
        if query:
            url_parts.append(f"?{query}")
        if fragment:
            url_parts.append(f"#{fragment}")

        rest_of_url = "".join(url_parts)
        # Use oauth2 as placeholder username and token as password
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

        # Setup shipment-data repo with GitLab authentication
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
            # Use doozer command to read group config product field
            doozer_cmd = ['doozer', f'--group={self.group}', 'config:read-group', 'product']

            _, product_output, _ = await exectools.cmd_gather_async(doozer_cmd)
            # Clean up the output - remove all whitespace (including newlines)
            product = product_output.strip()

            if product and product != 'None' and product != 'null':
                self.logger.info(f"Loaded product from group config: {product}")
                return product
            else:
                self.logger.debug("No product field found in group config, falling back to group name extraction")

        except Exception as e:
            self.logger.warning(f"Failed to load product from group config: {e}")

        # Fallback: extract product from group name (e.g., "oadp-1.3" -> "oadp")
        product = self.group.split('-')[0]
        self.logger.info(f"Using product extracted from group name: {product}")
        return product

    def extract_fbc_nvr(self, fbc_pullspec: str) -> Optional[str]:
        """
        Extract the NVR from the FBC image itself using oc image info and com.redhat.art.nvr label.
        """
        self.logger.info(f"Extracting FBC NVR from FBC pullspec: {fbc_pullspec}")

        try:
            oc_cmd = ['oc', 'image', 'info', fbc_pullspec, '--filter-by-os', 'amd64', '-o', 'json']

            # Add registry config for authentication if available
            konflux_art_images_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
            if konflux_art_images_auth_file:
                oc_cmd.extend(['--registry-config', konflux_art_images_auth_file])

            image_info_output, _ = exectools.cmd_assert(oc_cmd)
            image_info = json.loads(image_info_output)

            # Extract labels
            labels = image_info.get('config', {}).get('config', {}).get('Labels', {})
            nvr = labels.get('com.redhat.art.nvr')

            if nvr:
                self.logger.info(f"✓ Extracted FBC NVR: {nvr}")
                return nvr
            else:
                self.logger.warning(f"✗ Missing com.redhat.art.nvr label for FBC image {fbc_pullspec}")
                return None

        except Exception as e:
            self.logger.exception(f"✗ Failed to get image info for FBC {fbc_pullspec}: {e}")
            return None

    def categorize_nvrs(self, nvrs: List[str]) -> Dict[str, List[str]]:
        """
        Categorize NVRs into image builds and FBC builds.
        """
        self.logger.info(f"Categorizing {len(nvrs)} extracted NVRs...")

        categorized = {"image": [], "fbc": []}

        for nvr in nvrs:
            nvr_dict = parse_nvr(nvr)
            component_name = nvr_dict['name']

            if component_name.endswith('-fbc'):
                categorized["fbc"].append(nvr)
            else:
                # All other builds are considered image builds for simplicity
                categorized["image"].append(nvr)

        self.logger.info("Categorization results:")
        self.logger.info(f"  • Image builds: {len(categorized['image'])}")
        self.logger.info(f"  • FBC builds: {len(categorized['fbc'])}")

        return categorized

    async def create_snapshot(self, builds: List[str]) -> Optional[Snapshot]:
        """
        Create a snapshot from a list of build NVRs using elliott.
        """
        if not builds:
            self.logger.debug("No builds provided, skipping snapshot creation")
            return None

        self.logger.info(f"Creating Konflux snapshot for {len(builds)} builds...")

        # store builds in a temporary file, each nvr string in a new line
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
            for nvr in builds:
                temp_file.write(nvr + '\n')
            temp_file.flush()
            temp_file_path = temp_file.name

        # now call elliott snapshot new -f <temp_file_path>
        snapshot_cmd = self._elliott_base_command + [
            "snapshot",
            "new",
            f"--builds-file={temp_file_path}",
        ]

        konflux_art_images_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
        if konflux_art_images_auth_file:
            snapshot_cmd.append(f"--pull-secret={konflux_art_images_auth_file}")

        try:
            self.logger.info(f"Running elliott snapshot command: {' '.join(snapshot_cmd)}")
            stdout, _ = exectools.cmd_assert(snapshot_cmd)
        except Exception as e:
            self.logger.exception(f"Failed to create snapshot: {e}")
            raise
        finally:
            # remove the temporary file
            os.unlink(temp_file_path)

        # parse the output of the snapshot new command, it should be valid yaml
        try:
            new_snapshot_obj = yaml.load(stdout)
            self.logger.info("✓ Successfully created Konflux snapshot")
            return Snapshot(spec=SnapshotSpec(**new_snapshot_obj.get("spec")), nvrs=sorted(builds))
        except Exception as e:
            self.logger.exception(f"Failed to parse elliott snapshot output: {e}")
            self.logger.debug(f"Raw output was: {stdout}")
            raise

    def create_shipment_config(self, kind: str, snapshot: Optional[Snapshot]) -> ShipmentConfig:
        """
        Create a shipment configuration for the given kind and snapshot.
        """
        # Guard: ensure product is initialized
        if self.product is None:
            raise RuntimeError(
                "Product is not initialized. Please call run() first to load the product from group configuration, "
                "or ensure self.product is set before calling create_shipment_config()."
            )

        self.logger.info(f"Creating shipment config for {kind}...")

        # Create metadata - only set fbc=True for actual FBC catalog files
        metadata = Metadata(
            product=self.product,  # Get product from group configuration
            application=snapshot.spec.application if snapshot else "default-app",
            group=self.group,
            assembly=self.assembly,
            fbc=kind == 'fbc',  # Only True for FBC catalog files, False for image shipments
        )

        # Create environments - read from shipment repo config if available
        stage_rpa = "n/a"
        prod_rpa = "n/a"

        if hasattr(self, 'shipment_data_repo') and self.shipment_data_repo:
            try:
                config_path = self.shipment_data_repo._directory / "config.yaml"
                if config_path.exists():
                    with open(config_path, 'r') as f:
                        # Use yaml.safe_load from stdlib for reading config
                        shipment_config = stdlib_yaml.safe_load(f) or {}

                    application = snapshot.spec.application if snapshot else "default-app"
                    app_env_config = (
                        shipment_config.get("applications", {}).get(application, {}).get("environments", {})
                    )
                    stage_rpa = app_env_config.get("stage", {}).get("releasePlan", "n/a")
                    prod_rpa = app_env_config.get("prod", {}).get("releasePlan", "n/a")
            except Exception as e:
                self.logger.warning(f"Failed to read shipment config, using defaults: {e}")

        environments = Environments(stage=ShipmentEnv(releasePlan=stage_rpa), prod=ShipmentEnv(releasePlan=prod_rpa))

        # Create release notes data - set to null for release-from-fbc pipeline
        data = None

        # Create shipment
        shipment = Shipment(metadata=metadata, environments=environments, snapshot=snapshot, data=data)

        return ShipmentConfig(shipment=shipment)

    async def create_shipment_mr(self, shipments_by_kind: Dict[str, ShipmentConfig], env: str = "prod") -> str:
        """
        Create a new shipment MR with the given shipment config files.
        """
        if not self.create_mr:
            return ""

        self.logger.info("Creating shipment MR...")

        # Create branch name
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"prepare-shipment-{self.assembly}-{timestamp}"
        target_branch = "main"

        # Create and checkout branch
        await self.shipment_data_repo.create_branch(source_branch)

        # Update shipment data repo with shipment configs
        commit_message = f"Add shipment configurations for {self.product} {self.assembly}"
        updated = await self.update_shipment_data(shipments_by_kind, env, commit_message, source_branch)
        if not updated:
            raise ValueError("Failed to update shipment data repo. Please investigate.")

        source_project = self._get_gitlab_project(self.shipment_data_repo_push_url)
        target_project = self._get_gitlab_project(self.shipment_data_repo_pull_url)

        # Create MR title and description
        mr_title = f"Draft: Shipment for {self.product} {self.assembly}"
        mr_description = f"Shipment files created for {self.assembly} using release-from-fbc command"

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

        # Store the MR URL for later use
        self.shipment_mr_url = mr_url
        return mr_url

    async def update_shipment_data(
        self, shipments_by_kind: Dict[str, ShipmentConfig], env: str, commit_message: str, branch: str
    ) -> bool:
        """
        Update shipment data repo with the given shipment config files.
        """
        if not self.create_mr:
            return False

        # Get the timestamp from the branch name
        timestamp = branch.split("-")[-1]

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
        Common logic for writing a single shipment file.
        Returns the filepath where the file was written.
        """
        # Use improved naming: for FBC shipments use counter format, for image use simple format
        if advisory_kind.startswith('fbc'):
            # Extract counter from advisory_kind (e.g., 'fbc01' -> '01')
            counter = advisory_kind[3:]  # Remove 'fbc' prefix
            filename = f"{self.assembly}.fbc.{timestamp}{counter}.yaml"
        else:
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
        Write shipment files to the local repository without creating MR.
        """
        self.logger.info(f"Writing {len(shipments_by_kind)} shipment files to local repository...")

        for advisory_kind, shipment_config in shipments_by_kind.items():
            filepath = await self._write_shipment_file(advisory_kind, shipment_config, env, timestamp)
            self.logger.info(f"Created shipment file: {filepath}")

    async def validate_fbc_related_images(self, fbc_pullspecs: List[str]) -> List[str]:
        """
        Validate that all FBC pullspecs have the same related images.
        Returns the common related images if validation passes.
        """
        self.logger.info(f"Validating that all {len(fbc_pullspecs)} FBC builds have the same related images...")

        # Extract related images from each FBC sequentially to avoid temp directory conflicts
        fbc_related_images = {}
        for fbc_pullspec in fbc_pullspecs:
            related_nvrs = await extract_nvrs_from_fbc(fbc_pullspec, self.product)
            fbc_related_images[fbc_pullspec] = sorted(related_nvrs)

        # Compare related images across all FBCs
        reference_fbc = fbc_pullspecs[0]
        reference_images = fbc_related_images[reference_fbc]

        mismatches = []
        for fbc_pullspec in fbc_pullspecs[1:]:
            current_images = fbc_related_images[fbc_pullspec]
            if current_images != reference_images:
                # Find specific differences
                only_in_reference = set(reference_images) - set(current_images)
                only_in_current = set(current_images) - set(reference_images)
                mismatches.append(
                    {
                        'fbc': fbc_pullspec,
                        'only_in_reference': sorted(only_in_reference),
                        'only_in_current': sorted(only_in_current),
                    }
                )

        if mismatches:
            error_msg = f"FBC builds do not have matching related images.\nReference FBC: {reference_fbc}\n"
            for mismatch in mismatches:
                error_msg += f"\nMismatch with: {mismatch['fbc']}\n"
                if mismatch['only_in_reference']:
                    error_msg += f"  Only in reference: {mismatch['only_in_reference']}\n"
                if mismatch['only_in_current']:
                    error_msg += f"  Only in current: {mismatch['only_in_current']}\n"

            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

        self.logger.info(f"✓ All FBC builds have matching related images ({len(reference_images)} images)")
        return reference_images

    async def run(self) -> None:
        """
        Execute the simplified FBC release workflow for non-OpenShift products.
        """
        self.logger.info(f"Starting FBC-based release workflow for {self.assembly}")
        self.logger.info(f"Processing {len(self.fbc_pullspecs)} FBC pullspecs")

        # Initialize environment and repositories
        self.check_env_vars()
        self.setup_working_dir()
        if self.create_mr:
            await self.setup_shipment_repo()

        # Load product from group configuration
        self.product = await self._load_product_from_group_config()
        self.logger.info(f"Loaded product '{self.product}' - continuing workflow for {self.product} {self.assembly}")

        # Note: All products use the same shipment data repository
        # No need to update shipment repository based on product

        # Validate that all FBC builds have the same related images
        related_nvrs = await self.validate_fbc_related_images(self.fbc_pullspecs)

        # Extract FBC NVRs from each FBC pullspec
        fbc_nvrs = [
            nvr for fbc_pullspec in self.fbc_pullspecs if (nvr := self.extract_fbc_nvr(fbc_pullspec)) is not None
        ]

        if fbc_nvrs:
            self.logger.info(f"✓ Extracted {len(fbc_nvrs)} FBC NVRs: {fbc_nvrs}")
        else:
            self.logger.warning("Could not extract any FBC NVRs - FBC shipments will not be created")

        # Combine related images with FBC NVRs
        all_nvrs = related_nvrs[:] + fbc_nvrs

        if not all_nvrs:
            raise RuntimeError("No NVRs extracted from FBC images")

        # Categorize the extracted NVRs
        categorized_nvrs = self.categorize_nvrs(all_nvrs)

        # Create snapshots for image builds (only once since they're shared)
        image_snapshot = None
        if categorized_nvrs.get('image'):
            image_snapshot = await self.create_snapshot(categorized_nvrs['image'])

        # Create separate FBC snapshots for each FBC build
        fbc_snapshots = {}
        for fbc_nvr in fbc_nvrs:
            fbc_snapshots[fbc_nvr] = await self.create_snapshot([fbc_nvr])

        # Create shipment configurations
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        shipments_by_kind = {}

        # Create image shipment (shared for all FBC builds)
        if image_snapshot:
            shipment_config = self.create_shipment_config('image', image_snapshot)
            shipments_by_kind['image'] = shipment_config

        # Create separate FBC shipment for each FBC build
        fbc_counter = 1
        for _, fbc_snapshot in fbc_snapshots.items():
            if fbc_snapshot:
                shipment_config = self.create_shipment_config('fbc', fbc_snapshot)
                # Use counter-based key for unique naming
                shipment_kind = f"fbc{fbc_counter:02d}"
                shipments_by_kind[shipment_kind] = shipment_config
                fbc_counter += 1

        # Write shipment files to local repository
        if shipments_by_kind:
            await self.write_shipment_files_locally(shipments_by_kind, "prod", timestamp)

        # Create MR if requested
        if self.create_mr and shipments_by_kind:
            try:
                mr_url = await self.create_shipment_mr(shipments_by_kind, env="prod")
                if mr_url:
                    self.logger.info(f"Created shipment MR: {mr_url}")
            except Exception as e:
                self.logger.exception(f"Failed to create MR: {e}")
                if not self.dry_run:
                    self.logger.info("Continuing with local files only")

        # Generate completion message
        completion_msg = f"FBC-based release workflow completed for {self.product} {self.assembly}. Created {len(shipments_by_kind)} shipment files in repository: {self.shipment_data_repo._directory}"

        if self.shipment_mr_url:
            completion_msg += f". MR: {self.shipment_mr_url}"
        self.logger.info(completion_msg)


@cli.command("release-from-fbc")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group to operate on e.g. oadp-1.5, mta-7.0, mtc-1.8, logging-5.9 (NOTE: This command is intended for non-OpenShift products)",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly to operate on e.g. 1.5.3, 4.18.x",
)
@click.option(
    "--fbc-pullspecs",
    metavar="FBC_PULLSPECS",
    required=True,
    help="Comma-separated list of FBC image pullspecs to extract NVRs from",
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
    '--shipment-path',
    help='Path to shipment data repository for elliott commands. If not provided, defaults to the OCP shipment data repository URL.',
)
@pass_runtime
@click_coroutine
async def release_from_fbc(
    runtime: Runtime,
    group: str,
    assembly: str,
    fbc_pullspecs: str,
    create_mr: bool,
    shipment_data_repo_url: Optional[str],
    shipment_path: Optional[str],
):
    """
    Create shipment files from an FBC image for non-OpenShift products.
    
    This command is designed for products other than OpenShift (e.g., OADP, MTA, MTC, logging).
    It extracts NVRs from an FBC image and creates separate shipment files for 
    image builds and FBC builds.
    
    Note: For OpenShift releases, use the prepare-release-konflux command instead.
    
    \b
    # Create shipment files using default OCP shipment repository:
    $ artcd release-from-fbc \\
        --group oadp-1.5 \\
        --assembly 1.5.3 \\
        --fbc-pullspecs quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:oadp-operator-fbc-1.5.3-20251028153444
    
    \b
    # Create shipment files with custom shipment repository:
    $ artcd release-from-fbc \\
        --group oadp-1.5 \\
        --assembly 1.5.3 \\
        --fbc-pullspecs quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:oadp-operator-fbc-1.5.3-20251028153444 \\
        --shipment-path /path/to/custom-shipment-data
    
    \b
    # Create shipment files and MR (requires GITLAB_TOKEN env var):
    $ artcd release-from-fbc \\
        --group oadp-1.5 \\
        --assembly 1.5.3 \\
        --fbc-pullspecs quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:oadp-operator-fbc-1.5.3-20251028153444 \\
        --create-mr
    """
    # Parse comma-separated FBC pullspecs
    fbc_pullspecs_list = [spec.strip() for spec in fbc_pullspecs.split(',') if spec.strip()]
    if not fbc_pullspecs_list:
        raise click.ClickException("At least one FBC pullspec must be provided")

    # Create pipeline and run
    pipeline = ReleaseFromFbcPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        fbc_pullspecs=fbc_pullspecs_list,
        create_mr=create_mr,
        shipment_data_repo_url=shipment_data_repo_url,
        shipment_path=shipment_path,
    )

    await pipeline.run()
