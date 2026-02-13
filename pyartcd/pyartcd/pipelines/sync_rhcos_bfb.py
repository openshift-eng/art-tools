import asyncio
import shutil
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp
import click
from artcommonlib import exectools

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

BFB_ALLOWLIST = {"nvidiabfb", "ostree", "oci-manifest"}
GENERAL_RHCOS_ALLOWLIST = {"gcp", "initramfs", "iso", "kernel", "metal", "openstack", "qemu", "vmware", "dasd"}

BFB_S3_URL = "s3://art-srv-enterprise/pub/openshift-v4/aarch64/dependencies/rhcos-nvidiabfb"
GENERAL_RHCOS_S3_URL = ""


class SyncRhcosBfbPipeline:
    """
    Pipeline to sync RHCOS artifacts
    from the RHCOS release browser to mirror.openshift.com
    """

    def __init__(self, runtime: Runtime, stream: str, build: str, sync_type: str = "bfb"):
        self.runtime = runtime
        self.stream = stream
        self.build = build
        self.sync_type = sync_type
        self.major_minor = self.stream.split("-")[0]
        self.working_dir = self.runtime.working_dir
        self.artifacts_dir = self.working_dir / "rhcos-artifacts"
        self.rhcos_base_url = f"https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/storage/prod/streams/{self.stream}/builds/{self.build}/aarch64"

        if sync_type not in ["bfb"]:
            raise ValueError(f"Sync type '{sync_type}' not yet implemented.")

        # TODO: Add --enforce-allowlist as a CLI option after rhcos_sync is migrated
        self.enforce_allowlist = self.sync_type == "bfb"  # `bfb` type syncs only certain images from meta.json

        if self.sync_type == "bfb":
            self.allowlist = BFB_ALLOWLIST
            self.s3_base_url = BFB_S3_URL
        else:
            self.allowlist = GENERAL_RHCOS_ALLOWLIST
            self.s3_base_url = GENERAL_RHCOS_S3_URL

        self.meta_json = None
        self.ocp_version = None
        self.is_prerelease = False

    async def fetch_rhcos_metadata(self):
        """Fetch meta.json from RHCOS releases browser"""
        url = f"{self.rhcos_base_url}/meta.json"

        self.runtime.logger.info(f"Fetching RHCOS metadata from: {url}")

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch metadata from {url}: HTTP {response.status}")

                meta_json = await response.json()
                self.runtime.logger.info("Successfully fetched RHCOS metadata")
                return meta_json

    def discover_artifacts(self) -> List[str]:
        """Discover which artifacts should be synced based on meta.json contents and allowlist"""
        artifacts = []
        images = self.meta_json.get("images", {})

        for image_name, image_info in images.items():
            if self.enforce_allowlist and image_name not in self.allowlist:
                continue

            filename = image_info["path"]
            artifacts.append(filename)
            self.runtime.logger.info(f"Found image '{image_name}': {filename}")

        # Check for artifacts contained in allowlist that are not in meta.json
        if self.enforce_allowlist:
            found_images = {img for img in images.keys() if img in self.allowlist}
            missing_images = self.allowlist - found_images
            if missing_images:
                self.runtime.logger.warning(f"Following images from allowlist not found in meta.json: {missing_images}")

        return artifacts

    async def download_artifact(self, artifact_path: str) -> Path:
        """Download a single artifact from RHCOS release browser"""
        download_url = f"{self.rhcos_base_url}/{artifact_path}"
        local_path = self.artifacts_dir / artifact_path

        self.runtime.logger.info(f"Downloading {download_url}")

        # Configure timeouts - 30 minutes total, 5 minutes per chunk
        timeout = aiohttp.ClientTimeout(total=30 * 60, sock_read=5 * 60)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(download_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download {download_url}: HTTP {response.status}")

                # Log file size
                content_length = response.headers.get('content-length')
                if content_length:
                    size_mb = int(content_length) / (1024 * 1024)
                    self.runtime.logger.info(f"{artifact_path} size: {size_mb:.1f} MB")

                with open(local_path, 'wb') as f:
                    downloaded = 0
                    last_logged_mb = 0
                    async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Log progress every ~200MB
                        downloaded_mb = downloaded / (1024 * 1024)
                        if downloaded_mb - last_logged_mb >= 200:
                            self.runtime.logger.info(f"{artifact_path} downloaded: {downloaded_mb:.1f} MB")
                            last_logged_mb = downloaded_mb

        self.runtime.logger.info(f"Successfully downloaded {artifact_path} to {local_path}")
        return local_path

    async def download_all_artifacts(self, artifacts: List[str]) -> List[Path]:
        """Download all discovered artifacts"""
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

        download_tasks = [self.download_artifact(filename) for filename in artifacts]

        downloaded_paths = await asyncio.gather(*download_tasks)
        self.runtime.logger.info(f"Successfully downloaded all {len(downloaded_paths)} artifacts")
        return downloaded_paths

    def build_mirror_destinations(self) -> Tuple[List[str], List[str]]:
        """
        Build destination paths based on sync_type, OCP version and directory structure.
        Returns (versioned_paths, latest_paths)
        """
        if self.sync_type == "bfb":
            return self.build_bfb_destinations()
        else:
            # TODO Implement support for "general" RHCOS sync
            raise Exception(f"Sync type '{self.sync_type}' not yet implemented. Currently only 'bfb' is supported.")

    def build_bfb_destinations(self) -> Tuple[List[str], List[str]]:
        """Build destination paths for BFB artifacts"""
        versioned_paths = []
        latest_paths = []

        if self.is_prerelease:
            versioned_paths.append(f"{self.s3_base_url}/pre-release/{self.ocp_version}/{self.build}")

            # Latest paths for pre-release
            latest_paths.extend(
                [
                    f"{self.s3_base_url}/pre-release/latest",  # Global pre-release latest
                    f"{self.s3_base_url}/pre-release/latest-{self.major_minor}",  # Y-stream pre-release latest
                ]
            )
        else:
            versioned_paths.append(f"{self.s3_base_url}/{self.major_minor}/{self.ocp_version}/{self.build}")

            # Latest paths for GA'd releases
            latest_paths.extend(
                [
                    f"{self.s3_base_url}/{self.major_minor}/latest",  # Y-stream latest
                    f"{self.s3_base_url}/latest",  # Global latest
                ]
            )

        return versioned_paths, latest_paths

    async def sync_to_destination(self, dest_path: str):
        """Sync all artifacts from self.artifacts_dir to dest_path in s3"""
        artifacts = list(map(lambda path: str(path.name), self.artifacts_dir.iterdir()))
        self.runtime.logger.info(f"Syncing the following artifacts to {dest_path}: {', '.join(artifacts)}")
        await util.mirror_to_s3(
            source=str(self.artifacts_dir),
            dest=dest_path,
            dry_run=self.runtime.dry_run,
            delete=True,
        )
        self.runtime.logger.info(f"Successfully synced the following artifacts to {dest_path}: {', '.join(artifacts)}")

    def add_stable_files_to_artifacts_dir(self):
        """
        Add stable filename copies to artifacts_dir after versioned sync is complete
        (for example, creates copy of rhcos-9.6.20250707-1.4-ostree.aarch64-manifest.json
        named rhcos-ostree.aarch64-manifest.json)
        """

        def get_stable_filename(original_filename: str) -> Optional[str]:
            build_id_pattern = f"-{self.build}-"
            if build_id_pattern in original_filename:
                return original_filename.replace(build_id_pattern, "-")

            self.runtime.logger.warning(
                f"Could not construct stable filename (used in latest dirs) for {original_filename}. Will sync only {original_filename} without its copy with stable filename."
            )
            return None

        original_files = list(
            self.artifacts_dir.iterdir()
        )  # Save the list of files to a variable to avoid iterating over the new files we're adding
        for artifact_file in original_files:
            if artifact_file.is_file():
                stable_name = get_stable_filename(artifact_file.name)
                if stable_name is None:
                    continue

                stable_name_dest = self.artifacts_dir / stable_name
                if stable_name_dest.exists():
                    self.runtime.logger.info(f"Stable filename {stable_name} already exists, skipping copy")
                    continue

                shutil.copy2(artifact_file, stable_name_dest)
                self.runtime.logger.info(f"Added stable filename to artifacts: {artifact_file.name} -> {stable_name}")

    async def should_update_global_latest(self) -> bool:
        """
        Check if we should update global latest directory ("latest" or "/pre-release/latest")
        For pre-release, check existence of latest-{major}.{minor+1}/.
        For GA'd releases, check {major}.{minor+1}/.
        Only update if that directory does not exist.
        """

        # For both stable and pre-release, check if next major.minor exists
        def get_next_major_minor(major_minor: str) -> str:
            major, minor = map(int, major_minor.split("."))
            return f"{major}.{minor + 1}"

        next_major_minor = get_next_major_minor(self.major_minor)

        if self.is_prerelease:
            path_to_check = f"{self.s3_base_url}/pre-release/latest-{next_major_minor}/"
        else:
            path_to_check = f"{self.s3_base_url}/{next_major_minor}/"

        rc, stdout, stderr = await exectools.cmd_gather_async(["aws", "s3", "ls", path_to_check], check=False)

        # If command succeeds and has non-empty output, next version exists
        if rc == 0:
            return not bool(stdout.strip())
        # If command fails and has empty stderr, next version does not exist
        elif not stderr.strip():
            return True
        else:
            raise ChildProcessError(f"AWS S3 ls failed: {stderr}")

    async def sync_artifacts(self):
        """Sync artifacts to all appropriate destinations with proper latest management"""
        versioned_paths, latest_paths = self.build_mirror_destinations()

        # Always sync to versioned destinations (with original filenames)
        versioned_tasks = [self.sync_to_destination(path) for path in versioned_paths]
        await asyncio.gather(*versioned_tasks)

        # After versioned sync is complete, add copies with stable filenames to artifacts_dir
        # which will be synced to `latest` directories
        self.add_stable_files_to_artifacts_dir()

        latest_tasks = []

        for latest_path in latest_paths:
            # Check if this is the global latest (path ends with just "/latest")
            # vs Y-stream latest - path ends with "/{major.minor}/latest" (or just "/latest-{major.minor}" for pre-release)
            is_global_latest = latest_path.endswith("/latest") and not latest_path.endswith(
                f"/{self.major_minor}/latest"
            )

            if is_global_latest:
                if await self.should_update_global_latest():
                    self.runtime.logger.info(
                        f"Newer version does not exist on mirror, updating global latest {latest_path}"
                    )
                    latest_tasks.append(self.sync_to_destination(latest_path))
                else:
                    self.runtime.logger.info(f"Skipping {latest_path} update (newer version exists)")
            else:
                # Y-stream latest (both stable and pre-release) - always update
                latest_tasks.append(self.sync_to_destination(latest_path))

        if latest_tasks:
            await asyncio.gather(*latest_tasks)

    async def run(self):
        self.runtime.logger.info(
            f"Starting RHCOS {self.sync_type.upper()} sync for stream={self.stream}, build={self.build}"
        )

        try:
            self.meta_json = await self.fetch_rhcos_metadata()

            self.ocp_version = self.meta_json.get("coreos-assembler.oci-imported-labels", {}).get("rhcos.version", None)
            if self.ocp_version is None:
                raise Exception(
                    f"Could not retrieve OCP version from `coreos-assembler.oci-imported-labels.rhcos_version` in {self.rhcos_base_url}/meta.json"
                )

            self.is_prerelease = "ec" in self.ocp_version or "rc" in self.ocp_version

            artifacts = self.discover_artifacts()
            self.runtime.logger.info(f"Discovered the following artifacts: {', '.join(artifacts)}")

            await self.download_all_artifacts(artifacts)

            await self.sync_artifacts()

            self.runtime.logger.info(f"RHCOS {self.sync_type.upper()} sync completed successfully")

        except Exception as e:
            self.runtime.logger.error(f"RHCOS {self.sync_type.upper()} sync failed: {e}")
            raise


@cli.command("sync-rhcos-bfb", help="Sync RHCOS artifacts to mirror.openshift.com")
@click.option("--stream", required=True, help="RHCOS stream identifier (e.g., '4.20-9.6-nvidia-bfb')")
@click.option("--build", required=True, help="RHCOS build identifier (e.g., '9.6.20250707-1.3')")
@click.option(
    "--type",
    "sync_type",
    default="bfb",
    help="Type of RHCOS artifacts to sync: 'bfb' for NVIDIA BFB artifacts (default)",
)
@pass_runtime
@click_coroutine
async def sync_rhcos_bfb(runtime: Runtime, stream: str, build: str, sync_type: str):
    pipeline = SyncRhcosBfbPipeline(runtime, stream, build, sync_type)
    await pipeline.run()
