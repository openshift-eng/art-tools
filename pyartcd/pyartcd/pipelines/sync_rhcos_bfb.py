import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict

import aiohttp
import click

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime


class SyncRhcosBfbPipeline:
    """
    Pipeline to sync RHCOS NVIDIA BFB artifacts
    from the RHCOS release browser to mirror.openshift.com
    """

    def __init__(self, runtime: Runtime, stream: str, build: str):
        self.runtime = runtime
        self.stream = stream
        self.build = build
        self.major_minor = self.stream.split("-")[0]
        self.working_dir = self.runtime.working_dir
        self.rhcos_base_url = f"https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/storage/prod/streams/{self.stream}/builds/{self.build}/aarch64"

    async def fetch_rhcos_metadata(self) -> Dict[str, Any]:
        """Fetch meta.json JSON from RHCOS releases browser"""
        url = f"{self.rhcos_base_url}/meta.json"

        self.runtime.logger.info(f"Fetching RHCOS metadata from: {url}")

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch metadata from {url}: HTTP {response.status}")

                meta_json = await response.json()
                self.runtime.logger.info("Successfully fetched RHCOS metadata")
                return meta_json

    async def download_bfb_artifact(self, bfb_path: str) -> Path:
        """Download BFB artifact from RHCOS release browser to local working directory"""
        download_url = f"{self.rhcos_base_url}/{bfb_path}"

        filename = Path(bfb_path).name
        local_path = self.working_dir / filename

        self.runtime.logger.info(f"Downloading BFB artifact from: {download_url}")
        self.runtime.logger.info(f"Saving to: {local_path}")

        # Configure timeouts - 30 minutes total, 5 minutes per chunk
        timeout = aiohttp.ClientTimeout(total=30 * 60, sock_read=5 * 60)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(download_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download BFB artifact from {download_url}: HTTP {response.status}")

                # Log file size
                content_length = response.headers.get('content-length')
                if content_length:
                    size_mb = int(content_length) / (1024 * 1024)
                    self.runtime.logger.info(f"BFB file size: {size_mb:.1f} MB")

                with open(local_path, 'wb') as f:
                    downloaded = 0
                    last_logged_mb = 0
                    async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Log progress every ~200MB
                        downloaded_mb = downloaded / (1024 * 1024)
                        if downloaded_mb - last_logged_mb >= 200:
                            self.runtime.logger.info(f"Downloaded: {downloaded_mb:.1f} MB")
                            last_logged_mb = downloaded_mb

        self.runtime.logger.info(f"Successfully downloaded BFB artifact: {local_path}")
        return local_path

    async def sync_bfb_artifact(self, local_bfb_path: Path):
        """Sync downloaded .bfb artifact to mirror.openshift.com"""
        mirror_dest_path_base = "s3://art-srv-enterprise/pub/openshift-v4/aarch64/dependencies/nvidia-bfb/pre-release"
        mirror_dest_paths = [
            f"{mirror_dest_path_base}/{bfb_tag}"
            for bfb_tag in [f"{self.major_minor}-latest", f"{self.major_minor}-{self.build}"]
        ]

        async def sync_to_destination(dest_path: str):
            self.runtime.logger.info(f"Syncing {local_bfb_path} to {dest_path}")

            await util.mirror_to_s3(
                source=str(self.working_dir),
                dest=dest_path,
                exclude="*",
                include=local_bfb_path.name,
                dry_run=self.runtime.dry_run,
            )

            self.runtime.logger.info(f"Successfully synced {local_bfb_path} to {dest_path}")

        tasks = [sync_to_destination(dest_path) for dest_path in mirror_dest_paths]
        await asyncio.gather(*tasks)

    async def run(self):
        self.runtime.logger.info(f"Starting RHCOS BFB sync for stream={self.stream}, build={self.build}")

        try:
            meta_json = await self.fetch_rhcos_metadata()

            bfb_path = meta_json.get("images", {}).get("nvidiabfb", {}).get("path")
            if bfb_path is None:
                raise Exception("BFB path not found in meta.json")

            local_bfb_path = await self.download_bfb_artifact(bfb_path)
            await self.sync_bfb_artifact(local_bfb_path)

            self.runtime.logger.info("RHCOS BFB sync completed successfully")

        except Exception as e:
            self.runtime.logger.error(f"RHCOS BFB sync failed: {e}")
            raise


@cli.command("sync-rhcos-bfb", help="Sync RHCOS NVIDIA BFB artifacts to mirror.openshift.com")
@click.option("--stream", required=True, help="RHCOS stream identifier (e.g., '4.20-9.6-nvidia-bfb')")
@click.option("--build", required=True, help="RHCOS build identifier (e.g., '9.6.20250707-1.3')")
@pass_runtime
@click_coroutine
async def sync_rhcos_bfb(runtime: Runtime, stream: str, build: str):
    pipeline = SyncRhcosBfbPipeline(runtime, stream, build)
    await pipeline.run()
