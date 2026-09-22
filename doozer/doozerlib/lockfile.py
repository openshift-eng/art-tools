import hashlib
from dataclasses import dataclass
from logging import Logger
from pathlib import Path
from typing import Dict, Optional

import aiohttp
import yaml
from artcommonlib import logutil

from doozerlib.image import ImageMetadata

DEFAULT_ARTIFACT_LOCKFILE_NAME = "artifacts.lock.yaml"


@dataclass(frozen=True)
class ArtifactInfo:
    """
    Artifact metadata for generic file downloads.

    Attributes:
        url (str): URL where the artifact can be downloaded.
        checksum (str): SHA256 checksum of the artifact.
        filename (str): Filename of the artifact.
    """

    url: str
    checksum: str
    filename: str

    def to_dict(self) -> Dict[str, object]:
        """
        Convert the ArtifactInfo instance to a dictionary for YAML serialization.

        Returns:
            dict: Dictionary containing artifact metadata in Cachi2 format.
        """
        return {
            "download_url": self.url,
            "checksum": self.checksum,
            "filename": self.filename,
        }


class ArtifactLockfileGenerator:
    """
    Handles generation of artifact lockfiles for generic file downloads.

    Creates lockfiles compatible with Cachi2's generic fetcher for hermetic builds.
    """

    def __init__(self, logger: Optional[Logger] = None, runtime=None):
        """
        Initialize the ArtifactLockfileGenerator.

        Args:
            logger (Optional[Logger]): Logger instance for output.
            runtime: Runtime instance for configuration access.
        """
        self.logger = logger or logutil.get_logger(__name__)
        self.runtime = runtime

    def _extract_filename_from_url(self, url: str) -> str:
        """Extract filename from URL for artifact naming."""
        return url.split('/')[-1] or 'artifact'

    def should_generate_artifact_lockfile(self, image_meta: ImageMetadata, dest_dir: Path) -> bool:
        """
        Determine if artifact lockfile generation should proceed.

        Simplified check since artifact downloads are inexpensive compared to RPM resolution.

        Args:
            image_meta (ImageMetadata): Image metadata to check.
            dest_dir (Path): Destination directory for lockfile.

        Returns:
            bool: True if lockfile should be generated.
        """
        return image_meta.is_artifact_lockfile_enabled()

    async def generate_artifact_lockfile(
        self, image_meta: ImageMetadata, dest_dir: Path, filename: str = DEFAULT_ARTIFACT_LOCKFILE_NAME
    ) -> None:
        """
        Generate artifact lockfile for the given image metadata.

        Downloads artifacts, computes checksums, and writes YAML lockfile.

        Args:
            image_meta (ImageMetadata): Image metadata containing artifact requirements.
            dest_dir (Path): Directory to write lockfile to.
            filename (str): Name of lockfile to generate.
        """
        if not self.should_generate_artifact_lockfile(image_meta, dest_dir):
            self.logger.debug(f"Skipping artifact lockfile generation for {image_meta.distgit_key}")
            return

        self.logger.info(f"Generating artifact lockfile for {image_meta.distgit_key}")

        required_artifact_urls = image_meta.get_required_artifacts()
        if not required_artifact_urls:
            self.logger.warning(f"No artifacts defined for {image_meta.distgit_key}")
            return

        artifact_infos = []
        async with aiohttp.ClientSession() as session:
            for artifact_resource in required_artifact_urls:
                artifact_info = await self._download_and_compute_checksum(session, artifact_resource)
                artifact_infos.append(artifact_info)

        lockfile_data = {"metadata": {"version": "1.0"}, "artifacts": [info.to_dict() for info in artifact_infos]}

        output_path = dest_dir / filename
        self._write_yaml(lockfile_data, output_path)
        self.logger.info(f"Generated artifact lockfile: {output_path}")

    async def _download_and_compute_checksum(
        self, session: aiohttp.ClientSession, artifact_resource: dict
    ) -> ArtifactInfo:
        """
        Download artifact and compute its SHA256 checksum.

        Args:
            session (aiohttp.ClientSession): HTTP session for downloads.
            artifact_resource (dict): Resource definition with 'url' and optional 'filename' keys.

        Returns:
            ArtifactInfo: Artifact metadata with checksum.
        """
        url = artifact_resource['url']
        custom_filename = artifact_resource.get('filename')

        self.logger.debug(f"Downloading artifact from {url}")

        async with session.get(url) as response:
            response.raise_for_status()
            content = await response.read()

        checksum = hashlib.sha256(content).hexdigest()

        # Use custom filename if provided, otherwise extract from URL
        if custom_filename:
            filename = custom_filename
            self.logger.info(f"Using custom destination filename: {filename} for {url}")
        else:
            filename = self._extract_filename_from_url(url)

        return ArtifactInfo(url=url, checksum=f"sha256:{checksum}", filename=filename)

    def _write_yaml(self, data: dict, output_path: Path) -> None:
        """Write a Python dictionary to a YAML file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            yaml.safe_dump(data, f, sort_keys=False)
