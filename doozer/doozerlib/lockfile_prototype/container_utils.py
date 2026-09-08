"""
Container image utilities for RPM lockfile generation.

Provides async helpers for interacting with container images via
oc (tag-to-digest resolution, preferring manifest-list digests) and
podman (querying installed packages, reading files from images).
"""

import logging
import os

from artcommonlib import logutil
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.exectools import cmd_gather_async
from artcommonlib.util import oc_image_info_for_arch_async

from doozerlib.constants import BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL
from doozerlib.lockfile_prototype.constants import DEFAULT_PLATFORM, DIGEST_PREFIX, RPM_PSEUDO_PACKAGES
from doozerlib.lockfile_prototype.utils import build_env


class ContainerImageHelper:
    """
    Async utilities for interacting with container images.
    """

    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or logutil.get_logger(__name__)

    @staticmethod
    def _proxy_pullspec(pullspec: str) -> str:
        return pullspec.replace(BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL)

    @staticmethod
    def _repo_from_pullspec(pullspec: str) -> str:
        """
        Return the repository portion of a pullspec, stripping tag and digest.

        Arg(s):
            pullspec (str): Container image pullspec.
        Return Value(s):
            str: Repository (registry/namespace/name) without tag or digest.
        """
        if DIGEST_PREFIX in pullspec:
            pullspec = pullspec.split(DIGEST_PREFIX, 1)[0]
        # Strip tag after the last "/" to avoid stripping port numbers
        last_slash = pullspec.rfind("/")
        if ":" in pullspec[last_slash + 1 :]:
            return pullspec[: last_slash + 1] + pullspec[last_slash + 1 :].rsplit(":", 1)[0]
        return pullspec

    async def resolve_to_digest(self, pullspec: str) -> str:
        """
        Resolve a pullspec to a digest-pinned pullspec.

        Prefers the manifest-list digest (listDigest) so rpm-lockfile-prototype
        --image mode can extract per-arch rpmdbs via skopeo --override-arch
        (ART-22787). Falls back to the platform digest for single-arch images.
        Already-digest pullspecs are re-inspected so a platform digest can be
        upgraded to listDigest when the image is a manifest list.

        Arg(s):
            pullspec (str): Container image pullspec (tag or digest).
        Return Value(s):
            str: Pullspec with digest. Returns the original pullspec if inspect fails.
        """
        inspect_pullspec = self._proxy_pullspec(pullspec)
        registry_config = os.environ.get("QUAY_AUTH_FILE") or os.environ.get("REGISTRY_AUTH_FILE")
        self.logger.debug(f"Resolving to digest (prefer listDigest): {pullspec}")

        try:
            image_data = await oc_image_info_for_arch_async(inspect_pullspec, registry_config=registry_config)
        except Exception as e:
            self.logger.warning(f"Failed to resolve digest for {pullspec}, using original: {e}")
            return pullspec

        digest = image_data.get("listDigest") or image_data.get("digest")
        if not digest:
            self.logger.warning(f"No digest found for {pullspec}, using original")
            return pullspec

        resolved = f"{self._repo_from_pullspec(pullspec)}@{digest}"
        self.logger.debug(f"Resolved to: {resolved}")
        return resolved

    async def get_installed_packages(self, image_pullspec: str, arch: str) -> list[str]:
        """
        Query the list of installed RPM package names from a container image.

        Arg(s):
            image_pullspec (str): Fully-qualified image pullspec (digest preferred).
            arch (str): Brew architecture to query.
        Return Value(s):
            list[str]: Sorted unique package names installed in the image.
        """
        query_pullspec = self._proxy_pullspec(image_pullspec)
        cmd = [
            "podman",
            "run",
            "--rm",
            "--platform",
            f"linux/{go_arch_for_brew_arch(arch)}",
            "--entrypoint",
            "rpm",
            query_pullspec,
            "-qa",
            "--qf",
            r"%{NAME}\n",
        ]
        env = build_env()
        rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=env)
        if rc != 0:
            self.logger.warning(f"Failed to query packages from {image_pullspec}: {stderr[:200]}")
            return []
        return sorted(
            {line.strip() for line in stdout.splitlines() if line.strip() and line.strip() not in RPM_PSEUDO_PACKAGES}
        )

    async def read_file_from_image(self, image_pullspec: str, filepath: str) -> str:
        """
        Read a file from a container image via podman.

        Arg(s):
            image_pullspec (str): Fully-qualified image pullspec.
            filepath (str): Absolute path to file inside the image.
        Return Value(s):
            str: File contents, or empty string on failure.
        """
        query_pullspec = self._proxy_pullspec(image_pullspec)
        cmd = [
            "podman",
            "run",
            "--rm",
            "--platform",
            DEFAULT_PLATFORM,
            "--entrypoint",
            "cat",
            query_pullspec,
            filepath,
        ]
        env = build_env()
        rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=env)
        if rc != 0:
            self.logger.warning(f"Failed to read {filepath} from {query_pullspec}: {stderr[:200]}")
            return ""
        return stdout
