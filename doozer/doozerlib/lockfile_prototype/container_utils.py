"""
Container image utilities for RPM lockfile generation.

Provides async helpers for interacting with container images via
skopeo (tag-to-digest resolution) and podman (querying installed
packages, reading files from images).
"""

import json
import logging

from artcommonlib import logutil
from artcommonlib.exectools import cmd_gather_async

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

    async def resolve_to_digest(self, pullspec: str) -> str:
        """
        If pullspec uses a tag, resolve it to a digest via skopeo inspect.
        Returns the pullspec with @sha256:... instead of :tag.
        If already a digest, returns as-is.

        Arg(s):
            pullspec (str): Container image pullspec.
        Return Value(s):
            str: Pullspec with digest.
        """
        if DIGEST_PREFIX in pullspec:
            return pullspec

        env = build_env()
        cmd = ["skopeo", "inspect", "--no-tags", f"docker://{pullspec}"]
        self.logger.debug(f"Resolving tag to digest: {pullspec}")

        rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=env)

        if rc != 0:
            self.logger.warning(f"Failed to resolve digest for {pullspec}, using tag: {stderr[:200]}")
            return pullspec

        digest = json.loads(stdout).get("Digest", "")
        if not digest:
            self.logger.warning(f"No digest found for {pullspec}, using tag")
            return pullspec

        # Strip tag after the last "/" to avoid stripping port numbers
        last_slash = pullspec.rfind("/")
        if ":" in pullspec[last_slash + 1 :]:
            repo = pullspec[: last_slash + 1] + pullspec[last_slash + 1 :].rsplit(":", 1)[0]
        else:
            repo = pullspec
        resolved = f"{repo}@{digest}"
        self.logger.debug(f"Resolved to: {resolved}")
        return resolved

    async def get_installed_packages(self, image_pullspec: str) -> list[str]:
        """
        Query the list of installed RPM package names from a container image.

        Arg(s):
            image_pullspec (str): Fully-qualified image pullspec (digest preferred).
        Return Value(s):
            list[str]: Sorted unique package names installed in the image.
        """
        query_pullspec = self._proxy_pullspec(image_pullspec)
        cmd = [
            "podman",
            "run",
            "--rm",
            "--platform",
            DEFAULT_PLATFORM,
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
