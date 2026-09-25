"""
Combined pipeline for RHCOS boot image sync and container signing.

Migrates the preamble orchestration logic from the rhcos_sync Jenkinsfile
in aos-cd-jobs into pyartcd, so that the Jenkinsfile can be reduced to a
single ``artcd rhcos-sync`` call (plus credential setup).

The preamble logic ported here includes:
  1. RELEASE_TAG parsing and pullspec resolution
  2. Architecture / priv extraction from the release tag
  3. RHCOS metadata extraction via ``oc`` commands
  4. Mirror prefix computation (stable vs pre-release)
  5. Synclist generation from RHCOS meta.json
  6. "needsHappening" check (skip if already on mirror)
  7. Orchestration of existing SyncRhcosPipeline and SignRhcosContainersPipeline

Reference Jenkinsfile:
  aos-cd-jobs/jobs/build/rhcos_sync/Jenkinsfile
Reference Groovy lib:
  aos-cd-jobs/jobs/build/rhcos_sync/rhcoslib.groovy
"""

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import click
import yaml
from artcommonlib.arch_util import BREW_ARCHES, GO_ARCHES, brew_arch_for_go_arch, go_suffix_for_arch
from artcommonlib.constants import REGISTRY_CI_OPENSHIFT, REGISTRY_QUAY_OCP_RELEASE_DEV
from artcommonlib.registry_config import RegistryConfig

from pyartcd import oc
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.pipelines.sign_rhcos_containers import SignRhcosContainersPipeline
from pyartcd.pipelines.sync_rhcos import SyncRhcosPipeline
from pyartcd.runtime import Runtime

# RHCOS artifacts that are synced (from rhcoslib.groovy).
# The allowlist is not enforced by default; all images in meta.json are synced.
RHCOS_ALLOWLIST = ["gcp", "initramfs", "iso", "kernel", "metal", "openstack", "qemu", "vmware", "dasd"]
ENFORCE_ALLOWLIST = False


def parse_release_tag(tag: str) -> Tuple[str, bool]:
    """Extract architecture and priv flag from a release tag.

    Ported from release.groovy getReleaseTagArchPriv().

    Examples:
        "4.19.0-ec.0-x86_64"                          → ("x86_64", False)
        "4.12.0-0.nightly-arm64-2024-01-01-010101"     → ("aarch64", False)
        "4.12.0-0.nightly-priv-s390x-2024-01-01-0101"  → ("s390x", True)
        "4.19.0-ec.0"                                  → ("x86_64", False)

    Args:
        tag: Release tag string (without any pullspec prefix).

    Returns:
        Tuple of (brew_arch, is_priv).
    """
    components = tag.split("-")
    priv = "priv" in components

    arch = "x86_64"  # default when no arch component found
    all_known = set(GO_ARCHES + BREW_ARCHES)
    for component in components:
        if component in all_known:
            arch = brew_arch_for_go_arch(component)
            break

    return arch, priv


def resolve_pullspec(tag: str, arch: str, priv: bool) -> Tuple[str, str]:
    """Resolve a RELEASE_TAG to a container pullspec.

    Ported from the Jenkinsfile preamble pullspec resolution block.

    Logic:
      - If tag contains "/", treat the whole thing as a pullspec.
        The tag portion is extracted after the last ":".
      - If tag contains "nightly", build a registry.ci pullspec
        using the arch/priv suffix convention.
      - Otherwise, use quay.io/openshift-release-dev/ocp-release.

    Args:
        tag: The raw RELEASE_TAG value.
        arch: Brew-style architecture.
        priv: Whether this is a private release.

    Returns:
        Tuple of (pullspec, cleaned_tag) where cleaned_tag has any
        pullspec prefix stripped.
    """
    if "/" in tag:
        # Caller passed a full pullspec (e.g. registry.ci.openshift.org/ocp/release:4.19...)
        if ":" not in tag:
            raise click.BadParameter(f"RELEASE_TAG pullspec must include a :tag — got '{tag}'")
        pullspec = tag
        tag = tag.rsplit(":", 1)[-1]
        return pullspec, tag

    suffix = go_suffix_for_arch(arch, is_private=priv)

    if "nightly" in tag:
        pullspec = f"registry.ci.openshift.org/ocp{suffix}/release{suffix}:{tag}"
    else:
        pullspec = f"quay.io/openshift-release-dev/ocp-release:{tag}"

    return pullspec, tag


def extract_major_minor(tag: str) -> Tuple[int, int]:
    """Extract major.minor version numbers from a release tag.

    Ported from commonlib.groovy extractMajorMinorVersionNumbers().

    Args:
        tag: Release tag (e.g. "4.19.0-ec.0-x86_64").

    Returns:
        Tuple of (major, minor) as integers.

    Raises:
        ValueError: If no major.minor pattern is found.
    """
    match = re.match(r"(\d+)\.(\d+)", tag)
    if not match:
        raise ValueError(f"Cannot extract major.minor from tag: {tag}")
    return int(match.group(1)), int(match.group(2))


def compute_mirror_prefix(tag: str, major: int, minor: int, arch: str) -> str:
    """Determine the mirror prefix directory.

    Ported from the Jenkinsfile preamble mirrorPrefix logic.

    Stable releases (tag matches ``{major}.{minor}.{patch}-{arch}``)
    are mirrored under the OCP version directory (e.g. "4.19").
    Everything else goes under "pre-release".

    Args:
        tag: Cleaned release tag.
        major: Major version number.
        minor: Minor version number.
        arch: Architecture string.

    Returns:
        Mirror prefix string (e.g. "4.19" or "pre-release").
    """
    stable_pattern = re.compile(rf"^{major}\.{minor}\.\d+-{re.escape(arch)}$")
    if stable_pattern.match(tag):
        return f"{major}.{minor}"
    return "pre-release"


def compute_rhcos_stream(ocp_version: str, rhcos_build: str) -> str:
    """Compute the RHCOS stream identifier from OCP version and build ID.

    Ported from rhcoslib.groovy initialize().

    For OCP 4.19+ / 5.x+, the build ID format is ``9.6.20250121-0`` and
    the stream is ``rhel-{major}.{minor}`` (e.g. ``rhel-9.6``).

    For OCP ≤ 4.18, the build ID format is ``418.94.202410090804-0`` and
    the stream is ``{ocpVersion}-{rhelMajor}.{rhelMinor}``
    (e.g. ``4.18-9.4``).

    Args:
        ocp_version: OCP major.minor string (e.g. "4.19").
        rhcos_build: RHCOS build ID string.

    Returns:
        RHCOS stream identifier string.
    """
    parts = rhcos_build.split(".")
    major_str, minor_str = ocp_version.split(".")
    major_num = int(major_str)
    minor_num = int(minor_str)

    if major_num > 4 or (major_num == 4 and minor_num > 18):
        # New format: 9.6.20250121-0 → rhel-9.6
        rhel_stream = f"{parts[0]}.{parts[1]}"
        return f"rhel-{rhel_stream}"
    else:
        # Legacy format: 418.94.202410090804-0 → 4.18-9.4
        rhel_major = parts[1][0]
        rhel_minor = parts[1][1:]
        return f"{ocp_version}-{rhel_major}.{rhel_minor}"


class RhcosSyncPipeline:
    """Combined pipeline that replaces the rhcos_sync Jenkinsfile preamble.

    This pipeline:
      1. Parses the release tag and resolves the pullspec
      2. Extracts RHCOS metadata from the release payload
      3. Generates a synclist of artifacts to download
      4. Checks whether the sync has already been performed
      5. Delegates to SyncRhcosPipeline for boot image mirroring
      6. Delegates to SignRhcosContainersPipeline for container signing
    """

    # Base URL for the RHCOS release browser (production pipeline)
    RHCOS_RELEASES_BASE_URL = (
        "https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/storage/prod/streams"
    )

    def __init__(
        self,
        runtime: Runtime,
        release_tag: str,
        force: bool = False,
        no_latest: bool = False,
        sign_only: bool = False,
        signing_env: Optional[str] = None,
    ):
        self.runtime = runtime
        self.release_tag = release_tag
        self.force = force
        self.no_latest = no_latest
        self.sign_only = sign_only
        self.signing_env = signing_env
        self.logger = runtime.logger

        # These are populated during run()
        self.arch: str = ""
        self.priv: bool = False
        self.pullspec: str = ""
        self.tag: str = ""
        self.major: int = 0
        self.minor: int = 0
        self.ocp_version: str = ""
        self.name: str = ""
        self.mirror_prefix: str = ""
        self.rhcos_build: str = ""
        self.rhcos_stream: str = ""
        self.base_dir: str = ""
        # Primary RHCOS stream data (from .data.stream)
        self.rhcos_stream_data: dict = {}
        # Additional RHCOS streams (from .data.streams), keyed by stream name
        self.extra_streams: Dict[str, dict] = {}
        # Registry auth file path, set by RegistryConfig in run()
        self._registry_auth_file: str = ""

    async def run(self):
        """Execute the full RHCOS sync pipeline."""
        # Prevent oc from using default container auth.json — all registry auth
        # must come from explicit Jenkins credentials or oc login to temp files.
        # Follows the pattern established in ocp4_konflux.py.
        os.environ.pop("XDG_RUNTIME_DIR", None)

        self.logger.info("Starting RHCOS sync pipeline for release tag: %s", self.release_tag)

        # Step 1: Parse release tag → arch, priv, pullspec (no oc calls, no auth needed)
        self._parse_release_tag()

        # Read Jenkins credential env vars for registry authentication
        quay_auth_file = os.environ.get("QUAY_AUTH_FILE", "")
        kubeconfig = os.environ.get("KUBECONFIG", "")

        # Build the list of source credential files (skip empty strings)
        source_files = [f for f in [quay_auth_file] if f]

        # Registries needed by this pipeline:
        #   - quay.io/openshift-release-dev: release payload images
        #   - registry.ci.openshift.org: CI nightly payloads
        registries = [REGISTRY_QUAY_OCP_RELEASE_DEV, REGISTRY_CI_OPENSHIFT]

        with RegistryConfig(kubeconfig=kubeconfig, source_files=source_files, registries=registries) as auth_file:
            self._registry_auth_file = auth_file
            self.logger.info("Registry auth file: %s", auth_file)
            await self._run_pipeline()

    async def _run_pipeline(self):
        """Core pipeline logic, called within a RegistryConfig context."""
        # Step 2: Determine version name and mirror prefix
        await self._resolve_version_name()

        # Step 3: Extract RHCOS metadata from the release payload
        await self._extract_rhcos_metadata()

        self.logger.info(
            "RHCOS sync parameters: arch=%s, rhcosBuild=%s, name=%s, mirrorPrefix=%s, ocpVersion=%s",
            self.arch,
            self.rhcos_build,
            self.name,
            self.mirror_prefix,
            self.ocp_version,
        )

        # Step 4: Boot image sync (unless sign-only mode)
        if not self.sign_only:
            await self._sync_boot_images()

        # Step 5: Sign RHCOS container images (for all streams)
        await self._sign_containers()

        self.logger.info("RHCOS sync pipeline completed successfully")

    def _parse_release_tag(self):
        """Parse the release tag to extract arch, priv, pullspec, and version info.

        Ported from the Jenkinsfile preamble lines that call:
          - commonlib.extractMajorMinorVersionNumbers(tag)
          - releaselib.getReleaseTagArchPriv(tag)
          - releaselib.getArchPrivSuffix(arch, priv)
          - pullspec resolution logic
        """
        # First resolve pullspec (handles the "/" case which modifies tag)
        # But we need arch first for pullspec resolution, so parse tag first
        # with the raw tag to get arch/priv, then resolve pullspec.

        # If the tag is a pullspec, extract the tag portion for arch parsing
        raw_tag = self.release_tag
        tag_for_parsing = raw_tag
        if "/" in raw_tag:
            if ":" not in raw_tag:
                raise click.BadParameter(f"RELEASE_TAG pullspec must include a :tag — got '{raw_tag}'")
            tag_for_parsing = raw_tag.rsplit(":", 1)[-1]

        self.arch, self.priv = parse_release_tag(tag_for_parsing)
        self.major, self.minor = extract_major_minor(tag_for_parsing)
        self.ocp_version = f"{self.major}.{self.minor}"

        self.pullspec, self.tag = resolve_pullspec(raw_tag, self.arch, self.priv)

        self.base_dir = f"/pub/openshift-v4/{self.arch}/dependencies/rhcos"

        self.logger.info(
            "Parsed release tag: tag=%s, arch=%s, priv=%s, pullspec=%s, ocpVersion=%s",
            self.tag,
            self.arch,
            self.priv,
            self.pullspec,
            self.ocp_version,
        )

    async def _resolve_version_name(self):
        """Determine the release version name and mirror prefix.

        Ported from the Jenkinsfile preamble:
          - For nightlies: name = "dev-{ocpVersion}", noLatest = true
          - For stable: name = oc adm release info --template '{{ .metadata.version }}'
          - mirrorPrefix = ocpVersion if stable pattern, else "pre-release"
        """
        if "nightly" in self.tag:
            self.name = f"dev-{self.ocp_version}"
            self.no_latest = True
            self.logger.info("Nightly release detected: name=%s, forcing no_latest=True", self.name)
        else:
            # Get the release name from the payload metadata
            self.name = await oc.get_release_info_template_async(
                self.pullspec, "{{ .metadata.version }}", registry_config=self._registry_auth_file
            )
            self.logger.info("Release version name from payload: %s", self.name)

        self.mirror_prefix = compute_mirror_prefix(self.tag, self.major, self.minor, self.arch)
        self.logger.info("Mirror prefix: %s", self.mirror_prefix)

    async def _extract_rhcos_metadata(self):
        """Extract RHCOS metadata from the release payload.

        Ported from the Jenkinsfile preamble that runs:
          1. oc adm release info --image-for installer {pullspec}
          2. oc image extract --path /manifests/:{tmp} {installer_image}
          3. Parse coreos-bootimages.yaml for stream data and build ID
          4. Extract additional per-stream data from .data.streams (OCP 5.x)
        """
        with tempfile.TemporaryDirectory(prefix="rhcos-sync-") as tmp_dir:
            # Get the installer image pullspec from the release payload
            installer_image = await oc.get_release_image_pullspec_async(
                self.pullspec, "installer", registry_config=self._registry_auth_file
            )
            self.logger.info("Installer image: %s", installer_image)

            # Extract manifests from the installer image
            await oc.extract_release_image_async(
                installer_image, "/manifests/", tmp_dir, registry_config=self._registry_auth_file
            )

            # Parse coreos-bootimages.yaml
            bootimages_path = Path(tmp_dir) / "coreos-bootimages.yaml"
            if not bootimages_path.exists():
                raise FileNotFoundError(f"coreos-bootimages.yaml not found in installer image manifests at {tmp_dir}")

            with open(bootimages_path) as f:
                bootimages_yaml = yaml.safe_load(f)

            # Extract the primary stream data from .data.stream
            stream_raw = bootimages_yaml.get("data", {}).get("stream")
            if not stream_raw:
                raise ValueError("No .data.stream found in coreos-bootimages.yaml")

            if isinstance(stream_raw, str):
                self.rhcos_stream_data = json.loads(stream_raw)
            else:
                self.rhcos_stream_data = stream_raw

            # Write the primary stream data to a file for the signing pipeline
            rhcos_file = Path(self.runtime.working_dir) / f"rhcos-{self.arch}.json"
            with open(rhcos_file, "w") as f:
                json.dump(self.rhcos_stream_data, f)
            self.logger.info("Wrote primary RHCOS stream data to %s", rhcos_file)

            # Extract rhcosBuild ID from .architectures.{arch}.artifacts.qemu.release
            try:
                self.rhcos_build = self.rhcos_stream_data["architectures"][self.arch]["artifacts"]["qemu"]["release"]
            except KeyError as e:
                raise ValueError(
                    f"Cannot extract RHCOS build ID from stream data "
                    f"(path: .architectures.{self.arch}.artifacts.qemu.release): {e}"
                ) from e
            self.logger.info("RHCOS build ID: %s", self.rhcos_build)

            # Compute the RHCOS stream identifier for the release browser URL
            self.rhcos_stream = compute_rhcos_stream(self.ocp_version, self.rhcos_build)
            self.logger.info("RHCOS stream: %s", self.rhcos_stream)

            # Extract additional per-stream boot image data (e.g. rhel-10 in OCP 5.x).
            # .data.streams is a map of stream-name → stream-JSON; each may contain
            # container images that also need to be cosigned.
            streams_raw = bootimages_yaml.get("data", {}).get("streams")
            if streams_raw:
                if isinstance(streams_raw, str):
                    streams_raw = json.loads(streams_raw)
                if isinstance(streams_raw, dict):
                    for stream_name, content in streams_raw.items():
                        if isinstance(content, str):
                            content = json.loads(content)
                        self.extra_streams[stream_name] = content

                        # Write each extra stream to its own file
                        extra_file = Path(self.runtime.working_dir) / f"rhcos-{self.arch}-{stream_name}.json"
                        with open(extra_file, "w") as f:
                            json.dump(content, f)
                        self.logger.info("Extracted additional RHCOS stream: %s -> %s", stream_name, extra_file)

    async def _generate_synclist(self) -> Path:
        """Generate a synclist file by fetching meta.json from the RHCOS release browser.

        Ported from rhcoslib.groovy rhcosSyncPrintArtifacts().
        Downloads meta.json from the RHCOS release browser and extracts
        image URLs into a synclist file.

        Returns:
            Path to the generated synclist file.
        """
        build_url = f"{self.RHCOS_RELEASES_BASE_URL}/{self.rhcos_stream}/builds/{self.rhcos_build}/{self.arch}"
        meta_url = f"{build_url}/meta.json"

        self.logger.info(
            "Fetching RHCOS meta.json for stream=%s, build=%s, arch=%s",
            self.rhcos_stream,
            self.rhcos_build,
            self.arch,
        )

        async with aiohttp.ClientSession() as session:
            async with session.get(meta_url) as response:
                if response.status != 200:
                    raise RuntimeError(
                        f"Failed to fetch RHCOS meta.json for build {self.rhcos_build}: HTTP {response.status}"
                    )
                meta = await response.json()

        # Extract image URLs from meta.json
        image_urls: List[str] = []
        images = meta.get("images", {})
        for image_name, image_data in images.items():
            if ENFORCE_ALLOWLIST and image_name not in RHCOS_ALLOWLIST:
                continue
            path = image_data.get("path")
            if path:
                image_urls.append(f"{build_url}/{path}")

        self.logger.info("Generated synclist with %d artifacts", len(image_urls))

        # Write synclist to a file
        synclist_path = Path(self.runtime.working_dir) / f"rhcos-synclist-{self.arch}.txt"
        synclist_path.write_text("\n".join(image_urls) + "\n")

        return synclist_path

    async def _check_needs_happening(self) -> bool:
        """Check whether this RHCOS sync has already been performed.

        Ported from rhcoslib.groovy rhcosSyncNeedsHappening().
        Fetches rhcos-id.txt from mirror.openshift.com and compares
        the build ID. Returns False (skip) if they match.

        Returns:
            True if sync should proceed, False if already done.
        """
        if self.force:
            self.logger.info("FORCE flag set, skipping needsHappening check")
            return True

        mirror_url = f"https://mirror.openshift.com{self.base_dir}/{self.mirror_prefix}/{self.name}/rhcos-id.txt"
        self.logger.info("Checking existing RHCOS build on mirror: %s", mirror_url)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(mirror_url) as response:
                    if response.status != 200:
                        self.logger.info("rhcos-id.txt not found on mirror (HTTP %d), sync is needed", response.status)
                        return True
                    existing_build = (await response.text()).strip()
        except aiohttp.ClientError as e:
            self.logger.warning("Failed to check mirror for existing build: %s", e)
            return True

        self.logger.info("RHCOS build requested to sync: %s", self.rhcos_build)
        self.logger.info("RHCOS build on mirror: %s", existing_build)

        if existing_build == self.rhcos_build:
            self.logger.info("RHCOS build is already on mirror, skipping sync")
            return False

        self.logger.info("Requested RHCOS build is not on mirror, sync is needed")
        return True

    async def _sync_boot_images(self):
        """Generate synclist, check if sync is needed, and run SyncRhcosPipeline.

        Orchestrates the "sync" portion of the Jenkinsfile:
          1. Generate synclist from meta.json
          2. Check needsHappening
          3. Call SyncRhcosPipeline.run()
        """
        # Generate the synclist
        synclist_path = await self._generate_synclist()

        # Check if sync is needed
        needs_happening = await self._check_needs_happening()
        if not needs_happening:
            self.logger.info("Sync is not needed (already on mirror). Skipping boot image sync.")
            return

        # Determine signing environment
        signing_env = self.signing_env
        if signing_env is None:
            signing_env = "stage" if self.runtime.dry_run else "prod"

        # Run the existing SyncRhcosPipeline
        self.logger.info("Starting boot image sync...")
        pipeline = SyncRhcosPipeline(
            runtime=self.runtime,
            arch=self.arch,
            build_id=self.rhcos_build,
            version=self.name,
            mirror_prefix=self.mirror_prefix,
            base_dir=self.base_dir,
            synclist=str(synclist_path),
            no_latest=self.no_latest,
            signing_env=signing_env,
        )
        await pipeline.run()
        self.logger.info("Boot image sync completed")

    async def _sign_containers(self):
        """Sign RHCOS container images for all streams.

        Ported from the Jenkinsfile "Sign RHCOS container images" stage.
        Signs the primary RHCOS stream file plus any additional streams
        extracted from .data.streams (e.g. rhel-10 in OCP 5.x).
        """
        signing_env = self.signing_env
        if signing_env is None:
            signing_env = "stage" if self.runtime.dry_run else "prod"

        # Sign the primary RHCOS stream.
        # Use async_run() instead of run() because we are already inside an
        # async event loop — run() calls asyncio.run() which would raise
        # "RuntimeError: This event loop is already running".
        rhcos_file = Path(self.runtime.working_dir) / f"rhcos-{self.arch}.json"
        self.logger.info("Signing primary RHCOS containers from %s", rhcos_file)
        try:
            primary_pipeline = SignRhcosContainersPipeline(
                runtime=self.runtime,
                rhcos_file=str(rhcos_file),
                arch=self.arch,
                signing_env=signing_env,
            )
            await primary_pipeline.async_run()
            self.logger.info("Successfully signed primary RHCOS container images")
        except Exception as e:
            self.logger.error("Failed to sign primary RHCOS container images: %s", e)
            # The Jenkinsfile logs a warning but doesn't fail for the primary stream
            # However, it DOES fail for extra streams. We'll match that behavior.

        # Sign container images from any additional streams (e.g. rhel-10 in OCP 5.x).
        # The Jenkinsfile collects failures and throws after the loop.
        extra_failures: List[str] = []
        for stream_name in self.extra_streams:
            extra_file = Path(self.runtime.working_dir) / f"rhcos-{self.arch}-{stream_name}.json"
            if not extra_file.exists():
                self.logger.warning("Extra stream file not found: %s", extra_file)
                extra_failures.append(stream_name)
                continue

            self.logger.info("Signing additional RHCOS stream: %s (%s)", stream_name, extra_file)
            try:
                extra_pipeline = SignRhcosContainersPipeline(
                    runtime=self.runtime,
                    rhcos_file=str(extra_file),
                    arch=self.arch,
                    signing_env=signing_env,
                )
                await extra_pipeline.async_run()
                self.logger.info("Successfully signed extra RHCOS stream: %s", stream_name)
            except Exception as e:
                self.logger.error("Failed to sign stream %s: %s", stream_name, e)
                extra_failures.append(stream_name)

        if extra_failures:
            raise RuntimeError(f"Signing failed for extra RHCOS streams: {', '.join(extra_failures)}")


@cli.command("rhcos-sync", help="Sync RHCOS boot images and sign containers for an OCP release")
@click.option(
    "--release-tag",
    required=True,
    help=(
        "Release tag or pullspec from which to get RHCOS buildID reference "
        "(e.g. 4.19.0-ec.0-x86_64, 4.12.0-0.nightly-2024-01-01-010101, "
        "or registry.ci.openshift.org/ocp/release:4.19.0-ec.0-x86_64)"
    ),
)
@click.option("--force", is_flag=True, default=False, help="Download and sync even if already on mirror")
@click.option("--no-latest", is_flag=True, default=False, help="Do not update the 'latest' directory")
@click.option(
    "--sign-only",
    is_flag=True,
    default=False,
    help="Only sign RHCOS container images (skip boot image mirror sync)",
)
@click.option(
    "--signing-env",
    type=click.Choice(["prod", "stage"]),
    default=None,
    help="Signing environment (default: 'prod' for real runs, 'stage' for dry-run)",
)
@pass_runtime
@click_coroutine
async def rhcos_sync(
    runtime: Runtime,
    release_tag: str,
    force: bool,
    no_latest: bool,
    sign_only: bool,
    signing_env: Optional[str],
):
    """Sync RHCOS boot images to mirror.openshift.com and sign container images.

    This command replaces the orchestration preamble previously implemented
    in the aos-cd-jobs rhcos_sync Jenkinsfile.  It resolves the release tag
    to a pullspec, extracts RHCOS metadata from the payload, generates a
    synclist, checks whether the sync has already been done, and delegates
    to the existing sync-rhcos and sign-rhcos-containers pipelines.

    Examples:

      Sync a stable release:

        artcd -vv rhcos-sync --release-tag 4.19.0-x86_64

      Sync a nightly (dev) release:

        artcd -vv rhcos-sync --release-tag 4.19.0-0.nightly-2025-01-15-010101

      Sign-only mode (z-stream):

        artcd -vv rhcos-sync --release-tag 4.18.5-x86_64 --sign-only

      Dry-run with explicit signing env:

        artcd -vv --dry-run rhcos-sync --release-tag 4.19.0-ec.0-x86_64 --signing-env stage
    """
    pipeline = RhcosSyncPipeline(
        runtime=runtime,
        release_tag=release_tag,
        force=force,
        no_latest=no_latest,
        sign_only=sign_only,
        signing_env=signing_env,
    )
    await pipeline.run()
