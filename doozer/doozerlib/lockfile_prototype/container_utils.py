"""
Container image utilities for RPM lockfile generation.

Provides async helpers for interacting with container images via
oc (tag-to-digest resolution, preferring manifest-list digests,
and reading files from images), SBOMs (querying installed packages),
and extracted RPM databases as a fallback for images without usable SBOMs.
"""

import base64
import binascii
import json
import logging
import os
import tempfile
from pathlib import Path

from artcommonlib import logutil
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.exectools import cmd_gather_async
from artcommonlib.util import oc_image_info_for_arch_async
from packageurl import PackageURL

from doozerlib.constants import BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL
from doozerlib.lockfile_prototype.constants import DEFAULT_PLATFORM, DIGEST_PREFIX, RPM_PSEUDO_PACKAGES
from doozerlib.lockfile_prototype.utils import build_env

_SBOM_PREDICATE_TYPE = "https://spdx.dev/Document"


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
        Query installed RPM package names from an image SBOM, falling back to its RPM database.

        Arg(s):
            image_pullspec (str): Fully-qualified image pullspec (digest preferred).
            arch (str): Brew architecture to query.
        Return Value(s):
            list[str]: Sorted unique package names installed in the image.
        """
        query_pullspec = self._proxy_pullspec(image_pullspec)
        platform = f"linux/{go_arch_for_brew_arch(arch)}"
        self.logger.info("Discovering installed packages for %s [arch=%s, platform=%s]", image_pullspec, arch, platform)
        packages = await self._get_installed_packages_from_sbom(query_pullspec, platform)
        if packages:
            return packages

        self.logger.info("No usable SBOM for %s [platform=%s]; extracting RPMDB", image_pullspec, platform)
        return await self._get_installed_packages_from_rpmdb(query_pullspec, platform, image_pullspec)

    async def _get_installed_packages_from_sbom(self, image_pullspec: str, platform: str) -> list[str]:
        """
        Extract installed RPM package names from an SBOM attached to an image.

        Arg(s):
            image_pullspec (str): Image pullspec used to download the SBOM.
            platform (str): Image platform in ``os/architecture`` form.
        Return Value(s):
            list[str]: Sorted unique RPM package names, or an empty list when no usable SBOM exists.
        """
        self.logger.info("Checking SPDX attestation for %s [platform=%s]", image_pullspec, platform)
        content = await self._get_spdx_attestation(image_pullspec, platform)
        if content:
            packages = self._parse_sbom_package_names(content)
            if packages:
                self.logger.info(
                    "Found %d packages in SPDX attestation for %s [platform=%s]",
                    len(packages),
                    image_pullspec,
                    platform,
                )
                return packages

        self.logger.info(
            "No usable SPDX attestation for %s [platform=%s]; checking legacy SPDX attachment",
            image_pullspec,
            platform,
        )
        packages = await self._get_installed_packages_from_spdx_attachment(image_pullspec, platform)
        if packages:
            self.logger.info(
                "Found %d packages in SPDX attachment for %s [platform=%s]",
                len(packages),
                image_pullspec,
                platform,
            )
        return packages

    async def _get_spdx_attestation(self, image_pullspec: str, platform: str) -> dict | None:
        """
        Download and decode an SPDX in-toto attestation with Cosign.

        Arg(s):
            image_pullspec (str): Image pullspec used to download the attestation.
            platform (str): Image platform in ``os/architecture`` form.
        Return Value(s):
            dict | None: SPDX predicate, or None when no usable attestation exists.
        """
        cmd = [
            "cosign",
            "download",
            "attestation",
            image_pullspec,
            "--platform",
            platform,
            "--predicate-type",
            _SBOM_PREDICATE_TYPE,
        ]
        try:
            rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=build_env())
        except Exception as exc:
            self.logger.warning("Failed to download SPDX attestation from %s: %s", image_pullspec, exc)
            return None

        if rc != 0:
            if "no attestations" in stderr.lower():
                self.logger.debug("No SPDX attestation available for %s", image_pullspec)
            else:
                self.logger.warning("No usable SPDX attestation available for %s: %s", image_pullspec, stderr[:200])
            return None

        content = self._parse_sbom_attestation(stdout)
        if content is None:
            self.logger.warning("Failed to parse SPDX attestation from %s", image_pullspec)
        return content

    async def _get_installed_packages_from_spdx_attachment(self, image_pullspec: str, platform: str) -> list[str]:
        """
        Download and parse a legacy SPDX attachment.

        Arg(s):
            image_pullspec (str): Image pullspec used to find the SPDX attachment.
            platform (str): Image platform in ``os/architecture`` form.
        Return Value(s):
            list[str]: Sorted unique RPM package names, or an empty list when no SPDX attachment exists.
        """
        descriptor_cmd = [
            "oras",
            "manifest",
            "fetch",
            "--descriptor",
            "--platform",
            platform,
            image_pullspec,
            *self._registry_config_arg(),
        ]
        try:
            rc, stdout, stderr = await cmd_gather_async(descriptor_cmd, check=False)
        except Exception as exc:
            self.logger.debug("Failed to resolve image platform digest for SPDX attachment %s: %s", image_pullspec, exc)
            return []
        if rc != 0:
            self.logger.debug("No SPDX attachment available for %s: %s", image_pullspec, stderr[:200])
            return []

        try:
            descriptor = json.loads(stdout)
            image_digest = descriptor["digest"]
            algorithm, digest = image_digest.split(":", 1)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self.logger.debug("Failed to parse SPDX attachment image descriptor for %s: %s", image_pullspec, exc)
            return []
        if algorithm != "sha256":
            self.logger.debug("Unsupported image digest for SPDX attachment %s: %s", image_pullspec, image_digest)
            return []

        repository = self._repo_from_pullspec(image_pullspec)
        sbom_pullspec = f"{repository}:sha256-{digest}.sbom"
        manifest_cmd = ["oras", "manifest", "fetch", sbom_pullspec, *self._registry_config_arg()]
        try:
            rc, stdout, stderr = await cmd_gather_async(manifest_cmd, check=False)
        except Exception as exc:
            self.logger.debug("Failed to fetch SPDX attachment manifest for %s: %s", image_pullspec, exc)
            return []
        if rc != 0:
            self.logger.debug("No SPDX attachment manifest available for %s: %s", image_pullspec, stderr[:200])
            return []

        try:
            manifest = json.loads(stdout)
            sbom_layer = next(
                layer
                for layer in manifest["layers"]
                if layer.get("mediaType") in {"text/spdx+json", "application/spdx+json"}
            )
            layer_digest = sbom_layer["digest"]
        except (KeyError, StopIteration, TypeError, ValueError, json.JSONDecodeError) as exc:
            self.logger.debug("Failed to find SPDX layer in attachment for %s: %s", image_pullspec, exc)
            return []

        blob_cmd = [
            "oras",
            "blob",
            "fetch",
            "--no-tty",
            "--output",
            "-",
            f"{repository}@{layer_digest}",
            *self._registry_config_arg(),
        ]
        try:
            rc, stdout, stderr = await cmd_gather_async(blob_cmd, check=False)
        except Exception as exc:
            self.logger.debug("Failed to fetch SPDX attachment layer for %s: %s", image_pullspec, exc)
            return []
        if rc != 0:
            self.logger.debug("Failed to fetch SPDX attachment layer for %s: %s", image_pullspec, stderr[:200])
            return []

        try:
            content = json.loads(stdout)
        except (TypeError, json.JSONDecodeError) as exc:
            self.logger.debug("Failed to parse SPDX attachment for %s: %s", image_pullspec, exc)
            return []
        return self._parse_sbom_package_names(content)

    def _parse_sbom_package_names(self, content: dict) -> list[str]:
        """
        Parse installed RPM package names from an SPDX SBOM document.

        Arg(s):
            content (dict): SPDX SBOM document.
        Return Value(s):
            list[str]: Sorted unique RPM package names.
        """

        package_names: set[str] = set()
        for package in content.get("packages", []):
            for external_ref in package.get("externalRefs", []):
                if external_ref.get("referenceType") != "purl":
                    continue
                purl_string = external_ref.get("referenceLocator", "")
                if not purl_string.startswith("pkg:"):
                    continue
                try:
                    purl = PackageURL.from_string(purl_string)
                except ValueError as exc:
                    self.logger.warning("Failed to parse package URL from SPDX document: %s (%s)", purl_string, exc)
                    continue
                if purl.type != "rpm":
                    continue
                package_arch = purl.qualifiers.get("arch")
                if not package_arch or package_arch in {"src", "nosrc"} or purl.name in RPM_PSEUDO_PACKAGES:
                    continue
                # RPM PURLs without an upstream source RPM or Mobster's checksum and
                # repository ID can describe repository metadata rather than installed packages.
                has_upstream = bool(purl.qualifiers.get("upstream"))
                has_mobster_metadata = bool(purl.qualifiers.get("checksum") and purl.qualifiers.get("repository_id"))
                if not has_upstream and not has_mobster_metadata:
                    continue
                if purl.name:
                    package_names.add(purl.name)

        packages = sorted(package_names)
        return packages

    @staticmethod
    def _parse_sbom_attestation(stdout: str) -> dict | None:
        """
        Decode the SPDX document from a Cosign in-toto attestation.

        Arg(s):
            stdout (str): JSON-encoded Cosign attestation envelope.
        Return Value(s):
            dict | None: SPDX predicate, or None when the envelope is invalid.
        """
        try:
            envelope = json.loads(stdout)
            payload = base64.b64decode(envelope["payload"], validate=True)
            statement = json.loads(payload)
            predicate = statement["predicate"]
        except (binascii.Error, KeyError, TypeError, ValueError):
            return None
        return predicate if isinstance(predicate, dict) else None

    @staticmethod
    def _parse_package_names(stdout: str) -> list[str]:
        """
        Parse RPM package names from command output.

        Arg(s):
            stdout (str): Newline-separated RPM package names.
        Return Value(s):
            list[str]: Sorted unique package names excluding RPM pseudo-packages.
        """
        return sorted(
            {line.strip() for line in stdout.splitlines() if line.strip() and line.strip() not in RPM_PSEUDO_PACKAGES}
        )

    @staticmethod
    def _find_rpmdb_directories(extract_dir: Path) -> list[Path]:
        """
        Find candidate RPM database directories in an extracted image path.

        Arg(s):
            extract_dir (Path): Directory containing the extracted RPM database.
        Return Value(s):
            list[Path]: Candidate directories, preferring the extraction root.
        """
        directories = []
        if any((extract_dir / database_file).is_file() for database_file in ("rpmdb.sqlite", "Packages")):
            directories.append(extract_dir)
        for database_file in ("rpmdb.sqlite", "Packages"):
            for path in extract_dir.rglob(database_file):
                if path.is_file() and path.parent not in directories:
                    directories.append(path.parent)
        return directories

    async def _get_installed_packages_from_rpmdb(
        self, image_pullspec: str, platform: str, original_pullspec: str
    ) -> list[str]:
        """
        Extract and query an image RPM database with host-side tools.

        Arg(s):
            image_pullspec (str): Image pullspec passed to ``oc image extract``.
            platform (str): Image platform in ``os/architecture`` form.
            original_pullspec (str): Original image pullspec used for logging.
        Return Value(s):
            list[str]: Sorted unique package names, or an empty list on command failure.
        """
        rpmdb_paths = ("/usr/lib/sysimage/rpm/", "/var/lib/rpm/")
        with tempfile.TemporaryDirectory() as tmpdir:
            for index, rpmdb_path in enumerate(rpmdb_paths):
                extract_dir = Path(tmpdir) / f"rpmdb-{index}"
                extract_dir.mkdir()
                self.logger.info(
                    "Extracting RPMDB for %s [platform=%s] from %s", original_pullspec, platform, rpmdb_path
                )
                extract_cmd = [
                    "oc",
                    "image",
                    "extract",
                    image_pullspec,
                    "--path",
                    f"{rpmdb_path}:{extract_dir}",
                    "--confirm",
                    "--filter-by-os",
                    platform,
                    *self._registry_config_arg(),
                ]
                try:
                    rc, _, stderr = await cmd_gather_async(extract_cmd, check=False)
                except Exception as exc:
                    self.logger.warning("Failed to extract RPM database from %s: %s", image_pullspec, exc)
                    continue
                if rc != 0:
                    self.logger.debug("RPMDB path %s unavailable for %s: %s", rpmdb_path, image_pullspec, stderr[:200])
                    continue

                rpmdb_directories = self._find_rpmdb_directories(extract_dir)
                if not rpmdb_directories:
                    self.logger.debug("No RPM database files found at %s in %s", rpmdb_path, image_pullspec)
                    continue

                for rpmdb_dir in rpmdb_directories:
                    query_cmd = [
                        "rpm",
                        "--dbpath",
                        f"{rpmdb_dir}",
                        "-qa",
                        "--qf",
                        r"%{NAME}\n",
                    ]
                    try:
                        rc, stdout, stderr = await cmd_gather_async(query_cmd, check=False)
                    except Exception as exc:
                        self.logger.warning("Failed to query RPM database from %s: %s", original_pullspec, exc)
                        continue
                    if rc == 0:
                        packages = self._parse_package_names(stdout)
                        self.logger.info(
                            "Found %d packages in RPMDB for %s [platform=%s]",
                            len(packages),
                            original_pullspec,
                            platform,
                        )
                        return packages
                    self.logger.debug("RPMDB query failed for %s: %s", original_pullspec, stderr[:200])

        self.logger.warning("Unable to discover installed packages from %s [platform=%s]", original_pullspec, platform)
        return []

    @staticmethod
    def _registry_config_arg() -> list[str]:
        """
        Return ``['--registry-config', path]`` if a registry auth file is
        available in the environment, otherwise an empty list.
        """
        auth = os.environ.get("QUAY_AUTH_FILE") or os.environ.get("REGISTRY_AUTH_FILE")
        if auth:
            return ["--registry-config", auth]
        return []

    async def read_file_from_image(self, image_pullspec: str, filepath: str) -> str:
        """
        Read a file from a container image via ``oc image extract``.

        Handles simple file paths (``/more-pkgs``), glob patterns
        (``/etc/*.repo``), and directory paths (``/etc/yum.repos.d/``).
        For a simple file the basename is read directly (fast path).
        Otherwise all extracted regular files are concatenated in sorted
        order.

        Arg(s):
            image_pullspec (str): Fully-qualified image pullspec.
            filepath (str): Absolute path (or glob) inside the image.
        Return Value(s):
            str: File contents, or empty string on failure.
        """
        query_pullspec = self._proxy_pullspec(image_pullspec)
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = [
                "oc",
                "image",
                "extract",
                query_pullspec,
                "--path",
                f"{filepath}:{tmpdir}",
                "--confirm",
                "--filter-by-os",
                DEFAULT_PLATFORM,
                *self._registry_config_arg(),
            ]
            rc, _, stderr = await cmd_gather_async(cmd, check=False)
            if rc != 0:
                self.logger.warning("Failed to read %s from %s: %s", filepath, query_pullspec, stderr[:200])
                return ""

            # Fast path: simple file whose basename lands directly in tmpdir
            extracted = os.path.join(tmpdir, os.path.basename(filepath))
            try:
                with open(extracted) as f:
                    return f.read()
            except (FileNotFoundError, IsADirectoryError):
                pass

            # Fallback: glob/directory extraction — read all regular files
            files = sorted(p for p in Path(tmpdir).rglob("*") if p.is_file())
            if not files:
                self.logger.warning("No files extracted for %s from %s", filepath, query_pullspec)
                return ""
            parts = []
            for f in files:
                parts.append(f.read_text())
            return "".join(parts)
