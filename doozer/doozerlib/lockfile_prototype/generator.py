"""
RPM lockfile generator using rpm-lockfile-prototype.

Orchestrates lockfile generation by building rpms.in.yaml input configs
and delegating both Dockerfile package extraction and RPM resolution to
the upstream rpm-lockfile-prototype tool (v0.22.0+). Container image
interactions are delegated to ContainerImageHelper.
"""

import logging
import re
import shlex
from pathlib import Path

import yaml
from artcommonlib import logutil
from artcommonlib.arch_util import BREW_ARCHES
from dockerfile_parse import DockerfileParser

from doozerlib.image import ImageMetadata
from doozerlib.lockfile_prototype.constants import (
    BASEARCH_VAR,
    DEFAULT_RPM_LOCKFILE_NAME,
    DIGEST_PREFIX,
    DOCKERFILE_NAME,
    MAX_REINSTALL_STRIP_RETRIES,
    MAX_RESOLUTION_RETRIES,
)
from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.fallback import extract_generated_file_content
from doozerlib.lockfile_prototype.lockfile_merger import merge_lockfiles
from doozerlib.lockfile_prototype.models import (
    ArchSpecificPackage,
    LockfileData,
    ModuleEntry,
    PackageEntry,
    RepoEntry,
    RpmsInConfig,
)
from doozerlib.lockfile_prototype.resolver import RpmResolver
from doozerlib.lockfile_prototype.utils import format_version_pin, pick_minimum_evr
from doozerlib.repos import Repos


def _is_local_rpm(token: str) -> bool:
    """
    Return True if token is a local RPM file path or glob that can't
    be resolved via lockfile repos (e.g. ``/path/*.rpm``, ``foo.rpm``).
    """
    if token.endswith(".rpm"):
        return True
    if "/" in token and "*" in token:
        return True
    return False


_BARE_UPDATE_RE = re.compile(
    r"\b(?:microdnf|dnf|yum)\s+(?:-y\s+)?(?:update|upgrade)(?:\s+-y)?\s*(?:\\\n\s*&&\s*|&&\s*|;\s*|\n|(?=$))"
)

_INSTALL_CMD_RE = re.compile(
    r"\b(?:microdnf|dnf|yum)\s+(?:.*?\s+)?install\b(.*?)(?:&&|;|\n|$)",
    re.DOTALL,
)

_PKG_NAME_RE = re.compile(r"^[a-zA-Z][\w.+\-]*$")

_RPM_ARCHES = frozenset({"x86_64", "aarch64", "ppc64le", "s390x", "i686", "noarch", "src"})


def _extract_install_packages(entries: list[dict], stage_num: int) -> set[str]:
    """
    Extract package names from yum/dnf/microdnf install commands in a
    specific Dockerfile stage. This is a lightweight extraction used
    only to detect overlap with base image packages — the canonical
    extraction is done by the upstream tool's packagesFromContainerfile.

    Architecture qualifiers (e.g. glibc.x86_64) are stripped so the
    result matches the plain names returned by rpm -qa --qf %{NAME}.

    Arg(s):
        entries (list[dict]): DockerfileParser structure entries.
        stage_num (int): 0-indexed stage number.
    Return Value(s):
        set[str]: Package names found in install commands.
    """
    packages: set[str] = set()
    current_stage = -1
    for entry in entries:
        if entry["instruction"] == "FROM":
            current_stage += 1
        elif entry["instruction"] == "RUN" and current_stage == stage_num:
            for m in _INSTALL_CMD_RE.finditer(entry["value"]):
                for token in m.group(1).split():
                    token = token.strip().rstrip("\\")
                    if token.startswith("-") or token.startswith("$"):
                        continue
                    if _PKG_NAME_RE.match(token):
                        if "." in token:
                            name, _, suffix = token.rpartition(".")
                            if suffix in _RPM_ARCHES:
                                token = name
                        packages.add(token)
    return packages


def _detect_stages_with_bare_updates(entries: list[dict]) -> set[int]:
    """
    Detect stages containing bare dnf/yum/microdnf update/upgrade
    commands (no named packages). These need explicit upgrade target
    resolution because packagesFromContainerfile does not extract
    packages from bare update commands.

    Arg(s):
        entries (list[dict]): DockerfileParser structure entries.
    Return Value(s):
        set[int]: Stage numbers (0-indexed) with bare update commands.
    """
    stages: set[int] = set()
    stage_idx = -1
    for entry in entries:
        if entry["instruction"] == "FROM":
            stage_idx += 1
        elif entry["instruction"] == "RUN" and stage_idx >= 0:
            if _BARE_UPDATE_RE.search(entry["value"]):
                stages.add(stage_idx)
    return stages


def build_rpms_in_yaml(
    repos: list[RepoEntry],
    arches: list[str],
    packages: list[str],
    arch_specific_packages: dict[str, list[str]] | None = None,
    reinstall_packages: list[str] | None = None,
    upgrade_packages: list[str] | None = None,
    module_enable: list[str] | None = None,
    exclude_packages: list[str] | None = None,
) -> RpmsInConfig:
    """
    Build the rpms.in.yaml config for rpm-lockfile-prototype.

    Arg(s):
        repos (list[RepoEntry]): Repository entries.
        arches (list[str]): Target architectures.
        packages (list[str]): Extra package names beyond what the tool
            extracts from the Containerfile (e.g., $(cat ...) resolved
            packages, builddep packages).
        arch_specific_packages (dict[str, list[str]] | None): Per-arch packages.
        reinstall_packages (list[str] | None): Installed packages to reinstall
            from repos (ensures they appear in the lockfile).
        upgrade_packages (list[str] | None): Packages to upgrade.
        module_enable (list[str] | None): Module streams to enable
            (e.g., ["nodejs:18", "python36:3.6"]).
        exclude_packages (list[str] | None): Packages to exclude from
            resolution (e.g., OKD-only packages extracted from Containerfile
            that are absent from RHEL repos).
    Return Value(s):
        RpmsInConfig: Config ready for YAML serialization.
    """
    packages = [p for p in packages if not _is_local_rpm(p)]
    if arch_specific_packages:
        arch_specific_packages = {
            arch: [p for p in pkgs if not _is_local_rpm(p)] for arch, pkgs in arch_specific_packages.items()
        }
    if reinstall_packages:
        reinstall_packages = [p for p in reinstall_packages if not _is_local_rpm(p)]
    if upgrade_packages:
        upgrade_packages = [p for p in upgrade_packages if not _is_local_rpm(p)]

    package_entries: list[str | ArchSpecificPackage] = list(packages)
    if arch_specific_packages:
        for arch, arch_pkgs in arch_specific_packages.items():
            for pkg in arch_pkgs:
                if pkg not in packages:
                    package_entries.append(ArchSpecificPackage(name=pkg, arches={"only": arch}))

    return RpmsInConfig(
        arches=arches,
        contentOrigin={"repos": repos},
        packages=package_entries,
        reinstallPackages=list(reinstall_packages) if reinstall_packages else [],
        upgradePackages=list(upgrade_packages) if upgrade_packages else [],
        moduleEnable=list(module_enable) if module_enable else [],
        excludePackages=list(exclude_packages) if exclude_packages else [],
    )


class RpmLockfilePrototypeGenerator:
    """
    Orchestrates RPM lockfile generation using rpm-lockfile-prototype.

    Composes ContainerImageHelper for image operations and RpmResolver
    for DNF resolution.
    """

    def __init__(
        self,
        repos: Repos,
        working_dir: Path,
        logger: logging.Logger | None = None,
        container_helper: ContainerImageHelper | None = None,
        resolver: RpmResolver | None = None,
    ):
        self.repos = repos
        self.downstream_parents: list[str] = []
        self.fallback_installed: dict[int, list[str]] = {}
        self.parent_source_dirs: dict[int, Path] = {}
        self.logger = logger or logutil.get_logger(__name__)
        self.upgrades_dropped = False
        self._container = container_helper or ContainerImageHelper(logger=self.logger)
        self._resolver = resolver or RpmResolver(working_dir=working_dir, logger=self.logger)

    async def generate_lockfile(
        self,
        image_meta: ImageMetadata,
        dest_dir: Path,
        filename: str = DEFAULT_RPM_LOCKFILE_NAME,
        downstream_parents: list[str] | None = None,
        fallback_installed: dict[int, list[str]] | None = None,
        parent_source_dirs: dict[int, Path] | None = None,
    ) -> None:
        """
        Generate an RPM lockfile using rpm-lockfile-prototype.

        Package extraction from Dockerfile RUN commands is delegated to
        the upstream tool via the containerfile context in rpms.in.yaml.
        Extra packages (from $(cat ...) resolution, builddep, etc.) are
        passed in the packages field.

        Arg(s):
            image_meta (ImageMetadata): Image metadata with repo/arch config.
            dest_dir (Path): Directory containing the Dockerfile and where
                the lockfile will be written.
            filename (str): Output lockfile name.
            downstream_parents (list[str] | None): Per-stage base image pullspecs.
            fallback_installed (dict[int, list[str]] | None): Per-stage fallback
                package lists from parent lockfiles, used for conflict detection
                when base images are unreachable.
            parent_source_dirs (dict[int, Path] | None): Per-stage parent build
                directories for reading files when base images are unreachable.
        """
        if downstream_parents is not None:
            self.downstream_parents = downstream_parents
        if fallback_installed is not None:
            self.fallback_installed = fallback_installed
        if parent_source_dirs is not None:
            self.parent_source_dirs = parent_source_dirs
        if not image_meta.is_lockfile_generation_enabled():
            self.logger.debug(f"Lockfile generation disabled for {image_meta.distgit_key}")
            return
        enabled_repos = image_meta.get_enabled_repos()
        if not enabled_repos:
            self.logger.info(f"No enabled repos for {image_meta.distgit_key}, skipping")
            return
        dockerfile_path = dest_dir / DOCKERFILE_NAME
        if not dockerfile_path.exists():
            self.logger.warning(f"{image_meta.distgit_key}: no Dockerfile found, skipping")
            return

        arches = image_meta.get_arches()
        repo_list = self._build_repo_list(enabled_repos, arches)

        entries = DockerfileParser(str(dockerfile_path)).structure
        cat_packages = await self._resolve_cat_packages(entries, self.downstream_parents)

        # Count stages and track which ones have RUN commands.
        # Stages without RUN (and no $(cat ...) extra packages) can't
        # install RPMs, so we skip them to avoid unnecessary subprocess
        # invocations of rpm-lockfile-prototype.
        total_stages = 0
        stages_with_runs: set[int] = set()
        stage_idx = -1
        for entry in entries:
            if entry["instruction"] == "FROM":
                stage_idx += 1
                total_stages += 1
            elif entry["instruction"] == "RUN" and stage_idx >= 0:
                stages_with_runs.add(stage_idx)

        stages_with_bare_updates = _detect_stages_with_bare_updates(entries)

        out_file_path = dest_dir / filename
        if total_stages == 0:
            self._write_lockfile(None, out_file_path, image_meta.distgit_key)
            return

        stage_lockfiles = await self._resolve_all_stages(
            total_stages,
            cat_packages,
            stages_with_runs,
            stages_with_bare_updates,
            repo_list,
            arches,
            image_meta.distgit_key,
            dockerfile_path,
            entries,
        )
        lockfile = self._assemble_lockfile(stage_lockfiles, image_meta)
        self._write_lockfile(lockfile, out_file_path, image_meta.distgit_key)

    async def _resolve_cat_packages(
        self,
        entries: list[dict],
        downstream_parents: list[str],
    ) -> dict[int, list[str]]:
        """
        Find $(cat /filepath) patterns in Dockerfile install commands and
        resolve them by reading file contents from base images via podman.

        Arg(s):
            entries (list[dict]): DockerfileParser structure entries.
            downstream_parents (list[str]): Per-stage base image pullspecs.
        Return Value(s):
            dict[int, list[str]]: Stage number to extra package names.
        """
        cat_pattern = re.compile(r"\$\(\s*cat\s+(/\S+)\s*\)")

        stage_runs: list[list[str]] = []
        current_runs: list[str] = []
        seen_from = False

        for entry in entries:
            if entry["instruction"] == "FROM":
                if seen_from:
                    stage_runs.append(current_runs)
                seen_from = True
                current_runs = []
            elif entry["instruction"] == "RUN" and seen_from:
                current_runs.append(entry["value"])
        if seen_from:
            stage_runs.append(current_runs)

        extra_packages: dict[int, list[str]] = {}

        for stage_num, runs in enumerate(stage_runs):
            image_pullspec = downstream_parents[stage_num] if stage_num < len(downstream_parents) else None
            if not image_pullspec or "/" not in image_pullspec:
                continue

            cat_files: set[str] = set()
            for run_body in runs:
                for match in cat_pattern.finditer(run_body):
                    cat_files.add(match.group(1))

            if not cat_files:
                continue

            stage_pkgs: list[str] = []
            for filepath in cat_files:
                content = await self._container.read_file_from_image(image_pullspec, filepath)
                if not content and stage_num in self.parent_source_dirs:
                    content = self._read_file_from_parent_source(self.parent_source_dirs[stage_num], filepath)
                if not content:
                    continue
                self.logger.debug(f"Resolved $(cat {filepath}) from base image: {content.strip()}")
                try:
                    tokens = shlex.split(content.strip())
                except ValueError:
                    tokens = content.strip().split()
                for token in tokens:
                    token = token.strip()
                    if token and not token.startswith("-") and re.match(r"^[\w][\w.\-]*$", token):
                        stage_pkgs.append(token)

            if stage_pkgs:
                extra_packages[stage_num] = sorted(set(stage_pkgs))

        return extra_packages

    def _read_file_from_parent_source(self, parent_dir: Path, container_path: str) -> str:
        """
        Try to read a container file from the parent's build directory.
        Checks if the file exists directly in the source tree first.
        Falls back to parsing parent Dockerfiles for redirect commands.

        Arg(s):
            parent_dir (Path): Parent image's build directory.
            container_path (str): Absolute path inside the container.
        Return Value(s):
            str: File content, or empty string if not found.
        """
        local_file = parent_dir / container_path.lstrip("/")
        if local_file.is_file():
            self.logger.info(f"Resolved $(cat {container_path}) from parent source dir: {local_file}")
            return local_file.read_text()

        content = extract_generated_file_content(parent_dir, container_path)
        if content:
            self.logger.info(f"Resolved $(cat {container_path}) from parent Dockerfile RUN command")
        return content

    async def _resolve_all_stages(
        self,
        total_stages: int,
        cat_packages: dict[int, list[str]],
        stages_with_runs: set[int],
        stages_with_bare_updates: set[int],
        repo_list: list[RepoEntry],
        arches: list[str],
        distgit_key: str,
        dockerfile_path: Path,
        entries: list[dict],
    ) -> list[LockfileData]:
        """
        Resolve RPM packages for each Dockerfile stage.

        Package extraction from RUN commands (including builddep) is
        handled by the upstream tool via containerfile context. Extra
        packages ($(cat ...) resolution) are passed in the packages field.

        Stages with bare dnf/yum update commands get base image packages
        as upgrade targets so the lockfile includes the updated RPMs.

        Stages without RUN commands (and no $(cat ...) extra packages)
        are skipped — they can't install RPMs. The final stage is always
        processed because it may need base image reinstall packages.
        """
        stage_lockfiles: list[LockfileData] = []
        final_stage_num = total_stages - 1

        for stage_num in range(total_stages):
            if stage_num != final_stage_num:
                if stage_num not in stages_with_runs and stage_num not in cat_packages:
                    self.logger.debug(f"{distgit_key}: stage {stage_num}: no RUN commands, skipping")
                    continue

            image_pullspec = await self._determine_stage_pullspec(stage_num, distgit_key)

            if stage_num != final_stage_num:
                if self._has_rhel_version_mismatch(stage_num, repo_list, distgit_key):
                    continue

            extra_packages: list[str] = list(cat_packages.get(stage_num, []))

            reinstall_pkgs: list[str] | None = None
            upgrade_pkgs: list[str] | None = None
            if stage_num == final_stage_num:
                if image_pullspec:
                    base_pkgs = await self._get_base_image_packages(stage_num, image_pullspec, distgit_key)
                    if base_pkgs:
                        # Use upgrade semantics for base image packages so repos
                        # can provide newer versions. reinstallPackages pins to the
                        # installed EVR and overrides DNF's upgrade intent for the
                        # same package — e.g. python3-setuptools 53.0.0 from e4s
                        # would silently win over 67.6.1 in the ironic plashet even
                        # when the Containerfile explicitly installs a newer version.
                        upgrade_pkgs = list(base_pkgs)
                        self.logger.info(
                            f"{distgit_key}: stage {stage_num}: {len(upgrade_pkgs)} base image "
                            "packages added as upgrade targets into lockfile"
                        )
                else:
                    base_pkgs = await self._get_base_image_packages(stage_num, None, distgit_key)
                    if base_pkgs:
                        extra = [p for p in base_pkgs if p not in extra_packages]
                        if extra:
                            extra_packages = extra_packages + extra

            if stage_num in stages_with_bare_updates:
                if not image_pullspec:
                    # No base image to query (stage alias) — can't determine
                    # upgrade targets, so mark dropped to strip the bare update
                    self.upgrades_dropped = True
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: bare update detected but no "
                        "base image available, marking upgrades as dropped"
                    )
                elif stage_num != final_stage_num and not upgrade_pkgs:
                    # Final stage already populated upgrade_pkgs from base image;
                    # only query again for non-final stages with bare updates.
                    bare_update_base = await self._get_base_image_packages(stage_num, image_pullspec, distgit_key)
                    if bare_update_base:
                        upgrade_pkgs = list(bare_update_base)
                if upgrade_pkgs and stage_num in stages_with_bare_updates:
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: {len(upgrade_pkgs)} base image "
                        "packages added as upgrade targets for bare update"
                    )

            result = await self._resolve_with_reconciliation(
                repo_list,
                arches,
                extra_packages,
                image_pullspec,
                distgit_key,
                stage_num,
                reinstall_packages=reinstall_pkgs,
                containerfile_path=str(dockerfile_path),
                upgrade_packages=upgrade_pkgs,
            )

            # Pass 2: pin Dockerfile packages that overlap with the base
            # image. These are "already installed" so neither install nor
            # upgrade captures them, but cachi2 needs them in the lockfile.
            # Runs for every image-backed stage, not just final/bare-update.
            if image_pullspec:
                dockerfile_install_pkgs = _extract_install_packages(entries, stage_num)
                if dockerfile_install_pkgs:
                    if upgrade_pkgs:
                        base_pkg_set = set(upgrade_pkgs)
                    else:
                        stage_base_pkgs = await self._get_base_image_packages(stage_num, image_pullspec, distgit_key)
                        base_pkg_set = set(stage_base_pkgs) if stage_base_pkgs else set()
                    pin_candidates = sorted(dockerfile_install_pkgs & base_pkg_set)
                    if pin_candidates:
                        result = await self._pin_missing_dockerfile_packages(
                            result,
                            pin_candidates,
                            repo_list,
                            arches,
                            image_pullspec,
                            distgit_key,
                            stage_num,
                        )

            if result:
                stage_lockfiles.append(result)

        return stage_lockfiles

    async def _get_base_image_packages(self, stage_num: int, image_pullspec: str | None, distgit_key: str) -> list[str]:
        """
        Get installed package names from the base image for conflict
        detection. Tries a live podman query first; falls back to parent
        lockfile data if the image is unreachable.

        Arg(s):
            stage_num (int): Dockerfile stage number.
            image_pullspec (str | None): Base image pullspec (None = bare mode).
            distgit_key (str): Image identifier for logging.
        Return Value(s):
            list[str]: Package names, or empty if unavailable.
        """
        if image_pullspec:
            pkgs = await self._container.get_installed_packages(image_pullspec)
            if pkgs:
                return pkgs
        if stage_num in self.fallback_installed:
            self.logger.info(
                f"{distgit_key}: stage {stage_num}: using parent lockfile data "
                f"({len(self.fallback_installed[stage_num])} packages) for conflict detection"
            )
            return self.fallback_installed[stage_num]
        return []

    async def _determine_stage_pullspec(self, stage_num: int, distgit_key: str) -> str | None:
        """
        Determine the base image pullspec for a stage. Stage aliases
        (no "/") resolve to bare mode. Tags are resolved to digests.
        Falls back to bare mode when the image is unreachable.
        """
        image_pullspec = self.downstream_parents[stage_num] if stage_num < len(self.downstream_parents) else None
        if image_pullspec and "/" not in image_pullspec:
            image_pullspec = None
        if image_pullspec:
            resolved = await self._container.resolve_to_digest(image_pullspec)
            if resolved == image_pullspec and DIGEST_PREFIX not in image_pullspec:
                self.logger.warning(
                    f"{distgit_key}: stage {stage_num}: base image {image_pullspec} not reachable, "
                    "resolving in bare mode (lockfile will include all packages)"
                )
                image_pullspec = None
            else:
                image_pullspec = resolved
        return image_pullspec

    @staticmethod
    def _extract_rhel_version_from_pullspec(pullspec: str) -> int | None:
        """
        Extract RHEL major version from an image pullspec tag.

        Handles two tag formats:
        - rhel-8-golang-..., ubi-9-minimal, etc.
        - NVR-style: openshift-golang-builder-container-v1.25.9-...el8

        Arg(s):
            pullspec (str): Image pullspec with tag or digest.
        Return Value(s):
            int | None: RHEL major version (e.g. 8, 9), or None if
                not detectable.
        """
        # Search the full path (before digest) so image names like ubi9/ubi or
        # rhel8/buildah are matched even when the tag is generic (e.g. "latest")
        # or absent entirely.
        path = pullspec.split("@", 1)[0]
        m = re.search(r"(?:rhel|ubi|centos|scos)-?(\d+)", path)
        if m:
            return int(m.group(1))
        # Fallback: NVR-style tags embed the version as .el8 / .el9.
        # Guard here (not at the top) so bare paths without a tag still get
        # the path-based match above.
        image_ref = path.rsplit("/", 1)[-1]
        if ":" not in image_ref:
            return None
        tag = image_ref.rsplit(":", 1)[-1]
        m = re.search(r"\.el(\d+)", tag)
        if m:
            return int(m.group(1))
        return None

    @staticmethod
    def _extract_rhel_version_from_repos(repo_list: list[RepoEntry]) -> int | None:
        """
        Extract RHEL major version from repo content set IDs.

        Arg(s):
            repo_list (list[RepoEntry]): Repository entries with repoid
                fields like "rhel-9-for-x86_64-baseos-e4s-rpms__9_DOT_6".
        Return Value(s):
            int | None: RHEL major version, or None if not detectable.
        """
        for repo in repo_list:
            m = re.search(r"rhel-(\d+)", repo.repoid)
            if m:
                return int(m.group(1))
        return None

    def _has_rhel_version_mismatch(self, stage_num: int, repo_list: list[RepoEntry], distgit_key: str) -> bool:
        """
        Check whether a builder stage's RHEL version differs from the
        repos' RHEL version. Returns False when either version cannot
        be determined (fail-open).

        Arg(s):
            stage_num (int): Dockerfile stage number.
            repo_list (list[RepoEntry]): Repository entries.
            distgit_key (str): Image identifier for logging.
        Return Value(s):
            bool: True if a RHEL version mismatch is detected.
        """
        if stage_num >= len(self.downstream_parents):
            return False
        pullspec = self.downstream_parents[stage_num]
        if not pullspec or "/" not in pullspec:
            return False
        builder_rhel = self._extract_rhel_version_from_pullspec(pullspec)
        repo_rhel = self._extract_rhel_version_from_repos(repo_list)
        if builder_rhel is None or repo_rhel is None:
            return False
        if builder_rhel != repo_rhel:
            self.logger.warning(
                f"{distgit_key}: stage {stage_num}: RHEL version mismatch — "
                f"builder image is el{builder_rhel} but repos are el{repo_rhel}; "
                f"skipping package resolution for this stage entirely"
            )
            return True
        return False

    async def _resolve_stage_with_retry(
        self,
        repo_list: list[RepoEntry],
        arches: list[str],
        packages: list[str],
        image_pullspec: str | None,
        distgit_key: str,
        stage_num: int,
        reinstall_packages: list[str] | None = None,
        containerfile_path: str | None = None,
        upgrade_packages: list[str] | None = None,
    ) -> LockfileData | None:
        """
        Resolve a single stage, retrying after removing unavailable packages.

        When containerfile_path is set and packages extracted from the
        Containerfile are unavailable, retries with those packages added
        to excludePackages so the upstream tool skips them.

        Arg(s):
            repo_list (list[RepoEntry]): Repository entries.
            arches (list[str]): Target architectures.
            packages (list[str]): Extra packages ($(cat ...) etc.).
            image_pullspec (str | None): Base image pullspec.
            distgit_key (str): Image identifier for logging.
            stage_num (int): Dockerfile stage number (0-indexed).
            reinstall_packages (list[str] | None): Base image packages
                to reinstall from repos into the lockfile.
            containerfile_path (str | None): Dockerfile path for upstream
                package extraction.
            upgrade_packages (list[str] | None): Base image packages to
                upgrade (from bare dnf/yum update commands).
        Return Value(s):
            LockfileData | None: Lockfile data, or None if all packages filtered out.
        """
        remaining_packages = list(packages)

        # rpm-lockfile-prototype uses skopeo to pull the base image rpmdb.
        # brew.registry.redhat.io requires auth that skopeo may not have;
        # use the no-auth registry proxy instead.
        resolver_pullspec = ContainerImageHelper._proxy_pullspec(image_pullspec) if image_pullspec else None

        # When reinstall_packages comes from the base image, also pass
        # them as upgrade targets. base.reinstall() raises
        # PackagesNotAvailableError when the installed version isn't in
        # the configured repos — but rpm-lockfile-prototype swallows that
        # error when the package is also in upgradePackages.
        remaining_reinstall = list(reinstall_packages) if reinstall_packages else []
        remaining_upgrade = list(upgrade_packages) if upgrade_packages else []
        real_retries = 0
        reinstall_strip_count = 0
        excluded_packages: set[str] = set()

        while real_retries < MAX_RESOLUTION_RETRIES:
            all_upgrade_targets = list(remaining_reinstall)
            if remaining_upgrade:
                existing = set(all_upgrade_targets)
                all_upgrade_targets.extend(p for p in remaining_upgrade if p not in existing)
            effective_upgrade = all_upgrade_targets if (image_pullspec and all_upgrade_targets) else None
            in_yaml = build_rpms_in_yaml(
                repo_list,
                arches,
                remaining_packages,
                reinstall_packages=remaining_reinstall if image_pullspec else None,
                upgrade_packages=effective_upgrade,
                exclude_packages=sorted(excluded_packages) if excluded_packages else None,
            )

            try:
                mode = "image" if resolver_pullspec else "bare"
                total = len(remaining_packages) + len(remaining_reinstall) + len(remaining_upgrade)
                self.logger.info(f"{distgit_key}: stage {stage_num}: resolving {total} extra packages in {mode} mode")
                return await self._resolver.resolve(
                    in_yaml,
                    image_pullspec=resolver_pullspec,
                    containerfile_path=containerfile_path,
                    stage_num=stage_num + 1 if containerfile_path else None,
                )
            except RuntimeError as e:
                missing = RpmResolver.parse_missing_packages(str(e))
                if not missing:
                    raise
                # When using packagesFromContainerfile, the unavailable packages
                # come from the upstream extraction — we can't strip them from
                # our side. Add them to excludePackages so the tool skips them.
                if containerfile_path:
                    new_excludes = missing - excluded_packages
                    if new_excludes:
                        excluded_packages |= new_excludes
                        self.logger.info(
                            f"{distgit_key}: stage {stage_num}: excluding unavailable "
                            f"packages extracted from Containerfile: {sorted(new_excludes)}"
                        )
                        continue
                # Drop all bare-update upgrade packages on any miss
                # (all-or-nothing: partial upgrades cause EVR conflicts).
                upgrade_hit = missing & set(remaining_upgrade) if remaining_upgrade else set()
                if upgrade_hit:
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: dropping all "
                        f"{len(remaining_upgrade)} bare-update upgrade packages "
                        f"({len(upgrade_hit)} unavailable)"
                    )
                    remaining_upgrade.clear()
                    self.upgrades_dropped = True
                    missing = missing - upgrade_hit
                    if not missing:
                        continue
                reinstall_only = missing & set(remaining_reinstall)
                fully_missing = missing - reinstall_only
                removed = 0
                if reinstall_only:
                    remaining_reinstall = [p for p in remaining_reinstall if p not in reinstall_only]
                    removed += len(reinstall_only)
                    reinstall_strip_count += 1
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: stripped from reinstall only: {sorted(reinstall_only)}"
                    )
                if fully_missing:
                    before = len(remaining_packages) + len(remaining_reinstall)
                    remaining_packages = [p for p in remaining_packages if p not in fully_missing]
                    remaining_reinstall = [p for p in remaining_reinstall if p not in fully_missing]
                    actually_removed = before - len(remaining_packages) - len(remaining_reinstall)
                    removed += actually_removed
                    if actually_removed:
                        real_retries += 1
                        reinstall_strip_count = 0
                if not removed and not upgrade_hit:
                    raise
                self.logger.warning(
                    f"{distgit_key}: stage {stage_num}: retrying without unavailable packages: {sorted(missing)}"
                )
                if reinstall_strip_count >= MAX_REINSTALL_STRIP_RETRIES and remaining_reinstall:
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: {reinstall_strip_count} consecutive "
                        f"reinstall-only failures, bulk-dropping {len(remaining_reinstall)} "
                        "reinstall packages to avoid serial retries"
                    )
                    remaining_reinstall.clear()
                if (
                    not remaining_packages
                    and not remaining_reinstall
                    and not remaining_upgrade
                    and not containerfile_path
                ):
                    self.logger.warning(
                        f"{distgit_key}: stage {stage_num}: no packages remaining after filtering, skipping"
                    )
                    return None

        raise RuntimeError(f"{distgit_key}: stage {stage_num}: exceeded {MAX_RESOLUTION_RETRIES} retries")

    def _assemble_lockfile(self, stage_lockfiles: list[LockfileData], image_meta: ImageMetadata) -> LockfileData:
        """
        Merge stage lockfiles, filter empty arches, apply cross-arch merge.
        """
        if len(stage_lockfiles) == 1:
            final = stage_lockfiles[0]
        else:
            final = merge_lockfiles(stage_lockfiles)

        final.arches = [arch_entry for arch_entry in final.arches if arch_entry.packages or arch_entry.source]

        if image_meta.is_cross_arch_enabled():
            self._apply_cross_arch_merge(final)

        return final

    def _apply_cross_arch_merge(self, lockfile: LockfileData) -> None:
        """
        Merge packages across all architectures so each arch entry
        contains the full superset. Used for cross-arch builds.
        """
        self.logger.info("cross-architecture lockfile inclusion enabled, merging packages")
        all_packages: dict[str, PackageEntry] = {}
        all_modules: dict[str, ModuleEntry] = {}
        for arch_entry in lockfile.arches:
            for pkg in arch_entry.packages:
                if pkg.url not in all_packages:
                    all_packages[pkg.url] = pkg
            for mod in arch_entry.module_metadata:
                key = f"{mod.name or ''}:{mod.stream or ''}:{mod.version or ''}"
                if key not in all_modules:
                    all_modules[key] = mod
        merged_packages = list(all_packages.values())
        merged_modules = list(all_modules.values())
        for arch_entry in lockfile.arches:
            arch_entry.packages = merged_packages
            if merged_modules:
                arch_entry.module_metadata = merged_modules

    @staticmethod
    def _detect_cross_arch_mismatches(lockfile: LockfileData) -> dict[str, dict[str, str]]:
        """
        Detect packages with different EVR versions across architectures.

        Arg(s):
            lockfile (LockfileData): Resolved lockfile with per-arch results.
        Return Value(s):
            dict[str, dict[str, str]]: Mapping of package_name to
                {arch: evr} for packages with differing versions.
                Empty if all versions are consistent.
        """
        pkg_versions: dict[str, dict[str, str]] = {}
        for arch_entry in lockfile.arches:
            for pkg in arch_entry.packages:
                if pkg.name and pkg.evr:
                    pkg_versions.setdefault(pkg.name, {})[arch_entry.arch] = pkg.evr

        mismatches: dict[str, dict[str, str]] = {}
        for name, arch_evrs in pkg_versions.items():
            if len(arch_evrs) < 2:
                continue
            unique_evrs = set(arch_evrs.values())
            if len(unique_evrs) > 1:
                mismatches[name] = arch_evrs
        return mismatches

    @staticmethod
    def _compute_version_pins(mismatches: dict[str, dict[str, str]]) -> list[str]:
        """
        Compute version-pinned DNF package specs from cross-arch mismatches.
        Picks the minimum (oldest) version for each package.

        Arg(s):
            mismatches (dict[str, dict[str, str]]): From _detect_cross_arch_mismatches.
        Return Value(s):
            list[str]: Version-pinned package specs for DNF
                (e.g., ["libeconf-0.4.1-5.el9"]).
        """
        pins: list[str] = []
        for name, arch_evrs in sorted(mismatches.items()):
            min_evr = pick_minimum_evr(list(arch_evrs.values()))
            pins.append(format_version_pin(name, min_evr))
        return pins

    @staticmethod
    def _format_mismatches(mismatches: dict[str, dict[str, str]]) -> str:
        """
        Format cross-arch mismatches for error messages.
        """
        parts = []
        for name, arch_evrs in sorted(mismatches.items()):
            versions = ", ".join(f"{arch}={evr}" for arch, evr in sorted(arch_evrs.items()))
            parts.append(f"{name} ({versions})")
        return "; ".join(parts)

    async def _resolve_with_reconciliation(
        self,
        repo_list: list[RepoEntry],
        arches: list[str],
        packages: list[str],
        image_pullspec: str | None,
        distgit_key: str,
        stage_num: int,
        reinstall_packages: list[str] | None = None,
        containerfile_path: str | None = None,
        upgrade_packages: list[str] | None = None,
    ) -> LockfileData | None:
        """
        Resolve a stage with cross-arch version reconciliation.

        First resolves normally, then checks for version mismatches
        across architectures. If found, re-resolves with version-pinned
        packages to force consistent versions.

        Arg(s):
            repo_list (list[RepoEntry]): Repository entries.
            arches (list[str]): Target architectures.
            packages (list[str]): Extra package names ($(cat ...) etc.).
            image_pullspec (str | None): Base image pullspec.
            distgit_key (str): Image identifier for logging.
            stage_num (int): Dockerfile stage number.
            reinstall_packages (list[str] | None): Base image packages to
                reinstall from repos into the lockfile.
            containerfile_path (str | None): Dockerfile path for upstream
                package extraction.
            upgrade_packages (list[str] | None): Base image packages to
                upgrade (from bare dnf/yum update commands).
        Return Value(s):
            LockfileData | None: Resolved lockfile with consistent
                versions, or None if no packages remain.
        """
        first_pass = await self._resolve_stage_with_retry(
            repo_list,
            arches,
            packages,
            image_pullspec,
            distgit_key,
            stage_num,
            reinstall_packages=reinstall_packages,
            containerfile_path=containerfile_path,
            upgrade_packages=upgrade_packages,
        )
        if not first_pass:
            return None

        mismatches = self._detect_cross_arch_mismatches(first_pass)
        if not mismatches:
            return first_pass

        self.logger.warning(
            f"{distgit_key}: stage {stage_num}: cross-arch version mismatches in "
            f"{len(mismatches)} packages: {sorted(mismatches.keys())}"
        )

        version_pins = self._compute_version_pins(mismatches)
        self.logger.debug(
            f"{distgit_key}: stage {stage_num}: re-resolving with {len(version_pins)} version pins: {version_pins}"
        )

        mismatched_names = set(mismatches.keys())
        pinned_packages = [p for p in packages if p not in mismatched_names] + version_pins
        pinned_reinstall = (
            [p for p in reinstall_packages if p not in mismatched_names] if reinstall_packages else reinstall_packages
        )
        pinned_upgrade = (
            [p for p in upgrade_packages if p not in mismatched_names] if upgrade_packages else upgrade_packages
        )

        try:
            second_pass = await self._resolve_stage_with_retry(
                repo_list,
                arches,
                pinned_packages,
                image_pullspec,
                distgit_key,
                stage_num,
                reinstall_packages=pinned_reinstall,
                containerfile_path=containerfile_path,
                upgrade_packages=pinned_upgrade,
            )
        except RuntimeError as e:
            raise RuntimeError(
                f"{distgit_key}: stage {stage_num}: cross-arch version reconciliation failed. "
                f"Version-pinned re-resolution error: {e}. "
                f"Mismatched packages: {self._format_mismatches(mismatches)}"
            ) from e

        if not second_pass:
            raise RuntimeError(
                f"{distgit_key}: stage {stage_num}: cross-arch version reconciliation failed. "
                f"Re-resolution returned no results. "
                f"Mismatched packages: {self._format_mismatches(mismatches)}"
            )

        remaining = self._detect_cross_arch_mismatches(second_pass)
        if remaining:
            raise RuntimeError(
                f"{distgit_key}: stage {stage_num}: cross-arch version reconciliation failed. "
                f"Mismatches persist after re-resolution: {self._format_mismatches(remaining)}"
            )

        self.logger.info(f"{distgit_key}: stage {stage_num}: cross-arch versions reconciled successfully")
        return second_pass

    async def _pin_missing_dockerfile_packages(
        self,
        result: LockfileData | None,
        pin_candidates: list[str],
        repo_list: list[RepoEntry],
        arches: list[str],
        image_pullspec: str,
        distgit_key: str,
        stage_num: int,
    ) -> LockfileData | None:
        """
        Pin Dockerfile packages that overlap with the base image but
        were not captured by the upgrade pass.

        These packages are already installed at the repo-latest version,
        so neither install (already present) nor upgrade (nothing newer)
        produced a lockfile entry. A targeted per-arch reinstall pins
        them at their installed version. No upgradePackages is set here
        to avoid EVR conflicts between reinstall and upgrade.

        Arg(s):
            result (LockfileData | None): Pass 1 result.
            pin_candidates (list[str]): Dockerfile install packages that
                overlap with the base image.
            repo_list (list[RepoEntry]): Repository entries.
            arches (list[str]): Target architectures.
            image_pullspec (str): Base image pullspec.
            distgit_key (str): Image identifier for logging.
            stage_num (int): Dockerfile stage number.
        Return Value(s):
            LockfileData | None: Merged result, or None if both
                passes produced nothing.
        """
        locked_by_arch: dict[str, set[str]] = (
            {entry.arch: {pkg.name for pkg in entry.packages} for entry in result.arches} if result else {}
        )

        missing_by_arch: dict[str, list[str]] = {
            arch: sorted(p for p in pin_candidates if p not in locked_by_arch.get(arch, set())) for arch in arches
        }
        missing_by_arch = {arch: pkgs for arch, pkgs in missing_by_arch.items() if pkgs}
        if not missing_by_arch:
            return result

        resolver_pullspec = ContainerImageHelper._proxy_pullspec(image_pullspec)
        pin_results: list[LockfileData] = []

        for arch, missing in missing_by_arch.items():
            self.logger.info(
                f"{distgit_key}: stage {stage_num}: pinning {len(missing)} Dockerfile "
                f"packages not captured by upgrade pass on {arch}: {missing}"
            )
            in_yaml = build_rpms_in_yaml(
                repo_list,
                [arch],
                missing,
                reinstall_packages=missing,
            )
            try:
                pin_result = await self._resolver.resolve(in_yaml, image_pullspec=resolver_pullspec)
            except RuntimeError as e:
                self.logger.warning(
                    f"{distgit_key}: stage {stage_num}: pin pass failed on {arch}, "
                    f"some Dockerfile packages may be unlocked: {e}"
                )
                continue

            if pin_result and any(ae.packages for ae in pin_result.arches):
                pinned = {pkg.name for ae in pin_result.arches for pkg in ae.packages}
                self.logger.info(
                    f"{distgit_key}: stage {stage_num}: pin pass locked "
                    f"{len(pinned)} packages on {arch}: {sorted(pinned)}"
                )
                pin_results.append(pin_result)

        if not pin_results:
            return result

        to_merge = [result] if result else []
        to_merge.extend(pin_results)
        return merge_lockfiles(to_merge)

    def _write_lockfile(self, lockfile: LockfileData | None, path: Path, distgit_key: str) -> None:
        """
        Write lockfile to disk. Writes an empty lockfile when lockfile is None.
        """
        if lockfile is None:
            self.logger.debug(f"{distgit_key}: no packages or updates, writing empty lockfile")
            lockfile = LockfileData()
        path.write_text(yaml.safe_dump(lockfile.model_dump(exclude_none=True), sort_keys=False))
        self.logger.info(f"{distgit_key}: lockfile written to {path}")

    def _templatize_baseurl(self, baseurl: str) -> str:
        """
        Replace any known architecture string in a baseurl with $basearch.

        Arg(s):
            baseurl (str): Concrete repo URL.
        Return Value(s):
            str: URL with the arch path component replaced by $basearch.
        """
        for arch in BREW_ARCHES:
            if f"/{arch}/" in baseurl:
                return baseurl.replace(f"/{arch}/", f"/{BASEARCH_VAR}/")
        return baseurl

    def _get_repoid_for_content_set(self, repo, repo_name: str, first_arch: str) -> str:
        """
        Derive a repoid that matches what cachi2 DNF options use.

        Arg(s):
            repo: Repo object from the Repos collection.
            repo_name (str): ocp-build-data repo name (fallback).
            first_arch (str): Architecture used to obtain the content_set name.
        Return Value(s):
            str: Repoid suitable for rpms.in.yaml (may contain $basearch).
        """
        try:
            content_set_id = repo.content_set(first_arch)
        except ValueError:
            content_set_id = None

        if content_set_id is None:
            return f"{repo_name}-{BASEARCH_VAR}"

        if not content_set_id:
            return repo_name

        if first_arch in content_set_id:
            return content_set_id.replace(first_arch, BASEARCH_VAR)
        return content_set_id

    def _build_repo_list(self, enabled_repos: set[str], arches: list[str]) -> list[RepoEntry]:
        """
        Build repo list from Repos object for rpms.in.yaml.

        Arg(s):
            enabled_repos (set[str]): Repo names to include.
            arches (list[str]): Target architectures.
        Return Value(s):
            list[RepoEntry]: Repository entries.
        """
        repo_list: list[RepoEntry] = []
        first_arch = arches[0]
        for repo_name in sorted(enabled_repos):
            try:
                repo = self.repos[repo_name]
            except ValueError:
                continue
            try:
                baseurl = repo.baseurl(repotype="unsigned", arch=first_arch)
            except ValueError:
                self.logger.warning(f"Repo {repo_name} has no baseurl for {first_arch}, skipping")
                continue

            arch_urls = set()
            for arch in arches:
                try:
                    arch_urls.add(repo.baseurl(repotype="unsigned", arch=arch))
                except ValueError:
                    pass

            if len(arch_urls) <= 1:
                baseurl_template = baseurl
            else:
                baseurl_template = self._templatize_baseurl(baseurl)

            repoid = self._get_repoid_for_content_set(repo, repo_name, first_arch)
            extra_options = dict(repo._data.conf.get("extra_options", {}))
            if repo.cs_optional:
                extra_options.setdefault("skip_if_unavailable", True)
            repo_list.append(RepoEntry(repoid=repoid, baseurl=baseurl_template, options=extra_options))
        return repo_list
