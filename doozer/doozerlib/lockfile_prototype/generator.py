"""
RPM lockfile generator using rpm-lockfile-prototype.

Orchestrates lockfile generation by analyzing Dockerfiles, building
rpms.in.yaml input configs, and delegating resolution to RpmResolver.
Container image interactions are delegated to ContainerImageHelper.
"""

import fnmatch
import logging
import re
import shlex
from pathlib import Path

import yaml
from artcommonlib import logutil
from artcommonlib.arch_util import BREW_ARCHES
from artcommonlib.exectools import cmd_gather_async
from dockerfile_parse import DockerfileParser

from doozerlib.image import ImageMetadata
from doozerlib.lockfile_prototype.constants import (
    BASEARCH_VAR,
    DEFAULT_RPM_LOCKFILE_NAME,
    DIGEST_PREFIX,
    DOCKERFILE_NAME,
    MAX_RESOLUTION_RETRIES,
)
from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.dockerfile_parser import analyze_dockerfile_stages, collect_stage_vars
from doozerlib.lockfile_prototype.lockfile_merger import merge_lockfiles
from doozerlib.lockfile_prototype.models import (
    ArchSpecificPackage,
    LockfileData,
    ModuleEntry,
    PackageEntry,
    RepoEntry,
    RpmsInConfig,
    StageAnalysis,
    StageInfo,
)
from doozerlib.lockfile_prototype.resolver import RpmResolver
from doozerlib.lockfile_prototype.shell_parser import resolve_bash_expansion
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


def build_rpms_in_yaml(
    repos: list[RepoEntry],
    arches: list[str],
    packages: list[str],
    arch_specific_packages: dict[str, list[str]] | None = None,
    reinstall_packages: list[str] | None = None,
    upgrade_packages: list[str] | None = None,
    module_enable: list[str] | None = None,
) -> RpmsInConfig:
    """
    Build the rpms.in.yaml config for rpm-lockfile-prototype.

    Arg(s):
        repos (list[RepoEntry]): Repository entries.
        arches (list[str]): Target architectures.
        packages (list[str]): Common package names for all arches.
        arch_specific_packages (dict[str, list[str]] | None): Per-arch packages.
        reinstall_packages (list[str] | None): Installed packages to reinstall
            from repos (ensures they appear in the lockfile).
        upgrade_packages (list[str] | None): Packages to upgrade.
        module_enable (list[str] | None): Module streams to enable
            (e.g., ["nodejs:18", "python36:3.6"]).
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

        analysis, entries = self._analyze_dockerfile(dockerfile_path, dest_dir)
        analysis = await self._enrich_with_cat_packages(analysis, entries, image_meta.distgit_key)
        self._enrich_with_module_install_packages(analysis, image_meta.distgit_key)
        stages = self._select_stages_to_resolve(analysis)

        out_file_path = dest_dir / filename
        if not stages:
            self._write_lockfile(None, out_file_path, image_meta.distgit_key)
            return

        stage_lockfiles = await self._resolve_all_stages(
            stages, analysis, repo_list, arches, image_meta.distgit_key, dest_dir=dest_dir
        )
        lockfile = self._assemble_lockfile(stage_lockfiles, image_meta)
        self._write_lockfile(lockfile, out_file_path, image_meta.distgit_key)

    def _analyze_dockerfile(self, dockerfile_path: Path, source_dir: Path) -> tuple[StageAnalysis, list[dict]]:
        """
        Parse Dockerfile and return per-stage package analysis plus raw entries.
        """
        stages, entries = analyze_dockerfile_stages(dockerfile_path, source_dir=source_dir)
        return StageAnalysis(stages=stages), entries

    async def _enrich_with_cat_packages(
        self,
        analysis: StageAnalysis,
        entries: list[dict],
        distgit_key: str,
    ) -> StageAnalysis:
        """
        Resolve $(cat /filepath) patterns in Dockerfile install commands
        and merge additional packages into the analysis.
        """
        cat_packages = await self._resolve_cat_packages(entries, self.downstream_parents)
        for stage_num, pkgs in cat_packages.items():
            if stage_num < len(analysis.stages):
                stage = analysis.stages[stage_num]
                stage.packages = sorted(set(stage.packages + pkgs))
                self.logger.debug(
                    f"{distgit_key}: stage {stage_num}: added {len(pkgs)} packages from $(cat ...) resolution"
                )
        return analysis

    def _enrich_with_module_install_packages(self, analysis: StageAnalysis, distgit_key: str) -> None:
        """
        Convert module install specs (from dnf module install) to
        @module:stream package entries that rpm-lockfile-prototype
        understands. For example, nodejs:18/development becomes
        @nodejs:18/development in the packages list.
        """
        for stage in analysis.stages:
            if not stage.module_specs:
                continue

            install_specs = [s for s in stage.module_specs if "/" in s]
            if not install_specs:
                continue

            added: list[str] = []
            for spec in install_specs:
                at_spec = f"@{spec}"
                if at_spec not in stage.packages:
                    stage.packages = sorted(set(stage.packages) | {at_spec})
                    added.append(at_spec)

            if added:
                self.logger.info(f"{distgit_key}: added module install packages: {added}")

    async def _resolve_cat_packages(
        self,
        entries: list[dict],
        downstream_parents: list[str],
    ) -> dict[int, list[str]]:
        """
        Find $(cat /filepath) patterns in Dockerfile install commands and
        resolve them by reading file contents from base images via podman.

        Arg(s):
            entries (list[dict]): Parsed Dockerfile structure entries.
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
        if current_runs:
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
        First checks if the file exists directly in the source tree.
        If not, parses Dockerfiles for RUN commands that generate the
        file (e.g. ``echo ... > /filepath`` or ``sed ... > /filepath``).

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

        content = self._extract_generated_file_content(parent_dir, container_path)
        if content:
            self.logger.info(f"Resolved $(cat {container_path}) from parent Dockerfile RUN command")
        return content

    def _extract_generated_file_content(self, parent_dir: Path, container_path: str) -> str:
        """
        Parse Dockerfiles in parent_dir for RUN commands that write to
        container_path. Resolves ARG/ENV variables to reconstruct the
        file content.

        Supports patterns like:
            echo "pkg1 pkg2" > /filepath
            sed 's/.../.../g' <<<"..." > /filepath
            printf "..." > /filepath

        Arg(s):
            parent_dir (Path): Parent image's build directory.
            container_path (str): Absolute path inside the container.
        Return Value(s):
            str: Reconstructed file content, or empty string.
        """
        escaped_path = re.escape(container_path)
        redirect_re = re.compile(rf">\s*{escaped_path}\s*$", re.MULTILINE)

        for df_name in ("Dockerfile", "Dockerfile.base", "Containerfile"):
            df_path = parent_dir / df_name
            if not df_path.is_file():
                continue

            dfp = DockerfileParser(fileobj=open(df_path, "rb"))
            entries = dfp.structure

            variables: dict[str, str] = {}
            for entry in entries:
                if entry["instruction"] in ("ARG", "ENV"):
                    variables = collect_stage_vars([entry], inherited_vars=variables)
                elif entry["instruction"] == "RUN":
                    run_body = entry["value"]
                    if not redirect_re.search(run_body):
                        continue
                    # Extract the content being redirected
                    # Handle here-string: sed 's/x/y/g' <<<"content" > /path
                    heredoc_match = re.search(r'<<<\s*"([^"]*)"', run_body)
                    if heredoc_match:
                        raw_content = heredoc_match.group(1)
                        resolved = resolve_bash_expansion(raw_content, variables)
                        # Handle sed substitution if present
                        sed_match = re.search(r"sed\s+'s/(.)/(.)/g'", run_body)
                        if sed_match:
                            resolved = resolved.replace(sed_match.group(1), sed_match.group(2))
                        return resolved
                    # Handle echo/printf: echo "content" > /path
                    echo_match = re.search(r'(?:echo|printf)\s+["\']?([^"\'>\n]+)["\']?\s*>', run_body)
                    if echo_match:
                        raw_content = echo_match.group(1).strip()
                        return resolve_bash_expansion(raw_content, variables)

        return ""

    def _select_stages_to_resolve(self, analysis: StageAnalysis) -> list[tuple[int, list[str]]]:
        """
        Filter to stages that have packages, arch-specific packages, updates,
        or builddep packages.
        """
        return [
            (stage_num, stage.packages)
            for stage_num, stage in enumerate(analysis.stages)
            if stage.packages
            or stage.arch_packages
            or stage.has_update
            or stage.builddep_packages
            or stage.module_specs
        ]

    async def _resolve_all_stages(
        self,
        stages: list[tuple[int, list[str]]],
        analysis: StageAnalysis,
        repo_list: list[RepoEntry],
        arches: list[str],
        distgit_key: str,
        dest_dir: Path | None = None,
    ) -> list[LockfileData]:
        """
        Resolve RPM packages for each Dockerfile stage.
        """
        stage_lockfiles: list[LockfileData] = []
        final_stage_num = len(analysis.stages) - 1

        for stage_num, packages in stages:
            image_pullspec = await self._determine_stage_pullspec(stage_num, distgit_key)

            stage_info = analysis.stages[stage_num]
            if stage_info.builddep_packages and dest_dir:
                builddep_pkgs = await self._resolve_builddep_packages(
                    stage_info.builddep_packages, dest_dir, distgit_key
                )
                extra = [p for p in builddep_pkgs if p not in packages]
                if extra:
                    packages = list(packages) + extra

            is_update_only = stage_info.has_update and not packages
            if is_update_only:
                packages, upgrade_targets, image_pullspec = await self._handle_update_only_stage(
                    stage_num, image_pullspec, distgit_key
                )
                if not packages:
                    continue
            else:
                upgrade_targets = await self._resolve_bare_update_targets(
                    stage_num, analysis.stages[stage_num], distgit_key
                )

            has_bare_update = stage_info.has_update and not stage_info.update_targets
            if has_bare_update and not is_update_only and image_pullspec:
                base_pkgs = await self._get_base_image_packages(stage_num, image_pullspec, distgit_key)
                if base_pkgs:
                    upgrade_targets = list(base_pkgs)
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: {len(upgrade_targets)} base image "
                        "packages added as upgrade targets for bare update"
                    )

            reinstall_pkgs: list[str] | None = None
            strippable: set[str] | None = None
            if stage_num == final_stage_num and not is_update_only and has_bare_update:
                if image_pullspec and packages:
                    # Bare update + explicit installs in final stage: reinstall
                    # the Dockerfile packages so they appear in the lockfile
                    # even when already installed in the base image. Base image
                    # packages use upgrade semantics via upgrade_targets (set
                    # above), so we intentionally do NOT reinstall them here —
                    # a Dockerfile package that's also a base image package
                    # would otherwise get reinstalled at its currently-installed
                    # EVR, which can win over the separate upgrade request and
                    # silently keep an old version instead of upgrading.
                    reinstall_pkgs = [p for p in packages if p not in base_pkgs]
                    strippable = set()
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: {len(reinstall_pkgs)} Dockerfile "
                        "packages will be reinstalled into lockfile (bare update stage)"
                    )
            elif stage_num == final_stage_num and not is_update_only and not has_bare_update:
                if image_pullspec:
                    # --image mode: pass base image packages as reinstallPackages
                    # so they appear in the lockfile at repo versions. base.reinstall()
                    # handles missing/renamed packages gracefully (warns, skips).
                    # Skipped when the stage has a bare update (e.g. microdnf update -y):
                    # reinstall would pin base image versions and override upgrade semantics,
                    # preventing the update from picking up latest RPMs from repos.
                    base_pkgs = await self._get_base_image_packages(stage_num, image_pullspec, distgit_key)
                    if base_pkgs:
                        reinstall_pkgs = list(base_pkgs)
                        strippable = set(base_pkgs) - set(packages) - set(upgrade_targets)
                        self.logger.info(
                            f"{distgit_key}: stage {stage_num}: {len(reinstall_pkgs)} base image "
                            "packages will be reinstalled into lockfile"
                        )
                else:
                    # No base image available — resolve in bare mode with
                    # base image packages added to the install list for
                    # conflict detection.
                    base_pkgs = await self._get_base_image_packages(stage_num, None, distgit_key)
                    if base_pkgs:
                        extra = [p for p in base_pkgs if p not in packages]
                        if extra:
                            strippable = set(extra)
                            packages = packages + extra
            elif stage_num != final_stage_num and not is_update_only:
                # Non-final stage (builder): reinstall the Dockerfile packages
                # so they appear in the lockfile even when already installed
                # on some architectures, and add base image packages to the
                # install list for conflict detection.
                #
                # When the builder image RHEL version differs from the repos
                # (e.g. el8 builder with el9 repos), the stage's own packages
                # can't be soundly resolved either — the only repos we have
                # are for the wrong RHEL major, so skip locking this stage's
                # packages entirely rather than pinning a mismatched RPM.
                if self._has_rhel_version_mismatch(stage_num, repo_list, distgit_key):
                    continue
                if image_pullspec:
                    reinstall_pkgs = list(packages)
                base_pkgs = await self._get_base_image_packages(stage_num, image_pullspec, distgit_key)
                if base_pkgs:
                    extra = [p for p in base_pkgs if p not in packages]
                    if extra:
                        packages = packages + extra
                        self.logger.info(
                            f"{distgit_key}: stage {stage_num}: {len(extra)} base image "
                            "packages added to install list for conflict detection"
                        )

            enable_only = [s.split("/")[0] for s in stage_info.module_specs] if stage_info.module_specs else None

            all_packages = set(packages + [p for p in upgrade_targets if p not in packages])
            stripped_tracker: set[str] = set()
            result = await self._resolve_with_reconciliation(
                repo_list,
                arches,
                sorted(all_packages),
                stage_info.arch_packages,
                upgrade_targets,
                image_pullspec,
                distgit_key,
                stage_num,
                module_enable=enable_only,
                reinstall_packages=reinstall_pkgs,
                strippable_packages=strippable,
                stripped_tracker=stripped_tracker,
            )
            if result:
                # Any stage shape can inject base image packages (upgrade
                # targets, reinstall packages, conflict-detection extras)
                # that include arch-specific packages (e.g. dmidecode,
                # x86-only) and get stripped during resolution. Try to
                # recover them per-arch rather than dropping silently.
                if stripped_tracker and image_pullspec:
                    recovered = await self._recover_stripped_per_arch(
                        repo_list,
                        arches,
                        stripped_tracker,
                        image_pullspec,
                        distgit_key,
                        stage_num,
                    )
                    if recovered:
                        result = merge_lockfiles([result, recovered])
                stage_lockfiles.append(result)

        return stage_lockfiles

    async def _recover_stripped_per_arch(
        self,
        repo_list: list[RepoEntry],
        arches: list[str],
        stripped: set[str],
        image_pullspec: str,
        distgit_key: str,
        stage_num: int,
    ) -> LockfileData | None:
        """
        Try to recover stripped packages by resolving them per-arch.

        Packages like dmidecode only exist on some arches. Multi-arch
        resolution strips them entirely. This method tries each arch
        individually and merges successful results.
        """
        resolver_pullspec = ContainerImageHelper._proxy_pullspec(image_pullspec)
        recovered: list[LockfileData] = []
        for arch in arches:
            in_yaml = build_rpms_in_yaml(
                repo_list,
                [arch],
                sorted(stripped),
                upgrade_packages=sorted(stripped),
            )
            try:
                result = await self._resolver.resolve(in_yaml, image_pullspec=resolver_pullspec)
                if result and any(ae.packages for ae in result.arches):
                    recovered.append(result)
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: recovered {len(stripped)} stripped packages for {arch}"
                    )
            except RuntimeError:
                self.logger.debug(
                    f"{distgit_key}: stage {stage_num}: stripped packages not available for {arch}, skipping"
                )
        if not recovered:
            return None
        return merge_lockfiles(recovered) if len(recovered) > 1 else recovered[0]

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
        if ":" not in pullspec:
            return None
        tag = pullspec.split("@")[0].split(":")[-1]
        m = re.search(r"(?:rhel|ubi|centos|scos)-?(\d+)", tag)
        if m:
            return int(m.group(1))
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

    async def _handle_update_only_stage(
        self, stage_num: int, image_pullspec: str | None, distgit_key: str
    ) -> tuple[list[str], list[str], str | None]:
        """
        For update-only stages (no install commands), query the base
        image for installed packages and return them as upgrade targets.
        Falls back to parent lockfile data if the image is unreachable.

        Return Value(s):
            tuple: (packages, upgrade_targets, pullspec) — empty
                packages means skip this stage. upgrade_targets contains
                all installed package names so DNF uses upgrade semantics
                and respects existing dependency constraints.
        """
        packages = await self._get_base_image_packages(stage_num, image_pullspec, distgit_key)
        if not packages:
            self.logger.warning(
                f"{distgit_key}: stage {stage_num} is update-only but no package data available, skipping"
            )
            return [], [], None
        return packages, list(packages), image_pullspec

    async def _resolve_bare_update_targets(self, stage_num: int, stage_info: StageInfo, distgit_key: str) -> list[str]:
        """
        Return explicit update targets from the Dockerfile, or empty
        list for bare updates (which are stripped after lockfile generation).

        Arg(s):
            stage_num (int): Dockerfile stage number.
            stage_info (StageInfo): Parsed stage info with update flags.
            distgit_key (str): Image identifier for logging.
        Return Value(s):
            list[str]: Explicit update target packages, or empty list.
        """
        if not stage_info.has_update:
            return stage_info.update_targets
        if stage_info.update_targets:
            return stage_info.update_targets
        # Bare yum/dnf update (no named packages). Return empty here;
        # _resolve_all_stages expands upgrade targets from the base
        # image when --image mode is available.
        self.logger.info(f"{distgit_key}: stage {stage_num}: bare update detected")
        return []

    @staticmethod
    def _is_builddep_requirement(req: str) -> bool:
        """
        Return True if a requirement from rpm -qpR looks like a real
        package name (not a virtual provide, rpmlib dep, or file path).
        """
        if not req:
            return False
        if req.startswith("/") or req.startswith("rpmlib(") or req.startswith("config("):
            return False
        if "(" in req:
            return False
        return True

    async def _resolve_builddep_packages(
        self,
        builddep_patterns: list[str],
        dest_dir: Path,
        distgit_key: str,
    ) -> list[str]:
        """
        Resolve dnf builddep patterns to concrete package names by
        finding matching SRPMs in the source directory and extracting
        their BuildRequires.

        Arg(s):
            builddep_patterns (list[str]): Glob patterns from dnf builddep
                commands (e.g., ["pkcs11-helper*", "openvpn*"]).
            dest_dir (Path): Build source directory containing SRPMs.
            distgit_key (str): Image identifier for logging.
        Return Value(s):
            list[str]: Deduplicated package names from BuildRequires.
        """
        resolved: set[str] = set()

        for pattern in builddep_patterns:
            if pattern.endswith(".spec"):
                self.logger.warning(
                    f"{distgit_key}: builddep target '{pattern}' is a spec file — "
                    "only SRPMs are supported for BuildRequires extraction; "
                    "spec files cannot be parsed reliably due to macro resolution"
                )
                continue

            srpm_pattern = pattern if pattern.endswith(".src.rpm") else f"{pattern}.src.rpm"
            matching_srpms = [
                f
                for f in dest_dir.iterdir()
                if f.is_file() and f.name.endswith(".src.rpm") and fnmatch.fnmatch(f.name, srpm_pattern)
            ]

            if not matching_srpms:
                self.logger.warning(
                    f"{distgit_key}: no SRPM matching '{pattern}' found in {dest_dir}, "
                    "builddep packages will not be included in lockfile"
                )
                continue

            for srpm_path in matching_srpms:
                self.logger.info(f"{distgit_key}: extracting BuildRequires from {srpm_path.name}")
                try:
                    rc, stdout, stderr = await cmd_gather_async(["rpm", "-qpR", str(srpm_path)], check=False)
                    if rc != 0:
                        self.logger.warning(f"{distgit_key}: rpm -qpR {srpm_path.name} failed (rc={rc}): {stderr}")
                        continue
                    for line in stdout.strip().splitlines():
                        req = line.strip().split()[0] if line.strip() else ""
                        if self._is_builddep_requirement(req):
                            resolved.add(req)
                except Exception as exc:
                    self.logger.warning(f"{distgit_key}: failed to extract BuildRequires from {srpm_path.name}: {exc}")

        if resolved:
            self.logger.info(f"{distgit_key}: resolved {len(resolved)} builddep packages: {sorted(resolved)}")
        return sorted(resolved)

    def _build_resolve_config(
        self,
        repo_list: list[RepoEntry],
        arches: list[str],
        packages: list[str],
        arch_pkgs: dict[str, list[str]],
        update_targets: list[str],
        reinstall: list[str],
        promote_reinstall_to_upgrade: bool,
        image_pullspec: str | None,
        module_enable: list[str] | None,
    ) -> RpmsInConfig:
        """
        Build the rpms.in.yaml config for a resolve attempt.
        """
        upgrade_extras = reinstall if promote_reinstall_to_upgrade else []
        effective_upgrade = list(set(update_targets + upgrade_extras)) if image_pullspec else None
        return build_rpms_in_yaml(
            repo_list,
            arches,
            packages,
            arch_specific_packages=arch_pkgs,
            reinstall_packages=reinstall if image_pullspec else None,
            upgrade_packages=effective_upgrade,
            module_enable=module_enable,
        )

    @staticmethod
    def _strip_missing_packages(
        missing: set[str],
        remaining_packages: list[str],
        remaining_update_targets: list[str],
        remaining_reinstall: list[str],
        arch_pkgs: dict[str, list[str]],
    ) -> int:
        """
        Remove missing packages from all lists. Returns count of packages removed.
        """
        prev_count = (
            len(remaining_packages)
            + sum(len(v) for v in arch_pkgs.values())
            + len(remaining_update_targets)
            + len(remaining_reinstall)
        )
        remaining_packages[:] = [p for p in remaining_packages if p not in missing]
        remaining_update_targets[:] = [p for p in remaining_update_targets if p not in missing]
        remaining_reinstall[:] = [p for p in remaining_reinstall if p not in missing]
        for arch in list(arch_pkgs.keys()):
            arch_pkgs[arch] = [p for p in arch_pkgs[arch] if p not in missing]
            if not arch_pkgs[arch]:
                del arch_pkgs[arch]
        new_count = (
            len(remaining_packages)
            + sum(len(v) for v in arch_pkgs.values())
            + len(remaining_update_targets)
            + len(remaining_reinstall)
        )
        return prev_count - new_count

    async def _resolve_stage_with_retry(
        self,
        repo_list: list[RepoEntry],
        arches: list[str],
        packages: list[str],
        arch_pkgs: dict[str, list[str]],
        update_targets: list[str],
        image_pullspec: str | None,
        distgit_key: str,
        stage_num: int,
        module_enable: list[str] | None = None,
        reinstall_packages: list[str] | None = None,
        strippable_packages: set[str] | None = None,
        stripped_tracker: set[str] | None = None,
    ) -> LockfileData | None:
        """
        Resolve a single stage, retrying after removing unavailable packages.

        Arg(s):
            strippable_packages (set[str] | None): Packages that may be
                silently removed during retries (e.g. base image packages
                added for conflict detection). If a missing package is NOT
                in this set, it is a required Dockerfile package and the
                error is raised immediately.

        Return Value(s):
            LockfileData | None: Lockfile data, or None if all packages filtered out.
        """
        remaining_packages = list(packages)
        remaining_update_targets = list(update_targets)
        arch_pkgs = dict(arch_pkgs)

        # rpm-lockfile-prototype uses skopeo to pull the base image rpmdb.
        # brew.registry.redhat.io requires auth that skopeo may not have;
        # use the no-auth registry proxy instead.
        resolver_pullspec = ContainerImageHelper._proxy_pullspec(image_pullspec) if image_pullspec else None

        # When reinstall_packages comes from the base image (final stage),
        # also pass them as upgrade targets. base.reinstall() raises
        # PackagesNotAvailableError when the installed version isn't in
        # the configured repos — but rpm-lockfile-prototype swallows that
        # error when the package is also in upgradePackages (the upgrade
        # provides a replacement version).
        # For builder stages (strippable_packages is None), reinstall
        # packages are Dockerfile packages that may not be installed in
        # the base image — adding them to upgradePackages would cause
        # PackagesNotInstalledError.
        remaining_reinstall = list(reinstall_packages) if reinstall_packages else []
        promote_reinstall_to_upgrade = strippable_packages is not None
        retries_exhausted = False

        for _attempt in range(MAX_RESOLUTION_RETRIES):
            in_yaml = self._build_resolve_config(
                repo_list,
                arches,
                remaining_packages,
                arch_pkgs,
                remaining_update_targets,
                remaining_reinstall,
                promote_reinstall_to_upgrade,
                image_pullspec,
                module_enable,
            )

            try:
                mode = "image" if resolver_pullspec else "bare"
                total = len(remaining_packages) + sum(len(v) for v in arch_pkgs.values()) + len(remaining_reinstall)
                self.logger.info(f"{distgit_key}: stage {stage_num}: resolving {total} packages in {mode} mode")
                return await self._resolver.resolve(in_yaml, image_pullspec=resolver_pullspec)
            except RuntimeError as e:
                missing = RpmResolver.parse_missing_packages(str(e))
                if not missing:
                    raise
                if strippable_packages is not None:
                    required_missing = missing - strippable_packages
                    if required_missing:
                        self.logger.warning(
                            f"{distgit_key}: stage {stage_num}: required Dockerfile packages not available "
                            f"in configured repos, stripping: {sorted(required_missing)}"
                        )
                reinstall_only = missing & set(remaining_reinstall)
                fully_missing = missing - reinstall_only
                removed = 0
                if reinstall_only:
                    remaining_reinstall[:] = [p for p in remaining_reinstall if p not in reinstall_only]
                    removed += len(reinstall_only)
                    self.logger.info(
                        f"{distgit_key}: stage {stage_num}: stripped from reinstall only "
                        f"(keeping in install/upgrade): {sorted(reinstall_only)}"
                    )
                if fully_missing:
                    removed += self._strip_missing_packages(
                        fully_missing,
                        remaining_packages,
                        remaining_update_targets,
                        remaining_reinstall,
                        arch_pkgs,
                    )
                if not removed:
                    raise
                if stripped_tracker is not None:
                    stripped_tracker.update(missing)
                self.logger.warning(
                    f"{distgit_key}: stage {stage_num}: retrying without unavailable packages: {sorted(missing)}"
                )
                if not remaining_packages and not arch_pkgs and not remaining_reinstall:
                    self.logger.warning(
                        f"{distgit_key}: stage {stage_num}: no packages remaining after filtering, skipping"
                    )
                    return None
                if strippable_packages is not None:
                    required_reinstall = [p for p in remaining_reinstall if p not in strippable_packages]
                    if not required_reinstall and remaining_reinstall:
                        self.logger.info(
                            f"{distgit_key}: stage {stage_num}: all {len(remaining_reinstall)} "
                            "remaining reinstall packages are optional, skipping retries"
                        )
                        break
        else:
            retries_exhausted = True
        if strippable_packages is not None:
            required_pkgs = set(remaining_packages) - strippable_packages
            dropped = [p for p in remaining_reinstall if p not in required_pkgs]
            remaining_reinstall = [p for p in remaining_reinstall if p in required_pkgs]
            if retries_exhausted:
                self.logger.warning(
                    f"{distgit_key}: stage {stage_num}: exceeded {MAX_RESOLUTION_RETRIES} retries, "
                    f"dropped {len(dropped)} optional reinstall packages, "
                    f"keeping {len(remaining_reinstall)} required"
                )
        else:
            self.logger.warning(
                f"{distgit_key}: stage {stage_num}: exceeded {MAX_RESOLUTION_RETRIES} retries, "
                "continuing without reinstall packages"
            )
            remaining_reinstall.clear()
        # Disable reinstall→upgrade promotion for the fallback — reinstall
        # packages that aren't actually installed would cause
        # PackagesNotInstalledError from DNF if promoted. Upgrade targets
        # (base image packages) are left as-is: _resolve_fallback's own
        # strip-and-retry loop already handles any that turn out to be
        # unresolvable (e.g. arch-mismatched), so there's no need to
        # blindly drop all of them — doing so would strand packages that
        # are both a Dockerfile package and a base image package (only
        # reachable via upgrade, since they're excluded from reinstall
        # above) with no way to appear in the lockfile at all.
        promote_reinstall_to_upgrade = False
        self.upgrades_dropped = not remaining_update_targets

        return await self._resolve_fallback(
            repo_list,
            arches,
            remaining_packages,
            arch_pkgs,
            remaining_update_targets,
            remaining_reinstall,
            promote_reinstall_to_upgrade,
            image_pullspec,
            resolver_pullspec,
            module_enable,
            distgit_key,
            stage_num,
            stripped_tracker,
        )

    async def _resolve_fallback(
        self,
        repo_list: list[RepoEntry],
        arches: list[str],
        remaining_packages: list[str],
        arch_pkgs: dict[str, list[str]],
        remaining_update_targets: list[str],
        remaining_reinstall: list[str],
        promote_reinstall_to_upgrade: bool,
        image_pullspec: str | None,
        resolver_pullspec: str | None,
        module_enable: list[str] | None,
        distgit_key: str,
        stage_num: int,
        stripped_tracker: set[str] | None,
    ) -> LockfileData | None:
        """
        Last-resort resolve after the main retry loop is exhausted.

        Can still hit packages that are missing for a subset of arches
        (e.g. base image packages like dmidecode that only exist on
        x86_64) — keep stripping until it resolves instead of letting a
        single unguarded call blow up the whole rebase.
        """
        for _attempt in range(MAX_RESOLUTION_RETRIES):
            in_yaml = self._build_resolve_config(
                repo_list,
                arches,
                remaining_packages,
                arch_pkgs,
                remaining_update_targets,
                remaining_reinstall,
                promote_reinstall_to_upgrade,
                image_pullspec,
                module_enable,
            )
            try:
                return await self._resolver.resolve(in_yaml, image_pullspec=resolver_pullspec)
            except RuntimeError as e:
                missing = RpmResolver.parse_missing_packages(str(e))
                if not missing:
                    raise
                removed = self._strip_missing_packages(
                    missing, remaining_packages, remaining_update_targets, remaining_reinstall, arch_pkgs
                )
                if not removed:
                    raise
                if stripped_tracker is not None:
                    stripped_tracker.update(missing)
                self.logger.warning(
                    f"{distgit_key}: stage {stage_num}: fallback retry without unavailable packages: {sorted(missing)}"
                )
                if not remaining_packages and not arch_pkgs and not remaining_reinstall:
                    self.logger.warning(
                        f"{distgit_key}: stage {stage_num}: no packages remaining after fallback filtering, skipping"
                    )
                    return None
        raise RuntimeError(f"{distgit_key}: stage {stage_num}: exceeded {MAX_RESOLUTION_RETRIES} fallback retries")

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
    def _detect_partial_arch_upgrades(
        lockfile: LockfileData,
        arches: list[str],
        update_targets: list[str],
        already_stripped: set[str],
    ) -> set[str]:
        """
        Detect upgrade targets that appear in the lockfile for only a
        subset of architectures.

        When an errata update is available for some arches but not others,
        rpm-lockfile-prototype silently upgrades only the arches that have
        the newer version. The package appears in the lockfile for those
        arches but not the rest, causing a version mismatch in the built
        image (upgraded arches get the lockfile version, others keep the
        base image version).

        Packages already in ``already_stripped`` are excluded — they were
        handled by the retry/recovery path (arch-specific packages like
        dmidecode that caused resolution errors).

        Arg(s):
            lockfile (LockfileData): Resolved lockfile with per-arch results.
            arches (list[str]): All target architectures.
            update_targets (list[str]): Packages that are upgrade targets.
            already_stripped (set[str]): Packages already stripped by the
                retry loop (handled separately by per-arch recovery).
        Return Value(s):
            set[str]: Package names to strip from the lockfile.
        """
        if len(arches) < 2 or not update_targets:
            return set()

        update_set = set(update_targets)
        pkg_arch_count: dict[str, int] = {}
        for arch_entry in lockfile.arches:
            for pkg in arch_entry.packages:
                if pkg.name and pkg.name in update_set:
                    pkg_arch_count[pkg.name] = pkg_arch_count.get(pkg.name, 0) + 1

        partial: set[str] = set()
        for name, count in pkg_arch_count.items():
            if count < len(arches) and name not in already_stripped:
                partial.add(name)
        return partial

    @staticmethod
    def _strip_packages_from_lockfile(lockfile: LockfileData, names: set[str]) -> None:
        """
        Remove packages by name from all architecture entries in the
        lockfile.

        Arg(s):
            lockfile (LockfileData): Lockfile to modify in place.
            names (set[str]): Package names to remove.
        """
        for arch_entry in lockfile.arches:
            arch_entry.packages = [p for p in arch_entry.packages if p.name not in names]

    async def _resolve_with_reconciliation(
        self,
        repo_list: list[RepoEntry],
        arches: list[str],
        packages: list[str],
        arch_pkgs: dict[str, list[str]],
        update_targets: list[str],
        image_pullspec: str | None,
        distgit_key: str,
        stage_num: int,
        module_enable: list[str] | None = None,
        reinstall_packages: list[str] | None = None,
        strippable_packages: set[str] | None = None,
        stripped_tracker: set[str] | None = None,
    ) -> LockfileData | None:
        """
        Resolve a stage with cross-arch version reconciliation.

        First resolves normally, then checks for version mismatches
        across architectures. If found, re-resolves with version-pinned
        packages to force consistent versions.

        Arg(s):
            repo_list (list[RepoEntry]): Repository entries.
            arches (list[str]): Target architectures.
            packages (list[str]): Package names to install.
            arch_pkgs (dict[str, list[str]]): Per-arch package overrides.
            update_targets (list[str]): Packages to upgrade.
            image_pullspec (str | None): Base image pullspec.
            distgit_key (str): Image identifier for logging.
            stage_num (int): Dockerfile stage number.
            module_enable (list[str] | None): Module streams to enable.
            reinstall_packages (list[str] | None): Base image packages to
                reinstall from repos into the lockfile.
            strippable_packages (set[str] | None): Packages that may be
                silently removed during retries (conflict detection packages).
        Return Value(s):
            LockfileData | None: Resolved lockfile with consistent
                versions, or None if no packages remain.
        """
        first_pass = await self._resolve_stage_with_retry(
            repo_list,
            arches,
            packages,
            arch_pkgs,
            update_targets,
            image_pullspec,
            distgit_key,
            stage_num,
            module_enable=module_enable,
            reinstall_packages=reinstall_packages,
            strippable_packages=strippable_packages,
            stripped_tracker=stripped_tracker,
        )
        if not first_pass:
            return None

        partial_arch = self._detect_partial_arch_upgrades(first_pass, arches, update_targets, stripped_tracker or set())
        if partial_arch:
            self.logger.warning(
                f"{distgit_key}: stage {stage_num}: stripping {len(partial_arch)} "
                f"partial-arch upgrade targets to prevent cross-arch version "
                f"mismatch: {sorted(partial_arch)}"
            )
            self._strip_packages_from_lockfile(first_pass, partial_arch)

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

        pinned_packages = list(packages) + version_pins
        pinned_names = set(mismatches.keys())
        excluded = pinned_names | partial_arch
        pinned_update_targets = [t for t in update_targets if t not in excluded]
        pinned_reinstall = (
            [p for p in reinstall_packages if p not in pinned_names] if reinstall_packages else reinstall_packages
        )

        try:
            second_pass = await self._resolve_stage_with_retry(
                repo_list,
                arches,
                pinned_packages,
                arch_pkgs,
                pinned_update_targets,
                image_pullspec,
                distgit_key,
                stage_num,
                module_enable=module_enable,
                reinstall_packages=pinned_reinstall,
                strippable_packages=strippable_packages,
            )
        except RuntimeError:
            self.logger.warning(
                f"{distgit_key}: stage {stage_num}: version-pinned re-resolution failed, "
                "using original result with version mismatches"
            )
            return first_pass

        if not second_pass:
            return first_pass

        remaining = self._detect_cross_arch_mismatches(second_pass)
        if remaining:
            self.logger.warning(
                f"{distgit_key}: stage {stage_num}: {len(remaining)} mismatches persist after "
                "re-resolution, using original result"
            )
            return first_pass

        if partial_arch:
            self._strip_packages_from_lockfile(second_pass, partial_arch)
        self.logger.info(f"{distgit_key}: stage {stage_num}: cross-arch versions reconciled successfully")
        return second_pass

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
