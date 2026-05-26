"""
Rebaser integration points for the rpm-lockfile-prototype backend.

Called from KonfluxRebaser during the rebase step to generate lockfiles
and apply Dockerfile transforms that make hermetic builds work.
"""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from doozerlib.lockfile_prototype.constants import DEFAULT_RPM_LOCKFILE_NAME
from doozerlib.lockfile_prototype.dockerfile_transforms import (
    strip_bare_updates,
    strip_bare_updates_from_scripts,
    strip_reinstall_commands,
)
from doozerlib.lockfile_prototype.generator import RpmLockfilePrototypeGenerator
from doozerlib.lockfile_prototype.resolver import RpmResolver


async def generate_lockfile(
    metadata,
    dest_dir: Path,
    repos,
    base_dir: Path,
    downstream_parents: list[str] | None = None,
    parent_members: list | None = None,
    shared_dnf_cache: TemporaryDirectory | None = None,
    logger: logging.Logger | None = None,
) -> TemporaryDirectory | None:
    """
    Generate an RPM lockfile using the rpm-lockfile-prototype backend.

    Resolves parent image packages for multi-stage builds and stores
    the resolved package names on the metadata for child images.

    Arg(s):
        metadata: ImageMetadata for the image being rebased.
        dest_dir (Path): Build directory containing the Dockerfile.
        repos: Runtime Repos object for repository configuration.
        base_dir (Path): Base working directory for locating parent image sources.
        downstream_parents (list[str] | None): Parent image pullspecs per stage.
        parent_members (list | None): Parent ImageMetadata objects.
        shared_dnf_cache (TemporaryDirectory | None): Shared cache dir across images.
        logger (logging.Logger | None): Logger instance.
    Return Value(s):
        TemporaryDirectory | None: The shared cache (created if None was passed in).
    """
    _logger = logger or logging.getLogger(__name__)

    if shared_dnf_cache is None:
        shared_dnf_cache = TemporaryDirectory(prefix="rpm-lockfile-cache-")

    resolver = RpmResolver(logger=_logger, cache_dir=shared_dnf_cache.name)
    generator = RpmLockfilePrototypeGenerator(repos, resolver=resolver)

    fallback: dict[int, list[str]] = {}
    parent_dirs: dict[int, Path] = {}
    if parent_members and downstream_parents:
        for stage_num, pullspec in enumerate(downstream_parents):
            for parent in parent_members or []:
                if parent is None:
                    continue
                if parent.distgit_key in pullspec:
                    if parent.lockfile_packages:
                        fallback[stage_num] = parent.lockfile_packages
                    parent_dir = base_dir / parent.qualified_key
                    if parent_dir.is_dir():
                        parent_dirs[stage_num] = parent_dir
                    break

    await generator.generate_lockfile(
        metadata,
        dest_dir,
        downstream_parents=downstream_parents,
        fallback_installed=fallback or None,
        parent_source_dirs=parent_dirs or None,
    )

    lockfile_path = dest_dir / DEFAULT_RPM_LOCKFILE_NAME
    if lockfile_path.exists():
        lockfile_data = yaml.safe_load(lockfile_path.read_text())
        pkg_names: set[str] = set()
        for arch_entry in lockfile_data.get("arches", []):
            for pkg in arch_entry.get("packages", []):
                name = pkg.get("name")
                if name:
                    pkg_names.add(name)
        metadata.lockfile_packages = sorted(pkg_names)

    return shared_dnf_cache


def apply_dockerfile_transforms(
    dest_dir: Path,
    logger: logging.Logger | None = None,
) -> None:
    """
    Strip Dockerfile commands that fail in hermetic builds.

    Applied after lockfile generation so update targets are already
    resolved into the lockfile. Removes bare yum/dnf update commands
    and reinstall commands (redundant when the base image already has
    the pinned version).

    Arg(s):
        dest_dir (Path): Build directory containing the Dockerfile.
        logger (logging.Logger | None): Logger instance.
    """
    _logger = logger or logging.getLogger(__name__)

    df_path = dest_dir / "Dockerfile"
    df_content = df_path.read_text()
    df_content = strip_bare_updates(df_content)
    df_content = strip_reinstall_commands(df_content)
    df_path.write_text(df_content)
    strip_bare_updates_from_scripts(dest_dir, logger=_logger)
