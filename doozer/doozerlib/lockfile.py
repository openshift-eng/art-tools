import asyncio
import hashlib
from dataclasses import dataclass
from functools import total_ordering
from logging import Logger
from pathlib import Path
from typing import Dict, Optional, Tuple

import aiohttp
import yaml
from artcommonlib import logutil
from artcommonlib.rpm_utils import compare_nvr
from artcommonlib.telemetry import start_as_current_span_async
from opentelemetry import trace

from doozerlib.image import ImageMetadata
from doozerlib.repodata import Repodata, Rpm, RpmModule
from doozerlib.repos import Repos

TRACER = trace.get_tracer(__name__)
DEFAULT_RPM_LOCKFILE_NAME = "rpms.lock.yaml"
DEFAULT_ARTIFACT_LOCKFILE_NAME = "artifacts.lock.yaml"


def sort_repos_for_lockfile_resolution(repo_names: set[str]) -> list[str]:
    """
    Order repos so upstream RHEL content (baseos, appstream, CRB, ...) is preferred over OCP plashets
    (rhocp / *ose-rpms*) as a tiebreaker when the same EVR exists in multiple repos.

    :meth:`RpmInfoCollector._fetch_rpms_info_per_arch` always picks the highest EVR across all repos.
    When two repos provide the same highest EVR, the first one in iteration order wins — so RHEL repos
    are listed first to prefer the upstream/canonical source for system packages.
    """

    def sort_key(name: str) -> tuple[int, str]:
        lower = name.lower()
        if 'ose-rpms' in lower or 'rhocp' in lower:
            return (1, name)
        return (0, name)

    return sorted(repo_names, key=sort_key)


@total_ordering
@dataclass(frozen=True)
class RpmInfo:
    """
    Immutable data class representing a resolved RPM package and its metadata.

    Comparison is based only on (name, evr).

    Attributes:
        name (str): Name of the RPM package.
        evr (str): Epoch-Version-Release string identifying the package version.
        checksum (str): Checksum string of the RPM package.
        repoid (str): Repository identifier from which the package is resolved.
        size (int): Size of the RPM package in bytes.
        sourcerpm (str): Name of the source RPM package.
        url (str): URL where the RPM package can be downloaded.
    """

    name: str
    evr: str
    checksum: str
    repoid: str
    size: int
    sourcerpm: str
    url: str
    epoch: int
    version: str
    release: str

    @classmethod
    def from_rpm(cls, rpm: "Rpm", *, repoid: str, baseurl: str) -> "RpmInfo":
        """
        Create an RpmInfo instance from an Rpm object and additional metadata.

        Args:
            rpm (Rpm): An instance of an Rpm package to extract data from.
            repoid (str): Repository identifier string.
            baseurl (str): Base URL to prepend to rpm.location to form the full URL.

        Returns:
            RpmInfo: A new instance of RpmInfo populated with data from rpm and metadata.
        """
        epoch = str(rpm.epoch) if rpm.epoch not in (None, '', '0') else '0'
        evr = f"{epoch}:{rpm.version}-{rpm.release}"
        return cls(
            name=rpm.name,
            evr=evr,
            checksum=rpm.checksum,
            repoid=repoid,
            size=rpm.size,
            sourcerpm=rpm.sourcerpm,
            epoch=rpm.epoch or 0,
            version=rpm.version,
            release=rpm.release,
            url=f'{baseurl}{rpm.location}',
        )

    def to_dict(self) -> Dict[str, object]:
        """
        Convert the RpmInfo instance to a dictionary.

        Returns:
            dict: Dictionary containing all attributes of the instance.
        """
        return {
            "name": self.name,
            "evr": self.evr,
            "checksum": self.checksum,
            "repoid": self.repoid,
            "size": self.size,
            "sourcerpm": self.sourcerpm,
            "url": self.url,
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RpmInfo):
            return NotImplemented
        return (self.name, self.evr) == (other.name, other.evr)

    def __lt__(self, other: "RpmInfo") -> bool:
        # compare_nvr returns -1 if self < other, 0 if equal, 1 if self > other
        if self.name != other.name:
            return self.name < other.name

        return (
            compare_nvr(
                {"name": self.name, "version": self.version, "epoch": str(self.epoch), "release": self.release},
                {"name": other.name, "version": other.version, "epoch": str(other.epoch), "release": other.release},
            )
            == -1
        )


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


@dataclass(frozen=True)
class ModuleInfo:
    """
    Immutable data class representing repository module metadata for lockfile generation.

    Follows Hermeto specification: https://github.com/hermetoproject/hermeto/blob/main/docs/rpm.md#rpm-lockfile-format

    Represents repository-level module metadata (modules.yaml file) rather than individual modules.
    One entry per repository containing modules, not per module.

    Attributes:
        url (str): Download URL for modules.yaml metadata file (required by Hermeto)
        repoid (str): Repository content set ID (mandatory for Hermeto)
        checksum (str): SHA256 checksum of modules.yaml file
        size (int): File size of modules.yaml in bytes
    """

    url: str
    repoid: str
    checksum: str
    size: int

    @classmethod
    def from_repository_metadata(cls, *, repoid: str, baseurl: str, checksum: str, size: int, url: str) -> "ModuleInfo":
        """
        Create ModuleInfo from repository metadata.

        Args:
            repoid: Repository content set ID
            baseurl: Base repository URL
            checksum: SHA256 checksum of modules.yaml
            size: File size of modules.yaml in bytes
            url: Actual modules file URL from repomd.xml

        Returns:
            ModuleInfo: Hermeto-compliant repository module metadata
        """
        return cls(
            url=url,
            repoid=repoid,
            checksum=checksum,
            size=size,
        )

    def to_dict(self) -> Dict[str, object]:
        """Convert to Hermeto-compliant dictionary format."""
        return {
            "url": self.url,
            "repoid": self.repoid,
            "checksum": self.checksum,
            "size": self.size,
        }


class RpmInfoCollector:
    """
    Collects RpmInfo metadata for a given set of RPM names and architectures from specified repositories.

    This class supports asynchronous loading of repodata and concurrent collection of resolved package info,
    taking care to avoid re-fetching already-loaded repositories.
    """

    def __init__(self, repos: Repos, logger: Optional[Logger] = None):
        self.repos = repos
        self.loaded_repos: dict[str, Repodata] = {}
        self.logger = logger or logutil.get_logger(__name__)

    @start_as_current_span_async(TRACER, "lockfile.load_repos")
    async def _load_repos(self, requested_repos: set[str], arch: str):
        """
        Load repodata for the given repositories and architecture.

        Only repositories not already loaded in `self.loaded_repos` will be fetched.
        Logs a summary of which repos are skipped and which are loaded.

        Args:
            requested_repos (set[str]): Set of repository names to load.
            arch (str): Architecture identifier for fetching repodata.
        """
        not_yet_loaded = {repo for repo in requested_repos if f'{repo}-{arch}' not in self.loaded_repos}
        already_loaded = requested_repos - not_yet_loaded

        current_span = trace.get_current_span()
        current_span.update_name(f"lockfile.load_repos ({len(not_yet_loaded)}) {arch}")
        current_span.set_attribute("lockfile.arch", arch)
        current_span.set_attribute("lockfile.requested_repos", ",".join(sorted(requested_repos)))
        current_span.set_attribute("lockfile.already_loaded_count", len(already_loaded))
        current_span.set_attribute("lockfile.needs_loading_count", len(not_yet_loaded))

        if already_loaded:
            self.logger.info(f"Repos already loaded, skipping: {', '.join(sorted(already_loaded))} for arch {arch}")

        # Only fetch repos that are BOTH globally enabled AND requested (and not already loaded)
        globally_enabled = {r.name for r in self.repos.values() if r.enabled}
        repos_to_fetch = sorted((globally_enabled & requested_repos) - already_loaded)

        if not repos_to_fetch:
            self.logger.info("No new repos to load.")
            return

        self.logger.info(f"Loading repos: {', '.join(repos_to_fetch)} for arch {arch}")
        current_span.set_attribute("lockfile.repos_to_fetch", ",".join(repos_to_fetch))
        current_span.set_attribute("lockfile.repo_count", len(repos_to_fetch))

        repodatas = await asyncio.gather(*(self.repos[repo_name].get_repodata(arch) for repo_name in repos_to_fetch))

        self.loaded_repos.update({r.name: r for r in repodatas})
        self.logger.info(f"Finished loading repos: {', '.join(repos_to_fetch)} for arch {arch}")

    def _fetch_rpms_info_per_arch(
        self, rpm_names: set[str], repo_names: set[str], arch: str, enabled_modules: set[str] | None = None
    ) -> list[RpmInfo]:
        """
        Resolve RPM metadata for a specific architecture from the given repodata names.

        For plain package names the highest version across ALL repos is selected (matching
        dnf behaviour).  For NVR-pinned items the exact requested version is preserved in
        the output so that hermetic builds can install a specific older version.

        When the same EVR exists in multiple repos, the repo iteration order from
        ``sort_repos_for_lockfile_resolution`` acts as tiebreaker (RHEL upstream repos
        are preferred over rhocp plashets).

        Non-modular RPMs are preferred over modular ones when both exist for the same
        package name. This prevents the "highest EVR" logic from pulling in modular RPMs
        that would be blocked by dnf modular filtering at build time. RPMs from modules
        listed in ``enabled_modules`` (i.e. the image's ``modules:`` config) are exempt
        and compete on EVR normally — the user is explicitly requesting those modules.

        Args:
            rpm_names (set[str]): RPM names or NVRs to resolve.
            repo_names (set[str]): Names of repodata sources to search.
            arch (str): Target architecture.
            enabled_modules (set[str] | None): Module names from the image's ``modules:`` config.

        Returns:
            list[RpmInfo]: Resolved RPM package metadata.
        """
        nvr_to_name: dict[str, str] = {}
        for item in rpm_names:
            is_nvr, pkg_name = Repodata._detect_nvr_vs_name(item)
            if is_nvr:
                nvr_to_name[item] = pkg_name

        # Build a set of ALL modular NEVRAs so we can identify modular RPMs.
        # For non-enabled modules, non-modular versions are preferred.
        # For enabled modules, RPMs are resolved directly from module metadata
        # in a second pass, bypassing EVR comparison entirely.
        enabled_module_names = {m.split(':')[0] for m in (enabled_modules or set())}
        all_modular_nevras: set[str] = set()
        for repo_key, repodata in self.loaded_repos.items():
            if not repo_key.endswith(f'-{arch}'):
                continue
            for module in repodata.modules:
                all_modular_nevras.update(module.rpms)

        best_by_name: dict[str, RpmInfo] = {}
        best_is_modular: dict[str, bool] = {}
        pinned_by_nvr: dict[str, RpmInfo] = {}
        resolved_items: set[str] = set()

        for repo_name in sort_repos_for_lockfile_resolution(repo_names):
            repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
            if repodata is None:
                self.logger.error(
                    f'repodata {repo_name}-{arch} not found while fetching rpms, it should be loaded by now'
                )
                continue

            repo = self.repos._repos[repo_name]
            if repo is None:
                self.logger.error(f'repo {repo_name} not found')
                continue

            found_rpms, not_found = repodata.get_rpms(rpm_names, arch)
            resolved_items |= rpm_names - set(not_found)

            content_set_id = repo.content_set(arch)
            if content_set_id is None:
                self.logger.warning(f'repo {repo_name} has no content_set for {arch}, falling back to repo key')
                content_set_id = f'{repo_name}-{arch}'

            baseurl = repo.baseurl(repotype="unsigned", arch=arch)
            for rpm in found_rpms:
                info = RpmInfo.from_rpm(rpm, repoid=content_set_id, baseurl=baseurl)
                is_modular = rpm.nevra in all_modular_nevras

                for nvr_str, pkg_name in nvr_to_name.items():
                    if rpm.name == pkg_name and rpm.nvr == nvr_str and nvr_str not in pinned_by_nvr:
                        pinned_by_nvr[nvr_str] = info

                existing = best_by_name.get(rpm.name)
                if existing is None:
                    best_by_name[rpm.name] = info
                    best_is_modular[rpm.name] = is_modular
                elif best_is_modular.get(rpm.name) and not is_modular:
                    best_by_name[rpm.name] = info
                    best_is_modular[rpm.name] = False
                elif not best_is_modular.get(rpm.name) and is_modular:
                    pass  # Keep existing non-modular
                elif info > existing:
                    best_by_name[rpm.name] = info
                    best_is_modular[rpm.name] = is_modular

        missing = rpm_names - resolved_items
        if missing:
            self.logger.warning(
                f"Could not find {','.join(sorted(missing))} in {', '.join(repo_names)} for arch {arch}"
            )

        # Fix: when get_rpms() returned a modular RPM from a non-enabled module
        # (because it was the highest EVR in that repo), search primary_rpms
        # directly for a non-modular alternative with a lower EVR.
        nvr_pinned_names = set(nvr_to_name.values())
        for pkg_name in list(best_by_name):
            if not best_is_modular.get(pkg_name):
                continue
            if pkg_name in nvr_pinned_names:
                continue
            for repo_name in sort_repos_for_lockfile_resolution(repo_names):
                repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
                if repodata is None:
                    continue
                for rpm in repodata.primary_rpms:
                    if rpm.name == pkg_name and (rpm.arch == arch or rpm.arch == 'noarch'):
                        if rpm.nevra not in all_modular_nevras:
                            repo = self.repos._repos.get(repo_name)
                            if repo is None:
                                continue
                            content_set_id = repo.content_set(arch) or f'{repo_name}-{arch}'
                            baseurl = repo.baseurl(repotype="unsigned", arch=arch)
                            best_by_name[pkg_name] = RpmInfo.from_rpm(rpm, repoid=content_set_id, baseurl=baseurl)
                            best_is_modular[pkg_name] = False
                            break
                if not best_is_modular.get(pkg_name):
                    break

        # Second pass: for enabled modules, resolve RPMs directly from module
        # metadata instead of relying on EVR comparison from primary.xml.
        # This ensures correct module context selection and overrides any
        # non-modular versions that dnf would filter out at build time.
        if enabled_module_names:
            self._resolve_enabled_module_rpms(best_by_name, enabled_modules or set(), repo_names, arch, rpm_names)

        results: dict[tuple[str, str], RpmInfo] = {}
        for info in best_by_name.values():
            results[(info.name, info.evr)] = info
        for info in pinned_by_nvr.values():
            results[(info.name, info.evr)] = info

        return sorted(results.values())

    def _resolve_enabled_module_rpms(
        self,
        best_by_name: dict[str, RpmInfo],
        enabled_modules: set[str],
        repo_names: set[str],
        arch: str,
        rpm_names: set[str],
    ) -> None:
        """
        For enabled modules, resolve RPMs directly from module metadata.

        Instead of relying on EVR comparison from primary.xml (which picks
        wrong module contexts based on meaningless context hashes), this method:
        1. Finds all contexts for each enabled module
        2. Picks the context whose requirements match already-resolved packages
        3. Force-resolves RPMs from that context, overriding first-pass results

        This ensures the lockfile contains modular RPMs from a consistent context
        that matches the base packages (e.g. perl:5.26 context when perl-interpreter
        5.26 is resolved).
        """
        # Resolve bare module names to their default streams from repodata.
        # When a stream is specified (e.g. perl:5.26), use that stream.
        # When no stream is specified (e.g. perl), use the default stream from
        # modulemd-defaults metadata (e.g. perl:5.26 is the default on el8).
        enabled_streams = {m for m in enabled_modules if ':' in m}
        bare_names = {m for m in enabled_modules if ':' not in m}

        # Look up default streams for bare names
        for repo_name in repo_names:
            repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
            if repodata is None:
                continue
            for name in list(bare_names):
                default_stream = repodata.default_streams.get(name)
                if default_stream:
                    enabled_streams.add(f'{name}:{default_stream}')
                    bare_names.discard(name)

        module_contexts: dict[str, list[RpmModule]] = {}
        for repo_name in repo_names:
            repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
            if repodata is None:
                continue
            for module in repodata.modules:
                if module.name_stream in enabled_streams or module.name in bare_names:
                    module_contexts.setdefault(module.name_stream, []).append(module)

        if not module_contexts:
            return

        # Resolved versions from first pass (for context matching)
        resolved_versions: dict[str, str] = {name: info.version for name, info in best_by_name.items()}

        for name_stream, contexts in module_contexts.items():
            matching_contexts = self._pick_matching_module_contexts(contexts, resolved_versions)
            if not matching_contexts:
                continue

            # Collect RPMs from ALL matching contexts (same requirements).
            # Different packages may appear in different contexts of the same stream.
            target_nevras: set[str] = set()
            for ctx in matching_contexts:
                target_nevras.update(ctx.rpms)
            overridden: set[str] = set()

            for repo_name in repo_names:
                repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
                if repodata is None:
                    continue
                repo = self.repos._repos.get(repo_name)
                if repo is None:
                    continue
                content_set_id = repo.content_set(arch) or f'{repo_name}-{arch}'
                baseurl = repo.baseurl(repotype="unsigned", arch=arch)

                for rpm in repodata.primary_rpms:
                    if rpm.nevra in target_nevras and rpm.name not in overridden:
                        best_by_name[rpm.name] = RpmInfo.from_rpm(rpm, repoid=content_set_id, baseurl=baseurl)
                        overridden.add(rpm.name)

            if overridden:
                ctx_ids = ','.join(sorted({str(c.context) for c in matching_contexts}))
                self.logger.info(
                    f"Resolved {len(overridden)} RPMs from module {name_stream} "
                    f"context(s) {ctx_ids}: {','.join(sorted(overridden))}"
                )

    @staticmethod
    def _pick_matching_module_contexts(contexts: list[RpmModule], resolved_versions: dict[str, str]) -> list[RpmModule]:
        """
        Pick module contexts whose requirements match already-resolved packages.

        Returns ALL contexts with the best match score, since different packages
        may appear in different contexts of the same module stream (e.g.
        perl-Net-SSLeay in one context, perl-Mozilla-CA in another, both
        requiring perl:5.26).

        Falls back to all contexts with the highest module version if no
        requirements match.
        """
        scored: list[tuple[int, RpmModule]] = []

        for ctx in contexts:
            if not ctx.requires:
                scored.append((0, ctx))
                continue
            score = 0
            for dep_module, dep_streams in ctx.requires.items():
                for pkg_name, pkg_version in resolved_versions.items():
                    if dep_module in pkg_name:
                        for stream in dep_streams:
                            if pkg_version.startswith(stream):
                                score += 1
                                break
            scored.append((score, ctx))

        if not scored:
            return []

        best_score = max(s for s, _ in scored)
        if best_score > 0:
            return [ctx for s, ctx in scored if s == best_score]

        # No requirements matched — return contexts with highest module version
        max_version = max(ctx.version for _, ctx in scored)
        return [ctx for _, ctx in scored if ctx.version == max_version]

    @start_as_current_span_async(TRACER, "lockfile.fetch_rpms_info")
    async def fetch_rpms_info(
        self,
        arches: list[str],
        repositories: set[str],
        rpm_names: set[str],
        enabled_modules: set[str] | None = None,
    ) -> dict[str, list[RpmInfo]]:
        """
        Resolve RPM info across multiple architectures and repositories.

        Note: Repositories must be pre-loaded using _load_repos before calling this method.

        Args:
            arches (list[str]): Target architectures.
            repositories (set[str]): Names of repositories to search.
            rpm_names (set[str]): Names or NVRs of RPMs to resolve.
            enabled_modules (set[str] | None): Module names from the image's ``modules:`` config.

        Returns:
            dict[str, list[RpmInfo]]: Mapping of architecture to resolved RPM metadata.
        """
        current_span = trace.get_current_span()
        current_span.set_attribute("lockfile.arches", ",".join(arches))
        current_span.set_attribute("lockfile.repositories", ",".join(sorted(repositories)))
        current_span.set_attribute("lockfile.rpm_count", len(rpm_names))
        current_span.set_attribute("lockfile.arch_count", len(arches))

        results = await asyncio.gather(
            *[
                asyncio.get_running_loop().run_in_executor(
                    None, self._fetch_rpms_info_per_arch, rpm_names, repositories, arch, enabled_modules
                )
                for arch in arches
            ]
        )

        total_resolved = sum(len(result) for result in results)
        current_span.set_attribute("lockfile.total_resolved_packages", total_resolved)
        return dict(zip(arches, results, strict=True))

    def _get_modules_yaml_metadata(self, repo_name: str, arch: str) -> Tuple[str, int, str]:
        """
        Extract checksum, size and URL for modules.yaml from repodata.

        Args:
            repo_name: Repository name
            arch: Architecture

        Returns:
            Tuple[str, int, str]: (checksum, size, url) for modules.yaml file
        """
        repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
        if repodata is None:
            self.logger.warning(f'repodata {repo_name}-{arch} not found')
            return "sha256:unknown", 0, "repodata/modules.yaml"

        if (
            repodata.modules_checksum is not None
            and repodata.modules_size is not None
            and repodata.modules_url is not None
        ):
            return repodata.modules_checksum, repodata.modules_size, repodata.modules_url

        return "sha256:unknown", 0, "repodata/modules.yaml"

    def _fetch_modules_info_per_arch(self, module_names: set[str], repo_names: set[str], arch: str) -> list[ModuleInfo]:
        """
        Resolve repository module metadata for specific architecture from repodata sources.

        Finds repositories that contain any of the requested modules and returns one
        ModuleInfo entry per unique repository (not per module).

        Args:
            module_names: Set of module names to resolve (e.g., {"python36", "nodejs"})
            repo_names: Names of repodata sources to search
            arch: Target architecture

        Returns:
            list[ModuleInfo]: Repository-based module metadata (one entry per repo with modules)
        """
        module_info_list = []
        search_names = {name.split(':')[0] for name in module_names}
        unresolved_search_names = set(search_names)
        processed_repos = set()

        for repo_name in repo_names:
            repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
            if repodata is None:
                self.logger.error(f'repodata {repo_name}-{arch} not found while fetching modules')
                continue

            repo = self.repos._repos[repo_name]
            if repo is None:
                self.logger.error(f'repo {repo_name} not found')
                continue

            matching_modules = [
                module
                for module in repodata.modules
                if module.name in search_names and (module.arch == arch or module.arch == "noarch")
            ]

            self.logger.debug(
                f'Repository {repo_name}-{arch}: found {len(repodata.modules)} total modules, {len(matching_modules)} matching {module_names}'
            )

            if repodata.modules:
                module_summary = [(m.name, m.arch) for m in repodata.modules[:5]]
                self.logger.debug(f'Available modules (first 5): {module_summary}')

            if not matching_modules:
                self.logger.debug(f'No matching modules found in {repo_name}-{arch}, skipping repository')
                continue

            content_set_id = repo.content_set(arch) or f'{repo_name}-{arch}'

            repo_key = f"{content_set_id}"
            if repo_key in processed_repos:
                continue
            processed_repos.add(repo_key)

            baseurl = repo.baseurl(repotype="unsigned", arch=arch)
            modules_checksum, modules_size, modules_url = self._get_modules_yaml_metadata(repo_name, arch)

            module_info = ModuleInfo.from_repository_metadata(
                repoid=content_set_id, baseurl=baseurl, checksum=modules_checksum, size=modules_size, url=modules_url
            )
            module_info_list.append(module_info)

            found_modules = {module.name for module in matching_modules}
            unresolved_search_names -= found_modules

            if not unresolved_search_names:
                break

        if unresolved_search_names:
            self.logger.warning(
                f"Could not find modules {','.join(unresolved_search_names)} in {', '.join(repo_names)} for arch {arch}"
            )

        return sorted(module_info_list, key=lambda m: m.repoid)

    @start_as_current_span_async(TRACER, "lockfile.fetch_modules_info")
    async def fetch_modules_info(
        self, arches: list[str], repositories: set[str], module_names: set[str]
    ) -> dict[str, list[ModuleInfo]]:
        """
        Resolve module info across multiple architectures and repositories.

        Args:
            arches: Target architectures
            repositories: Names of repositories to search
            module_names: Names of modules to resolve (can be empty)

        Returns:
            dict[str, list[ModuleInfo]]: Always returns dict with arch keys, empty lists if no modules
        """
        current_span = trace.get_current_span()
        current_span.set_attribute("lockfile.module_count", len(module_names))

        if not module_names:
            self.logger.debug("No modules to install, returning empty module metadata")
            return {arch: [] for arch in arches}

        current_span.set_attribute("lockfile.arches", ",".join(arches))
        current_span.set_attribute("lockfile.repositories", ",".join(sorted(repositories)))

        results = await asyncio.gather(
            *[
                asyncio.get_running_loop().run_in_executor(
                    None, self._fetch_modules_info_per_arch, module_names, repositories, arch
                )
                for arch in arches
            ]
        )

        total_resolved = sum(len(result) for result in results)
        current_span.set_attribute("lockfile.total_resolved_modules", total_resolved)
        return dict(zip(arches, results, strict=True))


class RPMLockfileGenerator:
    """
    Handles generation of a lockfile detailing RPM packages across architectures and repositories.

    The lockfile captures resolved package metadata, ensuring reproducibility by
    hashing the RPM input list to avoid redundant regenerations. Uses RpmInfoCollector
    for asynchronous RPM metadata retrieval and outputs YAML lockfiles along with digest files.
    """

    def __init__(
        self,
        repos: Repos,
        logger: Optional[Logger] = None,
        runtime=None,
        lockfile_seed_nvrs: Optional[list[str]] = None,
    ):
        self.logger = logger or logutil.get_logger(__name__)
        self.builder = RpmInfoCollector(repos, self.logger)
        self.runtime = runtime
        self.lockfile_seed_nvrs = lockfile_seed_nvrs

    def _validate_cross_arch_version_sets(self, rpms_info_by_arch: dict[str, list[RpmInfo]]) -> None:
        """Warn when RPM latest version sets differ across architectures where packages exist."""
        package_arch_evrs = {}

        for arch, rpm_list in rpms_info_by_arch.items():
            # Group RPMs by package name
            packages = {}
            for rpm in rpm_list:
                packages.setdefault(rpm.name, []).append(rpm)

            # For each package, find latest version and add to validation set
            for package_name, package_rpms in packages.items():
                if len(package_rpms) == 1:
                    latest_rpm = package_rpms[0]
                else:
                    # Find latest using RpmInfo comparison (leverages @total_ordering)
                    latest_rpm = package_rpms[0]
                    for rpm in package_rpms[1:]:
                        if rpm > latest_rpm:
                            latest_rpm = rpm

                package_arch_evrs.setdefault(package_name, {}).setdefault(arch, set()).add(latest_rpm.evr)

        mismatches = []
        for package_name, arch_evrs in package_arch_evrs.items():
            if len(arch_evrs) < 2:
                continue

            evr_sets = list(arch_evrs.values())
            reference_set = evr_sets[0]

            if not all(evr_set == reference_set for evr_set in evr_sets[1:]):
                arch_details = [f"{arch}:{{{','.join(sorted(evr_set))}}}" for arch, evr_set in arch_evrs.items()]
                mismatches.append(f"{package_name} ({'; '.join(arch_details)})")

        if mismatches:
            self.logger.warning("RPM version set mismatches: %s", "; ".join(mismatches))

    def _align_cross_arch_versions(
        self, rpms_info_by_arch: dict[str, list[RpmInfo]], repo_names: set[str]
    ) -> dict[str, list[RpmInfo]]:
        """Align RPM latest versions across architectures to a common version.

        When independent per-arch resolution picks different "latest" versions
        (e.g. x86_64 gets v2 while aarch64 still has v1), this method
        downgrades each arch to the lowest of the per-arch latest EVRs —
        i.e. the version that the least-updated arch resolved.

        Pinned (NVR-specific) entries are never touched — only the "latest" pick
        per package name is considered for alignment.

        Args:
            rpms_info_by_arch: Mapping of arch → resolved RPM list (as returned
                by :meth:`RpmInfoCollector.fetch_rpms_info`).
            repo_names: Repository names used during resolution, needed to scan
                ``loaded_repos`` for alternative versions.

        Returns:
            A new dict with the same structure, where mismatched latest versions
            have been replaced by the shared target version, when that version
            is available in the loaded repos for the affected arch.
        """
        # Step 1: For each arch, identify the latest RpmInfo per package name.
        # A package may appear multiple times (pinned + latest); the "latest" is
        # the entry with the highest EVR for that name.
        arch_latest: dict[str, dict[str, RpmInfo]] = {}  # arch -> {pkg_name -> latest RpmInfo}
        for arch, rpm_list in rpms_info_by_arch.items():
            packages: dict[str, list[RpmInfo]] = {}
            for rpm in rpm_list:
                packages.setdefault(rpm.name, []).append(rpm)
            latest_map: dict[str, RpmInfo] = {}
            for pkg_name, pkg_rpms in packages.items():
                latest = max(pkg_rpms)
                latest_map[pkg_name] = latest
            arch_latest[arch] = latest_map

        # Step 2: Find packages present on 2+ arches with mismatched latest EVRs.
        all_pkg_names: set[str] = set()
        for latest_map in arch_latest.values():
            all_pkg_names.update(latest_map.keys())

        mismatched_packages: dict[str, dict[str, RpmInfo]] = {}  # pkg_name -> {arch -> latest RpmInfo}
        for pkg_name in all_pkg_names:
            arches_with_pkg = {
                arch: latest_map[pkg_name] for arch, latest_map in arch_latest.items() if pkg_name in latest_map
            }
            if len(arches_with_pkg) < 2:
                continue
            evrs = {rpm.evr for rpm in arches_with_pkg.values()}
            if len(evrs) > 1:
                mismatched_packages[pkg_name] = arches_with_pkg

        if not mismatched_packages:
            return rpms_info_by_arch

        # Step 3: For each mismatched package, determine the common target EVR.
        # The target is the minimum of the per-arch latest EVRs — i.e. the
        # highest version that the "least updated" arch has.
        target_evrs: dict[str, str] = {}  # pkg_name -> target evr
        for pkg_name, arches_map in mismatched_packages.items():
            target_rpm = min(arches_map.values())
            target_evrs[pkg_name] = target_rpm.evr

        # Step 4: For each mismatched package, collect candidate replacements
        # across ALL affected arches *before* applying any.  If any single
        # arch cannot resolve the target EVR, the whole package is skipped so
        # we never end up with a partial downgrade that leaves arches still
        # mismatched.
        replacement_rpms: dict[str, dict[str, RpmInfo]] = {}  # arch -> {pkg_name -> replacement RpmInfo}
        for pkg_name, target_evr in target_evrs.items():
            affected_arches = {
                arch: latest for arch, latest in mismatched_packages[pkg_name].items() if latest.evr != target_evr
            }
            if not affected_arches:
                continue

            # Try to find the target EVR in loaded repos for every affected arch.
            candidates: dict[str, RpmInfo] = {}  # arch -> replacement RpmInfo
            missing_arches: list[str] = []
            for arch, current_latest in affected_arches.items():
                found = self._find_rpm_by_evr_in_repos(pkg_name, target_evr, arch, repo_names)
                if found is not None:
                    candidates[arch] = found
                else:
                    missing_arches.append(arch)

            if missing_arches:
                self.logger.warning(
                    "Cross-arch alignment: cannot find %s %s for arch(es) %s in loaded repos; "
                    "skipping alignment for this package",
                    pkg_name,
                    target_evr,
                    ", ".join(sorted(missing_arches)),
                )
                continue

            # All affected arches can resolve — apply.
            for arch, replacement in candidates.items():
                replacement_rpms.setdefault(arch, {})[pkg_name] = replacement
                self.logger.info(
                    "Cross-arch alignment: %s on %s downgraded from %s to %s",
                    pkg_name,
                    arch,
                    affected_arches[arch].evr,
                    target_evr,
                )

        if not replacement_rpms:
            return rpms_info_by_arch

        # Step 5: Rebuild the per-arch RPM lists, swapping the latest entry for
        # aligned packages while preserving pinned/other entries.
        aligned: dict[str, list[RpmInfo]] = {}
        for arch, rpm_list in rpms_info_by_arch.items():
            arch_replacements = replacement_rpms.get(arch, {})
            if not arch_replacements:
                aligned[arch] = rpm_list
                continue

            new_list: list[RpmInfo] = []
            for rpm in rpm_list:
                if rpm.name in arch_replacements:
                    old_latest = arch_latest[arch][rpm.name]
                    if rpm.evr == old_latest.evr:
                        # This is the entry being replaced — swap it
                        new_list.append(arch_replacements[rpm.name])
                        continue
                new_list.append(rpm)

            # Deduplicate by (name, evr): a pinned entry could already
            # hold the target EVR, creating a duplicate when the latest
            # is replaced.  Keep the first occurrence (i.e. the pinned one).
            deduped: dict[tuple[str, str], RpmInfo] = {}
            for rpm in new_list:
                deduped.setdefault((rpm.name, rpm.evr), rpm)
            aligned[arch] = sorted(deduped.values())

        return aligned

    def _find_rpm_by_evr_in_repos(
        self, pkg_name: str, target_evr: str, arch: str, repo_names: set[str]
    ) -> Optional[RpmInfo]:
        """Search loaded repos for an RPM matching the given name, EVR, and arch.

        Iterates through repositories in the canonical order to find a specific
        version of a package.  Returns the first match, or ``None``.
        """
        for repo_name in sort_repos_for_lockfile_resolution(repo_names):
            repodata = self.builder.loaded_repos.get(f'{repo_name}-{arch}')
            if repodata is None:
                continue

            repo = self.builder.repos._repos.get(repo_name)
            if repo is None:
                continue

            content_set_id = repo.content_set(arch)
            if content_set_id is None:
                content_set_id = f'{repo_name}-{arch}'
            baseurl = repo.baseurl(repotype="unsigned", arch=arch)

            for rpm in repodata.primary_rpms:
                if rpm.name != pkg_name:
                    continue
                if rpm.arch != arch and rpm.arch != 'noarch':
                    continue
                candidate = RpmInfo.from_rpm(rpm, repoid=content_set_id, baseurl=baseurl)
                if candidate.evr == target_evr:
                    return candidate

        return None

    async def should_generate_lockfile(
        self, image_meta: ImageMetadata, dest_dir: Path, filename: str = DEFAULT_RPM_LOCKFILE_NAME
    ) -> tuple[bool, set[str]]:
        """
        Determine if lockfile generation is needed for an image.

        Args:
            image_meta: Image metadata containing repository and RPM configuration
            dest_dir: Destination directory for lockfile output
            filename: Name of the lockfile to generate

        Returns:
            tuple[bool, set[str]]: Whether to generate lockfile and set of RPMs to install
        """
        if not image_meta.is_lockfile_generation_enabled():
            return False, set()

        enabled_repos = image_meta.get_enabled_repos()
        if not enabled_repos:
            self.logger.info(f"Skipping lockfile generation for {image_meta.distgit_key}: repositories set is empty")
            return False, set()

        rpms_to_install = await image_meta.get_lockfile_rpms_to_install(lockfile_seed_nvrs=self.lockfile_seed_nvrs)

        if not rpms_to_install:
            self.logger.warning(
                f'Empty RPM list to install for {image_meta.distgit_key}; all required RPMs may be inherited? Skipping lockfile generation.'
            )
            return False, set()
        else:
            self.logger.info(f'{image_meta.distgit_key} image needs to install {len(rpms_to_install)} rpms')

        return True, rpms_to_install

    @start_as_current_span_async(TRACER, "lockfile.ensure_repositories_loaded")
    async def ensure_repositories_loaded(self, image_metas: list[ImageMetadata], base_dir: Path) -> None:
        """
        Determine which images need lockfiles and load repositories efficiently.

        Performs should_generate_lockfile checks for all images, then loads
        repository metadata only for images that actually need generation.

        Args:
            image_metas: List of image metadata objects to evaluate
            base_dir: Base directory for lockfile generation
        """
        current_span = trace.get_current_span()
        images_needing_lockfiles = []

        for image_meta in image_metas:
            dest_dir = base_dir / image_meta.distgit_key

            should_generate, _ = await self.should_generate_lockfile(image_meta, dest_dir)
            if should_generate:
                images_needing_lockfiles.append(image_meta)
                self.logger.info(f"Image {image_meta.distgit_key} needs lockfile generation")
            else:
                self.logger.info(f"Image {image_meta.distgit_key} skipping lockfile generation")

        repos_by_arch = {}

        for image_meta in images_needing_lockfiles:
            enabled_repos = image_meta.get_enabled_repos()
            arches = image_meta.get_arches()
            for arch in arches:
                if arch not in repos_by_arch:
                    repos_by_arch[arch] = set()
                repos_by_arch[arch].update(enabled_repos)

        total_repo_arch_pairs = sum(len(repos) for repos in repos_by_arch.values())
        current_span.set_attribute("lockfile.total_images", len(image_metas))
        current_span.set_attribute("lockfile.images_needing_generation", len(images_needing_lockfiles))
        current_span.set_attribute("lockfile.unique_repo_arch_pairs", total_repo_arch_pairs)

        if images_needing_lockfiles:
            self.logger.info(
                f"Loading repositories for {len(images_needing_lockfiles)} images needing lockfile generation"
            )

            if repos_by_arch:
                # Note: loading repos across arches in parallel (asyncio.gather) was tested
                # and did not improve performance. Keep sequential loading.
                for arch, repos_for_arch in repos_by_arch.items():
                    await self.builder._load_repos(repos_for_arch, arch)
            else:
                self.logger.info("No repositories to load for lockfile generation")
        else:
            self.logger.info("No images need lockfile generation - skipping repository loading")

    async def generate_lockfile(
        self, image_meta: ImageMetadata, dest_dir: Path, filename: str = DEFAULT_RPM_LOCKFILE_NAME
    ) -> None:
        """
        Generate RPM lockfile for image with resolved package metadata.

        Creates YAML lockfile containing exact RPM versions, checksums, and URLs
        for all architectures. Includes digest optimization to skip regeneration
        when RPM lists haven't changed.

        Args:
            image_meta: Image metadata containing configuration and RPM requirements
            dest_dir: Output directory for lockfile and digest files
            filename: Lockfile filename, defaults to rpms.lock.yaml
        """
        should_generate, rpms_to_install = await self.should_generate_lockfile(image_meta, dest_dir, filename)

        if not should_generate:
            return

        arches = image_meta.get_arches()
        enabled_repos = image_meta.get_enabled_repos()

        # Load repositories once for both RPM and module collection to avoid race condition
        # Only attempt loading if we have real repository objects (not test mocks)
        if hasattr(self.builder.repos, '_repos') and enabled_repos:
            try:
                await asyncio.gather(*(self.builder._load_repos(enabled_repos, arch) for arch in arches))
            except (TypeError, AttributeError):
                # Skip repository loading in test scenarios where mocks don't support async operations
                pass

        lockfile = {
            "lockfileVersion": 1,
            "lockfileVendor": "redhat",
            "arches": [],
        }

        modules_to_install = image_meta.get_lockfile_modules_to_install()

        rpms_info_by_arch, modules_info_by_arch = await asyncio.gather(
            self.builder.fetch_rpms_info(arches, enabled_repos, rpms_to_install, enabled_modules=modules_to_install),
            self.builder.fetch_modules_info(arches, enabled_repos, modules_to_install),
        )

        # Validate cross-architecture version set consistency (warn on mismatch)
        self._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Align mismatched latest versions to the highest common version
        rpms_info_by_arch = self._align_cross_arch_versions(rpms_info_by_arch, enabled_repos)

        if image_meta.is_cross_arch_enabled():
            self.logger.info("Cross-architecture lockfile inclusion enabled")

            all_rpms = []
            for arch_rpms in rpms_info_by_arch.values():
                all_rpms.extend(arch_rpms)

            for arch in arches:
                rpms_info_by_arch[arch] = all_rpms

        for arch, rpm_list in rpms_info_by_arch.items():
            module_list = modules_info_by_arch.get(arch, [])
            lockfile["arches"].append(
                {
                    "arch": arch,
                    "packages": [rpm.to_dict() for rpm in rpm_list],
                    "module_metadata": [module.to_dict() for module in module_list],
                }
            )

        lockfile_path = dest_dir / filename
        self._write_yaml(lockfile, lockfile_path)

    def _write_yaml(self, data: dict, output_path: Path) -> None:
        """
        Write a Python dictionary to a YAML file.

        Args:
            data (dict): Data to serialize.
            output_path (Path): File path to write YAML to.
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            yaml.safe_dump(data, f, sort_keys=False)


class ArtifactLockfileGenerator:
    """
    Handles generation of artifact lockfiles for generic file downloads.

    Creates lockfiles compatible with Cachi2's generic fetcher for hermetic builds.
    Simplified compared to RPMLockfileGenerator since artifact downloads are inexpensive.
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
