import asyncio
import hashlib
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from functools import total_ordering
from logging import Logger
from pathlib import Path
from typing import Any, Dict, Optional, Set

import aiohttp
import yaml
from artcommonlib import exectools, logutil
from artcommonlib.rpm_utils import compare_nvr
from artcommonlib.telemetry import start_as_current_span_async
from artcommonlib.util import download_file_from_github, split_git_url
from opentelemetry import trace

from doozerlib.image import ImageMetadata
from doozerlib.repodata import Repodata, Rpm
from doozerlib.repos import Repos

TRACER = trace.get_tracer(__name__)
DEFAULT_LOCKFILE_NAME = "rpms.lock.yaml"


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

        enabled_repos = {r.name for r in self.repos.values() if r.enabled}
        repos_to_fetch = sorted(enabled_repos | set(not_yet_loaded))

        if not repos_to_fetch:
            self.logger.info("No new repos to load.")
            return

        self.logger.info(f"Loading repos: {', '.join(repos_to_fetch)} for arch {arch}")
        current_span.set_attribute("lockfile.repos_to_fetch", ",".join(repos_to_fetch))
        current_span.set_attribute("lockfile.repo_count", len(repos_to_fetch))

        repodatas = await asyncio.gather(*(self.repos[repo_name].get_repodata(arch) for repo_name in repos_to_fetch))

        self.loaded_repos.update({r.name: r for r in repodatas})
        self.logger.info(f"Finished loading repos: {', '.join(repos_to_fetch)} for arch {arch}")

    def _fetch_rpms_info_per_arch(self, rpm_names: set[str], repo_names: set[str], arch: str) -> list[RpmInfo]:
        """
        Resolve RPM metadata for a specific architecture from the given repodata names.

        Args:
            rpm_names (set[str]): RPM names or NVRs to resolve.
            repo_names (set[str]): Names of repodata sources to search.
            arch (str): Target architecture.

        Returns:
            list[RpmInfo]: Resolved RPM package metadata.
        """
        rpm_info_list = []
        unresolved_rpms = set(rpm_names)
        missing_rpms = unresolved_rpms

        for repo_name in repo_names:
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

            found_rpms, missing_rpms = repodata.get_rpms(unresolved_rpms, arch)
            rpm_info_list.extend(
                [
                    RpmInfo.from_rpm(
                        rpm, repoid=f'{repo_name}-{arch}', baseurl=repo.baseurl(repotype="unsigned", arch=arch)
                    )
                    for rpm in found_rpms
                ]
            )

            if not missing_rpms:
                # Found all rpms, break early
                break

            unresolved_rpms = missing_rpms

        if missing_rpms:
            self.logger.warning(f"Could not find {','.join(missing_rpms)} in {', '.join(repo_names)} for arch {arch}")

        return sorted(rpm_info_list)

    @start_as_current_span_async(TRACER, "lockfile.fetch_rpms_info")
    async def fetch_rpms_info(
        self, arches: list[str], repositories: set[str], rpm_names: set[str]
    ) -> dict[str, list[RpmInfo]]:
        """
        Resolve RPM info across multiple architectures and repositories.

        Args:
            arches (list[str]): Target architectures.
            repositories (set[str]): Names of repositories to search.
            rpm_names (set[str]): Names or NVRs of RPMs to resolve.

        Returns:
            dict[str, list[RpmInfo]]: Mapping of architecture to resolved RPM metadata.
        """
        current_span = trace.get_current_span()
        current_span.set_attribute("lockfile.arches", ",".join(arches))
        current_span.set_attribute("lockfile.repositories", ",".join(sorted(repositories)))
        current_span.set_attribute("lockfile.rpm_count", len(rpm_names))
        current_span.set_attribute("lockfile.arch_count", len(arches))

        await asyncio.gather(*(self._load_repos(repositories, arch) for arch in arches))

        results = await asyncio.gather(
            *[
                asyncio.get_running_loop().run_in_executor(
                    None, self._fetch_rpms_info_per_arch, rpm_names, repositories, arch
                )
                for arch in arches
            ]
        )

        total_resolved = sum(len(result) for result in results)
        current_span.set_attribute("lockfile.total_resolved_packages", total_resolved)
        return dict(zip(arches, results))


class RPMLockfileGenerator:
    """
    Handles generation of a lockfile detailing RPM packages across architectures and repositories.

    The lockfile captures resolved package metadata, ensuring reproducibility by
    hashing the RPM input list to avoid redundant regenerations. Uses RpmInfoCollector
    for asynchronous RPM metadata retrieval and outputs YAML lockfiles along with digest files.
    """

    def __init__(self, repos: Repos, logger: Optional[Logger] = None, runtime=None):
        self.logger = logger or logutil.get_logger(__name__)
        self.builder = RpmInfoCollector(repos, self.logger)
        self.runtime = runtime

    def _get_target_branch(self, distgit_key: str) -> Optional[str]:
        """
        Construct target branch name using the same format as rebaser.

        Args:
            distgit_key (str): Distgit key to construct the target branch name.

        Returns:
            Optional[str]: Target branch name, or None if runtime is not available.
        """
        if not self.runtime:
            return None
        return "art-{group}-assembly-{assembly_name}-dgk-{distgit_key}".format(
            group=self.runtime.group,
            assembly_name=self.runtime.assembly,
            distgit_key=distgit_key,
        )

    async def should_generate_lockfile(
        self, image_meta: ImageMetadata, dest_dir: Path, filename: str = DEFAULT_LOCKFILE_NAME
    ) -> tuple[bool, set[str]]:
        """
        Determine if lockfile generation is needed and return RPMs to install.

        Checks lockfile generation configuration, repository availability, and
        digest optimization to avoid unnecessary regeneration.

        Args:
            image_meta: Image metadata containing configuration and distgit info
            dest_dir: Destination directory for lockfile output
            filename: Lockfile filename, defaults to rpms.lock.yaml

        Returns:
            tuple[bool, set[str]]: (should_generate, rpms_to_install)
        """
        if not image_meta.is_lockfile_generation_enabled():
            return False, set()

        enabled_repos = image_meta.get_enabled_repos()
        if not enabled_repos:
            self.logger.info(f"Skipping lockfile generation for {image_meta.distgit_key}: repositories set is empty")
            return False, set()

        rpms_to_install = await image_meta.fetch_rpms_from_build()
        if not rpms_to_install:
            self.logger.warning(
                f'Empty RPM list to install for {image_meta.distgit_key}; all required RPMs may be inherited? Skipping lockfile generation.'
            )
            return False, set()
        else:
            self.logger.info(f'{image_meta.distgit_key} image needs to install {len(rpms_to_install)} rpms')

        if image_meta.is_lockfile_force_enabled():
            self.logger.info(
                f"Force flag set for {image_meta.distgit_key}. Regenerating lockfile without digest check."
            )
            return True, rpms_to_install

        fingerprint = self._compute_hash(rpms_to_install)
        digest_path = dest_dir / f'{filename}.digest'

        old_fingerprint = None
        if digest_path.exists():
            try:
                with open(digest_path, 'r') as f:
                    old_fingerprint = f.read().strip()
            except Exception as e:
                self.logger.info(f"Failed to read local digest file '{digest_path}': {e}")

        if old_fingerprint is None:
            old_fingerprint = await self._get_digest_from_target_branch(digest_path, image_meta)
            if old_fingerprint:
                self.logger.info(f"Found digest in target branch for {image_meta.distgit_key}")

        if old_fingerprint == fingerprint:
            self.logger.info(f"No changes in RPM list for {image_meta.distgit_key}. Skipping lockfile generation.")
            return False, rpms_to_install
        elif old_fingerprint:
            self.logger.info(f"RPM list changed for {image_meta.distgit_key}. Regenerating lockfile.")

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
                self.logger.info(f"Image {image_meta.distgit_key} skipping lockfile generation (digest unchanged)")

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
                for arch, repos_for_arch in repos_by_arch.items():
                    await self.builder._load_repos(repos_for_arch, arch)
            else:
                self.logger.info("No repositories to load for lockfile generation")
        else:
            self.logger.info("No images need lockfile generation - skipping repository loading")

    async def _sync_from_upstream_if_needed(
        self, image_meta: ImageMetadata, dest_dir: Path, filename: str = DEFAULT_LOCKFILE_NAME
    ) -> None:
        """
        Download existing lockfile and digest from target branch if available.

        Attempts to fetch both lockfile and digest from the upstream target branch
        to enable digest optimization for unchanged RPM lists.

        Args:
            image_meta: Image metadata containing distgit information
            dest_dir: Local directory to write downloaded files
            filename: Lockfile filename, defaults to rpms.lock.yaml
        """
        lockfile_path = dest_dir / filename
        digest_path = dest_dir / f'{filename}.digest'

        upstream_digest = await self._get_digest_from_target_branch(digest_path, image_meta)
        if upstream_digest:
            try:
                digest_path.write_text(upstream_digest)
                self.logger.info(f"Downloaded digest file to {digest_path}")
            except Exception as e:
                self.logger.warning(f"Failed to write digest file '{digest_path}': {e}")

        upstream_lockfile = await self._get_lockfile_from_target_branch(lockfile_path, image_meta)
        if upstream_lockfile:
            try:
                lockfile_path.write_text(upstream_lockfile)
                self.logger.info(f"Downloaded lockfile to {lockfile_path}")
            except Exception as e:
                self.logger.warning(f"Failed to write lockfile '{lockfile_path}': {e}")
        else:
            self.logger.warning(f"Could not download lockfile from upstream branch for {image_meta.distgit_key}")

    @staticmethod
    def _compute_hash(rpms: set[str]) -> str:
        """
        Compute a SHA256 hash fingerprint from a set of RPM names.

        Args:
            rpms (set[str]): Set of RPM names.

        Returns:
            str: SHA256 hex digest of sorted RPM names joined by newline.
        """
        sorted_items = sorted(rpms)
        joined = '\n'.join(sorted_items)
        return hashlib.sha256(joined.encode('utf-8')).hexdigest()

    async def _get_digest_from_target_branch(self, digest_path: Path, image_meta: ImageMetadata) -> Optional[str]:
        """
        Fetch digest file content from the target art branch.

        Args:
            digest_path (Path): Path to the digest file relative to repo root.
            image_meta (ImageMetadata): Image metadata containing distgit information.

        Returns:
            Optional[str]: Digest content if found, None otherwise.
        """
        target_branch = self._get_target_branch(image_meta.distgit_key)
        if not target_branch:
            return None

        try:
            git_url = str(image_meta.config.content.source.git.url)
            digest_filename = digest_path.name

            server, org, repo_name = split_git_url(git_url)
            if 'github.com' not in server:
                self.logger.debug(f"Non-GitHub repository, skipping: {git_url}")
                return None

            token = os.environ.get('GITHUB_TOKEN')
            if not token:
                self.logger.debug("GITHUB_TOKEN not available")
                return None

            async with aiohttp.ClientSession() as session:
                with tempfile.NamedTemporaryFile(mode='w+') as tmp:
                    await download_file_from_github(git_url, target_branch, digest_filename, token, tmp.name, session)
                    tmp.seek(0)
                    return tmp.read().strip()

        except Exception as e:
            self.logger.debug(f"Error fetching digest from GitHub branch '{target_branch}': {e}")
            return None

    async def _get_lockfile_from_target_branch(self, lockfile_path: Path, image_meta: 'ImageMetadata') -> Optional[str]:
        """
        Fetch lockfile content from the target art branch.

        Args:
            lockfile_path (Path): Path to the lockfile relative to repo root.
            image_meta (ImageMetadata): Image metadata containing distgit information.

        Returns:
            Optional[str]: Lockfile content if found, None otherwise.
        """
        target_branch = self._get_target_branch(image_meta.distgit_key)
        if not target_branch:
            return None

        try:
            git_url = image_meta.distgit_remote_url()
            lockfile_filename = lockfile_path.name

            server, org, repo_name = split_git_url(git_url)
            if 'github.com' not in server:
                self.logger.debug(f"Non-GitHub repository, skipping: {git_url}")
                return None

            token = os.environ.get('GITHUB_TOKEN')
            if not token:
                self.logger.debug("GITHUB_TOKEN not available")
                return None

            async with aiohttp.ClientSession() as session:
                with tempfile.NamedTemporaryFile(mode='w+') as tmp:
                    await download_file_from_github(git_url, target_branch, lockfile_filename, token, tmp.name, session)
                    tmp.seek(0)
                    return tmp.read()

        except Exception as e:
            self.logger.debug(f"Error fetching lockfile from GitHub branch '{target_branch}': {e}")
            return None

    async def generate_lockfile(
        self, image_meta: ImageMetadata, dest_dir: Path, filename: str = DEFAULT_LOCKFILE_NAME
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
            if rpms_to_install:
                await self._sync_from_upstream_if_needed(image_meta, dest_dir, filename)
            return

        arches = image_meta.get_arches()
        enabled_repos = image_meta.get_enabled_repos()

        lockfile = {
            "lockfileVersion": 1,
            "lockfileVendor": "redhat",
            "arches": [],
        }

        rpms_info_by_arch = await self.builder.fetch_rpms_info(arches, enabled_repos, rpms_to_install)
        for arch, rpm_list in rpms_info_by_arch.items():
            lockfile["arches"].append(
                {
                    "arch": arch,
                    "packages": [rpm.to_dict() for rpm in rpm_list],
                    "module_metadata": [],
                }
            )

        lockfile_path = dest_dir / filename
        self._write_yaml(lockfile, lockfile_path)

        fingerprint = self._compute_hash(rpms_to_install)
        digest_path = dest_dir / f'{filename}.digest'
        try:
            digest_path.write_text(fingerprint)
        except Exception as e:
            self.logger.warning(f"Failed to write digest file '{digest_path}': {e}")

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
