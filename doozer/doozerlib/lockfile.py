import asyncio
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from functools import total_ordering
from logging import Logger
from pathlib import Path
from typing import Any, Dict, Optional, Set

import yaml

# Removed unused import 'List' from aiohttp_retry
from artcommonlib import exectools, logutil
from artcommonlib.logutil import EntityLoggingAdapter
from artcommonlib.rpm_utils import compare_nvr

from doozerlib.repodata import Repodata, Rpm
from doozerlib.repos import Repos


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

    async def _load_repos(self, requested_repos: set[str], arch: str, contextual_logger: Optional[Logger] = None):
        """
        Load repodata synchronously for the given repositories and architecture.

        Only repositories not already loaded in `self.loaded_repos` will be fetched.
        Logs a summary of which repos are skipped and which are loaded.

        Args:
            requested_repos (set[str]): Set of repository names to load.
            arch (str): Architecture identifier for fetching repodata.
            contextual_logger (Optional[Logger]): Optional contextual logger for this operation.
        """
        logger = contextual_logger or self.logger

        not_yet_loaded = {repo for repo in requested_repos if f'{repo}-{arch}' not in self.loaded_repos}
        already_loaded = requested_repos - not_yet_loaded

        if already_loaded:
            logger.info(f"Repos already loaded, skipping: {', '.join(sorted(already_loaded))} for arch {arch}")

        enabled_repos = {r.name for r in self.repos.values() if r.enabled}
        repos_to_fetch = sorted(enabled_repos | set(not_yet_loaded))

        if not repos_to_fetch:
            logger.info("No new repos to load.")
            return

        logger.info(f"Loading repos: {', '.join(repos_to_fetch)} for arch {arch}")

        entity = getattr(logger, 'extra', {}).get('entity') if hasattr(logger, 'extra') else None
        repodatas = await asyncio.gather(
            *(self.repos[repo_name].get_repodata(arch, entity=entity) for repo_name in repos_to_fetch)
        )

        self.loaded_repos.update({r.name: r for r in repodatas})
        logger.info(f"Finished loading repos: {', '.join(repos_to_fetch)} for arch {arch}")

    def _fetch_rpms_info_per_arch(
        self, rpm_names: set[str], repo_names: set[str], arch: str, contextual_logger: Optional[Logger] = None
    ) -> list[RpmInfo]:
        """
        Resolve RPM metadata for a specific architecture from the given repodata names.

        Args:
            rpm_names (set[str]): RPM names or NVRs to resolve.
            repo_names (set[str]): Names of repodata sources to search.
            arch (str): Target architecture.
            contextual_logger (Optional[Logger]): Optional contextual logger for this operation.

        Returns:
            list[RpmInfo]: Resolved RPM package metadata.
        """
        logger = contextual_logger or self.logger

        rpm_info_list = []
        unresolved_rpms = set(rpm_names)
        missing_rpms = unresolved_rpms

        for repo_name in repo_names:
            repodata = self.loaded_repos.get(f'{repo_name}-{arch}')
            if repodata is None:
                logger.error(f'repodata {repo_name}-{arch} not found while fetching rpms, it should be loaded by now')
                continue

            repo = self.repos._repos[repo_name]
            if repo is None:
                logger.error(f'repo {repo_name} not found')
                continue

            found_rpms, missing_rpms = repodata.get_rpms(unresolved_rpms)
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
            logger.warning(f"Could not find {','.join(missing_rpms)} in {', '.join(repo_names)} for arch {arch}")

        return sorted(rpm_info_list)

    async def fetch_rpms_info(
        self, arches: list[str], repositories: set[str], rpm_names: set[str], contextual_logger: Optional[Logger] = None
    ) -> dict[str, list[RpmInfo]]:
        """
        Resolve RPM info across multiple architectures and repositories.

        Args:
            arches (list[str]): Target architectures.
            repositories (set[str]): Names of repositories to search.
            rpm_names (set[str]): Names or NVRs of RPMs to resolve.
            contextual_logger (Optional[Logger]): Optional contextual logger for this operation.

        Returns:
            dict[str, list[RpmInfo]]: Mapping of architecture to resolved RPM metadata.
        """
        logger = contextual_logger or self.logger

        await asyncio.gather(*(self._load_repos(repositories, arch, contextual_logger=logger) for arch in arches))

        results = await asyncio.gather(
            *[
                asyncio.get_running_loop().run_in_executor(
                    None, self._fetch_rpms_info_per_arch, rpm_names, repositories, arch, logger
                )
                for arch in arches
            ]
        )

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

    def _get_digest_from_target_branch(
        self, digest_path: Path, distgit_key: str, contextual_logger: Optional[Logger] = None
    ) -> Optional[str]:
        """
        Fetch digest file content from the target art branch.

        Args:
            digest_path (Path): Path to the digest file relative to repo root.
            distgit_key (str): Distgit key to construct the target branch name.
            contextual_logger (Optional[Logger]): Optional contextual logger for this operation.

        Returns:
            Optional[str]: Digest content if found, None otherwise.
        """
        logger = contextual_logger or self.logger
        target_branch = self._get_target_branch(distgit_key)
        if not target_branch:
            return None

        try:
            # Use just the filename since the digest file is in the root of the git repo
            digest_filename = digest_path.name

            # Use exectools to run git show command
            rc, stdout, stderr = exectools.cmd_gather(['git', 'show', f'{target_branch}:{digest_filename}'])
            if rc == 0:
                return stdout.strip()
            else:
                logger.debug(f"Could not fetch digest file '{digest_filename}' from branch '{target_branch}': {stderr}")
                return None
        except Exception as e:
            logger.debug(f"Error fetching digest from git branch '{target_branch}': {e}")
            return None

    def _get_lockfile_from_target_branch(
        self, lockfile_path: Path, distgit_key: str, contextual_logger: Optional[Logger] = None
    ) -> Optional[str]:
        """
        Fetch lockfile content from the target art branch.

        Args:
            lockfile_path (Path): Path to the lockfile relative to repo root.
            distgit_key (str): Distgit key to construct the target branch name.
            contextual_logger (Optional[Logger]): Optional contextual logger for this operation.

        Returns:
            Optional[str]: Lockfile content if found, None otherwise.
        """
        logger = contextual_logger or self.logger
        target_branch = self._get_target_branch(distgit_key)
        if not target_branch:
            return None

        try:
            # Use just the filename since the lockfile is in the root of the git repo
            lockfile_filename = lockfile_path.name

            # Use exectools to run git show command
            rc, stdout, stderr = exectools.cmd_gather(['git', 'show', f'{target_branch}:{lockfile_filename}'])
            if rc == 0:
                return stdout
            else:
                logger.debug(f"Could not fetch lockfile '{lockfile_filename}' from branch '{target_branch}': {stderr}")
                return None
        except Exception as e:
            logger.debug(f"Error fetching lockfile from git branch '{target_branch}': {e}")
            return None

    async def generate_lockfile(
        self,
        arches: list[str],
        repositories: set[str],
        rpms: set[str],
        path: Path = Path('.'),
        filename: str = 'rpms.lock.yaml',
        distgit_key: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """
        Generate a lockfile YAML containing RPM info for specified arches and repos.

        Skips generation if RPM fingerprint digest matches an existing one, unless force=True.

        Args:
            arches (list[str]): Target architectures.
            repositories (set[str]): Repository names.
            rpms (set[str]): RPM names or NVRs to lock.
            path (str): Directory path to save lockfile and digest.
            filename (str): Lockfile filename.
            distgit_key (Optional[str]): Distgit key for fetching digest from target branch.
            force (bool): If True, ignore digest comparison and force regeneration.
        """
        logger = EntityLoggingAdapter(self.logger, {'entity': distgit_key}) if distgit_key else self.logger

        if not repositories:
            logger.warning("Skipping lockfile generation: repositories set is empty")
            return

        fingerprint = self._compute_hash(rpms)
        lockfile_path = path / filename
        digest_path = path / f'{filename}.digest'

        # Skip digest check if force=True
        if force:
            logger.info("Force flag set. Regenerating lockfile without digest check.")
        else:
            old_fingerprint = None
            if digest_path.exists():
                try:
                    with open(digest_path, 'r') as f:
                        old_fingerprint = f.read().strip()
                except Exception as e:
                    logger.warning(f"Failed to read local digest file '{digest_path}': {e}")

            upstream_digest_found = False
            if old_fingerprint is None and distgit_key:
                old_fingerprint = self._get_digest_from_target_branch(digest_path, distgit_key, logger)
                if old_fingerprint:
                    logger.info(f"Found digest in target branch for {distgit_key}")
                    upstream_digest_found = True

            if old_fingerprint == fingerprint:
                if upstream_digest_found and distgit_key:
                    logger.info("No changes in RPM list. Downloading digest and lockfile from upstream.")

                    if old_fingerprint:
                        try:
                            digest_path.write_text(old_fingerprint)
                            logger.info(f"Downloaded digest file to {digest_path}")
                        except Exception as e:
                            logger.warning(f"Failed to write digest file '{digest_path}': {e}")

                    upstream_lockfile = self._get_lockfile_from_target_branch(lockfile_path, distgit_key, logger)
                    if upstream_lockfile:
                        try:
                            lockfile_path.write_text(upstream_lockfile)
                            logger.info(f"Downloaded lockfile to {lockfile_path}")
                        except Exception as e:
                            logger.warning(f"Failed to write lockfile '{lockfile_path}': {e}")
                    else:
                        logger.warning(f"Could not download lockfile from upstream branch for {distgit_key}")
                else:
                    logger.info("No changes in RPM list. Skipping lockfile generation.")
                return
            elif old_fingerprint:
                logger.info("RPM list changed. Regenerating lockfile.")

        lockfile = {
            "lockfileVersion": 1,
            "lockfileVendor": "redhat",
            "arches": [],
        }

        rpms_info_by_arch = await self.builder.fetch_rpms_info(arches, repositories, rpms, contextual_logger=logger)
        for arch, rpm_list in rpms_info_by_arch.items():
            lockfile["arches"].append(
                {
                    "arch": arch,
                    "packages": [rpm.to_dict() for rpm in rpm_list],
                    "module_metadata": [],
                }
            )

        self._write_yaml(lockfile, lockfile_path)

        try:
            digest_path.write_text(fingerprint)
        except Exception as e:
            logger.warning(f"Failed to write digest file '{digest_path}': {e}")

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
