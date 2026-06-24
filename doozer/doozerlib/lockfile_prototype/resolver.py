"""
RPM resolution via rpm-lockfile-prototype subprocess.

Invokes the rpm-lockfile-prototype tool through system Python so
that python3-dnf (a system package built for the system Python) is
available. The venv Python may be a different minor version where
dnf cannot be imported.
"""

import logging
import re
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml
from artcommonlib import logutil
from artcommonlib.exectools import cmd_gather_async

from doozerlib.lockfile_prototype.constants import (
    DEFAULT_RPM_INFILE_NAME,
    DEFAULT_RPM_LOCKFILE_NAME,
    RPM_LOCKFILE_ENTRY_POINT,
    RPMDB_CACHE_ERROR_PATTERNS,
    RPMDB_CACHE_PATH,
    SYSTEM_PYTHON,
    VALID_PKG_NAME,
)
from doozerlib.lockfile_prototype.models import LockfileData, RpmsInConfig
from doozerlib.lockfile_prototype.utils import build_env


class RpmResolver:
    """
    Invokes rpm-lockfile-prototype via system Python subprocess.

    Maintains a persistent DNF repodata cache directory across
    resolve() calls so that repeated runs against the same repos
    (common during multi-image rebases) skip redundant downloads.
    """

    def __init__(self, working_dir: Path, logger: logging.Logger | None = None, cache_dir: str | None = None):
        self.logger = logger or logutil.get_logger(__name__)
        self._working_dir = str(working_dir)
        self._cache_dir_owner = (
            None if cache_dir else TemporaryDirectory(prefix="rpm-lockfile-cache-", dir=self._working_dir)
        )
        self._cache_path = cache_dir or self._cache_dir_owner.name

    async def resolve(
        self,
        config: RpmsInConfig,
        image_pullspec: str | None = None,
    ) -> LockfileData:
        """
        Resolve RPM packages by running rpm-lockfile-prototype via
        system Python as a subprocess.

        On RPMDB corruption errors, clears the cached RPMDB for the
        image and retries once before raising.

        Arg(s):
            config (RpmsInConfig): Input configuration.
            image_pullspec (str | None): Base image for rpmdb context.
                None means bare resolution.
        Return Value(s):
            LockfileData: Resolved lockfile.
        """
        with TemporaryDirectory(dir=self._working_dir) as tmpdir:
            in_file = Path(tmpdir) / DEFAULT_RPM_INFILE_NAME
            out_file = Path(tmpdir) / DEFAULT_RPM_LOCKFILE_NAME

            in_file.write_text(yaml.safe_dump(config.model_dump(exclude_none=True), sort_keys=False))

            cmd = [SYSTEM_PYTHON, "-c", RPM_LOCKFILE_ENTRY_POINT]
            if image_pullspec:
                cmd.extend(["--image", image_pullspec])
            else:
                cmd.append("--bare")
            cmd.extend(["--outfile", str(out_file), str(in_file)])

            env = build_env()
            env["RPM_LOCKFILE_PROTOTYPE_DNF_CACHE"] = self._cache_path
            env["TMPDIR"] = self._working_dir
            rc, _, stderr = await cmd_gather_async(cmd, check=False, env=env)

            if rc != 0:
                if image_pullspec and self._is_rpmdb_corrupt(stderr):
                    self._clear_rpmdb_cache(image_pullspec)
                    self.logger.info("Retrying rpm-lockfile-prototype after RPMDB cache error")
                    retry_rc, _, retry_stderr = await cmd_gather_async(cmd, check=False, env=env)
                    if retry_rc == 0:
                        return LockfileData.model_validate(yaml.safe_load(out_file.read_text()))
                    self.logger.warning("Retry also failed (exit code %d): %s", retry_rc, retry_stderr)

                raise RuntimeError(f"rpm-lockfile-prototype failed (exit code {rc}): {stderr}")

            return LockfileData.model_validate(yaml.safe_load(out_file.read_text()))

    @staticmethod
    def _is_rpmdb_corrupt(stderr: str) -> bool:
        """
        Check if stderr indicates a corrupt RPMDB cache.

        Arg(s):
            stderr (str): Standard error output from rpm-lockfile-prototype.
        Return Value(s):
            bool: True if corruption patterns are detected.
        """
        return any(pattern in stderr for pattern in RPMDB_CACHE_ERROR_PATTERNS)

    def _clear_rpmdb_cache(self, image_pullspec: str) -> bool:
        """
        Delete cached RPMDB entries for an image digest across all arches.

        Arg(s):
            image_pullspec (str): Image pullspec containing a digest
                (e.g. "registry.example.com/repo@sha256:abc123...").
        Return Value(s):
            bool: True if any cache entries were deleted.
        """
        match = re.search(r"@(sha256:[a-f0-9]+)", image_pullspec)
        if not match:
            self.logger.warning("Cannot extract digest from pullspec %s, skipping RPMDB cache cleanup", image_pullspec)
            return False

        digest = match.group(1)
        cleared = False

        if not RPMDB_CACHE_PATH.is_dir():
            return False

        for arch_dir in RPMDB_CACHE_PATH.iterdir():
            cache_entry = arch_dir / digest
            if cache_entry.is_dir():
                self.logger.warning("Clearing corrupt RPMDB cache: %s", cache_entry)
                try:
                    shutil.rmtree(cache_entry)
                    cleared = True
                except FileNotFoundError:
                    continue
                except OSError as ex:
                    self.logger.warning("Failed to remove RPMDB cache %s: %s", cache_entry, ex)

        return cleared

    @staticmethod
    def parse_missing_packages(error_text: str) -> set[str]:
        """
        Parse missing package names from rpm-lockfile-prototype error output.

        Handles the CLI format ("missing packages: X, Y"), DNF install/upgrade
        errors ("No match for argument: X"), and DNF reinstall errors
        ("no package matched: X").

        Arg(s):
            error_text (str): Error message from rpm-lockfile-prototype.
        Return Value(s):
            set[str]: Set of package names that were not found.
        """
        missing: set[str] = set()
        for line in error_text.splitlines():
            m = re.search(r"missing packages:\s*(.+)", line.strip())
            if m:
                missing.update(pkg.strip() for pkg in m.group(1).split(","))
            m = re.search(r"No match for argument:\s*(\S+)", line.strip())
            if m:
                missing.add(m.group(1).strip().rstrip(":"))
            m = re.search(r"no package matched:\s*(\S+)", line.strip())
            if m:
                missing.add(m.group(1).strip().rstrip(":"))
        return {p for p in missing if VALID_PKG_NAME.match(p)}
