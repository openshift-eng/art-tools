"""
RPM resolution via rpm-lockfile-prototype subprocess.

Invokes the rpm-lockfile-prototype tool through system Python so
that python3-dnf (a system package built for the system Python) is
available. The venv Python may be a different minor version where
dnf cannot be imported.
"""

import logging
import re
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml
from artcommonlib import logutil
from artcommonlib.exectools import cmd_gather_async

from doozerlib.lockfile_prototype.constants import (
    DEFAULT_RPM_INFILE_NAME,
    DEFAULT_RPM_LOCKFILE_NAME,
    RPM_LOCKFILE_ENTRY_POINT,
    SYSTEM_PYTHON,
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

    def __init__(self, logger: logging.Logger | None = None, cache_dir: str | None = None):
        self.logger = logger or logutil.get_logger(__name__)
        self._cache_dir_owner = None if cache_dir else TemporaryDirectory(prefix="rpm-lockfile-cache-")
        self._cache_path = cache_dir or self._cache_dir_owner.name

    async def resolve(
        self,
        config: RpmsInConfig,
        image_pullspec: str | None = None,
    ) -> LockfileData:
        """
        Resolve RPM packages by running rpm-lockfile-prototype via
        system Python as a subprocess.

        Arg(s):
            config (RpmsInConfig): Input configuration.
            image_pullspec (str | None): Base image for rpmdb context.
                None means bare resolution.
        Return Value(s):
            LockfileData: Resolved lockfile.
        """
        with TemporaryDirectory() as tmpdir:
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
            rc, _, stderr = await cmd_gather_async(cmd, check=False, env=env)

            if rc != 0:
                raise RuntimeError(f"rpm-lockfile-prototype failed (exit code {rc}): {stderr}")

            return LockfileData.model_validate(yaml.safe_load(out_file.read_text()))

    @staticmethod
    def parse_missing_packages(error_text: str) -> set[str]:
        """
        Parse missing package names from rpm-lockfile-prototype error output.

        Handles both the CLI format ("missing packages: X, Y") and the
        DNF format ("No match for argument: X").

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
        return missing
