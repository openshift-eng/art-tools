"""
RPM resolution via rpm-lockfile-prototype container.

Invokes the rpm-lockfile-prototype tool inside a podman container
so that python3-dnf and all system dependencies are self-contained.
The container image is pulled from the art-cluster internal registry
on first use if not already present locally.
"""

import logging
import os
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
    JENKINS_CACHE_DIR,
    RPM_LOCKFILE_IMAGE,
    RPMDB_CACHE_ERROR_PATTERNS,
    RPMDB_CACHE_SUBDIR,
    VALID_PKG_NAME,
)
from doozerlib.lockfile_prototype.models import LockfileData, RpmsInConfig


class RpmResolver:
    """
    Invokes rpm-lockfile-prototype via podman container.

    Maintains a persistent DNF repodata cache directory across
    resolve() calls so that repeated runs against the same repos
    (common during multi-image rebases) skip redundant downloads.
    """

    def __init__(
        self,
        working_dir: Path | None = None,
        logger: logging.Logger | None = None,
        cache_dir: str | None = None,
        image: str | None = None,
    ):
        self.logger = logger or logutil.get_logger(__name__)
        self._working_dir = str(working_dir) if working_dir else None
        self._cache_dir_owner = (
            None if cache_dir else TemporaryDirectory(prefix="rpm-lockfile-cache-", dir=self._working_dir)
        )
        self._cache_path = cache_dir or self._cache_dir_owner.name
        self._image = image or RPM_LOCKFILE_IMAGE

        # In Jenkins, preserve the pre-containerization RPMDB path
        # (JENKINS_CACHE_DIR/rpmdbs) by mounting JENKINS_CACHE_DIR at
        # XDG_CACHE_HOME/rpm-lockfile-prototype inside the container.
        # Outside Jenkins, honour XDG_CACHE_HOME or ~/.cache.
        if os.environ.get("JENKINS_HOME"):
            self._rpmdb_cache_path = JENKINS_CACHE_DIR / "rpmdbs"
        else:
            xdg_env = os.environ.get("XDG_CACHE_HOME")
            if xdg_env and Path(xdg_env).is_absolute():
                xdg_cache_home = Path(xdg_env)
            else:
                if xdg_env:
                    self.logger.warning("XDG_CACHE_HOME is not absolute (%r), falling back to ~/.cache", xdg_env)
                xdg_cache_home = Path.home() / ".cache"
            self._rpmdb_cache_path = xdg_cache_home / RPMDB_CACHE_SUBDIR
        self.logger.info("RPMDB cache path: %s", self._rpmdb_cache_path)

    def _build_podman_cmd(
        self,
        tmpdir: str,
        image_pullspec: str | None,
        containerfile_path: str | None = None,
    ) -> list[str]:
        """
        Build the podman run command with volume mounts and env vars.

        Arg(s):
            tmpdir (str): Host temp directory with rpms.in.yaml.
            image_pullspec (str | None): Base image for rpmdb context.
            containerfile_path (str | None): Host path to the Containerfile
                to mount read-only at /work/Containerfile inside the container.
        Return Value(s):
            list[str]: Complete podman command.
        """
        cmd = ["podman", "run", "--rm"]

        # Work directory: rpms.in.yaml input and rpms.lock.yaml output.
        # :Z (exclusive SELinux label) is correct — each resolve() call gets its
        # own unique TemporaryDirectory, so no other container shares this path.
        cmd.extend(["-v", f"{tmpdir}:/work:Z"])

        # DNF repodata cache — :z (shared) so parallel resolve() calls can read
        # the same repodata without re-downloading.
        cmd.extend(["-v", f"{self._cache_path}:/cache:z"])
        cmd.extend(["-e", "RPM_LOCKFILE_PROTOTYPE_DNF_CACHE=/cache"])

        # RPMDB cache via XDG_CACHE_HOME. The tool stores rpmdbs at
        # $XDG_CACHE_HOME/rpm-lockfile-prototype/rpmdbs. In Jenkins, mount
        # JENKINS_CACHE_DIR at XDG_CACHE_HOME/rpm-lockfile-prototype so rpmdbs
        # land at JENKINS_CACHE_DIR/rpmdbs — same path as before containerization,
        # no cold cache on upgrade. Outside Jenkins, mount the XDG_CACHE_HOME parent.
        self._rpmdb_cache_path.mkdir(parents=True, exist_ok=True)
        container_xdg = "/rpmdb-cache"
        if os.environ.get("JENKINS_HOME"):
            cmd.extend(["-v", f"{JENKINS_CACHE_DIR}:{container_xdg}/rpm-lockfile-prototype:z"])
        else:
            cmd.extend(["-v", f"{self._rpmdb_cache_path.parent.parent}:{container_xdg}:z"])
        cmd.extend(["-e", f"XDG_CACHE_HOME={container_xdg}"])

        # Mount host entitlement certs for accessing protected repos.
        # Use :ro without :z — SELinux won't allow relabeling system dirs.
        for host_path in ("/etc/pki/entitlement", "/etc/rhsm/ca", "/etc/pki/rpm-gpg"):
            if Path(host_path).is_dir():
                cmd.extend(["-v", f"{host_path}:{host_path}:ro"])

        # Registry auth
        auth_file = os.environ.get("QUAY_AUTH_FILE") or os.environ.get("REGISTRY_AUTH_FILE")
        if auth_file:
            cmd.extend(["-v", f"{auth_file}:/auth/auth.json:ro,z"])
            cmd.extend(["-e", "REGISTRY_AUTH_FILE=/auth/auth.json"])

        # Containerfile — mounted read-only so packagesFromContainerfile can read it.
        # The config references it as "Containerfile" (relative to /work/rpms.in.yaml).
        if containerfile_path:
            cmd.extend(["-v", f"{containerfile_path}:/work/Containerfile:ro,Z"])

        # Image name
        cmd.append(self._image)

        # Tool arguments
        if image_pullspec:
            cmd.extend(["--image", image_pullspec])
        else:
            cmd.append("--bare")
        cmd.extend(["--outfile", "/work/" + DEFAULT_RPM_LOCKFILE_NAME, "/work/" + DEFAULT_RPM_INFILE_NAME])

        return cmd

    async def resolve(
        self,
        config: RpmsInConfig,
        image_pullspec: str | None = None,
        containerfile_path: str | None = None,
        stage_num: int | None = None,
    ) -> LockfileData:
        """
        Resolve RPM packages by running rpm-lockfile-prototype in a
        podman container.

        When containerfile_path is set, the tool extracts packages from
        the Dockerfile's RUN commands via packagesFromContainerfile.
        This works alongside --image: the image controls the rpmdb while
        packagesFromContainerfile controls package extraction.

        On RPMDB corruption errors, clears the cached RPMDB for the
        image and retries once before raising.

        Arg(s):
            config (RpmsInConfig): Input configuration.
            image_pullspec (str | None): Base image for rpmdb context.
                None means bare resolution.
            containerfile_path (str | None): Absolute path to the
                Dockerfile for automatic package extraction.
            stage_num (int | None): 1-indexed stage number to extract
                packages from (None = last stage).
        Return Value(s):
            LockfileData: Resolved lockfile.
        """
        if containerfile_path:
            # Use the container-side path — the Containerfile is mounted at
            # /work/Containerfile by _build_podman_cmd. "Containerfile" is
            # relative to the input file at /work/rpms.in.yaml.
            pfc_spec: dict = {"file": "Containerfile"}
            if stage_num is not None:
                pfc_spec["stageNum"] = stage_num
            config.packagesFromContainerfile = pfc_spec

        with TemporaryDirectory(dir=self._working_dir) as tmpdir:
            in_file = Path(tmpdir) / DEFAULT_RPM_INFILE_NAME
            out_file = Path(tmpdir) / DEFAULT_RPM_LOCKFILE_NAME

            in_file.write_text(yaml.safe_dump(config.model_dump(exclude_none=True), sort_keys=False))

            cmd = self._build_podman_cmd(tmpdir, image_pullspec, containerfile_path)
            rc, _, stderr = await cmd_gather_async(cmd, check=False)

            if rc != 0:
                if image_pullspec and self._is_rpmdb_corrupt(stderr):
                    self._clear_rpmdb_cache(image_pullspec)
                    self.logger.info("Retrying rpm-lockfile-prototype after RPMDB cache error")
                    rc, _, stderr = await cmd_gather_async(cmd, check=False)
                    if rc == 0:
                        return LockfileData.model_validate(yaml.safe_load(out_file.read_text()))
                    error_summary = stderr.strip().rsplit("\n", 1)[-1]
                    self.logger.warning("Retry also failed (exit code %d): %s", rc, error_summary)
                    self.logger.debug("Full retry stderr:\n%s", stderr)

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

        if not self._rpmdb_cache_path.is_dir():
            return False

        for arch_dir in self._rpmdb_cache_path.iterdir():
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
        # Also keep local RPM path globs (e.g. /root/rpmbuild/RPMS/x86_64/pkg*)
        # so the retry logic can add them to excludePackages. The upstream tool
        # uses simple set subtraction to apply excludePackages, so the exact
        # string extracted from the Containerfile will be excluded on retry.
        return {
            p for p in missing if VALID_PKG_NAME.match(p) or (p.startswith("/") and ("*" in p or p.endswith(".rpm")))
        }
