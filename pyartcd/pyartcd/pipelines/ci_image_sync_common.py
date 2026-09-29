"""
Shared base class for the CI image sync pipelines (sync-ci-images, mirror-images-to-ci, ...).

These pipelines all clone ocp-build-data, build a registry credential config, and invoke
`doozer images:streams ...` subcommands against it. This base class centralizes that shared
plumbing so each pipeline only needs to implement its own doozer subcommand(s).
"""

import asyncio
import os
import re
import shutil
from pathlib import Path

from artcommonlib import exectools
from artcommonlib.constants import (
    KONFLUX_DEFAULT_FBC_REPO,
    KONFLUX_DEFAULT_IMAGE_REPO,
    KONFLUX_DEFAULT_IMAGE_SHARE_REPO,
    REGISTRY_CI_OPENSHIFT,
    REGISTRY_QUAY_OCP_RELEASE_DEV,
    REGISTRY_QUAY_OPENSHIFT,
    REGISTRY_REDHAT_IO,
)
from artcommonlib.github_auth import get_github_git_auth_env
from artcommonlib.registry_config import RegistryConfig, RegistryCredential


class CIImageSyncPipelineBase:
    """
    Base class providing shared ocp-build-data clone, registry credential, and doozer
    invocation logic for CI image sync pipelines.

    Subclasses are expected to set: self.runtime, self._logger, self.version,
    self.data_path, self.data_gitref, self.assembly, self.only_stream, self.images.
    """

    BUILD_SYSTEM = "konflux"
    GIT_CLONE_TIMEOUT = 300

    @property
    def _working_dir(self) -> str:
        """Get doozer working directory path for this version."""
        return f"{self.runtime.doozer_working}/wd-{self.version}"

    @staticmethod
    def _get_required_env(var_name: str) -> str:
        """
        Get required environment variable with validation.

        Args:
            var_name: Environment variable name

        Returns:
            Environment variable value

        Raises:
            ValueError: If environment variable not set
            FileNotFoundError: If file path doesn't exist (for *_FILE vars)
        """
        value = os.getenv(var_name)
        if not value:
            raise ValueError(f"Required environment variable {var_name} not set")

        # For file paths, verify existence
        if var_name.endswith('_FILE') or var_name == 'KUBECONFIG':
            if not Path(value).exists():
                raise FileNotFoundError(f"{var_name} file not found: {value}")

        return value

    def _create_registry_config(self) -> RegistryConfig:
        """
        Create RegistryConfig with all required registry credentials.

        Builds credential configuration from Jenkins-provided environment variables.

        Returns:
            RegistryConfig context manager

        Raises:
            ValueError: If required credentials are missing
            FileNotFoundError: If credential files don't exist
        """
        # Validate and retrieve all required credentials
        quay_auth_file = self._get_required_env('QUAY_AUTH_FILE')
        kubeconfig = self._get_required_env('KUBECONFIG')
        qci_user = self._get_required_env('QCI_USER')
        qci_password = self._get_required_env('QCI_PASSWORD')

        # Build RegistryConfig using constants from artcommonlib
        return RegistryConfig(
            source_files=[quay_auth_file],
            kubeconfig=kubeconfig,
            registries=[
                REGISTRY_CI_OPENSHIFT,
                REGISTRY_QUAY_OCP_RELEASE_DEV,
                KONFLUX_DEFAULT_IMAGE_REPO,
                KONFLUX_DEFAULT_IMAGE_SHARE_REPO,
                KONFLUX_DEFAULT_FBC_REPO,
                REGISTRY_REDHAT_IO,
            ],
            credentials=[
                RegistryCredential(REGISTRY_QUAY_OPENSHIFT, qci_user, qci_password),
            ],
        )

    def _get_gitref(self, version: str) -> str:
        """Get gitref to use: data_gitref if provided, otherwise version-specific branch."""
        return self.data_gitref or f"openshift-{version}"

    @staticmethod
    def _is_commit_sha(gitref: str) -> bool:
        """Check if gitref is a commit SHA (7-40 char hex string)."""
        return bool(gitref and re.fullmatch(r"[0-9a-f]{7,40}", gitref.lower()))

    async def _clone_ocp_build_data(self, version: str) -> Path:
        """
        Clone ocp-build-data repository for specified version.

        Uses GitHub App authentication for private repos.
        Supports branches, tags, and raw commit SHAs.

        Args:
            version: OCP version (e.g., "4.17")

        Returns:
            Path to cloned directory

        Raises:
            RuntimeError: If git clone fails or times out
        """
        group = f"openshift-{version}"
        group_dir = Path(self.runtime.working_dir) / group

        # Remove stale clone if exists
        if group_dir.exists():
            shutil.rmtree(group_dir)

        gitref = self._get_gitref(version)

        # Get GitHub App authentication for git commands (supports private repos)
        git_env = get_github_git_auth_env(url=self.data_path)

        self._logger.info(f"Cloning ocp-build-data for {group}")

        try:
            if self._is_commit_sha(gitref):
                # git clone --branch doesn't accept commit SHAs; clone then checkout separately
                self._logger.info(f"{version}: Cloning and checking out commit SHA {gitref}")
                clone_cmd = f"git clone {self.data_path} {group_dir}"
                rc, _, _ = await asyncio.wait_for(
                    exectools.cmd_gather_async(clone_cmd, env=git_env, stdout=None, stderr=None),
                    timeout=self.GIT_CLONE_TIMEOUT,
                )
                if rc != 0:
                    raise RuntimeError(f"Git clone failed for {group}")
                checkout_cmd = f"git -C {group_dir} checkout {gitref}"
                rc, _, _ = await asyncio.wait_for(
                    exectools.cmd_gather_async(checkout_cmd, env=git_env, stdout=None, stderr=None),
                    timeout=60,
                )
                if rc != 0:
                    raise RuntimeError(f"Git checkout {gitref} failed for {group}")
            else:
                # Standard clone for branches and tags
                cmd = f"git clone {self.data_path} --branch {gitref} --single-branch --depth 1 {group_dir}"
                rc, _, _ = await asyncio.wait_for(
                    exectools.cmd_gather_async(cmd, env=git_env, stdout=None, stderr=None),
                    timeout=self.GIT_CLONE_TIMEOUT,
                )
                if rc != 0:
                    raise RuntimeError(f"Git clone failed for {group}")
        except asyncio.TimeoutError as e:
            raise RuntimeError(f"Git clone timed out after {self.GIT_CLONE_TIMEOUT}s for {group}") from e

        return group_dir

    async def _run_doozer_command(
        self, doozer_opts: str, subcommand: str, extra_args: str = "", check: bool = True
    ) -> tuple[int, str, str]:
        """
        Execute a doozer command with standard options.

        Args:
            doozer_opts: Doozer global options (--working-dir, --group, etc.)
            subcommand: Doozer subcommand (e.g., "images:streams mirror")
            extra_args: Additional arguments for the subcommand
            check: Raise exception on non-zero return code

        Returns:
            Tuple of (return_code, stdout, stderr)

        Raises:
            Exception: If check=True and command fails
        """
        cmd = f"doozer {doozer_opts} {subcommand} {extra_args}".strip()

        self._logger.info(f"Running doozer command: {cmd}")

        # Stream output to Jenkins console in real-time
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=check, stdout=None, stderr=None)

        return rc, stdout, stderr

    def _build_doozer_options(self, group_dir: Path, auth_file: str) -> str:
        """Build doozer global options for all commands."""
        group = f"openshift-{self.version}"
        doozer_opts = (
            f"--working-dir {self._working_dir} "
            f"--data-path {group_dir} "
            f"--group {group} "
            f"--assembly {self.assembly} "
            f"--latest-parent-version "
            f"--build-system {self.BUILD_SYSTEM} "
            f"--registry-config {auth_file}"
        )
        return doozer_opts

    @property
    def _stream_arg(self) -> str:
        """Return the --stream subcommand arg if only_stream is set, empty string otherwise."""
        return f"--stream {self.only_stream}" if self.only_stream else ""

    @property
    def _image_args(self) -> str:
        """Return --image args for each image distgit key, empty string if none."""
        return " ".join(f"--image {img}" for img in self.images) if self.images else ""

    @property
    def _filter_args(self) -> str:
        """Return combined --stream and --image subcommand args."""
        return f"{self._stream_arg} {self._image_args}".strip()

    def _cleanup(self, group_dir: "Path | None") -> None:
        """Remove temporary clone directory."""
        if group_dir and group_dir.exists():
            shutil.rmtree(group_dir)
            self._logger.info(f"{self.version}: Cleaned up clone directory")
