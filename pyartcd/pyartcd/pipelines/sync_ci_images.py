"""
sync-ci-images pipeline: Sync CI testing images to match ART production builds.

Ensures CI and production use the same base images, builder images, and configurations
to maximize CI signal fidelity.
"""

import asyncio
import os
import re
import shutil
from pathlib import Path

# Import for CLI registration
import click
from artcommonlib import exectools, redis
from artcommonlib.constants import (
    KONFLUX_DEFAULT_FBC_REPO,
    KONFLUX_DEFAULT_IMAGE_REPO,
    KONFLUX_DEFAULT_IMAGE_SHARE_REPO,
    REGISTRY_CI_OPENSHIFT,
    REGISTRY_QUAY_OCP_RELEASE_DEV,
    REGISTRY_QUAY_OPENSHIFT,
    REGISTRY_REDHAT_IO,
)
from artcommonlib.github_auth import get_github_client_for_org, get_github_git_auth_env
from artcommonlib.registry_config import RegistryConfig, RegistryCredential

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.runtime import Runtime


class SyncCIImagesPipeline:
    """
    Syncs CI testing images to match ART production builds.

    Migrated from aos-cd-jobs/scheduled-jobs/build/sync-ci-images/Jenkinsfile
    to Python with explicit RegistryConfig credential management.
    """

    # Constants from Jenkinsfile
    BUILD_SYSTEM = "konflux"
    WAIT_TIME_MINUTES = 20
    PR_INTERSTITIAL_SECONDS = 840
    GIT_CLONE_TIMEOUT = 300
    DOOZER_DEFAULT_TIMEOUT = 3600

    def __init__(
        self,
        runtime: Runtime,
        for_release: str,
        data_path: str = "",
        data_gitref: str = "",
        only_stream: str = "",
        add_labels: str = "",
        assembly: str = "stream",
        skip_prs: bool = False,
        skip_waits: bool = False,
        force_run: bool = False,
        update_images_only_when_missing: bool = False,
    ) -> None:
        """
        Initialize sync-ci-images pipeline.

        Args:
            runtime: PyARTCD runtime instance
            for_release: OCP version to sync (e.g., "4.17")
            data_path: ocp-build-data fork URL (default: official repo)
            data_gitref: ocp-build-data git branch/tag/sha (default: use version branch)
            only_stream: Specific stream from streams.yml.
            add_labels: Space-delimited labels to add to PRs
            assembly: Assembly name (default: "stream")
            skip_prs: Skip opening reconciliation PRs
            skip_waits: Skip sleep delays
            force_run: Run even if ocp-build-data unchanged
            update_images_only_when_missing: Only update images if missing
        """
        self.runtime = runtime
        self._logger = runtime.logger
        self.version = for_release
        self.data_path = data_path or OCP_BUILD_DATA_URL
        self.data_gitref = data_gitref
        self.only_stream = only_stream
        self.add_labels = add_labels
        self.assembly = assembly
        self.skip_prs = skip_prs
        self.skip_waits = skip_waits
        self.force_run = force_run
        self.update_images_only_when_missing = update_images_only_when_missing

        # Validate parameters
        self._validate_parameters()

    def _validate_parameters(self) -> None:
        """
        Validate parameter combinations and formats.

        Raises:
            ValueError: If parameters are invalid or incompatible
        """
        # Validate version format (required)
        if not self.version:
            raise ValueError("FOR_RELEASE is required")

        if not re.match(r'^\d+\.\d+$', self.version):
            raise ValueError(f"Invalid FOR_RELEASE format: {self.version}. Expected format: X.Y (e.g., 4.17)")

        # Auto-set SKIP_PRS if ONLY_STREAM is set.
        if self.only_stream and not self.skip_prs:
            self._logger.info("Setting SKIP_PRS to true because ONLY_STREAM is set")
            self.skip_prs = True

        # Validate assembly format (alphanumeric, dash, dot, underscore)
        if self.assembly and not re.match(r'^[\w\-\.]+$', self.assembly):
            raise ValueError(
                f"Invalid ASSEMBLY format: {self.assembly}. Only alphanumeric, dash, dot, and underscore allowed"
            )

    def _get_required_env(self, var_name: str) -> str:
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

    async def _get_latest_commit_sha_github_api(self, version: str) -> str | None:
        """
        Get latest commit SHA using GitHub API with App authentication.

        Uses GitHub App credentials (GITHUB_APP_ID + private key) for change detection.
        Note: GITHUB_TOKEN PAT is still required separately for PR operations (doozer requirement).

        Args:
            version: OCP version (e.g., "4.17")

        Returns:
            Commit SHA if successful, None if failed or not GitHub
        """
        # Parse GitHub URL to extract owner and repo
        match = re.match(r"(?:https://github.com/|git@github.com:)([^/]+)/([^/.]+)", self.data_path)
        if not match:
            self._logger.info(f"{version}: Not a GitHub URL, skipping API method")
            return None

        owner, repo = match.groups()
        repo = repo.removesuffix(".git")

        # Enforce GitHub App credentials for change detection
        if not os.environ.get("GITHUB_APP_ID"):
            raise EnvironmentError(
                "GitHub App credentials required for change detection via GitHub API. "
                "Set GITHUB_APP_ID and GITHUB_APP_PRIVATE_KEY_PATH environment variables. "
                "Note: GITHUB_TOKEN (PAT) is also required separately for PR operations."
            )

        try:
            # Determine gitref: use data_gitref if provided, otherwise use version-specific branch
            gitref = self.data_gitref or f"openshift-{version}"

            # Get authenticated client - uses GitHub App (blocking call, run in thread)
            client = await asyncio.to_thread(get_github_client_for_org, owner)
            repo_obj = await asyncio.to_thread(client.get_repo, f"{owner}/{repo}")

            # Try resolving gitref (could be branch, SHA, or tag)
            try:
                branch = await asyncio.to_thread(repo_obj.get_branch, gitref)
                sha = branch.commit.sha
                self._logger.info(f"{version}: Resolved branch '{gitref}' to SHA {sha[:8]} via GitHub App API")
                return sha
            except Exception:
                try:
                    commit = await asyncio.to_thread(repo_obj.get_commit, gitref)
                    self._logger.info(
                        f"{version}: Resolved commit '{gitref[:8]}' to SHA {commit.sha[:8]} via GitHub App API"
                    )
                    return commit.sha
                except Exception:
                    ref = await asyncio.to_thread(repo_obj.get_git_ref, f"tags/{gitref}")
                    self._logger.info(
                        f"{version}: Resolved tag '{gitref}' to SHA {ref.object.sha[:8]} via GitHub App API"
                    )
                    return ref.object.sha

        except Exception as e:
            self._logger.warning(f"{version}: GitHub API failed: {e}")
            return None

    def _is_commit_sha(self, gitref: str) -> bool:
        """Check if gitref is a commit SHA (7-40 char hex string)."""
        return bool(gitref and re.fullmatch(r"[0-9a-f]{7,40}", gitref.lower()))

    async def _get_latest_commit_sha_git_ls_remote(self, version: str) -> str:
        """
        Get latest commit SHA using git ls-remote (fallback method).

        Uses GitHub App token for authentication via GIT_ASKPASS if needed.
        Works with both public and private repos.

        Tries refs/heads/{gitref} first, then refs/tags/{gitref} if branch not found.
        Note: Raw commit SHAs cannot be resolved by ls-remote and will raise an error.

        Args:
            version: OCP version (e.g., "4.17")

        Returns:
            Commit SHA

        Raises:
            RuntimeError: If git ls-remote fails or gitref is a raw SHA
        """
        # Determine gitref: use data_gitref if provided, otherwise use version-specific branch
        gitref = self.data_gitref or f"openshift-{version}"

        # Raw commit SHAs cannot be resolved by git ls-remote
        if self._is_commit_sha(gitref):
            # Return the SHA directly - caller should use GitHub API instead
            self._logger.info(f"{version}: data_gitref is a commit SHA, skipping git ls-remote")
            return gitref

        # Get GitHub App authentication for git commands
        # This uses GitHub App tokens via GIT_ASKPASS (not GITHUB_TOKEN PAT)
        git_env = get_github_git_auth_env(url=self.data_path)

        self._logger.info(f"{version}: Querying remote SHA via git ls-remote")

        try:
            # Try branch first
            cmd = f"git ls-remote {self.data_path} refs/heads/{gitref}"
            rc, stdout, stderr = await asyncio.wait_for(exectools.cmd_gather_async(cmd, env=git_env), timeout=30)

            if rc != 0:
                raise RuntimeError(f"git ls-remote failed for {gitref}: {stderr}")

            # Parse output: "abc123def456...    refs/heads/branch-name"
            sha = stdout.strip().split()[0] if stdout.strip() else None

            # If branch not found, try tag
            if not sha:
                self._logger.info(f"{version}: Branch not found, trying tag refs/tags/{gitref}")
                cmd = f"git ls-remote {self.data_path} refs/tags/{gitref}"
                rc, stdout, stderr = await asyncio.wait_for(exectools.cmd_gather_async(cmd, env=git_env), timeout=30)

                if rc != 0:
                    raise RuntimeError(f"git ls-remote failed for tag {gitref}: {stderr}")

                sha = stdout.strip().split()[0] if stdout.strip() else None

            if not sha:
                raise RuntimeError(f"No branch or tag found for {gitref}")

            self._logger.info(f"{version}: Resolved {gitref} to SHA {sha[:8]} via git ls-remote")
            return sha

        except asyncio.TimeoutError as e:
            raise RuntimeError(f"git ls-remote timed out for {gitref}") from e

    async def _get_latest_commit_sha(self, version: str) -> str:
        """
        Get latest commit SHA using GitHub API with git ls-remote fallback.

        Tries GitHub API first (fast, uses GitHub App).
        Falls back to git ls-remote if API unavailable or fails.

        Args:
            version: OCP version (e.g., "4.17")

        Returns:
            Commit SHA

        Raises:
            RuntimeError: If both methods fail
        """
        # Try GitHub API first (faster, no clone needed)
        sha = await self._get_latest_commit_sha_github_api(version)

        # Fallback to git ls-remote if API failed
        if sha is None:
            self._logger.info(f"{version}: Falling back to git ls-remote")
            sha = await self._get_latest_commit_sha_git_ls_remote(version)

        return sha

    async def _has_changes_stateless(self, version: str) -> tuple[bool, str]:
        """
        Check if ocp-build-data has changed since last run (stateless).

        Uses Redis to store last-processed SHA instead of filesystem clones.
        This enables stateless operation compatible with ephemeral containers.

        Args:
            version: OCP version (e.g., "4.17")

        Returns:
            Tuple of (has_changes: bool, current_sha: str)
        """
        # Get current commit SHA (via GitHub API or git ls-remote)
        current_sha = await self._get_latest_commit_sha(version)

        # Get last-processed SHA from Redis
        redis_key = f"sync-ci-images:last-sha:{version}"
        last_sha = await redis.get_value(redis_key)

        self._logger.info(f"{version}: Current SHA: {current_sha[:8]}")
        if last_sha:
            self._logger.info(f"{version}: Last processed SHA: {last_sha[:8]}")
        else:
            self._logger.info(f"{version}: No previous run found in Redis")

        # Check for changes
        if current_sha == last_sha and not self.force_run:
            self._logger.info(f"{version}: NO changes detected")
            return False, current_sha

        if self.force_run:
            self._logger.info(f"{version}: FORCE_RUN set, processing regardless of changes")
        else:
            self._logger.info(f"{version}: Changes detected")

        return True, current_sha

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

        # Remove stale clone if exists (Jenkinsfile line 128)
        if group_dir.exists():
            shutil.rmtree(group_dir)

        # Determine gitref: use data_gitref if provided, otherwise use version-specific branch
        gitref = self.data_gitref or group

        # Get GitHub App authentication for git commands (supports private repos)
        git_env = get_github_git_auth_env(url=self.data_path)

        self._logger.info(f"Cloning ocp-build-data for {group}")

        try:
            # Build git clone command
            # For commit SHAs, git clone --branch doesn't work, so clone then checkout
            if self._is_commit_sha(gitref):
                self._logger.info(f"{version}: Cloning and checking out commit SHA {gitref}")
                cmd = f"git clone {self.data_path} {group_dir} && git -C {group_dir} checkout {gitref}"
            else:
                # Standard clone for branches and tags
                cmd = f"git clone {self.data_path} --branch {gitref} --single-branch --depth 1 {group_dir}"

            # Use asyncio.wait_for for timeout since cmd_gather_async doesn't support timeout parameter
            rc, _, stderr = await asyncio.wait_for(
                exectools.cmd_gather_async(cmd, env=git_env), timeout=self.GIT_CLONE_TIMEOUT
            )

            if rc != 0:
                raise RuntimeError(f"Git clone failed for {group}: {stderr}")
        except asyncio.TimeoutError as e:
            raise RuntimeError(f"Git clone timed out after {self.GIT_CLONE_TIMEOUT}s for {group}") from e

        return group_dir

    async def _run_doozer_command(
        self, doozer_opts: str, subcommand: str, extra_args: str = "", check: bool = True, timeout: int = None
    ) -> tuple[int, str, str]:
        """
        Execute a doozer command with standard options.

        Args:
            doozer_opts: Doozer global options (--working-dir, --group, etc.)
            subcommand: Doozer subcommand (e.g., "images:streams mirror")
            extra_args: Additional arguments for the subcommand
            check: Raise exception on non-zero return code
            timeout: Command timeout in seconds (default: DOOZER_DEFAULT_TIMEOUT)

        Returns:
            Tuple of (return_code, stdout, stderr)

        Raises:
            Exception: If check=True and command fails
        """
        if timeout is None:
            timeout = self.DOOZER_DEFAULT_TIMEOUT

        cmd = f"doozer {doozer_opts} {subcommand} {extra_args}".strip()

        self._logger.info(f"Running doozer command: {cmd}")

        # Use asyncio.wait_for for timeout
        rc, stdout, stderr = await asyncio.wait_for(exectools.cmd_gather_async(cmd, check=check), timeout=timeout)

        return rc, stdout, stderr

    async def run(self) -> int:
        """
        Main pipeline execution: sync CI images for the configured OCP version.

        Returns:
            Return code: 0=success, 25=partial (PRs skipped), 50=failure
        """
        from pyartcd import jenkins

        # Set Jenkins title tag
        jenkins.update_title(f' [{self.version}]')

        self._logger.info(f"Starting sync-ci-images for {self.version}")

        group = f"openshift-{self.version}"
        group_dir = None

        try:
            # Check for changes using stateless detection (GitHub API or git ls-remote + Redis)
            has_changes, current_sha = await self._has_changes_stateless(self.version)
            if not has_changes:
                self._logger.info(f"{self.version}: No changes detected, skipping")
                return 0

            # Clone ocp-build-data only if changes detected
            group_dir = await self._clone_ocp_build_data(self.version)

            # Create registry config
            with self._create_registry_config() as auth_file:
                self._logger.info(f"Created registry config: {auth_file}")

                # Build doozer global options (before subcommand)
                working_dir = f"{self.runtime.doozer_working}/wd-{self.version}"
                doozer_opts = (
                    f"--working-dir {working_dir} "
                    f"--data-path {group_dir} "
                    f"--group {group} "
                    f"--assembly {self.assembly} "
                    f"--latest-parent-version "
                    f"--build-system {self.BUILD_SYSTEM} "
                    f"--registry-config {auth_file}"
                )

                if self.only_stream:
                    doozer_opts += f" --only-streams {self.only_stream}"

                # Step 1: Generate BuildConfigs
                # In dry-run mode, omit --apply flag instead of skipping the command
                self._logger.info(f"{self.version}: Generating BuildConfigs")
                apply_flag = "" if self.runtime.dry_run else "--apply"
                await self._run_doozer_command(
                    doozer_opts, "images:streams gen-buildconfigs", f"-o {working_dir}/buildconfigs.yaml {apply_flag}"
                )

                # Step 2: Mirror images
                # Pass --dry-run to doozer subcommand for better visibility
                self._logger.info(f"{self.version}: Mirroring images")
                mirror_args = ""
                if self.update_images_only_when_missing:
                    mirror_args += "--only-if-missing "
                if self.runtime.dry_run:
                    mirror_args += "--dry-run"
                await self._run_doozer_command(
                    doozer_opts,
                    "images:streams mirror",
                    mirror_args.strip(),
                )

                # Step 3: Start builds
                # Pass --dry-run to doozer subcommand
                self._logger.info(f"{self.version}: Starting builds")
                start_builds_args = "--dry-run" if self.runtime.dry_run else ""
                await self._run_doozer_command(doozer_opts, "images:streams start-builds", start_builds_args)

                # Step 4: Wait for builds if not skipping
                if not self.skip_waits and not self.runtime.dry_run:
                    self._logger.info(f"{self.version}: Waiting {self.WAIT_TIME_MINUTES} minutes for builds")
                    await asyncio.sleep(self.WAIT_TIME_MINUTES * 60)

                # Step 5: Check upstream consistency
                self._logger.info(f"{self.version}: Checking upstream consistency")
                await self._run_doozer_command(doozer_opts, "images:streams check-upstream", "")

                # Step 6: Open reconciliation PRs
                if not self.skip_prs:
                    self._logger.info(f"{self.version}: Opening reconciliation PRs")

                    # Validate GITHUB_TOKEN for PR operations (doozer requires PAT to fork repos)
                    github_token = os.environ.get('GITHUB_TOKEN')
                    if not github_token:
                        raise EnvironmentError(
                            "GITHUB_TOKEN (Personal Access Token) required for PR operations. "
                            "Doozer requires a PAT (not GitHub App token) to fork repos and open PRs. "
                            "Set GITHUB_TOKEN environment variable."
                        )

                    pr_args = f"--github-access-token {github_token} --interstitial {self.PR_INTERSTITIAL_SECONDS}"
                    if self.add_labels:
                        pr_args += f" --add-labels {self.add_labels}"
                    if self.runtime.dry_run:
                        pr_args += " --moist-run"  # doozer's dry-run equivalent for PRs

                    rc, _, _ = await self._run_doozer_command(
                        doozer_opts,
                        "images:streams prs open",
                        pr_args,
                        check=False,  # PRs can return 25 for partial success
                    )

                    if rc == 25:
                        self._logger.warning(f"{self.version}: Some PRs skipped (rc=25)")
                        # Don't update Redis on partial success - retry on next run
                        return 25
                    elif rc != 0:
                        raise RuntimeError(f"PR opening failed with rc={rc}")

            # Store current SHA in Redis for next run (stateless)
            if not self.runtime.dry_run:
                redis_key = f"sync-ci-images:last-sha:{self.version}"
                await redis.set_value(redis_key, current_sha)
                self._logger.info(f"{self.version}: Updated Redis with SHA {current_sha[:8]}")

            # Clean up clone directory (no need to persist between runs)
            if group_dir and group_dir.exists():
                shutil.rmtree(group_dir)
                self._logger.info(f"{self.version}: Cleaned up clone directory")

            self._logger.info(f"{self.version}: Completed successfully")
            return 0

        except Exception as e:
            self._logger.error(f"{self.version}: Failed with error: {e}", exc_info=True)

            # Clean up clone directory on failure
            if group_dir and group_dir.exists():
                shutil.rmtree(group_dir)
                self._logger.info(f"{self.version}: Cleaned up clone directory after failure")

            raise  # Re-raise to fail the job


# CLI Command Registration
@cli.command(
    "sync-ci-images",
    help="Sync CI testing images to match ART production builds for a single OCP version. "
    "Ensures CI and production use the same base images and configurations.",
)
@click.option(
    '--for-release',
    required=True,
    help='OCP version to sync (e.g., "4.17") - REQUIRED. Use schedule-sync-ci-images to process multiple versions.',
)
@click.option(
    '--data-path',
    required=False,
    default=OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--only-stream',
    default='',
    help='Process only specific stream from streams.yml. Automatically sets SKIP_PRS=true.',
)
@click.option(
    '--add-labels', default='', help='Space-delimited labels to add to reconciliation PRs (e.g., "backport candidate")'
)
@click.option('--assembly', default='stream', help='Assembly name to use for doozer operations (default: "stream")')
@click.option(
    '--skip-prs', is_flag=True, default=False, help='Skip opening reconciliation PRs (for testing/dry-run scenarios)'
)
@click.option(
    '--skip-waits', is_flag=True, default=False, help='Skip sleep delays between operations (for faster testing)'
)
@click.option(
    '--force-run', is_flag=True, default=False, help='Run even if ocp-build-data has not changed since last run'
)
@click.option(
    '--update-images-only-when-missing',
    is_flag=True,
    default=False,
    help='Pass --only-if-missing to doozer mirror (update only missing images)',
)
@pass_runtime
@click_coroutine
async def sync_ci_images_cli(
    runtime: Runtime,
    for_release: str,
    data_path: str,
    data_gitref: str,
    only_stream: str,
    add_labels: str,
    assembly: str,
    skip_prs: bool,
    skip_waits: bool,
    force_run: bool,
    update_images_only_when_missing: bool,
):
    """
    CLI entrypoint for sync-ci-images pipeline.

    Syncs CI testing images to match ART production builds.
    Typically invoked by schedule-sync-ci-images scheduler.

    Return codes:
        0: Sync completed successfully
        25: Partial success (some PRs were skipped)
        50: Sync failed
    """
    from pyartcd import jenkins, locks

    # Initialize Jenkins for title updates
    jenkins.init_jenkins()

    pipeline = SyncCIImagesPipeline(
        runtime,
        for_release=for_release,
        data_path=data_path,
        data_gitref=data_gitref,
        only_stream=only_stream,
        add_labels=add_labels,
        assembly=assembly,
        skip_prs=skip_prs,
        skip_waits=skip_waits,
        force_run=force_run,
        update_images_only_when_missing=update_images_only_when_missing,
    )

    # Run with per-version lock
    lock_name = locks.Lock.SYNC_CI_IMAGES.value.format(version=for_release)
    lock_id = jenkins.get_build_url()  # Jenkins build identifier

    try:
        exit_code = await locks.run_with_lock(
            coro=pipeline.run(),
            lock=locks.Lock.SYNC_CI_IMAGES,
            lock_name=lock_name,
            lock_id=lock_id,
        )
        exit(exit_code if exit_code is not None else 0)
    except Exception:
        runtime.logger.error(f"sync-ci-images failed for {for_release}", exc_info=True)
        exit(50)
