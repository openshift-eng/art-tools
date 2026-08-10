"""
open-reconciliation-prs pipeline: Open reconciliation PRs for CI upstream repos.

Extracted from sync-ci-images to run independently. This pipeline clones
ocp-build-data and runs `doozer images:streams prs open` to reconcile
BuildConfig drift in upstream CI repositories.
"""

import asyncio
import os
import re
import shutil
from pathlib import Path

import click
from artcommonlib import exectools
from artcommonlib.github_auth import get_github_git_auth_env

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.runtime import Runtime

# Regex patterns matching known transient network errors.
# Used to decide whether a failed doozer command should be retried.
TRANSIENT_ERROR_PATTERNS: list[str] = [
    r"ConnectionError",
    r"NameResolutionError",
    r"TimeoutError",
    r"Max retries exceeded",
    r"Connection refused",
    r"Connection reset",
    r"Temporary failure in name resolution",
    r"Network is unreachable",
]


class ReconcileCIUpstreamPipeline:
    """
    Opens reconciliation PRs for CI upstream repos.

    Extracted from SyncCIImagesPipeline to run as a standalone pipeline.
    Clones ocp-build-data, builds doozer options, and runs
    `doozer images:streams prs open` to reconcile BuildConfig drift.
    """

    # Constants (duplicated from sync_ci_images.py for self-containment)
    PR_INTERSTITIAL_SECONDS = 840
    GIT_CLONE_TIMEOUT = 300
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 60

    def __init__(
        self,
        runtime: Runtime,
        for_release: str,
        data_path: str = "",
        data_gitref: str = "",
        assembly: str = "stream",
        add_labels: str = "",
        moist_run: bool = False,
    ) -> None:
        """
        Initialize open-reconciliation-prs pipeline.

        Args:
            runtime: PyARTCD runtime instance
            for_release: OCP version to reconcile (e.g., "4.18")
            data_path: ocp-build-data fork URL (default: official repo)
            data_gitref: ocp-build-data git branch/tag/sha (default: use version branch)
            assembly: Assembly name (default: "stream")
            add_labels: Space-delimited labels to add to PRs
            moist_run: Dry-run mode for PR operations (doozer --moist-run)
        """
        self.runtime = runtime
        self._logger = runtime.logger
        self.version = for_release
        self.data_path = data_path or OCP_BUILD_DATA_URL
        self.data_gitref = data_gitref
        self.assembly = assembly
        self.add_labels = add_labels
        self.moist_run = moist_run

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
            raise ValueError(f"Invalid FOR_RELEASE format: {self.version}. Expected format: X.Y (e.g., 4.18)")

        # Validate assembly format (alphanumeric, dash, dot, underscore)
        if self.assembly and not re.match(r'^[\w.-]+$', self.assembly):
            raise ValueError(
                f"Invalid ASSEMBLY format: {self.assembly}. Only alphanumeric, dash, dot, and underscore allowed"
            )

    @property
    def _working_dir(self) -> str:
        """Get doozer working directory path for this version."""
        return f"{self.runtime.doozer_working}/wd-{self.version}"

    @staticmethod
    def _is_commit_sha(gitref: str) -> bool:
        """Check if gitref is a commit SHA (7-40 char hex string)."""
        return bool(gitref and re.fullmatch(r"[0-9a-f]{7,40}", gitref.lower()))

    def _get_gitref(self, version: str) -> str:
        """Get gitref to use: data_gitref if provided, otherwise version-specific branch."""
        return self.data_gitref or f"openshift-{version}"

    async def _clone_ocp_build_data(self, version: str) -> Path:
        """
        Clone ocp-build-data repository for specified version.

        Uses GitHub App authentication for private repos.
        Supports branches, tags, and raw commit SHAs.

        Args:
            version: OCP version (e.g., "4.18")

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
            # Build git clone command
            # For commit SHAs, git clone --branch doesn't work, so clone then checkout
            if self._is_commit_sha(gitref):
                self._logger.info(f"{version}: Cloning and checking out commit SHA {gitref}")
                cmd = f"git clone {self.data_path} {group_dir} && git -C {group_dir} checkout {gitref}"
            else:
                # Standard clone for branches and tags
                cmd = f"git clone {self.data_path} --branch {gitref} --single-branch --depth 1 {group_dir}"

            # Stream git clone output to console
            rc, _, _ = await asyncio.wait_for(
                exectools.cmd_gather_async(cmd, env=git_env, stdout=None, stderr=None), timeout=self.GIT_CLONE_TIMEOUT
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
            subcommand: Doozer subcommand (e.g., "images:streams prs open")
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

    def _build_doozer_options(self, group_dir: Path) -> str:
        """
        Build doozer global options for PR operations.

        Simplified compared to sync-ci-images: no registry-config,
        no build-system params (not needed for PR operations).
        """
        group = f"openshift-{self.version}"
        doozer_opts = (
            f"--working-dir {self._working_dir} "
            f"--data-path {group_dir} "
            f"--group {group} "
            f"--assembly {self.assembly} "
            f"--latest-parent-version"
        )
        return doozer_opts

    def _is_transient_error(self, stderr: str) -> bool:
        """Check if stderr contains any known transient network error pattern.

        Args:
            stderr: Standard error output from a failed command.

        Returns:
            True if stderr matches a transient error pattern (case-insensitive).
        """
        if not stderr:
            return False
        for pattern in TRANSIENT_ERROR_PATTERNS:
            if re.search(pattern, stderr, re.IGNORECASE):
                return True
        return False

    async def _open_reconciliation_prs(self, doozer_opts: str) -> int:
        """
        Open PRs to reconcile BuildConfig drift.

        Wraps the doozer command in a retry loop so transient network
        errors (DNS failures, connection resets, timeouts) are retried
        up to ``MAX_RETRIES`` times with a ``RETRY_DELAY_SECONDS``
        pause between attempts.

        Returns:
            Return code: 0=success, 25=partial (some PRs skipped)

        Raises:
            EnvironmentError: If GITHUB_TOKEN is not set
            RuntimeError: If PR opening fails after all retry attempts
        """
        self._logger.info(f"{self.version}: Opening reconciliation PRs")

        # Validate GITHUB_TOKEN is set (doozer reads it from the environment via its
        # --github-access-token click option with envvar='GITHUB_TOKEN')
        if not os.environ.get('GITHUB_TOKEN'):
            raise EnvironmentError(
                "GITHUB_TOKEN (Personal Access Token) required for PR operations. "
                "Doozer requires a PAT (not GitHub App token) to fork repos and open PRs. "
                "Set GITHUB_TOKEN environment variable."
            )

        pr_args = f"--interstitial {self.PR_INTERSTITIAL_SECONDS}"
        pr_args += ' --add-auto-labels'
        pr_args += ' --add-label "jira/valid-bug" --add-label "verified"'
        if self.add_labels:
            for label in self.add_labels.split():
                pr_args += f' --add-label "{label}"'
        if self.moist_run:
            pr_args += " --moist-run"  # doozer's dry-run equivalent for PRs

        last_stderr = ""
        for attempt in range(1, self.MAX_RETRIES + 1):
            rc, _, stderr = await self._run_doozer_command(
                doozer_opts,
                "images:streams prs open",
                pr_args,
                check=False,  # PRs can return 25 for partial success
            )
            last_stderr = stderr or ""

            # Determine whether this attempt hit a transient failure
            is_transient = self._is_transient_error(last_stderr)

            # rc == 1 with --moist-run is expected (simulation mode), not a failure
            if rc == 1 and self.moist_run:
                self._logger.info(f"{self.version}: PR simulation completed (rc=1 from --moist-run)")
                if attempt > 1:
                    self._logger.info(f"{self.version}: Succeeded on attempt {attempt}")
                return 0

            # Transient failure on rc == 1 when NOT in moist-run is a real crash
            if rc == 1 and not self.moist_run and is_transient:
                if attempt < self.MAX_RETRIES:
                    self._logger.warning(
                        f"{self.version}: Transient error on attempt {attempt}/{self.MAX_RETRIES} "
                        f"(rc={rc}). Retrying in {self.RETRY_DELAY_SECONDS}s..."
                    )
                    await asyncio.sleep(self.RETRY_DELAY_SECONDS)
                    continue

            # Non-zero / non-standard rc with transient error → retry
            if rc not in (0, 1, 25) and is_transient:
                if attempt < self.MAX_RETRIES:
                    self._logger.warning(
                        f"{self.version}: Transient error on attempt {attempt}/{self.MAX_RETRIES} "
                        f"(rc={rc}). Retrying in {self.RETRY_DELAY_SECONDS}s..."
                    )
                    await asyncio.sleep(self.RETRY_DELAY_SECONDS)
                    continue

            # --- Non-transient outcomes (or final attempt) ---
            if rc == 0:
                if attempt > 1:
                    self._logger.info(f"{self.version}: Succeeded on attempt {attempt}")
                return 0

            if rc == 25:
                self._logger.warning(f"{self.version}: Some PRs skipped (rc=25)")
                if attempt > 1:
                    self._logger.info(f"{self.version}: Succeeded on attempt {attempt}")
                return 25

            if rc != 0:
                raise RuntimeError(f"PR opening failed with rc={rc}")

        # All retry attempts exhausted
        raise RuntimeError(
            f"PR opening failed after {self.MAX_RETRIES} attempts due to transient errors (last rc from doozer). "
            f"Last stderr: {last_stderr[:500]}"
        )

    def _cleanup(self, group_dir: Path | None) -> None:
        """Remove temporary clone directory."""
        if group_dir and group_dir.exists():
            shutil.rmtree(group_dir)
            self._logger.info(f"{self.version}: Cleaned up clone directory")

    async def run(self) -> int:
        """
        Main pipeline: open reconciliation PRs for a single OCP version.

        Workflow:
        1. Clone ocp-build-data
        2. Build doozer options
        3. Open reconciliation PRs via doozer images:streams prs open

        Returns:
            Return code: 0=success, 25=partial (some PRs skipped), 50=failure
        """
        jenkins.update_title(f' [{self.version}]')
        self._logger.info(f"Starting open-reconciliation-prs for {self.version}")

        group_dir = None

        try:
            # Clone ocp-build-data
            group_dir = await self._clone_ocp_build_data(self.version)

            # Build doozer options (no registry config needed for PR operations)
            doozer_opts = self._build_doozer_options(group_dir)

            # Open reconciliation PRs
            rc = await self._open_reconciliation_prs(doozer_opts)
            if rc == 25:
                return 25

            return 0

        except Exception as e:
            self._logger.error(f"{self.version}: Failed with error: {e}", exc_info=True)
            raise  # Re-raise to fail the job

        finally:
            self._cleanup(group_dir)


# CLI Command Registration
@cli.command(
    "open-reconciliation-prs",
    help="Open reconciliation PRs for CI upstream repos. "
    "Clones ocp-build-data and runs doozer images:streams prs open "
    "to reconcile BuildConfig drift in upstream CI repositories.",
)
@click.option(
    '--for-release',
    required=True,
    help='OCP version to reconcile (e.g., "4.18") - REQUIRED.',
)
@click.option(
    '--data-path',
    required=False,
    default=OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option('--assembly', default='stream', help='Assembly name to use for doozer operations (default: "stream")')
@click.option(
    '--add-labels', default='', help='Space-delimited labels to add to reconciliation PRs (e.g., "backport candidate")'
)
@click.option(
    '--moist-run', is_flag=True, default=False, help='Dry-run mode for PR operations (passes --moist-run to doozer)'
)
@pass_runtime
@click_coroutine
async def open_reconciliation_prs_cli(
    runtime: Runtime,
    for_release: str,
    data_path: str,
    data_gitref: str,
    assembly: str,
    add_labels: str,
    moist_run: bool,
):
    """
    CLI entrypoint for open-reconciliation-prs pipeline.

    Opens reconciliation PRs for CI upstream repos by running
    doozer images:streams prs open.

    Return codes:
        0: PRs opened successfully
        25: Partial success (some PRs were skipped)
        50: Pipeline failed
    """
    from pyartcd import locks

    # Initialize Jenkins for title updates
    jenkins.init_jenkins()

    pipeline = ReconcileCIUpstreamPipeline(
        runtime,
        for_release=for_release,
        data_path=data_path,
        data_gitref=data_gitref,
        assembly=assembly,
        add_labels=add_labels,
        moist_run=moist_run,
    )

    # Run with per-version lock
    lock_name = locks.Lock.OPEN_RECONCILIATION_PRS.value.format(version=for_release)
    lock_id = jenkins.get_build_path_or_random()  # Jenkins build identifier

    try:
        exit_code = await locks.run_with_lock(
            coro=pipeline.run(),
            lock=locks.Lock.OPEN_RECONCILIATION_PRS,
            lock_name=lock_name,
            lock_id=lock_id,
        )
        exit(exit_code if exit_code is not None else 0)
    except Exception:
        runtime.logger.error(f"open-reconciliation-prs failed for {for_release}", exc_info=True)
        exit(50)
