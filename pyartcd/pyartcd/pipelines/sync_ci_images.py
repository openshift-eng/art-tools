"""
sync-ci-images pipeline: Sync CI testing images to match ART production builds.

Ensures CI and production use the same base images, builder images, and configurations
to maximize CI signal fidelity.
"""

import asyncio
import os
import re

# Import for CLI registration
import click
from artcommonlib import exectools, redis
from artcommonlib.github_auth import get_github_client_for_org, get_github_git_auth_env

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.pipelines.ci_image_sync_common import CIImageSyncPipelineBase
from pyartcd.runtime import Runtime


class SyncCIImagesPipeline(CIImageSyncPipelineBase):
    """
    Orchestrates the sync-ci-images sub-jobs for a single OCP version.

    Checks ocp-build-data for changes, then delegates the actual work to
    independent Jenkins jobs: reconcile-ci-upstream (fire-and-forget),
    mirror-images-to-ci, and sync-ci-buildconfigs (see ART-21959, ART-21963).
    """

    # Constants from Jenkinsfile
    WAIT_TIME_MINUTES = 20

    def __init__(
        self,
        runtime: Runtime,
        for_release: str,
        data_path: str = "",
        data_gitref: str = "",
        only_stream: str = "",
        images: str = "",
        assembly: str = "stream",
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
            images: Comma-separated distgit keys of images with ci_alignment.upstream_image.
            assembly: Assembly name (default: "stream")
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
        self.images = [i.strip() for i in images.split(',') if i.strip()] if images else []
        self.assembly = assembly
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

        # Validate assembly format (alphanumeric, dash, dot, underscore)
        if self.assembly and not re.match(r'^[\w.-]+$', self.assembly):
            raise ValueError(
                f"Invalid ASSEMBLY format: {self.assembly}. Only alphanumeric, dash, dot, and underscore allowed"
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
            gitref = self._get_gitref(version)

            # Get authenticated client - uses GitHub App (blocking call, run in thread)
            client = await asyncio.to_thread(get_github_client_for_org, owner)
            repo_obj = await asyncio.to_thread(client.get_repo, self._get_github_repo_path(owner, repo))

            # Try resolving gitref (could be branch, SHA, or tag)
            return await self._resolve_github_ref(repo_obj, gitref, version)

        except Exception as e:
            self._logger.warning(f"{version}: GitHub API failed: {e}")
            return None

    @staticmethod
    def _get_github_repo_path(owner: str, repo: str) -> str:
        """Construct GitHub repository path for PyGithub API (format: owner/repo)."""
        return f"{owner}/{repo}"

    async def _resolve_github_ref(self, repo_obj, gitref: str, version: str) -> str:
        """
        Resolve a GitHub reference (branch, commit, or tag) to a commit SHA.

        Tries branch first, then commit, then tag.

        Args:
            repo_obj: PyGithub Repository object
            gitref: Git reference to resolve (branch, commit SHA, or tag)
            version: OCP version (for logging)

        Returns:
            Resolved commit SHA

        Raises:
            Exception: If ref cannot be resolved
        """
        # Try branch
        try:
            branch = await asyncio.to_thread(repo_obj.get_branch, gitref)
            self._logger.info(
                f"{version}: Resolved branch '{gitref}' to SHA {branch.commit.sha[:8]} via GitHub App API"
            )
            return branch.commit.sha
        except Exception:
            pass

        # Try commit
        try:
            commit = await asyncio.to_thread(repo_obj.get_commit, gitref)
            self._logger.info(f"{version}: Resolved commit '{gitref[:8]}' to SHA {commit.sha[:8]} via GitHub App API")
            return commit.sha
        except Exception:
            pass

        # Try tag
        ref = await asyncio.to_thread(repo_obj.get_git_ref, f"tags/{gitref}")
        self._logger.info(f"{version}: Resolved tag '{gitref}' to SHA {ref.object.sha[:8]} via GitHub App API")
        return ref.object.sha

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
        gitref = self._get_gitref(version)

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

    async def _check_for_changes(self) -> tuple[bool, str]:
        """Check if ocp-build-data has changes since last run."""
        has_changes, current_sha = await self._has_changes_stateless(self.version)
        if not has_changes:
            self._logger.info(f"{self.version}: No changes detected, skipping")
        return has_changes, current_sha

    async def _record_successful_run(self, current_sha: str) -> None:
        """Store current SHA in Redis to track last successful run."""
        if not self.runtime.dry_run:
            redis_key = f"sync-ci-images:last-sha:{self.version}"
            await redis.set_value(redis_key, current_sha)
            self._logger.info(f"{self.version}: Updated Redis with SHA {current_sha[:8]}")

    def _shared_sub_job_params(self) -> dict:
        """Orchestrator config shared by every downstream sync-ci-images sub-job."""
        return {
            'assembly': self.assembly,
            'data_path': self.data_path,
            'data_gitref': self.data_gitref,
            'dry_run': self.runtime.dry_run,
        }

    def _start_reconcile_ci_upstream(self) -> None:
        """
        Fire-and-forget trigger for reconcile-ci-upstream (open-reconciliation-prs).

        Independent of the other jobs, so a failure to trigger it must not block
        mirror-images-to-ci or sync-ci-buildconfigs. Triggered even during a dry-run
        of the orchestrator itself -- dry_run is forwarded so the sub-job runs in its
        own dry-run mode instead of being skipped entirely.
        """
        try:
            _, url = jenkins.start_open_reconciliation_prs(
                version=self.version, return_build_url=True, **self._shared_sub_job_params()
            )
            jenkins.update_description(f'<a href="{url}">open-reconciliation-prs</a><br/>')
        except Exception as e:
            self._logger.warning(f"{self.version}: Failed to trigger reconcile-ci-upstream (fire-and-forget): {e}")

    async def _trigger_and_wait(self, start_fn, job_name: str, **extra_params) -> None:
        """
        Trigger a downstream Jenkins job and block until it completes successfully.

        Always triggers the job, even during a dry-run of the orchestrator itself --
        dry_run is forwarded (via _shared_sub_job_params) so the sub-job runs in its
        own dry-run mode instead of being skipped entirely.

        start_fn blocks synchronously (polling) for as long as the sub-job runs, so it
        is offloaded to a thread to avoid stalling the event loop -- and with it, this
        process's other async work (e.g. the Redis lock's background auto-extend task).
        """
        result, url = await asyncio.to_thread(
            start_fn,
            version=self.version,
            only_stream=self.only_stream,
            images=','.join(self.images),
            block_until_complete=True,
            return_build_url=True,
            **self._shared_sub_job_params(),
            **extra_params,
        )
        jenkins.update_description(f'<a href="{url}">{job_name}</a>: {result}<br/>')
        if result != 'SUCCESS':
            raise RuntimeError(f"{self.version}: {job_name} did not succeed (result={result})")

    async def run(self) -> int:
        """
        Orchestrate the sync-ci-images sub-jobs for a single OCP version.

        Workflow:
        1. Check for changes in ocp-build-data
        2. Fire-and-forget: reconcile-ci-upstream
        3. Run and wait: mirror-images-to-ci
        4. Then run and wait: sync-ci-buildconfigs

        Returns:
            Return code: 0=success, 50=failure
        """
        jenkins.update_title(f' [{self.version}]')
        self._logger.info(f"Starting sync-ci-images orchestrator for {self.version}")

        # Check for changes
        has_changes, current_sha = await self._check_for_changes()
        if not has_changes:
            return 0

        self._start_reconcile_ci_upstream()

        await self._trigger_and_wait(
            jenkins.start_mirror_images_to_ci,
            'mirror-images-to-ci',
            update_images_only_when_missing=self.update_images_only_when_missing,
        )
        await self._trigger_and_wait(jenkins.start_sync_ci_buildconfigs, 'sync-ci-buildconfigs')

        # Record success
        await self._record_successful_run(current_sha)
        return 0


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
    help='Process only specific stream from streams.yml.',
)
@click.option(
    '--images',
    default='',
    help='Comma-separated distgit keys to sync (e.g. ci-openshift-base.rhel10). '
    'Each must have ci_alignment.upstream_image set.',
)
@click.option('--assembly', default='stream', help='Assembly name to use for doozer operations (default: "stream")')
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
    images: str,
    assembly: str,
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
        images=images,
        assembly=assembly,
        skip_waits=skip_waits,
        force_run=force_run,
        update_images_only_when_missing=update_images_only_when_missing,
    )

    # Run with per-version lock
    lock_name = locks.Lock.SYNC_CI_IMAGES.value.format(version=for_release)
    lock_id = jenkins.get_build_path_or_random()  # Jenkins build identifier

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
