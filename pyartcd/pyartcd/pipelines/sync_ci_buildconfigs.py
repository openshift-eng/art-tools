"""
sync-ci-buildconfigs pipeline: Generate/apply BuildConfigs, trigger CI builds,
and verify upstream imagestream consistency.

Extracted from sync-ci-images (see ART-21962) so these three sequential steps
can run as an independent job once mirror-images-to-ci has completed.
"""

import re

# Import for CLI registration
import click

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.pipelines.ci_image_sync_common import CIImageSyncPipelineBase
from pyartcd.runtime import Runtime


class SyncCIBuildconfigsPipeline(CIImageSyncPipelineBase):
    """
    Generates and applies BuildConfigs, triggers CI builds, and verifies upstream
    imagestream consistency.

    Must run after mirror-images-to-ci has completed, since start-builds requires
    base/builder images to already be mirrored to CI registries.
    """

    def __init__(
        self,
        runtime: Runtime,
        for_release: str,
        data_path: str = "",
        data_gitref: str = "",
        only_stream: str = "",
        images: str = "",
        assembly: str = "stream",
    ) -> None:
        """
        Initialize sync-ci-buildconfigs pipeline.

        Args:
            runtime: PyARTCD runtime instance
            for_release: OCP version to sync (e.g., "4.17")
            data_path: ocp-build-data fork URL (default: official repo)
            data_gitref: ocp-build-data git branch/tag/sha (default: use version branch)
            only_stream: Specific stream from streams.yml.
            images: Comma-separated distgit keys of images with ci_alignment.upstream_image.
            assembly: Assembly name (default: "stream")
        """
        self.runtime = runtime
        self._logger = runtime.logger
        self.version = for_release
        self.data_path = data_path or OCP_BUILD_DATA_URL
        self.data_gitref = data_gitref
        self.only_stream = only_stream
        self.images = [i.strip() for i in images.split(',') if i.strip()] if images else []
        self.assembly = assembly

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

    async def _generate_and_apply_buildconfigs(self, doozer_opts: str) -> None:
        """Generate BuildConfigs and apply them to CI cluster."""
        self._logger.info(f"{self.version}: Generating BuildConfigs")
        apply_flag = "" if self.runtime.dry_run else "--apply"
        await self._run_doozer_command(
            doozer_opts,
            "images:streams gen-buildconfigs",
            f"{self._filter_args} -o {self._working_dir}/buildconfigs.yaml {apply_flag}",
        )

    async def _trigger_ci_builds(self, doozer_opts: str, auth_file: str) -> None:
        """Start CI builds for updated images."""
        self._logger.info(f"{self.version}: Starting builds")
        start_builds_args = f"{self._filter_args} --registry-auth {auth_file} "
        if self.runtime.dry_run:
            start_builds_args += "--dry-run"
        await self._run_doozer_command(doozer_opts, "images:streams start-builds", start_builds_args.strip())

    async def _verify_upstream_consistency(self, doozer_opts: str, auth_file: str) -> None:
        """Verify CI imagestreams match expected state."""
        self._logger.info(f"{self.version}: Checking upstream consistency")
        await self._run_doozer_command(
            doozer_opts, "images:streams check-upstream", f"{self._filter_args} --registry-auth {auth_file}"
        )

    async def run(self) -> int:
        """
        Main pipeline: generate BuildConfigs, trigger CI builds, and verify
        upstream consistency for a single OCP version.

        Workflow:
        1. Clone ocp-build-data
        2. Generate and apply BuildConfigs to CI cluster
        3. Trigger CI builds
        4. Verify upstream imagestream consistency

        Returns:
            Return code: 0=success, 50=failure
        """
        jenkins.update_title(f' [{self.version}]')
        self._logger.info(f"Starting sync-ci-buildconfigs for {self.version}")

        group_dir = None

        try:
            group_dir = await self._clone_ocp_build_data(self.version)

            with self._create_registry_config() as auth_file:
                self._logger.info(f"Created registry config: {auth_file}")
                doozer_opts = self._build_doozer_options(group_dir, auth_file)

                await self._generate_and_apply_buildconfigs(doozer_opts)
                await self._trigger_ci_builds(doozer_opts, auth_file)
                await self._verify_upstream_consistency(doozer_opts, auth_file)

            self._cleanup(group_dir)
            return 0

        except Exception as e:
            self._logger.error(f"{self.version}: Failed with error: {e}", exc_info=True)
            self._cleanup(group_dir)
            raise  # Re-raise to fail the job


# CLI Command Registration
@cli.command(
    "sync-ci-buildconfigs",
    help="Generate/apply BuildConfigs, trigger CI builds, and verify upstream imagestream "
    "consistency for a single OCP version. Must run after mirror-images-to-ci.",
)
@click.option(
    '--for-release',
    required=True,
    help='OCP version to sync (e.g., "4.17") - REQUIRED.',
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
@pass_runtime
@click_coroutine
async def sync_ci_buildconfigs_cli(
    runtime: Runtime,
    for_release: str,
    data_path: str,
    data_gitref: str,
    only_stream: str,
    images: str,
    assembly: str,
):
    """
    CLI entrypoint for sync-ci-buildconfigs pipeline.

    Generates/applies BuildConfigs, triggers CI builds, and verifies upstream
    consistency. Typically invoked by the sync-ci-images orchestrator after
    mirror-images-to-ci completes.

    Return codes:
        0: Completed successfully
        50: Failed
    """
    from pyartcd import jenkins, locks

    # Initialize Jenkins for title updates
    jenkins.init_jenkins()

    pipeline = SyncCIBuildconfigsPipeline(
        runtime,
        for_release=for_release,
        data_path=data_path,
        data_gitref=data_gitref,
        only_stream=only_stream,
        images=images,
        assembly=assembly,
    )

    # Run with per-version lock
    lock_name = locks.Lock.SYNC_CI_BUILDCONFIGS.value.format(version=for_release)
    lock_id = jenkins.get_build_path_or_random()  # Jenkins build identifier

    try:
        exit_code = await locks.run_with_lock(
            coro=pipeline.run(),
            lock=locks.Lock.SYNC_CI_BUILDCONFIGS,
            lock_name=lock_name,
            lock_id=lock_id,
        )
        exit(exit_code if exit_code is not None else 0)
    except Exception:
        runtime.logger.error(f"sync-ci-buildconfigs failed for {for_release}", exc_info=True)
        exit(50)
