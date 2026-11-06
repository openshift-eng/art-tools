"""
mirror-images-to-ci pipeline: Mirror builder/base images to CI registries.

Extracted from sync-ci-images (see ART-21961) so mirroring can run as an
independent, idempotent job rather than a step in the monolithic pipeline.
"""

import re

# Import for CLI registration
import click

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.pipelines.ci_image_sync_common import CIImageSyncPipelineBase
from pyartcd.runtime import Runtime


class MirrorImagesToCIPipeline(CIImageSyncPipelineBase):
    """
    Mirrors builder and base images to CI registries (quay, app.ci).

    Mirroring is idempotent, so this pipeline always runs and does not gate
    on whether ocp-build-data has changed since the last run.
    """

    def __init__(
        self,
        runtime: Runtime,
        version: str,
        data_path: str = "",
        data_gitref: str = "",
        only_stream: str = "",
        images: str = "",
        assembly: str = "stream",
        update_images_only_when_missing: bool = False,
    ) -> None:
        """
        Initialize mirror-images-to-ci pipeline.

        Args:
            runtime: PyARTCD runtime instance
            version: OCP version to sync (e.g., "4.17")
            data_path: ocp-build-data fork URL (default: official repo)
            data_gitref: ocp-build-data git branch/tag/sha (default: use version branch)
            only_stream: Specific stream from streams.yml.
            images: Comma-separated distgit keys of images with ci_alignment.upstream_image.
            assembly: Assembly name (default: "stream")
            update_images_only_when_missing: Only update images if missing
        """
        self.runtime = runtime
        self._logger = runtime.logger
        self.version = version
        self.data_path = data_path or OCP_BUILD_DATA_URL
        self.data_gitref = data_gitref
        self.only_stream = only_stream
        self.images = [i.strip() for i in images.split(',') if i.strip()] if images else []
        self.assembly = assembly
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
            raise ValueError("VERSION is required")

        if not re.match(r'^\d+\.\d+$', self.version):
            raise ValueError(f"Invalid VERSION format: {self.version}. Expected format: X.Y (e.g., 4.17)")

        # Validate assembly format (alphanumeric, dash, dot, underscore)
        if self.assembly and not re.match(r'^[\w.-]+$', self.assembly):
            raise ValueError(
                f"Invalid ASSEMBLY format: {self.assembly}. Only alphanumeric, dash, dot, and underscore allowed"
            )

    async def _mirror_images_to_ci(self, doozer_opts: str, auth_file: str) -> None:
        """Mirror builder and base images to CI registries."""
        self._logger.info(f"{self.version}: Mirroring images")
        mirror_args = f"{self._filter_args} --registry-auth {auth_file} "
        if self.update_images_only_when_missing:
            mirror_args += "--only-if-missing "
        if self._live_test_mode:
            mirror_args += "--live-test-mode "
        if self.runtime.dry_run:
            mirror_args += "--dry-run"
        await self._run_doozer_command(doozer_opts, "images:streams mirror", mirror_args.strip())

    async def run(self) -> int:
        """
        Main pipeline: mirror builder/base images to CI registries for a single OCP version.

        Workflow:
        1. Clone ocp-build-data
        2. Mirror builder/base images to CI registries

        Returns:
            Return code: 0=success, 50=failure
        """
        jenkins.update_title(f' [{self.version}]')
        self._logger.info(f"Starting mirror-images-to-ci for {self.version}")

        group_dir = None

        try:
            group_dir = await self._clone_ocp_build_data(self.version)

            with self._create_registry_config() as auth_file:
                self._logger.info(f"Created registry config: {auth_file}")
                doozer_opts = self._build_doozer_options(group_dir, auth_file)

                await self._mirror_images_to_ci(doozer_opts, auth_file)

            self._cleanup(group_dir)
            return 0

        except Exception as e:
            self._logger.error(f"{self.version}: Failed with error: {e}", exc_info=True)
            self._cleanup(group_dir)
            raise  # Re-raise to fail the job


# CLI Command Registration
@cli.command(
    "mirror-images-to-ci",
    help="Mirror builder/base images to CI registries (quay, app.ci) for a single OCP version. "
    "Extracted from sync-ci-images so it can run independently and unconditionally.",
)
@click.option(
    '--version',
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
@click.option(
    '--update-images-only-when-missing',
    is_flag=True,
    default=False,
    help='Pass --only-if-missing to doozer mirror (update only missing images)',
)
@pass_runtime
@click_coroutine
async def mirror_images_to_ci_cli(
    runtime: Runtime,
    version: str,
    data_path: str,
    data_gitref: str,
    only_stream: str,
    images: str,
    assembly: str,
    update_images_only_when_missing: bool,
):
    """
    CLI entrypoint for mirror-images-to-ci pipeline.

    Mirrors builder/base images to CI registries. Typically invoked by the
    sync-ci-images orchestrator.

    Return codes:
        0: Mirroring completed successfully
        50: Mirroring failed
    """
    from pyartcd import jenkins, locks

    # Initialize Jenkins for title updates
    jenkins.init_jenkins()

    pipeline = MirrorImagesToCIPipeline(
        runtime,
        version=version,
        data_path=data_path,
        data_gitref=data_gitref,
        only_stream=only_stream,
        images=images,
        assembly=assembly,
        update_images_only_when_missing=update_images_only_when_missing,
    )

    # Run with per-version lock
    lock_name = locks.Lock.MIRROR_IMAGES_TO_CI.value.format(version=version)
    lock_id = jenkins.get_build_path_or_random()  # Jenkins build identifier

    try:
        exit_code = await locks.run_with_lock(
            coro=pipeline.run(),
            lock=locks.Lock.MIRROR_IMAGES_TO_CI,
            lock_name=lock_name,
            lock_id=lock_id,
        )
        exit(exit_code if exit_code is not None else 0)
    except Exception:
        runtime.logger.error(f"mirror-images-to-ci failed for {version}", exc_info=True)
        exit(50)
