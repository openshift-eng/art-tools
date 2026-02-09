"""
Base Image Handler - Orchestrates snapshot-to-release workflow for golang base images.

This module handles the workflow for converting golang base image builds
from Konflux snapshots to releases. Currently creates snapshots and releases,
with URL extraction and streams.yml updates to be implemented later.
"""

import asyncio
import os
from typing import Optional, Tuple

from artcommonlib import logutil
from artcommonlib.exectools import cmd_assert, cmd_gather_async
from artcommonlib.util import (
    get_utc_now_formatted_str,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib.backend.konflux_client import API_VERSION, KIND_RELEASE, KIND_SNAPSHOT, KonfluxClient
from doozerlib.image import ImageMetadata
from elliottlib.cli.konflux_release_cli import create_release_from_snapshot
from elliottlib.runtime import Runtime as ElliottRuntime
from kubernetes.dynamic import exceptions

LOGGER = logutil.get_logger(__name__)


class BaseImageHandler:
    """
    Handles the snapshot-to-release workflow for golang base images.

    This class orchestrates the process:
    1. Create snapshot from NVR
    2. Create release from snapshot using appropriate release plan
    3. Wait for release completion

    Future enhancements:
    - Extract URLs from release artifacts
    - Create PR to update streams.yml with new URLs
    """

    def __init__(self, metadata: ImageMetadata, nvr: str, image_pullspec: str, dry_run: bool = False):
        """
        Initialize BaseImageHandler.

        Args:
            metadata: ImageMetadata instance containing component information
            nvr: Build NVR (Name-Version-Release)
            image_pullspec: Original Konflux build image pullspec
            dry_run: Whether this is a dry run
        """
        self.metadata = metadata
        self.runtime = metadata.runtime
        self.nvr = nvr
        self.image_pullspec = image_pullspec
        self.dry_run = dry_run
        self.logger = LOGGER

        self.namespace = resolve_konflux_namespace_by_product(self.runtime.product, None)
        kubeconfig = resolve_konflux_kubeconfig_by_product(self.runtime.product, None)

        self.konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.namespace,
            config_file=kubeconfig,
            context=None,
            dry_run=dry_run,
        )

    async def process_base_image_completion(self) -> Optional[Tuple[str, str]]:
        """
        Process base image build completion through snapshot-to-release workflow.

        Returns:
            Tuple[str, str]: (release_name, snapshot_name) if successful, None if failed
        """
        try:
            self.logger.info(f"Starting base image snapshot-release workflow for NVR: {self.nvr}")

            snapshot_name = await self._create_snapshot(self.nvr)
            if not snapshot_name:
                self.logger.error("Failed to create snapshot, aborting workflow")
                return None

            group_version = self.runtime.group.replace('openshift-', '').replace('.', '-')
            release_plan = f"ocp-art-base-images-silent-{group_version}-rhel9"

            release_name = await self._create_release_from_snapshot(snapshot_name, release_plan)
            if not release_name:
                self.logger.error("Failed to create release, aborting workflow")
                return None

            completed_successfully = await self._wait_for_release_completion(release_name)
            if not completed_successfully:
                self.logger.error("Release did not complete successfully, aborting workflow")
                return None

            self.logger.info("✓ Base image workflow completed successfully")
            self.logger.info(f"  Snapshot: {snapshot_name}")
            self.logger.info(f"  Release: {release_name}")
            self.logger.info(f"  Release plan: {release_plan}")

            # TODO: URL extraction and streams.yml update will be implemented
            # once we have clarity on the Release artifact structure

            return release_name, snapshot_name  # Return release info instead of URLs for now

        except Exception as e:
            self.logger.error(f"Base image workflow failed: {e}")
            return None

    async def _create_snapshot(self, nvr: str) -> Optional[str]:
        """
        Create Konflux snapshot from build NVR using existing Elliott functionality.

        Args:
            nvr: Build NVR to create snapshot from

        Returns:
            str: Snapshot name if successful, None if failed
        """
        try:
            component_name = self.metadata.distgit_key

            timestamp = get_utc_now_formatted_str()
            group_safe = self.runtime.group.replace('openshift-', '').replace('.', '-')
            custom_snapshot_name = f"{group_safe}-{component_name}-{timestamp}"

            cmd = [
                "elliott",
                "-g",
                self.runtime.group,
                "snapshot",
                "new",
                nvr,
                "--apply",
                "--name",
                custom_snapshot_name,
            ]

            konflux_art_images_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
            if konflux_art_images_auth_file:
                cmd.append(f"--pull-secret={konflux_art_images_auth_file}")
            else:
                self.logger.warning(
                    "KONFLUX_ART_IMAGES_AUTH_FILE not set - Elliott snapshot may fail with authorization errors"
                )

            if self.dry_run:
                self.logger.info(f"DRY RUN - would execute: {' '.join(cmd)}")
                return f"dry-run-snapshot-{nvr}"

            try:
                rc, _, stderr = await cmd_gather_async(cmd, check=False)

                if rc != 0:
                    self.logger.error(f"Elliott command failed with exit code {rc}")
                    self.logger.error(f"Command was: {' '.join(cmd)}")
                    self.logger.error(f"Error output: {stderr}")
                    return None

            except Exception as cmd_error:
                self.logger.error(f"Elliott command execution failed: {cmd_error}")
                self.logger.error(f"Command was: {' '.join(cmd)}")
                raise

            self.logger.info(f"✓ Created base-image snapshot: {custom_snapshot_name}")
            return custom_snapshot_name

        except Exception as e:
            self.logger.error(f"Failed to create snapshot from NVR {nvr}: {e}")
            self.logger.error(f"Exception type: {type(e).__name__}")
            self.logger.error(f"Exception details: {str(e)}")
            return None

    async def _create_release_from_snapshot(self, snapshot_name: str, release_plan: str) -> Optional[str]:
        """
        Create Konflux release from snapshot using Elliott's new functionality.

        Args:
            snapshot_name: Name of the snapshot to create release from
            release_plan: Release plan to use

        Returns:
            str: Release name if successful, None if failed
        """
        try:
            elliott_runtime = ElliottRuntime()

            elliott_runtime.group = self.runtime.group
            elliott_runtime.group_config = self.runtime.group_config
            elliott_runtime.assembly = getattr(self.runtime, 'assembly', 'stream')
            elliott_runtime.product = getattr(self.runtime, 'product', 'ocp')
            elliott_runtime.dry_run = self.dry_run

            release_name = await create_release_from_snapshot(
                runtime=elliott_runtime,
                snapshot_name=snapshot_name,
                release_plan=release_plan,
                namespace=self.namespace,
                apply=not self.dry_run,
            )

            return release_name

        except Exception as e:
            self.logger.error(f"Failed to create release from snapshot {snapshot_name}: {e}")
            return None

    async def _wait_for_release_completion(self, release_name: str, timeout_minutes: int = 30) -> bool:
        """
        Wait for Konflux release to complete successfully.

        Args:
            release_name: Name of the release to monitor
            timeout_minutes: Maximum time to wait

        Returns:
            bool: True if release completed successfully, False otherwise
        """
        try:
            if self.dry_run:
                return True

            timeout_seconds = timeout_minutes * 60
            poll_interval = 30  # Poll every 30 seconds
            elapsed = 0

            while elapsed < timeout_seconds:
                try:
                    release_obj = await self.konflux_client._get(API_VERSION, KIND_RELEASE, release_name)
                    status = release_obj.get('status', {})
                    conditions = status.get('conditions', [])

                    for condition in conditions:
                        if condition.get('type') == 'Released':
                            status = condition.get('status')
                            reason = condition.get('reason', '')

                            if status == 'True' and reason == 'Succeeded':
                                self.logger.info(f"✓ Release {release_name} completed successfully")
                                return True
                            elif status == 'False' and reason == 'Failed':
                                message = condition.get('message', 'No details')
                                self.logger.error(f"Release {release_name} failed: {message}")
                                # Log additional failure details from ManagedPipelineProcessed if available
                                for cond in conditions:
                                    if cond.get('type') == 'ManagedPipelineProcessed' and cond.get('status') == 'False':
                                        pipeline_msg = cond.get('message', '')
                                        if pipeline_msg:
                                            self.logger.error(f"Pipeline failure details: {pipeline_msg}")
                                return False
                            elif status == 'False' and reason == 'Progressing':
                                # This is normal - Release is still in progress
                                if elapsed % 60 == 0:  # Log every minute
                                    self.logger.info(
                                        f"Release {release_name} is progressing... ({elapsed // 60} minutes elapsed)"
                                    )
                                break

                except exceptions.NotFoundError:
                    self.logger.error(f"Release {release_name} not found")
                    return False

                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

            self.logger.error(f"Release {release_name} timed out after {timeout_minutes} minutes")
            return False

        except Exception as e:
            self.logger.error(f"Failed to monitor release {release_name}: {e}")
            return False
