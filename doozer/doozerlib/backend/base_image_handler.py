"""
Base Image Handler - Orchestrates snapshot-to-release workflow for golang base images.

This module handles the workflow for converting golang base image builds
from Konflux snapshots to releases. Currently creates snapshots and releases,
with URL extraction and streams.yml updates to be implemented later.
"""

import asyncio
from typing import List, Optional, Tuple

from artcommonlib import logutil
from artcommonlib.util import (
    get_utc_now_formatted_str,
    normalize_group_name_for_k8s,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib.backend.konflux_client import API_VERSION, KIND_RELEASE, KIND_RELEASE_PLAN, KIND_SNAPSHOT, KonfluxClient
from doozerlib.image import ImageMetadata
from kubernetes.dynamic import exceptions

LOGGER = logutil.get_logger(__name__)
ART_IMAGES_BASE_RELEASE_PLAN = "ocp-art-images-base-silent"


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

    def __init__(self, runtime, image_data_list: List[Tuple[ImageMetadata, str, str]], dry_run: bool = False):
        """
        Initialize handler for batch processing (single image = batch of 1).

        Args:
            runtime: Runtime instance
            image_data_list: List of (ImageMetadata, nvr, pullspec) tuples
            dry_run: Whether to perform dry run
        """
        self.runtime = runtime
        self.image_data_list = image_data_list
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
            self.logger.info(f"Starting base image snapshot-release workflow for {len(self.image_data_list)} images")

            snapshot_name = await self._create_snapshot()
            if not snapshot_name:
                self.logger.error("Failed to create snapshot, aborting workflow")
                return None

            release_name = await self._create_release_from_snapshot(snapshot_name)
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
            self.logger.info(f"  Release plan: {ART_IMAGES_BASE_RELEASE_PLAN}")

            return release_name, snapshot_name

        except Exception as e:
            self.logger.error(f"Base image workflow failed: {e}")
            return None

    def _derive_component_name(self, group_name: str, base_component: str) -> str:
        """
        Derive the correct component name for snapshot creation.

        Args:
            group_name: Raw group name from runtime (e.g., "openshift-4.21")
            base_component: Component name from distgit.component with "-container" suffix removed

        Returns:
            str: Properly formatted component name matching ReleasePlanAdmission.yml expectations
        """
        ose_group = group_name.replace('.', '-')
        if ose_group.startswith("openshift-"):
            ose_group = ose_group.replace("openshift-", "ose-", 1)

        if base_component == "openshift-base-nodejs":
            component = "openshift-base-nodejs-rhel9"
        elif base_component == "ose-aws-efs-utils":
            component = "ose-aws-efs-utils-base"
        elif base_component == "ose-ovn-kubernetes-base":
            component = "ovn-kubernetes-base"
        else:
            component = base_component

        return f"{ose_group}-{component}"

    async def _create_snapshot(self) -> Optional[str]:
        """
        Create snapshot from image_data_list (always batch)

        Returns:
            str: Snapshot name if successful, None if failed
        """
        try:
            group_safe = normalize_group_name_for_k8s(self.runtime.group)
            if not group_safe:
                raise ValueError(f"Group name '{self.runtime.group}' produces invalid normalized name for Kubernetes")

            timestamp = get_utc_now_formatted_str()
            snapshot_name = f"{group_safe}-batch-base-images-{timestamp}"

            if self.dry_run:
                self.logger.info(f"[DRY-RUN] Would create snapshot: {snapshot_name}")
                return snapshot_name

            components = []
            for metadata, nvr, image_pullspec in self.image_data_list:
                base_component = metadata.config.distgit.component
                if base_component.endswith("-container"):
                    base_component = base_component[:-10]

                comp_name = self._derive_component_name(self.runtime.group_config.name, base_component)
                components.append(
                    {
                        "name": comp_name,
                        "containerImage": image_pullspec,
                    }
                )

            application_name = self.runtime.group_config.name.replace('.', '-')

            snapshot_obj = {
                "apiVersion": API_VERSION,
                "kind": KIND_SNAPSHOT,
                "metadata": {
                    "name": snapshot_name,
                    "namespace": self.namespace,
                    "labels": {
                        "test.appstudio.openshift.io/type": "override",
                        "appstudio.openshift.io/application": application_name,
                    },
                },
                "spec": {
                    "application": application_name,
                    "components": components,
                },
            }

            return await self._create_snapshot_object(snapshot_obj)

        except Exception as e:
            self.logger.error(f"Failed to create snapshot: {e}")
            self.logger.error(f"Exception type: {type(e).__name__}")
            self.logger.error(f"Exception details: {str(e)}")
            return None

    async def _create_snapshot_object(self, snapshot_obj) -> Optional[str]:
        """Create snapshot resource with unique timestamped name"""
        try:
            result_snapshot = await self.konflux_client._create(snapshot_obj)
            snapshot_url = self.konflux_client.resource_url(result_snapshot)
            self.logger.info(f"✓ Created base-image snapshot: {snapshot_url}")
            return result_snapshot.metadata.name
        except Exception as e:
            self.logger.error(f"Failed to create snapshot: {e}")
            return None

    async def _create_release_from_snapshot(self, snapshot_name: str) -> Optional[str]:
        """
        Create Konflux release using the same pattern as CreateReleaseCli.new_release().

        Args:
            snapshot_name: Name of the snapshot to create release from

        Returns:
            str: Release name if successful, None if failed
        """
        try:
            if not self.dry_run:
                self.logger.info(f"Verifying release plan {ART_IMAGES_BASE_RELEASE_PLAN} exists...")
                try:
                    await self.konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, ART_IMAGES_BASE_RELEASE_PLAN)
                except exceptions.NotFoundError:
                    raise RuntimeError(
                        f"Release plan {ART_IMAGES_BASE_RELEASE_PLAN} not found in namespace {self.namespace}"
                    )

                self.logger.info(f"Waiting for snapshot {snapshot_name} to become available...")
                snapshot_available = await self._wait_for_snapshot_availability(snapshot_name)
                if not snapshot_available:
                    raise RuntimeError(f"Snapshot {snapshot_name} did not become available in time")

            application_name = self.runtime.group_config.name.replace('.', '-')

            metadata = {
                "generateName": "ocp-base-image-release-",
                "namespace": self.namespace,
                "labels": {
                    "appstudio.openshift.io/application": application_name,
                },
                "annotations": {
                    "art.redhat.com/kind": "image",
                    "art.redhat.com/group": self.runtime.group_config.name,
                    "art.redhat.com/assembly": getattr(self.runtime, 'assembly', 'stream'),
                    "art.redhat.com/env": "base-image-workflow",
                },
            }

            release_obj = {
                "apiVersion": API_VERSION,
                "kind": KIND_RELEASE,
                "metadata": metadata,
                "spec": {
                    "releasePlan": ART_IMAGES_BASE_RELEASE_PLAN,
                    "snapshot": snapshot_name,
                },
            }

            if self.dry_run:
                self.logger.info(f"[DRY-RUN] Would create release with plan: {ART_IMAGES_BASE_RELEASE_PLAN}")
                self.logger.info(f"[DRY-RUN] Release object: {release_obj}")
                return f"dry-run-release-{snapshot_name}"

            created_release = await self.konflux_client._create(release_obj)
            release_name = created_release.metadata.name
            release_url = self.konflux_client.resource_url(created_release)

            self.logger.info(f"✓ Created base-image release: {release_url}")
            return release_name

        except Exception as e:
            self.logger.error(f"Failed to create release from snapshot {snapshot_name}: {e}")
            self.logger.error(f"Exception type: {type(e).__name__}")
            self.logger.error(f"Exception details: {str(e)}")
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
            poll_interval = 30
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
                                for cond in conditions:
                                    if cond.get('type') == 'ManagedPipelineProcessed' and cond.get('status') == 'False':
                                        pipeline_msg = cond.get('message', '')
                                        if pipeline_msg:
                                            self.logger.error(f"Pipeline failure details: {pipeline_msg}")
                                return False
                            elif status == 'False' and reason == 'Progressing':
                                if elapsed % 60 == 0:
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

    async def _wait_for_snapshot_availability(self, snapshot_name: str, timeout_minutes: int = 1) -> bool:
        """
        Wait for Konflux snapshot to become available after creation.

        Args:
            snapshot_name: Name of the snapshot to wait for
            timeout_minutes: Maximum time to wait

        Returns:
            bool: True if snapshot becomes available, False otherwise
        """
        try:
            if self.dry_run:
                return True

            timeout_seconds = timeout_minutes * 60
            poll_interval = 10
            elapsed = 0

            while elapsed < timeout_seconds:
                try:
                    await self.konflux_client._get(API_VERSION, KIND_SNAPSHOT, snapshot_name)
                    self.logger.info(f"✓ Snapshot {snapshot_name} is available")
                    return True
                except exceptions.NotFoundError:
                    self.logger.info(f"Waiting for snapshot {snapshot_name}... ({elapsed}s elapsed)")
                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval

            self.logger.error(f"Snapshot {snapshot_name} not available after {timeout_minutes} minutes")
            return False

        except Exception as e:
            self.logger.error(f"Failed to wait for snapshot {snapshot_name}: {e}")
            return False
