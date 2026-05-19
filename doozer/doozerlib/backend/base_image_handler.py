"""
Konflux snapshot → release orchestration for OCP base / golang-builder images.

:class:`BaseImageHandler` accepts exactly one :class:`BaseImageSnapshotInput` per run. Hydration from Konflux DB for the
CLI lives in ``doozerlib.cli.images`` (see ``images:release-to-base-repo``).
"""

import asyncio
from dataclasses import dataclass
from typing import List, Optional, Tuple

from artcommonlib import logutil
from artcommonlib.util import (
    get_utc_now_formatted_str,
    normalize_group_name_for_k8s,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib.backend.konflux_client import API_VERSION, KIND_RELEASE, KIND_RELEASE_PLAN, KIND_SNAPSHOT, KonfluxClient
from doozerlib.constants import ART_IMAGES_BASE_APPLICATION
from kubernetes.dynamic import exceptions

LOGGER = logutil.get_logger(__name__)
ART_IMAGES_BASE_RELEASE_PLAN = "ocp-art-images-base-silent"


@dataclass(frozen=True)
class BaseImageSnapshotInput:
    """One base-image snapshot component (container + optional source git for the Konflux Snapshot CR)."""

    nvr: str
    distgit_key: str
    container_image: str
    rebase_repo_url: str = ''
    rebase_commitish: str = ''
    is_golang_builder: bool = False


class BaseImageHandler:
    """
    Create a Konflux Snapshot for ``art-images-base`` with **one** component, then a Release using
    ``ocp-art-images-base-silent``, and wait until the Release reports success.
    """

    def __init__(self, runtime, dry_run: bool = False):
        self.runtime = runtime
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

    async def snapshot_release(self, snapshot_input: BaseImageSnapshotInput) -> Optional[Tuple[str, str]]:
        """
        Run snapshot→release for exactly one base-image component.

        Returns:
            ``(release_name, snapshot_name)`` on success, else ``None``.
        """
        inp = snapshot_input

        if not inp.container_image:
            self.logger.error("No container image pullspec on snapshot input for NVR %s", inp.nvr)
            return None

        metadata = self.runtime.image_map.get(inp.distgit_key)
        if metadata is None:
            self.logger.error("Could not resolve metadata for distgit %s (%s)", inp.distgit_key, inp.nvr)
            return None

        if not metadata.should_trigger_base_image_release():
            self.logger.warning(
                "Image %s does not qualify for base image release workflow (NVR %s)",
                inp.distgit_key,
                inp.nvr,
            )
            return None

        if inp.rebase_repo_url and inp.rebase_commitish:
            self.logger.debug("Snapshot input %s includes source git revision", inp.nvr)
        else:
            self.logger.warning("No source git URL/revision on snapshot input for %s", inp.nvr)

        self.logger.info("Starting base image snapshot-release for NVR %s", inp.nvr)

        component = self._build_component_from_snapshot_input(inp)
        snapshot_name = await self._snapshot_from_components([component])
        if not snapshot_name:
            self.logger.error("Failed to create snapshot, aborting workflow")
            return None

        release_name = await self._create_release_from_snapshot(snapshot_name, inp.nvr)
        if not release_name:
            self.logger.error("Failed to create release, aborting workflow")
            return None

        if not await self._wait_for_release_completion(release_name):
            self.logger.error("Release did not complete successfully, aborting workflow")
            return None

        self.logger.info("✓ Base image workflow completed successfully")
        self.logger.info("  Snapshot: %s", snapshot_name)
        self.logger.info("  Release: %s", release_name)
        self.logger.info("  Release plan: %s", ART_IMAGES_BASE_RELEASE_PLAN)

        return release_name, snapshot_name

    def _build_component_from_snapshot_input(self, snapshot_input: BaseImageSnapshotInput) -> dict:
        """One Konflux snapshot component dict from :class:`BaseImageSnapshotInput`."""
        from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder

        app_name = KonfluxImageBuilder.get_application_name(self.runtime.group_config.name)
        nvr = snapshot_input.nvr

        if snapshot_input.is_golang_builder:
            comp_name = KonfluxImageBuilder.get_golang_builder_component_name(nvr)
        else:
            comp_name = KonfluxImageBuilder.get_component_name(app_name, snapshot_input.distgit_key)

        component = {
            "name": comp_name,
            "containerImage": snapshot_input.container_image,
        }

        if snapshot_input.rebase_repo_url and snapshot_input.rebase_commitish:
            component["source"] = {
                "git": {
                    "url": snapshot_input.rebase_repo_url,
                    "revision": snapshot_input.rebase_commitish,
                }
            }
            self.logger.debug("Added source information for %s from snapshot input", nvr)

        return component

    async def _snapshot_from_components(self, components: List[dict]) -> Optional[str]:
        """Build Snapshot CR from component list and create it in Konflux."""
        try:
            group_safe = normalize_group_name_for_k8s(self.runtime.group)

            if not group_safe:
                raise ValueError(f"Group name '{self.runtime.group}' produces invalid normalized name for Kubernetes")

            timestamp = get_utc_now_formatted_str()
            snapshot_name = f"{group_safe}-base-image-{timestamp}"

            if self.dry_run:
                self.logger.info("[DRY-RUN] Would create snapshot: %s", snapshot_name)
                return snapshot_name

            snapshot_obj = {
                "apiVersion": API_VERSION,
                "kind": KIND_SNAPSHOT,
                "metadata": {
                    "name": snapshot_name,
                    "namespace": self.namespace,
                    "labels": {
                        "test.appstudio.openshift.io/type": "override",
                        "appstudio.openshift.io/application": ART_IMAGES_BASE_APPLICATION,
                    },
                },
                "spec": {
                    "application": ART_IMAGES_BASE_APPLICATION,
                    "components": components,
                },
            }

            return await self._create_snapshot_object(snapshot_obj)

        except Exception as e:
            self.logger.error("Failed to create snapshot: %s", e)
            self.logger.error("Exception type: %s", type(e).__name__)
            self.logger.error("Exception details: %s", str(e))
            return None

    async def _create_snapshot_object(self, snapshot_obj) -> Optional[str]:
        """Create snapshot resource with unique timestamped name."""
        try:
            result_snapshot = await self.konflux_client._create(snapshot_obj)
            snapshot_url = self.konflux_client.resource_url(result_snapshot)
            self.logger.info("✓ Created base-image snapshot: %s", snapshot_url)
            return result_snapshot.metadata.name
        except Exception as e:
            self.logger.error("Failed to create snapshot: %s", e)
            return None

    async def _create_release_from_snapshot(self, snapshot_name: str, released_nvrs: str) -> Optional[str]:
        """
        Create Konflux Release from snapshot using the ocp-art-images-base-silent ReleasePlan.
        """
        try:
            if not self.dry_run:
                self.logger.info("Verifying release plan %s exists...", ART_IMAGES_BASE_RELEASE_PLAN)
                try:
                    await self.konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, ART_IMAGES_BASE_RELEASE_PLAN)
                except exceptions.NotFoundError:
                    raise RuntimeError(
                        f"Release plan {ART_IMAGES_BASE_RELEASE_PLAN} not found in namespace {self.namespace}"
                    ) from None

                self.logger.info("Waiting for snapshot %s to become available...", snapshot_name)
                snapshot_available = await self._wait_for_snapshot_availability(snapshot_name)
                if not snapshot_available:
                    raise RuntimeError(f"Snapshot {snapshot_name} did not become available in time")

            metadata = {
                "generateName": "ocp-base-image-release-",
                "namespace": self.namespace,
                "labels": {
                    "appstudio.openshift.io/application": ART_IMAGES_BASE_APPLICATION,
                },
                "annotations": {
                    "art.redhat.com/kind": "image",
                    "art.redhat.com/group": self.runtime.group_config.name,
                    "art.redhat.com/assembly": getattr(self.runtime, "assembly", "stream"),
                    "art.redhat.com/env": "base-image-workflow",
                    "art.redhat.com/nvrs": released_nvrs,
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
                self.logger.info("[DRY-RUN] Would create release with plan: %s", ART_IMAGES_BASE_RELEASE_PLAN)
                self.logger.info("[DRY-RUN] Release object: %s", release_obj)
                return f"dry-run-release-{snapshot_name}"

            created_release = await self.konflux_client._create(release_obj)
            release_name = created_release.metadata.name
            release_url = self.konflux_client.resource_url(created_release)

            self.logger.info("✓ Created base-image release: %s", release_url)
            return release_name

        except Exception as e:
            self.logger.error("Failed to create release from snapshot %s: %s", snapshot_name, e)
            self.logger.error("Exception type: %s", type(e).__name__)
            self.logger.error("Exception details: %s", str(e))
            return None

    async def _wait_for_release_completion(self, release_name: str, timeout_minutes: int = 30) -> bool:
        """Wait for Konflux release ``Released/Succeeded`` condition."""
        try:
            if self.dry_run:
                return True

            timeout_seconds = timeout_minutes * 60
            poll_interval = 30
            elapsed = 0

            while elapsed < timeout_seconds:
                try:
                    release_obj = await self.konflux_client._get(API_VERSION, KIND_RELEASE, release_name)
                    release_status = release_obj.get("status", {})
                    conditions = release_status.get("conditions", [])

                    for condition in conditions:
                        if condition.get("type") == "Released":
                            cond_status = condition.get("status")
                            reason = condition.get("reason", "")

                            if cond_status == "True" and reason == "Succeeded":
                                self.logger.info("✓ Release %s completed successfully", release_name)
                                return True
                            elif cond_status == "False" and reason == "Failed":
                                message = condition.get("message", "No details")
                                self.logger.error("Release %s failed: %s", release_name, message)
                                for cond in conditions:
                                    if cond.get("type") == "ManagedPipelineProcessed" and cond.get("status") == "False":
                                        pipeline_msg = cond.get("message", "")
                                        if pipeline_msg:
                                            self.logger.error("Pipeline failure details: %s", pipeline_msg)
                                return False
                            elif cond_status == "False" and reason == "Progressing":
                                if elapsed % 60 == 0:
                                    self.logger.info(
                                        "Release %s is progressing... (%s minutes elapsed)",
                                        release_name,
                                        elapsed // 60,
                                    )
                                break

                except exceptions.NotFoundError:
                    self.logger.error("Release %s not found", release_name)
                    return False

                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

            self.logger.error("Release %s timed out after %s minutes", release_name, timeout_minutes)
            return False

        except Exception as e:
            self.logger.error("Failed to monitor release %s: %s", release_name, e)
            return False

    async def _wait_for_snapshot_availability(self, snapshot_name: str, timeout_minutes: int = 1) -> bool:
        """Wait until Snapshot object is readable after create."""
        try:
            if self.dry_run:
                return True

            timeout_seconds = timeout_minutes * 60
            poll_interval = 10
            elapsed = 0

            while elapsed < timeout_seconds:
                try:
                    await self.konflux_client._get(API_VERSION, KIND_SNAPSHOT, snapshot_name)
                    self.logger.info("✓ Snapshot %s is available", snapshot_name)
                    return True
                except exceptions.NotFoundError:
                    self.logger.info("Waiting for snapshot %s... (%ss elapsed)", snapshot_name, elapsed)
                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval

            self.logger.error("Snapshot %s not available after %s minutes", snapshot_name, timeout_minutes)
            return False

        except Exception as e:
            self.logger.error("Failed to wait for snapshot %s: %s", snapshot_name, e)
            return False
