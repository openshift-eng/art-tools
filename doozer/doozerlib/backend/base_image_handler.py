"""
Konflux snapshot → release orchestration for OCP base / golang-builder images.

:class:`BaseImageHandler` accepts exactly one :class:`BaseImageSnapshotInput` per run. Hydration from Konflux DB for the
CLI lives in ``doozerlib.cli.images`` (see ``images:release-to-base-repo``).
"""

import asyncio
from dataclasses import dataclass
from typing import Optional, Tuple

from artcommonlib import logutil
from artcommonlib.util import (
    get_utc_now_formatted_str,
    normalize_group_name_for_k8s,
    resolve_konflux_base_image_release_targets,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib import util as doozer_util
from doozerlib.backend.konflux_client import API_VERSION, KIND_RELEASE, KIND_RELEASE_PLAN, KIND_SNAPSHOT, KonfluxClient
from kubernetes.dynamic import exceptions


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
    Create a Konflux Snapshot for the product's base-image Application with **one** component, then a Release using
    the product's silent base-image ReleasePlan, and wait until the Release reports success.
    """

    def __init__(self, runtime, dry_run: bool = False):
        self.runtime = runtime
        self.dry_run = dry_run
        # Prefix until snapshot_release scopes `[entity]` to the image (same pattern as ImageMetadata).
        self.logger = logutil.EntityLoggingAdapter(self.runtime.logger, extra={'entity': 'base-image'})

        self.namespace = resolve_konflux_namespace_by_product(self.runtime.product, None)
        kubeconfig = resolve_konflux_kubeconfig_by_product(self.runtime.product, None)

        resolved_plan, resolved_application = resolve_konflux_base_image_release_targets(self.runtime.product)
        self.base_image_release_plan = resolved_plan
        self.base_image_application = resolved_application
        self.logger.info(
            f"Base image Konflux targets: namespace={self.namespace} "
            f"releasePlan={self.base_image_release_plan} application={self.base_image_application}"
        )

        self.konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.namespace,
            config_file=kubeconfig,
            context=None,
            dry_run=dry_run,
        )

    def _scoped_logger(self, entity: str):
        return logutil.EntityLoggingAdapter(self.runtime.logger, extra={'entity': entity})

    async def snapshot_release(self, snapshot_input: BaseImageSnapshotInput) -> Optional[Tuple[str, str]]:
        """
        Run snapshot→release for exactly one base-image component.

        Returns:
            ``(release_name, snapshot_name)`` on success, else ``None``.
        """
        # Provisional entity (matches qualified_key for typical OCP images) until image_map lookup succeeds.
        self.logger = self._scoped_logger(f"containers/{snapshot_input.distgit_key}")

        if not snapshot_input.container_image:
            self.logger.error(f"No container image pullspec (nvr={snapshot_input.nvr})")
            return None

        metadata = self.runtime.image_map.get(snapshot_input.distgit_key)
        if metadata is None:
            self.logger.error("Image metadata not found in runtime.image_map")
            return None

        self.logger = self._scoped_logger(metadata.qualified_key)

        if not metadata.should_trigger_base_image_release():
            self.logger.warning("Does not qualify for base image release workflow")
            return None

        if snapshot_input.rebase_repo_url and snapshot_input.rebase_commitish:
            self.logger.debug("Snapshot input includes source git revision")
        else:
            self.logger.warning("No source git URL/revision on snapshot input")

        component = self._build_component_from_snapshot_input(snapshot_input)
        self.logger.info(f"Starting snapshot-release (nvr={snapshot_input.nvr}, component={component['name']})")

        snapshot_name = await self._snapshot_from_component(component)
        if not snapshot_name:
            self.logger.error("Failed to create snapshot")
            return None

        release_name = await self._create_release_from_snapshot(snapshot_name, snapshot_input.nvr)
        if not release_name:
            self.logger.error(f"Failed to create release (snapshot={snapshot_name})")
            return None

        if not await self._wait_for_release_completion(release_name):
            self.logger.error(f"Release did not complete successfully (release={release_name})")
            return None

        self.logger.info(
            f"Snapshot-release completed (nvr={snapshot_input.nvr}, snapshot={snapshot_name}, release={release_name})"
        )

        return release_name, snapshot_name

    def _build_component_from_snapshot_input(self, snapshot_input: BaseImageSnapshotInput) -> dict:
        """One Konflux snapshot component dict from :class:`BaseImageSnapshotInput`."""
        app_name = doozer_util.konflux_application_name(self.runtime.group_config.name)
        nvr = snapshot_input.nvr

        if snapshot_input.is_golang_builder:
            comp_name = doozer_util.konflux_golang_builder_component_name(nvr)
        else:
            comp_name = doozer_util.konflux_image_component_name(app_name, snapshot_input.distgit_key)

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
            self.logger.debug("Added source git from snapshot input")

        return component

    async def _snapshot_from_component(self, component: dict) -> Optional[str]:
        """Build Snapshot CR with one component and create it in Konflux."""
        try:
            group_safe = normalize_group_name_for_k8s(self.runtime.group)

            if not group_safe:
                raise ValueError(f"Group name '{self.runtime.group}' produces invalid normalized name for Kubernetes")

            timestamp = get_utc_now_formatted_str()
            snapshot_name = f"{group_safe}-base-image-{timestamp}"

            if self.dry_run:
                self.logger.info(f"[DRY-RUN] Would create snapshot {snapshot_name}")
                return snapshot_name

            snapshot_obj = {
                "apiVersion": API_VERSION,
                "kind": KIND_SNAPSHOT,
                "metadata": {
                    "name": snapshot_name,
                    "namespace": self.namespace,
                    "labels": {
                        "test.appstudio.openshift.io/type": "override",
                        "appstudio.openshift.io/application": self.base_image_application,
                    },
                },
                "spec": {
                    "application": self.base_image_application,
                    "components": [component],
                },
            }

            return await self._create_snapshot_object(snapshot_obj)

        except Exception as e:
            self.logger.error(f"Failed to create snapshot ({type(e).__name__}: {e})")
            return None

    async def _create_snapshot_object(self, snapshot_obj) -> Optional[str]:
        """Create snapshot resource with unique timestamped name."""
        try:
            result_snapshot = await self.konflux_client._create(snapshot_obj)
            snapshot_url = self.konflux_client.resource_url(result_snapshot)
            name = result_snapshot.metadata.name
            self.logger.info(f"Created snapshot {name} ({snapshot_url})")
            return name
        except Exception as e:
            meta_name = snapshot_obj.get("metadata", {}).get("name", "")
            self.logger.error(f"Failed to create snapshot object (name={meta_name}): {type(e).__name__}: {e}")
            return None

    async def _create_release_from_snapshot(self, snapshot_name: str, released_nvr: str) -> Optional[str]:
        """
        Create Konflux Release from snapshot using the product's silent base-image ReleasePlan.

        Args:
            snapshot_name: Snapshot resource name.
            released_nvr: Single image NVR (stored on annotation ``art.redhat.com/nvrs`` for compatibility).
        """
        try:
            if not self.dry_run:
                self.logger.info(f"Verifying release plan {self.base_image_release_plan} exists")
                try:
                    await self.konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, self.base_image_release_plan)
                except exceptions.NotFoundError:
                    raise RuntimeError(
                        f"Release plan {self.base_image_release_plan} not found in namespace {self.namespace}"
                    ) from None

                self.logger.info(f"Waiting for snapshot {snapshot_name} to become available")
                snapshot_available = await self._wait_for_snapshot_availability(snapshot_name)
                if not snapshot_available:
                    raise RuntimeError(f"Snapshot {snapshot_name} did not become available in time")

            release_metadata = {
                "generateName": "ocp-base-image-release-",
                "namespace": self.namespace,
                "labels": {
                    "appstudio.openshift.io/application": self.base_image_application,
                },
                "annotations": {
                    "art.redhat.com/kind": "image",
                    "art.redhat.com/group": self.runtime.group_config.name,
                    "art.redhat.com/assembly": getattr(self.runtime, "assembly", "stream"),
                    "art.redhat.com/env": "base-image-workflow",
                    "art.redhat.com/nvrs": released_nvr,
                },
            }

            release_obj = {
                "apiVersion": API_VERSION,
                "kind": KIND_RELEASE,
                "metadata": release_metadata,
                "spec": {
                    "releasePlan": self.base_image_release_plan,
                    "snapshot": snapshot_name,
                },
            }

            if self.dry_run:
                self.logger.info(
                    f"[DRY-RUN] Would create release for snapshot={snapshot_name} plan={self.base_image_release_plan}"
                )
                self.logger.debug(f"[DRY-RUN] Release object: {release_obj}")
                return f"dry-run-release-{snapshot_name}"

            created_release = await self.konflux_client._create(release_obj)
            release_name = created_release.metadata.name
            release_url = self.konflux_client.resource_url(created_release)

            self.logger.info(f"Created release {release_name} for snapshot {snapshot_name} ({release_url})")
            return release_name

        except Exception as e:
            self.logger.error(f"Failed to create release from snapshot {snapshot_name} ({type(e).__name__}: {e})")
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
                                self.logger.info(f"Release {release_name} completed successfully")
                                return True
                            elif cond_status == "False" and reason == "Failed":
                                message = condition.get("message", "No details")
                                self.logger.error(f"Release {release_name} failed: {message}")
                                for cond in conditions:
                                    if cond.get("type") == "ManagedPipelineProcessed" and cond.get("status") == "False":
                                        pipeline_msg = cond.get("message", "")
                                        if pipeline_msg:
                                            self.logger.error(f"Pipeline failure: {pipeline_msg}")
                                return False
                            elif cond_status == "False" and reason == "Progressing":
                                if elapsed % 60 == 0:
                                    self.logger.info(
                                        f"Release {release_name} still progressing ({elapsed // 60} min elapsed)"
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
            self.logger.error(f"Failed to monitor release {release_name}: {type(e).__name__}: {e}")
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
                    self.logger.info(f"Snapshot {snapshot_name} is available")
                    return True
                except exceptions.NotFoundError:
                    self.logger.info(f"Waiting for snapshot {snapshot_name} ({elapsed}s elapsed)")
                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval

            self.logger.error(f"Snapshot {snapshot_name} not available after {timeout_minutes} minutes")
            return False

        except Exception as e:
            self.logger.error(f"Failed waiting for snapshot {snapshot_name}: {type(e).__name__}: {e}")
            return False
