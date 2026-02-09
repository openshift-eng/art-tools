import os
import sys
from dataclasses import dataclass
from typing import Optional

import click
from artcommonlib import logutil
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.format_util import green_print, red_print
from artcommonlib.util import (
    get_utc_now_formatted_str,
    new_roundtrip_yaml_handler,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib.backend.konflux_client import (
    API_VERSION,
    KIND_APPLICATION,
    KIND_RELEASE,
    KIND_RELEASE_PLAN,
    KIND_SNAPSHOT,
    KonfluxClient,
)
from doozerlib.constants import KONFLUX_UI_HOST
from kubernetes.dynamic import ResourceInstance, exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import Shipment, ShipmentConfig, ShipmentEnv

yaml = new_roundtrip_yaml_handler()

LOGGER = logutil.get_logger(__name__)


@dataclass
class ReleaseConfig:
    snapshot: str
    release_plan: str
    application: str
    release_name: str
    data: str = None


class CreateReleaseCli:
    def __init__(
        self,
        runtime: Runtime,
        config_path: str,
        release_env: str,
        konflux_config: dict,
        image_repo_pull_secret: dict,
        dry_run: bool,
        kind: str,
        job_url: str = None,
        force: bool = False,
    ):
        self.runtime = runtime
        self.config_path = config_path
        self.konflux_config = konflux_config
        self.image_repo_pull_secret = image_repo_pull_secret
        self.dry_run = dry_run
        self.job_url = job_url
        self.konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_config['namespace'],
            config_file=self.konflux_config['kubeconfig'],
            context=self.konflux_config['context'],
            dry_run=self.dry_run,
        )
        self.konflux_client.verify_connection()
        self.release_env = release_env
        self.force = force
        self.kind = kind

    @staticmethod
    def _validate_release_notes_for_openshift(group: str, release_notes):
        """Validate that release notes fields are non-empty for openshift- groups."""
        if not group.startswith("openshift-"):
            return

        if not (release_notes and hasattr(release_notes, '__dict__')):
            return

        required_fields = ['synopsis', 'topic', 'description', 'solution']
        empty_fields = [field for field in required_fields if not (getattr(release_notes, field, None) or '').strip()]

        if empty_fields:
            raise ValueError(
                f"For openshift- groups, the following releaseNotes fields cannot be empty: {', '.join(empty_fields)}"
            )

    async def run(self) -> Optional[ResourceInstance]:
        # Initialize runtime if not already initialized (for direct class usage in tests)
        if not getattr(self.runtime, 'initialized', False):
            self.runtime.initialize(build_system='konflux', with_shipment=True)

        LOGGER.info(f"Loading {self.config_path}...")
        config_raw = self.runtime.shipment_gitdata.load_yaml_file(self.config_path)
        if not config_raw:
            raise ValueError(
                f"Error loading shipment config from {self.runtime.shipment_gitdata.data_path}/{self.config_path}"
            )

        LOGGER.info("Validating yaml config...")
        config = ShipmentConfig.model_validate(config_raw)

        # Validate shipment metadata with group metadata and with what's passed via args
        meta = config.shipment.metadata
        if meta.product != self.runtime.product:
            raise ValueError(
                f"shipment.metadata.product={meta.product} is expected to be the "
                f"same as runtime.product={self.runtime.product}"
            )
        elif meta.group != self.runtime.group:
            raise ValueError(
                f"shipment.metadata.group={meta.group} is expected to be the same as runtime.group={self.runtime.group}"
            )
        elif meta.assembly != self.runtime.assembly:
            raise ValueError(
                f"shipment.metadata.assembly={meta.assembly} is expected to be the "
                f"same as runtime.assembly={self.runtime.assembly}"
            )

        # Validate release notes for openshift- groups
        if config.shipment.data and config.shipment.data.releaseNotes:
            self._validate_release_notes_for_openshift(self.runtime.group, config.shipment.data.releaseNotes)

        # Ensure CRDs are accessible
        try:
            await self.konflux_client._get_api(API_VERSION, KIND_APPLICATION)
            await self.konflux_client._get_api(API_VERSION, KIND_SNAPSHOT)
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE)
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE_PLAN)
        except exceptions.ResourceNotFoundError:
            raise RuntimeError("Cannot access release resources in the cluster. Passed the right kubeconfig?")

        # make sure application exists
        LOGGER.info(f"Fetching application {meta.application} ...")
        try:
            await self.konflux_client._get(API_VERSION, KIND_APPLICATION, meta.application)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {meta.application} in the cluster. Does it exist?")

        release_name = self.get_object_name()

        env_config: ShipmentEnv = getattr(config.shipment.environments, self.release_env)
        if env_config.shipped() and not self.force:
            LOGGER.warning(
                f"existing release metadata is not empty for {self.release_env}: "
                f"{env_config.model_dump()}. If you want to proceed, either remove the release metadata from the shipment config or use the --force flag."
            )
            return None

        # Create snapshot first using the spec from shipment config
        LOGGER.info("Creating snapshot from shipment config...")
        snapshot_obj = await self.create_snapshot(config.shipment)
        created_snapshot = await self.konflux_client._create(snapshot_obj)
        snapshot_name = created_snapshot.metadata.name
        snapshot_url = self.konflux_client.resource_url(created_snapshot)
        if self.dry_run:
            LOGGER.info("[DRY-RUN] Would have created Konflux Snapshot at %s", snapshot_url)
        else:
            LOGGER.info("Successfully created Snapshot %s", snapshot_url)

        # Create release config using the created snapshot name
        release_config = self.get_release_config(config.shipment, self.release_env, release_name, snapshot_name)
        LOGGER.info(
            f"Constructed release config with snapshot={release_config.snapshot}"
            f" releasePlan={release_config.release_plan}"
        )

        release_obj = await self.new_release(release_config)
        created_release = await self.konflux_client._create(release_obj)
        release_url = self.konflux_client.resource_url(created_release)
        if self.dry_run:
            LOGGER.info("[DRY-RUN] Would have created Konflux Release at %s", release_url)
        else:
            LOGGER.info("Successfully created Release %s", release_url)
        return created_release

    def get_object_name(self) -> str:
        assembly_str = self.runtime.assembly.replace(".", "-")
        return f"{self.runtime.product}-{self.release_env}-{assembly_str}-{self.kind}-{get_utc_now_formatted_str()}"

    async def create_snapshot(self, shipment: Shipment) -> dict:
        """
        Create a Konflux Snapshot manifest from the given shipment's snapshot spec.
        """
        if not (shipment.snapshot and shipment.snapshot.spec.components):
            raise ValueError("A valid snapshot must be provided")

        snapshot_name = self.get_object_name()

        # Prepare metadata with labels and optional annotations
        metadata = {
            "name": snapshot_name,
            "namespace": self.konflux_config['namespace'],
            "labels": {
                "test.appstudio.openshift.io/type": "override",
                "appstudio.openshift.io/application": shipment.metadata.application,
            },
        }

        metadata["annotations"] = self.get_annotations()

        snapshot_obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_SNAPSHOT,
            "metadata": metadata,
            "spec": shipment.snapshot.spec.model_dump(exclude_none=True),
        }
        return snapshot_obj

    @staticmethod
    def get_release_config(shipment: Shipment, env: str, release_name: str, snapshot_name: str) -> ReleaseConfig:
        """
        Construct a konflux release config based on env and raw config
        """
        rc = ReleaseConfig(
            snapshot=snapshot_name,
            release_plan=getattr(shipment.environments, env).releasePlan,
            application=shipment.metadata.application,
            release_name=release_name,
        )

        if shipment.data:
            # We only reserve live_id for prod release, therefore releaseNotes.live_id is only meant for prod
            # For stage release, we want konflux to automatically assign a live_id to the release
            if env != "prod":
                shipment.data.releaseNotes.live_id = None

            # Do not set exclude_unset=True when dumping, since Konflux
            # expects certain keys to always be set, even if empty.
            # Those are appropriately set as default values in pydantic shipment model
            rc.data = shipment.data.model_dump(exclude_none=True)
        return rc

    async def new_release(self, release_config: ReleaseConfig) -> dict:
        release_plan = release_config.release_plan
        snapshot = release_config.snapshot

        # make sure releasePlan exists
        LOGGER.info(f"Fetching release plan {release_plan} ...")
        try:
            await self.konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, release_plan)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {release_plan} in the cluster. Does it exist?")

        # make sure snapshot exists
        LOGGER.info(f"Fetching snapshot {snapshot} ...")
        try:
            await self.konflux_client._get(API_VERSION, KIND_SNAPSHOT, snapshot)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {snapshot} in the cluster. Does it exist?")

        # Prepare metadata with labels and optional annotations
        metadata = {
            "name": release_config.release_name,
            "namespace": self.konflux_config['namespace'],
            "labels": {"appstudio.openshift.io/application": release_config.application},
        }

        metadata["annotations"] = self.get_annotations()

        release_obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_RELEASE,
            "metadata": metadata,
            "spec": {
                "releasePlan": release_plan,
                "snapshot": snapshot,
            },
        }
        if release_config.data:
            release_obj["spec"]["data"] = release_config.data

        return release_obj

    def get_annotations(self) -> dict:
        annotations = {
            "art.redhat.com/assembly": self.runtime.assembly,
            "art.redhat.com/env": self.release_env,
            "art.redhat.com/kind": self.kind,
        }
        if self.job_url:
            annotations["art.redhat.com/job-url"] = self.job_url
        return annotations


async def create_release_from_snapshot(
    runtime: Runtime,
    snapshot_name: str,
    release_plan: str,
    namespace: str = None,
    apply: bool = True,
) -> str:
    """
    Create Konflux release from existing snapshot - library function for programmatic use.

    This function creates a Release resource from an existing snapshot using the specified
    release plan. It's designed to be called programmatically from other components like
    the BaseImageHandler in doozer.

    Note: This function uses 'apply' parameter instead of 'dry_run' to maintain consistency
    with snapshot_cli.py conventions and avoid refactoring the entire codebase. We prioritize
    code sanity by using the same terminology pattern across related functions.

    Args:
        runtime: Elliott runtime instance
        snapshot_name: Name of existing snapshot to create release from
        release_plan: Konflux release plan to use
        namespace: Kubernetes namespace (auto-detected if None)
        apply: Whether to actually create the resource (False = preview only)

    Returns:
        str: Name of the created release

    Raises:
        RuntimeError: If snapshot or release plan doesn't exist
        ValueError: If required parameters are missing
    """
    if not snapshot_name:
        raise ValueError("Snapshot name is required")

    if not release_plan:
        raise ValueError("Release plan name is required")

    if namespace is None:
        namespace = resolve_konflux_namespace_by_product(runtime.product, None)

    kubeconfig = resolve_konflux_kubeconfig_by_product(runtime.product, None)

    konflux_client = KonfluxClient.from_kubeconfig(
        default_namespace=namespace,
        config_file=kubeconfig,
        context=None,
        dry_run=not apply,
    )
    konflux_client.verify_connection()

    LOGGER.info(f"Verifying snapshot {snapshot_name} exists...")
    try:
        await konflux_client._get(API_VERSION, KIND_SNAPSHOT, snapshot_name)
    except exceptions.NotFoundError:
        raise RuntimeError(f"Snapshot {snapshot_name} not found in namespace {namespace}")

    # Generate application name from group (e.g., openshift-4.22 -> openshift-4-22)
    application_name = runtime.group_config.name.replace('.', '-')

    release_resource = {
        "apiVersion": API_VERSION,
        "kind": KIND_RELEASE,
        "metadata": {
            "generateName": "ocp-base-image-release-",
            "namespace": namespace,
            "labels": {
                "appstudio.openshift.io/application": application_name,
            },
            "annotations": {
                "art.redhat.com/kind": "image",
                "art.redhat.com/group": runtime.group_config.name,
                "art.redhat.com/assembly": runtime.assembly,
                "art.redhat.com/env": "base-image-workflow",
            },
        },
        "spec": {"releasePlan": release_plan, "snapshot": snapshot_name},
    }

    if not apply:
        LOGGER.info("Apply=False - would create release resource:")
        LOGGER.info(f"Resource: {release_resource}")
        return f"preview-release-{snapshot_name}"

    try:
        created_release = await konflux_client._create(release_resource)
        release_name = created_release.metadata.name

        LOGGER.info(f"Release URL: {konflux_client.resource_url(created_release)}")

        return release_name

    except Exception as e:
        LOGGER.error(f"Failed to create release from snapshot: {e}")
        raise RuntimeError(f"Release creation failed: {e}")


@cli.group("release", short_help="Commands for managing Konflux Releases")
def konflux_release_cli():
    pass


@konflux_release_cli.command(
    "new", short_help="Create a new Konflux Release in the given namespace for the given shipment-data configuration"
)
@click.option(
    '--konflux-kubeconfig',
    metavar='KUBECONF_PATH',
    help='Path to the kubeconfig file to use for Konflux cluster connections. If not provided, will be auto-detected based on group (e.g., KONFLUX_SA_KUBECONFIG for openshift- groups, OADP_KONFLUX_SA_KUBECONFIG for oadp- groups).',
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    help='The namespace to use for Konflux cluster connections. If not provided, will be auto-detected based on group (e.g., ocp-art-tenant for openshift- groups, art-oadp-tenant for oadp- groups).',
)
@click.option(
    '--pull-secret',
    metavar='PATH',
    help='Path to the pull secret file to use. For example, if the snapshot contains images from quay.io/org/repo then provide the pull secret to read from that repo.',
)
@click.option(
    '--config',
    metavar='CONFIG_PATH',
    required=True,
    help='Path of the shipment config file to use for creating release. The path should be from the root of the  '
    'given shipment-data repo.',
)
@click.option(
    '--env',
    metavar='RELEASE_ENV',
    required=True,
    type=click.Choice(["stage", "prod"]),
    help='Release environment to create the release for',
)
@click.option('--apply', is_flag=True, default=False, help='Create the release in cluster (False by default)')
@click.option(
    '--job-url',
    metavar='URL',
    help='The URL of the job that created this release. This will be added as an annotation to both the snapshot and release objects.',
)
@click.option(
    '--force', is_flag=True, default=False, help='Force the creation of the release even if it already exists'
)
@click.option(
    '--kind',
    metavar='KIND',
    required=True,
    help='The kind of release being created (e.g. "image", "metadata", "fbc" etc)',
)
@click.pass_obj
@click_coroutine
async def new_release_cli(
    runtime: Runtime,
    konflux_kubeconfig,
    konflux_context,
    konflux_namespace,
    pull_secret,
    config,
    env,
    apply,
    job_url,
    force,
    kind,
):
    """
    Create a new Konflux Release in the given namespace based on the config provided
    \b
    $ elliott -g openshift-4.18 --assembly 4.18.2 --shipment-path
    "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data@add_4.18.2_test_release"
    release new --env stage --config shipment/ocp/openshift-4.18/openshift-4-18/4.18.2.202503210000.yml
    """
    # Initialize runtime to populate runtime.product before using resolver functions
    runtime.initialize(build_system='konflux', with_shipment=True)

    # Resolve kubeconfig and namespace using utility functions
    resolved_kubeconfig = resolve_konflux_kubeconfig_by_product(runtime.product, konflux_kubeconfig)
    resolved_namespace = resolve_konflux_namespace_by_product(runtime.product, konflux_namespace)

    konflux_config = {
        'kubeconfig': resolved_kubeconfig,
        'namespace': resolved_namespace,
        'context': konflux_context,
    }

    pipeline = CreateReleaseCli(
        runtime,
        config_path=config,
        release_env=env,
        konflux_config=konflux_config,
        image_repo_pull_secret=pull_secret,
        dry_run=not apply,
        job_url=job_url,
        force=force,
        kind=kind,
    )
    release = await pipeline.run()
    if release:
        yaml.dump(release.to_dict(), sys.stdout)


@konflux_release_cli.command("release-from-snapshot", short_help="Create Konflux release from existing snapshot")
@click.argument("snapshot_name", metavar="SNAPSHOT_NAME")
@click.option("--release-plan", required=True, help="Konflux release plan name to use")
@click.option("--apply", is_flag=True, default=False, help="Apply the release resource immediately")
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    help='The namespace to use for Konflux cluster connections. If not provided, will be auto-detected based on group.',
)
@click.pass_obj
@click_coroutine
async def release_from_snapshot_cli(
    runtime: Runtime, snapshot_name: str, release_plan: str, apply: bool, konflux_namespace: str
):
    """
    Create a Konflux Release resource from an existing snapshot.

    This command creates a Release resource that references an existing snapshot
    and uses the specified release plan to orchestrate the release process.
    The release will generate both quay.io and registry.redhat.io URLs upon completion.

    Examples:

    Create release from snapshot (preview mode):
    elliott -g openshift-4.22 release release-from-snapshot my-snapshot --release-plan ocp-art-base-images-silent-4-22-rhel9

    Apply the release immediately:
    elliott -g openshift-4.22 release release-from-snapshot my-snapshot --release-plan ocp-art-base-images-silent-4-22-rhel9 --apply
    """
    # Initialize runtime for Konflux operations
    runtime.initialize(build_system='konflux')

    try:
        release_name = await create_release_from_snapshot(
            runtime=runtime,
            snapshot_name=snapshot_name,
            release_plan=release_plan,
            namespace=konflux_namespace,
            apply=apply,
        )

        if apply:
            green_print(f"✓ Successfully created release: {release_name}")
            LOGGER.info(
                f"Monitor release progress: kubectl get release {release_name} -n {konflux_namespace or 'ocp-art-tenant'}"
            )
        else:
            LOGGER.info("Preview completed - use --apply to create the release")

    except Exception as e:
        red_print(f"Failed to create release from snapshot: {e}")
        LOGGER.error(f"CLI command failed: {e}")
        exit(1)
