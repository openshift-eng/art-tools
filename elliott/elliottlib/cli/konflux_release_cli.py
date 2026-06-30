import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Set

import aiohttp
import click
from artcommonlib import logutil
from artcommonlib.constants import KONFLUX_RELEASE_DATA_RPA_BASE_URL
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
from kubernetes.dynamic import ResourceInstance, exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import Shipment, ShipmentConfig, ShipmentEnv

yaml = new_roundtrip_yaml_handler()

LOGGER = logutil.get_logger(__name__)


async def fetch_rpa(rpa_name: str) -> dict:
    url = f"{KONFLUX_RELEASE_DATA_RPA_BASE_URL}/{rpa_name}.yaml"
    headers = {}
    gitlab_token = os.environ.get("GITLAB_TOKEN")
    if gitlab_token:
        headers["Private-Token"] = gitlab_token

    timeout = aiohttp.ClientTimeout(total=30, sock_read=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                error_msg = f"Failed to fetch RPA from {url}: HTTP {response.status}"
                if response.status == 403:
                    error_msg += ". 403 Forbidden may indicate missing GITLAB_TOKEN environment variable."
                raise ValueError(error_msg)
            content = await response.text()

    rpa_data = yaml.load(content)
    if not rpa_data:
        raise ValueError(f"RPA file {rpa_name}.yaml is empty or invalid")
    return rpa_data


SUPPORTED_RPA_KINDS = {
    "image": "ocp-art-advisory",
    "metadata": "ocp-art-advisory",
    "extras": "ocp-art-advisory",
    "fbc": "ocp-art-fbc",
}


async def validate_snapshot_against_rpa(group: str, env: str, kind: str, snapshot_components: List[str]) -> None:
    match = re.fullmatch(r"openshift-(\d+)\.(\d+)", group)
    if not match:
        return
    major, minor = match.group(1), match.group(2)

    if kind not in SUPPORTED_RPA_KINDS:
        raise ValueError(
            f"Unsupported release kind for RPA validation: {kind!r}. Supported: {sorted(SUPPORTED_RPA_KINDS)}"
        )
    rpa_name = f"{SUPPORTED_RPA_KINDS[kind]}-{env}-{major}-{minor}"

    LOGGER.info("Fetching RPA %s to validate snapshot components...", rpa_name)
    rpa_data = await fetch_rpa(rpa_name)

    allowed_names: Set[str] = set()
    if kind == "fbc":
        allowed_packages = rpa_data.get("spec", {}).get("data", {}).get("fbc", {}).get("allowedPackages", [])
        allowed_names = set(allowed_packages)
    else:
        components = rpa_data.get("spec", {}).get("data", {}).get("mapping", {}).get("components", [])
        for comp in components:
            if isinstance(comp, dict) and "name" in comp:
                allowed_names.add(comp["name"])

    if not allowed_names:
        raise ValueError(f"RPA {rpa_name} has no components/allowedPackages defined")

    snapshot_names = set(snapshot_components)
    missing = snapshot_names - allowed_names
    if missing:
        raise ValueError(
            f"The following snapshot components are not listed in RPA {rpa_name} "
            f"and would be silently filtered out during release: {sorted(missing)}"
        )

    LOGGER.info("All %d snapshot components validated against RPA %s", len(snapshot_names), rpa_name)


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

        # Validate snapshot components against RPA before creating anything
        if self.runtime.group.startswith("openshift-") and config.shipment.snapshot:
            LOGGER.info("Validating snapshot components against RPA...")
            component_names = [c.name for c in config.shipment.snapshot.spec.components]
            await validate_snapshot_against_rpa(self.runtime.group, self.release_env, self.kind, component_names)

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


@konflux_release_cli.command(
    "validate-rpa",
    short_help="Validate that snapshot components are listed in the ReleasePlanAdmission",
)
@click.option(
    '--config',
    metavar='CONFIG_PATH',
    required=True,
    help='Path of the shipment config file. The path should be from the root of the given shipment-data repo.',
)
@click.option(
    '--env',
    metavar='RELEASE_ENV',
    required=True,
    type=click.Choice(["stage", "prod"]),
    help='Release environment to validate against',
)
@click.option(
    '--kind',
    metavar='KIND',
    required=True,
    type=click.Choice(["image", "metadata", "extras", "fbc"]),
    help='The kind of release',
)
@click.pass_obj
@click_coroutine
async def validate_rpa_cli(runtime: Runtime, config, env, kind):
    """
    Validate that all snapshot components in a shipment config are present in the
    ReleasePlanAdmission (RPA) for the given environment and kind.

    Components not listed in the RPA are silently filtered out during release,
    which can cause builds to be missed without any error.

    \b
    $ elliott -g openshift-4.18 --assembly 4.18.2 --shipment-path \\
        "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data@branch" \\
        release validate-rpa --config shipment/ocp/openshift-4.18/openshift-4-18/4.18.2.yml \\
        --env prod --kind image
    """
    runtime.initialize(build_system='konflux', with_shipment=True)

    LOGGER.info("Loading %s...", config)
    config_raw = runtime.shipment_gitdata.load_yaml_file(config)
    if not config_raw:
        raise ValueError(f"Error loading shipment config from {runtime.shipment_gitdata.data_path}/{config}")

    shipment_config = ShipmentConfig.model_validate(config_raw)
    if not (shipment_config.shipment.snapshot and shipment_config.shipment.snapshot.spec.components):
        raise ValueError("Shipment config has no snapshot components to validate")

    component_names = [c.name for c in shipment_config.shipment.snapshot.spec.components]
    LOGGER.info("Validating %d components against RPA for %s/%s...", len(component_names), env, kind)

    await validate_snapshot_against_rpa(runtime.group, env, kind, component_names)
    LOGGER.info("Validation passed: all components are present in the RPA")
