import os
import sys
from dataclasses import dataclass
from time import perf_counter
from typing import Optional

import click
from artcommonlib import logutil
from artcommonlib.util import (
    get_utc_now_formatted_str,
    new_roundtrip_yaml_handler,
    normalize_k8s_dns_label,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
    validate_k8s_dns_label,
)
from doozerlib import opm
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
from elliottlib.shipment_utils import SKIPPED_RELEASE_PLAN, is_release_plan_skipped

yaml = new_roundtrip_yaml_handler()

LOGGER = logutil.get_logger(__name__)


def _extract_fbc_packages(blobs: list[dict]) -> set[str]:
    """Extract declared package names from a rendered FBC fragment."""
    packages = set()
    for blob in blobs:
        if not isinstance(blob, dict):
            raise ValueError("Invalid catalog entry while extracting FBC packages")
        if blob.get("schema") != "olm.package":
            continue
        name = blob.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("FBC catalog has an invalid olm.package name")
        packages.add(name)
    if not packages:
        raise ValueError("FBC catalog has no declared olm.package entries")
    return packages


async def _render_fbc_packages(
    shipment: Shipment, registry_auth_file: Optional[str]
) -> list[tuple[str, str, set[str]]]:
    """Render each distinct snapshot image once and retain its component context."""
    auth_file = registry_auth_file or os.environ.get("QUAY_AUTH_FILE") or os.environ.get("IMAGE_REPO_PULL_SECRET")
    auth = opm.OpmRegistryAuth(path=auth_file) if auth_file else None
    packages_by_image = {}
    component_packages = []
    for component in shipment.snapshot.spec.components:
        pullspec = component.containerImage
        if pullspec not in packages_by_image:
            start = perf_counter()
            try:
                blobs = await opm.render(pullspec, auth=auth)
                packages_by_image[pullspec] = _extract_fbc_packages(blobs or [])
            except Exception as exc:
                raise ValueError(
                    f"Cannot read FBC packages for component {component.name!r} ({pullspec}): {exc}"
                ) from exc
            LOGGER.info("Rendered FBC component %s in %.2f seconds", component.name, perf_counter() - start)
        component_packages.append((component.name, pullspec, packages_by_image[pullspec]))
    return component_packages


async def validate_snapshot_against_rpa(
    shipment: Shipment,
    env: str,
    kind: str,
    konflux_client: Optional[KonfluxClient] = None,
    registry_auth_file: Optional[str] = None,
) -> None:
    """Check components against the deployed RPA referenced by each configured RP.

    Validate both environments, starting with the requested one, so missing
    production components are caught during stage preparation. An intentionally
    skipped stage is omitted when validating production. All resource lookups
    are read-only and respect the product's kubeconfig and tenant namespace.
    """
    is_fbc = shipment.metadata.fbc or kind == "fbc"
    component_packages = None

    environments = shipment.environments.model_dump()
    if env not in environments:
        raise ValueError(f"Unsupported release env for RPA validation: {env!r}")
    if not shipment.snapshot or not shipment.snapshot.spec.components:
        raise ValueError("Shipment config has no snapshot components to validate")

    if konflux_client is None:
        konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=resolve_konflux_namespace_by_product(shipment.metadata.product),
            config_file=resolve_konflux_kubeconfig_by_product(shipment.metadata.product),
            context=None,
            dry_run=True,
        )

    snapshot_names = {component.name for component in shipment.snapshot.spec.components}
    envs_to_check = [env] + [other for other in environments if other != env]
    for check_env in envs_to_check:
        release_plan = environments[check_env]["releasePlan"]
        if is_release_plan_skipped(release_plan) and check_env == "stage" and check_env != env:
            LOGGER.info("Skipping RPA validation for intentionally skipped %s environment", check_env)
            continue
        if not release_plan.strip() or release_plan.strip().casefold() in ("n/a", "skipped"):
            raise ValueError(f"Cannot validate {check_env}: releasePlan is unresolved or skipped: {release_plan!r}")

        try:
            rp = (await konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, release_plan)).to_dict()
            labels = rp.get("metadata", {}).get("labels", {}) or {}
            rpa_name = labels.get("release.appstudio.openshift.io/releasePlanAdmission")
            target = rp.get("spec", {}).get("target")
            if not isinstance(rpa_name, str) or not rpa_name.strip():
                raise ValueError(f"ReleasePlan {release_plan!r} has no releasePlanAdmission label")
            if not isinstance(target, str) or not target.strip():
                raise ValueError(f"ReleasePlan {release_plan!r} has no spec.target namespace")
            if rp.get("spec", {}).get("application") != shipment.metadata.application:
                raise ValueError(
                    f"ReleasePlan {release_plan!r} does not target application {shipment.metadata.application!r}"
                )
            rpa = (await konflux_client._get(API_VERSION, "ReleasePlanAdmission", rpa_name, namespace=target)).to_dict()
        except Exception as exc:
            raise ValueError(f"Cannot resolve {check_env} RPA from ReleasePlan {release_plan!r}: {exc}") from exc

        LOGGER.info("Validating %s shipment components against %s RPA %s/%s", kind, check_env, target, rpa_name)
        if is_fbc:
            allowed = rpa.get("spec", {}).get("data", {}).get("fbc", {}).get("allowedPackages")
            if (
                not isinstance(allowed, list)
                or not allowed
                or any(not isinstance(p, str) or not p.strip() for p in allowed)
            ):
                raise ValueError(f"{check_env} RPA {target}/{rpa_name} must define a nonempty allowedPackages list")
            if component_packages is None:
                component_packages = await _render_fbc_packages(shipment, registry_auth_file)
            allowed_packages = set(allowed)
            for component, pullspec, packages in component_packages:
                missing = packages - allowed_packages
                if missing:
                    raise ValueError(
                        f"FBC component {component!r} ({pullspec}) has packages not listed in {check_env} RPA "
                        f"{target}/{rpa_name} allowedPackages: {sorted(missing)}"
                    )
            LOGGER.info("All FBC packages validated against %s RPA %s/%s", check_env, target, rpa_name)
            continue

        components = rpa.get("spec", {}).get("data", {}).get("mapping", {}).get("components", [])
        allowed_names = {comp["name"] for comp in components if isinstance(comp, dict) and "name" in comp}
        if not allowed_names:
            raise ValueError(f"RPA {target}/{rpa_name} has no components defined")
        missing = snapshot_names - allowed_names
        if missing:
            raise ValueError(
                f"The following {kind} shipment components are not listed in {check_env} RPA {target}/{rpa_name} "
                f"and would be silently filtered out during release: {sorted(missing)}"
            )
        LOGGER.info(
            "All %d snapshot components of %s shipment validated against RPA %s/%s",
            len(snapshot_names),
            kind,
            target,
            rpa_name,
        )


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
        image_repo_pull_secret: Optional[str],
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

        # Reject skip-stage sentinel before creating a Snapshot (new_release also checks).
        if is_release_plan_skipped(env_config.releasePlan):
            raise RuntimeError(
                f"Cannot create a Konflux release: releasePlan is set to the skip-stage sentinel "
                f"({env_config.releasePlan!r}). Stage was configured as {SKIPPED_RELEASE_PLAN}; no Konflux "
                f"stage release should be created. If this is unexpected, fix config.yaml / the "
                f"shipment file and regenerate CI."
            )

        # Validate snapshot components against RPA before creating anything
        if config.shipment.snapshot:
            LOGGER.info("Validating snapshot components against RPA...")
            await validate_snapshot_against_rpa(
                config.shipment,
                self.release_env,
                self.kind,
                self.konflux_client,
                registry_auth_file=self.image_repo_pull_secret,
            )

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
        timestamp = get_utc_now_formatted_str()
        raw_prefix = f"{self.runtime.product}-{self.release_env}-{self.runtime.assembly}-{self.kind}"
        prefix = normalize_k8s_dns_label(raw_prefix, max_length=63 - len(timestamp) - 1)
        if not prefix:
            raise ValueError(f"Release object name prefix {raw_prefix!r} cannot be normalized to a Kubernetes name")
        return f"{prefix}-{timestamp}"

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
        snapshot = validate_k8s_dns_label(release_config.snapshot, "Release snapshot reference")

        if is_release_plan_skipped(release_plan):
            raise RuntimeError(
                f"Cannot create a Konflux release: releasePlan is set to the skip-stage sentinel "
                f"({release_plan!r}). Stage was configured as {SKIPPED_RELEASE_PLAN}; no Konflux "
                f"stage release should be created. If this is unexpected, fix config.yaml / the "
                f"shipment file and regenerate CI."
            )

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
    type=click.Choice(["image", "metadata", "extras", "microshift-bootc", "fbc"]),
    help='The kind of release',
)
@click.pass_obj
@click_coroutine
async def validate_rpa_cli(runtime: Runtime, config, env, kind):
    """
    Validate that all snapshot components in a shipment config are present in the
    ReleasePlanAdmission (RPA) for the given environment and kind.

    Image components are checked against the RPA component mapping. FBC images
    are rendered once per distinct pullspec and their declared packages are
    checked against allowedPackages. Registry authentication uses QUAY_AUTH_FILE
    or IMAGE_REPO_PULL_SECRET for this standalone command.

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

    await validate_snapshot_against_rpa(shipment_config.shipment, env, kind)
    LOGGER.info("Validation passed: all components are present in the RPA")
