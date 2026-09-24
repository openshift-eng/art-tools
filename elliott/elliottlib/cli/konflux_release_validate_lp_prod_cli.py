"""Pre-validate layered-product FBC production releases.

This command is a fail-fast preflight rather than a lock. Two pipelines that
validate before either exposes active GitLab or Konflux state can still race.
"""

import logging
from pathlib import Path
from typing import Iterable

import click
from artcommonlib.gitlab import GitLabClient
from artcommonlib.product_catalog import get_product_config
from artcommonlib.product_ids import ProductId
from artcommonlib.util import (
    KubeCondition,
    extract_ocp_version_from_fbc_nvr,
    new_roundtrip_yaml_handler,
    normalize_k8s_dns_label,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib import constants
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.opm import OpmRegistryAuth, render

from elliottlib.cli.common import click_coroutine
from elliottlib.cli.konflux_release_cli import konflux_release_cli
from elliottlib.shipment_model import ShipmentConfig
from elliottlib.shipment_utils import get_shipment_config_records_from_mr, inspect_shipment_mr_ci_state

LOGGER = logging.getLogger(__name__)
YAML = new_roundtrip_yaml_handler()
_ACTIVE_RELEASE_REASONS = frozenset({'Unknown', 'Not Found', 'Progressing'})


def _catalog_package(blob: dict) -> str | None:
    """Return the package name represented by an FBC blob.

    Args:
        blob: Parsed declarative-config blob.

    Returns:
        Package name, or ``None`` for an unsupported blob without ownership.
    """
    schema = blob.get('schema')
    if schema == 'olm.package':
        return blob.get('name')
    return blob.get('package')


def _catalog_channels(blobs: Iterable[dict]) -> tuple[set[str], dict[tuple[str, str], set[str]]]:
    """Collect package ownership and channel entries from rendered FBC blobs.

    Args:
        blobs: Parsed declarative-config blobs.

    Returns:
        A package set and a mapping of ``(package, channel)`` to entry names.

    Raises:
        ValueError: If a channel is missing its package or name.
    """
    packages: set[str] = set()
    channels: dict[tuple[str, str], set[str]] = {}
    for blob in blobs:
        package = _catalog_package(blob)
        if package:
            packages.add(package)
        if blob.get('schema') != 'olm.channel':
            continue
        channel = blob.get('name')
        if not package or not channel:
            raise ValueError(f"Invalid olm.channel blob without package/name: {blob!r}")
        entries = blob.get('entries') or []
        if not isinstance(entries, list) or any(
            not isinstance(entry, dict) or not entry.get('name') for entry in entries
        ):
            raise ValueError(f"Invalid entries in olm.channel {package}/{channel}: {entries!r}")
        channels.setdefault((package, channel), set()).update(entry['name'] for entry in entries)
    return packages, channels


def find_pruned_entries(
    production_blobs: Iterable[dict], fragment_blobs: Iterable[dict]
) -> dict[tuple[str, str], set[str]]:
    """Find production channel entries omitted by an outgoing fragment.

    Only packages owned by the outgoing fragment are considered. This keeps
    product fragments isolated while detecting a missing channel as removal of
    all of that channel's existing entries.

    Args:
        production_blobs: Blobs rendered from the current production index.
        fragment_blobs: Blobs rendered from an outgoing FBC fragment.

    Returns:
        Missing entries keyed by ``(package, channel)``.

    Raises:
        ValueError: If the fragment contains no identifiable package.
    """
    production_packages, production_channels = _catalog_channels(production_blobs)
    fragment_packages, fragment_channels = _catalog_channels(fragment_blobs)
    if not fragment_packages:
        raise ValueError("Outgoing FBC fragment contains no identifiable package")

    pruned: dict[tuple[str, str], set[str]] = {}
    for key, existing_entries in production_channels.items():
        package, _ = key
        if package not in fragment_packages or package not in production_packages:
            continue
        missing = existing_entries - fragment_channels.get(key, set())
        if missing:
            pruned[key] = missing
    return pruned


def _release_dict(release) -> dict:
    """Convert a Kubernetes dynamic resource to a plain dictionary.

    Args:
        release: Kubernetes dynamic resource or plain dictionary.

    Returns:
        Plain Release resource dictionary.
    """
    return release.to_dict() if hasattr(release, 'to_dict') else release


class ValidateLpProdCli:
    """Validate one FBC-bearing layered-product production change set."""

    def __init__(
        self,
        configs: tuple[str, ...],
        mr_url: str,
        pull_secret: str | None,
        konflux_kubeconfig: str | None = None,
        konflux_context: str | None = None,
        konflux_namespace: str | None = None,
    ):
        """Initialize the validation command.

        Args:
            configs: Local shipment configuration paths for one product.
            mr_url: Current ocp-shipment-data merge request URL.
            pull_secret: Registry authentication file used by ``opm render``.
            konflux_kubeconfig: Optional Konflux kubeconfig override.
            konflux_context: Optional kubeconfig context.
            konflux_namespace: Optional Konflux namespace override.
        """
        self.config_paths = configs
        self.mr_url = mr_url
        self.pull_secret = pull_secret
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace

    def _load_configs(self) -> tuple[str, list[ShipmentConfig]]:
        """Load configs and return their canonical product.

        Returns:
            Canonical product name and validated shipment configurations.

        Raises:
            ValueError: If configs are empty, span products, or contain no FBC.
        """
        if not self.config_paths:
            raise ValueError("At least one --config is required")
        configs = []
        canonical_products = set()
        for config_path in self.config_paths:
            with Path(config_path).open(encoding='utf-8') as stream:
                config = ShipmentConfig.model_validate(YAML.load(stream))
            configs.append(config)
            canonical_products.add(get_product_config(config.shipment.metadata.product).product_name)
        if len(canonical_products) != 1:
            raise ValueError(
                f"validate-lp-prod requires one product per invocation; found {sorted(canonical_products)}"
            )
        if not any(config.shipment.metadata.fbc for config in configs):
            raise ValueError("validate-lp-prod requires at least one FBC shipment config")
        return canonical_products.pop(), configs

    def _validate_gitlab_concurrency(self, product: str) -> None:
        """Fail if another same-product MR has active production work.

        Args:
            product: Canonical layered-product name.

        Raises:
            RuntimeError: If another matching MR has active production work or
                GitLab state cannot be classified safely.
        """
        gitlab_client = GitLabClient.from_url(self.mr_url)
        project_path, current_iid = gitlab_client._parse_mr_url(self.mr_url)
        for mr_summary in gitlab_client.list_merge_requests(project_path, state='opened', target_branch='main'):
            mr_url = mr_summary.web_url
            _, mr_iid = gitlab_client._parse_mr_url(mr_url)
            if mr_iid == current_iid:
                continue
            records = get_shipment_config_records_from_mr(mr_url, kinds=None, product=product, environment='prod')
            if not records:
                continue
            mr = gitlab_client.get_mr_from_url(mr_url)
            state = inspect_shipment_mr_ci_state(gitlab_client, mr_url, mr)
            if state.active_prod:
                raise RuntimeError(
                    f"Another production release for layered product {product!r} is active in {mr_url}: "
                    f"{'; '.join(state.active_prod)}. Retry after it finishes."
                )

    def _new_konflux_client(self, product: str) -> KonfluxClient:
        """Create the product-scoped Konflux client used for validation.

        Args:
            product: Canonical layered-product name.

        Returns:
            Connected client for the product's Konflux namespace.
        """
        kubeconfig = resolve_konflux_kubeconfig_by_product(product, self.konflux_kubeconfig)
        namespace = resolve_konflux_namespace_by_product(product, self.konflux_namespace)
        client = KonfluxClient.from_kubeconfig(
            default_namespace=namespace,
            config_file=kubeconfig,
            context=self.konflux_context,
            dry_run=False,
        )
        client.verify_connection()
        return client

    async def _validate_konflux_concurrency(self, product: str, client: KonfluxClient) -> None:
        """Fail if a same-product Konflux production Release is active.

        Args:
            product: Canonical layered-product name.
            client: Connected product-scoped Konflux client.

        Raises:
            RuntimeError: If an active matching Release is found or its
                environment metadata is contradictory.
        """
        product_config = get_product_config(product)
        product_names = (product_config.product_name, *product_config.aliases)
        prefixes = tuple(f"{normalize_k8s_dns_label(name)}-prod-" for name in product_names)
        for release in await client.list_releases():
            release_data = _release_dict(release)
            metadata = release_data.get('metadata', {})
            name = metadata.get('name', '')
            if not name.startswith(prefixes):
                continue
            annotations = metadata.get('annotations', {})
            release_env = annotations.get('art.redhat.com/env')
            if release_env not in (None, 'prod'):
                raise RuntimeError(
                    f"Cannot safely classify Konflux Release {name!r}: name indicates prod but "
                    f"art.redhat.com/env is {release_env!r}"
                )
            released = KubeCondition.find_condition(release_data, 'Released')
            if released is not None and released.reason and released.reason not in _ACTIVE_RELEASE_REASONS:
                continue
            release_url = client.resource_url(release_data)
            job_url = annotations.get('art.redhat.com/job-url')
            origin = f" (created by {job_url})" if job_url else ""
            raise RuntimeError(
                f"Another production Release for layered product {product!r} is active: {release_url}{origin}. "
                "Retry after it finishes."
            )

    async def _validate_fbc_fragments(self, configs: list[ShipmentConfig]) -> None:
        """Fail if any outgoing FBC fragment prunes a production entry.

        Args:
            configs: Shipment configurations from this product's change set.

        Raises:
            ValueError: If FBC shipment or rendered catalog data is malformed.
            RuntimeError: If an outgoing fragment omits production entries.
            IOError: If a production index or fragment cannot be rendered.
        """
        auth = OpmRegistryAuth(path=self.pull_secret)
        production_cache: dict[str, list[dict]] = {}
        for config, config_path in zip(configs, self.config_paths):
            shipment = config.shipment
            if not shipment.metadata.fbc:
                continue
            if not shipment.snapshot:
                raise ValueError(f"FBC shipment {config_path} has no snapshot")
            if len(shipment.snapshot.nvrs) != 1:
                raise ValueError(f"Expected one NVR in FBC shipment {config_path}, found {len(shipment.snapshot.nvrs)}")
            ocp_version = extract_ocp_version_from_fbc_nvr(shipment.snapshot.nvrs[0])
            if not ocp_version:
                raise ValueError(f"Cannot determine target OCP version from FBC NVR in {config_path}")
            major, minor = ocp_version.split('.', 1)
            production_index = constants.PRODUCTION_INDEX_PULLSPEC_FORMAT.format(major=major, minor=minor)
            if production_index not in production_cache:
                production_cache[production_index] = await render(production_index, auth=auth)
            production_blobs = production_cache[production_index]

            for component in shipment.snapshot.spec.components:
                fragment_pullspec = component.containerImage
                fragment_blobs = await render(fragment_pullspec, auth=auth)
                pruned = find_pruned_entries(production_blobs, fragment_blobs)
                if not pruned:
                    continue
                details = '; '.join(
                    f"package={package}, channel={channel}, removed={sorted(entries)}"
                    for (package, channel), entries in sorted(pruned.items())
                )
                raise RuntimeError(
                    f"FBC production validation failed for {config_path} ({fragment_pullspec}) against "
                    f"{production_index}: {details}"
                )

    async def run(self) -> None:
        """Run all layered-product production validations.

        OCP is a defensive no-op. The generated shipment CI does not invoke
        this command for OCP, stage, or non-FBC changes.

        Raises:
            ValueError: If shipment configuration is invalid.
            RuntimeError: If concurrent production work or pruning is found.
            IOError: If registry catalog data cannot be rendered.
        """
        product, configs = self._load_configs()
        if get_product_config(product).product_id is ProductId.OCP:
            LOGGER.info("Skipping validate-lp-prod for OCP")
            return
        self._validate_gitlab_concurrency(product)
        konflux_client = self._new_konflux_client(product)
        await self._validate_konflux_concurrency(product, konflux_client)
        await self._validate_fbc_fragments(configs)
        LOGGER.info("Layered-product production validation passed for %s", product)


@konflux_release_cli.command(
    "validate-lp-prod",
    short_help="Validate an FBC-bearing layered-product production release",
)
@click.option('--config', 'configs', metavar='PATH', multiple=True, required=True, help='Shipment config path.')
@click.option('--mr-url', required=True, metavar='URL', help='Current ocp-shipment-data merge request URL.')
@click.option('--pull-secret', metavar='PATH', help='Registry authentication file for rendering FBC images.')
@click.option('--konflux-kubeconfig', metavar='PATH', help='Konflux kubeconfig override.')
@click.option('--konflux-context', metavar='CONTEXT', help='Konflux kubeconfig context.')
@click.option('--konflux-namespace', metavar='NAMESPACE', help='Konflux namespace override.')
@click.pass_obj
@click_coroutine
async def validate_lp_prod_cli(
    _runtime,
    configs: tuple[str, ...],
    mr_url: str,
    pull_secret: str | None,
    konflux_kubeconfig: str | None,
    konflux_context: str | None,
    konflux_namespace: str | None,
):
    """Validate LP concurrency and FBC preservation before production."""
    validator = ValidateLpProdCli(
        configs=configs,
        mr_url=mr_url,
        pull_secret=pull_secret,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
    )
    try:
        await validator.run()
    except Exception as exc:
        raise click.ClickException(f"validate-lp-prod failed: {exc}") from exc
