"""Pre-validate layered-product FBC production releases.

This command is a fail-fast preflight rather than a lock. Two pipelines that
validate before either exposes active GitLab or Konflux state can still race.
"""

import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Iterator, TextIO

import click
import psutil
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
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.backend.konflux_fbc import PRODUCTION_INDEX_PULLSPEC_FORMAT
from doozerlib.opm import OpmRegistryAuth, gather_opm
from tenacity import retry, stop_after_attempt, wait_fixed

from elliottlib.cli.common import click_coroutine
from elliottlib.cli.konflux_release_cli import konflux_release_cli
from elliottlib.shipment_model import ShipmentConfig
from elliottlib.shipment_utils import get_shipment_config_records, inspect_shipment_mr_ci_state

LOGGER = logging.getLogger(__name__)
YAML = new_roundtrip_yaml_handler()
_ACTIVE_RELEASE_REASONS = frozenset({'Unknown', 'Not Found', 'Progressing'})
_MEMORY_BYTES_PER_GIB = 1024**3


@dataclass(frozen=True)
class _CatalogRenderStats:
    """Resource and object counts for one streamed catalog render."""

    objects_read: int
    objects_retained: int
    channels: int
    entries: int
    render_seconds: float
    filter_seconds: float
    total_seconds: float


@dataclass(frozen=True)
class _RenderedCatalog:
    """Compact catalog projection and its render statistics."""

    blobs: list[dict]
    stats: _CatalogRenderStats


@dataclass(frozen=True)
class _FragmentValidation:
    """One outgoing fragment and the production index it targets."""

    config_path: str
    fragment_pullspec: str
    production_index: str
    rendered: _RenderedCatalog


def _format_memory_bytes(value: int) -> str:
    """Format a byte count for human-readable progress logs.

    Args:
        value: Number of bytes.

    Returns:
        Byte count formatted in GiB.
    """
    return f'{value / _MEMORY_BYTES_PER_GIB:.2f} GiB'


def _read_memory_value(path: Path) -> int | None:
    """Read a numeric cgroup memory value when the file is available.

    Args:
        path: Cgroup memory file to read.

    Returns:
        Parsed byte count, or ``None`` when the file is absent, unlimited, or invalid.
    """
    try:
        value = path.read_text(encoding='utf-8').strip()
    except (OSError, UnicodeDecodeError):
        return None
    if value == 'max':
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed < 1 << 60 else None


def _memory_status() -> str:
    """Describe memory available to the validator process.

    Returns:
        Cgroup current, limit, and available memory when available, followed by
        the validator process RSS. Falls back to host-level available memory
        when no cgroup limit is visible. Returns ``unavailable`` when memory
        information cannot be read; diagnostics must never stop validation.
    """
    try:
        return _memory_status_from_system()
    except Exception as exc:
        LOGGER.debug('Unable to determine validator memory status: %s', exc)
        return 'unavailable'


def _memory_status_from_system() -> str:
    """Read cgroup or host memory information for diagnostic logging.

    Returns:
        Formatted memory information.

    Raises:
        Exception: If the operating system or ``psutil`` cannot provide memory
            information. The public ``_memory_status`` wrapper handles this
            because diagnostics must remain best effort.
    """
    cgroup_paths = (
        (Path('/sys/fs/cgroup/memory.current'), Path('/sys/fs/cgroup/memory.max')),
        (
            Path('/sys/fs/cgroup/memory/memory.usage_in_bytes'),
            Path('/sys/fs/cgroup/memory/memory.limit_in_bytes'),
        ),
    )
    cgroup_memory = next(
        (
            (current, limit)
            for current_path, limit_path in cgroup_paths
            if (current := _read_memory_value(current_path)) is not None
            and (limit := _read_memory_value(limit_path)) is not None
        ),
        None,
    )
    process_rss = psutil.Process().memory_info().rss
    if cgroup_memory is not None:
        current, limit = cgroup_memory
        available = max(limit - current, 0)
        return (
            f'cgroup_current={_format_memory_bytes(current)} '
            f'cgroup_limit={_format_memory_bytes(limit)} '
            f'cgroup_available={_format_memory_bytes(available)} '
            f'process_rss={_format_memory_bytes(process_rss)}'
        )
    host_memory = psutil.virtual_memory()
    return (
        f'host_available={_format_memory_bytes(host_memory.available)} '
        f'host_total={_format_memory_bytes(host_memory.total)} '
        f'process_rss={_format_memory_bytes(process_rss)}'
    )


def _iter_json_objects(stream: TextIO) -> Iterator[dict]:
    """Yield whitespace-delimited JSON objects from a text stream.

    Args:
        stream: Text stream containing the JSON object stream emitted by ``opm render``.

    Yields:
        Parsed JSON objects.

    Raises:
        ValueError: If the stream contains invalid JSON or a non-object value.
    """
    decoder = json.JSONDecoder()
    buffer = ''
    eof = False
    while True:
        chunk = stream.read(1024 * 1024)
        if chunk:
            buffer += chunk
        else:
            eof = True

        position = 0
        while True:
            while position < len(buffer) and buffer[position].isspace():
                position += 1
            if position >= len(buffer):
                buffer = ''
                break
            try:
                value, end = decoder.raw_decode(buffer, position)
            except json.JSONDecodeError as exc:
                if eof:
                    raise ValueError('Invalid JSON in opm render output') from exc
                buffer = buffer[position:]
                break
            if not isinstance(value, dict):
                raise ValueError(f'Expected JSON object in opm render output, found {type(value).__name__}')
            yield value
            position = end

        if eof:
            if buffer.strip():
                raise ValueError('Trailing data in opm render output')
            return


def _compact_catalog_blob(blob: dict, packages: set[str] | None) -> dict | None:
    """Keep only ownership and channel data needed by LP production validation.

    Args:
        blob: Parsed declarative-config object.
        packages: Package names to retain, or ``None`` to retain all packages.

    Returns:
        A compact blob compatible with ``_catalog_channels``, or ``None`` when
        the blob is unrelated to the requested packages.
    """
    package = _catalog_package(blob)
    if not package or (packages is not None and package not in packages):
        return None

    schema = blob.get('schema')
    compact = {'schema': schema}
    if schema == 'olm.package':
        compact['name'] = blob.get('name')
    else:
        compact['package'] = package
        if blob.get('name') is not None:
            compact['name'] = blob['name']
    if schema == 'olm.channel':
        compact['entries'] = blob.get('entries')
    return compact


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
    if schema in {'olm.channel', 'olm.bundle', 'olm.deprecations'}:
        return blob.get('package')
    return blob.get('package') or blob.get('name')


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


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
async def _render_catalog(input: str, packages: set[str] | None, auth: OpmRegistryAuth) -> _RenderedCatalog:
    """Run the LP-specific optimized ``opm render`` path and retain ownership data.

    This path is intentionally separate from the shared ``doozerlib.opm.render``
    helper. It is specially optimized for performance and low resource
    consumption in constrained CI environments. LP validation renders public
    production catalogs and usually needs only the package ownership and
    channel entries for the outgoing fragments, so JSON output is spooled to
    disk, parsed incrementally, and compacted.

    Args:
        input: Catalog image or FBC directory to render.
        packages: Package names to retain, or ``None`` for all packages.
        auth: Registry authentication used by ``opm``.

    Returns:
        Compact catalog data and render statistics.

    Raises:
        IOError: If ``opm render`` exits unsuccessfully.
        ValueError: If the rendered output is invalid or contains no matching objects.
    """
    started = time.perf_counter()
    with TemporaryDirectory(prefix='_elliott_lp_prod_') as temp_dir:
        output_path = Path(temp_dir, 'catalog.json')
        env = os.environ.copy()
        env.setdefault('GOGC', '20')
        render_started = time.perf_counter()
        with output_path.open('w', encoding='utf-8') as output:
            rc, _, err = await gather_opm(
                ['render', '--migrate-level', 'none', '-o', 'json', '--', input],
                auth=auth,
                check=False,
                env=env,
                stdout=output,
            )
        render_seconds = time.perf_counter() - render_started
        if rc != 0:
            raise IOError(f'opm render failed with exit code {rc}: {err}')

        filter_started = time.perf_counter()
        objects_read = 0
        compact_blobs = []
        with output_path.open(encoding='utf-8') as rendered_output:
            for blob in _iter_json_objects(rendered_output):
                objects_read += 1
                compact = _compact_catalog_blob(blob, packages)
                if compact is not None:
                    compact_blobs.append(compact)
        filter_seconds = time.perf_counter() - filter_started

    if not compact_blobs and (packages is None or objects_read == 0):
        package_description = sorted(packages) if packages is not None else 'any package'
        raise ValueError(f'opm render returned no objects for {input} and packages {package_description}')

    channels = [blob for blob in compact_blobs if blob.get('schema') == 'olm.channel']
    entry_count = sum(len(blob.get('entries') or []) for blob in channels if isinstance(blob.get('entries'), list))
    stats = _CatalogRenderStats(
        objects_read=objects_read,
        objects_retained=len(compact_blobs),
        channels=len(channels),
        entries=entry_count,
        render_seconds=render_seconds,
        filter_seconds=filter_seconds,
        total_seconds=time.perf_counter() - started,
    )
    return _RenderedCatalog(blobs=compact_blobs, stats=stats)


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
        product_aliases = get_product_config(product).aliases
        project_path, current_iid = gitlab_client._parse_mr_url(self.mr_url)
        project = gitlab_client.get_project(project_path)
        source_projects = {project.id: project}
        for mr_summary in gitlab_client.list_merge_requests(
            project_path, state='opened', project=project, target_branch='main'
        ):
            mr_url = mr_summary.web_url
            mr_iid = mr_summary.iid
            if str(mr_iid) == current_iid:
                continue
            mr = project.mergerequests.get(mr_iid)
            source_project_id = mr.source_project_id
            if source_project_id not in source_projects:
                source_projects[source_project_id] = gitlab_client.get_project(source_project_id)
            records = get_shipment_config_records(
                mr,
                source_projects[source_project_id],
                kinds=None,
                product=product,
                product_aliases=product_aliases,
                environment='prod',
            )
            if not records:
                continue
            state = inspect_shipment_mr_ci_state(gitlab_client, mr_url, mr, project=project)
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

    async def _log_runtime_diagnostics(self) -> None:
        """Log the ``opm`` version and available memory before catalog rendering.

        Version lookup is diagnostic only. If it fails, validation continues so
        the existing render failure remains the authoritative result.
        """
        memory_before = _memory_status()
        try:
            rc, stdout, stderr = await gather_opm(['version'], auth=OpmRegistryAuth(path=self.pull_secret), check=False)
        except Exception as exc:
            LOGGER.warning(
                'validate-lp-prod could not determine opm version: %s; memory_before_render=%s',
                exc,
                memory_before,
            )
            return
        version = ' '.join((stdout or '').split()) or 'unknown'
        if rc == 0:
            LOGGER.info('validate-lp-prod opm=%s memory_before_render=%s', version, memory_before)
        else:
            LOGGER.warning(
                'validate-lp-prod opm version command failed rc=%s stderr=%s memory_before_render=%s',
                rc,
                ' '.join((stderr or '').split()),
                memory_before,
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
        fragment_validations: list[_FragmentValidation] = []
        index_packages: defaultdict[str, set[str]] = defaultdict(set)
        index_versions: dict[str, str] = {}
        fragment_number = 0
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
            production_index = PRODUCTION_INDEX_PULLSPEC_FORMAT.format(major=major, minor=minor)
            index_versions[production_index] = ocp_version
            for component in shipment.snapshot.spec.components:
                fragment_pullspec = component.containerImage
                fragment_number += 1
                fragment_started = time.perf_counter()
                LOGGER.info(
                    'validate-lp-prod rendering fragment %d: %s (target_ocp=%s production_index=%s memory=%s)',
                    fragment_number,
                    fragment_pullspec,
                    ocp_version,
                    production_index,
                    _memory_status(),
                )
                rendered_fragment = await _render_catalog(fragment_pullspec, None, auth)
                fragment_packages, _ = _catalog_channels(rendered_fragment.blobs)
                if not fragment_packages:
                    raise ValueError(f'Outgoing FBC fragment contains no identifiable package: {fragment_pullspec}')
                LOGGER.info(
                    'validate-lp-prod finished fragment %d: %s packages=%s elapsed=%.2fs memory=%s',
                    fragment_number,
                    fragment_pullspec,
                    sorted(fragment_packages),
                    time.perf_counter() - fragment_started,
                    _memory_status(),
                )
                fragment_validations.append(
                    _FragmentValidation(
                        config_path=config_path,
                        fragment_pullspec=fragment_pullspec,
                        production_index=production_index,
                        rendered=rendered_fragment,
                    )
                )
                index_packages[production_index].update(fragment_packages)

        total_indexes = len(index_packages)
        for index_number, (production_index, packages) in enumerate(index_packages.items(), 1):
            index_started = time.perf_counter()
            LOGGER.info(
                'validate-lp-prod starting index %d/%d: ocp=%s production_index=%s packages=%s memory=%s',
                index_number,
                total_indexes,
                index_versions[production_index],
                production_index,
                sorted(packages),
                _memory_status(),
            )
            rendered_production = await _render_catalog(production_index, packages, auth)
            comparisons = [
                fragment for fragment in fragment_validations if fragment.production_index == production_index
            ]
            comparison_started = time.perf_counter()
            failures = []
            for fragment in comparisons:
                pruned = find_pruned_entries(rendered_production.blobs, fragment.rendered.blobs)
                if not pruned:
                    continue
                details = '; '.join(
                    f"fragment={fragment.config_path} ({fragment.fragment_pullspec}), "
                    f"package={package}, channel={channel}, removed={sorted(entries)}"
                    for (package, channel), entries in sorted(pruned.items())
                )
                failures.append(details)
            comparison_seconds = time.perf_counter() - comparison_started
            fragment_seconds = sum(fragment.rendered.stats.total_seconds for fragment in comparisons)
            result = 'FAIL' if failures else 'PASS'
            summary = (
                f"validate-lp-prod index={index_number}/{total_indexes} ocp={index_versions[production_index]} "
                f"production_index={production_index} "
                f"packages={sorted(packages)} "
                f"fragments={len(comparisons)} objects_read={rendered_production.stats.objects_read} "
                f"objects_retained={rendered_production.stats.objects_retained} "
                f"channels={rendered_production.stats.channels} entries={rendered_production.stats.entries} "
                f"fragment_time={fragment_seconds:.2f}s production_render={rendered_production.stats.render_seconds:.2f}s "
                f"production_filter={rendered_production.stats.filter_seconds:.2f}s "
                f"comparison={comparison_seconds:.2f}s total={time.perf_counter() - index_started:.2f}s "
                f"memory={_memory_status()} "
                f"result={result}"
            )
            if failures:
                LOGGER.info('%s removed_entries=%s', summary, '; '.join(failures))
                raise RuntimeError(
                    f"FBC production validation failed against {production_index}: {'; '.join(failures)}"
                )
            LOGGER.info(summary)

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
        await self._log_runtime_diagnostics()
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
