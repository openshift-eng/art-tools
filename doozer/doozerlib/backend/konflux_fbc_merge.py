"""Build one releasable FBC from explicit layered-product minor releases."""

import asyncio
import copy
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from artcommonlib.config import BuildDataLoader
from artcommonlib.gitdata import GitData
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import extract_ocp_version_from_fbc_nvr
from artcommonlib.variants import get_build_variant_for_product
from dockerfile_parse import DockerfileParser
from doozerlib import opm, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_fbc import (
    LIFECYCLE_SCHEMA,
    PRODUCTION_INDEX_PULLSPEC_FORMAT,
    KonfluxFbcFragmentMerger,
    KonfluxFbcRebaser,
    get_target_fbc_arches,
)


@dataclass
class LayeredFbcInput:
    record: KonfluxFbcBuildRecord
    bundle: KonfluxBundleBuildRecord
    package: str
    channels: tuple[str, ...]
    catalog: list[dict]
    group_revision: str
    csv_name: str


def _package(blob: dict) -> str:
    return blob['name'] if blob['schema'] == 'olm.package' else blob.get('package', '')


def _catalog_index(blobs: list[dict], packages: set[str]) -> dict[tuple[str, str, str], dict]:
    indexed = {}
    for blob in blobs:
        package = _package(blob)
        if package not in packages or blob['schema'] == LIFECYCLE_SCHEMA:
            continue
        key = (blob['schema'], package, blob.get('name', package))
        if key in indexed and indexed[key] != blob:
            raise ValueError(f'Conflicting catalog object {key}')
        indexed[key] = copy.deepcopy(blob)
    return indexed


def merge_layered_catalogs(baseline: list[dict], inputs: list[LayeredFbcInput]) -> list[dict]:
    """Overlay only the channels owned by the supplied bundles onto production."""
    packages = {source.package for source in inputs}
    if not packages:
        raise ValueError('A unified FBC requires at least one operator package')
    merged = _catalog_index(baseline, packages)
    owners = {}
    defaults = {package: set() for package in packages}
    for package in packages:
        existing = merged.get(('olm.package', package, package))
        if existing:
            defaults[package].add(existing['defaultChannel'])

    for source in sorted(inputs, key=lambda source: source.record.nvr):
        package = source.package
        catalog = _catalog_index(source.catalog, {package})
        package_key = ('olm.package', package, package)
        if package_key not in catalog:
            raise ValueError(f'{source.record.nvr}: missing package {package}')
        csv = source.csv_name
        if not csv:
            raise ValueError(f'{source.record.nvr}: source bundle {source.bundle.nvr} has no selected CSV name')
        if ('olm.bundle', package, csv) not in catalog:
            raise ValueError(f'{source.record.nvr}: missing bundle {package}/{csv} for selected CSV')
        defaults[package].add(catalog[package_key]['defaultChannel'])
        if package_key not in merged:
            merged[package_key] = catalog[package_key]
        bundle_names = set()
        for channel in source.channels:
            key = ('olm.channel', package, channel)
            if key in owners:
                raise ValueError(
                    f'Channel {package}/{channel} is updated by both {owners[key]} and {source.record.nvr}'
                )
            owners[key] = source.record.nvr
            if key not in catalog:
                raise ValueError(f'{source.record.nvr}: missing channel {package}/{channel}')
            updated = catalog[key]
            entries = {entry['name'] for entry in updated['entries']}
            if csv not in entries:
                raise ValueError(f'{source.record.nvr}: selected CSV {csv} is missing from channel {package}/{channel}')
            published = {entry['name'] for entry in merged.get(key, {}).get('entries', [])}
            if missing := published - entries:
                raise ValueError(f'{source.record.nvr}: stale channel {package}/{channel} removes {sorted(missing)}')
            updated_entries = {entry['name']: entry for entry in updated['entries']}
            for entry in merged.get(key, {}).get('entries', []):
                if updated_entries[entry['name']] != entry:
                    raise ValueError(
                        f'{source.record.nvr}: conflicting published entry {package}/{channel}/{entry["name"]}'
                    )
            merged[key] = updated
            bundle_names.update(entries)
        for name in sorted(bundle_names):
            key = ('olm.bundle', package, name)
            if key not in catalog:
                raise ValueError(f'{source.record.nvr}: missing bundle {package}/{name}')
            if key in merged and merged[key] != catalog[key]:
                raise ValueError(f'{source.record.nvr}: conflicting bundle {package}/{name}')
            merged[key] = catalog[key]

    for package in sorted(packages):
        channels = [key[2] for key in merged if key[:2] == ('olm.channel', package)]
        candidates = {KonfluxFbcRebaser._resolve_default_channel(channels, default) for default in defaults[package]}
        if len(candidates) != 1 or not candidates.issubset(channels):
            raise ValueError(f'Ambiguous default channel for {package}: {sorted(candidates)}')
        merged[('olm.package', package, package)]['defaultChannel'] = candidates.pop()
    return [merged[key] for key in sorted(merged)]


class LayeredFbcMerger(KonfluxFbcFragmentMerger):
    """Reuse the FBC image builder with channel-aware content and release metadata."""

    def __init__(self, *, runtime, target_version: str, registry_auth: str | None, **kwargs):
        if not re.fullmatch(r'\d+\.\d+', target_version):
            raise ValueError('OCP target must have MAJOR.MINOR format')
        self.runtime = runtime
        self.target_version = target_version
        self.product = runtime.product
        self.identity = None
        self.name = None
        self.version = target_version
        self.release = f'{datetime.now(timezone.utc):%Y%m%d%H%M%S%f}.ocp{target_version}'
        self.nvr = None
        self.inputs: list[LayeredFbcInput] = []
        self.catalog: list[dict] = []
        self.provenance = {}
        self._build_arches: list[str] | None = None
        self.db = KonfluxDb()
        self.db.bind(KonfluxFbcBuildRecord)
        self.bundle_db = KonfluxDb()
        self.bundle_db.bind(KonfluxBundleBuildRecord)
        super().__init__(
            group=runtime.group,
            group_config=runtime.group_config.primitive(),
            working_dir=Path(runtime.working_dir) / 'konflux-fbc-sources',
            registry_auth=registry_auth,
            major_minor_override=tuple(map(int, target_version.split('.'))),
            **kwargs,
        )

    def get_application_name(self, group: str):
        if not self.identity:
            raise ValueError('Prepare the operator inputs before building a unified FBC')
        return self.name.replace('.', '-').replace('_', '-')

    def get_component_name(self, group: str):
        if not self.identity:
            raise ValueError('Prepare the operator inputs before building a unified FBC')
        return self.identity

    def _get_build_arches(self) -> list[str]:
        if self._build_arches is None:
            raise ValueError('Prepare the target architectures before building a unified FBC')
        return self._build_arches

    def _set_product_identity(self):
        self.name = f'{self.product}-fbc-unified'
        self.identity = f'{self.name}-{self.target_version}'.replace('.', '-').replace('_', '-')
        self.nvr = f'{self.name}-{self.version}-{self.release}'
        self.working_dir = Path(self.runtime.working_dir) / 'konflux-fbc-sources' / self.identity
        self.fbc_git_branch = f'art-{self.identity}'

    async def _pin_image(self, pullspec: str) -> str:
        info = await util.oc_image_info_for_arch_async(pullspec, registry_config=self.registry_auth)
        digest = info.get('listDigest') or info.get('digest')
        if not digest or not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
            raise ValueError(f'Cannot determine immutable digest for {pullspec}')
        repository = pullspec.split('@')[0]
        if ':' in repository.rsplit('/', 1)[-1]:
            repository = repository.rsplit(':', 1)[0]
        return f'{repository}@{digest}'

    def _load_group(self, group: str) -> tuple[dict, str]:
        # Each source group gets its own clone; never switch the execution runtime's checkout.
        gitdata = GitData(
            data_path=self.runtime.data_path,
            clone_dir=str(Path(self.runtime.working_dir) / 'fbc-merge-build-data' / group),
            commitish=group,
            reclone=True,
        )
        loader = BuildDataLoader(
            data_path=self.runtime.data_path,
            clone_dir=gitdata.clone_dir,
            commitish=group,
            build_system='konflux',
            gitdata=gitdata,
        )
        config = loader.load_group_config(assembly='stream', releases_config=None)
        if config.get('product') != self.product or self.target_version not in config.get('OCP_TARGET_VERSIONS', []):
            raise ValueError(f'{group} does not support product {self.product} on OCP {self.target_version}')
        return config, gitdata.commit_hash

    async def prepare(self, builds: tuple[str, ...]):
        if not builds:
            raise ValueError('At least one FBC build is required')
        self._build_arches = None
        self._build_arches = await get_target_fbc_arches(
            self.runtime.data_path, self.major_minor_override, self._logger
        )
        self.inputs = []
        seen = set()
        revisions = {}
        auth = opm.OpmRegistryAuth(self.registry_auth) if self.registry_auth else None
        migrate_level = 'bundle-object-to-csv-metadata' if self.major_minor_override >= (4, 17) else 'none'
        for build in builds:
            requested_pullspec = None
            if '/' in build:
                requested_pullspec = await self._pin_image(build)
                info = await util.oc_image_info_for_arch_async(requested_pullspec, registry_config=self.registry_auth)
                nvr = info['config']['config'].get('Labels', {}).get('com.redhat.art.nvr')
                if not nvr:
                    raise ValueError(f'Missing FBC NVR label on {build}')
            else:
                nvr = build
            # Query across groups: the generic NVR lookup guesses openshift-* for v-prefixed versions.
            record = await anext(
                self.db.search_builds_by_fields(
                    where={'nvr': nvr, 'outcome': str(KonfluxBuildOutcome.SUCCESS)}, limit=1
                ),
                None,
            )
            if not record or extract_ocp_version_from_fbc_nvr(record.nvr) != self.target_version:
                raise ValueError(f'{build} is not a successful FBC for OCP {self.target_version}')
            if not record.group or not re.fullmatch(r'[\w.-]+', record.group):
                raise ValueError(f'Invalid source group for {build}')
            if not record.rebase_repo_url or not re.fullmatch(r'[0-9a-f]{40}', record.rebase_commitish or ''):
                raise ValueError(f'{build} has no immutable FBC source revision')
            if record.group not in revisions:
                _, revisions[record.group] = await asyncio.to_thread(self._load_group, record.group)
            if len(record.bundle_nvrs) != 1:
                raise ValueError(f'{build} must reference exactly one source bundle')
            pinned = await self._pin_image(record.image_pullspec)
            if requested_pullspec and requested_pullspec != pinned:
                raise ValueError(f'{build} does not match its recorded FBC image {pinned}')
            record.image_pullspec = pinned
            bundle_records = await self.bundle_db.get_build_records_by_nvrs(
                record.bundle_nvrs, group=record.group, exclude_large_columns=True
            )
            bundle = bundle_records[0]
            bundle_info = await util.oc_image_info_for_arch_async(
                bundle.image_pullspec, registry_config=self.registry_auth
            )
            labels = bundle_info['config']['config']['Labels']
            package = labels.get('operators.operatorframework.io.bundle.package.v1')
            channels = tuple(
                sorted(set(labels.get('operators.operatorframework.io.bundle.channels.v1', '').split(',')))
            )
            if not package or not channels or any(not channel for channel in channels):
                raise ValueError(f'{bundle.nvr} has missing package or channel labels')
            # The recorded CSV comes from package.yaml's currentCSV, which can differ from the actual CSV.
            bundle_blobs = await opm.render(bundle.image_pullspec, auth=auth, migrate_level=migrate_level)
            if len(bundle_blobs) != 1 or bundle_blobs[0].get('schema') != 'olm.bundle':
                raise ValueError(f'{bundle.nvr}: expected exactly one rendered olm.bundle')
            bundle_blob = bundle_blobs[0]
            if bundle_blob.get('package') != package:
                raise ValueError(f'{bundle.nvr}: rendered bundle package does not match label {package}')
            csv_name = bundle_blob.get('name')
            if not isinstance(csv_name, str) or not csv_name:
                raise ValueError(f'{bundle.nvr}: rendered bundle has no CSV name')
            selection = (record.name, record.group)
            if selection in seen:
                raise ValueError(f'Multiple FBC builds supplied for {selection}')
            seen.add(selection)
            catalog = await opm.render(pinned, auth=auth, migrate_level=migrate_level)
            if {blob['name'] for blob in catalog if blob['schema'] == 'olm.package'} != {package}:
                raise ValueError(f'{build} must contain exactly its source bundle package {package}')
            self.inputs.append(
                LayeredFbcInput(record, bundle, package, channels, catalog, revisions[record.group], csv_name)
            )

        baseline = await self._pin_image(
            PRODUCTION_INDEX_PULLSPEC_FORMAT.format(
                major=self.major_minor_override[0], minor=self.major_minor_override[1]
            )
        )
        baseline_blobs = await opm.render(baseline, auth=auth, migrate_level=migrate_level)
        self.catalog = merge_layered_catalogs(baseline_blobs, self.inputs)
        self._set_product_identity()
        self.provenance = {
            'product': self.product,
            'operators': sorted({source.package for source in self.inputs}),
            'ocp_target_version': self.target_version,
            'production_index': baseline,
            'execution_group': self.runtime.group,
            'inputs': [
                {
                    'nvr': source.record.nvr,
                    'pullspec': source.record.image_pullspec,
                    'group': source.record.group,
                    'group_revision': source.group_revision,
                    'bundle_nvr': source.bundle.nvr,
                    'bundle_csv_name': source.csv_name,
                    'package': source.package,
                    'channels': list(source.channels),
                }
                for source in sorted(self.inputs, key=lambda source: source.record.nvr)
            ],
        }

    async def _render_catalog(self, fragments):
        return self.catalog

    async def _merge_idms(self, fragments):
        def fetch_mirrors(record):
            url = urlparse(record.rebase_repo_url)
            if url.hostname != 'github.com':
                raise ValueError(f'Unsupported FBC source repository {record.rebase_repo_url}')
            repo_name = url.path.strip('/').removesuffix('.git')
            client = get_github_client_for_org(repo_name.split('/')[0])
            content = client.get_repo(repo_name).get_contents(
                '.tekton/images-mirror-set.yaml', ref=record.rebase_commitish
            )
            return opm.yaml.load(content.decoded_content)

        sources = {}
        for source in self.inputs:
            idms = await asyncio.to_thread(fetch_mirrors, source.record)
            for mirror in (idms or {}).get('spec', {}).get('imageDigestMirrors', []):
                sources.setdefault(mirror['source'], set()).update(mirror['mirrors'])
        return {
            'apiVersion': 'config.openshift.io/v1',
            'kind': 'ImageDigestMirrorSet',
            'metadata': {'name': 'art-unified-fbc-mirror-set'},
            'spec': {
                'imageDigestMirrors': [
                    {'source': source, 'mirrors': sorted(mirrors)} for source, mirrors in sorted(sources.items())
                ]
            },
        }

    async def _prepare_source(self, build_repo: BuildRepo):
        self.build_source = build_repo
        # Replace previous catalog contents and use the package layout required by validate-fbc.
        catalog_dir = build_repo.local_dir / 'catalog'
        for path in catalog_dir.iterdir():
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()
        catalogs = {}
        for blob in self.catalog:
            catalogs.setdefault(_package(blob), []).append(blob)
        for package, blobs in sorted(catalogs.items()):
            package_dir = catalog_dir / package
            package_dir.mkdir()
            with (package_dir / 'catalog.yaml').open('w') as catalog_file:
                opm.yaml.dump_all(blobs, catalog_file)
        dfp = DockerfileParser(str(build_repo.local_dir / 'catalog.Dockerfile'))
        dfp.labels['com.redhat.art.name'] = self.name
        dfp.labels['com.redhat.art.nvr'] = self.nvr
        dfp.envs = {
            '__doozer_group': self.group,
            '__doozer_version': self.version,
            '__doozer_release': self.release,
            '__doozer_bundle_nvrs': ','.join(sorted(source.bundle.nvr for source in self.inputs)),
        }
        (build_repo.local_dir / 'unified-provenance.json').write_text(json.dumps(self.provenance, indent=2) + '\n')

    async def _record_build(self, build_repo, plr_info, outcome):
        plr = plr_info.to_dict()
        status = plr.get('status', {})
        results = {result['name']: result['value'] for result in status.get('results', [])}
        pullspec = None
        if outcome == KonfluxBuildOutcome.SUCCESS:
            if not results.get('IMAGE_URL') or not results.get('IMAGE_DIGEST'):
                raise ValueError('Successful unified FBC PipelineRun has no IMAGE_URL or IMAGE_DIGEST')
            pullspec = f"{results['IMAGE_URL'].rsplit(':', 1)[0]}@{results['IMAGE_DIGEST']}"
        record = KonfluxFbcBuildRecord(
            name=self.name,
            group=self.group,
            assembly=self.runtime.assembly,
            version=self.version,
            release=self.release,
            nvr=self.nvr,
            rebase_repo_url=build_repo.https_url,
            rebase_commitish=build_repo.commit_hash,
            source_repo=build_repo.https_url,
            commitish=build_repo.commit_hash,
            outcome=outcome,
            build_id=plr_info.name,
            build_pipeline_url=self._konflux_client.resource_url(plr),
            build_component=self.get_component_name(self.group),
            image_pullspec=pullspec,
            image_tag=self.nvr if pullspec else None,
            art_job_url=os.environ.get('BUILD_URL', ''),
            bundle_nvrs=sorted(source.bundle.nvr for source in self.inputs),
            arches=self._get_build_arches(),
            build_variant=get_build_variant_for_product(self.product),
            start_time=datetime.fromisoformat(status['startTime'].replace('Z', '+00:00'))
            if status.get('startTime')
            else datetime.now(timezone.utc),
            end_time=datetime.fromisoformat(status['completionTime'].replace('Z', '+00:00'))
            if status.get('completionTime')
            else None,
        )
        await asyncio.to_thread(self.db.add_build, record)

    async def _build(self, build_repo, target_index, git_auth_secret):
        if self.dry_run:
            return None
        client = self._konflux_client
        secret = await client.ensure_git_auth_secret(namespace=self.konflux_namespace)
        refresh = asyncio.create_task(client.token_refresh_loop(namespace=self.konflux_namespace))
        try:
            return await super()._build(build_repo, target_index, secret)
        finally:
            refresh.cancel()
            try:
                await refresh
            except asyncio.CancelledError:
                pass
            try:
                await client.delete_git_auth_secret(namespace=self.konflux_namespace)
            except Exception:
                self._logger.exception('Failed to clean up unified FBC git-auth secret')

    async def merge(self, image_repo: str) -> dict:
        plr_info = await self.run([source.record.image_pullspec for source in self.inputs], f'{image_repo}:{self.nvr}')
        result = {
            'nvr': self.nvr,
            'pullspec': None,
            'build_url': None,
            'dry_run': self.dry_run,
            'provenance': self.provenance,
            'provenance_url': None,
        }
        if plr_info:
            plr = plr_info.to_dict()
            results = {result['name']: result['value'] for result in plr['status']['results']}
            result['pullspec'] = f"{results['IMAGE_URL'].rsplit(':', 1)[0]}@{results['IMAGE_DIGEST']}"
            result['build_url'] = self._konflux_client.resource_url(plr)
            result['provenance_url'] = (
                f'{self.build_source.https_url.removesuffix(".git")}/blob/'
                f'{self.build_source.commit_hash}/unified-provenance.json'
            )
        return result
