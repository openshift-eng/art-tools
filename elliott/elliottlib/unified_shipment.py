"""Shared identity and input validation for assembly-based unified shipments."""

import json
import re
from functools import lru_cache
from pathlib import PurePosixPath

from artcommonlib import exectools
from artcommonlib.assembly import assembly_group_config, assembly_image_pins
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.model import Model
from artcommonlib.product_catalog import get_product_config
from artcommonlib.util import extract_ocp_version_from_fbc_nvr, oc_image_info_for_arch_async, split_git_url
from yaml import safe_load

from elliottlib.shipment_model import Shipment
from elliottlib.shipment_utils import _checked_ci_status, _prod_bridge_was_attempted


def effective_inputs(releases: dict, assembly: str, target: str) -> dict:
    """Capture release content without mutable delivery bookkeeping.

    Args:
        releases: Complete releases.yml document, including inherited assemblies.
        assembly: Contributing named assembly.
        target: Catalog OCP major.minor version.

    Returns:
        Effective image pins and immutable operator FBC selections.

    Raises:
        ValueError: If the contributing assembly or target records are absent.
    """
    definition = releases.get('releases', {}).get(assembly, {}).get('assembly', {})
    records = definition.get('group', {}).get('fbc_builds', {}).get(target)
    if not records:
        raise ValueError(f'{assembly} has no FBC records for OCP {target}')
    pins = sorted(set(assembly_image_pins(Model(releases), assembly).values()))
    if not pins:
        raise ValueError(f'{assembly} has no effective image pins')
    selected = {}
    for operator, record in sorted(records.items()):
        if sorted(record.get('operand_nvrs', [])) != pins:
            raise ValueError(f'{assembly}/{operator} has stale FBC pins')
        pullspec = record.get('pullspec', '')
        if not re.fullmatch(r'[^@\s]+@sha256:[0-9a-f]{64}', pullspec):
            raise ValueError(f'{assembly}/{operator} has no immutable FBC digest')
        selected[operator] = {'nvr': record['nvr'], 'pullspec': pullspec}
    return {'image_nvrs': pins, 'fbcs': selected}


def validate_contributor_inputs(provenance: dict, contributors: list, load_releases) -> None:
    """Compare current assembly content with the pinned catalog input selection.

    Args:
        provenance: Catalog provenance read at its immutable source commit.
        contributors: Validated shipment contributor models.
        load_releases: Callable accepting (group, revision); None means current branch.

    Raises:
        ValueError: If provenance associations differ or relevant input content changed.
    """
    target = provenance.get('ocp_target_version')
    if not isinstance(target, str) or not re.fullmatch(r'[0-9]+\.[0-9]+', target):
        raise ValueError('Provenance has no valid OCP target')
    recorded = provenance.get('inputs')
    if not isinstance(recorded, list) or not recorded:
        raise ValueError('Assembly-based unified provenance has no inputs')
    identities = {(a.group, a.assembly, a.revision) for a in contributors}
    actual = {(r['group'], r.get('assembly'), r.get('assembly_group_revision')) for r in recorded}
    if identities != actual:
        raise ValueError('Shipment contributors do not match pinned catalog provenance')
    for a in contributors:
        pinned = load_releases(a.group, a.revision)
        expected = effective_inputs(pinned, a.assembly, target)
        original_pointer = pinned['releases'][a.assembly]['assembly'].get('group', {}).get('shipment', {}).get('mr')
        if original_pointer != a.imageShipmentMR:
            raise ValueError(f'{a.group}/{a.assembly} image shipment differs from pinned preparation')
        selections = {
            r['nvr']: r['pullspec'] for r in recorded if r['group'] == a.group and r['assembly'] == a.assembly
        }
        if selections != {r['nvr']: r['pullspec'] for r in expected['fbcs'].values()}:
            raise ValueError(f'{a.group}/{a.assembly} FBC selection differs from pinned provenance')
        current = load_releases(a.group, None)
        if effective_inputs(current, a.assembly, target) != expected:
            raise ValueError(f'{a.group}/{a.assembly} changed since unified preparation; regenerate and restage')
        pointer = current['releases'][a.assembly]['assembly'].get('group', {}).get('shipment', {}).get('mr')
        if pointer != a.imageShipmentMR:
            raise ValueError(f'{a.group}/{a.assembly} image shipment association changed; reconcile before release')


def catalog_target(shipment) -> str:
    """Return a unified artifact's target, rejecting conflicting NVR/component identity."""

    target = extract_ocp_version_from_fbc_nvr(shipment.snapshot.nvrs[0])
    product = get_product_config(shipment.metadata.product)
    prefix = product.build_data_group_prefix or product.product_name
    application = f'{prefix}-fbc-unified'
    if (
        not target
        or shipment.metadata.application != application
        or not shipment.snapshot.nvrs[0].startswith(f'{application}-{target}-')
    ):
        raise ValueError('Unified application or artifact OCP target is invalid')
    if shipment.snapshot.spec.components[0].name != f'{application}-{target}'.replace('.', '-'):
        raise ValueError('Unified component and artifact target disagree')
    return target


def load_pinned_provenance(shipment) -> dict:
    """Read the standard provenance file at the catalog's immutable source commit."""

    source = shipment.snapshot.spec.components[0].source.git
    host, owner, name = split_git_url(source.url)
    if host != 'github.com':
        raise ValueError('Unified source provenance requires a GitHub repository')
    repo = get_github_client_for_org(owner).get_repo(f'{owner}/{name}')
    provenance = json.loads(repo.get_contents('unified-provenance.json', ref=source.revision).decoded_content)
    if provenance.get('product') != shipment.metadata.product or provenance.get('ocp_target_version') != catalog_target(
        shipment
    ):
        raise ValueError('Unified artifact and pinned provenance disagree')
    return provenance


def validate_current_inputs(shipment, provenance: dict) -> None:
    """Recompute pinned/current effective inputs, using provenance's build-data repository."""

    url = provenance.get('build_data_repo', 'https://github.com/openshift-eng/ocp-build-data')
    host, owner, name = split_git_url(url)
    if host != 'github.com':
        raise ValueError('Unified assembly inputs require GitHub build data')
    repo = get_github_client_for_org(owner).get_repo(f'{owner}/{name}')

    heads = {a.group: repo.get_branch(a.group).commit.sha for a in shipment.unified.assemblies}

    @lru_cache
    def load(group, revision):
        revision = revision or heads[group]
        return safe_load(repo.get_contents('releases.yml', ref=revision).decoded_content)

    validate_contributor_inputs(provenance, shipment.unified.assemblies, load)

    for contributor in shipment.unified.assemblies:
        for revision in (contributor.revision, heads[contributor.group]):
            releases = load(contributor.group, revision)
            group = safe_load(repo.get_contents('group.yml', ref=revision).decoded_content)
            effective = assembly_group_config(Model(releases), contributor.assembly, Model(group))
            if (
                effective.product != shipment.metadata.product
                or catalog_target(shipment) not in effective.OCP_TARGET_VERSIONS
            ):
                raise ValueError(f'{contributor.group} product or supported target coverage changed; reconcile inputs')


def shipment_files(project, mr, ref):
    """Read only shipment YAML files changed by one candidate MR at an exact ref."""

    changes = mr.changes()
    if changes.get('overflow'):
        raise RuntimeError(f'MR {mr.web_url} change list is truncated')
    for change in changes['changes']:
        path = change['new_path']
        if change.get('deleted_file') or not path.startswith('shipment/') or not path.endswith(('.yaml', '.yml')):
            continue
        data = safe_load(project.files.get(file_path=path, ref=ref).decode())
        yield path, data['shipment']


def revision_shipment_files(project, mr, revision):
    """Read revision-specific MR membership, including files removed from today's diff.

    GitLab computes the merge base against the MR base so unrelated earlier
    target-branch files are excluded. Compare results must be complete.
    """
    comparison = project.repository_compare(mr.diff_refs['base_sha'], revision, straight=False)
    if comparison.get('compare_timeout') or comparison.get('overflow'):
        raise RuntimeError(f'Cannot inspect complete historical shipment membership for {mr.web_url}')
    for change in comparison['diffs']:
        if change.get('too_large') or change.get('collapsed'):
            raise RuntimeError(f'Historical shipment diff is incomplete for {mr.web_url}')
        path = change['new_path']
        if change.get('deleted_file') or not path.startswith('shipment/') or not path.endswith(('.yaml', '.yml')):
            continue
        yield path, safe_load(project.files.get(file_path=path, ref=revision).decode())['shipment']


def production_pipelines(project, mr):
    """Yield attempted production parent/child pipelines using existing CI state rules."""

    for summary in mr.pipelines.list(get_all=True):
        parent = project.pipelines.get(summary.id)
        for bridge in parent.bridges.list(get_all=True):
            if bridge.name != 'prod-job':
                continue
            status = _checked_ci_status(bridge, f'{parent.web_url} prod-job')
            downstream = getattr(bridge, 'downstream_pipeline', None)
            if _prod_bridge_was_attempted(bridge, status, downstream, parent.web_url):
                if downstream and downstream.get('project_id', project.id) != project.id:
                    raise RuntimeError('Unexpected cross-project production pipeline')
                child = project.pipelines.get(downstream['id']) if downstream else None
                yield parent, child


def validate_image_production(shipment, provenance: dict, gitlab_client) -> None:
    """Require successful production watch jobs for the exact contributing image content.

    Approval, merge, stage success and prod labels alone are not delivery proof.
    The successful job's parent revision must contain the current image snapshot,
    including all digests pinned from build records at catalog build time.
    """

    for contributor in shipment.unified.assemblies:
        project_path, _ = gitlab_client._parse_mr_url(contributor.imageShipmentMR)
        project = gitlab_client.get_project(project_path)
        mr = gitlab_client.get_mr_from_url(contributor.imageShipmentMR)
        required = set()
        for record in provenance['inputs']:
            if record['group'] == contributor.group and record['assembly'] == contributor.assembly:
                required.add(record['bundle_nvr'])
        # Resolve original effective pins, rather than trusting only the FBC's bundle list.

        _, owner, name = split_git_url(
            provenance.get('build_data_repo', 'https://github.com/openshift-eng/ocp-build-data')
        )
        repo = get_github_client_for_org(owner).get_repo(f'{owner}/{name}')
        original = safe_load(repo.get_contents('releases.yml', ref=contributor.revision).decoded_content)
        required.update(effective_inputs(original, contributor.assembly, catalog_target(shipment))['image_nvrs'])
        images = {}
        for path, data in shipment_files(project, mr, mr.sha):
            meta = data.get('metadata', {})
            if (
                not meta.get('fbc')
                and meta.get('product') == shipment.metadata.product
                and meta.get('group') == contributor.group
                and meta.get('assembly') == contributor.assembly
            ):
                images[path] = data
        if not images or not required.issubset({n for data in images.values() for n in data['snapshot']['nvrs']}):
            raise RuntimeError(f'{contributor.imageShipmentMR} does not contain matching contributing images')
        pending = set(images)
        for parent, child in production_pipelines(project, mr):
            if child is None:
                continue
            successful = {j.name for j in child.jobs.list(get_all=True) if j.status == 'success'}
            historic = dict(shipment_files(project, mr, parent.sha))
            for path in list(pending):
                job = f'{PurePosixPath(path).stem}-watch-prod'
                if job in successful and historic.get(path, {}).get('snapshot') == images[path]['snapshot']:
                    pending.remove(path)
        if pending:
            raise RuntimeError(f'Image production incomplete for {contributor.imageShipmentMR}: {sorted(pending)}')

        expected = provenance.get('image_digests', {}).get(contributor.group, {})
        if set(expected) != required or any(not re.fullmatch(r'sha256:[0-9a-f]{64}', d) for d in expected.values()):
            raise RuntimeError(f'Missing build digest provenance for {contributor.group}/{contributor.assembly}')
        shipped_digests = {
            component['containerImage'].rpartition('@')[2]
            for image in images.values()
            for component in image['snapshot']['spec']['components']
        }
        mismatches = sorted(nvr for nvr, digest in expected.items() if digest not in shipped_digests)
        if mismatches:
            raise RuntimeError(
                f'{contributor.imageShipmentMR} production snapshot is missing pinned digests: {mismatches}'
            )


def validate_production_freeze(shipment, config_path: str, gitlab_client, mr_url: str) -> None:
    """Reject shipment content edits after any production attempt, allowing result bookkeeping."""
    project_path, _ = gitlab_client._parse_mr_url(mr_url)
    project = gitlab_client.get_project(project_path)
    mr = gitlab_client.get_mr_from_url(mr_url)

    def content(data):
        data = dict(data)
        data['environments'] = {
            env: {k: v for k, v in values.items() if k not in ('result', 'advisory')}
            for env, values in data['environments'].items()
        }
        return data

    def shipment_set(revision):
        return {
            path: content(Shipment.model_validate(data).model_dump(exclude_none=True, exclude_unset=True))
            for path, data in revision_shipment_files(project, mr, revision)
            if data.get('metadata', {}).get('unified')
        }

    current = None
    for parent, _ in production_pipelines(project, mr):
        if current is None:
            current = shipment_set(mr.sha)
            supplied = content(shipment.model_dump(exclude_none=True, exclude_unset=True))
            if current.get(config_path) != supplied:
                raise RuntimeError('Release job shipment differs from current MR content')
        if shipment_set(parent.sha) != current:
            raise RuntimeError('Unified shipment content changed after production started; create a new MR')


async def validate_unified_release(
    shipment, env: str, registry_auth: str | None = None, gitlab_client=None, mr_url=None, config_path=None
) -> dict:
    """Validate pinned artifact identity, current inputs and production release prerequisites."""

    provenance = await exectools.to_thread(load_pinned_provenance, shipment)
    component = shipment.snapshot.spec.components[0]
    info = await oc_image_info_for_arch_async(component.containerImage, registry_config=registry_auth)
    labels = info['config']['config'].get('Labels', {})
    if (
        labels.get('com.redhat.art.nvr') != shipment.snapshot.nvrs[0]
        or labels.get('org.opencontainers.image.revision') != component.source.git.revision
    ):
        raise ValueError('Unified image labels do not match shipment NVR/source revision')
    await exectools.to_thread(validate_current_inputs, shipment, provenance)
    if mr_url:
        await exectools.to_thread(validate_production_freeze, shipment, config_path, gitlab_client, mr_url)
    if env == 'prod':
        if not mr_url or gitlab_client is None:
            raise ValueError('Unified production requires its shipment MR URL for content-freeze checks')
        await exectools.to_thread(validate_image_production, shipment, provenance, gitlab_client)
    return provenance
