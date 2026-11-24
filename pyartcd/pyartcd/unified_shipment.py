"""Generate fresh complete unified shipment MRs; never reuse MRs or build artifacts."""

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from artcommonlib import exectools
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.github_auth import build_git_auth_env
from artcommonlib.gitlab import GitLabClient
from artcommonlib.product_catalog import get_product_config
from elliottlib.cli.konflux_release_cli import validate_snapshot_against_rpa
from elliottlib.shipment_model import ShipmentConfig
from elliottlib.unified_shipment import catalog_target, shipment_files, validate_unified_release
from yaml import safe_dump, safe_load

from pyartcd.git import GitRepository
from pyartcd.lp_shipment import create_shipment_mr_with_retry, resolve_shipment_release_plans

UNIFIED_LABEL = 'unified-fbc'


def branch_prefix(product: str) -> str:
    """Return the stable branch discovery prefix for the canonical product."""
    config = get_product_config(product)
    return f'unified-fbc-{config.build_data_group_prefix or config.product_name}-'


def candidate_targets(project, mr, product: str, revision=None) -> set[str]:
    """Read only one candidate MR's unified shipment files, not its CI history."""
    targets = set()
    for _, data in shipment_files(project, mr, revision or mr.sha):
        if data.get('metadata', {}).get('unified') and data['metadata'].get('product') == product:
            targets.add(catalog_target(ShipmentConfig.model_validate({'shipment': data}).shipment))
    if not targets:
        raise RuntimeError(f'Cannot determine unified target coverage for {mr.web_url}')
    return targets


def check_overlap(client, project, product: str, targets: set[str]) -> None:
    """Stop on overlapping generated MRs or active deliveries within shipment-data only.

    Uses the unified label and product branch prefix as discovery conventions.
    Legacy/manual MRs must opt into those conventions. Titles are not identity.
    """
    prefix = branch_prefix(product)
    checked = set()

    def inspect(mr, pipeline_url=None, revision=None):
        if not mr.source_branch.startswith(prefix):
            return
        key = (mr.iid, revision)
        if key in checked:
            return
        checked.add(key)
        source = project if revision else client.get_project(mr.source_project_id)
        if targets & candidate_targets(source, mr, product, revision):
            raise RuntimeError(
                f'Overlapping unified shipment: {mr.web_url}' + (f' ({pipeline_url})' if pipeline_url else '')
            )

    for summary in client.list_merge_requests(
        project.path_with_namespace,
        state='opened',
        project=project,
        target_branch='main',
        labels=[UNIFIED_LABEL],
        per_page=100,
    ):
        if summary.source_branch.startswith(prefix):
            inspect(project.mergerequests.get(summary.iid))
    # Only active pipelines in this project, not historical pipelines across GitLab.
    seen_pipelines = set()
    for status in ('created', 'waiting_for_resource', 'preparing', 'pending', 'running', 'scheduled'):
        for source_filter in ({}, {'source': 'parent_pipeline'}):
            for summary in project.pipelines.list(status=status, get_all=True, per_page=100, **source_filter):
                if summary.id in seen_pipelines:
                    continue
                seen_pipelines.add(summary.id)
                ref = summary.ref
                if not (ref.startswith(prefix) or ref.startswith('refs/merge-requests/')):
                    continue
                match = re.fullmatch(r'refs/merge-requests/([0-9]+)/(?:head|merge)', ref)
                if match:
                    candidates = [project.mergerequests.get(int(match.group(1)))]
                else:
                    candidates = project.mergerequests.list(
                        state='all', source_branch=ref, target_branch='main', get_all=True
                    )
                for candidate in candidates:
                    mr = project.mergerequests.get(candidate.iid)
                    if UNIFIED_LABEL in (mr.labels or []):
                        inspect(mr, summary.web_url, summary.sha)


async def create_unified_shipment(pipeline, plans: list, outcomes: list[dict]) -> str:
    """Validate every target and publish exactly one complete fresh shipment MR.

    Args:
        pipeline: Assembly-based unified orchestrator with a successful result set.
        plans: Validated per-target input selections.
        outcomes: Successful immutable build results in matching plan order.

    Returns:
        URL of the newly created draft MR. Nothing is auto-released.

    Raises:
        ValueError: If any target metadata, provenance or release mapping is invalid.
        RuntimeError: If a conflicting delivery exists or MR creation fails.
    """
    if len(plans) != len(outcomes) or not plans:
        raise ValueError('Cannot create a partial unified shipment')
    runtime = pipeline.runtime
    settings = runtime.config.get('shipment_config', {})
    pull = settings.get('shipment_data_url', SHIPMENT_DATA_URL_TEMPLATE)
    push = settings.get('shipment_data_push_url', pull)
    pull_url, push_url = urlparse(pull), urlparse(push)
    if pull_url.scheme != 'https' or push_url.scheme != 'https' or push_url.netloc != pull_url.netloc:
        raise ValueError('Shipment push URL must use HTTPS on the configured shipment GitLab host')
    token = os.environ.get('GITLAB_TOKEN')
    if not token:
        raise ValueError('GITLAB_TOKEN is required to create a unified shipment MR')
    client = GitLabClient.from_url(pull, gitlab_token=token)
    project = client.get_project(pull_url.path.removesuffix('.git').strip('/'))
    source = client.get_project(push_url.path.removesuffix('.git').strip('/'))
    repo = GitRepository(Path(runtime.working_dir) / 'shipment-data')
    await repo.setup(
        push, upstream_remote_url=pull, remote_auth_envs={'origin': build_git_auth_env(token, username='oauth2')}
    )
    await repo.fetch_switch_branch('main', remote='upstream')
    configs = []
    for plan, outcome in zip(plans, outcomes):
        required = ('nvr', 'pullspec', 'application', 'component', 'source_repo', 'source_commit', 'build_url')
        if outcome.get('status') != 'Succeeded' or any(not outcome.get(k) for k in required):
            raise ValueError(f'OCP {plan.target}: incomplete successful unified build metadata')
        stage, prod = resolve_shipment_release_plans(repo, outcome['application'])
        provenance = outcome['provenance']
        # Resolve associations from the same pinned build-data snapshots used for the build.
        from artcommonlib.github_auth import get_github_client_for_org
        from artcommonlib.util import split_git_url

        _, owner, name = split_git_url(pipeline.data_path)
        build_data = get_github_client_for_org(owner).get_repo(f'{owner}/{name}')
        contributors = {}
        for record in provenance['inputs']:
            key = (record['group'], record.get('assembly'), record.get('assembly_group_revision'))
            releases = safe_load(build_data.get_contents('releases.yml', ref=key[2]).decoded_content)
            mr_url = releases['releases'][key[1]]['assembly']['group']['shipment']['mr']
            contributors[key] = dict(group=key[0], assembly=key[1], revision=key[2], imageShipmentMR=mr_url)
        config = ShipmentConfig.model_validate(
            {
                'shipment': {
                    'metadata': {
                        'product': pipeline.product,
                        'application': outcome['application'],
                        'fbc': True,
                        'unified': True,
                    },
                    'snapshot': {
                        'spec': {
                            'application': outcome['application'],
                            'components': [
                                {
                                    'name': outcome['component'],
                                    'containerImage': outcome['pullspec'],
                                    'source': {
                                        'git': {'url': outcome['source_repo'], 'revision': outcome['source_commit']}
                                    },
                                }
                            ],
                        },
                        'nvrs': [outcome['nvr']],
                    },
                    'environments': {'stage': {'releasePlan': stage}, 'prod': {'releasePlan': prod}},
                    'unified': {'assemblies': [contributors[k] for k in sorted(contributors)]},
                }
            }
        )
        if catalog_target(config.shipment) != plan.target:
            raise ValueError(f'OCP {plan.target}: artifact target mismatch')
        expected = {(v['group'], v['assembly'], v['group_revision'], v['pullspec']) for v in plan.inputs.values()}
        actual = {
            (v['group'], v['assembly'], v['assembly_group_revision'], v['pullspec']) for v in provenance['inputs']
        }
        if actual != expected:
            raise ValueError(f'OCP {plan.target}: build provenance does not match requested assembly inputs')
        pinned = await validate_unified_release(config.shipment, 'stage', pipeline.options['--registry-auth'])
        if pinned != provenance:
            raise ValueError(f'OCP {plan.target}: build result and committed provenance disagree')
        await validate_snapshot_against_rpa(
            config.shipment, 'prod', 'fbc', registry_auth_file=pipeline.options['--registry-auth']
        )
        configs.append((plan.target, config))

    async def publish():
        await exectools.to_thread(check_overlap, client, project, pipeline.product, {p.target for p in plans})
        stamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')
        branch = branch_prefix(pipeline.product) + stamp
        await repo.create_branch(branch)
        for target, config in configs:
            application = config.shipment.metadata.application
            path = repo._directory / 'shipment' / pipeline.product / 'unified' / application / 'prod'
            name = f'{pipeline.product}.fbc.ocp{target}.{stamp}.yaml'
            destination = (path / name).resolve()
            if not destination.is_relative_to(repo._directory.resolve()):
                raise ValueError('Unified shipment destination escapes the checkout')
            destination.parent.mkdir(parents=True, exist_ok=True)
            shipment_data = config.model_dump(exclude_unset=True, exclude_none=True)
            destination.write_text(safe_dump(shipment_data, sort_keys=False))
        await repo.commit_push(f'Add unified FBC shipment for {pipeline.product}')
        links = sorted({a.imageShipmentMR for _, c in configs for a in c.shipment.unified.assemblies})
        mr = await create_shipment_mr_with_retry(
            source,
            {
                'source_branch': branch,
                'target_branch': 'main',
                'target_project_id': project.id,
                'title': f'Draft: Unified FBC shipment for {pipeline.product} ({", ".join(t for t, _ in configs)})',
                'description': 'Assembly-based unified FBC catalogs. Contributing image shipments:\n\n'
                + '\n'.join(links),
                'labels': [UNIFIED_LABEL],
                'remove_source_branch': True,
            },
            runtime.logger,
        )
        return mr.web_url

    from pyartcd import locks

    url = await locks.run_with_lock(
        publish(), locks.Lock.UNIFIED_SHIPMENT, locks.Lock.UNIFIED_SHIPMENT.value.format(product=pipeline.product)
    )
    if not url:
        raise RuntimeError('Unified shipment MR creation did not complete')
    return url
