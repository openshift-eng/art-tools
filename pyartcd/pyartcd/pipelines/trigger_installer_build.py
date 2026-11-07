"""Submit an installer build for an OCP payload, manually or after successful promotion."""

import asyncio
import hashlib
import logging
import os
import re

import click
import requests
from ruamel.yaml import YAML
from semver import VersionInfo

from pyartcd import oc, tekton
from pyartcd.cli import cli, click_coroutine, pass_runtime

LOGGER = logging.getLogger(__name__)


async def trigger_installer_build(payload: str, kubeconfig: str | None = None, *, dry_run: bool = False) -> str | None:
    """Resolve a published amd64 payload and submit its matching installer PipelineRun.

    A confirmed absent installer group is unsupported and skipped. Access failures
    are errors. Deterministic run names deduplicate retries while the run is retained.
    """
    if not payload or not payload.strip():
        raise ValueError('A published OCP payload pullspec is required')
    payload = payload.strip()
    if '://' in payload or not re.fullmatch(r'[^@\s?#]+(?:@sha256:[0-9a-f]{64})?', payload):
        raise ValueError('Payload must be an OCI image reference without embedded credentials or URL parameters')
    info = await oc.get_release_image_info(
        payload, raise_if_not_found=True, registry_config=os.environ.get('QUAY_AUTH_FILE')
    )
    version = VersionInfo.parse(info['metadata']['version'])
    if info.get('config', {}).get('architecture') != 'amd64':
        raise ValueError('Installer payload triggering currently requires an amd64 payload')
    digest = info.get('digest', '')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
        raise ValueError('Release metadata must include a valid sha256 payload digest')
    repository = payload.partition('@')[0]
    # Strip an image tag, preserving a possible port in the registry hostname.
    if ':' in repository.rsplit('/', 1)[-1]:
        repository = repository.rsplit(':', 1)[0]
    canonical_payload = f'{repository}@{digest}'
    group = f'installer-ove-ui-{version.major}.{version.minor}'
    response = await asyncio.to_thread(
        requests.get,
        f'https://raw.githubusercontent.com/openshift-eng/ocp-build-data/{group}/group.yml',
        timeout=(10, 30),
        allow_redirects=False,
    )
    if response.status_code == 404:
        LOGGER.info('No installer group %s; skipping payload digest %.19s', group, digest)
        return None
    if response.status_code != 200:
        raise RuntimeError(f'Could not read installer group {group}: HTTP {response.status_code}')
    config = YAML(typ='safe').load(response.text)
    if not isinstance(config, dict) or config.get('product') != 'openshift_agent_installer':
        raise ValueError(f'{group} must configure product openshift_agent_installer')
    group_version = str(config.get('version', '')).removeprefix('v')
    if group_version.count('.') == 1:
        group_version += '.0'
    group_version = VersionInfo.parse(group_version)
    if (version.major, version.minor) != (group_version.major, group_version.minor):
        raise ValueError(f'{group} version does not match payload version {version}')
    if 'x86_64' not in config.get('arches', []):
        raise ValueError(f'{group} does not support x86_64')
    identity = hashlib.sha256(f'{group}:{digest}'.encode()).hexdigest()[:24]
    run_name = f'installer-build-{version.major}-{version.minor}-{identity}'
    params = {
        'group': group,
        'assembly': 'stream',
        'image-build-strategy': 'only',
        'image-list': 'art-agent-installer-iso',
        'payload': canonical_payload,
    }
    if dry_run:
        LOGGER.info('[DRY RUN] Would submit %s for group %s (payload digest %.19s)', run_name, group, digest)
        return None
    # The orchestration Pipeline lives on the ART cluster, not the Konflux build cluster.
    kubeconfig = kubeconfig or os.environ.get('ART_CLUSTER_ART_CD_PIPELINE_KUBECONFIG')
    if not kubeconfig:
        raise ValueError('Installer submission requires --kubeconfig or ART_CLUSTER_ART_CD_PIPELINE_KUBECONFIG')
    return await asyncio.to_thread(
        tekton.start_pipeline_run,
        'build-layered-products',
        params,
        namespace='art-installer-agent-tenant',
        kubeconfig=kubeconfig,
        run_name=run_name,
    )


@cli.command('trigger-installer-build', help=__doc__)
@click.option('--payload', required=True, help='The exact published OCP release image pullspec.')
@click.option(
    '--kubeconfig', type=click.Path(exists=True, dir_okay=False), help='Installer namespace submitter kubeconfig.'
)
@pass_runtime
@click_coroutine
async def trigger_installer_build_cli(runtime, payload: str, kubeconfig: str | None):
    """Submit a manual installer build; automatic builds follow successful promotion."""
    await trigger_installer_build(payload, kubeconfig, dry_run=runtime.dry_run)
