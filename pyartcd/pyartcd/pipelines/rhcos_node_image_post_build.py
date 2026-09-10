import os
import re
from typing import Dict

import click
from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_REPO, RHCOS_IMAGE_REPO
from artcommonlib.registry_config import RegistryConfig
from artcommonlib.util import sync_to_quay

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.rhcos_jenkins_client import RhcosJenkinsClient
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config


class RhcosNodeImagePostBuildPipeline:
    """Run and publish a tested pair of Konflux-built RHCOS images."""

    _RELEASE_PATTERN = re.compile(r'^(?P<version>\d+\.\d+)-(?P<rhel_version>\d+\.\d+)$')
    _DIGEST_PULLSPEC_PATTERN = re.compile(rf'^{re.escape(RHCOS_IMAGE_REPO)}@sha256:[0-9a-f]{{64}}$')

    def __init__(self, runtime: Runtime, release: str, node_image: str, extensions_image: str):
        self.runtime = runtime
        self.release = release
        self.node_image = node_image
        self.extensions_image = extensions_image

    def _validate_inputs(self) -> str:
        match = self._RELEASE_PATTERN.fullmatch(self.release)
        if not match:
            raise ValueError(f'RELEASE must be an OCP/RHEL stream such as 5.0-9.8: {self.release}')

        for name, pullspec in (
            ('NODE_IMAGE', self.node_image),
            ('EXTENSIONS_IMAGE', self.extensions_image),
        ):
            if not self._DIGEST_PULLSPEC_PATTERN.fullmatch(pullspec):
                raise ValueError(f'{name} must be an immutable pullspec from {RHCOS_IMAGE_REPO}: {pullspec}')

        return match.group('version')

    async def _destination_tags(self, version: str) -> Dict[str, str]:
        """Resolve the floating destination tags for this RHEL stream."""

        rhel_version = self.release.split('-', 1)[1]
        group_config = await load_group_config(group=f'openshift-{version}', assembly='stream')
        tags = {}

        for payload_tag in group_config.get('rhcos', {}).get('payload_tags', []):
            configured_tag = payload_tag.get('rhcos_index_tag', '')
            floating_tag = configured_tag.rsplit(':', 1)[-1] if ':' in configured_tag else ''
            configured_rhel_version = str(payload_tag.get('rhel_version', ''))
            if not floating_tag or not (
                configured_rhel_version == rhel_version or floating_tag.startswith(f'{self.release}-')
            ):
                continue

            if floating_tag.endswith('-node-image-extensions'):
                tags['extensions'] = floating_tag
            elif floating_tag.endswith('-node-image'):
                tags['node'] = floating_tag

        missing = {'node', 'extensions'} - tags.keys()
        if missing:
            raise ValueError(f'group.yml has no RHCOS floating tag(s) for {self.release}: {sorted(missing)}')

        return tags

    async def _promote(self, tags: Dict[str, str]):
        quay_auth_file = os.environ.get('QUAY_AUTH_FILE')
        rhcos_quay_auth_file = os.environ.get('RHCOS_QUAY_AUTH_FILE')
        if not quay_auth_file or not rhcos_quay_auth_file:
            raise ValueError('QUAY_AUTH_FILE and RHCOS_QUAY_AUTH_FILE are required for RHCOS promotion')

        with RegistryConfig(
            source_files=[quay_auth_file, rhcos_quay_auth_file],
            registries=[KONFLUX_DEFAULT_IMAGE_REPO, RHCOS_IMAGE_REPO],
        ) as merged_auth_file:
            previous_auth_file = os.environ.get('QUAY_AUTH_FILE')
            os.environ['QUAY_AUTH_FILE'] = merged_auth_file
            try:
                await sync_to_quay(self.node_image, KONFLUX_DEFAULT_IMAGE_REPO, [tags['node']])
                await sync_to_quay(self.extensions_image, KONFLUX_DEFAULT_IMAGE_REPO, [tags['extensions']])
            finally:
                if previous_auth_file is None:
                    os.environ.pop('QUAY_AUTH_FILE', None)
                else:
                    os.environ['QUAY_AUTH_FILE'] = previous_auth_file

    async def run(self):
        """Run integration testing and mirror the exact tested image digests."""

        version = self._validate_inputs()
        if self.runtime.dry_run:
            self.runtime.logger.info(
                'Dry run: would run build-node-image and promote %s and %s for %s',
                self.node_image,
                self.extensions_image,
                self.release,
            )
            return

        client = RhcosJenkinsClient(kubeconfig_env_var='RHCOS_JENKINS_KUBECONFIG')
        build_number = client.trigger_build(
            'build-node-image',
            {
                'RELEASE': self.release,
                'NODE_IMAGE': self.node_image,
                'EXTENSIONS_IMAGE': self.extensions_image,
            },
        )
        result = client.wait_for_build('build-node-image', build_number)
        if result['result'] != 'SUCCESS':
            raise RuntimeError(
                f'RHCOS integration test failed for {self.release}: '
                f'{result.get("url", "")} - {result.get("description", "")}'
            )

        tags = await self._destination_tags(version)
        self.runtime.logger.info(
            'RHCOS integration test passed; promoting exact image digests with tags %s',
            tags,
        )
        await self._promote(tags)


@cli.command('rhcos-node-image-post-build', help='Test and promote a pair of Konflux-built RHCOS images')
@click.option('--release', required=True, help='RHCOS release stream, for example 5.0-9.8')
@click.option('--node-image', required=True, help='Immutable node image pullspec')
@click.option('--extensions-image', required=True, help='Immutable extensions image pullspec')
@pass_runtime
@click_coroutine
async def rhcos_node_image_post_build(runtime: Runtime, release: str, node_image: str, extensions_image: str):
    pipeline = RhcosNodeImagePostBuildPipeline(
        runtime=runtime,
        release=release,
        node_image=node_image,
        extensions_image=extensions_image,
    )
    await pipeline.run()
