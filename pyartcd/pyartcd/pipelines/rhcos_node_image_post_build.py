import html
import json
import os
import re
from typing import Dict, Optional

import click
from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_REPO, RHCOS_IMAGE_REPO
from artcommonlib.exectools import cmd_assert_async, cmd_gather_async
from artcommonlib.registry_config import RegistryConfig
from artcommonlib.util import sync_to_quay

from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.rhcos_jenkins_client import RhcosJenkinsClient
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config


class RhcosNodeImagePostBuildPipeline:
    """Run and publish a tested pair of Konflux-built RHCOS images."""

    _RELEASE_PATTERN = re.compile(r'^(?P<version>\d+\.\d+)-(?P<rhel_version>\d+\.\d+)$')
    _DIGEST_PULLSPEC_PATTERN = re.compile(rf'^{re.escape(RHCOS_IMAGE_REPO)}@sha256:[0-9a-f]{{64}}$')
    _RELEASE_LABEL_TIMESTAMP_PATTERN = re.compile(r'^(\d{12})\.')
    _INTEGRATION_TEST_TIMEOUT = 90 * 60

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

    async def _extract_timestamp_from_nvr(self, pullspec: str, auth_file: str) -> Optional[str]:
        """Extract the 12-digit build timestamp from the ``release`` label of a source image.

        Uses ``oc image info`` to read the image labels and parses the
        ``YYYYMMDDHHMI`` timestamp from the ``release`` label value
        (e.g. ``202610151443.p2.g62965e8.assembly.stream.el9``).

        Returns ``None`` when the label is absent or its format is
        unrecognised so that older groups whose images lack the label
        can still be promoted with floating tags only.
        """

        cmd = [
            'oc',
            'image',
            'info',
            '-o',
            'json',
            pullspec,
            '--filter-by-os=linux/amd64',
            f'--registry-config={auth_file}',
        ]

        self.runtime.logger.info('Reading release label from image %s', pullspec)
        _, stdout, _ = await cmd_gather_async(cmd, check=True)
        info = json.loads(stdout)

        release_label = info.get('config', {}).get('config', {}).get('Labels', {}).get('release', '')
        if not release_label:
            self.runtime.logger.warning('Image %s has no release label; skipping timestamped tags', pullspec)
            return None

        match = self._RELEASE_LABEL_TIMESTAMP_PATTERN.match(release_label)
        if not match:
            self.runtime.logger.warning(
                'Could not extract timestamp from release label %r of image %s; skipping timestamped tags',
                release_label,
                pullspec,
            )
            return None

        timestamp = match.group(1)
        self.runtime.logger.info('Extracted timestamp %s from release label %s', timestamp, release_label)
        return timestamp

    async def _resolve_image_digest_pullspec(self, pullspec: str, auth_file: str) -> str:
        """Resolve a mutable image tag to its immutable manifest-list digest pullspec."""

        self.runtime.logger.info('Resolving manifest-list digest for %s', pullspec)
        _, stdout, _ = await cmd_gather_async(
            [
                'oc',
                'image',
                'info',
                '--filter-by-os=linux/amd64',
                '-o',
                'json',
                pullspec,
                f'--registry-config={auth_file}',
            ],
            check=True,
        )
        image_info = json.loads(stdout)
        digest = image_info.get('listDigest') or image_info.get('digest')
        if not digest:
            raise RuntimeError(f'Image info did not return a manifest-list digest for {pullspec}')

        digest_pullspec = f'{pullspec.rsplit(":", 1)[0]}@{digest}'
        self.runtime.logger.info('Resolved %s to immutable pullspec %s', pullspec, digest_pullspec)
        return digest_pullspec

    async def _apply_manifest_list_tag_label(
        self,
        source_pullspec: str,
        dest_repo: str,
        dest_tag: str,
        label_value: str,
        auth_file: str,
    ) -> str:
        """Apply the ``coreos.build.manifest-list-tag`` label and return the destination pullspec."""

        dest = f'{dest_repo}:{dest_tag}'
        label_patch = json.dumps({'Labels': {'coreos.build.manifest-list-tag': label_value}})
        self.runtime.logger.info(
            'Applying coreos.build.manifest-list-tag=%s to each platform in %s -> %s',
            label_value,
            source_pullspec,
            dest,
        )

        async def inspect_manifest_list(cmd):
            exit_code, stdout, stderr = await cmd_gather_async(cmd, check=False)
            if exit_code:
                self.runtime.logger.warning('Manifest inspection failed: %s', stderr.strip())
                return None
            try:
                manifest_list = json.loads(stdout)
            except json.JSONDecodeError:
                self.runtime.logger.warning('Manifest inspection returned invalid JSON')
                return None
            manifests = manifest_list.get('list', {}).get('manifests') or manifest_list.get('manifests')
            if not manifests:
                self.runtime.logger.warning('Manifest inspection returned no platform manifests')
                return None
            return manifests

        manifests = await inspect_manifest_list(
            [
                'oc',
                'image',
                'info',
                source_pullspec,
                '--filter-by-os=.*',
                '-o',
                'json',
                f'--registry-config={auth_file}',
            ]
        )
        if manifests is None:
            self.runtime.logger.info('Retrying manifest inspection with skopeo')
            manifests = await inspect_manifest_list(
                [
                    'skopeo',
                    'inspect',
                    '--raw',
                    f'--authfile={auth_file}',
                    f'docker://{source_pullspec}',
                ]
            )
        if manifests is None:
            self.runtime.logger.info('Retrying manifest inspection with oc without an OS filter')
            manifests = await inspect_manifest_list(
                ['oc', 'image', 'info', source_pullspec, '-o', 'json', f'--registry-config={auth_file}']
            )
        if manifests is None:
            raise RuntimeError(f'Unable to inspect platform manifests for {source_pullspec}')

        platforms = []
        seen_architectures = set()
        for manifest in manifests:
            architecture = manifest.get('platform', {}).get('architecture')
            digest = manifest.get('digest')
            if not architecture or not digest:
                continue
            if not re.fullmatch(r'[A-Za-z0-9_.-]+', architecture):
                raise ValueError(f'Unexpected architecture in manifest list: {architecture!r}')
            if architecture in seen_architectures:
                raise ValueError(f'Manifest list contains multiple entries for architecture {architecture!r}')
            seen_architectures.add(architecture)
            platforms.append((architecture, digest))

        if not platforms:
            raise RuntimeError(f'No architecture-specific manifests found for {source_pullspec}')

        source_repo = source_pullspec.split('@', 1)[0]
        temp_tags = []
        manifest_created = False
        try:
            for architecture, digest in platforms:
                temp_tag = f'{dest_tag}-labeled-{architecture}'
                temp_tags.append(temp_tag)
                platform_pullspec = f'{source_repo}@{digest}'
                temp_pullspec = f'{dest_repo}:{temp_tag}'
                self.runtime.logger.info(
                    'Labeling %s platform image %s into %s', architecture, platform_pullspec, temp_pullspec
                )
                await cmd_assert_async(
                    [
                        'oc',
                        'image',
                        'append',
                        f'--from={platform_pullspec}',
                        f'--to={temp_pullspec}',
                        f'--image={label_patch}',
                        f'--registry-config={auth_file}',
                    ]
                )

            self.runtime.logger.info('Creating destination manifest list %s', dest)
            await cmd_assert_async(['buildah', 'manifest', 'rm', dest], check=False)
            await cmd_assert_async(['buildah', 'manifest', 'create', dest])
            manifest_created = True
            for architecture, _ in platforms:
                temp_pullspec = f'{dest_repo}:{dest_tag}-labeled-{architecture}'
                self.runtime.logger.info('Adding %s platform image %s to %s', architecture, temp_pullspec, dest)
                await cmd_assert_async(
                    ['buildah', 'manifest', 'add', f'--authfile={auth_file}', dest, f'docker://{temp_pullspec}']
                )

            self.runtime.logger.info('Pushing manifest list %s', dest)
            await cmd_assert_async(
                ['buildah', 'manifest', 'push', '--all', f'--authfile={auth_file}', dest, f'docker://{dest}']
            )
        finally:
            if manifest_created:
                try:
                    await cmd_assert_async(['buildah', 'manifest', 'rm', dest])
                except Exception as error:
                    self.runtime.logger.warning('Could not remove local buildah manifest %s: %s', dest, error)
            for temp_tag in temp_tags:
                temp_pullspec = f'{dest_repo}:{temp_tag}'
                self.runtime.logger.info('Cleaning up temporary platform tag %s', temp_pullspec)
                try:
                    await cmd_assert_async(['skopeo', 'delete', f'--authfile={auth_file}', f'docker://{temp_pullspec}'])
                except Exception as error:
                    self.runtime.logger.warning(
                        'Could not remove temporary platform tag %s; it may remain in the registry: %s',
                        temp_pullspec,
                        error,
                    )

        self.runtime.logger.info('Labeled image available at %s', dest)
        return dest

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
                # Extract the build timestamp from the NVR tag on the source node image.
                # Returns None when the release label is absent or has an unexpected
                # format (e.g. older groups that predate the label convention).
                timestamp = await self._extract_timestamp_from_nvr(self.node_image, merged_auth_file)

                node_tags = [tags['node']]
                extensions_tags = [tags['extensions']]
                node_pullspec = self.node_image
                node_labeled_tags = set()

                if timestamp:
                    # Build timestamped tags from the release stream and extracted timestamp
                    node_timestamped_tag = f'{self.release}-{timestamp}-node-image'
                    extensions_timestamped_tag = f'{self.release}-{timestamp}-node-image-extensions'
                    self.runtime.logger.info(
                        'Timestamped tags from NVR: node=%s, extensions=%s',
                        node_timestamped_tag,
                        extensions_timestamped_tag,
                    )

                    node_pullspec = await self._apply_manifest_list_tag_label(
                        source_pullspec=self.node_image,
                        dest_repo=KONFLUX_DEFAULT_IMAGE_REPO,
                        dest_tag=node_timestamped_tag,
                        label_value=node_timestamped_tag,
                        auth_file=merged_auth_file,
                    )
                    node_pullspec = await self._resolve_image_digest_pullspec(node_pullspec, merged_auth_file)
                    node_tags.append(node_timestamped_tag)
                    node_labeled_tags.add(node_timestamped_tag)
                    extensions_tags.append(extensions_timestamped_tag)
                else:
                    self.runtime.logger.info('No timestamp available; promoting with floating tags only')

                # Sync the labeled node image to art-images unless it is already there
                node_repo = node_pullspec.split('@', 1)[0]
                if ':' in node_repo.rsplit('/', 1)[-1]:
                    node_repo = node_repo.rsplit(':', 1)[0]
                if node_repo == KONFLUX_DEFAULT_IMAGE_REPO:
                    self.runtime.logger.info(
                        'Skipping sync for %s; image is already in destination repository %s',
                        node_pullspec,
                        KONFLUX_DEFAULT_IMAGE_REPO,
                    )
                    for tag in node_tags:
                        if tag in node_labeled_tags:
                            continue

                        destination = f'{KONFLUX_DEFAULT_IMAGE_REPO}:{tag}'
                        self.runtime.logger.info(
                            'Mirroring labeled image from %s to remaining tag %s', node_pullspec, destination
                        )
                        await cmd_gather_async(
                            [
                                'oc',
                                'image',
                                'mirror',
                                '--keep-manifest-list',
                                node_pullspec,
                                destination,
                                f'--registry-config={merged_auth_file}',
                            ],
                            check=True,
                        )
                else:
                    await sync_to_quay(node_pullspec, KONFLUX_DEFAULT_IMAGE_REPO, node_tags)

                # Sync the extensions image to art-images
                await sync_to_quay(self.extensions_image, KONFLUX_DEFAULT_IMAGE_REPO, extensions_tags)
            finally:
                if previous_auth_file is None:
                    os.environ.pop('QUAY_AUTH_FILE', None)
                else:
                    os.environ['QUAY_AUTH_FILE'] = previous_auth_file

    def _build_description(self, integration_test: dict, tags: Optional[Dict[str, str]] = None) -> str:
        """Build the Jenkins description for the integration test and promotion."""

        integration_url = html.escape(integration_test.get('url', ''), quote=True)
        integration_result = html.escape(str(integration_test.get('result', 'UNKNOWN')))
        integration_job = f'<a href="{integration_url}">build-node-image</a>' if integration_url else 'build-node-image'
        description = (
            f'RHCOS integration test: {integration_job} (result: {integration_result})<br/>'
            f'Input node image: <code>{html.escape(self.node_image)}</code><br/>'
            f'Input extensions image: <code>{html.escape(self.extensions_image)}</code><br/>'
        )
        if tags:
            description += (
                'Updated pullspecs:<br/>'
                f'<code>{html.escape(KONFLUX_DEFAULT_IMAGE_REPO)}:{html.escape(tags["node"])}</code><br/>'
                f'<code>{html.escape(KONFLUX_DEFAULT_IMAGE_REPO)}:{html.escape(tags["extensions"])}</code><br/>'
            )
        return description

    def _update_build_description(self, description: str):
        """Update the Jenkins description without changing the build result if it fails."""

        try:
            jenkins.init_jenkins()
            jenkins.update_description(description)
        except Exception:
            self.runtime.logger.warning('Unable to update the Jenkins build description', exc_info=True)

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
        result = client.wait_for_build('build-node-image', build_number, timeout=self._INTEGRATION_TEST_TIMEOUT)
        if result['result'] != 'SUCCESS':
            self._update_build_description(self._build_description(result))
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
        self._update_build_description(self._build_description(result, tags))


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
