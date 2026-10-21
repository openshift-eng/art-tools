import hashlib
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_REPO, RHCOS_IMAGE_REPO
from pyartcd.pipelines.rhcos_node_image_post_build import RhcosNodeImagePostBuildPipeline


def _make_pipeline(dry_run=False):
    runtime = MagicMock()
    runtime.dry_run = dry_run
    return RhcosNodeImagePostBuildPipeline(
        runtime=runtime,
        release='5.0-9.8',
        node_image=f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
        extensions_image=f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}',
    )


class TestRhcosNodeImagePostBuildPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.group_config = {
            'rhcos': {
                'payload_tags': [
                    {
                        'name': 'rhel-coreos',
                        'rhel_version': '9.8',
                        'rhcos_index_tag': 'quay.io/openshift-release-dev/ocp-v4.0-art-dev:5.0-9.8-node-image',
                    },
                    {
                        'name': 'rhel-coreos-extensions',
                        'rhel_version': '9.8',
                        'rhcos_index_tag': (
                            'quay.io/openshift-release-dev/ocp-v4.0-art-dev:5.0-9.8-node-image-extensions'
                        ),
                    },
                ]
            }
        }

    # ---- _extract_timestamp_from_nvr ----

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock)
    async def test_extract_timestamp_from_nvr_success(self, mock_cmd_gather):
        pipeline = _make_pipeline()
        oc_image_info_output = json.dumps(
            {'config': {'config': {'Labels': {'release': '202610141234.p0.g1234abc.assembly.stream.el9'}}}}
        )
        mock_cmd_gather.return_value = (0, oc_image_info_output, '')

        result = await pipeline._extract_timestamp_from_nvr(
            f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
            '/fake/auth.json',
        )

        self.assertEqual(result, '202610141234')
        cmd = mock_cmd_gather.call_args.args[0]
        self.assertIn('oc', cmd)
        self.assertIn('image', cmd)
        self.assertIn('info', cmd)
        self.assertIn(f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}', cmd)
        self.assertIn('--filter-by-os=linux/amd64', cmd)
        self.assertIn('--registry-config=/fake/auth.json', cmd)

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock)
    async def test_extract_timestamp_from_nvr_no_release_label(self, mock_cmd_gather):
        pipeline = _make_pipeline()
        oc_image_info_output = json.dumps({'config': {'config': {'Labels': {}}}})
        mock_cmd_gather.return_value = (0, oc_image_info_output, '')

        result = await pipeline._extract_timestamp_from_nvr(
            f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
            '/fake/auth.json',
        )

        self.assertIsNone(result)
        pipeline.runtime.logger.warning.assert_called_once()
        self.assertIn('no release label', pipeline.runtime.logger.warning.call_args.args[0])

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock)
    async def test_extract_timestamp_from_nvr_bad_release_label(self, mock_cmd_gather):
        pipeline = _make_pipeline()
        oc_image_info_output = json.dumps({'config': {'config': {'Labels': {'release': 'not-a-timestamp'}}}})
        mock_cmd_gather.return_value = (0, oc_image_info_output, '')

        result = await pipeline._extract_timestamp_from_nvr(
            f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
            '/fake/auth.json',
        )

        self.assertIsNone(result)
        pipeline.runtime.logger.warning.assert_called_once()
        self.assertIn('Could not extract timestamp', pipeline.runtime.logger.warning.call_args.args[0])

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock)
    async def test_apply_manifest_list_tag_label_mutates_whole_list_and_publishes_digest(self, mock_cmd_gather):
        pipeline = _make_pipeline()
        source_pullspec = pipeline.node_image
        raw_manifest = (
            json.dumps(
                {
                    'schemaVersion': 2,
                    'manifests': [
                        {'digest': f'sha256:{"1" * 64}', 'platform': {'os': 'linux', 'architecture': 'amd64'}},
                        {
                            'digest': f'sha256:{"2" * 64}',
                            'platform': {'os': 'linux', 'architecture': 'arm64', 'variant': 'v8'},
                        },
                    ],
                }
            )
            + '\n'
        )
        mock_cmd_gather.side_effect = [(0, '', ''), (0, raw_manifest, ''), (0, '', '')]

        labeled_pullspec = await pipeline._apply_manifest_list_tag_label(
            source_pullspec, KONFLUX_DEFAULT_IMAGE_REPO, 'tag', 'tag', '/fake/auth.json'
        )

        expected_pullspec = f'{KONFLUX_DEFAULT_IMAGE_REPO}@sha256:{hashlib.sha256(raw_manifest.encode()).hexdigest()}'
        self.assertEqual(labeled_pullspec, expected_pullspec)
        self.assertEqual(mock_cmd_gather.await_count, 3)
        append_cmd, inspect_cmd, mirror_cmd = [await_call.args[0] for await_call in mock_cmd_gather.await_args_list]
        self.assertEqual(append_cmd[:3], ['oc', 'image', 'append'])
        self.assertIn(f'--from={source_pullspec}', append_cmd)
        self.assertIn('--keep-manifest-list', append_cmd)
        self.assertFalse(any(argument.startswith('--filter-by-os') for argument in append_cmd))
        image_patch = next(argument.split('=', 1)[1] for argument in append_cmd if argument.startswith('--image='))
        self.assertEqual(json.loads(image_patch), {'Labels': {'coreos.build.manifest-list-tag': 'tag'}})
        self.assertIn('--registry-config=/fake/auth.json', append_cmd)
        staging_pullspec = next(argument.split('=', 1)[1] for argument in append_cmd if argument.startswith('--to='))
        self.assertRegex(staging_pullspec, rf'^{KONFLUX_DEFAULT_IMAGE_REPO}:rhcos-label-[0-9a-f]{{32}}$')
        self.assertEqual(
            inspect_cmd, ['skopeo', 'inspect', '--raw', '--authfile=/fake/auth.json', f'docker://{staging_pullspec}']
        )
        self.assertEqual(
            mirror_cmd,
            [
                'oc',
                'image',
                'mirror',
                '--keep-manifest-list',
                expected_pullspec,
                f'{KONFLUX_DEFAULT_IMAGE_REPO}:tag',
                '--registry-config=/fake/auth.json',
            ],
        )
        self.assertTrue(all(await_call.kwargs == {'check': True} for await_call in mock_cmd_gather.await_args_list))

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock)
    async def test_apply_manifest_list_tag_label_uses_unique_staging_tags(self, mock_cmd_gather):
        pipeline = _make_pipeline()
        raw_manifest = json.dumps({'manifests': [{'digest': f'sha256:{"1" * 64}'}]})
        mock_cmd_gather.side_effect = [(0, '', ''), (0, raw_manifest, ''), (0, '', '')] * 2
        for _ in range(2):
            await pipeline._apply_manifest_list_tag_label(
                pipeline.node_image, KONFLUX_DEFAULT_IMAGE_REPO, 'tag', 'tag', '/fake/auth.json'
            )
        appends = [
            await_call.args[0]
            for await_call in mock_cmd_gather.await_args_list
            if await_call.args[0][:3] == ['oc', 'image', 'append']
        ]
        staging_destinations = [
            next(argument for argument in command if argument.startswith('--to=')) for command in appends
        ]
        self.assertEqual(len(set(staging_destinations)), 2)

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock)
    async def test_apply_manifest_list_tag_label_rejects_invalid_output_before_publication(self, mock_cmd_gather):
        pipeline = _make_pipeline()
        for raw_manifest in ('not JSON', '[]', '{}', '{"manifests":[]}', '{"manifests":"not a list"}'):
            with self.subTest(raw_manifest=raw_manifest):
                mock_cmd_gather.reset_mock()
                mock_cmd_gather.side_effect = [(0, '', ''), (0, raw_manifest, '')]
                with self.assertRaises(ValueError):
                    await pipeline._apply_manifest_list_tag_label(
                        pipeline.node_image, KONFLUX_DEFAULT_IMAGE_REPO, 'tag', 'tag', '/fake/auth.json'
                    )
                self.assertEqual(mock_cmd_gather.await_count, 2)

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock)
    async def test_apply_manifest_list_tag_label_propagates_each_command_failure(self, mock_cmd_gather):
        pipeline = _make_pipeline()
        raw_manifest = json.dumps({'manifests': [{'digest': f'sha256:{"1" * 64}'}]})
        successes = [(0, '', ''), (0, raw_manifest, '')]
        for failed_stage in range(3):
            with self.subTest(failed_stage=failed_stage):
                mock_cmd_gather.reset_mock()
                mock_cmd_gather.side_effect = successes[:failed_stage] + [ChildProcessError('command failed')]
                with self.assertRaisesRegex(ChildProcessError, 'command failed'):
                    await pipeline._apply_manifest_list_tag_label(
                        pipeline.node_image, KONFLUX_DEFAULT_IMAGE_REPO, 'tag', 'tag', '/fake/auth.json'
                    )
                self.assertEqual(mock_cmd_gather.await_count, failed_stage + 1)
                commands = [await_call.args[0] for await_call in mock_cmd_gather.await_args_list]
                self.assertFalse(
                    any(command[0] == 'buildah' or command[:2] == ['skopeo', 'delete'] for command in commands)
                )

    # ---- full pipeline integration test ----

    @patch.dict(
        'os.environ',
        {
            'QUAY_AUTH_FILE': '/tmp/quay-auth.json',
            'RHCOS_QUAY_AUTH_FILE': '/tmp/rhcos-quay-auth.json',
        },
        clear=True,
    )
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RegistryConfig')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.load_group_config', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.update_description')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RhcosJenkinsClient')
    async def test_tests_then_skips_sync_for_labeled_image_already_in_destination_repo(
        self,
        mock_client_type,
        mock_update_description,
        mock_init_jenkins,
        mock_load_group_config,
        mock_sync,
        mock_registry_config,
    ):
        mock_load_group_config.return_value = self.group_config
        mock_client = mock_client_type.return_value
        mock_client.trigger_build.return_value = 46173
        mock_client.wait_for_build.return_value = {
            'result': 'SUCCESS',
            'url': 'https://jenkins.example.com/job/build-node-image/46173/',
        }
        mock_registry_config.return_value.__enter__.return_value = '/tmp/merged-auth.json'

        pipeline = _make_pipeline()

        labeled_node_digest_pullspec = f'{KONFLUX_DEFAULT_IMAGE_REPO}@sha256:{"c" * 64}'
        labeled_extensions_digest_pullspec = f'{KONFLUX_DEFAULT_IMAGE_REPO}@sha256:{"d" * 64}'
        with (
            patch.object(
                pipeline, '_extract_timestamp_from_nvr', new_callable=AsyncMock, return_value='202610141234'
            ) as mock_extract,
            patch.object(
                pipeline,
                '_apply_manifest_list_tag_label',
                new_callable=AsyncMock,
                side_effect=[labeled_node_digest_pullspec, labeled_extensions_digest_pullspec],
            ) as mock_apply_label,
            patch(
                'pyartcd.pipelines.rhcos_node_image_post_build.cmd_gather_async', new_callable=AsyncMock
            ) as mock_cmd_gather,
        ):
            await pipeline.run()

            # Verify the NVR timestamp was extracted from the node image digest
            mock_extract.assert_awaited_once_with(
                f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
                '/tmp/merged-auth.json',
            )
            mock_apply_label.assert_has_awaits(
                [
                    call(
                        source_pullspec=f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
                        dest_repo=KONFLUX_DEFAULT_IMAGE_REPO,
                        dest_tag='5.0-9.8-202610141234-node-image',
                        label_value='5.0-9.8-202610141234-node-image',
                        auth_file='/tmp/merged-auth.json',
                    ),
                    call(
                        source_pullspec=f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}',
                        dest_repo=KONFLUX_DEFAULT_IMAGE_REPO,
                        dest_tag='5.0-9.8-202610141234-node-image-extensions',
                        label_value='5.0-9.8-202610141234-node-image-extensions',
                        auth_file='/tmp/merged-auth.json',
                    ),
                ]
            )
            self.assertEqual(mock_cmd_gather.await_count, 4)
            mock_cmd_gather.assert_any_await(
                [
                    'oc',
                    'image',
                    'mirror',
                    '--keep-manifest-list',
                    labeled_node_digest_pullspec,
                    f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image',
                    '--registry-config=/tmp/merged-auth.json',
                ],
                check=True,
            )
            mock_cmd_gather.assert_any_await(
                [
                    'oc',
                    'image',
                    'mirror',
                    '--keep-manifest-list',
                    labeled_extensions_digest_pullspec,
                    f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image-extensions',
                    '--registry-config=/tmp/merged-auth.json',
                ],
                check=True,
            )
            for pullspec, digest in (
                (labeled_node_digest_pullspec, 'c' * 64),
                (labeled_extensions_digest_pullspec, 'd' * 64),
            ):
                mock_cmd_gather.assert_any_await(
                    [
                        'oc',
                        'image',
                        'mirror',
                        '--keep-manifest-list',
                        pullspec,
                        f'{KONFLUX_DEFAULT_IMAGE_REPO}:sha256-{digest}',
                        '--registry-config=/tmp/merged-auth.json',
                    ],
                    check=True,
                )
            self.assertEqual(
                [await_call.args[0][5] for await_call in mock_cmd_gather.await_args_list],
                [
                    f'{KONFLUX_DEFAULT_IMAGE_REPO}:sha256-{"c" * 64}',
                    f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image',
                    f'{KONFLUX_DEFAULT_IMAGE_REPO}:sha256-{"d" * 64}',
                    f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image-extensions',
                ],
            )

        mock_client.trigger_build.assert_called_once_with(
            'build-node-image',
            {
                'RELEASE': '5.0-9.8',
                'NODE_IMAGE': f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
                'EXTENSIONS_IMAGE': f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}',
            },
        )
        mock_client.wait_for_build.assert_called_once_with('build-node-image', 46173, timeout=90 * 60)

        # Both labeled images are already in art-images; only their floating tags need mirroring.
        self.assertEqual(mock_sync.await_count, 0)
        pipeline.runtime.logger.info.assert_any_call(
            'Skipping sync for %s; image is already in destination repository %s',
            labeled_node_digest_pullspec,
            KONFLUX_DEFAULT_IMAGE_REPO,
        )
        pipeline.runtime.logger.info.assert_any_call(
            'Mirroring labeled image from %s to remaining tag %s',
            labeled_node_digest_pullspec,
            f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image',
        )
        pipeline.runtime.logger.info.assert_any_call(
            'Skipping sync for %s; image is already in destination repository %s',
            labeled_extensions_digest_pullspec,
            KONFLUX_DEFAULT_IMAGE_REPO,
        )
        pipeline.runtime.logger.info.assert_any_call(
            'Mirroring labeled image from %s to remaining tag %s',
            labeled_extensions_digest_pullspec,
            f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image-extensions',
        )
        mock_init_jenkins.assert_called_once()
        mock_update_description.assert_called_once()
        description = mock_update_description.call_args.args[0]
        self.assertIn('https://jenkins.example.com/job/build-node-image/46173/', description)
        self.assertIn(f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image', description)
        self.assertIn(f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image-extensions', description)
        self.assertIn(f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}', description)
        self.assertIn(f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}', description)

    @patch.dict(
        'os.environ',
        {
            'QUAY_AUTH_FILE': '/tmp/quay-auth.json',
            'RHCOS_QUAY_AUTH_FILE': '/tmp/rhcos-quay-auth.json',
        },
        clear=True,
    )
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RegistryConfig')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.load_group_config', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.update_description')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RhcosJenkinsClient')
    async def test_promotes_with_floating_tags_only_when_timestamp_missing(
        self,
        mock_client_type,
        mock_update_description,
        mock_init_jenkins,
        mock_load_group_config,
        mock_sync,
        mock_registry_config,
    ):
        """When _extract_timestamp_from_nvr returns None (e.g. older groups without a
        release label), promotion should still succeed using only floating tags."""

        mock_load_group_config.return_value = self.group_config
        mock_client = mock_client_type.return_value
        mock_client.trigger_build.return_value = 46173
        mock_client.wait_for_build.return_value = {
            'result': 'SUCCESS',
            'url': 'https://jenkins.example.com/job/build-node-image/46173/',
        }
        mock_registry_config.return_value.__enter__.return_value = '/tmp/merged-auth.json'

        pipeline = _make_pipeline()

        # Simulate a missing release label by returning None
        with (
            patch.object(pipeline, '_extract_timestamp_from_nvr', new_callable=AsyncMock, return_value=None),
            patch.object(pipeline, '_apply_manifest_list_tag_label', new_callable=AsyncMock) as mock_apply_label,
        ):
            await pipeline.run()
            mock_apply_label.assert_not_awaited()

        # Node image should be synced with only the floating tag (no timestamped tag)
        node_sync_call = mock_sync.call_args_list[0]
        self.assertEqual(node_sync_call.args[0], f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}')
        self.assertEqual(node_sync_call.args[1], KONFLUX_DEFAULT_IMAGE_REPO)
        node_tags = node_sync_call.args[2]
        self.assertEqual(node_tags, ['5.0-9.8-node-image'])

        # Extensions image should be synced with only the floating tag
        ext_sync_call = mock_sync.call_args_list[1]
        self.assertEqual(ext_sync_call.args[0], f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}')
        self.assertEqual(ext_sync_call.args[1], KONFLUX_DEFAULT_IMAGE_REPO)
        ext_tags = ext_sync_call.args[2]
        self.assertEqual(ext_tags, ['5.0-9.8-node-image-extensions'])

        self.assertEqual(mock_sync.await_count, 2)
        pipeline.runtime.logger.info.assert_any_call('No timestamp available; promoting with floating tags only')

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RhcosJenkinsClient')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.update_description')
    async def test_failed_integration_test_does_not_promote(
        self, mock_update_description, mock_init_jenkins, mock_client_type, mock_sync
    ):
        mock_client = mock_client_type.return_value
        mock_client.trigger_build.return_value = 46173
        mock_client.wait_for_build.return_value = {
            'result': 'FAILURE',
            'url': 'https://jenkins.example.com/job/build-node-image/46173/',
        }

        with self.assertRaisesRegex(RuntimeError, 'integration test failed'):
            await _make_pipeline().run()

        mock_sync.assert_not_awaited()
        mock_init_jenkins.assert_called_once()
        mock_update_description.assert_called_once()
        self.assertIn(
            'https://jenkins.example.com/job/build-node-image/46173/', mock_update_description.call_args.args[0]
        )

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.update_description')
    def test_description_update_failure_does_not_fail_pipeline(self, mock_update_description, mock_init_jenkins):
        mock_update_description.side_effect = RuntimeError('description update failed')
        pipeline = _make_pipeline()

        pipeline._update_build_description('description')

        mock_init_jenkins.assert_called_once()
        mock_update_description.assert_called_once_with('description')
        pipeline.runtime.logger.warning.assert_called_once()

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RhcosJenkinsClient')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.sync_to_quay', new_callable=AsyncMock)
    async def test_dry_run_does_not_trigger_or_promote(self, mock_sync, mock_client_type):
        await _make_pipeline(dry_run=True).run()

        mock_client_type.assert_not_called()
        mock_sync.assert_not_awaited()

    async def test_rejects_mutable_pullspec(self):
        pipeline = _make_pipeline()
        pipeline.node_image = f'{RHCOS_IMAGE_REPO}:latest'

        with self.assertRaisesRegex(ValueError, 'NODE_IMAGE'):
            await pipeline.run()


if __name__ == '__main__':
    unittest.main()
