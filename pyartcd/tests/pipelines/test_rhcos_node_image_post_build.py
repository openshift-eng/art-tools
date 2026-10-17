import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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
    async def test_tests_then_promotes_exact_digests_with_configured_tags(
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

        # Mock _extract_timestamp_from_nvr to return a known timestamp
        with patch.object(
            pipeline, '_extract_timestamp_from_nvr', new_callable=AsyncMock, return_value='202610141234'
        ) as mock_extract:
            await pipeline.run()

            # Verify the NVR timestamp was extracted from the node image digest
            mock_extract.assert_awaited_once_with(
                f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
                '/tmp/merged-auth.json',
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

        # Node image should be synced directly (no label mutation) with both tags
        node_sync_call = mock_sync.call_args_list[0]
        self.assertEqual(node_sync_call.args[0], f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}')
        self.assertEqual(node_sync_call.args[1], KONFLUX_DEFAULT_IMAGE_REPO)
        node_tags = node_sync_call.args[2]
        self.assertEqual(len(node_tags), 2)
        self.assertEqual(node_tags[0], '5.0-9.8-node-image')
        self.assertEqual(node_tags[1], '5.0-9.8-202610141234-node-image')

        # Extensions image should be synced directly with both tags
        ext_sync_call = mock_sync.call_args_list[1]
        self.assertEqual(ext_sync_call.args[0], f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}')
        self.assertEqual(ext_sync_call.args[1], KONFLUX_DEFAULT_IMAGE_REPO)
        ext_tags = ext_sync_call.args[2]
        self.assertEqual(len(ext_tags), 2)
        self.assertEqual(ext_tags[0], '5.0-9.8-node-image-extensions')
        self.assertEqual(ext_tags[1], '5.0-9.8-202610141234-node-image-extensions')

        self.assertEqual(mock_sync.await_count, 2)
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
        with patch.object(pipeline, '_extract_timestamp_from_nvr', new_callable=AsyncMock, return_value=None):
            await pipeline.run()

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
