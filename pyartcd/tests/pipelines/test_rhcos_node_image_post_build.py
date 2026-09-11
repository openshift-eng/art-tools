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
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.update_description')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RhcosJenkinsClient')
    async def test_tests_then_promotes_exact_digests_with_configured_tags(
        self,
        mock_client_type,
        mock_update_description,
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

        await pipeline.run()

        mock_client.trigger_build.assert_called_once_with(
            'build-node-image',
            {
                'RELEASE': '5.0-9.8',
                'NODE_IMAGE': f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
                'EXTENSIONS_IMAGE': f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}',
            },
        )
        mock_client.wait_for_build.assert_called_once_with('build-node-image', 46173)
        mock_sync.assert_any_await(
            f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
            KONFLUX_DEFAULT_IMAGE_REPO,
            ['5.0-9.8-node-image'],
        )
        mock_sync.assert_any_await(
            f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}',
            KONFLUX_DEFAULT_IMAGE_REPO,
            ['5.0-9.8-node-image-extensions'],
        )
        self.assertEqual(mock_sync.await_count, 2)
        mock_update_description.assert_called_once()
        description = mock_update_description.call_args.args[0]
        self.assertIn('https://jenkins.example.com/job/build-node-image/46173/', description)
        self.assertIn(f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image', description)
        self.assertIn(f'{KONFLUX_DEFAULT_IMAGE_REPO}:5.0-9.8-node-image-extensions', description)
        self.assertIn(f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}', description)
        self.assertIn(f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}', description)

    @patch('pyartcd.pipelines.rhcos_node_image_post_build.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.RhcosJenkinsClient')
    @patch('pyartcd.pipelines.rhcos_node_image_post_build.jenkins.update_description')
    async def test_failed_integration_test_does_not_promote(self, mock_update_description, mock_client_type, mock_sync):
        mock_client = mock_client_type.return_value
        mock_client.trigger_build.return_value = 46173
        mock_client.wait_for_build.return_value = {
            'result': 'FAILURE',
            'url': 'https://jenkins.example.com/job/build-node-image/46173/',
        }

        with self.assertRaisesRegex(RuntimeError, 'integration test failed'):
            await _make_pipeline().run()

        mock_sync.assert_not_awaited()
        mock_update_description.assert_called_once()
        self.assertIn(
            'https://jenkins.example.com/job/build-node-image/46173/', mock_update_description.call_args.args[0]
        )

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
