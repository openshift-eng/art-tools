#!/usr/bin/env python3

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.constants import RHCOS_IMAGE_REPO
from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline, ocp4


def _make_pipeline(assembly='stream', version='4.21'):
    runtime = MagicMock()
    runtime.doozer_working = '/tmp/doozer-working'
    runtime.dry_run = False
    runtime.new_slack_client.return_value = MagicMock()
    with patch('pyartcd.pipelines.ocp4_konflux.util.default_release_suffix', return_value='202408190000'):
        return KonfluxOcpPipeline(
            runtime=runtime,
            assembly=assembly,
            version=version,
            image_build_strategy='all',
            rpm_build_strategy='none',
            build_priority='auto',
            data_path='https://github.com/openshift-eng/ocp-build-data',
        )


class TestRhcosIntegrationCli(unittest.TestCase):
    def setUp(self):
        self.required_args = [
            '--image-build-strategy=none',
            '--image-list=none',
            '--rpm-build-strategy=none',
            '--rpm-list=none',
            '--assembly=stream',
            '--version=4.21',
        ]

    def test_post_build_run_by_default(self):
        with ocp4.make_context('beta:ocp4-konflux', self.required_args) as context:
            self.assertFalse(context.params['skip_post_build'])

        self.assertFalse(_make_pipeline().skip_post_build)

    def test_post_build_can_be_skipped(self):
        args = [*self.required_args, '--skip-post-build']

        with ocp4.make_context('beta:ocp4-konflux', args) as context:
            self.assertTrue(context.params['skip_post_build'])


class TestRhcosPostBuildBuildCommand(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.pipelines.ocp4_konflux.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_skip_option_is_forwarded_to_doozer(self, cmd_assert):
        pipeline = _make_pipeline()
        pipeline.skip_post_build = True

        await pipeline.build_images()

        self.assertIn('--skip-post-build', cmd_assert.await_args.args[0])


class TestRegistryAuthConfiguration(unittest.IsolatedAsyncioTestCase):
    @patch.dict(
        os.environ,
        {
            'QUAY_AUTH_FILE': '/tmp/quay-auth.json',
            'KONFLUX_OPERATOR_INDEX_AUTH_FILE': '/tmp/redhat-registry-auth.json',
            'RHCOS_QUAY_AUTH_FILE': '/tmp/rhcos-quay-auth.json',
            'QCI_USER': 'qci-user',
            'QCI_PASSWORD': 'qci-password',
        },
        clear=True,
    )
    @patch('pyartcd.pipelines.ocp4_konflux.shutil.copy2')
    @patch('pyartcd.pipelines.ocp4_konflux.tempfile.mkdtemp', return_value='/tmp/docker-config-test')
    @patch('pyartcd.pipelines.ocp4_konflux.RegistryConfig')
    async def test_run_merges_rhcos_registry_credentials(
        self,
        mock_registry_config,
        _mock_mkdtemp,
        _mock_copy,
    ):
        mock_registry_config.return_value.__enter__.return_value = '/tmp/merged-auth.json'
        pipeline = _make_pipeline()
        pipeline._run_pipeline = AsyncMock()

        await pipeline.run()

        registry_config_args = mock_registry_config.call_args.kwargs
        self.assertEqual(
            registry_config_args['source_files'],
            ['/tmp/quay-auth.json', '/tmp/redhat-registry-auth.json', '/tmp/rhcos-quay-auth.json'],
        )
        self.assertIn(RHCOS_IMAGE_REPO, registry_config_args['registries'])
        pipeline._run_pipeline.assert_awaited_once_with()
        self.assertEqual(os.environ['QUAY_AUTH_FILE'], '/tmp/quay-auth.json')
        self.assertNotIn('DOCKER_CONFIG', os.environ)


if __name__ == '__main__':
    unittest.main()
