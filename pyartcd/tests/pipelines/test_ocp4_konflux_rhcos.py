#!/usr/bin/env python3

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_SHARE_REPO, RHCOS_IMAGE_REPO
from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline, ocp4


def _make_pipeline(assembly='stream', version='4.21'):
    runtime = MagicMock()
    runtime.doozer_working = '/tmp/doozer-working'
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

    def test_rhcos_integration_tests_run_by_default(self):
        with ocp4.make_context('beta:ocp4-konflux', self.required_args) as context:
            self.assertFalse(context.params['skip_rhcos_integration_tests'])

        self.assertFalse(_make_pipeline().skip_rhcos_integration_tests)

    def test_rhcos_integration_tests_can_be_skipped(self):
        args = [*self.required_args, '--skip-rhcos-integration-tests']

        with ocp4.make_context('beta:ocp4-konflux', args) as context:
            self.assertTrue(context.params['skip_rhcos_integration_tests'])

    def test_skip_rhcos_integration_tests_is_a_one_way_flag(self):
        option = next(param for param in ocp4.params if param.name == 'skip_rhcos_integration_tests')

        self.assertEqual(option.opts, ['--skip-rhcos-integration-tests'])
        self.assertFalse(option.secondary_opts)


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


class TestRhcosIntegrationPullspecs(unittest.IsolatedAsyncioTestCase):
    async def test_rhcos_repo_pullspecs_are_rewritten_to_share_repo(self):
        pipeline = _make_pipeline()
        pipeline.rhcos_jenkins_client = MagicMock()
        pipeline.rhcos_jenkins_client.trigger_build.return_value = 123
        pipeline.rhcos_jenkins_client.wait_for_build.return_value = {
            'result': 'SUCCESS',
            'url': 'https://jenkins.example.com/job/build-node-image/123/',
        }
        node_digest = 'a' * 64
        extensions_digest = 'b' * 64

        await pipeline._trigger_rhcos_pair_test(
            'rhel9',
            pipeline.RHCOS_RHEL9_PAIR,
            {
                'rhcos-node-image': f'{RHCOS_IMAGE_REPO}@sha256:{node_digest}',
                'rhcos-node-extensions': f'{RHCOS_IMAGE_REPO}@sha256:{extensions_digest}',
            },
            {},
            {'rhel9': '4.21-9.8'},
        )

        parameters = pipeline.rhcos_jenkins_client.trigger_build.call_args.args[1]
        self.assertEqual(
            parameters['NODE_IMAGE'],
            f'{KONFLUX_DEFAULT_IMAGE_SHARE_REPO}@sha256:{node_digest}',
        )
        self.assertEqual(
            parameters['EXTENSIONS_IMAGE'],
            f'{KONFLUX_DEFAULT_IMAGE_SHARE_REPO}@sha256:{extensions_digest}',
        )


if __name__ == '__main__':
    unittest.main()
