#!/usr/bin/env python3

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_REPO, KONFLUX_DEFAULT_IMAGE_SHARE_REPO, RHCOS_IMAGE_REPO
from artcommonlib.util import run_safe
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
    async def test_rhcos_repo_pullspecs_are_passed_to_build_node_image(self):
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
            f'{RHCOS_IMAGE_REPO}@sha256:{node_digest}',
        )
        self.assertEqual(
            parameters['EXTENSIONS_IMAGE'],
            f'{RHCOS_IMAGE_REPO}@sha256:{extensions_digest}',
        )

    @patch('pyartcd.pipelines.ocp4_konflux.load_group_config', new_callable=AsyncMock)
    async def test_rhel_pair_failures_are_independent_and_only_passing_pair_is_promotable(self, mock_load_group_config):
        pipeline = _make_pipeline()
        pipeline.rhcos_jenkins_client = MagicMock()
        pipeline.rhcos_jenkins_client.trigger_build.side_effect = [901, 1001]
        pipeline.rhcos_jenkins_client.wait_for_build.side_effect = [
            {'result': 'SUCCESS', 'url': 'https://jenkins.example.com/job/build-node-image/901/'},
            {
                'result': 'FAILURE',
                'url': 'https://jenkins.example.com/job/build-node-image/1001/',
                'description': 'RHEL 10 integration failure',
            },
        ]
        mock_load_group_config.return_value = {
            'rhcos': {
                'payload_tags': [
                    {'rhel_version': '9.8'},
                    {'rhel_version': '10.0'},
                ]
            },
            'vars': {'RHCOS_EL_MAJOR': '9', 'RHCOS_EL_MINOR': '8'},
        }

        records = []
        for name, digest, image_tag in (
            ('rhcos-node-image', 'a' * 64, 'rhcos-node-image-9.8.20260909'),
            ('rhcos-node-extensions', 'b' * 64, 'rhcos-node-extensions-9.8.20260909'),
            ('rhcos-node-image-rhel10', 'c' * 64, 'rhcos-node-image-rhel10-10.0.20260909'),
            ('rhcos-node-extensions-rhel10', 'd' * 64, 'rhcos-node-extensions-rhel10-10.0.20260909'),
        ):
            records.append(
                {
                    'name': name,
                    'status': '0',
                    'image_pullspec': f'{RHCOS_IMAGE_REPO}@sha256:{digest}',
                    'image_tag': image_tag,
                }
            )
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': records})

        critical_failures = []
        with self.assertLogs('pyartcd.pipelines.ocp4_konflux', level='WARNING') as logs:
            await run_safe(pipeline.trigger_rhcos_integration_tests, critical_failures)

        self.assertEqual([name for name, _ in critical_failures], ['trigger_rhcos_integration_tests'])
        self.assertEqual(pipeline.rhcos_jenkins_client.trigger_build.call_count, 2)
        self.assertTrue(any('RHCOS rhel10 integration test failed' in message for message in logs.output))
        self.assertEqual(
            pipeline._rhcos_promotable_pairs['rhel9']['rhcos-node-image']['pullspec'],
            f"{RHCOS_IMAGE_REPO}@sha256:{'a' * 64}",
        )
        self.assertEqual(
            pipeline._rhcos_promotable_pairs['rhel9']['rhcos-node-extensions']['image_tag'],
            'rhcos-node-extensions-9.8.20260909',
        )
        self.assertNotIn('rhel10', pipeline._rhcos_promotable_pairs)

        with patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock) as mock_sync:
            await pipeline.promote_rhcos_images()

        mock_sync.assert_has_awaits(
            [
                call(
                    f"{RHCOS_IMAGE_REPO}@sha256:{'a' * 64}",
                    KONFLUX_DEFAULT_IMAGE_REPO,
                    ['rhcos-node-image-9.8.20260909', 'rhcos-node-image-4.21'],
                ),
                call(
                    f"{RHCOS_IMAGE_REPO}@sha256:{'b' * 64}",
                    KONFLUX_DEFAULT_IMAGE_REPO,
                    ['rhcos-node-extensions-9.8.20260909', 'rhcos-node-extensions-4.21'],
                ),
            ]
        )
        self.assertEqual(mock_sync.await_count, 2)

    async def test_rhcos_promotion_skips_unrun_pairs(self):
        pipeline = _make_pipeline()
        pipeline.skip_rhcos_integration_tests = True
        pipeline._rhcos_promotable_pairs = {
            'rhel9': {
                'rhcos-node-image': {
                    'pullspec': f"{RHCOS_IMAGE_REPO}@sha256:{'a' * 64}",
                    'image_tag': 'rhcos-node-image-9.8.20260909',
                }
            }
        }

        with patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock) as mock_sync:
            await pipeline.promote_rhcos_images()

        mock_sync.assert_not_awaited()

    async def test_failed_or_unrun_rhcos_pairs_are_not_mirrored(self):
        pipeline = _make_pipeline()
        pipeline.parse_record_log = MagicMock(
            return_value={
                'image_build_konflux': [
                    {
                        'name': 'other-image',
                        'status': '0',
                        'nvrs': 'other-image-4.21-1',
                        'image_pullspec': 'quay.io/source/other-image@sha256:' + 'e' * 64,
                        'image_tag': 'other-image-4.21.0',
                    },
                    {
                        'name': 'rhcos-node-image',
                        'status': '0',
                        'nvrs': 'rhcos-node-image-4.21-1',
                        'image_pullspec': f"{RHCOS_IMAGE_REPO}@sha256:{'a' * 64}",
                        'image_tag': 'rhcos-node-image-9.8.20260909',
                    },
                    {
                        'name': 'rhcos-node-extensions',
                        'status': '1',
                        'nvrs': 'rhcos-node-extensions-4.21-1',
                    },
                ]
            }
        )

        with (
            patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock) as mock_sync,
            patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False),
        ):
            await pipeline.mirror_images()

            mock_sync.assert_awaited_once_with(
                'quay.io/source/other-image@sha256:' + 'e' * 64,
                KONFLUX_DEFAULT_IMAGE_SHARE_REPO,
                ['other-image-4.21.0', 'other-image-4.21'],
            )

            # No pair passed, so promotion must not add any RHCOS mirror calls.
            await pipeline.promote_rhcos_images()

        self.assertEqual(mock_sync.await_count, 1)


if __name__ == '__main__':
    unittest.main()
