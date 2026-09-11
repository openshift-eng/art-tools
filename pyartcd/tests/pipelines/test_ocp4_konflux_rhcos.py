#!/usr/bin/env python3

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.constants import RHCOS_IMAGE_REPO
from artcommonlib.util import run_safe
from pyartcd.jenkins import Jobs
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


class TestRhcosPostBuildDelegation(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.pipeline = _make_pipeline()
        self.records = [
            {
                'name': 'rhcos-node-image',
                'status': '0',
                'image_pullspec': f'{RHCOS_IMAGE_REPO}@sha256:{"a" * 64}',
            },
            {
                'name': 'rhcos-node-extensions',
                'status': '0',
                'image_pullspec': f'{RHCOS_IMAGE_REPO}@sha256:{"b" * 64}',
            },
            {
                'name': 'rhcos-node-image-rhel10',
                'status': '0',
                'image_pullspec': f'{RHCOS_IMAGE_REPO}@sha256:{"c" * 64}',
            },
            {
                'name': 'rhcos-node-extensions-rhel10',
                'status': '0',
                'image_pullspec': f'{RHCOS_IMAGE_REPO}@sha256:{"d" * 64}',
            },
        ]
        self.group_config = {
            'rhcos': {
                'payload_tags': [
                    {'rhel_version': '9.8'},
                    {'rhel_version': '10.0'},
                ]
            },
            'vars': {'RHCOS_EL_MAJOR': '9', 'RHCOS_EL_MINOR': '8'},
        }

    @patch.dict(os.environ, {'ART_TOOLS_COMMIT': 'locriandev@feature/art-23426'}, clear=False)
    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.update_description')
    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.start_build')
    async def test_child_job_receives_exact_pullspecs_and_parameters(
        self,
        mock_start_build,
        mock_update_description,
    ):
        mock_start_build.return_value = 'SUCCESS'

        result = await self.pipeline._trigger_rhcos_pair_test(
            'rhel9',
            ('rhcos-node-image', 'rhcos-node-extensions'),
            {'rhcos-node-image': self.records[0]['image_pullspec']},
            {record['name']: record['image_pullspec'] for record in self.records},
            {'rhel9': '4.21-9.8'},
        )

        params = mock_start_build.call_args.args[1]
        self.assertEqual(params['ART_TOOLS_COMMIT'], 'locriandev@feature/art-23426')
        self.assertEqual(params['RELEASE'], '4.21-9.8')
        self.assertEqual(params['NODE_IMAGE'], self.records[0]['image_pullspec'])
        self.assertEqual(params['EXTENSIONS_IMAGE'], self.records[1]['image_pullspec'])
        self.assertFalse(params['DRY_RUN'])
        mock_start_build.assert_called_once_with(
            Jobs.RHCOS_NODE_IMAGE_POST_BUILD,
            params,
            block_until_complete=True,
        )
        self.assertEqual(result['NODE_IMAGE'], params['NODE_IMAGE'])
        mock_update_description.assert_called_once()
        description = mock_update_description.call_args.args[0]
        self.assertIn('rhcos-node-image-post-build', description)
        self.assertIn('/job/aos-cd-builds/job/build%252Frhcos-node-image-post-build/', description)
        self.assertNotIn('EXTENSIONS_IMAGE', description)

    @patch('pyartcd.pipelines.ocp4_konflux.load_group_config', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.update_description')
    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.start_build')
    async def test_pairs_are_independent_and_failures_mark_parent_unstable(
        self,
        mock_start_build,
        _mock_update_description,
        mock_load_group_config,
    ):
        mock_load_group_config.return_value = self.group_config
        mock_start_build.side_effect = ['SUCCESS', 'FAILURE']
        self.pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': self.records})

        critical_failures = []
        with self.assertLogs('pyartcd.pipelines.ocp4_konflux', level='WARNING') as logs:
            await run_safe(self.pipeline.trigger_rhcos_integration_tests, critical_failures)

        self.assertEqual(mock_start_build.call_count, 2)
        self.assertEqual([name for name, _ in critical_failures], ['trigger_rhcos_integration_tests'])
        self.assertTrue(any('RHCOS rhel10 integration test failed' in message for message in logs.output))
        self.assertEqual(
            mock_start_build.call_args_list[0].args[1]['NODE_IMAGE'],
            self.records[0]['image_pullspec'],
        )
        self.assertEqual(
            mock_start_build.call_args_list[1].args[1]['EXTENSIONS_IMAGE'],
            self.records[3]['image_pullspec'],
        )

    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.start_build')
    async def test_dry_run_records_no_child_build(self, mock_start_build):
        self.pipeline.runtime.dry_run = True

        await self.pipeline._trigger_rhcos_pair_test(
            'rhel9',
            ('rhcos-node-image', 'rhcos-node-extensions'),
            {'rhcos-node-image': self.records[0]['image_pullspec']},
            {record['name']: record['image_pullspec'] for record in self.records},
            {'rhel9': '4.21-9.8'},
        )

        mock_start_build.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.start_build')
    async def test_non_stream_assembly_skips_rhcos_integration_tests(self, mock_start_build):
        self.pipeline.assembly = 'test'
        self.pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': self.records})

        await self.pipeline.trigger_rhcos_integration_tests()

        mock_start_build.assert_not_called()
        self.pipeline.parse_record_log.assert_not_called()


if __name__ == '__main__':
    unittest.main()
