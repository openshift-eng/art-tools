import io
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import click
from pyartcd.pipelines.seed_lockfile import SeedLockfilePipeline


class TestSeedLockfilePipeline(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _create_pipeline(**overrides) -> SeedLockfilePipeline:
        defaults = dict(
            runtime=MagicMock(dry_run=False, doozer_working='/tmp/doozer-working'),
            version='4.22',
            image_list='ironic,ovn-kubernetes',
        )
        defaults.update(overrides)
        return SeedLockfilePipeline(**defaults)

    def test_init_parses_image_list(self):
        pipeline = self._create_pipeline()
        self.assertEqual(pipeline.images, ['ironic', 'ovn-kubernetes'])

    def test_init_parses_seed_nvrs(self):
        pipeline = self._create_pipeline(
            seed_nvrs='ironic@ironic-container-v4.22.0-assembly.test,'
            'ovn-kubernetes@ovn-kubernetes-container-v4.22.0-assembly.test'
        )
        self.assertEqual(
            pipeline.seed_map,
            {
                'ironic': 'ironic-container-v4.22.0-assembly.test',
                'ovn-kubernetes': 'ovn-kubernetes-container-v4.22.0-assembly.test',
            },
        )

    def test_init_empty_image_list_raises(self):
        with self.assertRaises(click.BadParameter):
            self._create_pipeline(image_list='')

    def test_init_invalid_seed_nvrs_raises(self):
        with self.assertRaises(click.BadParameter):
            self._create_pipeline(seed_nvrs='ironic-container-v4.22.0')

    def test_init_defaults(self):
        pipeline = self._create_pipeline()
        self.assertEqual(pipeline.assembly, 'stream')
        self.assertEqual(pipeline.seed_map, {})

    def test_doozer_base_command(self):
        pipeline = self._create_pipeline(data_gitref='my-branch')
        cmd = pipeline._doozer_base_command(assembly='test')
        self.assertIn('--assembly=test', cmd)
        self.assertIn('--build-system=konflux', cmd)
        self.assertIn('--group=openshift-4.22@my-branch', cmd)
        self.assertIn('--images=ironic,ovn-kubernetes', cmd)
        self.assertIn('--latest-parent-version', cmd)

    def test_doozer_base_command_with_arches(self):
        pipeline = self._create_pipeline(arches=('x86_64', 'aarch64'))
        cmd = pipeline._doozer_base_command(assembly='stream')
        self.assertIn('--arches=x86_64,aarch64', cmd)

    def test_extract_seed_map_from_record_log(self):
        record_log_content = (
            'image_build_konflux|name=ironic|nvrs=ironic-container-v4.22.0-assembly.test|status=0|has_olm_bundle=0\n'
            'image_build_konflux|name=ovn-kubernetes|nvrs=ovn-kubernetes-container-v4.22.0-assembly.test|status=0|has_olm_bundle=0\n'
        )
        pipeline = self._create_pipeline()
        fake_file = io.StringIO(record_log_content)
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.open', return_value=fake_file):
                pipeline._extract_seed_map_from_record_log()

        self.assertEqual(
            pipeline.seed_map,
            {
                'ironic': 'ironic-container-v4.22.0-assembly.test',
                'ovn-kubernetes': 'ovn-kubernetes-container-v4.22.0-assembly.test',
            },
        )

    def test_extract_seed_map_skips_failures(self):
        record_log_content = (
            'image_build_konflux|name=ironic|nvrs=ironic-container-v4.22.0-assembly.test|status=0|has_olm_bundle=0\n'
            'image_build_konflux|name=ovn-kubernetes|nvrs=n/a|status=-1|has_olm_bundle=0\n'
        )
        pipeline = self._create_pipeline()
        fake_file = io.StringIO(record_log_content)
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.open', return_value=fake_file):
                pipeline._extract_seed_map_from_record_log()

        self.assertEqual(
            pipeline.seed_map,
            {
                'ironic': 'ironic-container-v4.22.0-assembly.test',
            },
        )

    def test_extract_seed_map_missing_record_log(self):
        pipeline = self._create_pipeline()
        with patch('pathlib.Path.exists', return_value=False):
            pipeline._extract_seed_map_from_record_log()
        self.assertEqual(pipeline.seed_map, {})

    def test_extract_seed_map_populates_test_results(self):
        record_log_content = (
            'image_build_konflux|name=ironic|nvrs=ironic-container-v4.22.0-assembly.test|status=0|has_olm_bundle=0\n'
            'image_build_konflux|name=ovn-kubernetes|nvrs=n/a|status=-1|has_olm_bundle=0\n'
        )
        pipeline = self._create_pipeline()
        fake_file = io.StringIO(record_log_content)
        with patch('pathlib.Path.exists', return_value=True):
            with patch('pathlib.Path.open', return_value=fake_file):
                pipeline._extract_seed_map_from_record_log()

        self.assertIn('ironic', pipeline.test_results)
        self.assertIn('ovn-kubernetes', pipeline.test_results)
        self.assertEqual(pipeline.test_results['ironic']['status'], '0')
        self.assertEqual(pipeline.test_results['ovn-kubernetes']['status'], '-1')

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_title')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_description')
    @patch('pyartcd.pipelines.seed_lockfile.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_run_with_seed_nvrs_skips_build(self, mock_cmd, _desc, _title, _init):
        """When seed NVRs are provided, Phase 1 is skipped; Phase 2 does rebase + build."""
        pipeline = self._create_pipeline(seed_nvrs='ironic@ironic-container-v4.22.0-assembly.test')
        pipeline.slack_client.say = AsyncMock()
        with patch.object(pipeline, '_extract_stream_results_from_record_log'):
            await pipeline.run()

        # Phase 2 only: rebase + build = 2 calls
        self.assertEqual(mock_cmd.await_count, 2)

        # First call: rebase in stream with lockfile-seed-nvrs
        rebase_cmd = mock_cmd.call_args_list[0][0][0]
        self.assertIn('beta:images:konflux:rebase', rebase_cmd)
        self.assertIn('--assembly=stream', rebase_cmd)
        seed_arg = [a for a in rebase_cmd if a.startswith('--lockfile-seed-nvrs=')][0]
        self.assertIn('ironic-container-v4.22.0-assembly.test', seed_arg)

        # Second call: build in stream
        build_cmd = mock_cmd.call_args_list[1][0][0]
        self.assertIn('beta:images:konflux:build', build_cmd)
        self.assertIn('--assembly=stream', build_cmd)

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_title')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_description')
    @patch('pyartcd.pipelines.seed_lockfile.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_run_without_seed_nvrs_builds_first(self, mock_cmd, _desc, _title, _init):
        """When no seed NVRs, Phase 1 build runs, then Phase 2 rebase + build."""
        pipeline = self._create_pipeline()
        pipeline.slack_client.say = AsyncMock()

        with (
            patch.object(pipeline, '_extract_seed_map_from_record_log') as mock_extract,
            patch.object(pipeline, '_extract_stream_results_from_record_log'),
        ):

            def set_seed_map():
                pipeline.seed_map = {'ironic': 'ironic-container-v4.22.0-assembly.test'}

            mock_extract.side_effect = set_seed_map

            await pipeline.run()

        # Phase 1: rebase + build, Phase 2: rebase + build = 4 calls
        self.assertEqual(mock_cmd.await_count, 4)

        # First call: rebase in test assembly
        rebase_cmd = mock_cmd.call_args_list[0][0][0]
        self.assertIn('--assembly=test', rebase_cmd)
        self.assertIn('beta:images:konflux:rebase', rebase_cmd)
        self.assertIn('open', rebase_cmd)

        # Second call: build in test assembly
        build_cmd = mock_cmd.call_args_list[1][0][0]
        self.assertIn('--assembly=test', build_cmd)
        self.assertIn('beta:images:konflux:build', build_cmd)
        self.assertIn('open', build_cmd)

        # Third call: rebase in stream with lockfile-seed-nvrs
        stream_rebase_cmd = mock_cmd.call_args_list[2][0][0]
        self.assertIn('--assembly=stream', stream_rebase_cmd)
        self.assertIn('beta:images:konflux:rebase', stream_rebase_cmd)

        # Fourth call: build in stream
        stream_build_cmd = mock_cmd.call_args_list[3][0][0]
        self.assertIn('--assembly=stream', stream_build_cmd)
        self.assertIn('beta:images:konflux:build', stream_build_cmd)
        self.assertNotIn('open', stream_build_cmd)

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.seed_lockfile.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_run_fails_if_no_builds_succeed(self, mock_cmd, _init):
        """Raises RuntimeError if Phase 1 produces no seed map entries."""
        pipeline = self._create_pipeline()

        with patch.object(pipeline, '_extract_seed_map_from_record_log'):
            # seed_map stays empty
            with self.assertRaises(RuntimeError):
                await pipeline.run()

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_title')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_description')
    @patch('pyartcd.pipelines.seed_lockfile.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_dry_run_skips_push(self, mock_cmd, _desc, _title, _init):
        """In dry run, --push is not added to rebase and --dry-run is added to build."""
        pipeline = self._create_pipeline(
            runtime=MagicMock(dry_run=True, doozer_working='/tmp/doozer-working'),
            seed_nvrs='ironic@ironic-container-v4.22.0-assembly.test',
        )
        pipeline.slack_client.say = AsyncMock()
        with patch.object(pipeline, '_extract_stream_results_from_record_log'):
            await pipeline.run()

        self.assertEqual(mock_cmd.await_count, 2)

        rebase_cmd = mock_cmd.call_args_list[0][0][0]
        self.assertNotIn('--push', rebase_cmd)

        build_cmd = mock_cmd.call_args_list[1][0][0]
        self.assertIn('--dry-run', build_cmd)

    def test_categorize_results(self):
        """Results are correctly categorized into failed/solved/stream-failed."""
        pipeline = self._create_pipeline(image_list='img-a,img-b,img-c')
        pipeline.test_results = {
            'img-a': {'name': 'img-a', 'status': '-1'},
            'img-b': {'name': 'img-b', 'status': '0'},
            'img-c': {'name': 'img-c', 'status': '0'},
        }
        pipeline.stream_results = {
            'img-b': {'name': 'img-b', 'status': '0'},
            'img-c': {'name': 'img-c', 'status': '-1'},
        }

        test_failed, solved, stream_failed = pipeline._categorize_results()
        self.assertEqual(test_failed, ['img-a'])
        self.assertEqual(solved, ['img-b'])
        self.assertEqual(stream_failed, ['img-c'])

    def test_build_url_with_record_id(self):
        """_build_url produces /build? link when record_id is present."""
        url = SeedLockfilePipeline._build_url(
            'openshift-4.22',
            {
                'nvrs': 'ironic-container-v4.22.0-202603201234.p0.assembly.stream.el9',
                'record_id': 'abc-123',
                'status': '0',
            },
        )
        self.assertIn('/build?', url)
        self.assertIn('nvr=ironic-container', url)
        self.assertIn('record_id=abc-123', url)
        self.assertIn('outcome=success', url)

    def test_build_url_without_record_id(self):
        """_build_url falls back to search page when record_id is absent."""
        url = SeedLockfilePipeline._build_url(
            'openshift-4.22',
            {'nvrs': 'ironic-container-v4.22.0-202603201234.p0.assembly.stream.el9', 'status': '-1'},
        )
        self.assertIn('/?nvr=', url)
        self.assertNotIn('/build?', url)

    def test_build_url_no_nvr(self):
        """_build_url returns empty string when no NVR is available."""
        url = SeedLockfilePipeline._build_url('openshift-4.22', {'nvrs': 'n/a', 'status': '-1'})
        self.assertEqual(url, '')

    def test_search_url_contains_image_and_group(self):
        url = SeedLockfilePipeline._search_url('ironic', 'openshift-4.22')
        self.assertIn('name=^ironic$', url)
        self.assertIn('group=openshift-4.22', url)
        self.assertIn('engine=konflux', url)

    def test_link_fallback_search(self):
        """_link returns a search URL when _build_url returns empty and fallback_search is True."""
        pipeline = self._create_pipeline(image_list='ironic')
        pipeline.stream_results = {'ironic': {'name': 'ironic', 'nvrs': 'n/a', 'status': '-1'}}
        url = pipeline._link('ironic', 'stream', 'openshift-4.22', fallback_search=True)
        self.assertIn('name=^ironic$', url)
        self.assertIn('group=openshift-4.22', url)

    def test_link_no_fallback_returns_empty(self):
        """_link returns empty when _build_url returns empty and fallback_search is False."""
        pipeline = self._create_pipeline(image_list='ironic')
        pipeline.stream_results = {'ironic': {'name': 'ironic', 'nvrs': 'n/a', 'status': '-1'}}
        url = pipeline._link('ironic', 'stream', 'openshift-4.22', fallback_search=False)
        self.assertEqual(url, '')

    def test_slack_report_content(self):
        """Slack report contains expected sections."""
        pipeline = self._create_pipeline(image_list='img-a,img-b,img-c')
        report = pipeline._build_slack_report(test_failed=['img-a'], solved=['img-b'], stream_failed=['img-c'])
        self.assertIn('open` build failed', report)
        self.assertIn('img-a', report)
        self.assertIn('Solved', report)
        self.assertIn('img-b', report)
        self.assertIn('Stream build failed', report)
        self.assertIn('img-c', report)

    def test_slack_report_uses_succeeded_for_test_assembly(self):
        """Slack report shows 'Succeeded' instead of 'Solved' for non-stream assembly."""
        pipeline = self._create_pipeline(image_list='img-a', assembly='test')
        report = pipeline._build_slack_report(test_failed=[], solved=['img-a'], stream_failed=[])
        self.assertIn('Succeeded', report)
        self.assertNotIn('Solved', report)

    def test_jenkins_description_content(self):
        """Jenkins HTML description contains expected sections."""
        pipeline = self._create_pipeline(image_list='img-a,img-b,img-c')
        html = pipeline._build_jenkins_description(test_failed=['img-a'], solved=['img-b'], stream_failed=['img-c'])
        self.assertIn('img-a', html)
        self.assertIn('open build failed', html)
        self.assertIn('Solved', html)
        self.assertIn('img-b', html)
        self.assertIn('Stream build failed', html)
        self.assertIn('img-c', html)

    def test_jenkins_description_uses_succeeded_for_test_assembly(self):
        """Jenkins description shows 'Succeeded' instead of 'Solved' for non-stream assembly."""
        pipeline = self._create_pipeline(image_list='img-a', assembly='test')
        html = pipeline._build_jenkins_description(test_failed=[], solved=['img-a'], stream_failed=[])
        self.assertIn('Succeeded', html)
        self.assertNotIn('Solved', html)

    def test_jenkins_description_includes_seed_nvrs(self):
        """Jenkins description includes seed NVR section."""
        pipeline = self._create_pipeline(
            image_list='ironic',
            seed_nvrs='ironic@ironic-container-v4.22.0-assembly.test',
        )
        pipeline.stream_results = {'ironic': {'name': 'ironic', 'status': '0', 'nvrs': 'ironic-nvr', 'record_id': 'r1'}}
        html = pipeline._build_jenkins_description(test_failed=[], solved=['ironic'], stream_failed=[])
        self.assertIn('Seed NVRs', html)
        self.assertIn('ironic-container-v4.22.0-assembly.test', html)

    def test_jenkins_description_includes_data_link(self):
        """Jenkins description includes ocp-build-data branch link when data_gitref is set."""
        pipeline = self._create_pipeline(
            image_list='ironic',
            data_path='https://github.com/joepvd/ocp-build-data',
            data_gitref='ART-14902-4.22-ose-frr',
        )
        html = pipeline._build_jenkins_description(test_failed=[], solved=[], stream_failed=[])
        self.assertIn('https://github.com/joepvd/ocp-build-data/tree/ART-14902-4.22-ose-frr', html)
        self.assertIn('Data:', html)

    def test_jenkins_description_no_data_link_without_gitref(self):
        """Jenkins description omits data link when data_gitref is not set."""
        pipeline = self._create_pipeline(image_list='ironic')
        html = pipeline._build_jenkins_description(test_failed=[], solved=[], stream_failed=[])
        self.assertNotIn('Data:', html)

    def test_jenkins_description_stream_failed_has_fallback_link(self):
        """stream_failed entries get a fallback search URL when build URL is unavailable."""
        pipeline = self._create_pipeline(image_list='img-c')
        pipeline.stream_results = {'img-c': {'name': 'img-c', 'nvrs': 'n/a', 'status': '-1'}}
        pipeline.test_results = {'img-c': {'name': 'img-c', 'nvrs': 'img-c-nvr', 'status': '0', 'record_id': 'r1'}}
        html = pipeline._build_jenkins_description(test_failed=[], solved=[], stream_failed=['img-c'])
        self.assertIn('stream', html)
        self.assertIn('name=^img-c$', html)

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_title')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_description')
    async def test_post_report_sets_title_with_solved(self, mock_desc, mock_title):
        """_post_report appends solved images to the Jenkins build title."""
        pipeline = self._create_pipeline(image_list='ironic')
        pipeline.stream_results = {'ironic': {'name': 'ironic', 'status': '0'}}
        pipeline.slack_client.say = AsyncMock()

        await pipeline._post_report()

        pipeline.slack_client.say.assert_awaited_once()
        mock_title.assert_called_once()
        self.assertIn('solved', mock_title.call_args[0][0])
        self.assertIn('ironic', mock_title.call_args[0][0])

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_title')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_description')
    async def test_post_report_no_solved_in_title_for_test_assembly(self, mock_desc, mock_title):
        """_post_report does not add 'solved' to title when assembly is not stream."""
        pipeline = self._create_pipeline(image_list='ironic', assembly='test')
        pipeline.stream_results = {'ironic': {'name': 'ironic', 'status': '0'}}
        pipeline.slack_client.say = AsyncMock()

        await pipeline._post_report()

        mock_title.assert_called_once()
        title_arg = mock_title.call_args[0][0]
        self.assertIn('[test]', title_arg)
        self.assertNotIn('solved', title_arg)

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_title')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_description')
    async def test_post_report_includes_jira_key_in_title(self, mock_desc, mock_title):
        """_post_report includes jira_key in the Jenkins title."""
        pipeline = self._create_pipeline(image_list='ironic', assembly='test', jira_key='ART-14902')
        pipeline.stream_results = {'ironic': {'name': 'ironic', 'status': '0'}}
        pipeline.slack_client.say = AsyncMock()

        await pipeline._post_report()

        mock_title.assert_called_once()
        title_arg = mock_title.call_args[0][0]
        self.assertIn('ART-14902', title_arg)
        self.assertIn('[test]', title_arg)

    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_title')
    @patch('pyartcd.pipelines.seed_lockfile.jenkins.update_description')
    async def test_post_report_no_title_update_for_stream_no_solved(self, mock_desc, mock_title):
        """_post_report does not update title when assembly is stream and nothing solved."""
        pipeline = self._create_pipeline(image_list='ironic')
        pipeline.stream_results = {'ironic': {'name': 'ironic', 'status': '-1'}}
        pipeline.test_results = {'ironic': {'name': 'ironic', 'status': '0'}}
        pipeline.slack_client.say = AsyncMock()

        await pipeline._post_report()

        mock_title.assert_not_called()

    def test_init_stores_jira_key(self):
        pipeline = self._create_pipeline(jira_key='ART-12345')
        self.assertEqual(pipeline.jira_key, 'ART-12345')

    def test_init_jira_key_defaults_to_empty(self):
        pipeline = self._create_pipeline()
        self.assertEqual(pipeline.jira_key, '')
