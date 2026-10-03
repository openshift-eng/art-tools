from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.build_sync import BuildSyncPipeline


def _make_pipeline(assembly='4.21.0', build_system='konflux', dry_run=False):
    runtime = MagicMock()
    runtime.dry_run = dry_run
    runtime.logger = MagicMock()
    runtime.working_dir = Path('/tmp')
    runtime.new_slack_client.return_value = MagicMock()

    with patch('pyartcd.pipelines.build_sync.get_build_url', return_value='http://jenkins/build/1'):
        return BuildSyncPipeline(
            runtime=runtime,
            version='5.1',
            assembly=assembly,
            publish=False,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            emergency_ignore_issues=False,
            retrigger_current_nightly=False,
            doozer_data_gitref=None,
            images=None,
            exclude_arches=None,
            skip_multiarch_payload=False,
            embargo_permit_ack=False,
            build_system=build_system,
        )


class TestBuildSyncPipeline(IsolatedAsyncioTestCase):
    @patch('pyartcd.pipelines.build_sync.uses_konflux_imagestream_override', return_value=False)
    def test_test_imagestream_name_is_assembly_specific(self, _override):
        pipeline = _make_pipeline()

        self.assertEqual(pipeline._test_imagestream_base_name(), '5.1-konflux-art-assembly-4.21.0-test')

    @patch('pyartcd.pipelines.build_sync.uses_konflux_imagestream_override', return_value=True)
    def test_override_version_uses_art_base(self, _override):
        pipeline = _make_pipeline()

        self.assertEqual(pipeline._test_imagestream_base_name(), '5.1-art-assembly-4.21.0-test')

    def test_brew_test_imagestream_name_uses_brew_base(self):
        pipeline = _make_pipeline(build_system='brew')

        self.assertEqual(
            pipeline._test_imagestream_base_name(),
            '5.1-art-assembly-4.21.0-test',
        )

    def test_stream_uses_production_imagestream(self):
        pipeline = _make_pipeline(assembly='stream')

        self.assertIsNone(pipeline._test_imagestream_base_name())

    @patch('pyartcd.pipelines.build_sync.jenkins.update_title')
    def test_marks_test_runs_in_jenkins(self, update_title):
        pipeline = _make_pipeline()

        pipeline._mark_test_run()

        update_title.assert_called_once_with(' [TEST]')

    @patch('pyartcd.pipelines.build_sync.jenkins.update_title')
    def test_does_not_mark_stream_runs_in_jenkins(self, update_title):
        pipeline = _make_pipeline(assembly='stream')

        pipeline._mark_test_run()

        update_title.assert_not_called()

    @patch('pyartcd.pipelines.build_sync.uses_konflux_imagestream_override', return_value=False)
    @patch('pyartcd.pipelines.build_sync.jenkins.init_jenkins')
    @patch.object(BuildSyncPipeline, '_populate_ci_imagestreams', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_update_uses_test_imagestream_and_multiarch(self, cmd_assert, _populate, _init_jenkins, _override):
        pipeline = _make_pipeline()

        await pipeline._update_nightly_imagestreams()

        cmd = cmd_assert.await_args.args[0]
        self.assertIn('--is-name=5.1-konflux-art-assembly-4.21.0-test', cmd)
        self.assertIn('--apply-multi-arch', cmd)

    @patch('pyartcd.pipelines.build_sync.jenkins.init_jenkins')
    @patch.object(BuildSyncPipeline, '_populate_ci_imagestreams', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_stream_update_does_not_override_imagestream(self, cmd_assert, _populate, _init_jenkins):
        pipeline = _make_pipeline(assembly='stream')

        await pipeline._update_nightly_imagestreams()

        cmd = cmd_assert.await_args.args[0]
        self.assertFalse(any(argument.startswith('--is-name=') for argument in cmd))

    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_publish_allows_missing_images(self, cmd_assert):
        pipeline = _make_pipeline()
        with NamedTemporaryFile(mode='w') as metadata_file:
            metadata_file.write('metadata:\n  namespace: ocp\n  name: 5.1-art-assembly-4.21.0-test\n')
            metadata_file.flush()

            await pipeline._publish(metadata_file.name)

        cmd = cmd_assert.await_args.args[0]
        self.assertIn('--allow-missing-images', cmd)
