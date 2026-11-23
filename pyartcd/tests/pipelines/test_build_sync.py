import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from pyartcd.pipelines.build_sync import BuildSyncPipeline, build_sync


class TestBuildSync(IsolatedAsyncioTestCase):
    def pipeline_args(self, **overrides):
        return (
            dict(
                runtime=MagicMock(dry_run=False),
                version='4.22',
                assembly='stream',
                publish=False,
                data_path='https://github.com/openshift-eng/ocp-build-data',
                emergency_ignore_issues=False,
                retrigger_current_nightly=False,
                doozer_data_gitref=None,
                images=None,
                exclude_arches=None,
                skip_multiarch_payload=False,
                embargo_permit_ack=False,
                build_system='konflux',
            )
            | overrides
        )

    def test_rejects_brew_payloads(self):
        with self.assertRaisesRegex(ValueError, 'Brew build-sync has been retired'):
            BuildSyncPipeline(**self.pipeline_args(build_system='brew'))

    @patch('pyartcd.pipelines.build_sync.uses_konflux_imagestream_override')
    def test_preserves_konflux_imagestream_names_and_counter(self, override):
        for enabled, expected in [(True, '4.22-art-latest'), (False, '4.22-konflux-art-latest')]:
            with self.subTest(override=enabled):
                override.return_value = enabled
                pipeline = BuildSyncPipeline(**self.pipeline_args())
                self.assertEqual(pipeline.is_base_name, expected)
                self.assertEqual(pipeline.fail_count_name, 'count:build-sync-failure:konflux:stream:4.22')

    @patch('pyartcd.pipelines.build_sync.jenkins.init_jenkins')
    def test_cli_rejects_brew_before_connecting_to_jenkins(self, init_jenkins):
        result = CliRunner().invoke(build_sync, ['--version', '4.22', '--build-system', 'brew'])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("'brew' is not 'konflux'", result.output)
        init_jenkins.assert_not_called()

    @patch('pyartcd.pipelines.build_sync.jenkins')
    @patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async')
    @patch('pyartcd.pipelines.build_sync.branch_arches', return_value=['x86_64'])
    @patch('pyartcd.pipelines.build_sync.GroupRuntime.create')
    @patch('pyartcd.pipelines.build_sync.load_releases_config')
    async def test_build_sync_reads_private_visibility_from_selected_ref(
        self, load_releases_config, _group_runtime, branch_arches, execute, _jenkins
    ):
        data_path = 'https://example.com/private-build-data'
        data_ref = 'a' * 40
        load_releases_config.return_value = {
            "releases": {"art1": {"assembly": {"type": "custom", "visibility": "private"}}}
        }
        pipeline = await BuildSyncPipeline.create(
            **self.pipeline_args(assembly='art1', data_path=data_path, doozer_data_gitref=data_ref)
        )
        pipeline.comment_on_assembly_pr = AsyncMock()
        pipeline._backup_all_imagestreams = AsyncMock()
        pipeline._populate_ci_imagestreams = AsyncMock()

        await pipeline._run_pipeline()
        await pipeline.handle_success()

        load_releases_config.assert_awaited_once_with(f'openshift-4.22@{data_ref}', data_path)
        branch_arches.assert_awaited_once_with(
            group='openshift-4.22',
            assembly='art1',
            build_system='konflux',
            data_path=data_path,
            doozer_data_gitref=data_ref,
        )
        execute.assert_awaited_once()
        command = execute.await_args.args[0]
        self.assertIn(f'--group=openshift-4.22@{data_ref}', command)
        self.assertIn(f'--data-path={data_path}', command)
        self.assertIn('--apply', command)
        self.assertIn('--apply-multi-arch', command)
        pipeline.comment_on_assembly_pr.assert_not_awaited()
        pipeline._backup_all_imagestreams.assert_not_awaited()
        pipeline._populate_ci_imagestreams.assert_not_awaited()
        pipeline.slack_client.say.assert_not_called()

    @patch('pyartcd.pipelines.build_sync.GroupRuntime.create')
    @patch('pyartcd.pipelines.build_sync.load_releases_config')
    async def test_private_assembly_rejects_public_options(self, load_releases_config, _group_runtime):
        load_releases_config.return_value = {
            "releases": {"art1": {"assembly": {"type": "custom", "visibility": "private"}}}
        }
        for option in ('publish', 'retrigger_current_nightly', 'embargo_permit_ack'):
            with self.subTest(option=option), self.assertRaisesRegex(ValueError, "visibility: private is incompatible"):
                await BuildSyncPipeline.create(**self.pipeline_args(assembly='art1', **{option: True}))

    async def test_compresses_backups_concurrently_before_archiving(self):
        pipeline = BuildSyncPipeline(**self.pipeline_args())
        backup_files = [f'namespace-{i}.backup.yaml' for i in range(5)]
        backup_sizes = dict(zip(backup_files, (30, 10, 50, 40, 20)))
        expected_order = [backup_files[i] for i in (2, 3, 0, 4, 1)]
        started = []
        four_started = asyncio.Event()
        release = asyncio.Event()
        commands = []

        async def run_command(cmd):
            if cmd[0] == 'lzma':
                started.append(cmd)
                if len(started) == 4:
                    four_started.set()
                await release.wait()
            else:
                commands.append(cmd)

        with (
            patch('pyartcd.pipelines.build_sync.glob.glob', return_value=backup_files) as glob_mock,
            patch('pyartcd.pipelines.build_sync.os.path.getsize', side_effect=backup_sizes.__getitem__),
            patch('pyartcd.pipelines.build_sync.exectools.cmd_assert_async', side_effect=run_command),
        ):
            task = asyncio.create_task(pipeline._create_backup_archive())
            try:
                await asyncio.wait_for(four_started.wait(), timeout=2)
                await asyncio.sleep(0)
                self.assertEqual(len(started), 4)
                self.assertEqual(commands, [])
            finally:
                release.set()
                await task

        self.assertEqual(started, [['lzma', '-6', path] for path in expected_order])
        compressed_files = [f'{path}.lzma' for path in expected_order]
        self.assertEqual(
            commands,
            [
                ['tar', 'cvf', 'app.ci-backup.tgz', '--gzip', *compressed_files],
                ['rm', *compressed_files],
            ],
        )
        glob_mock.assert_called_once_with('*.backup.yaml')
