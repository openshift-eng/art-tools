import lzma
import os
import tarfile
from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from pyartcd.pipelines.build_sync import BuildSyncPipeline, build_sync


class TestBuildSyncRetirement(TestCase):
    def pipeline(self, build_system='konflux'):
        return BuildSyncPipeline(
            runtime=MagicMock(),
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
            build_system=build_system,
        )

    def test_rejects_brew_payloads(self):
        with self.assertRaisesRegex(ValueError, 'Brew build-sync has been retired'):
            self.pipeline('brew')

    @patch('pyartcd.pipelines.build_sync.uses_konflux_imagestream_override')
    def test_preserves_konflux_imagestream_names_and_counter(self, override):
        for enabled, expected in [(True, '4.22-art-latest'), (False, '4.22-konflux-art-latest')]:
            with self.subTest(override=enabled):
                override.return_value = enabled
                pipeline = self.pipeline()
                self.assertEqual(pipeline.is_base_name, expected)
                self.assertEqual(pipeline.fail_count_name, 'count:build-sync-failure:konflux:stream:4.22')

    @patch('pyartcd.pipelines.build_sync.jenkins.init_jenkins')
    def test_cli_rejects_brew_before_connecting_to_jenkins(self, init_jenkins):
        result = CliRunner().invoke(build_sync, ['--version', '4.22', '--build-system', 'brew'])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("'brew' is not 'konflux'", result.output)
        init_jenkins.assert_not_called()


class TestBuildSyncBackups(IsolatedAsyncioTestCase):
    def setUp(self):
        self.pipeline = TestBuildSyncRetirement().pipeline()
        self.pipeline.runtime.dry_run = False
        self.pipeline.supported_arches = ['x86_64', 'aarch64']

    async def _snapshot(self, command):
        namespace = command.split(' -n ')[1].split()[0]
        kind = 'List' if ' get is ' in command else 'Secret'
        return 0, f'kind: {kind}\nmetadata:\n  namespace: {namespace}\n', ''

    @patch.dict(os.environ, {'KUBECONFIG': '/unused/kubeconfig'})
    async def test_backup_archive_restores_all_snapshots_as_yaml(self):
        # Exercise the real archive writer, mocking only the cluster reads.
        with TemporaryDirectory() as directory, chdir(directory):
            with patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', side_effect=self._snapshot) as gather:
                await self.pipeline._backup_all_imagestreams()

            self.assertEqual(gather.await_count, 8)
            expected = {}
            for namespace in ('ocp', 'ocp-priv', 'ocp-arm64', 'ocp-arm64-priv'):
                expected[f'{namespace}.backup.yaml'] = f'kind: List\nmetadata:\n  namespace: {namespace}\n'
                expected[f'{namespace}.release-upgrade-graph.backup.yaml'] = (
                    f'kind: Secret\nmetadata:\n  namespace: {namespace}\n'
                )

            # The artifact remains an LZMA-compressed tar, with no nested compression.
            with (
                lzma.open('app.ci-backup.tgz', 'rb') as compressed,
                tarfile.open(fileobj=compressed, mode='r:') as archive,
            ):
                self.assertEqual(set(archive.getnames()), set(expected))
                for name, content in expected.items():
                    self.assertEqual(archive.extractfile(name).read().decode(), content)
            self.assertEqual([p.name for p in Path('.').iterdir()], ['app.ci-backup.tgz'])

    @patch.dict(os.environ, {'KUBECONFIG': '/unused/kubeconfig'})
    async def test_failed_archive_preserves_snapshots(self):
        with TemporaryDirectory() as directory, chdir(directory):
            with (
                patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', side_effect=self._snapshot),
                patch(
                    'pyartcd.pipelines.build_sync.BuildSyncPipeline._create_backup_archive',
                    side_effect=ChildProcessError('archive failed'),
                ) as commands,
            ):
                with self.assertRaisesRegex(ChildProcessError, 'archive failed'):
                    await self.pipeline._backup_all_imagestreams()
            commands.assert_called_once()
            self.assertEqual(len(list(Path('.').glob('*.backup.yaml'))), 8)

    async def test_dry_run_skips_cluster_and_archive_operations(self):
        self.pipeline.runtime.dry_run = True
        with (
            patch('pyartcd.pipelines.build_sync.exectools.cmd_gather_async', new_callable=AsyncMock) as gather,
            patch('pyartcd.pipelines.build_sync.exectools.to_thread', new_callable=AsyncMock) as commands,
        ):
            await self.pipeline._backup_all_imagestreams()
        gather.assert_not_awaited()
        commands.assert_not_awaited()
