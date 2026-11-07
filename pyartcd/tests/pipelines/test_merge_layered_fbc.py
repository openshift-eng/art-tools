import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from pyartcd.cli import cli
from pyartcd.pipelines.merge_layered_fbc import MergeLayeredFbcPipeline


class TestMergeLayeredFbcPipeline(unittest.IsolatedAsyncioTestCase):
    def test_public_command_name(self):
        self.assertIn('lp-unified-fbc', cli.commands)
        self.assertNotIn('merge-layered-fbc', cli.commands)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runtime = Mock(working_dir=Path(self.temp.name), doozer_working='/tmp/doozer', dry_run=False)
        self.result = {
            'nvr': 'quay-fbc-unified-5.0-1.ocp5.0',
            'pullspec': 'quay.io/test/fbc@sha256:abc',
            'build_url': 'https://konflux.example/build',
            'dry_run': False,
            'provenance': {'operators': ['container-security-operator', 'quay-bridge-operator', 'quay-operator']},
        }
        (Path(self.temp.name) / 'unified-fbc.json').write_text(json.dumps(self.result))
        self.pipeline = MergeLayeredFbcPipeline(
            runtime=self.runtime,
            ocp_target_version='5.0',
            fbc_builds=(
                'quay-operator-fbc-3.18.2-1.ocp5.0',
                'quay-operator-fbc-3.19.1-1.ocp5.0',
                'container-security-operator-fbc-3.18.2-1.ocp5.0',
                'quay-bridge-operator-fbc-3.19.1-1.ocp5.0',
            ),
        )
        self.pipeline.group = 'quay-3.18'
        self.pipeline._resolve_groups = AsyncMock(return_value=['quay-3.18', 'quay-3.19'])

    async def test_passes_all_inputs_and_build_overrides(self):
        self.pipeline.options['--konflux-kubeconfig'] = '/tmp/quay-kubeconfig'
        self.pipeline.skip_tasks = ('clair-scan',)
        with patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock()) as command:
            result = await self.pipeline._build()
        self.assertEqual(result, self.result)
        cmd = command.await_args.args[0]
        self.assertIn('--group=quay-3.18', cmd)
        self.assertIn('--major-minor=5.0', cmd)
        self.assertEqual(cmd.count('--fbc-build'), 4)
        self.assertEqual(
            tuple(cmd[i + 1] for i, value in enumerate(cmd) if value == '--fbc-build'), self.pipeline.builds
        )
        self.assertIn('/tmp/quay-kubeconfig', cmd)
        self.assertIn('clair-scan', cmd)
        self.assertNotIn('--dry-run', cmd)
        self.assertNotIn('--skip-rebase', cmd)

    async def test_passes_skip_rebase_to_doozer(self):
        self.pipeline.skip_rebase = True
        with patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock()) as command:
            await self.pipeline._build()
        self.assertIn('--skip-rebase', command.await_args.args[0])

    async def test_custom_branch_automatically_skips_rebase(self):
        branch = 'codex/art-quay-fbc-unified-prod-4-22'
        pipeline = MergeLayeredFbcPipeline(
            runtime=self.runtime,
            ocp_target_version='5.0',
            fbc_builds=self.pipeline.builds,
            fbc_branch=branch,
        )
        pipeline.group = self.pipeline.group
        with patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock()) as command:
            await pipeline._build()
        cmd = command.await_args.args[0]
        self.assertIn('--skip-rebase', cmd)
        self.assertEqual(cmd[cmd.index('--fbc-branch') + 1], branch)

    def test_cli_accepts_branch_without_build_inputs(self):
        with cli.commands['lp-unified-fbc'].make_context(
            'lp-unified-fbc', ['--ocp-target-version=5.0', '--fbc-branch=codex/production']
        ) as context:
            self.assertEqual(context.params['fbc_builds'], ())

    async def test_branch_only_resolves_configuration_without_build_database(self):
        provenance = {'execution_group': 'quay-3.16', 'product': 'quay', 'ocp_target_version': '5.0'}

        async def run_locked(**kwargs):
            self.assertEqual(kwargs['lock_name'], 'lock:fbc-merge:quay:ocp-5.0')
            return await kwargs['coro']

        for dry_run, repository in ((True, None), (False, 'git@github.com:example/custom-fbc.git')):
            with self.subTest(dry_run=dry_run, repository=repository):
                self.runtime.dry_run = dry_run
                pipeline = MergeLayeredFbcPipeline(
                    runtime=self.runtime, ocp_target_version='5.0', fbc_branch='codex/production', fbc_repo=repository
                )
                repo = Mock()
                repo.get_contents.return_value.decoded_content = json.dumps(provenance).encode()
                with (
                    patch('pyartcd.pipelines.merge_layered_fbc.get_github_client_for_org') as client,
                    patch('pyartcd.pipelines.merge_layered_fbc.KonfluxDb') as db,
                    patch(
                        'pyartcd.pipelines.merge_layered_fbc.load_group_config',
                        new=AsyncMock(return_value={'product': 'quay', 'OCP_TARGET_VERSIONS': ['5.0']}),
                    ) as config,
                    patch(
                        'pyartcd.pipelines.merge_layered_fbc.locks.run_with_lock',
                        new=AsyncMock(side_effect=run_locked),
                    ) as lock,
                    patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock()) as command,
                ):
                    client.return_value.get_repo.return_value = repo
                    await pipeline.run()
                db.assert_not_called()
                client.assert_called_once_with('example' if repository else 'openshift-priv')
                client.return_value.get_repo.assert_called_once_with(
                    'example/custom-fbc' if repository else 'openshift-priv/art-fbc'
                )
                repo.get_contents.assert_called_once_with('unified-provenance.json', ref='codex/production')
                config.assert_awaited_once_with('quay-3.16', 'stream', None, pipeline.data_path)
                self.assertEqual(lock.await_count, 0 if dry_run else 1)
                cmd = command.await_args.args[0]
                self.assertIn('--group=quay-3.16', cmd)
                self.assertIn('--skip-rebase', cmd)
                self.assertNotIn('--fbc-build', cmd)

    async def test_branch_only_rejects_invalid_provenance_before_build(self):
        valid = {'execution_group': 'quay-3.16', 'product': 'quay', 'ocp_target_version': '5.0'}
        for change, error in (
            ({'execution_group': None}, 'execution_group'),
            ({'execution_group': '--invalid group'}, 'execution_group'),
            ({'ocp_target_version': '4.22'}, 'does not match OCP'),
            ({'product': 'logging'}, 'expected product quay'),
            ({'product': None}, 'layered-product identity'),
        ):
            with self.subTest(change=change):
                pipeline = MergeLayeredFbcPipeline(
                    runtime=self.runtime, ocp_target_version='5.0', fbc_branch='codex/production', product='quay'
                )
                with (
                    patch('pyartcd.pipelines.merge_layered_fbc.get_github_client_for_org') as client,
                    patch.object(pipeline, '_build', new=AsyncMock()) as build,
                ):
                    client.return_value.get_repo.return_value.get_contents.return_value.decoded_content = json.dumps(
                        valid | change
                    ).encode()
                    with self.assertRaisesRegex(ValueError, error):
                        await pipeline.run()
                build.assert_not_awaited()

    async def test_empty_builds_without_custom_branch_are_rejected(self):
        pipeline = MergeLayeredFbcPipeline(runtime=self.runtime, ocp_target_version='5.0', skip_rebase=True)
        with self.assertRaisesRegex(ValueError, 'unless --fbc-branch is set'):
            await pipeline.run()

    async def test_lock_uses_product_and_target(self):
        async def run_locked(**kwargs):
            self.assertEqual(kwargs['lock_name'], 'lock:fbc-merge:quay:ocp-5.0')
            return await kwargs['coro']

        with (
            patch(
                'pyartcd.pipelines.merge_layered_fbc.load_group_config',
                new=AsyncMock(
                    return_value={
                        'product': 'quay',
                        'OCP_TARGET_VERSIONS': ['5.0'],
                    }
                ),
            ),
            patch('pyartcd.pipelines.merge_layered_fbc.locks.run_with_lock', new=AsyncMock(side_effect=run_locked)),
            patch.object(self.pipeline, '_build', new=AsyncMock(return_value=self.result)),
        ):
            self.assertEqual(await self.pipeline.run(), self.result)

    async def test_dry_run_skips_redis_and_passes_flag(self):
        self.runtime.dry_run = True
        with (
            patch(
                'pyartcd.pipelines.merge_layered_fbc.load_group_config',
                new=AsyncMock(
                    return_value={
                        'product': 'quay',
                        'OCP_TARGET_VERSIONS': ['5.0'],
                    }
                ),
            ),
            patch('pyartcd.pipelines.merge_layered_fbc.locks.run_with_lock', new=AsyncMock()) as lock,
            patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock()) as command,
        ):
            await self.pipeline.run()
        lock.assert_not_called()
        self.assertIn('--dry-run', command.await_args.args[0])

    async def test_unsupported_target_fails_before_lock(self):
        with (
            patch(
                'pyartcd.pipelines.merge_layered_fbc.load_group_config',
                new=AsyncMock(
                    return_value={
                        'product': 'quay',
                        'OCP_TARGET_VERSIONS': ['4.20'],
                    }
                ),
            ),
            patch('pyartcd.pipelines.merge_layered_fbc.locks.run_with_lock', new=AsyncMock()) as lock,
        ):
            with self.assertRaisesRegex(ValueError, 'does not support'):
                await self.pipeline.run()
        lock.assert_not_called()

    async def test_build_failure_does_not_return_stale_result(self):
        with patch(
            'pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async',
            new=AsyncMock(side_effect=RuntimeError('failed')),
        ):
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                await self.pipeline._build()

    async def test_mixed_products_fail_before_build(self):
        with (
            patch(
                'pyartcd.pipelines.merge_layered_fbc.load_group_config',
                new=AsyncMock(
                    side_effect=[
                        {'product': 'quay', 'OCP_TARGET_VERSIONS': ['5.0']},
                        {'product': 'logging', 'OCP_TARGET_VERSIONS': ['5.0']},
                    ]
                ),
            ),
            patch.object(self.pipeline, '_build', new=AsyncMock()) as build,
        ):
            with self.assertRaisesRegex(ValueError, 'belongs to logging, expected product quay'):
                await self.pipeline.run()
        build.assert_not_awaited()

    def test_cli_accepts_assemblies_without_target(self):
        with cli.commands['lp-unified-fbc'].make_context('lp-unified-fbc', ['--assembly=3.18.5']) as context:
            self.assertIsNone(context.params['ocp_target_version'])
            self.assertEqual(context.params['assemblies'], ('3.18.5',))

    async def test_explicit_builds_still_require_target(self):
        pipeline = MergeLayeredFbcPipeline(self.runtime, fbc_builds=self.pipeline.builds)
        with self.assertRaisesRegex(ValueError, 'required for explicit FBC'):
            await pipeline.run()

    async def test_custom_branch_infers_target_when_omitted(self):
        self.runtime.dry_run = True
        pipeline = MergeLayeredFbcPipeline(self.runtime, fbc_branch='custom')
        repo = Mock()
        repo.get_contents.return_value.decoded_content = json.dumps(
            {
                'execution_group': 'quay-3.18',
                'product': 'quay',
                'ocp_target_version': '4.22',
            }
        ).encode()
        with (
            patch('pyartcd.pipelines.merge_layered_fbc.get_github_client_for_org') as client,
            patch(
                'pyartcd.pipelines.merge_layered_fbc.load_group_config',
                new=AsyncMock(return_value={'product': 'quay', 'OCP_TARGET_VERSIONS': ['4.22']}),
            ),
            patch.object(pipeline, '_build', new=AsyncMock(return_value=self.result)),
        ):
            client.return_value.get_repo.return_value = repo
            await pipeline.run()
        self.assertEqual(pipeline.target, '4.22')

    def test_unsafe_git_inputs_are_rejected_before_commands_or_clients(self):
        for kwargs in (
            {'fbc_branch': '--upload-pack=command'},
            {'fbc_repo': 'https://user:dummy-secret@example.invalid/fbc.git'},
            {'data_path': 'https://dummy-secret@github.com/example/build-data'},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError) as exc:
                MergeLayeredFbcPipeline(self.runtime, **kwargs)
            self.assertNotIn('dummy-secret', str(exc.exception))

    async def test_custom_branch_provenance_rejects_unsafe_source_groups(self):
        for group in ('.', '..', '--detach', 'quay/3.18'):
            p = MergeLayeredFbcPipeline(self.runtime, '4.22', fbc_branch='codex/production')
            repo = Mock()
            repo.get_contents.return_value.decoded_content = json.dumps({'execution_group': group}).encode()
            with patch('pyartcd.pipelines.merge_layered_fbc.get_github_client_for_org') as client:
                client.return_value.get_repo.return_value = repo
                with self.subTest(group=group), self.assertRaisesRegex(ValueError, 'execution_group'):
                    await p._resolve_branch_group()
