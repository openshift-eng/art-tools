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
