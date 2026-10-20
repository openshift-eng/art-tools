import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from pyartcd.pipelines.merge_layered_fbc import MergeLayeredFbcPipeline


class TestMergeLayeredFbcPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runtime = Mock(working_dir=Path(self.temp.name), doozer_working='/tmp/doozer', dry_run=False)
        self.result = {
            'nvr': 'quay-merged-fbc-5.0-1.ocp5.0',
            'pullspec': 'quay.io/test/fbc@sha256:abc',
            'build_url': 'https://konflux.example/build',
            'dry_run': False,
            'provenance': {},
        }
        (Path(self.temp.name) / 'merged-fbc.json').write_text(json.dumps(self.result))
        self.pipeline = MergeLayeredFbcPipeline(
            runtime=self.runtime,
            group='quay-3.18',
            ocp_target_version='5.0',
            fbc_builds=('quay-fbc-3.18.2-1.ocp5.0', 'quay-fbc-3.19.1-1.ocp5.0'),
        )

    async def test_passes_all_inputs_and_build_overrides(self):
        self.pipeline.options['--konflux-kubeconfig'] = '/tmp/quay-kubeconfig'
        self.pipeline.skip_tasks = ('clair-scan',)
        with patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock()) as command:
            result = await self.pipeline._build()
        self.assertEqual(result, self.result)
        cmd = command.await_args.args[0]
        self.assertIn('--group=quay-3.18', cmd)
        self.assertIn('--major-minor=5.0', cmd)
        self.assertEqual(cmd.count('--fbc-build'), 2)
        self.assertIn('/tmp/quay-kubeconfig', cmd)
        self.assertIn('clair-scan', cmd)
        self.assertNotIn('--dry-run', cmd)

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
