import inspect
import unittest
from unittest.mock import AsyncMock, Mock, patch

import click
from doozerlib.cli.fbc_merge import fbc_merge_layered
from doozerlib.runtime import Runtime


class TestFbcMergeLayeredCli(unittest.IsolatedAsyncioTestCase):
    async def test_skip_rebase_bypasses_catalog_preparation(self):
        runtime = Mock(spec=Runtime)
        runtime.product = 'quay'
        runtime.group = 'quay-3.18'
        runtime.upcycle = False
        for skip_rebase, branch in ((False, None), (True, None), (False, 'codex/prod-4-22'), (True, 'codex/prod-4-22')):
            with self.subTest(skip_rebase=skip_rebase, branch=branch):
                args = ['--fbc-build=quay-operator-fbc-3.18.1-1.ocp4.20', '--major-minor=4.20']
                if skip_rebase:
                    args.append('--skip-rebase')
                if branch:
                    args.append('--fbc-branch=' + branch)
                with fbc_merge_layered.make_context('beta:fbc:merge-layered', args) as context:
                    options = context.params
                merger = Mock(prepare=AsyncMock(), merge=AsyncMock(return_value={'dry_run': False}))
                with (
                    patch('doozerlib.cli.fbc_merge.LayeredFbcMerger', return_value=merger) as factory,
                    patch('doozerlib.cli.fbc_merge.resolve_konflux_kubeconfig_by_product', return_value=None),
                    patch('doozerlib.cli.fbc_merge.click.echo'),
                ):
                    await inspect.unwrap(fbc_merge_layered.callback)(runtime, **options)
                factory.assert_called_once()
                self.assertEqual(factory.call_args.kwargs['fbc_branch'], branch)
                if skip_rebase or branch:
                    merger.prepare.assert_not_awaited()
                else:
                    merger.prepare.assert_awaited_once_with(options['builds'])
                merger.merge.assert_awaited_once_with(options['image_repo'], skip_rebase=skip_rebase or bool(branch))

    async def test_empty_build_inputs_require_custom_branch(self):
        runtime = Mock(spec=Runtime, product='quay', group='quay-3.16', upcycle=False)
        for branch in (None, 'codex/production'):
            with self.subTest(branch=branch):
                args = ['--major-minor=4.22']
                if branch:
                    args.append('--fbc-branch=' + branch)
                with fbc_merge_layered.make_context('beta:fbc:merge-layered', args) as context:
                    options = context.params
                merger = Mock(prepare=AsyncMock(), merge=AsyncMock(return_value={'dry_run': True}))
                with (
                    patch('doozerlib.cli.fbc_merge.LayeredFbcMerger', return_value=merger),
                    patch('doozerlib.cli.fbc_merge.resolve_konflux_kubeconfig_by_product', return_value=None),
                    patch('doozerlib.cli.fbc_merge.click.echo'),
                ):
                    if branch:
                        await inspect.unwrap(fbc_merge_layered.callback)(runtime, **options)
                        merger.prepare.assert_not_awaited()
                        merger.merge.assert_awaited_once_with(options['image_repo'], skip_rebase=True)
                    else:
                        with self.assertRaisesRegex(click.ClickException, 'unless --fbc-branch is set'):
                            await inspect.unwrap(fbc_merge_layered.callback)(runtime, **options)
