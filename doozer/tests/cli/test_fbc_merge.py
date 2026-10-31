import inspect
import unittest
from unittest.mock import AsyncMock, Mock, patch

from doozerlib.cli.fbc_merge import fbc_merge_layered
from doozerlib.runtime import Runtime


class TestFbcMergeLayeredCli(unittest.IsolatedAsyncioTestCase):
    async def test_skip_rebase_bypasses_catalog_preparation(self):
        runtime = Mock(spec=Runtime)
        runtime.product = 'quay'
        runtime.group = 'quay-3.18'
        runtime.upcycle = False
        for skip_rebase in (False, True):
            with self.subTest(skip_rebase=skip_rebase):
                args = ['--fbc-build=quay-operator-fbc-3.18.1-1.ocp4.20', '--major-minor=4.20']
                if skip_rebase:
                    args.append('--skip-rebase')
                with fbc_merge_layered.make_context('beta:fbc:merge-layered', args) as context:
                    options = context.params
                merger = Mock(prepare=AsyncMock(), merge=AsyncMock(return_value={'dry_run': False}))
                with (
                    patch('doozerlib.cli.fbc_merge.LayeredFbcMerger', return_value=merger),
                    patch('doozerlib.cli.fbc_merge.resolve_konflux_kubeconfig_by_product', return_value=None),
                    patch('doozerlib.cli.fbc_merge.click.echo'),
                ):
                    await inspect.unwrap(fbc_merge_layered.callback)(runtime, **options)
                if skip_rebase:
                    merger.prepare.assert_not_awaited()
                else:
                    merger.prepare.assert_awaited_once_with(options['builds'])
                merger.merge.assert_awaited_once_with(options['image_repo'], skip_rebase=skip_rebase)
