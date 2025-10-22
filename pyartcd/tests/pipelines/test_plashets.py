import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.build_plashets import BuildPlashetsPipeline


class TestBuildCompose(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.jenkins.init_jenkins')
    @patch('pyartcd.jenkins.update_title')
    @patch('pyartcd.locks.LockManager.from_lock', return_value=AsyncMock)
    @patch('pyartcd.pipelines.build_plashets.build_plashets', return_value=AsyncMock)
    @patch('pyartcd.jenkins.start_sync_for_ci')
    @patch('pyartcd.jenkins.start_rhcos')
    async def test_build_plashets(self, mocked_rhcos, mocked_sync_for_ci, mocked_build_plashets, mocked_lm, *_):
        build_plashets_pipeline = BuildPlashetsPipeline(
            runtime=MagicMock(dry_run=False),
            group='openshift-4.13',
            release='bogus',
            assembly='stream',
            data_path='',
            data_gitref='',
        )

        mocked_cm = AsyncMock()
        mocked_cm.__aenter__ = AsyncMock()
        mocked_cm.__aexit__ = AsyncMock()

        mocked_lm.return_value = AsyncMock()
        mocked_lm.return_value.lock.return_value = mocked_cm

        mocked_build_plashets.return_value = {}

        # Build not permitted
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=False)
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_not_awaited()
        mocked_rhcos.assert_not_called()
        mocked_sync_for_ci.assert_not_called()

        # Build permitted, assembly != 'stream'
        mocked_build_plashets.reset_mock()
        mocked_rhcos.reset_mock()
        mocked_sync_for_ci.reset_mock()
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=True)
        build_plashets_pipeline.assembly = 'test'
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_awaited_once()
        mocked_rhcos.assert_not_called()
        mocked_sync_for_ci.assert_not_called()

        # Build permitted, assembly = 'stream'
        mocked_build_plashets.reset_mock()
        mocked_rhcos.reset_mock()
        mocked_sync_for_ci.reset_mock()
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=True)
        build_plashets_pipeline.assembly = 'stream'
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_awaited_once()
        mocked_sync_for_ci.assert_called_once_with(version='4.13', block_until_building=False)
