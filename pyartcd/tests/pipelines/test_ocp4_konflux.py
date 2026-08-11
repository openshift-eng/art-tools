#!/usr/bin/env python3

import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline
from pyartcd.runtime import Runtime


class TestRecordMassRebuildStart(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.doozer_working = '/tmp/doozer_working'
        self.runtime.new_slack_client.return_value = AsyncMock()
        self.pipeline = KonfluxOcpPipeline(
            runtime=self.runtime,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            image_build_strategy='none',
            image_list='',
            rpm_build_strategy='none',
            rpm_list='',
            version='4.21',
            build_priority='auto',
        )

    @patch('pyartcd.pipelines.ocp4_konflux.redis.set_value')
    async def test_records_timestamp_for_group(self, mock_set_value):
        await self.pipeline._record_mass_rebuild_start()

        mock_set_value.assert_called_once()
        key, value = mock_set_value.call_args.args
        self.assertEqual(key, 'appdata:konflux:last-mass-rebuild-start:4.21')
        datetime.fromisoformat(value)  # should not raise
        self.assertEqual(mock_set_value.call_args.kwargs.get('expiry'), 60 * 60 * 24 * 7)


if __name__ == '__main__':
    unittest.main()
