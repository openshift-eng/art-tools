"""Unit tests for quay_doomsday_backup pipeline."""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from pyartcd.pipelines.quay_doomsday_backup import QuayDoomsdaySync
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient


class TestQuayDoomsdaySync(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = Mock(spec=Runtime)
        self.runtime.dry_run = False
        self.runtime.logger = Mock()
        self.mock_slack_client = Mock(spec=SlackClient)
        self.mock_slack_client.say_in_thread = AsyncMock(
            return_value={"channel": "C123", "message": {"ts": "1234.5678"}}
        )
        self.mock_slack_client._client = Mock()
        self.mock_slack_client._client.reactions_add = AsyncMock()
        self.runtime.new_slack_client = Mock(return_value=self.mock_slack_client)
        self.version = "4.15.5"
        self.arches = "x86_64,s390x"

    def _make_pipeline(self) -> QuayDoomsdaySync:
        return QuayDoomsdaySync(runtime=self.runtime, version=self.version, arches=self.arches)

    @patch("pyartcd.pipelines.quay_doomsday_backup.mkdirs")
    async def test_run_succeeds_when_all_arches_sync(self, _mkdirs_mock):
        pipeline = self._make_pipeline()
        pipeline.sync_arch = AsyncMock(return_value=True)

        await pipeline.run()

        self.assertEqual(pipeline.sync_arch.await_count, 2)
        self.mock_slack_client._client.reactions_add.assert_awaited_once()

    @patch("pyartcd.pipelines.quay_doomsday_backup.mkdirs")
    async def test_run_raises_when_some_arches_fail(self, _mkdirs_mock):
        pipeline = self._make_pipeline()
        pipeline.sync_arch = AsyncMock(side_effect=[True, False])

        with self.assertRaisesRegex(RuntimeError, "Failed to sync arches for 4.15.5: s390x"):
            await pipeline.run()

        self.mock_slack_client.say_in_thread.assert_awaited()
        self.mock_slack_client._client.reactions_add.assert_not_awaited()

    @patch("pyartcd.pipelines.quay_doomsday_backup.mkdirs")
    async def test_run_raises_when_all_arches_fail(self, _mkdirs_mock):
        pipeline = self._make_pipeline()
        pipeline.sync_arch = AsyncMock(return_value=False)

        with self.assertRaisesRegex(RuntimeError, "Failed to sync arches for 4.15.5: x86_64, s390x"):
            await pipeline.run()

        self.mock_slack_client.say_in_thread.assert_awaited()
        self.mock_slack_client._client.reactions_add.assert_not_awaited()
