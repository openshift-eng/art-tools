from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.release_readiness.checks.nightly import check_nightly_status
from pyartcd.pipelines.release_readiness.models import Status


class TestNightlyStatusCheck(IsolatedAsyncioTestCase):
    async def test_accepted_recently(self):
        """
        Recent accepted nightly -> GREEN.
        """

        recent_time = datetime.now(timezone.utc) - timedelta(hours=6)
        ts = recent_time.strftime("%Y-%m-%d-%H%M%S")
        mock_tags_data = {"tags": [{"name": f"4.21.0-0.nightly-{ts}", "phase": "Accepted"}]}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_tags_data)
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock()

        with patch(
            "pyartcd.pipelines.release_readiness.checks.nightly.aiohttp.ClientSession", return_value=mock_session
        ):
            result = await check_nightly_status(4, 21, "konflux")

        self.assertEqual(result.status, Status.GREEN)

    async def test_no_accepted_nightly(self):
        """
        No accepted nightly -> RED.
        """

        mock_tags_data = {
            "tags": [
                {"name": "4.21.0-0.nightly-2026-07-08-120000", "phase": "Rejected"},
                {"name": "4.21.0-0.nightly-2026-07-08-100000", "phase": "Rejected"},
            ]
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_tags_data)
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock()

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock()

        with patch(
            "pyartcd.pipelines.release_readiness.checks.nightly.aiohttp.ClientSession", return_value=mock_session
        ):
            result = await check_nightly_status(4, 21, "konflux")

        self.assertEqual(result.status, Status.RED)
