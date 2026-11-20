from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import redis.exceptions
from artcommonlib import constants
from artcommonlib import redis as redismod
from tenacity import wait_none


def _coroutine_ping(*behaviors):
    """
    Mimic redis.asyncio.Redis.ping: a regular (non-async) callable that returns a fresh
    coroutine each call. Using this shape (instead of AsyncMock) ensures the retry only
    works via AsyncRetrying; it would regress to a no-op if applied to ping directly.
    """
    behaviors = list(behaviors)
    state = {"i": 0}

    def ping():
        behavior = behaviors[min(state["i"], len(behaviors) - 1)]
        state["i"] += 1

        async def _call():
            if isinstance(behavior, BaseException):
                raise behavior
            return behavior

        return _call()

    return MagicMock(side_effect=ping)


def _make_conn(ping, keys=None):
    conn = MagicMock()
    conn.ping = ping
    conn.keys = AsyncMock(return_value=keys if keys is not None else [])
    conn.aclose = AsyncMock()
    return conn


class TestHandleConnectionAsync(IsolatedAsyncioTestCase):
    def setUp(self):
        self.env = patch.dict("os.environ", {"REDIS_SERVER_PASSWORD": "secret"})
        self.env.start()
        # Override the connect wait test-locally; patch.dict restores it afterwards.
        self.wait = patch.dict(redismod._REDIS_CONNECT_RETRY, {"wait": wait_none()})
        self.wait.start()

    def tearDown(self):
        self.wait.stop()
        self.env.stop()

    @patch("artcommonlib.redis.redis.asyncio.from_url", new_callable=AsyncMock)
    async def test_connect_retries_then_succeeds(self, from_url):
        conn = _make_conn(_coroutine_ping(redis.exceptions.TimeoutError("boom"), True), keys=["a", "b"])
        from_url.return_value = conn

        result = await redismod.get_keys("count*")

        self.assertEqual(result, ["a", "b"])
        self.assertEqual(conn.ping.call_count, 2)
        conn.keys.assert_awaited_once_with("count*")
        conn.aclose.assert_awaited_once()

    @patch("artcommonlib.redis.redis.asyncio.from_url", new_callable=AsyncMock)
    async def test_connect_retry_exhaustion_raises(self, from_url):
        conn = _make_conn(_coroutine_ping(redis.exceptions.ConnectionError("down")))
        from_url.return_value = conn

        with self.assertRaises(redis.exceptions.ConnectionError):
            await redismod.get_keys("count*")

        self.assertEqual(conn.ping.call_count, 3)
        conn.keys.assert_not_awaited()
        conn.aclose.assert_awaited_once()

    @patch("artcommonlib.redis.redis.asyncio.from_url", new_callable=AsyncMock)
    async def test_authentication_error_not_retried(self, from_url):
        conn = _make_conn(_coroutine_ping(redis.exceptions.AuthenticationError("bad password")))
        from_url.return_value = conn

        with self.assertRaises(redis.exceptions.AuthenticationError):
            await redismod.get_keys("count*")

        self.assertEqual(conn.ping.call_count, 1)
        conn.keys.assert_not_awaited()
        conn.aclose.assert_awaited_once()

    @patch("artcommonlib.redis.redis.asyncio.from_url", new_callable=AsyncMock)
    async def test_command_invoked_exactly_once_and_not_replayed(self, from_url):
        conn = _make_conn(_coroutine_ping(True))
        conn.keys = AsyncMock(side_effect=redis.exceptions.TimeoutError("lost reply"))
        from_url.return_value = conn

        with self.assertRaises(redis.exceptions.TimeoutError):
            await redismod.get_keys("count*")

        self.assertEqual(conn.ping.call_count, 1)
        self.assertEqual(conn.keys.await_count, 1)
        conn.aclose.assert_awaited_once()

    @patch("artcommonlib.redis.redis.asyncio.from_url", new_callable=AsyncMock)
    async def test_connect_retry_log_omits_host_and_port(self, from_url):
        host_port = "my-redis.example.internal:6379"
        conn = _make_conn(_coroutine_ping(redis.exceptions.ConnectionError(f"Error connecting to {host_port}"), True))
        from_url.return_value = conn

        with self.assertLogs("artcommonlib.redis", level="WARNING") as logs:
            await redismod.get_keys("count*")

        output = "\n".join(logs.output)
        self.assertNotIn("my-redis.example.internal", output)
        self.assertNotIn("6379", output)
        self.assertNotIn(constants.REDIS_HOST, output)
        self.assertNotIn(str(constants.REDIS_PORT), output)
        self.assertIn("ConnectionError", output)

    @patch("artcommonlib.redis.redis.asyncio.from_url", new_callable=AsyncMock)
    async def test_connect_retry_debug_logs_omit_host_and_port(self, from_url):
        host_port = "my-redis.example.internal:6379"
        conn = _make_conn(_coroutine_ping(redis.exceptions.ConnectionError(f"Error connecting to {host_port}"), True))
        from_url.return_value = conn

        with self.assertLogs("artcommonlib.redis", level="DEBUG") as logs:
            await redismod.get_keys("count*")

        # assertLogs formats each record (appending any exc_info traceback), so this covers exception text too.
        output = "\n".join(logs.output)
        self.assertNotIn("my-redis.example.internal", output)
        self.assertNotIn("6379", output)
        self.assertNotIn(constants.REDIS_HOST, output)
        self.assertNotIn(str(constants.REDIS_PORT), output)


class TestHandleConnectionSync(TestCase):
    def setUp(self):
        self.env = patch.dict("os.environ", {"REDIS_SERVER_PASSWORD": "secret"})
        self.env.start()
        self.wait = patch.dict(redismod._REDIS_CONNECT_RETRY, {"wait": wait_none()})
        self.wait.start()

    def tearDown(self):
        self.wait.stop()
        self.env.stop()

    @patch("artcommonlib.redis.redis.from_url")
    def test_sync_connect_retries_then_succeeds(self, from_url):
        conn = MagicMock()
        conn.ping.side_effect = [redis.exceptions.TimeoutError("boom"), True]
        conn.get.return_value = "value"
        from_url.return_value = conn

        result = redismod.get_value_sync("key")

        self.assertEqual(result, "value")
        self.assertEqual(conn.ping.call_count, 2)
        conn.get.assert_called_once_with("key")
        conn.close.assert_called_once()

    @patch("artcommonlib.redis.redis.from_url")
    def test_sync_authentication_error_not_retried(self, from_url):
        conn = MagicMock()
        conn.ping.side_effect = redis.exceptions.AuthenticationError("bad password")
        from_url.return_value = conn

        with self.assertRaises(redis.exceptions.AuthenticationError):
            redismod.get_value_sync("key")

        self.assertEqual(conn.ping.call_count, 1)
        conn.get.assert_not_called()
        conn.close.assert_called_once()

    @patch("artcommonlib.redis.redis.from_url")
    def test_sync_command_invoked_exactly_once(self, from_url):
        conn = MagicMock()
        conn.ping.return_value = True
        conn.get.side_effect = redis.exceptions.TimeoutError("lost reply")
        from_url.return_value = conn

        with self.assertRaises(redis.exceptions.TimeoutError):
            redismod.get_value_sync("key")

        self.assertEqual(conn.get.call_count, 1)
        conn.close.assert_called_once()
