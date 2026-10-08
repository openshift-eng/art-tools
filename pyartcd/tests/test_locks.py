import asyncio
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import Mock, patch

from aioredlock import Aioredlock
from pyartcd.locks import DEFAULT_LOCK_TIMEOUT, LOCK_POLICY, LOCK_WAIT_LOG_INTERVAL, Lock, LockManager


class TestLocks(TestCase):
    def test_lock_policy(self):
        lock: Lock = Lock.BUILD
        lock_policy: dict = LOCK_POLICY[lock]
        self.assertEqual(lock_policy['retry_count'], 36000)
        self.assertEqual(lock_policy['retry_delay_min'], 0.1)
        self.assertEqual(lock_policy['lock_timeout'], DEFAULT_LOCK_TIMEOUT)

    def test_lock_name(self):
        lock: Lock = Lock.BUILD
        lock_name = lock.value.format(version='4.14')
        self.assertEqual(lock_name, 'lock:build:4.14')

    @patch("artcommonlib.redis.redis_url", return_value='fake_url')
    @patch("aioredlock.algorithm.Aioredlock.__attrs_post_init__")
    def test_lock_manager(self, *_):
        lock: Lock = Lock.PLASHET
        policy = LOCK_POLICY[lock]
        lm = LockManager.from_lock(lock)
        self.assertEqual(lm.retry_count, policy['retry_count'])
        self.assertEqual(lm.retry_delay_min, policy['retry_delay_min'])
        self.assertEqual(lm.internal_lock_timeout, policy['lock_timeout'])
        self.assertEqual(lm.redis_connections, ['fake_url'])


class TestLockWaitLogging(IsolatedAsyncioTestCase):
    async def test_reports_wait_and_stops_after_acquisition(self):
        with patch('aioredlock.algorithm.Aioredlock.__attrs_post_init__'):
            manager = LockManager([])

        scheduled = []

        def schedule(delay, callback):
            handle = Mock()
            scheduled.append((delay, callback, handle))
            return handle

        async def acquire(_manager, _resource, *args, **kwargs):
            scheduled[0][1]()
            return 'lock'

        loop = asyncio.get_running_loop()
        with (
            patch.object(loop, 'call_later', side_effect=schedule),
            patch.object(Aioredlock, 'lock', new=acquire),
            patch.object(manager.logger, 'info') as log_info,
        ):
            lock = await manager.lock('resource')

        self.assertEqual(lock, 'lock')
        self.assertEqual(len(scheduled), 2)
        self.assertEqual(scheduled[0][0], LOCK_WAIT_LOG_INTERVAL)
        log_info.assert_any_call('Still waiting to acquire lock %s', 'resource')
        scheduled[-1][2].cancel.assert_called_once()

    async def test_stops_reporting_after_cancellation(self):
        with patch('aioredlock.algorithm.Aioredlock.__attrs_post_init__'):
            manager = LockManager([])

        handle = Mock()

        async def cancel(_manager, _resource, *args, **kwargs):
            raise asyncio.CancelledError

        loop = asyncio.get_running_loop()
        with (
            patch.object(loop, 'call_later', return_value=handle),
            patch.object(Aioredlock, 'lock', new=cancel),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await manager.lock('resource')

        handle.cancel.assert_called_once()
