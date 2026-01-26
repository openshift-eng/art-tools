from unittest import TestCase
from unittest.mock import patch

from pyartcd.locks import DEFAULT_LOCK_TIMEOUT, LOCK_POLICY, Lock, LockManager


class TestLocks(TestCase):
    def test_lock_policy(self):
        lock: Lock = Lock.BUILD
        lock_policy: dict = LOCK_POLICY[lock]
        self.assertEqual(lock_policy["retry_count"], 36000)
        self.assertEqual(lock_policy["retry_delay_min"], 0.1)
        self.assertEqual(lock_policy["lock_timeout"], DEFAULT_LOCK_TIMEOUT)

    def test_lock_name(self):
        lock: Lock = Lock.BUILD
        lock_name = lock.value.format(version="4.14")
        self.assertEqual(lock_name, "lock:build:4.14")

    @patch("artcommonlib.redis.redis_url", return_value="fake_url")
    @patch("aioredlock.algorithm.Aioredlock.__attrs_post_init__")
    def test_lock_manager(self, *_):
        lock: Lock = Lock.PLASHET
        policy = LOCK_POLICY[lock]
        lm = LockManager.from_lock(lock)
        self.assertEqual(lm.retry_count, policy["retry_count"])
        self.assertEqual(lm.retry_delay_min, policy["retry_delay_min"])
        self.assertEqual(lm.internal_lock_timeout, policy["lock_timeout"])
        self.assertEqual(lm.redis_connections, ["fake_url"])
