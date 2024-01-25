import enum
import logging
from types import coroutine
from tenacity import retry, retry_if_exception_type, wait_fixed, stop_after_attempt, TryAgain

from aioredlock import Aioredlock, LockError

from artcommonlib import redis


# Defines the pipeline locks managed by Redis
class Lock(enum.Enum):
    OLM_BUNDLE = 'lock:olm-bundle-{version}'
    MIRRORING_RPMS = 'lock:mirroring-rpms:{version}'
    PLASHET = 'lock:compose:{assembly}:{version}'
    BUILD = 'lock:build:{version}'
    MASS_REBUILD = 'lock:mass-rebuild-serializer'
    SIGNING = 'lock:signing:{signing_env}'
    BUILD_SYNC = 'lock:build-sync:{version}'


class Keys(enum.Enum):
    MASS_REBUILD_QUEUE = 'appdata:mass-rebuild-queue'


# This constant defines for each lock type:
# - how many times the lock manager should try to acquire the lock before giving up
# - the sleep interval between two consecutive retries, in seconds
# - a timeout, after which the lock will expire and clear itself
LOCK_POLICY = {
    Lock.OLM_BUNDLE: {
        'retry_count': 36000,
        'retry_delay_min': 0.1,
        'lock_timeout': 60 * 60 * 4,  # 4 hours
    },
    Lock.MIRRORING_RPMS: {
        'retry_count': 36000,
        'retry_delay_min': 0.1,
        'lock_timeout': 60 * 60 * 6,  # 6 hours
    },
    Lock.PLASHET: {
        'retry_count': 36000,
        'retry_delay_min': 0.1,
        'lock_timeout': 60 * 60 * 12,  # 12 hours
    },
    Lock.BUILD: {
        'retry_count': 36000 * 1,
        'retry_delay_min': 0.1,
        'lock_timeout': 60 * 60 * 24,  # 24 hours
    },
    Lock.MASS_REBUILD: {
        'retry_count': 36000 * 8,
        'retry_delay_min': 0.1,
        'lock_timeout': 60 * 60 * 24,  # 24 hours
    },
    Lock.SIGNING: {
        'retry_count': 36000,
        'retry_delay_min': 0.1,
        'lock_timeout': 60 * 60 * 2,  # 2 hour
    },
    Lock.BUILD_SYNC: {
        'retry_count': 36000,
        'retry_delay_min': 0.1,
        'lock_timeout': 60 * 60 * 2,  # 2 hour
    },
}


class LockManager(Aioredlock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger('pyartcd')

    @staticmethod
    def from_lock(lock: Lock, use_ssl=True):
        """
        Builds and returns a new aioredlock.Aioredlock instance. Requires following env vars to be defined:
        - REDIS_SERVER_PASSWORD: authentication token to the Redis server
        - REDIS_HOST: hostname where Redis is deployed
        - REDIS_PORT: port where Redis is exposed

        If use_ssl is set, we assume Redis server is using a secure connection, and the protocol will be rediss://
        Otherwise, it will fall back to the unsecure redis://

        'lock' identifies the desired lock from an Enum class. Each lock is associated with a 'lock_policy' object;
        'lock_policy' is a dictionary that maps the behavioral features of the lock manager.
         It needs to be structured as:

        lock_policy = {
            'retry_count': int,
            'retry_delay_min': float,
            'lock_timeout': float
        }

        where:
        - lock_timeout represents the expiration date in seconds
          of all the locks instantiated on this LockManager instance.
        - retry_count is the number of attempts to acquire the lock.
          If exceeded, the lock operation will throw an Exception
        - retry_delay is the delay time in seconds between two consecutive attempts to acquire a resource

        Altogether, if the resource cannot be acquired in (retry_count * retry_delay), the lock operation will fail.
        """

        lock_policy = LOCK_POLICY[lock]
        return LockManager(
            [redis.redis_url(use_ssl)],
            internal_lock_timeout=lock_policy['lock_timeout'],
            retry_count=lock_policy['retry_count'],
            retry_delay_min=lock_policy['retry_delay_min']
        )

    async def lock(self, resource, *args, **kwargs):
        self.logger.info('Trying to acquire lock %s', resource)
        lock = await super().lock(resource, *args, **kwargs)
        self.logger.info('Acquired resource %s', lock.resource)
        return lock

    async def unlock(self, lock):
        try:
            self.logger.info('Releasing lock "%s"', lock.resource)
            await super().unlock(lock)
            self.logger.info('Lock released')
        except LockError:
            self.logger.warning('Failed releasing lock %s', lock.resource)
            raise

    async def get_lock_id(self, resource) -> str:
        self.logger.debug('Retrieving identifier for lock %s', resource)
        return await redis.get_value(resource)

    async def get_locks(self, version: str = None):
        """
        All locks stored in Redis follow the pattern <lock-name>-lock-<ocp-version>
        If version is provided, return all the locks acquired for that version.
        Otherwise, just return all locks currently acquired in Redis
        """

        if version:
            pattern = f'lock:*:{version}'
        else:
            pattern = 'lock:*'

        self.logger.info('Retrieving locks matching pattern "%s"', pattern)
        return await redis.get_keys(pattern)


async def enqueue_for_lock(coro: coroutine, lock: Lock, lock_name: str, lock_id: str,
                           ocp_version: str, version_queue_name):
    lock_manager = LockManager.from_lock(lock)
    return await _enqueue_for_lock(coro, lock_manager, lock, lock_name, lock_id, ocp_version, version_queue_name)


@retry(
    wait=wait_fixed(600),  # wait for 10 minutes between retries
    stop=stop_after_attempt(600 * 24),  # wait for 24 hours
    retry=retry_if_exception_type(TryAgain),
)
async def _enqueue_for_lock(coro: coroutine, lock_manager, lock: Lock, lock_name: str, lock_id: str,
                            ocp_version: str, version_queue_name):
    if not await lock_manager.is_locked(lock_name):
        # TODO: use a redis tx here
        # fetch the first element in the reverse sorted set by score (item with the max score)
        result = await redis.call('zrange', version_queue_name, 0, 0, desc=True)
        # there should be at least 1 entry, if not error out
        version_on_top = result[0]
        if version_on_top == ocp_version:
            # remove yourself from the queue
            await redis.call('zpopmax', version_queue_name)
            return await run_with_lock(coro, lock, lock_name, lock_id)
        else:
            lock_manager.logger.info(f"Do not have priority for {lock_name} as {version_on_top} is ahead. Waiting ..")

    lock_manager.logger.info(f"Do not have priority for {lock_name} Waiting ..")
    raise TryAgain()


async def run_with_lock(coro: coroutine, lock: Lock, lock_name: str, lock_id: str = None, skip_if_locked: bool = False):
    """
    Tries to acquire a lock then awaits the provided coroutine object
    :param coro: coroutine to be awaited
    :param lock: enum object of Lock kind
    :param lock_name: string to be attached to the lock object
    :param lock_id: lock identifier. If None, will auto-generate one
    :param skip_if_locked: do not wait if resource is already locked, just skip the task
    """

    lock_manager = LockManager.from_lock(lock)

    try:
        if skip_if_locked and await lock_manager.is_locked(lock_name):
            lock_manager.logger.info('Looks like there is another task ongoing -- skipping for this run')
            coro.close()
            return

        async with await lock_manager.lock(resource=lock_name, lock_identifier=lock_id):
            return await coro

    except LockError as e:
        lock_manager.logger.error('Failed acquiring lock %s: %s', lock_name, e)
        raise

    finally:
        await lock_manager.destroy()
