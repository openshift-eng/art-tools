import logging
import os
import typing
from functools import wraps
from string import Template

import redis
from artcommonlib import constants

logger = logging.getLogger(__name__)

# Redis instance template, to be rendered with env vars
redis_url_template = Template('${protocol}://:${redis_password}@${redis_host}:${redis_port}')


class RedisError(Exception):
    pass


def redis_url(use_ssl=True):
    if not os.environ.get('REDIS_SERVER_PASSWORD', None):
        raise RedisError('Please define REDIS_SERVER_PASSWORD env var')

    return redis_url_template.substitute(
        protocol='rediss' if use_ssl else 'redis',
        redis_password=os.environ['REDIS_SERVER_PASSWORD'],
        redis_host=constants.REDIS_HOST,
        redis_port=constants.REDIS_PORT,
    )


def handle_connection(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        url = redis_url(use_ssl=True)
        conn = await redis.asyncio.from_url(url, decode_responses=True)
        res = await func(conn, *args, **kwargs)
        await conn.aclose()
        return res

    return wrapper


def handle_connection_sync(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        url = redis_url(use_ssl=True)
        conn = redis.from_url(url, decode_responses=True)
        res = func(conn, *args, **kwargs)
        conn.close()
        return res

    return wrapper


@handle_connection
async def call(conn: redis.asyncio.client.Redis, func_name: str, *args, **kwargs):
    """
    Call any redis client function with opening and closing of connection auto handled
    call('zadd', name, mapping) -> redis_connection.zadd(name, mapping)
    """
    func = getattr(conn, func_name)
    return await func(*args, **kwargs)


@handle_connection
async def get_value(conn: redis.asyncio.client.Redis, key: str):
    """
    Returns value for a given key
    """

    value = await conn.get(key)
    logger.debug('Key %s has value %s', key, value)
    return value


@handle_connection_sync
def get_value_sync(conn: redis.client.Redis, key: str):
    """
    Same as get_value(), but synchronous
    """

    value = conn.get(key)
    logger.debug('Key %s has value %s', key, value)
    return value


@handle_connection
async def set_value(conn: redis.asyncio.client.Redis, key: str, value, expiry=None):
    """
    Sets value for a key

    expiry optionally sets an expiry flag in seconds
    """

    logger.debug('Setting key %s to %s', key, value)
    await conn.set(key, value, ex=expiry)


@handle_connection_sync
def set_value_sync(conn: redis.client.Redis, key: str, value, expiry=None):
    """
    Same as set_value(), but synchronous
    """

    logger.debug('Setting key %s to %s', key, value)
    conn.set(key, value, ex=expiry)


@handle_connection
async def get_keys(conn: redis.asyncio.client.Redis, pattern: str):
    """
    Returns a list of keys (string) matching pattern (e.g. "count*")
    """

    keys = await conn.keys(pattern)
    logger.debug('Found keys matching pattern %s: %s', pattern, ', '.join(keys))
    return keys


@handle_connection_sync
def get_keys_sync(conn: redis.client.Redis, pattern: str):
    """
    Same as get_keys(), but synchronous
    """

    keys = conn.keys(pattern)
    logger.debug('Found keys matching pattern %s: %s', pattern, ', '.join(keys))
    return keys


@handle_connection
async def delete_key(conn: redis.asyncio.client.Redis, key: str) -> int:
    """
    Deletes given key from Redis DB
    Returns: 1 if successful, 0 otherwise
    """

    logger.debug('Deleting key %s', key)
    res = await conn.delete(key)
    logger.debug('Key %s %s', key, 'deleted' if res else 'not found')
    return res


@handle_connection_sync
def delete_key_sync(conn: redis.asyncio.client.Redis, key: str) -> int:
    """
    Same as delete_key, but synchronous
    """

    logger.debug('Deleting key %s', key)
    res = conn.delete(key)
    logger.debug('Key %s %s', key, 'deleted' if res else 'not found')
    return res


@handle_connection
async def delete_keys_by_pattern(conn: redis.asyncio.client.Redis, pattern: str) -> int:
    """
    Deletes all keys matching the given pattern from Redis DB.

    Arg(s):
        pattern (str): Redis key pattern (e.g., "count:*", "prefix:*:suffix")
    Return Value(s):
        int: Number of keys deleted
    """
    logger.debug('Finding keys matching pattern %s', pattern)
    keys = await conn.keys(pattern)

    if not keys:
        logger.debug('No keys found matching pattern %s', pattern)
        return 0

    logger.debug('Deleting %d keys matching pattern %s', len(keys), pattern)
    res = await conn.delete(*keys)
    logger.debug('%d keys deleted', res)
    return res


@handle_connection_sync
def delete_keys_by_pattern_sync(conn: redis.client.Redis, pattern: str) -> int:
    """
    Same as delete_keys_by_pattern, but synchronous
    """
    logger.debug('Finding keys matching pattern %s', pattern)
    keys = conn.keys(pattern)

    if not keys:
        logger.debug('No keys found matching pattern %s', pattern)
        return 0

    logger.debug('Deleting %d keys matching pattern %s', len(keys), pattern)
    res = conn.delete(*keys)
    logger.debug('%d keys deleted', res)
    return res


@handle_connection
async def list_push(conn: redis.asyncio.client.Redis, key: str, value: str) -> None:
    """
    Push to a list in Redis, like a queue (FIFO). Docs: https://redis.io/docs/data-types/lists/
    """

    logger.debug('Pushing value %s to list %s', value, key)
    await conn.lpush(key, value)


@handle_connection
async def list_pop(conn: redis.asyncio.client.Redis, key: str) -> typing.Optional[str]:
    """
    Pop a value from a list, like a queue (FIFO). Docs: https://redis.io/docs/data-types/lists/
    :return: A string if a value is present, else None
    """

    value = await conn.rpop(key)
    logger.debug('List %s has value %s', key, value)
    return value


@handle_connection
async def list_see_all(conn: redis.asyncio.client.Redis, key: str) -> typing.Optional[list]:
    """
    Get all values from the Redis list. Does not pop values, list will still exist in Redis

    :return: List of values from the key, or None if the list is empty or does not exist.
    """
    values = await conn.lrange(key, 0, -1)

    return values


async def list_push_all(key: str, values: list) -> None:
    """
    Push all values from list to the Redis key
    """
    for value in values:
        await list_push(key, value)


@handle_connection
async def get_multiple_values(conn: redis.asyncio.client.Redis, keys: list[str]):
    """
    Retrieves values for a list of Redis string keys using a single MGET operation.

    :param conn: An active Redis asyncio client connection.
    :param keys: List of Redis keys (strings) to retrieve values for.
    :return: A list of values corresponding to the given keys. If a key does not exist, its value will be None.
    """
    return await conn.mget(keys)
