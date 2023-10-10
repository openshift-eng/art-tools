import logging
import os
import typing
from functools import wraps
from string import Template

import aioredis

logger = logging.getLogger(__name__)

# Redis instance template, to be renderes with env vars
redis = Template('${protocol}://:${redis_password}@${redis_host}:${redis_port}')


class RedisError(Exception):
    pass


def redis_url(use_ssl=True):
    if not os.environ.get('REDIS_SERVER_PASSWORD', None):
        raise RedisError('Please define REDIS_SERVER_PASSWORD env var')
    if not os.environ.get('REDIS_HOST', None):
        raise RedisError('Please define REDIS_HOST env var')
    if not os.environ.get('REDIS_PORT', None):
        raise RedisError('Please define REDIS_PORT env var')

    return redis.substitute(
        protocol='rediss' if use_ssl else 'redis',
        redis_password=os.environ['REDIS_SERVER_PASSWORD'],
        redis_host=os.environ['REDIS_HOST'],
        redis_port=os.environ['REDIS_PORT']
    )


def handle_connection(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        url = redis_url(use_ssl=True)
        conn = await aioredis.create_redis(url, encoding="utf-8")
        res = await func(conn, *args, **kwargs)
        conn.close()
        return res

    return wrapper


@handle_connection
async def get_value(conn: aioredis.commands.Redis, key: str):
    """
    Returns value for a given key
    """

    value = await conn.get(key)
    logger.debug('Key %s has value %s', key, value)
    return value


@handle_connection
async def set_value(conn: aioredis.commands.Redis, key: str, value):
    """
    Sets value for a key
    """

    logger.debug('Setting key %s to %s', key, value)
    await conn.set(key, value)


@handle_connection
async def get_keys(conn: aioredis.commands.Redis, pattern: str):
    """
    Returns a list of keys (string) matching pattern (e.g. "*.count")
    """

    keys = await conn.keys(pattern)
    logger.debug('Found keys matching pattern %s: %s', pattern, ', '.join(keys))
    return keys


@handle_connection
async def delete_key(conn: aioredis.commands.Redis, key: str) -> int:
    """
    Deletes given key from Redis DB
    Returns: 1 if successful, 0 otherwise
    """

    logger.debug('Deleting key %s', key)
    res = await conn.delete(key)
    logger.debug('Key %s %s', key, 'deleted' if res else 'not found')
    return res


@handle_connection
async def list_push(conn: aioredis.commands.Redis, key: str, value: str) -> None:
    """
    Push to a list in Redis, like a queue (FIFO). Docs: https://redis.io/docs/data-types/lists/
    """

    logger.debug('Pushing value %s to list %s', value, key)
    await conn.lpush(key, value)


@handle_connection
async def list_pop(conn: aioredis.commands.Redis, key: str) -> typing.Optional[str]:
    """
    Pop a value from a list, like a queue (FIFO). Docs: https://redis.io/docs/data-types/lists/
    :return: A string if a value is present, else None
    """

    value = await conn.rpop(key)
    logger.debug('List %s has value %s', key, value)
    return value


@handle_connection
async def list_see_all(conn: aioredis.commands.Redis, key: str) -> typing.Optional[list]:
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
