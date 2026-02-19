"""Shared utility for running ``oc image info`` with Redis caching.

For sha256-pinned pullspecs (containing ``@sha256:``), the raw JSON output
is cached in Redis (when credentials are available) to avoid redundant
registry lookups across processes and executions.

Usage::

    from artcommonlib.oc_image_info import oc_image_info__cached, oc_image_info__cached_async

    # Sync
    stdout = oc_image_info__cached(pullspec, '--filter-by-os=amd64')
    image_info = json.loads(stdout)

    # Async
    stdout = await oc_image_info__cached_async(pullspec, '--show-multiarch')
    image_info = json.loads(stdout)

For callers that don't expect the image to change during the process lifetime,
``__lru`` variants add an in-memory ``lru_cache`` / ``alru_cache`` on top::

    from artcommonlib.oc_image_info import oc_image_info__cached__lru, oc_image_info__cached_async__lru
"""

import hashlib
import logging
import os
from functools import lru_cache
from typing import Optional, Tuple

from artcommonlib import exectools
from async_lru import alru_cache
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

# Cached entries expire after 60 days (seconds).
_CACHE_EXPIRY_SECONDS = 60 * 24 * 60 * 60

# Bump to invalidate every cached entry at once.
_CACHE_KEY_VERSION = "v1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FORBIDDEN_OPTION_PREFIXES = ("-o", "--output", "-a", "--registry-config")


def _make_cache_key(pullspec: str, options: tuple[str, ...]) -> str:
    """Derive a deterministic Redis key from the pullspec and oc options.

    Options are sorted so that argument order does not matter.

    ``-o`` / ``--output`` must not appear because the caller is expected to
    let :func:`_build_cmd` add ``-o json`` unconditionally.
    ``-a`` / ``--registry-config`` must not appear because authentication
    is handled via the separate *registry_config* parameter and must be
    excluded from the cache key (it does not affect the output).

    :raises ValueError: If a forbidden option is detected.
    """
    for opt in options:
        for prefix in _FORBIDDEN_OPTION_PREFIXES:
            # Match exact flag (e.g. "-o") or flag=value (e.g. "-o=json", "--registry-config=/path")
            if opt == prefix or opt.startswith(f"{prefix}=") or opt.startswith(f"{prefix} "):
                raise ValueError(
                    f"Option {opt!r} must not be passed in *options*. "
                    f"'-o json' is added automatically; use the registry_config parameter for auth."
                )

    key_input = f"{pullspec}\0{chr(0).join(sorted(options))}"
    digest = hashlib.sha256(key_input.encode()).hexdigest()
    return f"oc_image_info:{_CACHE_KEY_VERSION}:{digest}"


def _is_cacheable(pullspec: str) -> bool:
    """Images referenced by sha256 digest are immutable and safe to cache."""
    return "@sha256:" in pullspec


def _redis_available() -> bool:
    """Return True when Redis credentials are present in the environment."""
    return bool(os.environ.get("REDIS_SERVER_PASSWORD"))


def _build_cmd(pullspec: str, options: Tuple[str, ...], registry_config: Optional[str]) -> list[str]:
    """Build the ``oc image info`` command list."""
    cmd: list[str] = ["oc", "image", "info", "-o", "json"]
    cmd.extend(options)
    if registry_config:
        cmd.append(f"--registry-config={registry_config}")
    cmd.append(pullspec)
    return cmd


def _cache_context(pullspec: str, options: Tuple[str, ...]):
    """Return ``(use_cache, cache_key)`` for the given request.

    If caching is not applicable, *cache_key* is an empty string (never used).
    """
    use_cache = _is_cacheable(pullspec) and _redis_available()
    cache_key = _make_cache_key(pullspec, options) if use_cache else ""
    return use_cache, cache_key


def _redis_get(cache_key: str, pullspec: str) -> Optional[str]:
    """Try to read a cached value from Redis (sync). Returns None on miss or error."""
    try:
        from artcommonlib import redis as art_redis

        cached = art_redis.get_value_sync(cache_key)
        if cached is not None:
            logger.debug("Redis cache hit for oc image info: %s", pullspec)
            return cached
    except Exception:
        logger.debug("Redis cache read failed for %s", pullspec, exc_info=True)
    return None


def _redis_set(cache_key: str, value: str, pullspec: str) -> None:
    """Try to write a value to Redis (sync). Errors are logged and swallowed."""
    try:
        from artcommonlib import redis as art_redis

        art_redis.set_value_sync(cache_key, value, expiry=_CACHE_EXPIRY_SECONDS)
        logger.debug("Cached oc image info result in Redis for %s", pullspec)
    except Exception:
        logger.debug("Redis cache write failed for %s", pullspec, exc_info=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
def oc_image_info__cached(
    pullspec: str,
    *options: str,
    registry_config: Optional[str] = None,
) -> str:
    """Run ``oc image info -o json [options] <pullspec>`` and return stdout.

    When the pullspec contains ``@sha256:`` and Redis credentials are present,
    the raw output is cached so that subsequent calls — even from different
    processes — skip the registry round-trip.

    Retries up to 3 times (with a 10 s wait) before giving up.

    :param pullspec: Image pull specification.
    :param options: Extra ``oc image info`` flags, e.g. ``'--filter-by-os=amd64'``,
        ``'--show-multiarch'``.  Do **not** pass ``-o json`` (always added) or
        authentication flags (use *registry_config* instead).
    :param registry_config: Optional path to a registry auth config file.
        Excluded from the cache key because it does not change the output.
    :returns: Raw JSON stdout from ``oc image info``.
    :raises ChildProcessError: If the command fails after all retry attempts.
    """
    use_cache, cache_key = _cache_context(pullspec, options)

    if use_cache:
        cached = _redis_get(cache_key, pullspec)
        if cached is not None:
            return cached

    rc, stdout, stderr = exectools.cmd_gather(_build_cmd(pullspec, options, registry_config))
    if rc != 0:
        raise ChildProcessError(f"oc image info failed (rc={rc}): {stderr}")

    if use_cache:
        _redis_set(cache_key, stdout, pullspec)

    return stdout


@lru_cache(maxsize=1000)
def oc_image_info__cached__lru(
    pullspec: str,
    *options: str,
    registry_config: Optional[str] = None,
) -> str:
    """Like :func:`oc_image_info__cached` but with an additional
    process-local ``lru_cache`` so repeated calls within the same execution
    skip both the Redis and registry round-trips.

    Use this when you do not expect the image to change during the process
    lifetime (e.g. sha256-pinned pullspecs or tag lookups within a single
    doozer run).
    """
    return oc_image_info__cached(pullspec, *options, registry_config=registry_config)


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
async def oc_image_info__cached_async(
    pullspec: str,
    *options: str,
    registry_config: Optional[str] = None,
) -> str:
    """Async variant of :func:`oc_image_info__cached`.

    Uses synchronous Redis calls (which are extremely fast for simple
    GET/SET operations) to keep a single code path for cache logic.
    """
    use_cache, cache_key = _cache_context(pullspec, options)

    if use_cache:
        cached = _redis_get(cache_key, pullspec)
        if cached is not None:
            return cached

    rc, stdout, stderr = await exectools.cmd_gather_async(
        _build_cmd(pullspec, options, registry_config),
        check=False,
    )
    if rc != 0:
        raise ChildProcessError(f"oc image info failed (rc={rc}): {stderr}")

    if use_cache:
        _redis_set(cache_key, stdout, pullspec)

    return stdout


@alru_cache(maxsize=1000)
async def oc_image_info__cached_async__lru(
    pullspec: str,
    *options: str,
    registry_config: Optional[str] = None,
) -> str:
    """Like :func:`oc_image_info__cached_async` but with an additional
    process-local ``alru_cache`` so repeated calls within the same execution
    skip both the Redis and registry round-trips.

    Use this when you do not expect the image to change during the process
    lifetime (e.g. sha256-pinned pullspecs or tag lookups within a single
    doozer run).
    """
    return await oc_image_info__cached_async(pullspec, *options, registry_config=registry_config)
