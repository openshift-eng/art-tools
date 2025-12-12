import asyncio
import concurrent
import copy
import inspect
import logging
import pprint
import re
import threading
import typing
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

from artcommonlib import bigquery
from artcommonlib.konflux import konflux_build_record
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxRecord
from artcommonlib.util import extract_group_from_nvr
from google.cloud.bigquery import Row, SchemaField
from sqlalchemy import BinaryExpression, Boolean, Column, DateTime, Null, String, func
from sqlalchemy.sql import text

SCHEMA_LEVEL = 1

# Exponential search windows: 7, 14, 28, 56, 112, 224, 448 days
# Doubles each time, covers ~15 months maximum
EXPONENTIAL_SEARCH_WINDOWS = [7, 14, 28, 56, 112, 224, 448]

# Special group name for builder base image cache that aggregates all RHEL specific ocp-build-data groups
BUILDER_BASE_IMAGE_GROUP = "builder_base_image"


class CacheRecordsType(Enum):
    """
    Enum representing the two types of cached records.

    SMALL_COLUMNS: Records without large columns (installed_rpms, installed_packages) - optimized for most queries
    ALL_COLUMNS: Records with all columns including installed_rpms and installed_packages - used when needed
    """

    SMALL_COLUMNS = "small_columns"
    ALL_COLUMNS = "all_columns"

    def __str__(self):
        return self.value

    @property
    def display_name(self):
        """Return capitalized display name for logging."""
        return self.value.replace('_', ' ').title()


@dataclass
class CacheMetrics:
    """Metrics for cache hits and misses."""

    hits: int = 0
    misses: int = 0


class BuildCache:
    """
    Thread-safe in-memory cache of recent builds, per-group.

    Maintains TWO separate caches for each group, lazy-loaded on first access:
    1. Small columns cache: Records without installed_rpms/installed_packages (optimized, most queries)
    2. All columns cache: Records with all columns including installed_rpms/installed_packages

    Stores builds indexed by:
    - group → { name → [builds sorted by start_time desc], nvr → build }
    """

    def __init__(self, cache_days: int = 30):
        # Cache groups indexed by cache type
        # Each cache type has: group → { 'by_name': {}, 'by_nvr': {}, 'oldest': datetime, 'newest': datetime }
        self.cache_groups = {
            CacheRecordsType.SMALL_COLUMNS: {},  # Records without installed_rpms/installed_packages
            CacheRecordsType.ALL_COLUMNS: {},  # Records with all columns
        }

        # Cache metrics indexed by cache type
        self.cache_metrics = {
            CacheRecordsType.SMALL_COLUMNS: CacheMetrics(),
            CacheRecordsType.ALL_COLUMNS: CacheMetrics(),
        }

        self._lock = threading.RLock()
        self._cache_days = cache_days
        self.logger = logging.getLogger(__name__)

    def _increment_hit(self, cache_type: CacheRecordsType):
        """Increment the appropriate hit counter."""
        self.cache_metrics[cache_type].hits += 1

    def _increment_miss(self, cache_type: CacheRecordsType):
        """Increment the appropriate miss counter."""
        self.cache_metrics[cache_type].misses += 1

    def get_group_cache(self, group: str, cache_type: CacheRecordsType = CacheRecordsType.SMALL_COLUMNS) -> dict:
        """
        Get the cache for a specific group, creating it if it doesn't exist.

        :param group: Group name
        :param cache_type: Type of cache (SMALL_COLUMNS or ALL_COLUMNS)
        :return: The group's cache dict
        """
        groups = self.cache_groups[cache_type]
        if group not in groups:
            groups[group] = {
                "by_name": defaultdict(list),
                "by_nvr": {},
                "oldest": None,
                "newest": None,
                "total_builds": 0,
            }
        return groups[group]

    def add_builds(
        self,
        builds: typing.List[KonfluxRecord],
        group: typing.Optional[str] = None,
        cache_type: CacheRecordsType = CacheRecordsType.SMALL_COLUMNS,
    ):
        """
        Add multiple builds to cache. All builds must be from the same group, except for
        the special builder_base_image group which can contain builds from multiple groups.

        :param builds: List of KonfluxBuildRecord objects to cache (all must have same group unless group is builder_base_image)
        :param group: Optional group name (e.g., 'openshift-4.18'). If not provided, uses builds[0].group
        :param cache_type: Type of cache to add to (SMALL_COLUMNS or ALL_COLUMNS)
        :raises ValueError: If builds are from different groups or group cannot be determined (except for builder_base_image)
        """
        if not builds:
            return

        # Determine group from first build if not provided
        if not group:
            group = builds[0].group
            if not group:
                raise ValueError("Cannot add builds to cache: no group provided and builds[0].group is None")

        # Special handling for builder_base_image group - allows builds from multiple groups
        is_builder_base_image = group == BUILDER_BASE_IMAGE_GROUP

        # Verify all builds are from the same group (unless builder_base_image)
        if not is_builder_base_image:
            mismatched_builds = [(build.nvr, build.group) for build in builds if build.group and build.group != group]
            if mismatched_builds:
                raise ValueError(
                    f"All builds must be from group '{group}'. Found builds from different groups: {mismatched_builds}"
                )

        with self._lock:
            group_cache = self.get_group_cache(group, cache_type=cache_type)

            for build in builds:
                # Index by name
                group_cache['by_name'][build.name].append(build)

                # Index by NVR - prioritize successful builds over failed ones
                existing_build = group_cache['by_nvr'].get(build.nvr)
                if existing_build is None:
                    # No existing build with this NVR, add it
                    group_cache['by_nvr'][build.nvr] = build
                elif (
                    existing_build.outcome != KonfluxBuildOutcome.SUCCESS
                    and build.outcome == KonfluxBuildOutcome.SUCCESS
                ):
                    # Existing build is not successful, but new build is - replace with successful build
                    group_cache['by_nvr'][build.nvr] = build
                elif existing_build.outcome == build.outcome and build.start_time > existing_build.start_time:
                    group_cache['by_nvr'][build.nvr] = build

                # Track time range
                if build.start_time:
                    if group_cache['oldest'] is None or build.start_time < group_cache['oldest']:
                        group_cache['oldest'] = build.start_time
                    if group_cache['newest'] is None or build.start_time > group_cache['newest']:
                        group_cache['newest'] = build.start_time

                group_cache['total_builds'] += 1

            # Sort each name's build list by start_time descending (newest first)
            for name in group_cache['by_name']:
                group_cache['by_name'][name].sort(key=lambda b: b.start_time or datetime.min, reverse=True)

            self.logger.info(
                f"{cache_type.display_name} cache loaded {len(builds)} builds for group '{group}' "
                f"(total: {group_cache['total_builds']})"
            )

    def get_by_nvr(
        self,
        nvr: str,
        group: typing.Optional[str] = None,
        cache_type: CacheRecordsType = CacheRecordsType.SMALL_COLUMNS,
    ) -> typing.Optional[KonfluxRecord]:
        """
        Get specific build by NVR.

        Searches in specified group if provided, otherwise searches across all groups.

        :param nvr: Build NVR
        :param group: Optional group to search in
        :param cache_type: Type of cache to search (SMALL_COLUMNS or ALL_COLUMNS)
        :return: Build record or None
        """
        with self._lock:
            groups = self.cache_groups[cache_type]
            # If group specified, only search that group
            if group:
                if group in groups:
                    build = groups[group]["by_nvr"].get(nvr)
                    if build:
                        self._increment_hit(cache_type)
                        self.logger.debug(f"{cache_type.display_name} cache HIT: NVR {nvr} in group {group}")
                        return build

            # No group specified - search all groups. This includes the builder_base_image group if it has been loaded.
            else:
                for group_name, group_cache in groups.items():
                    build = group_cache["by_nvr"].get(nvr)
                    if build:
                        self._increment_hit(cache_type)
                        self.logger.debug(f"{cache_type.display_name} cache HIT: NVR {nvr} in group {group_name}")
                        return build

            self._increment_miss(cache_type)
            self.logger.debug(f"{cache_type.display_name} cache MISS: NVR {nvr}")
            return None

    def get_by_name(
        self,
        name: str,
        group: str,
        outcome: typing.Optional[typing.Union[KonfluxBuildOutcome, str]] = None,
        assembly: typing.Optional[str] = None,
        el_target: typing.Optional[str] = None,
        artifact_type: typing.Optional[typing.Union[ArtifactType, str]] = None,
        engine: typing.Optional[typing.Union[Engine, str]] = None,
        embargoed: typing.Optional[bool] = None,
        completed_before: typing.Optional[datetime] = None,
        cache_type: CacheRecordsType = CacheRecordsType.SMALL_COLUMNS,
    ) -> typing.Optional[KonfluxRecord]:
        """
        Get latest build for name with optional filters from specified group.

        Returns the most recent build matching all specified criteria.

        :param name: Component name
        :param group: Group name (required)
        :param outcome: Filter by outcome (success/failure) - accepts enum or string
        :param assembly: Filter by assembly
        :param el_target: Filter by el_target (e.g., 'el8', 'el9')
        :param artifact_type: Filter by artifact type (rpm/image) - accepts enum or string
        :param engine: Filter by engine (brew/konflux) - accepts enum or string
        :param embargoed: Filter by embargoed status
        :param completed_before: Filter by completion time (only return builds completed before this time)
        :param cache_type: Type of cache to search (SMALL_COLUMNS or ALL_COLUMNS)
        :return: Latest matching build or None
        """
        # Normalize enum parameters - accept strings or enums
        if outcome is not None and not isinstance(outcome, KonfluxBuildOutcome):
            outcome = KonfluxBuildOutcome(outcome)
        if artifact_type is not None and not isinstance(artifact_type, ArtifactType):
            artifact_type = ArtifactType(artifact_type)
        if engine is not None and not isinstance(engine, Engine):
            engine = Engine(engine)

        with self._lock:
            groups = self.cache_groups[cache_type]
            # Check if group cached
            if group not in groups:
                self._increment_miss(cache_type)
                self.logger.warning(f"{cache_type.display_name} cache MISS: Group {group} not cached")
                return None

            group_cache = groups[group]
            builds = group_cache['by_name'].get(name, [])
            if not builds:
                self._increment_miss(cache_type)
                self.logger.debug(f"{cache_type.display_name} cache MISS: No builds for name {name} in group {group}")
                return None

            # Filter builds by criteria
            for build in builds:  # Already sorted newest first
                # Apply filters
                if outcome is not None and build.outcome != outcome:
                    continue
                if assembly is not None and build.assembly != assembly:
                    continue
                if el_target is not None and build.el_target != el_target:
                    continue
                if artifact_type is not None and build.artifact_type != artifact_type:
                    continue
                if engine is not None and build.engine != engine:
                    continue
                if embargoed is not None and build.embargoed != embargoed:
                    continue
                if completed_before is not None and build.start_time is not None:
                    # Ensure completed_before is timezone-aware for comparison
                    cb_time = (
                        completed_before.astimezone(timezone.utc)
                        if completed_before.tzinfo
                        else completed_before.replace(tzinfo=timezone.utc)
                    )
                    build_time = (
                        build.start_time.astimezone(timezone.utc)
                        if build.start_time.tzinfo
                        else build.start_time.replace(tzinfo=timezone.utc)
                    )
                    if build_time >= cb_time:
                        continue

                # Found matching build
                self._increment_hit(cache_type)
                self.logger.debug(f"{cache_type.display_name} cache HIT: {name} in group {group} with filters")
                return build

            # No matching build found
            self._increment_miss(cache_type)
            self.logger.debug(
                f"{cache_type.display_name} cache MISS: {name} in group {group} with filters (have builds but none match)"
            )
            return None

    def is_group_loaded(self, group: str, cache_type: CacheRecordsType = CacheRecordsType.SMALL_COLUMNS) -> bool:
        """
        Check if group is already loaded in cache.

        :param group: Group name
        :param cache_type: Type of cache to check (SMALL_COLUMNS or ALL_COLUMNS)
        :return: True if group is cached
        """
        with self._lock:
            return group in self.cache_groups[cache_type]

    def stats(self, group: typing.Optional[str] = None) -> dict:
        """
        Get cache statistics.

        :param group: Optional group to get stats for. If None, returns aggregate stats.
        :return: Dictionary with cache stats
        """
        with self._lock:
            # Calculate hit rates for each cache type
            hit_rates = {}
            for cache_type in CacheRecordsType:
                metrics = self.cache_metrics[cache_type]
                total_queries = metrics.hits + metrics.misses
                hit_rates[cache_type] = (metrics.hits / total_queries * 100) if total_queries > 0 else 0

            if group:
                # Group-specific stats
                stats = {"group": group}

                # Add stats for each cache type
                for cache_type in CacheRecordsType:
                    if group in self.cache_groups[cache_type]:
                        group_cache = self.cache_groups[cache_type][group]
                        stats[str(cache_type)] = {
                            "total_builds": group_cache["total_builds"],
                            "unique_names": len(group_cache["by_name"]),
                            "unique_nvrs": len(group_cache["by_nvr"]),
                            "oldest_build": group_cache["oldest"].isoformat() if group_cache["oldest"] else None,
                            "newest_build": group_cache["newest"].isoformat() if group_cache["newest"] else None,
                        }

                # Add global metrics for each cache type
                for cache_type in CacheRecordsType:
                    metrics = self.cache_metrics[cache_type]
                    stats[f"{cache_type}_cache_hits"] = metrics.hits
                    stats[f"{cache_type}_cache_misses"] = metrics.misses
                    stats[f"{cache_type}_hit_rate"] = f"{hit_rates[cache_type]:.1f}%"

                return stats
            else:
                # Aggregate stats across all groups
                stats = {}

                # Add groups cached list for each cache type
                for cache_type in CacheRecordsType:
                    stats[f"{cache_type}_groups_cached"] = list(self.cache_groups[cache_type].keys())

                # Add aggregate stats for each cache type
                for cache_type in CacheRecordsType:
                    groups = self.cache_groups[cache_type]
                    metrics = self.cache_metrics[cache_type]
                    total_builds = sum(g["total_builds"] for g in groups.values())
                    unique_nvrs = sum(len(g["by_nvr"]) for g in groups.values())

                    stats[f"{cache_type}_cache"] = {
                        "total_builds": total_builds,
                        "unique_nvrs": unique_nvrs,
                        "cache_hits": metrics.hits,
                        "cache_misses": metrics.misses,
                        "hit_rate": f"{hit_rates[cache_type]:.1f}%",
                    }

                return stats

    def clear(
        self,
        group: typing.Optional[str] = None,
        cache_type: typing.Optional[CacheRecordsType] = None,
    ):
        """
        Clear cached data.

        :param group: Optional group to clear. If None, clears all groups.
        :param cache_type: If specified, clear only that cache type (SMALL_COLUMNS or ALL_COLUMNS).
                          If None, clear both caches.
        """
        with self._lock:
            # Determine which cache types to clear
            types_to_clear = [cache_type] if cache_type else list(CacheRecordsType)

            if group:
                # Clear specific group
                cleared = []
                for ct in types_to_clear:
                    if group in self.cache_groups[ct]:
                        del self.cache_groups[ct][group]
                        cleared.append(ct.display_name)

                if cleared:
                    self.logger.info(f"Cache cleared for group '{group}': {', '.join(cleared)}")
            else:
                # Clear all groups
                for ct in types_to_clear:
                    self.cache_groups[ct].clear()
                    self.cache_metrics[ct].hits = 0
                    self.cache_metrics[ct].misses = 0

                type_desc = "both" if cache_type is None else cache_type.display_name
                self.logger.info(f"Cache cleared for all groups ({type_desc})")


class KonfluxDb:
    # Class-level singleton cache shared across all KonfluxDb instances
    _shared_cache: typing.Optional[BuildCache] = None
    _cache_lock = threading.RLock()
    _group_loading_events: typing.Dict[str, asyncio.Event] = {}  # Per-group events for coordinating lazy-load

    def __init__(self, enable_cache: bool = True, cache_days: int = 30):
        """
        Initialize KonfluxDb client.

        All instances share a single global cache for efficiency. The cache is created
        on first instantiation with enable_cache=True.

        :param enable_cache: If True, enable the shared build cache. Default True.
        :param cache_days: Number of days of recent builds to cache per group. Default 30.
                          Only used when creating the cache for the first time.
        """
        self.logger = logging.getLogger(__name__)
        self.bq_client = bigquery.BigQueryClient()
        self.record_cls = None

        # Initialize shared cache on first use
        with KonfluxDb._cache_lock:
            if enable_cache and KonfluxDb._shared_cache is None:
                KonfluxDb._shared_cache = BuildCache(cache_days=cache_days)
                self.logger.debug(f"Initialized shared BuildCache with {cache_days} day window")

        # Reference the shared cache (or None if caching disabled)
        self.cache = KonfluxDb._shared_cache if enable_cache else None

    def bind(self, record_cls: typing.Type[KonfluxRecord]):
        """
        Binds the DB client to a specific table, via the KonfluxRecord class definition that represents it.
        When bound, all insert/select statements will target that table, until the DB is bound to a different one.

        If the client instance has never been bound, all attempts to insert/select will throw an exception.
        """

        self.bq_client.bind(record_cls.TABLE_ID)
        self.record_cls = record_cls

    async def _ensure_group_cached(
        self, group: typing.Optional[str], cache_type: CacheRecordsType = CacheRecordsType.SMALL_COLUMNS
    ):
        """
        Lazy-load cache for group if not already loaded.

        Automatically loads the last N days of builds for the group on first access.
        Uses per-group async events to coordinate concurrent load attempts - only one
        coroutine will actually load while others wait for it to complete.

        When group is None or empty, loads the special BUILDER_BASE_IMAGE_GROUP which
        aggregates builds from all groups matching the pattern rhel[0-9]+-* or *-rhel[0-9]+.
        This should match all of our builder and base images in ocp-build-data.

        :param group: Group name (e.g., 'openshift-4.18') or None/empty for builder_base_image
        :param cache_type: Type of cache to load (SMALL_COLUMNS or ALL_COLUMNS)
        """
        if not self.cache:
            return

        # Map empty/None group to builder_base_image special group
        if not group:
            group = BUILDER_BASE_IMAGE_GROUP

        event_key = f"{group}:{cache_type}"  # Separate events for small_columns vs all_columns cache loading

        # Quick check - if already loaded, return immediately
        if self.cache.is_group_loaded(group, cache_type=cache_type):
            self.logger.debug(f"{cache_type.display_name} cache already loaded for group '{group}'")
            return

        while True:
            # Quick check - if already loaded, return immediately
            if self.cache.is_group_loaded(group, cache_type=cache_type):
                self.logger.debug(f"{cache_type.display_name} cache already loaded for group '{group}'")
                return
            # Get or create an event for this group (thread-safe)
            with KonfluxDb._cache_lock:
                if event_key not in KonfluxDb._group_loading_events:
                    # Create new event for this group+type
                    event = asyncio.Event()
                    KonfluxDb._group_loading_events[event_key] = event
                    should_load = True
                else:
                    # Another coroutine is already loading
                    event = KonfluxDb._group_loading_events[event_key]
                    should_load = False

            if not should_load:
                # Wait for the other coroutine to finish loading
                self.logger.debug(f"Waiting for another coroutine to load {cache_type} cache for group '{group}'...")
                await event.wait()
                continue
            else:
                break

        # We're the one who will load
        try:
            self.logger.info(
                f"Lazy-loading {cache_type} cache for group '{group}' (last {self.cache._cache_days} days)..."
            )

            # Build query for last N days of builds in this group
            start_time = datetime.now(tz=timezone.utc) - timedelta(days=self.cache._cache_days)
            where_clauses = [
                Column('outcome', String).in_(['success', 'failure']),
                Column('start_time', DateTime) >= start_time,
            ]

            # For builder_base_image group, match groups starting with rhel[0-9]+- or ending with -rhel[0-9]+
            # For regular groups, match exactly
            if group == BUILDER_BASE_IMAGE_GROUP:
                # Match groups like rhel8-openshift-4.18, rhel9-openshift-4.19, openshift-4.18-rhel8, etc.
                rhel_pattern = r'^rhel[0-9]+-|-rhel[0-9]+$'
                regexp_condition = func.REGEXP_CONTAINS(Column('group', String), rhel_pattern)
                where_clauses.append(regexp_condition)
            else:
                where_clauses.append(Column('group', String) == group)

            order_by_clause = Column('start_time', quote=True).desc()

            # Execute single large query
            # For small_columns cache: exclude large columns (installed_rpms, installed_packages)
            # For all_columns cache: fetch all columns
            rows = await self.bq_client.select(
                where_clauses=where_clauses,
                order_by_clause=order_by_clause,
                limit=None,  # Get all results
                exclude_columns=['installed_rpms', 'installed_packages']
                if cache_type == CacheRecordsType.SMALL_COLUMNS
                else None,
            )

            # Load all rows into cache (thread-safe operation)
            builds = [self.from_result_row(row) for row in rows]
            self.cache.add_builds(builds, group, cache_type=cache_type)

            self.logger.info(f"{cache_type.display_name} cache loaded for group '{group}': {len(builds)} builds")

        except Exception as e:
            self.logger.error(f"Failed to load {cache_type} cache for group '{group}': {e}")
            raise
        finally:
            # Signal completion to any waiting coroutines
            event.set()
            with KonfluxDb._cache_lock:
                del KonfluxDb._group_loading_events[event_key]

    def cache_stats(self, group: typing.Optional[str] = None) -> dict:
        """
        Get cache statistics.

        :param group: Optional group to get stats for. If None, returns aggregate stats.
        :return: Dictionary with cache stats, or empty dict if cache disabled
        """
        if not self.cache:
            return {'enabled': False}

        stats = self.cache.stats(group=group)
        stats['enabled'] = True
        return stats

    @classmethod
    def clear_shared_cache(cls, group: typing.Optional[str] = None):
        """
        Clear the shared cache across all KonfluxDb instances.

        :param group: Optional group to clear. If None, clears all groups.
        """
        with cls._cache_lock:
            if cls._shared_cache:
                cls._shared_cache.clear(group=group)
            # Clear loading events for the group(s) being cleared
            if group:
                cls._group_loading_events.pop(group, None)
            else:
                cls._group_loading_events.clear()

    def generate_build_schema(self):
        """
        Generate a schema that can be fed into the create_table() function,
        starting from the representation of a Konflux build record object
        """

        fields = []
        annotations = typing.get_type_hints(
            self.record_cls.__init__
        )  # konflux_build_record.KonfluxBuildRecord.__init__)
        parameters = inspect.signature(
            self.record_cls.__init__
        ).parameters  # konflux_build_record.KonfluxBuildRecord.__init__).parameters

        for param_name, param in parameters.items():
            if param_name == 'self':
                continue

            field_type = annotations.get(param_name, str)  # Default to string if type is not provided
            mode = 'NULLABLE' if param.default is param.empty else 'REQUIRED'

            # Handle Optional types (Union[X, None])
            origin = typing.get_origin(field_type)
            if origin is typing.Union:
                # Get the non-None type from Optional[X]
                args = typing.get_args(field_type)
                field_type = next((arg for arg in args if arg is not type(None)), field_type)

            if field_type is int:
                field_type_str = 'INTEGER'
            elif field_type is float:
                field_type_str = 'FLOAT'
            elif field_type is bool:
                field_type_str = 'BOOLEAN'
            elif field_type is datetime:
                field_type_str = 'TIMESTAMP'
            elif field_type is list:
                field_type_str = 'STRING'
                mode = 'REPEATED'
            else:
                field_type_str = 'STRING'

            fields.append(SchemaField(param_name, field_type_str, mode=mode))

        self.logger.info('Generated DB schema:\n%s', pprint.pformat(fields))
        return fields

    def add_build(self, build: konflux_build_record.KonfluxRecord):
        """
        Insert a build record into Konflux DB
        """

        def value_or_null(value):
            """
            Return the value representation to be inserted into the query String.
            Strings need to be quoted in '', other values (e.g. timestamps) are rendered without quotes.
            None values translate into NULL
            """

            if value is None:
                return 'NULL'

            elif isinstance(value, str):
                return f"'{value}'"

            else:
                return str(value)

        # Fill in missing record fields
        build.ingestion_time = datetime.now(tz=timezone.utc)
        build.schema_level = SCHEMA_LEVEL

        # Execute query
        items = {k: f"{value_or_null(v)}" for k, v in build.to_dict().items()}
        self.bq_client.insert(items)

    async def add_builds(self, builds: typing.List[konflux_build_record.KonfluxBuildRecord]):
        """
        Insert a list of Konflux build records using a parallel async loop to enable concurrent queries.
        """

        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.gather(*(loop.run_in_executor(pool, self.add_build, build) for build in builds))

    async def search_builds_by_fields(
        self,
        start_search: typing.Optional[datetime] = None,
        end_search: typing.Optional[datetime] = None,
        where: typing.Optional[typing.Dict[str, typing.Any]] = None,
        extra_patterns: typing.Optional[dict] = None,
        array_contains: typing.Optional[typing.Dict[str, str]] = None,
        order_by: str = '',
        sorting: str = 'DESC',
        limit: typing.Optional[int] = None,
        strict: bool = False,
        exclude_large_columns: bool = True,
    ) -> typing.AsyncIterator[KonfluxRecord]:
        """
        Execute a SELECT * from the BigQuery table using exponential window expansion.

        Uses exponential window expansion (7, 14, 28, 56, 112, 224, 448 days) for cost optimization.
        If start_search is specified, will not search earlier than that date.

        :param start_search: Optional lower bound for start_time field (don't search before this).
        :param end_search: Upper bound for start_time field. If None, uses current time.
        :param where: Dictionary mapping column names to values for WHERE clause.
        :param extra_patterns: Dictionary mapping column names to regex patterns.
        :param array_contains: Dictionary mapping array field names to values to search for.
        :param order_by: Column to order by (default: start_time).
        :param sorting: Sorting order ('DESC' or 'ASC').
        :param limit: Maximum number of results to return.
        :param strict: If True, raise IOError if no results found.
        :param exclude_large_columns: If True (default), exclude installed_rpms and installed_packages
                                      columns from the query to reduce query cost and latency.
        :return: AsyncIterator yielding KonfluxRecord objects.
        """

        if start_search and end_search and start_search >= end_search:
            raise ValueError(f"start_search {start_search} must be earlier than end_search {end_search}")
        end_search = end_search.astimezone(timezone.utc) if end_search else datetime.now(tz=timezone.utc)
        earliest_search = start_search.astimezone(timezone.utc) if start_search else None

        base_clauses = []
        where = where or {}

        # Normalize enum values in where dict - convert enums to strings for SQL comparison
        where = {
            k: str(v) if isinstance(v, (KonfluxBuildOutcome, ArtifactType, Engine)) else v for k, v in where.items()
        }

        # Unless otherwise specified, only look for builds in 'success' or 'failure' state
        if 'outcome' not in where:
            base_clauses.append(Column('outcome', String).in_(['success', 'failure']))

        for col_name, col_value in where.items():
            if col_value is not None:
                if isinstance(col_value, list):
                    # Translating into queries like "AND outcome IN ('success', 'failed')"
                    col_value = [str(outcome) for outcome in col_value]
                    base_clauses.append(Column(col_name, String).in_(col_value))
                else:
                    base_clauses.append(Column(col_name, String) == col_value)
            else:
                base_clauses.append(Column(col_name, String).is_(None))
        extra_patterns = extra_patterns or {}
        for col_name, col_value in extra_patterns.items():
            regexp_condition = func.REGEXP_CONTAINS(Column(col_name, String), col_value)
            base_clauses.append(regexp_condition)

        array_contains = array_contains or {}
        for array_field, search_value in array_contains.items():
            # Generate SQL condition like: 'search_value' IN UNNEST(array_field)
            # We need to use literal_column to create raw SQL since sqlalchemy doesn't have direct UNNEST support
            array_condition = text(f"'{search_value}' IN UNNEST({array_field})")
            base_clauses.append(array_condition)

        order_by_clause = Column(order_by if order_by else 'start_time', quote=True)
        order_by_clause = order_by_clause.desc() if sorting == 'DESC' else order_by_clause.asc()

        # Exponential window search: 7, 14, 28, 56, 112, 224, 448 days
        total_rows = 0
        previous_start = end_search

        for window_days in EXPONENTIAL_SEARCH_WINDOWS:
            start_window = end_search - timedelta(days=window_days)

            # Respect start_search constraint if provided
            if earliest_search and start_window < earliest_search:
                start_window = earliest_search

            where_clauses = base_clauses + [
                Column('start_time', DateTime) >= start_window,
                Column('start_time', DateTime) < previous_start,
            ]

            try:
                self.logger.debug(
                    f"Querying {window_days}-day window: [{start_window.date()}, {previous_start.date()})"
                )
                rows = await self.bq_client.select(
                    where_clauses=where_clauses,
                    order_by_clause=order_by_clause,
                    limit=limit - total_rows if limit is not None else None,
                    exclude_columns=['installed_rpms', 'installed_packages'] if exclude_large_columns else None,
                )
            except Exception as e:
                self.logger.error(f'Failed executing query for {window_days}-day window: {e}')
                raise

            self.logger.debug(f'Found {rows.total_rows} builds in {window_days}-day window')
            for row in rows:
                total_rows += 1
                yield self.from_result_row(row)
                if limit is not None and total_rows >= limit:
                    return

            previous_start = start_window

            # If we hit the start_search boundary, stop searching
            if earliest_search and start_window <= earliest_search:
                self.logger.debug(f"Reached start_search boundary at {window_days}-day window, stopping")
                break

        if total_rows == 0:
            # We can print out BinaryExpression search clause, but it gets much trickier with Function
            # that comes into play when extra_patterns is used, so exclude those cases
            self.logger.debug(
                'No builds found with the given criteria: %s',
                [
                    f"{clause.left}={clause.right.value if not isinstance(clause.right, Null) else clause.right}"
                    for clause in base_clauses
                    if isinstance(clause, BinaryExpression)
                ],
            )
            if strict:
                raise IOError('No builds found with the given criteria')

    async def get_latest_builds(
        self,
        names: typing.List[str],
        group: str,
        outcome: typing.Union[KonfluxBuildOutcome, str] = KonfluxBuildOutcome.SUCCESS,
        assembly: str = 'stream',
        el_target: typing.Optional[str] = None,
        artifact_type: typing.Optional[typing.Union[ArtifactType, str]] = None,
        engine: typing.Optional[typing.Union[Engine, str]] = None,
        completed_before: typing.Optional[datetime] = None,
        embargoed: bool = None,
        extra_patterns: dict = {},
        strict: bool = False,
    ) -> typing.List[typing.Optional[KonfluxRecord]]:
        """
        For a list of component names, run get_latest_build() in a concurrent pool executor.
        """

        # Normalize enum parameters - accept strings or enums
        if outcome is not None and not isinstance(outcome, KonfluxBuildOutcome):
            outcome = KonfluxBuildOutcome(outcome)
        if artifact_type is not None and not isinstance(artifact_type, ArtifactType):
            artifact_type = ArtifactType(artifact_type)
        if engine is not None and not isinstance(engine, Engine):
            engine = Engine(engine)

        return await asyncio.gather(
            *[
                self.get_latest_build(
                    name=name,
                    group=group,
                    outcome=outcome,
                    assembly=assembly,
                    el_target=el_target,
                    artifact_type=artifact_type,
                    engine=engine,
                    completed_before=completed_before,
                    embargoed=embargoed,
                    extra_patterns=extra_patterns,
                    strict=strict,
                )
                for name in names
            ]
        )

    async def get_latest_build(
        self,
        name: typing.Optional[str] = None,
        nvr: typing.Optional[str] = None,
        group: typing.Optional[str] = None,
        outcome: typing.Union[KonfluxBuildOutcome, str] = KonfluxBuildOutcome.SUCCESS,
        assembly: typing.Optional[str] = None,
        el_target: typing.Optional[str] = None,
        artifact_type: typing.Optional[typing.Union[ArtifactType, str]] = None,
        engine: typing.Optional[typing.Union[Engine, str]] = None,
        completed_before: typing.Optional[datetime] = None,
        embargoed: typing.Optional[bool] = None,
        extra_patterns: dict = {},
        strict: bool = False,
        use_cache: bool = True,
        exclude_large_columns: bool = True,
    ) -> typing.Optional[KonfluxRecord]:
        """
        Get latest build with optimized caching and exponential window search.

        Can search by name OR by NVR. Uses cache first when available, falls back
        to BigQuery with exponential window expansion (7, 14, 28, 56, 112, 224, 448 days).

        When group is not specified and cannot be extracted from NVR, uses the
        builder_base_image group which aggregates RHEL builder groups.

        :param name: component name, e.g. 'ironic' (optional if nvr provided)
        :param nvr: build NVR (optional, alternative to name search)
        :param group: e.g. 'openshift-4.18' (optional - will be extracted from nvr or use builder_base_image)
        :param outcome: 'success' | 'failure'
        :param assembly: assembly name filter
        :param el_target: e.g. 'el8', 'el9'
        :param artifact_type: 'rpm' | 'image'
        :param engine: 'brew' | 'konflux'
        :param completed_before: cut off timestamp for builds completion time
        :param embargoed: filter by embargoed status
        :param extra_patterns: e.g. {'release': 'b45ea65'} for LIKE queries
        :param strict: If True, raise IOError if build not found
        :param use_cache: If True, check appropriate cache first (small_columns or all_columns based on exclude_large_columns).
                         Default True.
        :param exclude_large_columns: If True (default), exclude installed_rpms and installed_packages
                                      columns from BigQuery queries and use small_columns cache to reduce cost and latency.
                                      Set to False when you know you'll need these columns (uses all_columns cache).
        :return: Latest matching build or None
        :raise: IOError if build not found and strict=True
        :raise: ValueError if neither name nor nvr provided, or if name provided without group
        """

        # Validate inputs
        if not name and not nvr:
            raise ValueError("Must provide either 'name' or 'nvr' parameter")
        if name and not group:
            raise ValueError("Must provide 'group' when searching by name")

        # Normalize enum parameters - accept strings or enums
        if outcome is not None and not isinstance(outcome, KonfluxBuildOutcome):
            outcome = KonfluxBuildOutcome(outcome)
        if artifact_type is not None and not isinstance(artifact_type, ArtifactType):
            artifact_type = ArtifactType(artifact_type)
        if engine is not None and not isinstance(engine, Engine):
            engine = Engine(engine)

        # Extract group from NVR if nvr is provided but group is not
        if nvr and group is None:
            group = extract_group_from_nvr(nvr)
            if group:
                self.logger.debug(f"Extracted group '{group}' from NVR {nvr}")

        # Determine which cache type to use based on exclude_large_columns
        cache_type = CacheRecordsType.SMALL_COLUMNS if exclude_large_columns else CacheRecordsType.ALL_COLUMNS

        # Lazy-load cache for group if needed (or builder_base_image if group still None)
        if use_cache:
            await self._ensure_group_cached(group, cache_type=cache_type)

        # NVR lookup (fast path)
        if nvr:
            if use_cache and self.cache:
                # Try group-specific lookup first if group provided
                cached = self.cache.get_by_nvr(nvr, group=group, cache_type=cache_type)
                if cached:
                    # Verify the cached build matches the requested outcome
                    if outcome is None or cached.outcome == outcome:
                        return cached
                    else:
                        self.logger.debug(
                            f"Cached NVR {nvr} has outcome {cached.outcome}, but requested {outcome}. "
                            "Falling through to BigQuery."
                        )

            # Cache miss - fall through to BigQuery NVR search
            self.logger.debug(f"NVR {nvr} not in {cache_type} cache, querying BigQuery")

        # Name lookup with filters (common path)
        if name and use_cache and self.cache:
            cached = self.cache.get_by_name(
                name=name,
                group=group,
                outcome=outcome,
                assembly=assembly,
                el_target=el_target,
                artifact_type=artifact_type,
                engine=engine,
                embargoed=embargoed,
                completed_before=completed_before,
                cache_type=cache_type,
            )
            if cached:
                # Verify extra_patterns if specified
                if extra_patterns:
                    for col_name, col_value in extra_patterns.items():
                        build_value = getattr(cached, col_name, None)
                        # Boolean columns need exact match
                        if col_name in ('hermetic', 'embargoed'):
                            # Convert string representation to boolean if needed
                            if isinstance(col_value, str):
                                expected_bool = col_value.lower() in ('true', '1', 'yes')
                            else:
                                expected_bool = bool(col_value)
                            if build_value != expected_bool:
                                self.logger.debug(f"Cached build doesn't match extra_pattern {col_name}={col_value}")
                                break
                        else:
                            # String columns use substring matching
                            if build_value is None or col_value not in str(build_value):
                                self.logger.debug(f"Cached build doesn't match extra_pattern {col_name}={col_value}")
                                break
                    else:
                        # All extra_patterns match
                        return cached
                else:
                    return cached

        # Cache miss or disabled → query BigQuery with exponential windows
        result = await self._query_bigquery_exponential(
            name=name,
            nvr=nvr,
            group=group,
            outcome=outcome,
            assembly=assembly,
            el_target=el_target,
            artifact_type=artifact_type,
            engine=engine,
            completed_before=completed_before,
            embargoed=embargoed,
            extra_patterns=extra_patterns,
            exclude_large_columns=exclude_large_columns,
        )

        # Update appropriate cache if use_cache=True
        if result and self.cache and result.group and use_cache:
            self.cache.add_builds([result], result.group, cache_type=cache_type)

        if not result and strict:
            raise IOError(f"Build record not found for name={name}, nvr={nvr}")

        return result

    async def _query_bigquery_exponential(
        self,
        name: typing.Optional[str] = None,
        nvr: typing.Optional[str] = None,
        group: typing.Optional[str] = None,
        outcome: typing.Optional[KonfluxBuildOutcome] = None,
        assembly: typing.Optional[str] = None,
        el_target: typing.Optional[str] = None,
        artifact_type: typing.Optional[ArtifactType] = None,
        engine: typing.Optional[Engine] = None,
        completed_before: typing.Optional[datetime] = None,
        embargoed: typing.Optional[bool] = None,
        extra_patterns: typing.Optional[dict] = None,
        exclude_large_columns: bool = True,
    ) -> typing.Optional[KonfluxRecord]:
        """
        Query BigQuery with exponential window expansion.

        Searches progressively expanding windows: 7, 14, 28, 56, 112, 224, 448 days.
        If completed_before is set, optimizes by starting search from that timestamp
        instead of now (since no valid results exist after it). Continues expanding
        backwards through all windows until first result is found.

        :param name: Component name
        :param nvr: Build NVR (alternative to name search)
        :param group: Group name (e.g., 'openshift-4.18')
        :param outcome: Build outcome filter
        :param assembly: Assembly name filter
        :param el_target: EL target filter
        :param artifact_type: Artifact type filter
        :param engine: Engine filter
        :param completed_before: Timestamp filter - only return builds that started before this time
        :param embargoed: Embargoed status filter
        :param extra_patterns: Extra pattern matching (regex)
        :return: First matching build or None
        """
        # Build base WHERE clauses
        base_clauses = []

        if name:
            base_clauses.append(Column('name', String) == name)
        if nvr:
            base_clauses.append(Column('nvr', String) == nvr)
        if group:
            base_clauses.append(Column('group', String) == group)
        if outcome is not None:
            base_clauses.append(Column('outcome', String) == str(outcome))
        if assembly is not None:
            base_clauses.append(Column('assembly', String) == assembly)
        if el_target is not None:
            base_clauses.append(Column('el_target', String) == el_target)
        if artifact_type is not None:
            base_clauses.append(Column('artifact_type', String) == str(artifact_type))
        if engine is not None:
            base_clauses.append(Column('engine', String) == str(engine))
        if embargoed is not None:
            base_clauses.append(Column('embargoed', Boolean) == embargoed)

        # Add extra_patterns (regex matching for strings, equality for booleans)
        extra_patterns = extra_patterns or {}
        for col_name, col_value in extra_patterns.items():
            # Boolean columns should use equality, not regex
            if col_name in ('hermetic', 'embargoed'):
                # Convert string representation to boolean if needed
                if isinstance(col_value, str):
                    bool_value = col_value.lower() in ('true', '1', 'yes')
                else:
                    bool_value = bool(col_value)
                base_clauses.append(Column(col_name, Boolean) == bool_value)
            else:
                # String columns use regex matching
                regexp_condition = func.REGEXP_CONTAINS(Column(col_name, String), col_value)
                base_clauses.append(regexp_condition)

        # Order by start_time descending (newest first)
        order_by_clause = Column('start_time', quote=True).desc()

        # Determine search starting point
        # If completed_before is set, start searching from that point instead of now
        # (no valid results can exist after completed_before)
        end_search = datetime.now(tz=timezone.utc)
        if completed_before:
            completed_before_utc = completed_before.astimezone(timezone.utc)
            if end_search > completed_before_utc:
                end_search = completed_before_utc

        # Exponential window search: 7, 14, 28, 56, 112, 224, 448 days
        # Continue expanding backwards through all windows until we find a result
        for window_days in EXPONENTIAL_SEARCH_WINDOWS:
            start_window = end_search - timedelta(days=window_days)

            # Build time range WHERE clause
            where_clauses = base_clauses + [
                Column('start_time', DateTime) >= start_window,
                Column('start_time', DateTime) < end_search,
            ]

            # Add completed_before filter if specified
            if completed_before:
                where_clauses.append(Column('start_time', DateTime) < completed_before)

            try:
                self.logger.debug(
                    f"Querying BigQuery: window={window_days}d, range=[{start_window.date()}, {end_search.date()})"
                )

                rows = await self.bq_client.select(
                    where_clauses=where_clauses,
                    order_by_clause=order_by_clause,
                    limit=1,  # Only need first result
                    exclude_columns=['installed_rpms', 'installed_packages'] if exclude_large_columns else None,
                )

                if rows.total_rows > 0:
                    result = self.from_result_row(next(rows))
                    self.logger.debug(f"Found build in {window_days}-day window: {result.nvr}")
                    return result

            except Exception as e:
                self.logger.error(f"Failed querying {window_days}-day window: {e}")
                raise

        # No results found in any window
        self.logger.debug(f"No builds found in exponential search up to {EXPONENTIAL_SEARCH_WINDOWS[-1]} days")
        return None

    def from_result_row(self, row: Row) -> KonfluxRecord:
        """
        Given a google.cloud.bigquery.table.Row object, construct and return a KonfluxBuild object
        """
        assert self.record_cls is not None, 'DB client is not bound to a table'
        try:
            return self.record_cls(**{field: (row[field]) for field in row.keys()})

        except AttributeError as e:
            self.logger.error(
                'Could not construct a %s object from result row %s: %s', self.record_cls.__name__, row, e
            )
            raise

    async def get_build_record_by_nvr(
        self,
        nvr: str,
        outcome: typing.Union[KonfluxBuildOutcome, str] = KonfluxBuildOutcome.SUCCESS,
        strict: bool = True,
    ) -> typing.Optional[KonfluxRecord]:
        """Get a build record by NVR.

        Uses optimized cache-first lookup via get_latest_build().

        :param nvr: The NVR of the build.
        :param outcome: The outcome of the build.
        :param strict: If True, raise an exception if the build record is not found.
        :return: The build record; None if the build record is not found.
        :raise: IOError if the build record is not found and strict is True.
        """
        # Use optimized get_latest_build with NVR parameter
        # This will check cache first, then use exponential windows
        return await self.get_latest_build(nvr=nvr, outcome=outcome, strict=strict)

    async def get_build_records_by_nvrs(
        self,
        nvrs: typing.Sequence[str],
        outcome: typing.Union[KonfluxBuildOutcome, str] = KonfluxBuildOutcome.SUCCESS,
        where: typing.Optional[typing.Dict[str, typing.Any]] = None,
        strict: bool = True,
    ) -> list[KonfluxRecord | None]:
        """Get build records by NVRs.

        Uses optimized cache-first lookups via get_latest_build() for each NVR.
        Queries run in parallel for performance.

        :param nvrs: The NVRs of the builds.
        :param outcome: The outcome of the builds.
        :param where: Additional fields to filter the build records.
        :param strict: If True, raise an exception if any build record is not found.
        :return: The build records.
        """
        nvrs = list(nvrs)

        # Validate where parameter
        if where:
            if "nvr" in where or "outcome" in where:
                raise ValueError(
                    "'nvr' and 'outcome' fields are reserved and should not be used in the 'where' parameter"
                )

        # Extract additional filters from where if provided
        assembly = where.get('assembly') if where else None
        el_target = where.get('el_target') if where else None
        artifact_type = where.get('artifact_type') if where else None
        engine = where.get('engine') if where else None
        embargoed = where.get('embargoed') if where else None

        # Use optimized get_latest_build for each NVR (runs in parallel)
        async def _task(nvr):
            return await self.get_latest_build(
                nvr=nvr,
                outcome=outcome,
                assembly=assembly,
                el_target=el_target,
                artifact_type=artifact_type,
                engine=engine,
                embargoed=embargoed,
                strict=strict,
            )

        records = await asyncio.gather(*(_task(nvr) for nvr in nvrs), return_exceptions=True)

        # Check for errors
        errors = [(nvr, record) for nvr, record in zip(nvrs, records) if isinstance(record, BaseException)]
        if errors:
            error_strings = [f"NVR {nvr}: {str(exc)}" for nvr, exc in errors]
            error_message = f"Failed to fetch NVRs from Konflux DB: {'; '.join(error_strings)}"
            raise IOError(error_message, errors)

        return typing.cast(list[KonfluxRecord | None], records)
