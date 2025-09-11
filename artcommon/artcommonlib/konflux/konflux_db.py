import asyncio
import concurrent
import copy
import inspect
import logging
import pprint
import typing
from datetime import datetime, timedelta, timezone

from artcommonlib import bigquery
from artcommonlib.konflux import konflux_build_record
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxRecord
from google.cloud.bigquery import Row, SchemaField
from sqlalchemy import BinaryExpression, Boolean, Column, DateTime, Null, String, func
from sqlalchemy.sql import text

SCHEMA_LEVEL = 1
DEFAULT_SEARCH_WINDOW = 90
DEFAULT_SEARCH_DAYS = 360  # By default, search for the last 360 days of data


class KonfluxDb:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.bq_client = bigquery.BigQueryClient()
        self.record_cls = None

    def bind(self, record_cls: typing.Type[KonfluxRecord]):
        """
        Binds the DB client to a specific table, via the KonfluxRecord class definition that represents it.
        When bound, all insert/select statements will target that table, until the DB is bound to a different one.

        If the client instance has never been bound, all attempts to insert/select will throw an exception.
        """

        self.bq_client.bind(record_cls.TABLE_ID)
        self.record_cls = record_cls

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
        window_size: typing.Optional[int] = None,
        where: typing.Optional[typing.Dict[str, typing.Any]] = None,
        extra_patterns: typing.Optional[dict] = None,
        array_contains: typing.Optional[typing.Dict[str, str]] = None,
        order_by: str = '',
        sorting: str = 'DESC',
        limit: typing.Optional[int] = None,
        strict: bool = False,
    ) -> typing.AsyncIterator[KonfluxRecord]:
        """
        Execute a SELECT * from the BigQuery table.

        "where" is an optional dictionary that maps names and values to define a WHERE clause.
        "start_search" is a lower bound to be applied to the partitioning field `start_time`. If None, the search starts 360 days ago.
        "end_search" can optionally be provided as an upper bound for the same field. If None, the search ends now.
        "window_size" is the number of days to search in each iteration. If None, defaults to DEFAULT_SEARCH_WINDOW.
        "extra_patterns" is an optional dictionary that maps names and values to define extra patterns to be matched.
        "array_contains" is an optional dictionary that maps array field names to values that should be contained in those arrays.
        "order_by" is the column to order by.
        "sorting" is the sorting order.
        "limit" is the maximum number of results to return. None for no limit.
        "strict" is a flag that raises an exception if no results are found.

        Return a generator that yields KonfluxRecord objects.
        """

        if start_search and end_search and start_search >= end_search:
            raise ValueError(f"start_search {start_search} must be earlier than end_search {end_search}")
        end_search = end_search.astimezone(timezone.utc) if end_search else datetime.now(tz=timezone.utc)
        start_search = (
            start_search.astimezone(timezone.utc) if start_search else end_search - timedelta(days=DEFAULT_SEARCH_DAYS)
        )
        assert window_size is None or window_size > 0, f"search_window {window_size} must be a positive integer"
        window_size = window_size or DEFAULT_SEARCH_WINDOW

        base_clauses = []
        where = where or {}

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

        # Table is partitioned by start_time. Perform an iterative search within given days of windows, going back to start_search
        total_rows = 0
        for window in range(0, (end_search - start_search).days, window_size):
            end_window = end_search - timedelta(days=window)
            start_window = max(end_window - timedelta(days=window_size), start_search)
            where_clauses = base_clauses + [
                Column('start_time', DateTime) >= start_window,
                Column('start_time', DateTime) < end_window,
            ]
            try:
                rows = await self.bq_client.select(
                    where_clauses=where_clauses,
                    order_by_clause=order_by_clause,
                    limit=limit - total_rows if limit is not None else None,
                )
            except Exception as e:
                self.logger.error('Failed executing query: %s', e)
                raise
            self.logger.debug('Found %s builds in batch %s', rows.total_rows, (window // window_size) + 1)
            for row in rows:
                total_rows += 1
                yield self.from_result_row(row)
                if limit is not None and total_rows >= limit:
                    return
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
        outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
        assembly: str = 'stream',
        el_target: typing.Optional[str] = None,
        artifact_type: typing.Optional[ArtifactType] = None,
        engine: typing.Optional[Engine] = None,
        completed_before: typing.Optional[datetime] = None,
        embargoed: bool = None,
        extra_patterns: dict = {},
        strict: bool = False,
    ) -> typing.List[typing.Optional[KonfluxRecord]]:
        """
        For a list of component names, run get_latest_build() in a concurrent pool executor.
        """

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
        name: str,
        group: str,
        outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
        assembly: typing.Optional[str] = None,
        el_target: typing.Optional[str] = None,
        artifact_type: typing.Optional[ArtifactType] = None,
        engine: typing.Optional[Engine] = None,
        completed_before: typing.Optional[datetime] = None,
        embargoed: bool = None,
        extra_patterns: dict = {},
        strict: bool = False,
    ) -> typing.Optional[KonfluxRecord]:
        """
        Search for the latest Konflux build information in BigQuery.

        :param name: component name, e.g. 'ironic'
        :param group: e.g. 'openshift-4.18'
        :param outcome: 'success' | 'failure'
        :param assembly: assembly name, if omitted any assembly is matched
        :param el_target: e.g. 'el8'
        :param artifact_type: 'rpm' | 'image'
        :param engine: 'brew' | 'konflux'
        :param completed_before: cut off timestamp for builds completion time
        :param embargoed: set to True to find a private build
        :param extra_patterns: e.g. {'release': 'b45ea65'} will result in adding "AND release LIKE '%b45ea65%'" to the query
        :param strict: If True, raise an IOError if the build record is not found.
        :return: The latest build record; None if the build record is not found.
        :raise: IOError if the build record is not found and strict is True.
        """

        # Table is partitioned by start_time. Perform an iterative search within 3-month windows, going back to 3 years
        # at most. This will let us reduce the amount of scanned data (and the BigQuery usage cost), as in the vast
        # majority of cases we would find a build in the first 3-month interval.
        base_clauses = [
            Column('name', String) == name,
            Column('group', String) == group,
            Column('outcome', String) == str(outcome),
        ]
        if assembly:
            base_clauses.append(Column('assembly', String) == assembly)
        if embargoed is not None:
            base_clauses.append(Column('embargoed', Boolean) == embargoed)

        order_by_clause = Column('start_time', quote=True).desc()

        if completed_before:
            completed_before = completed_before.astimezone(timezone.utc)
            self.logger.info('Searching for %s builds completed before %s', name, completed_before)
            base_clauses.extend([Column('end_time').isnot(None), Column('end_time', DateTime) <= completed_before])

        if el_target:
            base_clauses.append(Column('el_target', String) == el_target)

        if artifact_type:
            base_clauses.append(Column('artifact_type', String) == str(artifact_type))

        if engine:
            base_clauses.append(Column('engine', String) == str(engine))

        for col_name, col_value in extra_patterns.items():
            base_clauses.append(Column(col_name, String).like(f"%{col_value}%"))

        end_search = datetime.now(tz=timezone.utc) if not completed_before else completed_before
        start_search = end_search - timedelta(days=DEFAULT_SEARCH_DAYS)
        for window in range(0, DEFAULT_SEARCH_DAYS, DEFAULT_SEARCH_WINDOW):
            end_window = end_search - timedelta(days=window)
            start_window = max(end_window - timedelta(days=DEFAULT_SEARCH_WINDOW), start_search)
            where_clauses = copy.copy(base_clauses)
            where_clauses.extend(
                [
                    Column('start_time', DateTime) >= start_window,
                    Column('start_time', DateTime) < end_window,
                ]
            )

            results = await self.bq_client.select(where_clauses, order_by_clause=order_by_clause, limit=1)
            if results.total_rows == 0:
                continue  # No builds found in this window, try the next one
            return self.from_result_row(next(results))

        # If we got here, no builds have been found in the whole 36 months period
        if strict:
            raise IOError(f"Build record for {name} not found.")
        self.logger.debug(
            'No builds found for %s in %s with status %s in assembly %s and target %s',
            name,
            group,
            outcome.value,
            assembly,
            el_target,
        )
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
        self, nvr: str, outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS, strict: bool = True
    ) -> typing.Optional[KonfluxRecord]:
        """Get a build record by NVR.
        Note that this function only searches for the build record in the last 3 years.

        :param nvr: The NVR of the build.
        :param outcome: The outcome of the build.
        :param strict: If True, raise an exception if the build record is not found.
        :return: The build record; None if the build record is not found.
        :raise: IOError if the build record is not found and strict is True.
        """
        where = {"nvr": nvr, "outcome": str(outcome)}
        result = await anext(self.search_builds_by_fields(where=where, limit=1), None)
        if result:
            return result
        # If we got here, no builds have been found in the whole 3-year period
        if strict:
            raise IOError(f"Build record with NVR {nvr} not found.")
        self.logger.warning('No builds found for NVR %s', nvr)
        return None

    async def get_build_records_by_nvrs(
        self,
        nvrs: typing.Sequence[str],
        outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
        where: typing.Optional[typing.Dict[str, typing.Any]] = None,
        strict: bool = True,
    ) -> list[KonfluxRecord | None]:
        """Get build records by NVRS.
        Note that this function only searches for the build records in the last 3 years.
        :param nvrs: The NVRS of the builds.
        :param outcome: The outcome of the builds.
        :param where: Additional fields to filter the build records.
        :param strict: If True, raise an exception if any build record is not found.
        :return: The build records.
        """
        nvrs = list(nvrs)
        if not where:
            where = {}
        else:
            # do not modify the original dict
            where = where.copy()
            if "nvr" in where or "outcome" in where:
                raise ValueError(
                    "'nvr' and 'outcome' fields are reserved and should not be used in the 'where' parameter"
                )

        async def _task(nvr):
            where.update({"nvr": nvr, "outcome": str(outcome)})
            return await anext(self.search_builds_by_fields(where=where, limit=1, strict=strict), None)

        records = await asyncio.gather(*(_task(nvr) for nvr in nvrs), return_exceptions=True)

        errors = [(nvr, record) for nvr, record in zip(nvrs, records) if isinstance(record, BaseException)]
        if errors:
            error_strings = [f"NVR {nvr}: {str(exc)}" for nvr, exc in errors]
            error_message = f"Failed to fetch NVRs from Konflux DB: {'; '.join(error_strings)}"
            raise IOError(error_message, errors)

        return typing.cast(list[KonfluxRecord | None], records)
