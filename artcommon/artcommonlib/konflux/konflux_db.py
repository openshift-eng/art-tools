import asyncio
import concurrent
import copy
import inspect
import logging
import pprint
import typing
from datetime import datetime, timedelta, timezone

from google.cloud.bigquery import SchemaField, Row
from sqlalchemy import Column, String, DateTime

from artcommonlib import bigquery
from artcommonlib.konflux import konflux_build_record
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, ArtifactType, KonfluxRecord, Engine

SCHEMA_LEVEL = 1


class KonfluxDb:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.bq_client = bigquery.BigQueryClient()
        self.record_cls = None

    def bind(self, record_cls: type):
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
        annotations = typing.get_type_hints(self.record_cls.__init__)  # konflux_build_record.KonfluxBuildRecord.__init__)
        parameters = inspect.signature(self.record_cls.__init__).parameters  # konflux_build_record.KonfluxBuildRecord.__init__).parameters

        for param_name, param in parameters.items():
            if param_name == 'self':
                continue

            field_type = annotations.get(param_name, str)  # Default to string if type is not provided
            mode = 'NULLABLE' if param.default is param.empty else 'REQUIRED'

            if field_type == int:
                field_type_str = 'INTEGER'
            elif field_type == float:
                field_type_str = 'FLOAT'
            elif field_type == bool:
                field_type_str = 'BOOLEAN'
            elif field_type == datetime:
                field_type_str = 'TIMESTAMP'
            elif field_type == list:
                field_type_str = 'STRING'
                mode = 'REPEATED'
            else:
                field_type_str = 'STRING'

            fields.append(SchemaField(param_name, field_type_str, mode=mode))

        self.logger.info('Generated DB schema:\n%s', pprint.pformat(fields))
        return fields

    def add_build(self, build):
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

    async def search_builds_by_fields(self, start_search: datetime, end_search: datetime = None,
                                      where: typing.Dict[str, typing.Any] = None, order_by: str = '',
                                      sorting: str = 'DESC', limit: int = None) -> list:
        """
        Execute a SELECT * from the BigQuery table.

        "where" is an optional dictionary that maps names and values to define a WHERE clause.
        "start_search" is a lower bound to be applied to the partitioning field `start_time`.
        "end_search" can optionally be provided as an upper bound for the same field.

        Return a (possibly empty) list of results
        """

        query = f'SELECT * FROM `{self.bq_client.table_ref}`'
        query += f" WHERE `start_time` > '{start_search}'"
        if end_search:
            query += f" AND `start_time` < '{end_search}'"

        where_clauses = []
        where = where if where is not None else {}
        for name, value in where.items():
            if value is not None:
                where_clauses.append(f"`{name}` = '{value}'")
            else:
                where_clauses.append(f"`{name}` IS NULL")
        if where_clauses:
            query += ' AND '
            query += ' AND '.join(where_clauses)

        if order_by:
            query += f' ORDER BY `{order_by}` {sorting}'

        if limit is not None:
            assert isinstance(limit, int)
            assert limit >= 0, 'LIMIT expects a non-negative integer literal or parameter '
            query += f' LIMIT {limit}'

        results = await self.bq_client.query_async(query)
        self.logger.debug('Found %s builds', results.total_rows)
        return [self.from_result_row(result) for result in results]

    async def get_latest_builds(self, names: typing.List[str], group: str,
                                outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS, assembly: str = 'stream',
                                el_target: str = None, artifact_type: ArtifactType = None,
                                engine: Engine = None, completed_before: datetime = None)\
            -> typing.List[konflux_build_record.KonfluxBuildRecord]:
        """
        For a list of component names, run get_latest_build() in a concurrent pool executor.
        Filter results that are None, which means that no builds have been found for that specific component
        """

        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            results = await asyncio.gather(*(loop.run_in_executor(
                pool, self.get_latest_build, name, group, outcome, assembly, el_target, artifact_type, engine,
                completed_before)
                for name in names))

        return [r for r in results if r]

    def get_latest_build(self, name: str, group: str, outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
                         assembly: str = 'stream', el_target: str = None, artifact_type: ArtifactType = None,
                         engine: Engine = None, completed_before: datetime = None, extra_patterns: dict = {})\
            -> typing.Optional[konflux_build_record.KonfluxBuildRecord]:
        """
        Search for the latest Konflux build information in BigQuery.

        :param name: component name, e.g. 'ironic'
        :param group: e.g. 'openshift-4.18'
        :param outcome: 'success' | 'failure'
        :param assembly: non-standard assembly name, defaults to 'stream' if omitted
        :param el_target: e.g. 'el8'
        :param artifact_type: 'rpm' | 'image'
        :param engine: 'brew' | 'konflux'
        :param completed_before: cut off timestamp for builds completion time
        :param extra_patterns: e.g. {'release': 'b45ea65'} will result in adding "AND release LIKE '%b45ea65%'" to the query
        """

        if not completed_before:
            completed_before = datetime.now(tz=timezone.utc)
        self.logger.info('Searching for %s builds completed before %s', name, completed_before)

        # Table is partitioned by start_time. Perform an iterative search within 3-month windows, going back to 3 years
        # at most. This will let us reduce the amount of scanned data (and the BigQuery usage cost), as in the vast
        # majority of cases we would find a build in the first 3-month interval.
        base_clauses = [
            Column('name', String) == name,
            Column('group', String) == group,
            Column('outcome', String) == str(outcome),
            Column('assembly', String) == assembly,
            Column('end_time').isnot(None),
            Column('end_time', DateTime) < completed_before
        ]
        order_by_clause = Column('start_time', quote=True).desc()

        if el_target:
            base_clauses.append(Column('el_target', String) == el_target)

        if artifact_type:
            base_clauses.append(Column('artifact_type', String) == str(artifact_type))

        if engine:
            base_clauses.append(Column('engine', String) == str(engine))

        for name, value in extra_patterns.items():
            base_clauses.append(Column(name, String).like(f"%{value}%"))

        for window in range(12):
            end_search = completed_before - window * 3 * timedelta(days=30)
            start_search = end_search - 3 * timedelta(days=30)

            where_clauses = copy.copy(base_clauses)
            where_clauses.extend([
                Column('start_time', DateTime) >= start_search,
                Column('start_time', DateTime) < end_search,
            ])

            results = self.bq_client.select(where_clauses, order_by_clause=order_by_clause, limit=1)

            try:
                return self.from_result_row(next(results))

            except (StopIteration, Exception):
                # No builds found in current window, shift to the earlier one
                continue

        # If we got here, no builds have been found in the whole 36 months period
        self.logger.warning('No builds found for %s with status %s in assembly %s and target %s',
                            name, outcome.value, assembly, el_target)
        return None

    def from_result_row(self, row: Row) -> typing.Optional[KonfluxRecord]:
        """
        Given a google.cloud.bigquery.table.Row object, construct and return a KonfluxBuild object
        """

        try:
            return self.record_cls(**{
                field: (row[field])
                for field in row.keys()
            })

        except AttributeError as e:
            self.logger.error('Could not construct a %s object from result row %s: %s',
                              self.record_cls.__name__, row, e)
            raise
