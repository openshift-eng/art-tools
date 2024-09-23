import asyncio
import concurrent
import copy
import inspect
import logging
import typing
from datetime import datetime, timedelta

from sqlalchemy import Column, String, DateTime

from artcommonlib import bigquery
from artcommonlib.konflux import konflux_build_record
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, ArtifactType

SCHEMA_LEVEL = 1


class KonfluxDb:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.bq_client = bigquery.BigQueryClient()
        self.column_names = self._get_column_names()  # e.g. "(name, group, version, release, ... )"

    @staticmethod
    def _get_column_names() -> list:
        """
        Inspects the KonfluxBuildRecord class definition, and returns a list of fields
        To be used with INSERT statements
        """

        fields = []
        parameters = inspect.signature(konflux_build_record.KonfluxBuildRecord.__init__).parameters

        for param_name, _ in parameters.items():
            if param_name == 'self':
                continue
            fields.append(param_name)

        return fields

    def add_build(self, build: konflux_build_record.KonfluxBuildRecord):
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
        build.ingestion_time = datetime.now()
        build.schema_level = SCHEMA_LEVEL

        # Execute query
        values = [f"{value_or_null(value)}" for value in build.to_dict().values()]
        self.bq_client.insert(self.column_names, values)

    async def add_builds(self, builds: typing.List[konflux_build_record.KonfluxBuildRecord]):
        """
        Insert a list of Konflux build records using a parallel async loop to enable concurrent queries.
        """

        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.gather(*(loop.run_in_executor(pool, self.add_build, build) for build in builds))

    def search_builds_by_fields(self, start_search: datetime, end_search: datetime = None,
                                where: typing.Dict[str, typing.Any] = None, order_by: str = '',
                                sorting: str = 'DESC', limit: int = None):
        """
        Execute a SELECT * from the BigQuery table.

        "where" is an optional dictionary that maps names and values to define a WHERE clause.
        "start_search" is a lower bound to be applied to the partitioning field `start_time`.
        "end_search" can optionally be provided as an upper bound for the same field.

        This is a generator function, so it must be looped over to get the query results. For each result row,
        a KonfluxBuildRecord is constructed and yielded at each iteration.
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

        results = self.bq_client.query(query)
        self.logger.info('Found %s builds', results.total_rows)

        while True:
            try:
                yield konflux_build_record.from_result_row(next(results))
            except StopIteration:
                return

    async def get_latest_builds(self, names: typing.List[str], group: str,
                                outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS, assembly: str = 'stream',
                                el_target: str = None, artifact_type: ArtifactType = None,
                                completed_before: datetime = None)\
            -> typing.List[konflux_build_record.KonfluxBuildRecord]:
        """
        For a list of component names, run get_latest_build() in a concurrent pool executor.
        Filter results that are None, which means that no builds have been found for that specific component
        """

        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            results = await asyncio.gather(*(loop.run_in_executor(
                pool, self.get_latest_build, name, group, outcome, assembly, el_target, artifact_type, completed_before)
                for name in names))

        return [r for r in results if r]

    def get_latest_build(self, name: str, group: str, outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
                         assembly: str = 'stream', el_target: str = None, artifact_type: ArtifactType = None,
                         completed_before: datetime = None) -> typing.Optional[konflux_build_record.KonfluxBuildRecord]:
        """
        Search for the latest Konflux build information in BigQuery.

        :param name: component name, e.g. 'ironic'
        :param group: e.g. 'openshift-4.18'
        :param outcome: 'success' | 'failure'
        :param assembly: non-standard assembly name, defaults to 'stream' if omitted
        :param el_target: e.g. 'el8'
        :param artifact_type: 'rpm' | 'image'
        :param completed_before: cut off timestamp for builds completion time
        """

        if not completed_before:
            completed_before = datetime.now()
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
                return konflux_build_record.from_result_row(next(results))

            except (StopIteration, Exception):
                # No builds found in current window, shift to the earlier one
                continue

        # If we got here, no builds have been found in the whole 36 months period
        self.logger.warning('No builds found for %s with status %s in assembly %s and target %s',
                            name, outcome.value, assembly, el_target)
        return None
