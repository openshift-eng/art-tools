import asyncio
import logging
import threading
import typing
from concurrent.futures import CancelledError as FutureCancelledError
from time import monotonic

from artcommonlib import constants
from google.cloud import bigquery
from google.cloud.bigquery.retry import DEFAULT_JOB_RETRY, DEFAULT_RETRY
from google.cloud.bigquery.table import RowIterator
from sqlalchemy import BinaryExpression, UnaryExpression
from sqlalchemy.dialects import mysql

QUERY_TIMEOUT_SECONDS = 15 * 60
REQUEST_TIMEOUT_SECONDS = 30


class BigQueryClient:
    def __init__(self):
        try:
            self.client = bigquery.Client(project=constants.GOOGLE_CLOUD_PROJECT)
        except:
            raise EnvironmentError(
                f'Unable to access {constants.GOOGLE_CLOUD_PROJECT}. Initialize default application context or set GOOGLE_APPLICATION_CREDENTIALS'
            )

        self._table_ref = None
        self.logger = logging.getLogger(__name__)

        # Make the gcp logger less noisy
        logging.getLogger('google.auth.transport.requests').setLevel(logging.WARNING)

    def bind(self, table_id: str):
        self._table_ref = f'{self.client.project}.{constants.DATASET_ID}.{table_id}'
        self.logger.debug('Bound to table %s', self.table_ref)

    @property
    def table_ref(self):
        return self._table_ref

    def query(self, query: str, cancel_event: typing.Optional[threading.Event] = None) -> RowIterator:
        """
        Execute a query in BigQuery and return a generator object with the results
        """

        self.logger.debug('Executing query: %s', query)

        try:
            deadline = monotonic() + QUERY_TIMEOUT_SECONDS
            retry = DEFAULT_RETRY.with_timeout(REQUEST_TIMEOUT_SECONDS)
            job_retry = DEFAULT_JOB_RETRY.with_timeout(REQUEST_TIMEOUT_SECONDS)
            job = self.client.query(query, timeout=REQUEST_TIMEOUT_SECONDS, retry=retry, job_retry=job_retry)
            while True:
                if cancel_event and cancel_event.is_set():
                    raise FutureCancelledError()
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError(f'BigQuery query exceeded {QUERY_TIMEOUT_SECONDS} seconds')
                request_timeout = min(remaining, REQUEST_TIMEOUT_SECONDS)
                retry = DEFAULT_RETRY.with_timeout(request_timeout)
                job_retry = DEFAULT_JOB_RETRY.with_timeout(request_timeout)
                try:
                    results = job.result(timeout=request_timeout, retry=retry, job_retry=job_retry)
                except TimeoutError:
                    continue
                if cancel_event and cancel_event.is_set():
                    raise FutureCancelledError()
                self.logger.debug('Query returned %s result rows', results.total_rows)
                return results

        except FutureCancelledError:
            raise
        except TimeoutError:
            self.logger.error('BigQuery query timed out (budget: %s seconds)', QUERY_TIMEOUT_SECONDS)
            raise
        except Exception as err:
            self.logger.error('Failed executing query %s: ', err)
            raise

    async def query_async(self, query: str) -> RowIterator:
        """
        Asynchronously execute a query in BigQuery and return a generator object with the results
        """

        cancel_event = threading.Event()
        try:
            return await asyncio.to_thread(self.query, query, cancel_event)
        except asyncio.CancelledError:
            cancel_event.set()
            raise

    def insert(self, items: dict) -> None:
        """
        Translate a dictionary of (key, value) pairs into an INSERT INTO statement.
        Execute the query on BigQuery

        This is currently using the standard table API, which could possibly exceed BigQuery quotas
        See https://cloud.google.com/bigquery/quotas#load_job_per_table.long for details
        In case this happens, we can use the streaming api, e.g. self.client.insert_rows_json(self.table_ref, builds)
        This can lead to unconsistent results, as there might be delays from the time a record is inserted,
        and the time it is actually available for retrieval. If we can't live with this limitation, we should consider
        adopting the new BigQuery Storage API, safe but harder to implement:
        https://github.com/googleapis/python-bigquery-storage/blob/main/samples/snippets/append_rows_proto2.py
        """

        query = f'INSERT INTO `{self._table_ref}` ('
        query += ", ".join([f"`{name}`" for name in items.keys()])
        query += ') VALUES ('
        query += ', '.join(items.values())
        query += ')'
        self.query(query)

    async def select(
        self,
        where_clauses: typing.List[BinaryExpression] = None,
        order_by_clause: typing.Optional[UnaryExpression] = None,
        limit=None,
        exclude_columns: typing.Optional[typing.List[str]] = None,
    ) -> RowIterator:
        """
        Execute a SELECT statement and return a generator object with the results.

        where_clauses is an optional list of sqlalchemy.BinaryExpression objects that translate into
        "name = 'ose-installer-artifacts' AND outcome = 'success'" etc.

        order_by_clause is an optional sqlalchemy.UnaryExpression that translates into '"start_time" DESC' or the like

        limit is an optional value to include in a LIMIT clause

        exclude_columns is an optional list of column names to exclude from the SELECT statement
        (using BigQuery's EXCEPT syntax). Useful for excluding large columns like installed_rpms
        and installed_packages to reduce query costs and latency.
        """

        if exclude_columns:
            # Use BigQuery's EXCEPT syntax to exclude specified columns
            exclude_clause = ', '.join(exclude_columns)
            query = f"SELECT * EXCEPT ({exclude_clause}) FROM `{self.table_ref}`"
        else:
            query = f"SELECT * FROM `{self.table_ref}`"

        if where_clauses:
            where_conditions = " AND ".join(
                [
                    str(where_clause.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}))
                    for where_clause in where_clauses
                ]
            )
            # Un-escape %% to % - BigQuery doesn't need MySQL-style percent escaping
            where_conditions = where_conditions.replace('%%', '%')
            query += f' WHERE {where_conditions}'

        if order_by_clause is not None:
            order_by_string = str(
                order_by_clause.compile(dialect=mysql.dialect(), compile_kwargs={'literal_binds': True})
            )
            # Un-escape %% to % - BigQuery doesn't need MySQL-style percent escaping
            order_by_string = order_by_string.replace('%%', '%')
            query += f' ORDER BY {order_by_string}'

        if limit is not None:
            assert isinstance(limit, int)
            assert limit >= 0, 'LIMIT expects a non-negative integer literal or parameter '
            query += f' LIMIT {limit}'

        return await self.query_async(query)
