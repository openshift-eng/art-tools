import logging
import os

import typing

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from sqlalchemy import BinaryExpression, UnaryExpression
from sqlalchemy.dialects import mysql

from artcommonlib import constants


class BigQueryClient:
    def __init__(self):
        if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
            raise EnvironmentError('Missing required environment variable GOOGLE_APPLICATION_CREDENTIALS')

        self.client = bigquery.Client()
        self._table_ref = f'{self.client.project}.{constants.DATASET_ID}.{constants.TABLE_ID}'
        self.logger = logging.getLogger(__name__)

        # Make the gcp logger less noisy
        logging.getLogger('google.auth.transport.requests').setLevel(logging.WARNING)

    @property
    def table_ref(self):
        return self._table_ref

    def query(self, query: str) -> RowIterator:
        """
        Execute a query in BigQuery and return a generator object with the results
        """

        self.logger.info('Executing query: %s', query)

        try:
            results = self.client.query(query).result()
            self.logger.debug('Query returned %s result rows', results.total_rows)
            return results

        except Exception as err:
            self.logger.error('Failed executing query %s: ', err)
            raise

    def insert(self, names: typing.List[str], values: list) -> None:
        """
        Translate a list of column names and values into an INSERT INTO statement.
        Execute the query on BigQuery

        This is currently using the standard table API, which could possibly exceed BigQuery quotas
        See https://cloud.google.com/bigquery/quotas#load_job_per_table.long for details
        In case this happens, we can use the streaming api, e.g. self.client.insert_rows_json(self.table_ref, builds)
        This can lead to unconsistent results, as there might be delays from the time a record is inserted,
        and the time it is actually available for retrieval. If we can't live with this limitation, we should consider
        adopting the new BigQuery Storage API, safe but harder to implement:
        https://github.com/googleapis/python-bigquery-storage/blob/main/samples/snippets/append_rows_proto2.py
        """

        assert isinstance(names, list)
        assert isinstance(values, list)
        assert names and values, 'Names and values cannot be empty lists'

        if len(names) != len(values):
            raise ValueError('Provided value row does not match the column count')

        query = f'INSERT INTO `{self._table_ref}` ({", ".join([f"`{name}`" for name in names])}) VALUES '
        values = (f"'{value}'" if isinstance(value, str) else str(value) for value in values)
        query += '(' + ', '.join(values) + ')'
        self.query(query)

    def select(self, where_clauses: typing.List[BinaryExpression] = None,
               order_by_clause: typing.Optional[UnaryExpression] = None,
               limit=None) -> RowIterator:
        """
        Execute a SELECT statement and return a generator object with the results.

        where_clauses is an optional list of sqlalchemy.BinaryExpression objects that translate into
        "name = 'ose-installer-artifacts' AND outcome = 'success'" etc.

        order_by_clause is an optional sqlalchemy.UnaryExpression that translates into '"start_time" DESC' or the like

        limit is an optional value to include in a LIMIT clause
        """

        query = f"SELECT * FROM `{self.table_ref}`"

        if where_clauses:
            where_conditions = " AND ".join(
                [str(where_clause.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}))
                 for where_clause in where_clauses])
            query += f' WHERE {where_conditions}'

        if order_by_clause is not None:
            order_by_string = order_by_clause.compile(dialect=mysql.dialect(), compile_kwargs={'literal_binds': True})
            query += f' ORDER BY {order_by_string}'

        if limit is not None:
            assert isinstance(limit, int)
            assert limit >= 0, 'LIMIT expects a non-negative integer literal or parameter '
            query += f' LIMIT {limit}'

        results = self.query(query)
        return results
