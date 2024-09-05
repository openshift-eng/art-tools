import asyncio
import concurrent
import inspect
import logging
import os
import typing
from datetime import datetime

from google.cloud import bigquery

from artcommonlib.konflux import konflux_build_record

SCHEMA_LEVEL = 1


class KonfluxDb:
    REQUIRED_VARS = ['GOOGLE_CLOUD_PROJECT', 'GOOGLE_APPLICATION_CREDENTIALS', 'DATASET_ID', 'TABLE_ID']

    def __init__(self):
        self._check_env_vars()
        self.client = bigquery.Client()
        self.dataset_id = os.environ['DATASET_ID']
        self.table_ref = f'{self.client.project}.{self.dataset_id}.{os.environ["TABLE_ID"]}'
        self.logger = logging.getLogger(__name__)
        self.column_names = self._get_column_names()  # e.g. "(name, group, version, release, ... )"

        # Make the gcp logger less noisy
        logging.getLogger('google.auth.transport.requests').setLevel(logging.WARNING)

    def _check_env_vars(self):
        """
        Check if all required env vars are defined. Raise an exception otherwise
        """

        missing_vars = [var for var in self.REQUIRED_VARS if var not in os.environ]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

    @staticmethod
    def _get_column_names() -> str:
        """
        Inspects the KonfluxBuildRecord class definition, and returns its fields a list
        To be used with INSERT statements
        """

        fields = []
        parameters = inspect.signature(konflux_build_record.KonfluxBuildRecord.__init__).parameters

        for param_name, _ in parameters.items():
            if param_name == 'self':
                continue
            fields.append(f"`{param_name}`")

        return f'({", ".join(fields)})'

    def add_build(self, build: konflux_build_record.KonfluxBuildRecord):
        """
        Insert a build record into Konflux DB
        """

        # Fill in missing record fields
        build.ingestion_time = datetime.now()
        build.schema_level = SCHEMA_LEVEL
        query = f'INSERT INTO `{self.table_ref}` {self.column_names} VALUES '
        values = (f"'{value}'" if isinstance(value, str) else str(value) for value in build.to_dict().values())
        query += '(' + ', '.join(values) + ')'
        self.logger.info('Executing query: %s', query)

        # This is using the standard table API, which could possibly exceed BigQuery quotas
        # See https://cloud.google.com/bigquery/quotas#load_job_per_table.long for details
        # In case this happens, we can use the streaming api, e.g. self.client.insert_rows_json(self.table_ref, builds)
        # This can lead to unconsistent results, as there might be delays from the time a record is inserted,
        # and the time it is actually available for retrieval. If we can't live with this limitation, we should consider
        # adopting the new BigQuery Storage API, safe but harder to implement:
        # https://github.com/googleapis/python-bigquery-storage/blob/main/samples/snippets/append_rows_proto2.py
        self.client.query(query).result()

    async def add_builds(self, builds: typing.List[konflux_build_record.KonfluxBuildRecord]):
        """
        Insert a list of Konflux build records
        """

        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await asyncio.gather(*(loop.run_in_executor(pool, self.add_build, build) for build in builds))

    def search_builds_by_fields(self, names: list, values: list):
        query = f'SELECT * FROM `{self.table_ref}` WHERE '
        where_clauses = [f'`{field}` = "{value}"' for field, value in zip(names, values)]
        query += ' AND'.join(where_clauses)

        self.logger.info('Executing query: %s', query)
        results = self.client.query(query).result()
        self.logger.info('Found %s builds', results.total_rows)

        for row in results:
            yield konflux_build_record.from_result_row(row)

    def search_builds_by_field(self, name, value):
        """
        Search for all builds matching component name
        Returns a generator of KonfluxBuildRecord objects
        """

        query = f'SELECT * FROM `{self.table_ref}` WHERE {name} = '

        if isinstance(value, str):
            query += f'"{value}"'
        else:
            query += f'{value}'

        self.logger.info('Executing query: %s', query)
        results = self.client.query(query).result()
        self.logger.info('Found %s builds', results.total_rows)

        for row in results:
            yield konflux_build_record.from_result_row(row)

    def search_builds_by_name(self, name: str):
        """
        Search for all builds matching component name
        Returns a generator of KonfluxBuildRecord objects
        """

        return self.search_builds_by_field('name', name)

    def search_build_by_build_id(self, build_id: str) -> typing.Optional[konflux_build_record.KonfluxBuildRecord]:
        """
        Search for a build matching build ID.
        If no build is found, return None.
        If more than one build is found, raise an error.
        Otherwise, return a KonfluxBuildRecord object
        """

        self.logger.info('Searching for a build matching ID "%s"', build_id)
        builds = list(self.search_builds_by_field('build_id', build_id))

        if not builds:
            return None

        elif len(builds) > 1:
            raise ValueError('More than one build found with build ID equal to "%s"', build_id)

        return builds[0]

    def search_builds_by_record_id(self, record_id: str):
        """
        Search for all builds matching record ID
        Returns a generator of KonfluxBuildRecord objects
        """

        return self.search_builds_by_field('record_id', record_id)

    def search_builds_by_nvr(self, nvr: str):
        """
        Search for all builds matching NVR
        Returns a generator of KonfluxBuildRecord objects
        """

        return self.search_builds_by_field('nvr', nvr)
