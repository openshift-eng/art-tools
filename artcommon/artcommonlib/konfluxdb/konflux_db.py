import typing

from google.cloud import bigquery

from artcommonlib import logutil
from artcommonlib.konfluxdb.konflux_build import KonfluxBuild, from_result_row


class KonfluxDb:
    """
    Requires ~/.config/gcloud/application_default_credentials.json
    """

    def __init__(self, project_id: str, dataset_id: str):
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.logger = logutil.get_logger(__name__)

    def _get_table_ref(self, table_id: str):
        """
        Returns the fully qualified table reference
        """

        return f'{self.client.project}.{self.dataset_id}.{table_id}'

    def add_builds(self, builds: list, table_id: str):
        """
        Insert a list of Konflux builds
        """

        table_ref = self._get_table_ref(table_id)

        try:
            # TODO https://cloud.google.com/knowledge/kb/cannot-update-or-delete-over-bigquery-streaming-tables-000004334
            # Or simply use INSERT
            self.client.insert_rows_json(table_ref, builds)
            self.logger.info('Inserted %s rows into table "%s" successfully.', len(builds), table_ref)

        except Exception as e:
            self.logger.error('Failed inserting data into table "%s": %s', table_ref, e)

    def search_builds_by_component(self, table_id: str, component: str):
        """
        Search for all builds matching component name
        Returns a generator of KonfluxBuild objects
        """

        query = f'SELECT * FROM `{self._get_table_ref(table_id)}` WHERE name = "{component}"'

        # Set up the query parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("build_id", "STRING", component)
            ]
        )

        # Get the results
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()
        self.logger.info('Found %d builds matching component %s', results.total_rows, component)

        for row in results:
            yield from_result_row(row)

    def search_builds_by_id_or_nvr(self, table_id: str, build_id: str = None, nvr: str = None) ->\
            typing.Optional[KonfluxBuild]:
        """
        Search the DB for builds matching the provided NVR or UUID.
        If multiple builds match, we have a problem with colliding build NVRs or UUIDs, and an error will be raised.

        If no builds are found, return None.
        Otherwise, costruct a konfluxdb.konflux_build.KonfluxBuild object from the result row, and return it.
        """

        if (build_id and nvr) or (not build_id and not nvr):
            raise ValueError('Please specify either a NVR or a UUID to search for, but not both')

        field = 'build_id' if build_id else 'nvr'

        query = f'SELECT * FROM `{self._get_table_ref(table_id)}` WHERE {field} = "{eval(field)}"'

        # Set up the query parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("nvr", "STRING", nvr)
            ]
        )

        # Execute the query
        query_job = self.client.query(query, job_config=job_config)

        # Get the results
        results = query_job.result()
        total_rows = results.total_rows
        if total_rows > 1:
            self.logger.error('More than one build found with %s equal to "%s"', field, eval(field))
            raise ValueError

        try:
            row = next(results)
            return from_result_row(row)

        except StopIteration:
            self.logger.warning('No builds found with %s equal to "%s"', field, eval(field))
            return None
