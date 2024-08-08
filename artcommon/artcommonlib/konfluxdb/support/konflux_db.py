import pprint

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from artcommonlib.konfluxdb.konflux_build import KonfluxBuild
from artcommonlib.konfluxdb.konflux_db import KonfluxDb
from artcommonlib.konfluxdb.support.konflux_build import generate_build_schema


class SupportKonfluxDb(KonfluxDb):

    def init_db(self, table_id: str, delete: bool = False):
        """
        Initializes the Konflux DB by creating the table that holds the build info.
        If delete is True, the builds table will be deleted if found, then re-created.
        If clear is True and the build table already exists, all rows will be deleted.
        """

        schema = generate_build_schema(KonfluxBuild)
        self.logger.info('Generating DB schema:\n%s', pprint.pformat(schema))
        self.create_table(table_id, schema, delete=delete)
        self.logger.info('Konflux DB initialized')

    def table_exists(self, table_id: str) -> bool:
        """
        Returns True if {project_id}.{dataset_id}.{table_id} exists, False otherwise
        """

        table_ref = self._get_table_ref(table_id)
        try:
            self.client.get_table(table_ref)
            self.logger.info('Table "%s" exists.', table_ref)
            return True

        except NotFound:
            self.logger.info('Table "%s" does not exist.', table_ref)
            return False

    def create_table(self, table_id: str, schema: list, delete: bool = False):
        """
        Creates a table in the DB. If delete is False, will do nothing when 'table_id' alredy exists.
        Otherwise, it will delete the table and re-create it with the applied schema
        """

        if self.table_exists(table_id):
            # Build table already present
            if not delete:
                # Table not tagged for deletion. Do nothing
                return

            # Table tagged for deletion. Remove it
            self.delete_table(table_id)

        # Create the table with the provided schema
        table_ref = self._get_table_ref(table_id)
        self.logger.info('Creating table "%s"...', table_ref)
        table = bigquery.Table(table_ref, schema=schema)

        try:
            self.client.create_table(table)
            self.logger.info('Table "%s" created successfully.', table_ref)

        except Exception as e:
            self.logger.error('Failed to create table "%s": %s', table_ref, e)
            raise

    def delete_table(self, table_id: str):
        """
        Deletes the table 'table_id' from the DB
        """

        table_ref = self._get_table_ref(table_id)
        try:
            self.client.delete_table(table_ref)
            self.logger.info('Table "%s" deleted successfully.', table_ref)

        except Exception as e:
            self.logger.info('Failed to delete table "%s": %s', table_ref, e)
            raise
