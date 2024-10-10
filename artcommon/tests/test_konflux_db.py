import asyncio
from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from google.cloud.bigquery import SchemaField

from artcommonlib import constants
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord, KonfluxBundleBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb


class TestKonfluxDB(IsolatedAsyncioTestCase):
    @patch('os.environ', {'GOOGLE_APPLICATION_CREDENTIALS': ''})
    @patch('artcommonlib.bigquery.bigquery.Client')
    def setUp(self, _):
        self.db = KonfluxDb()
        self.db.bind(KonfluxBuildRecord)
        self.db.bq_client._table_ref = constants.BUILDS_TABLE_ID

    @patch('artcommonlib.bigquery.BigQueryClient.query')
    def test_add_builds(self, query_mock):
        build = KonfluxBuildRecord()

        self.db.add_build(build)
        query_mock.assert_called_once()

        query_mock.reset_mock()
        asyncio.run(self.db.add_builds([]))
        query_mock.assert_not_called()

        query_mock.reset_mock()
        asyncio.run(self.db.add_builds([build]))
        query_mock.assert_called_once()

        query_mock.reset_mock()
        asyncio.run(self.db.add_builds([build for _ in range(10)]))
        self.assertEqual(query_mock.call_count, 10)

    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_search_builds_by_fields(self, query_mock):
        start_search = datetime(2024, 9, 23, 9, 0, 0, 0)
        await self.db.search_builds_by_fields(start_search=start_search, where={})
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'")

        query_mock.reset_mock()
        end_search = start_search + timedelta(days=7)
        await self.db.search_builds_by_fields(start_search=start_search, end_search=end_search, where={})
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'"
            " AND `start_time` < '2024-09-30 09:00:00'")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(start_search=start_search, where=None)
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(start_search=start_search,
                                              where={'name': 'ironic', 'group': 'openshift-4.18'})
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'"
            " AND `name` = 'ironic' AND `group` = 'openshift-4.18'")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(start_search=start_search, where={'name': None})
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00' AND `name` IS NULL")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(start_search=start_search, where={'name': None, 'group': None})
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'"
            " AND `name` IS NULL AND `group` IS NULL")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(
            start_search=start_search,
            where={'name': 'ironic', 'group': 'openshift-4.18'},
            order_by='start_time')
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'"
            " AND `name` = 'ironic' AND `group` = 'openshift-4.18' ORDER BY `start_time` DESC")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(
            start_search=start_search,
            where={'name': 'ironic', 'group': 'openshift-4.18'},
            order_by='start_time', sorting='ASC')
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'"
            " AND `name` = 'ironic' AND `group` = 'openshift-4.18' ORDER BY `start_time` ASC")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(
            start_search=start_search,
            where={'name': 'ironic', 'group': 'openshift-4.18'},
            order_by='start_time', sorting='ASC', limit=0)
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'"
            " AND `name` = 'ironic' AND `group` = 'openshift-4.18' ORDER BY `start_time` ASC LIMIT 0")

        query_mock.reset_mock()
        await self.db.search_builds_by_fields(
            start_search=start_search,
            where={'name': 'ironic', 'group': 'openshift-4.18'},
            order_by='start_time', sorting='ASC', limit=10)
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE `start_time` > '2024-09-23 09:00:00'"
            " AND `name` = 'ironic' AND `group` = 'openshift-4.18' ORDER BY `start_time` ASC LIMIT 10")

        query_mock.reset_mock()
        with self.assertRaises(AssertionError):
            await self.db.search_builds_by_fields(
                start_search=start_search,
                where={'name': 'ironic', 'group': 'openshift-4.18'},
                order_by='start_time', sorting='ASC', limit=-1)

    @patch('artcommonlib.konflux.konflux_db.datetime')
    @patch('artcommonlib.bigquery.BigQueryClient.query')
    def test_get_latest_build(self, query_mock, datetime_mock):
        now = datetime(2022, 1, 1, 12, 0, 0)
        lower_bound = now - 3 * timedelta(days=30)
        datetime_mock.now.return_value = now
        self.db.get_latest_build(name='ironic', group='openshift-4.18', outcome='success')
        query_mock.assert_called_once_with(f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ironic' "
                                           "AND `group` = 'openshift-4.18' AND outcome = 'success' "
                                           "AND assembly = 'stream' AND end_time IS NOT NULL "
                                           f"AND end_time < '{str(now)}' "
                                           f"AND start_time >= '{str(lower_bound)}' "
                                           f"AND start_time < '{now}' "
                                           "ORDER BY `start_time` DESC LIMIT 1")

        query_mock.reset_mock()
        asyncio.run(self.db.get_latest_builds(names=['ironic', 'ose-installer-artifacts'], group='openshift-4.18',
                                              outcome='success'))

        actual_calls = [query_mock.call_args_list[x][0][0] for x in range(0, 2)]
        self.assertIn(f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ironic' "
                      "AND `group` = 'openshift-4.18' AND outcome = 'success' "
                      "AND assembly = 'stream' AND end_time IS NOT NULL "
                      f"AND end_time < '{str(now)}' "
                      f"AND start_time >= '{str(lower_bound)}' "
                      f"AND start_time < '{now}' "
                      "ORDER BY `start_time` DESC LIMIT 1", actual_calls)

        self.assertIn(f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ose-installer-artifacts' "
                      "AND `group` = 'openshift-4.18' AND outcome = 'success' "
                      "AND assembly = 'stream' AND end_time IS NOT NULL "
                      f"AND end_time < '{str(now)}' "
                      f"AND start_time >= '{str(lower_bound)}' "
                      f"AND start_time < '{now}' "
                      "ORDER BY `start_time` DESC LIMIT 1", actual_calls)

    def test_generate_builds_schema(self):
        expected_fields = [
            SchemaField('name', 'STRING', 'REQUIRED'),
            SchemaField('group', 'STRING', 'REQUIRED'),
            SchemaField('version', 'STRING', 'REQUIRED'),
            SchemaField('release', 'STRING', 'REQUIRED'),
            SchemaField('assembly', 'STRING', 'REQUIRED'),
            SchemaField('el_target', 'STRING', 'REQUIRED'),
            SchemaField('arches', 'STRING', 'REPEATED'),
            SchemaField('installed_packages', 'STRING', 'REPEATED'),
            SchemaField('parent_images', 'STRING', 'REPEATED'),
            SchemaField('source_repo', 'STRING', 'REQUIRED'),
            SchemaField('commitish', 'STRING', 'REQUIRED'),
            SchemaField('rebase_repo_url', 'STRING', 'REQUIRED'),
            SchemaField('rebase_commitish', 'STRING', 'REQUIRED'),
            SchemaField('embargoed', 'BOOLEAN', 'REQUIRED'),
            SchemaField('start_time', 'TIMESTAMP', 'REQUIRED'),
            SchemaField('end_time', 'TIMESTAMP', 'REQUIRED'),
            SchemaField('artifact_type', 'STRING', 'REQUIRED'),
            SchemaField('engine', 'STRING', 'REQUIRED'),
            SchemaField('image_pullspec', 'STRING', 'REQUIRED'),
            SchemaField('image_tag', 'STRING', 'REQUIRED'),
            SchemaField('outcome', 'STRING', 'REQUIRED'),
            SchemaField('art_job_url', 'STRING', 'REQUIRED'),
            SchemaField('build_pipeline_url', 'STRING', 'REQUIRED'),
            SchemaField('pipeline_commit', 'STRING', 'REQUIRED'),
            SchemaField('schema_level', 'INTEGER', 'REQUIRED'),
            SchemaField('ingestion_time', 'TIMESTAMP', 'REQUIRED'),
            SchemaField('record_id', 'STRING', 'REQUIRED'),
            SchemaField('build_id', 'STRING', 'REQUIRED'),
            SchemaField('nvr', 'STRING', 'REQUIRED'),

        ]
        self.db.bind(KonfluxBuildRecord)
        self.assertEqual(self.db.generate_build_schema(), expected_fields)

    def test_generate_bundle_builds_schema(self):
        expected_fields = [
            SchemaField('name', 'STRING', 'REQUIRED'),
            SchemaField('group', 'STRING', 'REQUIRED'),
            SchemaField('version', 'STRING', 'REQUIRED'),
            SchemaField('release', 'STRING', 'REQUIRED'),
            SchemaField('assembly', 'STRING', 'REQUIRED'),
            SchemaField('source_repo', 'STRING', 'REQUIRED'),
            SchemaField('commitish', 'STRING', 'REQUIRED'),
            SchemaField('rebase_repo_url', 'STRING', 'REQUIRED'),
            SchemaField('rebase_commitish', 'STRING', 'REQUIRED'),
            SchemaField('start_time', 'TIMESTAMP', 'REQUIRED'),
            SchemaField('end_time', 'TIMESTAMP', 'REQUIRED'),
            SchemaField('artifact_type', 'STRING', 'REQUIRED'),
            SchemaField('engine', 'STRING', 'REQUIRED'),
            SchemaField('image_pullspec', 'STRING', 'REQUIRED'),
            SchemaField('image_tag', 'STRING', 'REQUIRED'),
            SchemaField('outcome', 'STRING', 'REQUIRED'),
            SchemaField('art_job_url', 'STRING', 'REQUIRED'),
            SchemaField('build_pipeline_url', 'STRING', 'REQUIRED'),
            SchemaField('pipeline_commit', 'STRING', 'REQUIRED'),
            SchemaField('schema_level', 'INTEGER', 'REQUIRED'),
            SchemaField('ingestion_time', 'TIMESTAMP', 'REQUIRED'),
            SchemaField('operand_nvrs', 'STRING', 'REPEATED', None, None, (), None),
            SchemaField('operator_nvr', 'STRING', 'REQUIRED', None, None, (), None),
            SchemaField('record_id', 'STRING', 'REQUIRED'),
            SchemaField('build_id', 'STRING', 'REQUIRED'),
            SchemaField('nvr', 'STRING', 'REQUIRED'),
        ]
        self.db.bind(KonfluxBundleBuildRecord)
        self.assertEqual(self.db.generate_build_schema(), expected_fields)
