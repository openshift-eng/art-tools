import asyncio
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib import constants
from artcommonlib.konflux.konflux_build_record import (
    Engine,
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from google.cloud.bigquery import Row, SchemaField


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

    @patch('artcommonlib.konflux.konflux_db.datetime')
    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_search_builds_by_fields(self, query_mock, datetime_mock):
        datetime_mock.now.return_value = datetime(2024, 9, 30, 9, 0, 0, tzinfo=timezone.utc)
        start_search = datetime(2024, 9, 23, 9, 0, 0, 0, tzinfo=timezone.utc)
        await anext(self.db.search_builds_by_fields(start_search=start_search, where={}), None)
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` DESC"
        )

        query_mock.reset_mock()
        end_search = start_search + timedelta(days=7)
        await anext(self.db.search_builds_by_fields(start_search=start_search, end_search=end_search, where={}), None)
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            f"ORDER BY `start_time` DESC"
        )

        query_mock.reset_mock()
        await anext(self.db.search_builds_by_fields(start_search=start_search, where=None), None)
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` DESC"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search, where={'name': 'ironic', 'group': 'openshift-4.18'}
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name = 'ironic' AND `group` = 'openshift-4.18' AND "
            f"start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00'"
            " ORDER BY `start_time` DESC"
        )

        query_mock.reset_mock()
        await anext(self.db.search_builds_by_fields(start_search=start_search, where={'name': None}), None)
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name IS NULL AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00'"
            " ORDER BY `start_time` DESC"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(start_search=start_search, where={'name': None, 'group': None}), None
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name IS NULL AND `group` IS NULL "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` DESC"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search, where={'name': 'ironic', 'group': 'openshift-4.18'}, order_by='start_time'
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name = 'ironic' AND `group` = 'openshift-4.18' "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` DESC"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                where={'name': 'ironic', 'group': 'openshift-4.18'},
                order_by='start_time',
                sorting='ASC',
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name = 'ironic' AND `group` = 'openshift-4.18' "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` ASC"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                where={'name': 'ironic', 'group': 'openshift-4.18'},
                order_by='start_time',
                sorting='ASC',
                limit=0,
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name = 'ironic' AND `group` = 'openshift-4.18' "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` ASC LIMIT 0"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                where={'name': 'ironic', 'group': 'openshift-4.18'},
                order_by='start_time',
                sorting='ASC',
                limit=10,
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name = 'ironic' AND `group` = 'openshift-4.18' "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` ASC LIMIT 10"
        )

        query_mock.reset_mock()
        with self.assertRaises(AssertionError):
            await anext(
                self.db.search_builds_by_fields(
                    start_search=start_search,
                    where={'name': 'ironic', 'group': 'openshift-4.18'},
                    order_by='start_time',
                    sorting='ASC',
                    limit=-1,
                ),
                None,
            )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                extra_patterns={'name': 'installer'},
                order_by='start_time',
                sorting='ASC',
                limit=10,
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"REGEXP_CONTAINS(name, 'installer') "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` ASC LIMIT 10"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                extra_patterns={'name': '^ose-installer$'},
                order_by='start_time',
                sorting='ASC',
                limit=10,
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"REGEXP_CONTAINS(name, '^ose-installer$') "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` ASC LIMIT 10"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                extra_patterns={'name': 'installer', 'group': 'openshift'},
                order_by='start_time',
                sorting='ASC',
                limit=10,
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"REGEXP_CONTAINS(name, 'installer') AND REGEXP_CONTAINS(`group`, 'openshift') "
            "AND start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` ASC LIMIT 10"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                where={
                    'engine': [Engine.BREW, Engine.KONFLUX],
                    'name': ['ironic', 'ose-installer'],
                },
                order_by='start_time',
                sorting='ASC',
                limit=10,
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            "engine IN ('brew', 'konflux') AND name IN ('ironic', 'ose-installer') AND "
            "start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` ASC LIMIT 10"
        )

        query_mock.reset_mock()
        await anext(
            self.db.search_builds_by_fields(
                start_search=start_search,
                where={'name': 'test-operator-fbc', 'group': 'openshift-4.18'},
                array_contains={'bundle_nvrs': 'test-operator-bundle-container-v4.18.0-123'},
                order_by='start_time',
                sorting='DESC',
                limit=1,
            ),
            None,
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE outcome IN ('success', 'failure') AND "
            f"name = 'test-operator-fbc' AND `group` = 'openshift-4.18' AND "
            f"'test-operator-bundle-container-v4.18.0-123' IN UNNEST(bundle_nvrs) AND "
            f"start_time >= '2024-09-23 09:00:00+00:00' AND start_time < '2024-09-30 09:00:00+00:00' "
            "ORDER BY `start_time` DESC LIMIT 1"
        )

    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_search_builds_by_fields_windowed(self, mock_query_async: AsyncMock):
        mocked_rows = [
            [Row(('ironic', '1.0.0', '3'), {'name': 0, 'version': 1, 'release': 2})],
            [],
            [],
            [
                Row(('ironic', '1.0.0', '2'), {'name': 0, 'version': 1, 'release': 2}),
                Row(('ironic', '1.0.0', '1'), {'name': 0, 'version': 1, 'release': 2}),
            ],
        ]
        mock_query_async.side_effect = [
            MagicMock(total_rows=len(batch), __iter__=MagicMock(return_value=iter(batch))) for batch in mocked_rows
        ]
        records = [
            record
            async for record in self.db.search_builds_by_fields(
                start_search=datetime(2024, 10, 1, 8, 0, 0, tzinfo=timezone.utc),
                end_search=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                window_size=30,
                where={'name': 'ironic', 'group': 'openshift-4.18'},
            )
        ]
        expected_queries = [
            "SELECT * FROM `builds` WHERE outcome IN ('success', 'failure') AND name = 'ironic' AND `group` = 'openshift-4.18' AND start_time >= '2024-12-02 12:00:00+00:00' AND start_time < '2025-01-01 12:00:00+00:00' ORDER BY `start_time` DESC",
            "SELECT * FROM `builds` WHERE outcome IN ('success', 'failure') AND name = 'ironic' AND `group` = 'openshift-4.18' AND start_time >= '2024-11-02 12:00:00+00:00' AND start_time < '2024-12-02 12:00:00+00:00' ORDER BY `start_time` DESC",
            "SELECT * FROM `builds` WHERE outcome IN ('success', 'failure') AND name = 'ironic' AND `group` = 'openshift-4.18' AND start_time >= '2024-10-03 12:00:00+00:00' AND start_time < '2024-11-02 12:00:00+00:00' ORDER BY `start_time` DESC",
            "SELECT * FROM `builds` WHERE outcome IN ('success', 'failure') AND name = 'ironic' AND `group` = 'openshift-4.18' AND start_time >= '2024-10-01 08:00:00+00:00' AND start_time < '2024-10-03 12:00:00+00:00' ORDER BY `start_time` DESC",
        ]
        actual_queries = [call[0][0] for call in mock_query_async.await_args_list]
        self.assertListEqual(actual_queries, expected_queries)
        self.assertEqual(len(records), 3)

    @patch('artcommonlib.konflux.konflux_db.datetime')
    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_get_latest_build(self, query_mock, datetime_mock):
        now = datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        lower_bound = now - 3 * timedelta(days=30)
        datetime_mock.now.return_value = now

        await self.db.get_latest_build(name='ironic', group='openshift-4.18', outcome='success')
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ironic' "
            "AND `group` = 'openshift-4.18' AND outcome = 'success' "
            f"AND start_time >= '{str(lower_bound)}' "
            f"AND start_time < '{now}' "
            "ORDER BY `start_time` DESC LIMIT 1"
        )

        query_mock.reset_mock()
        await self.db.get_latest_build(name='ironic', group='openshift-4.18', outcome='success', assembly='stream')
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ironic' "
            "AND `group` = 'openshift-4.18' AND outcome = 'success' "
            "AND assembly = 'stream' "
            f"AND start_time >= '{str(lower_bound)}' "
            f"AND start_time < '{now}' "
            "ORDER BY `start_time` DESC LIMIT 1"
        )

        query_mock.reset_mock()
        await self.db.get_latest_build(
            name='ironic', group='openshift-4.18', outcome='success', completed_before=now, assembly='stream'
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ironic' "
            "AND `group` = 'openshift-4.18' AND outcome = 'success' "
            "AND assembly = 'stream' AND end_time IS NOT NULL "
            f"AND end_time <= '{now}' "
            f"AND start_time >= '{str(lower_bound)}' "
            f"AND start_time < '{now}' "
            "ORDER BY `start_time` DESC LIMIT 1"
        )

        query_mock.reset_mock()
        like = {'release': 'b45ea65'}
        await self.db.get_latest_build(
            name='ironic', group='openshift-4.18', outcome='success', extra_patterns=like, assembly='stream'
        )
        query_mock.assert_called_once_with(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ironic' "
            "AND `group` = 'openshift-4.18' AND outcome = 'success' "
            "AND assembly = 'stream' "
            f"AND `release` LIKE '%%b45ea65%%' "
            f"AND start_time >= '{str(lower_bound)}' "
            f"AND start_time < '{now}' "
            "ORDER BY `start_time` DESC LIMIT 1"
        )

        query_mock.reset_mock()
        await self.db.get_latest_builds(
            names=['ironic', 'ose-installer-artifacts'], group='openshift-4.18', outcome=KonfluxBuildOutcome.SUCCESS
        )

        actual_calls = [query_mock.call_args_list[x][0][0] for x in range(0, 2)]
        self.assertIn(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ironic' "
            "AND `group` = 'openshift-4.18' AND outcome = 'success' "
            "AND assembly = 'stream' "
            f"AND start_time >= '{str(lower_bound)}' "
            f"AND start_time < '{now}' "
            "ORDER BY `start_time` DESC LIMIT 1",
            actual_calls,
        )

        self.assertIn(
            f"SELECT * FROM `{constants.BUILDS_TABLE_ID}` WHERE name = 'ose-installer-artifacts' "
            "AND `group` = 'openshift-4.18' AND outcome = 'success' "
            "AND assembly = 'stream' "
            f"AND start_time >= '{str(lower_bound)}' "
            f"AND start_time < '{now}' "
            "ORDER BY `start_time` DESC LIMIT 1",
            actual_calls,
        )

    @patch('artcommonlib.konflux.konflux_db.datetime')
    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_get_latest_build_windowed(self, mock_query_async: AsyncMock, datetime_mock: MagicMock):
        datetime_mock.now.return_value = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_query_async.side_effect = [
            MagicMock(total_rows=0, __next__=MagicMock(side_effect=StopIteration)),
            MagicMock(total_rows=0, __next__=MagicMock(side_effect=StopIteration)),
            MagicMock(total_rows=0, __next__=MagicMock(side_effect=StopIteration)),
            MagicMock(
                total_rows=1,
                __next__=MagicMock(
                    return_value=Row(
                        ('ironic', '1.0.0', '2', "ironic-1.0.0-2"), {'name': 0, 'version': 1, 'release': 2, 'nvr': 3}
                    )
                ),
            ),
        ]
        record = await self.db.get_latest_build(name='ironic', group='openshift-4.18', assembly='stream')
        expected_queries = [
            "SELECT * FROM `builds` WHERE name = 'ironic' AND `group` = 'openshift-4.18' AND outcome = 'success' AND assembly = 'stream' AND start_time >= '2024-10-03 12:00:00+00:00' AND start_time < '2025-01-01 12:00:00+00:00' ORDER BY `start_time` DESC LIMIT 1",
            "SELECT * FROM `builds` WHERE name = 'ironic' AND `group` = 'openshift-4.18' AND outcome = 'success' AND assembly = 'stream' AND start_time >= '2024-07-05 12:00:00+00:00' AND start_time < '2024-10-03 12:00:00+00:00' ORDER BY `start_time` DESC LIMIT 1",
            "SELECT * FROM `builds` WHERE name = 'ironic' AND `group` = 'openshift-4.18' AND outcome = 'success' AND assembly = 'stream' AND start_time >= '2024-04-06 12:00:00+00:00' AND start_time < '2024-07-05 12:00:00+00:00' ORDER BY `start_time` DESC LIMIT 1",
            "SELECT * FROM `builds` WHERE name = 'ironic' AND `group` = 'openshift-4.18' AND outcome = 'success' AND assembly = 'stream' AND start_time >= '2024-01-07 12:00:00+00:00' AND start_time < '2024-04-06 12:00:00+00:00' ORDER BY `start_time` DESC LIMIT 1",
        ]
        actual_queries = [call[0][0] for call in mock_query_async.await_args_list]
        self.assertListEqual(actual_queries, expected_queries)
        self.assertEqual(record.nvr, "ironic-1.0.0-2")

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
            SchemaField('installed_rpms', 'STRING', 'REPEATED'),
            SchemaField('parent_images', 'STRING', 'REPEATED'),
            SchemaField('source_repo', 'STRING', 'REQUIRED'),
            SchemaField('commitish', 'STRING', 'REQUIRED'),
            SchemaField('rebase_repo_url', 'STRING', 'REQUIRED'),
            SchemaField('rebase_commitish', 'STRING', 'REQUIRED'),
            SchemaField('embargoed', 'BOOLEAN', 'REQUIRED'),
            SchemaField('hermetic', 'BOOLEAN', 'REQUIRED'),
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
            SchemaField('build_component', 'STRING', 'REQUIRED'),
            SchemaField('build_priority', 'STRING', 'REQUIRED'),
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
            SchemaField('bundle_package_name', 'STRING', 'REQUIRED'),
            SchemaField('bundle_csv_name', 'STRING', 'REQUIRED'),
            SchemaField('record_id', 'STRING', 'REQUIRED'),
            SchemaField('build_id', 'STRING', 'REQUIRED'),
            SchemaField('nvr', 'STRING', 'REQUIRED'),
            SchemaField('build_component', 'STRING', 'REQUIRED'),
        ]
        self.db.bind(KonfluxBundleBuildRecord)
        self.assertEqual(self.db.generate_build_schema(), expected_fields)

    @patch("artcommonlib.konflux.konflux_db.KonfluxDb.search_builds_by_fields")
    async def test_get_build_record_by_nvr(self, search_builds_by_fields_mock: MagicMock):
        search_builds_by_fields_mock.return_value.__anext__.return_value = KonfluxBundleBuildRecord(
            nvr='ironic-1.2.3-4.el8'
        )
        nvr = 'ironic-1.2.3-4.el8'
        build = await self.db.get_build_record_by_nvr('ironic-1.2.3-4.el8')
        self.assertEqual(build.nvr, nvr)
        search_builds_by_fields_mock.assert_called_once_with(
            where={'nvr': nvr, 'outcome': str(KonfluxBuildOutcome.SUCCESS)}, limit=1
        )
