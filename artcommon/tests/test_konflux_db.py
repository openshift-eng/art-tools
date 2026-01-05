import asyncio
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib import constants
from artcommonlib.konflux.konflux_build_record import (
    ArtifactType,
    Engine,
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
)
from artcommonlib.konflux.konflux_db import CacheRecordsType, KonfluxDb
from google.cloud.bigquery import Row, SchemaField


class TestKonfluxDB(IsolatedAsyncioTestCase):
    @patch('os.environ', {'GOOGLE_APPLICATION_CREDENTIALS': ''})
    @patch('artcommonlib.bigquery.bigquery.Client')
    def setUp(self, _):
        # Clear the shared cache before each test to ensure test isolation
        KonfluxDb.clear_shared_cache()

        self.db = KonfluxDb()
        self.db.bind(KonfluxBuildRecord)
        self.db.bq_client._table_ref = constants.BUILDS_TABLE_ID

    def tearDown(self):
        # Clear the shared cache after each test to ensure test isolation
        KonfluxDb.clear_shared_cache()

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

    def test_add_builds_cache_validation(self):
        """Test that cache.add_builds validates all builds are from the same group"""
        cache = self.db.cache

        # Test 1: All builds from same group - should succeed
        builds_same_group = [
            KonfluxBuildRecord(name='build1', version='1.0', release='1.el8', group='openshift-4.18'),
            KonfluxBuildRecord(name='build2', version='2.0', release='2.el8', group='openshift-4.18'),
            KonfluxBuildRecord(name='build3', version='3.0', release='3.el8', group='openshift-4.18'),
        ]
        cache.add_builds(builds_same_group)  # Should not raise

        # Test 2: Builds from different groups - should raise ValueError
        builds_mixed_groups = [
            KonfluxBuildRecord(name='build1', version='1.0', release='1.el8', group='openshift-4.18'),
            KonfluxBuildRecord(name='build2', version='2.0', release='2.el8', group='openshift-4.17'),
            KonfluxBuildRecord(name='build3', version='3.0', release='3.el8', group='openshift-4.18'),
        ]
        with self.assertRaises(ValueError) as ctx:
            cache.add_builds(builds_mixed_groups)
        self.assertIn("All builds must be from group 'openshift-4.18'", str(ctx.exception))
        self.assertIn("openshift-4.17", str(ctx.exception))

        # Test 3: No group provided and first build has no group - should raise ValueError
        builds_no_group = [
            KonfluxBuildRecord(name='build1', version='1.0', release='1.el8', group=None),
        ]
        with self.assertRaises(ValueError) as ctx:
            cache.add_builds(builds_no_group)
        self.assertIn("no group provided and builds[0].group is None", str(ctx.exception))

        # Test 4: Explicit group parameter overrides build.group - validates against explicit group
        builds_with_explicit_group = [
            KonfluxBuildRecord(name='build1', version='1.0', release='1.el8', group='openshift-4.17'),
        ]
        with self.assertRaises(ValueError) as ctx:
            cache.add_builds(builds_with_explicit_group, group='openshift-4.18')
        self.assertIn("All builds must be from group 'openshift-4.18'", str(ctx.exception))

    @patch('artcommonlib.konflux.konflux_db.datetime')
    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_search_builds_by_fields(self, query_mock, datetime_mock):
        datetime_mock.now.return_value = datetime(2024, 9, 30, 9, 0, 0, tzinfo=timezone.utc)
        start_search = datetime(2024, 9, 23, 9, 0, 0, 0, tzinfo=timezone.utc)
        await anext(self.db.search_builds_by_fields(start_search=start_search, where={}), None)
        # Default behavior is to NOT exclude large columns (exclude_large_columns=None by default)
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

    @patch('artcommonlib.konflux.konflux_db.datetime')
    @patch('artcommonlib.bigquery.BigQueryClient.select')
    async def test_search_builds_by_fields_windowed(self, select_mock, datetime_mock):
        """
        Test exponential window search behavior.
        Verifies that search_builds_by_fields queries progressive time windows
        (7, 14, 28, 56, 112, 224, 448 days) until results are found or limit is reached.
        """
        # Mock current time
        now = datetime(2024, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        datetime_mock.now.return_value = now

        # Mock BigQuery responses - first window returns results
        mock_row = MagicMock()
        mock_response = MagicMock()
        mock_response.total_rows = 1
        mock_response.__iter__ = MagicMock(return_value=iter([mock_row]))
        select_mock.return_value = mock_response

        # Mock from_result_row to return a build
        mock_build = KonfluxBuildRecord(name='test-build', version='1.0.0', release='1.el8', group='openshift-4.18')
        with patch.object(self.db, 'from_result_row', return_value=mock_build):
            # Test 1: Results found in first window (7 days)
            results = [
                build
                async for build in self.db.search_builds_by_fields(
                    where={'name': 'test-build', 'group': 'openshift-4.18'}, limit=1
                )
            ]

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].name, 'test-build')
            # Should only query once (first 7-day window)
            self.assertEqual(select_mock.call_count, 1)

        # Test 2: Empty windows until 28-day window
        select_mock.reset_mock()

        # First two windows return empty results
        empty_response = MagicMock()
        empty_response.total_rows = 0
        empty_response.__iter__ = MagicMock(return_value=iter([]))

        # Third window (28 days) returns result
        found_response = MagicMock()
        found_response.total_rows = 1
        found_response.__iter__ = MagicMock(return_value=iter([mock_row]))

        select_mock.side_effect = [empty_response, empty_response, found_response]

        with patch.object(self.db, 'from_result_row', return_value=mock_build):
            results = [
                build
                async for build in self.db.search_builds_by_fields(
                    where={'name': 'old-build', 'group': 'openshift-4.18'}, limit=1
                )
            ]

            self.assertEqual(len(results), 1)
            # Should query 3 times (7, 14, 28 day windows)
            self.assertEqual(select_mock.call_count, 3)

        # Test 3: Respects start_search boundary
        select_mock.reset_mock()
        select_mock.side_effect = None
        select_mock.return_value = empty_response

        # Set start_search to only allow searching back 10 days
        start_search = now - timedelta(days=10)

        with patch.object(self.db, 'from_result_row', return_value=mock_build):
            results = [
                build
                async for build in self.db.search_builds_by_fields(
                    start_search=start_search, where={'name': 'bounded-build'}, limit=1
                )
            ]

            # Should only query windows that fit within the boundary
            # 7-day window fits, 14-day window will be constrained to start_search
            self.assertGreaterEqual(select_mock.call_count, 1)
            self.assertLessEqual(select_mock.call_count, 2)

    @patch('artcommonlib.konflux.konflux_db.KonfluxDb._ensure_group_cached')
    @patch('artcommonlib.konflux.konflux_db.KonfluxDb.from_result_row')
    @patch('artcommonlib.bigquery.BigQueryClient.select')
    async def test_get_latest_build(self, select_mock, from_row_mock, ensure_cached_mock):
        # Mock lazy loading to do nothing
        ensure_cached_mock.return_value = None

        # Mock the BigQuery response
        expected_build = KonfluxBundleBuildRecord(
            name='ironic',
            version='1.2.3',
            release='4.el8',
            group='openshift-4.18',
            outcome='success',
            start_time=datetime.now(tz=timezone.utc),
            end_time=datetime.now(tz=timezone.utc),
        )

        # Mock from_result_row to return our build
        from_row_mock.return_value = expected_build

        # Create a mock response with total_rows attribute
        mock_response = MagicMock()
        mock_response.total_rows = 1
        mock_response.__iter__ = MagicMock(return_value=iter([MagicMock()]))  # Iterator needs at least one item
        select_mock.return_value = mock_response

        # Test basic call
        result = await self.db.get_latest_build(
            name='ironic', group='openshift-4.18', outcome=KonfluxBuildOutcome.SUCCESS
        )

        self.assertEqual(result.nvr, 'ironic-1.2.3-4.el8')
        self.assertEqual(result.name, 'ironic')
        # Verify cache was loaded with correct cache_type (ALL_COLUMNS by default since exclude_large_columns=None)
        ensure_cached_mock.assert_called_once_with('openshift-4.18', cache_type=CacheRecordsType.ALL_COLUMNS)
        select_mock.assert_called_once()

        # Test with assembly parameter
        select_mock.reset_mock()
        from_row_mock.reset_mock()
        ensure_cached_mock.reset_mock()
        from_row_mock.return_value = expected_build
        mock_response.__iter__ = MagicMock(return_value=iter([MagicMock()]))
        select_mock.return_value = mock_response

        result = await self.db.get_latest_build(
            name='ironic', group='openshift-4.18', outcome=KonfluxBuildOutcome.SUCCESS, assembly='stream'
        )

        self.assertEqual(result.nvr, 'ironic-1.2.3-4.el8')
        ensure_cached_mock.assert_called_once_with('openshift-4.18', cache_type=CacheRecordsType.ALL_COLUMNS)
        select_mock.assert_called_once()

        # Test cache disabled
        select_mock.reset_mock()
        from_row_mock.reset_mock()
        ensure_cached_mock.reset_mock()
        from_row_mock.return_value = expected_build
        mock_response.__iter__ = MagicMock(return_value=iter([MagicMock()]))
        select_mock.return_value = mock_response

        result = await self.db.get_latest_build(
            name='ironic', group='openshift-4.18', outcome=KonfluxBuildOutcome.SUCCESS, use_cache=False
        )

        self.assertEqual(result.nvr, 'ironic-1.2.3-4.el8')
        # When use_cache=False, _ensure_group_cached should not be called
        ensure_cached_mock.assert_not_called()
        select_mock.assert_called_once()

    @patch('artcommonlib.konflux.konflux_db.KonfluxDb._ensure_group_cached')
    @patch('artcommonlib.konflux.konflux_db.KonfluxDb.from_result_row')
    @patch('artcommonlib.bigquery.BigQueryClient.select')
    async def test_get_latest_builds(self, select_mock, from_row_mock, ensure_cached_mock):
        # Mock lazy loading to do nothing
        ensure_cached_mock.return_value = None

        # Mock the BigQuery response
        now = datetime.now(tz=timezone.utc)
        build1 = KonfluxBundleBuildRecord(
            name='ironic',
            version='1.2.3',
            release='4.el8',
            group='openshift-4.18',
            start_time=now,
            end_time=now,
        )
        build2 = KonfluxBundleBuildRecord(
            name='ose-installer-artifacts',
            version='1.0.0',
            release='1.el8',
            group='openshift-4.18',
            start_time=now,
            end_time=now,
        )

        # Track which build to return based on call order
        builds = [build1, build2]
        from_row_mock.side_effect = builds

        mock_response = MagicMock()
        mock_response.total_rows = 1
        mock_response.__iter__ = MagicMock(return_value=iter([MagicMock()]))
        select_mock.return_value = mock_response

        results = await self.db.get_latest_builds(
            names=['ironic', 'ose-installer-artifacts'], group='openshift-4.18', outcome=KonfluxBuildOutcome.SUCCESS
        )

        self.assertEqual(len(results), 2)
        self.assertIn('ironic', [r.name for r in results if r])
        self.assertIn('ose-installer-artifacts', [r.name for r in results if r])
        # _ensure_group_cached should be called once per component
        self.assertEqual(ensure_cached_mock.call_count, 2)

    @patch('artcommonlib.konflux.konflux_db.KonfluxDb._ensure_group_cached')
    @patch('artcommonlib.konflux.konflux_db.datetime')
    @patch('artcommonlib.bigquery.BigQueryClient.select')
    async def test_get_latest_build_windowed(self, select_mock, datetime_mock, ensure_cached_mock):
        """
        Test that get_latest_build uses exponential window search on cache miss.
        Verifies cache is checked first, then falls back to windowed BigQuery search.
        """
        # Mock current time
        now = datetime(2024, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        datetime_mock.now.return_value = now

        # Mock lazy loading to do nothing (simulating cache miss)
        ensure_cached_mock.return_value = None

        # Simulate cache miss by having empty cache
        self.db.cache.get_by_name = MagicMock(return_value=[])
        self.db.cache.get_by_nvr = MagicMock(return_value=None)

        # Mock BigQuery response - build found in second window (14 days)
        expected_build = KonfluxBuildRecord(
            name='old-build',
            version='2.0.0',
            release='5.el8',
            group='openshift-4.18',
            outcome='success',
            start_time=now - timedelta(days=12),  # 12 days old
        )

        # First window (7 days) returns empty
        empty_response = MagicMock()
        empty_response.total_rows = 0
        empty_response.__iter__ = MagicMock(return_value=iter([]))

        # Second window (14 days) returns the build
        found_response = MagicMock()
        found_response.total_rows = 1
        mock_row = MagicMock()
        found_response.__iter__ = MagicMock(return_value=iter([mock_row]))

        select_mock.side_effect = [empty_response, found_response]

        with patch.object(self.db, 'from_result_row', return_value=expected_build):
            result = await self.db.get_latest_build(
                name='old-build', group='openshift-4.18', outcome=KonfluxBuildOutcome.SUCCESS
            )

            self.assertIsNotNone(result)
            self.assertEqual(result.name, 'old-build')
            self.assertEqual(result.nvr, 'old-build-2.0.0-5.el8')

            # Cache should be checked/loaded (ALL_COLUMNS by default since exclude_large_columns=None)
            ensure_cached_mock.assert_called_once_with('openshift-4.18', cache_type=CacheRecordsType.ALL_COLUMNS)

            # Should have queried 2 windows (7 days empty, then 14 days with result)
            self.assertEqual(select_mock.call_count, 2)

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
            SchemaField('build_priority', 'INTEGER', 'REQUIRED'),
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
            SchemaField('build_priority', 'INTEGER', 'REQUIRED'),
        ]
        self.db.bind(KonfluxBundleBuildRecord)
        self.assertEqual(self.db.generate_build_schema(), expected_fields)

    @patch("artcommonlib.konflux.konflux_db.KonfluxDb.get_latest_build")
    async def test_get_build_record_by_nvr(self, get_latest_build_mock: MagicMock):
        nvr = 'ironic-1.2.3-4.el8'
        expected_build = KonfluxBundleBuildRecord(nvr=nvr)
        get_latest_build_mock.return_value = expected_build

        build = await self.db.get_build_record_by_nvr(nvr)

        self.assertEqual(build.nvr, nvr)
        get_latest_build_mock.assert_called_once_with(
            nvr=nvr, outcome=KonfluxBuildOutcome.SUCCESS, strict=True, exclude_large_columns=False
        )

    def test_get_latest_build_with_string_enums(self):
        """Test that string enum parameters can be converted to enums for cache comparisons"""
        # This tests the critical fix: elliott CLI passes enum parameters as strings (e.g., 'konflux', 'success'),
        # and get_latest_build()/get_latest_builds()/cache.get_by_name() must convert them to enums
        # for proper cache comparisons. The type hints now reflect this: Union[EnumType, str].

        # Verify string-to-enum conversion works correctly
        test_cases = [
            ('success', KonfluxBuildOutcome.SUCCESS),
            ('failure', KonfluxBuildOutcome.FAILURE),
            ('konflux', Engine.KONFLUX),
            ('brew', Engine.BREW),
            ('image', ArtifactType.IMAGE),
            ('rpm', ArtifactType.RPM),
        ]

        for string_val, expected_enum in test_cases:
            if isinstance(expected_enum, KonfluxBuildOutcome):
                result = KonfluxBuildOutcome(string_val)
            elif isinstance(expected_enum, Engine):
                result = Engine(string_val)
            elif isinstance(expected_enum, ArtifactType):
                result = ArtifactType(string_val)

            self.assertEqual(result, expected_enum, f"String '{string_val}' should convert to {expected_enum}")

            # Verify enum equality works (critical for cache lookups)
            self.assertTrue(result == expected_enum, f"Converted enum {result} should equal {expected_enum}")

    def test_cache_prioritizes_successful_builds(self):
        """Test that cache prioritizes successful builds over failed builds for the same NVR"""
        cache = self.db.cache
        group = 'openshift-4.18'
        nvr = 'test-build-1.0.0-1.el8'

        # Create two builds with same NVR but different outcomes
        failed_build = KonfluxBuildRecord(
            name='test-build',
            version='1.0.0',
            release='1.el8',
            group=group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.FAILURE,
            start_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        )

        successful_build = KonfluxBuildRecord(
            name='test-build',
            version='1.0.0',
            release='1.el8',
            group=group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            start_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        )

        # Test 1: Add failed build first, then successful build - should keep successful
        cache.add_builds([failed_build, successful_build], group)
        cached = cache.get_by_nvr(nvr, group)
        self.assertIsNotNone(cached)
        self.assertEqual(cached.outcome, KonfluxBuildOutcome.SUCCESS, "Should prioritize successful build")

        # Clear and test opposite order
        cache.clear(group)

        # Test 2: Add successful build first, then failed build - should keep successful
        cache.add_builds([successful_build, failed_build], group)
        cached = cache.get_by_nvr(nvr, group)
        self.assertIsNotNone(cached)
        self.assertEqual(
            cached.outcome, KonfluxBuildOutcome.SUCCESS, "Should keep successful build even when failed comes after"
        )

    @patch('artcommonlib.konflux.konflux_db.KonfluxDb._ensure_group_cached')
    @patch('artcommonlib.bigquery.BigQueryClient.select')
    async def test_get_latest_build_checks_outcome_from_cache(self, select_mock, ensure_cached_mock):
        """Test that get_latest_build verifies outcome when returning from cache"""
        ensure_cached_mock.return_value = None

        group = 'openshift-4.18'
        nvr = 'test-build-1.0.0-1.el8'

        # Manually populate cache with a failed build
        failed_build = KonfluxBuildRecord(
            name='test-build',
            version='1.0.0',
            release='1.el8',
            group=group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.FAILURE,
            start_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        )
        self.db.cache.add_builds([failed_build], group)

        # Mock BigQuery to return a successful build
        successful_build = KonfluxBuildRecord(
            name='test-build',
            version='1.0.0',
            release='1.el8',
            group=group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            start_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        )

        mock_row = MagicMock()
        mock_response = MagicMock()
        mock_response.total_rows = 1
        mock_response.__iter__ = MagicMock(return_value=iter([mock_row]))
        select_mock.return_value = mock_response

        with patch.object(self.db, 'from_result_row', return_value=successful_build):
            # Request successful build - cache has failed, should fall through to BigQuery
            result = await self.db.get_latest_build(nvr=nvr, group=group, outcome=KonfluxBuildOutcome.SUCCESS)

            self.assertIsNotNone(result)
            self.assertEqual(
                result.outcome,
                KonfluxBuildOutcome.SUCCESS,
                "Should return successful build from BigQuery, not failed from cache",
            )
            # BigQuery should have been called since cache had wrong outcome
            select_mock.assert_called_once()
