import threading
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock, patch

from artcommonlib import constants
from artcommonlib.bigquery import QUERY_TIMEOUT_SECONDS, REQUEST_TIMEOUT_SECONDS, BigQueryClient
from sqlalchemy import Column, String


class TestBigQuery(IsolatedAsyncioTestCase):
    @patch('os.environ', {'GOOGLE_APPLICATION_CREDENTIALS': ''})
    @patch('artcommonlib.bigquery.bigquery.Client')
    def setUp(self, _):
        self.client = BigQueryClient()
        self.client._table_ref = constants.BUILDS_TABLE_ID


class TestInsert(TestBigQuery):
    @patch('artcommonlib.bigquery.BigQueryClient.query')
    def test_insert(self, query_mock):
        query_mock.reset_mock()
        self.client.insert({'name': "'ironic'"})
        query_mock.assert_called_once_with(f"INSERT INTO `{constants.BUILDS_TABLE_ID}` (`name`) VALUES ('ironic')")

        query_mock.reset_mock()
        self.client.insert({'name': "'ironic'", 'group': "'openshift-4.18'"})
        query_mock.assert_called_once_with(
            f"INSERT INTO `{constants.BUILDS_TABLE_ID}` (`name`, `group`) VALUES ('ironic', 'openshift-4.18')"
        )
        return


class TestQuery(TestBigQuery):
    @patch('artcommonlib.bigquery.monotonic', side_effect=[100, 110])
    def test_query_bounds_submission_and_result_wait(self, _):
        results = Mock(total_rows=2)
        job = self.client.client.query.return_value
        job.result.return_value = results

        self.assertIs(self.client.query('SELECT 1'), results)

        submit_kwargs = self.client.client.query.call_args.kwargs
        self.assertEqual(submit_kwargs['timeout'], REQUEST_TIMEOUT_SECONDS)
        self.assertEqual(submit_kwargs['retry'].timeout, REQUEST_TIMEOUT_SECONDS)
        self.assertEqual(submit_kwargs['job_retry'].timeout, REQUEST_TIMEOUT_SECONDS)
        result_kwargs = job.result.call_args.kwargs
        self.assertEqual(result_kwargs['timeout'], QUERY_TIMEOUT_SECONDS - 10)
        self.assertEqual(result_kwargs['retry'].timeout, REQUEST_TIMEOUT_SECONDS)
        self.assertEqual(result_kwargs['job_retry'].timeout, REQUEST_TIMEOUT_SECONDS)

    @patch('artcommonlib.bigquery.monotonic', side_effect=[100, 100 + QUERY_TIMEOUT_SECONDS + 1])
    def test_query_does_not_wait_after_submission_exhausts_budget(self, _):
        with self.assertRaises(TimeoutError):
            self.client.query('SELECT 1')

        self.client.client.query.return_value.result.assert_not_called()

    def test_query_reports_result_timeout(self):
        self.client.client.query.return_value.result.side_effect = TimeoutError

        with patch.object(self.client.logger, 'error') as log_error:
            with self.assertRaises(TimeoutError):
                self.client.query('SELECT 1')

        log_error.assert_called_once_with('BigQuery query timed out (budget: %s seconds)', QUERY_TIMEOUT_SECONDS)

    async def test_async_query_offloads_submission_and_result_wait(self):
        event_loop_thread = threading.get_ident()
        worker_threads = []
        results = Mock(total_rows=1)
        job = Mock()

        def submit(*args, **kwargs):
            worker_threads.append(threading.get_ident())
            return job

        def wait(*args, **kwargs):
            worker_threads.append(threading.get_ident())
            return results

        self.client.client.query.side_effect = submit
        job.result.side_effect = wait

        self.assertIs(await self.client.query_async('SELECT 1'), results)
        self.assertEqual(len(worker_threads), 2)
        self.assertEqual(worker_threads[0], worker_threads[1])
        self.assertNotEqual(worker_threads[0], event_loop_thread)


class TestSelect(TestBigQuery):
    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_where_clauses(self, query_mock):
        await self.client.select()
        query_mock.assert_called_once_with('SELECT * FROM `builds`')

        query_mock.reset_mock()
        await self.client.select(where_clauses=[])
        query_mock.assert_called_once_with('SELECT * FROM `builds`')

        query_mock.reset_mock()
        await self.client.select(where_clauses=None)
        query_mock.assert_called_once_with('SELECT * FROM `builds`')

        query_mock.reset_mock()
        where_clauses = [Column('name', String) == 'ironic']
        await self.client.select(where_clauses=where_clauses)
        query_mock.assert_called_once_with("SELECT * FROM `builds` WHERE name = 'ironic'")

        query_mock.reset_mock()
        where_clauses = [Column('name', String) == 'ironic', Column('group', String) == 'openshift-4.18']
        await self.client.select(where_clauses=where_clauses)
        query_mock.assert_called_once_with(
            "SELECT * FROM `builds` WHERE name = 'ironic' AND `group` = 'openshift-4.18'"
        )

    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_order_by(self, query_mock):
        order_by_clause = None
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with('SELECT * FROM `builds`')

        query_mock.reset_mock()
        order_by_clause = Column('start_time', quote=True)
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with('SELECT * FROM `builds` ORDER BY `start_time`')

        query_mock.reset_mock()
        order_by_clause = Column('start_time', quote=True).desc()
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with('SELECT * FROM `builds` ORDER BY `start_time` DESC')

        query_mock.reset_mock()
        order_by_clause = Column('start_time', quote=True).asc()
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with('SELECT * FROM `builds` ORDER BY `start_time` ASC')

    @patch('artcommonlib.bigquery.BigQueryClient.query_async')
    async def test_limit(self, query_mock):
        await self.client.select(limit=None)
        query_mock.assert_called_once_with('SELECT * FROM `builds`')

        query_mock.reset_mock()
        await self.client.select(limit=0)
        query_mock.assert_called_once_with('SELECT * FROM `builds` LIMIT 0')

        query_mock.reset_mock()
        await self.client.select(limit=10)
        query_mock.assert_called_once_with('SELECT * FROM `builds` LIMIT 10')

        query_mock.reset_mock()
        with self.assertRaises(AssertionError):
            await self.client.select(limit=-1)

        query_mock.reset_mock()
        with self.assertRaises(AssertionError):
            await self.client.select(limit='1')
