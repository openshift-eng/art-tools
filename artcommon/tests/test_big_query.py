from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from artcommonlib import constants
from artcommonlib.bigquery import BigQueryClient
from sqlalchemy import Column, String


class TestBigQuery(IsolatedAsyncioTestCase):
    @patch("os.environ", {"GOOGLE_APPLICATION_CREDENTIALS": ""})
    @patch("artcommonlib.bigquery.bigquery.Client")
    def setUp(self, _):
        self.client = BigQueryClient()
        self.client._table_ref = constants.BUILDS_TABLE_ID


class TestInsert(TestBigQuery):
    @patch("artcommonlib.bigquery.BigQueryClient.query")
    def test_insert(self, query_mock):
        query_mock.reset_mock()
        self.client.insert({"name": "'ironic'"})
        query_mock.assert_called_once_with(f"INSERT INTO `{constants.BUILDS_TABLE_ID}` (`name`) VALUES ('ironic')")

        query_mock.reset_mock()
        self.client.insert({"name": "'ironic'", "group": "'openshift-4.18'"})
        query_mock.assert_called_once_with(
            f"INSERT INTO `{constants.BUILDS_TABLE_ID}` (`name`, `group`) VALUES ('ironic', 'openshift-4.18')"
        )
        return


class TestSelect(TestBigQuery):
    @patch("artcommonlib.bigquery.BigQueryClient.query_async")
    async def test_where_clauses(self, query_mock):
        await self.client.select()
        query_mock.assert_called_once_with("SELECT * FROM `builds`")

        query_mock.reset_mock()
        await self.client.select(where_clauses=[])
        query_mock.assert_called_once_with("SELECT * FROM `builds`")

        query_mock.reset_mock()
        await self.client.select(where_clauses=None)
        query_mock.assert_called_once_with("SELECT * FROM `builds`")

        query_mock.reset_mock()
        where_clauses = [Column("name", String) == "ironic"]
        await self.client.select(where_clauses=where_clauses)
        query_mock.assert_called_once_with("SELECT * FROM `builds` WHERE name = 'ironic'")

        query_mock.reset_mock()
        where_clauses = [Column("name", String) == "ironic", Column("group", String) == "openshift-4.18"]
        await self.client.select(where_clauses=where_clauses)
        query_mock.assert_called_once_with(
            "SELECT * FROM `builds` WHERE name = 'ironic' AND `group` = 'openshift-4.18'"
        )

    @patch("artcommonlib.bigquery.BigQueryClient.query_async")
    async def test_order_by(self, query_mock):
        order_by_clause = None
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with("SELECT * FROM `builds`")

        query_mock.reset_mock()
        order_by_clause = Column("start_time", quote=True)
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with("SELECT * FROM `builds` ORDER BY `start_time`")

        query_mock.reset_mock()
        order_by_clause = Column("start_time", quote=True).desc()
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with("SELECT * FROM `builds` ORDER BY `start_time` DESC")

        query_mock.reset_mock()
        order_by_clause = Column("start_time", quote=True).asc()
        await self.client.select(order_by_clause=order_by_clause)
        query_mock.assert_called_once_with("SELECT * FROM `builds` ORDER BY `start_time` ASC")

    @patch("artcommonlib.bigquery.BigQueryClient.query_async")
    async def test_limit(self, query_mock):
        await self.client.select(limit=None)
        query_mock.assert_called_once_with("SELECT * FROM `builds`")

        query_mock.reset_mock()
        await self.client.select(limit=0)
        query_mock.assert_called_once_with("SELECT * FROM `builds` LIMIT 0")

        query_mock.reset_mock()
        await self.client.select(limit=10)
        query_mock.assert_called_once_with("SELECT * FROM `builds` LIMIT 10")

        query_mock.reset_mock()
        with self.assertRaises(AssertionError):
            await self.client.select(limit=-1)

        query_mock.reset_mock()
        with self.assertRaises(AssertionError):
            await self.client.select(limit="1")
