from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.release_readiness.checks.bundle_fbc import check_bundle_fbc_coverage
from pyartcd.pipelines.release_readiness.models import Status


class TestBundleFbcCoverageCheck(IsolatedAsyncioTestCase):
    async def test_all_covered(self):
        mock_op_a = MagicMock(name="op-a", nvr="op-a-1.0-1")
        mock_op_a.name = "op-a"
        mock_op_b = MagicMock(name="op-b", nvr="op-b-1.0-1")
        mock_op_b.name = "op-b"

        mock_bundle_a = MagicMock(nvr="op-a-bundle-1.0-1")
        mock_bundle_b = MagicMock(nvr="op-b-bundle-1.0-1")

        with (
            patch(
                "pyartcd.pipelines.release_readiness.checks.bundle_fbc._load_operator_names",
                return_value=["op-a", "op-b"],
            ),
            patch("pyartcd.pipelines.release_readiness.checks.bundle_fbc.KonfluxDb") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db
            mock_db.bind = MagicMock()
            mock_db.get_latest_build = AsyncMock(
                side_effect=[
                    mock_op_a,
                    mock_op_b,
                    mock_bundle_a,
                    mock_bundle_b,
                ]
            )

            async def mock_fbc_search(**kwargs):
                yield MagicMock(nvr="fbc-1.0-1")

            mock_db.search_builds_by_fields = mock_fbc_search

            result = await check_bundle_fbc_coverage("openshift-4.21", "konflux", "/tmp/working")

        self.assertEqual(result.status, Status.GREEN)
        self.assertIn("2/2", result.summary)

    async def test_missing_bundles(self):
        mock_op_a = MagicMock(name="op-a", nvr="op-a-1.0-1")
        mock_op_a.name = "op-a"
        mock_op_b = MagicMock(name="op-b", nvr="op-b-1.0-1")
        mock_op_b.name = "op-b"

        with (
            patch(
                "pyartcd.pipelines.release_readiness.checks.bundle_fbc._load_operator_names",
                return_value=["op-a", "op-b"],
            ),
            patch("pyartcd.pipelines.release_readiness.checks.bundle_fbc.KonfluxDb") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db
            mock_db.bind = MagicMock()
            mock_db.get_latest_build = AsyncMock(
                side_effect=[
                    mock_op_a,
                    mock_op_b,
                    None,
                    None,
                ]
            )

            result = await check_bundle_fbc_coverage("openshift-4.21", "konflux", "/tmp/working")

        self.assertEqual(result.status, Status.RED)
        self.assertIn("0/2", result.summary)

    async def test_no_operators(self):
        with patch("pyartcd.pipelines.release_readiness.checks.bundle_fbc._load_operator_names", return_value=[]):
            result = await check_bundle_fbc_coverage("openshift-4.21", "konflux", "/tmp/working")

        self.assertEqual(result.status, Status.GREEN)
        self.assertIn("No OLM operators", result.summary)
