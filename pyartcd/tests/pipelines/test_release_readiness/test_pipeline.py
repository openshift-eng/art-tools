from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from pyartcd.pipelines.release_readiness.models import CheckResult, Status
from pyartcd.pipelines.release_readiness.pipeline import ReleaseReadinessPipeline


class TestReleaseReadinessPipeline(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = MagicMock()
        self.mock_runtime.working_dir = MagicMock()
        self.mock_runtime.logger = MagicMock()
        self.mock_runtime.dry_run = True

    def _make_pipeline(self, **kwargs):
        defaults = {
            "runtime": self.mock_runtime,
            "group": "openshift-4.21",
            "build_system": "konflux",
        }
        defaults.update(kwargs)
        return ReleaseReadinessPipeline(**defaults)

    async def test_run_all_green(self):
        """
        All checks green, report returned.
        """

        pipeline = self._make_pipeline()
        green_check = CheckResult(name="test", status=Status.GREEN, summary="OK ✅")

        with (
            patch("pyartcd.pipelines.release_readiness.pipeline.get_next_dev_cut_off", return_value=None),
            patch("pyartcd.pipelines.release_readiness.pipeline.check_nightly_status", return_value=green_check),
            patch("pyartcd.pipelines.release_readiness.pipeline.check_blocker_bugs", return_value=green_check),
            patch("pyartcd.pipelines.release_readiness.pipeline.check_build_failures", return_value=green_check),
            patch("pyartcd.pipelines.release_readiness.pipeline.check_build_sync", return_value=green_check),
            patch("pyartcd.pipelines.release_readiness.pipeline.check_bundle_fbc_coverage", return_value=green_check),
        ):
            report = await pipeline.run()

        self.assertEqual(report.overall_status, Status.GREEN)

    async def test_overall_status_is_worst(self):
        """
        Overall status should be the worst of all checks.
        """

        pipeline = self._make_pipeline()
        green_check = CheckResult(name="test", status=Status.GREEN, summary="OK ✅")

        with (
            patch("pyartcd.pipelines.release_readiness.pipeline.get_next_dev_cut_off", return_value=None),
            patch(
                "pyartcd.pipelines.release_readiness.pipeline.check_nightly_status",
                return_value=CheckResult(name="nightly", status=Status.GREEN, summary="OK ✅"),
            ),
            patch(
                "pyartcd.pipelines.release_readiness.pipeline.check_blocker_bugs",
                return_value=CheckResult(name="blockers", status=Status.RED, summary="2 blockers ❌"),
            ),
            patch(
                "pyartcd.pipelines.release_readiness.pipeline.check_build_failures",
                return_value=CheckResult(name="failures", status=Status.GREEN, summary="OK ✅"),
            ),
            patch(
                "pyartcd.pipelines.release_readiness.pipeline.check_build_sync",
                return_value=CheckResult(name="build_sync", status=Status.GREEN, summary="OK ✅"),
            ),
            patch("pyartcd.pipelines.release_readiness.pipeline.check_bundle_fbc_coverage", return_value=green_check),
        ):
            report = await pipeline.run()

        self.assertEqual(report.overall_status, Status.RED)
