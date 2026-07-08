import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from pyartcd.pipelines.release_readiness.checks.build_failures import check_build_failures
from pyartcd.pipelines.release_readiness.models import Status


class TestBuildFailuresCheck(IsolatedAsyncioTestCase):
    async def test_no_failures(self):
        concerns = [{"image_name": "image-a", "code": "LATEST_BUILT_SUCCEEDED"}]

        with patch(
            "pyartcd.pipelines.release_readiness.checks.build_failures.exectools.cmd_gather_async",
            return_value=(0, json.dumps(concerns), ""),
        ):
            result = await check_build_failures("openshift-4.21", "konflux", "/tmp/working")

        self.assertEqual(result.status, Status.GREEN)

    async def test_with_failures(self):
        concerns = [
            {"image_name": "image-a", "code": "FAILING_AT_LEAST_FOR"},
            {"image_name": "image-b", "code": "LATEST_ATTEMPT_FAILED"},
            {"image_name": "image-c", "code": "LATEST_BUILT_SUCCEEDED"},
        ]

        with patch(
            "pyartcd.pipelines.release_readiness.checks.build_failures.exectools.cmd_gather_async",
            return_value=(0, json.dumps(concerns), ""),
        ):
            result = await check_build_failures("openshift-4.21", "konflux", "/tmp/working")

        self.assertEqual(result.status, Status.GREEN)
        self.assertIn("2", result.summary)
        self.assertIn("failing", result.summary)
