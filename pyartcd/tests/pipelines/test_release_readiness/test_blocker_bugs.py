import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from pyartcd.pipelines.release_readiness.checks.blocker_bugs import check_blocker_bugs
from pyartcd.pipelines.release_readiness.models import Status


class TestBlockerBugsCheck(IsolatedAsyncioTestCase):
    async def test_no_blockers(self):
        with patch(
            "pyartcd.pipelines.release_readiness.checks.blocker_bugs.exectools.cmd_gather_async",
            return_value=(0, "[]", ""),
        ):
            result = await check_blocker_bugs("openshift-4.21", "/tmp/working")

        self.assertEqual(result.status, Status.GREEN)

    async def test_blockers_found(self):
        bugs_json = json.dumps(
            [
                {"id": "OCPBUGS-123", "component": "Networking", "status": "NEW", "summary": "Bug", "url": "..."},
                {"id": "OCPBUGS-456", "component": "Storage", "status": "ASSIGNED", "summary": "Bug2", "url": "..."},
            ]
        )

        with patch(
            "pyartcd.pipelines.release_readiness.checks.blocker_bugs.exectools.cmd_gather_async",
            return_value=(0, bugs_json, ""),
        ):
            result = await check_blocker_bugs("openshift-4.21", "/tmp/working")

        self.assertEqual(result.status, Status.RED)
        self.assertIn("2", result.summary)

    async def test_blocker_check_error(self):
        with patch(
            "pyartcd.pipelines.release_readiness.checks.blocker_bugs.exectools.cmd_gather_async",
            side_effect=Exception("kerberos expired"),
        ):
            result = await check_blocker_bugs("openshift-4.21", "/tmp/working")

        self.assertEqual(result.status, Status.YELLOW)
