import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from pipeline_ui import app as api

NAMESPACE = "art-test-tenant"


class RunDetailTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.run = {
            "metadata": {
                "namespace": NAMESPACE,
                "name": "rebuilt-run",
                "uid": "rebuilt-uid",
                "annotations": {
                    "art.openshift.io/rebuilt-from": "source-uid",
                    "art.openshift.io/rebuilt-from-name": "source-run",
                },
            },
            "spec": {"pipelineRef": {"name": "example"}},
            "status": {},
        }
        self.gateway = MagicMock()
        self.gateway.__aenter__.return_value = self.gateway
        self.gateway.run = AsyncMock(return_value=self.run)
        self.gateway_class = self.enterContext(patch.object(api, "Gateway", return_value=self.gateway))
        self.enterContext(patch.object(api, "NAMESPACES", (NAMESPACE,)))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api.app),
            base_url="https://ui.example",
            headers={"x-forwarded-access-token": "test-token"},
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_run_detail_exposes_rebuild_source_name_and_uid(self):
        response = await self.client.get(f"/api/runs/{NAMESPACE}/rebuilt-run")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["rebuiltFrom"], {"name": "source-run", "uid": "source-uid"})

    async def test_uid_only_older_rebuild_has_no_source_link_target(self):
        self.run["metadata"]["annotations"] = {"art.openshift.io/rebuilt-from": "source-uid"}

        response = await self.client.get(f"/api/runs/{NAMESPACE}/rebuilt-run")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["rebuiltFrom"])


if __name__ == "__main__":
    unittest.main()
