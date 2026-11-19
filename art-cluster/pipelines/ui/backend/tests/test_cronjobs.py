import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from pipeline_ui import app as api
from pipeline_ui.cluster import UpstreamError

NAMESPACE = "art-openshift-tenant"
OTHER_NAMESPACE = "art-other-tenant"
NAME = "ocp4-scan-konflux-hp"


class CronJobTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cronjob = {
            "metadata": {"namespace": NAMESPACE, "name": NAME, "uid": "cron-uid"},
            "spec": {"schedule": "26,56 * * * *", "timeZone": "Etc/UTC"},
            "status": {"active": [], "lastScheduleTime": "2026-11-19T15:56:00Z"},
        }
        self.gateway = MagicMock()
        self.gateway.__aenter__.return_value = self.gateway
        self.gateway.cronjob = AsyncMock(return_value=self.cronjob)
        self.gateway_class = self.enterContext(patch.object(api, "Gateway", return_value=self.gateway))
        self.enterContext(patch.object(api, "NAMESPACES", (NAMESPACE, OTHER_NAMESPACE)))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api.app),
            base_url="https://ui.example",
            headers={"x-forwarded-access-token": "test-token"},
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_all_tenants_lists_visible_cronjobs_and_preserves_namespace_denial(self):
        async def list_kube(namespace, resource, *, api_group):
            self.assertEqual((resource, api_group), ("cronjobs", "batch/v1"))
            if namespace == OTHER_NAMESPACE:
                raise UpstreamError(403, "Forbidden")
            yield self.cronjob

        self.gateway.list_kube.side_effect = list_kube

        response = await self.client.get("/api/cronjobs")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.json()["items"], [api.cronjob_summary(self.cronjob)])
        self.gateway_class.assert_called_once_with("test-token")

        denied = await self.client.get("/api/cronjobs", params={"namespace": OTHER_NAMESPACE})
        self.assertEqual(denied.status_code, 403)

    async def test_detail_matches_current_owner_uid_and_exposes_job_summary_only(self):
        def job(name, created, uid, status):
            return {
                "metadata": {
                    "name": name,
                    "creationTimestamp": created,
                    "ownerReferences": [{"kind": "CronJob", "name": NAME, "uid": uid}],
                },
                "spec": {"template": {"spec": {"containers": [{"env": [{"name": "SECRET", "value": "test-value"}]}]}}},
                "status": status,
            }

        async def list_jobs(_namespace, _resource, *, api_group):
            yield job(
                "older-success",
                "2026-11-19T14:26:00Z",
                "cron-uid",
                {
                    "conditions": [{"type": "Complete", "status": "True"}],
                    "completionTime": "2026-11-19T14:27:00Z",
                },
            )
            yield job("unrelated-old-owner", "2026-11-19T14:56:00Z", "old-uid", {})
            yield job(
                "newer-failure",
                "2026-11-19T15:26:00Z",
                "cron-uid",
                {
                    "conditions": [{"type": "Failed", "status": "True", "lastTransitionTime": "2026-11-19T15:27:00Z"}],
                },
            )

        self.gateway.list_kube.side_effect = list_jobs
        response = await self.client.get(f"/api/cronjobs/{NAMESPACE}/{NAME}")

        self.assertEqual(response.status_code, 200)
        self.gateway.cronjob.assert_awaited_once_with(NAMESPACE, NAME)
        self.gateway.list_kube.assert_called_once_with(NAMESPACE, "jobs", api_group="batch/v1")
        jobs = response.json()["jobs"]
        self.assertEqual(
            [(j["name"], j["status"]) for j in jobs],
            [
                ("newer-failure", "Failed"),
                ("older-success", "Succeeded"),
            ],
        )
        self.assertEqual(jobs[0]["completed"], "2026-11-19T15:27:00Z")
        self.assertNotIn("test-value", response.text)

    async def test_job_history_denial_preserves_cronjob_and_auth_failure_propagates(self):
        self.gateway.list_kube.side_effect = UpstreamError(403, "Forbidden")
        path = f"/api/cronjobs/{NAMESPACE}/{NAME}"
        response = await self.client.get(path)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["schedule"], "26,56 * * * *")
        self.assertIsNone(response.json()["jobs"])

        self.gateway.list_kube.side_effect = UpstreamError(401, "Unauthorized")
        self.assertEqual((await self.client.get(path)).status_code, 401)

    async def test_unconfigured_namespace_and_missing_token_never_reach_cluster(self):
        response = await self.client.get(f"/api/cronjobs/unconfigured/{NAME}")
        self.assertEqual(response.status_code, 404)
        del self.client.headers["x-forwarded-access-token"]
        self.assertEqual((await self.client.get("/api/cronjobs")).status_code, 401)
        self.gateway_class.assert_not_called()
