import json
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from pipeline_ui import app as api
from pipeline_ui.cluster import UpstreamError


INCIDENT = {
    "id": "kerberos-1",
    "title": "Kerberos authentication is intermittent",
    "message": "Layered product scans may fail intermittently.",
    "severity": "warning",
    "status": "investigating",
    "started": "2026-10-14T08:00:00Z",
    "updated": "2026-10-14T08:00:00Z",
    "description": "Token renewal is failing intermittently.",
    "links": [{"label": "Ticket", "url": "https://issues.example/ART-1"}],
}
CONFIGMAP = {
    "metadata": {"resourceVersion": "12"},
    "data": {"incidents.json": json.dumps({"incidents": [INCIDENT]})},
}


class IncidentApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateway = MagicMock()
        self.gateway.__aenter__.return_value = self.gateway
        self.gateway.kube_json = AsyncMock()
        self.gateway_class = self.enterContext(patch.object(api, "Gateway", return_value=self.gateway))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api.app),
            base_url="https://ui.example",
            headers={"x-forwarded-access-token": "test-token"},
        )
        self.client.cookies.set(api.CSRF_COOKIE, "csrf-token")

    async def asyncTearDown(self):
        await self.client.aclose()

    def allow_admin(self):
        async def kube_json(method, path, **kwargs):
            if path.endswith("selfsubjectaccessreviews"):
                return {"status": {"allowed": True}}
            if method == "GET":
                return CONFIGMAP
            if method == "PATCH":
                return {"metadata": {"resourceVersion": "13"}}
            raise AssertionError(f"Unexpected Kubernetes request: {method} {path}")

        self.gateway.kube_json.side_effect = kube_json

    async def test_admin_feed_uses_live_configmap_and_reports_management_access(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.get("/api/incidents")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["canManage"])
        self.assertEqual(response.json()["incidents"][0]["id"], "kerberos-1")
        self.assertEqual(response.headers["cache-control"], "no-store")

    async def test_missing_mounted_feed_still_checks_admin_access(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", side_effect=FileNotFoundError):
            response = await self.client.get("/api/incidents")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["canManage"])
        self.assertEqual(response.json()["incidents"][0]["id"], "kerberos-1")

    async def test_reader_feed_does_not_expose_management_access(self):
        async def kube_json(method, path, **_kwargs):
            if path.endswith("selfsubjectaccessreviews"):
                return {"status": {"allowed": False}}
            raise AssertionError(f"Reader should not read ConfigMap: {method} {path}")

        self.gateway.kube_json.side_effect = kube_json
        with patch.object(api.Path, "read_text", return_value=json.dumps({"incidents": [INCIDENT]})):
            response = await self.client.get("/api/incidents")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["canManage"])
        self.assertEqual(response.json()["incidents"][0]["id"], "kerberos-1")

    async def test_create_records_start_and_update_times(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "New issue", "message": "Scans may fail", "links": INCIDENT["links"]},
            )

        self.assertEqual(response.status_code, 201)
        created = response.json()["incidents"][0]
        self.assertTrue(created["id"])
        self.assertEqual(created["started"], created["updated"])
        datetime.fromisoformat(created["started"].replace("Z", "+00:00"))
        self.assertEqual(created["links"][0]["url"], "https://issues.example/ART-1")
        patch_call = next(call for call in self.gateway.kube_json.await_args_list if call.args[0] == "PATCH")
        self.assertEqual(patch_call.kwargs["json"]["metadata"]["resourceVersion"], "12")

    async def test_update_records_new_update_time_without_resolving(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.put(
                "/api/incidents/kerberos-1",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "Updated issue", "message": "Still intermittent", "status": "identified"},
            )

        self.assertEqual(response.status_code, 200)
        updated = response.json()["incidents"][0]
        self.assertEqual(updated["title"], "Updated issue")
        self.assertEqual(updated["status"], "identified")
        self.assertIsNone(updated["resolvedAt"])
        self.assertNotEqual(updated["updated"], INCIDENT["updated"])

    async def test_close_records_resolved_status_and_close_time(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents/kerberos-1/close", headers={"x-csrf-token": "csrf-token"}
            )

        self.assertEqual(response.status_code, 200)
        closed = response.json()["incidents"][0]
        self.assertEqual(closed["status"], "resolved")
        self.assertEqual(closed["updated"], closed["resolvedAt"])
        datetime.fromisoformat(closed["resolvedAt"].replace("Z", "+00:00"))

    async def test_mutation_requires_csrf_token(self):
        response = await self.client.post("/api/incidents", json={"title": "Issue", "message": "Details"})

        self.assertEqual(response.status_code, 403)
        self.gateway_class.assert_not_called()

    async def test_configmap_permission_denial_rejects_mutation(self):
        self.gateway.kube_json.side_effect = UpstreamError(403, "Forbidden")
        response = await self.client.post(
            "/api/incidents",
            headers={"x-csrf-token": "csrf-token"},
            json={"title": "Issue", "message": "Details"},
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json(), {"detail": "Forbidden"})

    async def test_invalid_incident_input_is_rejected(self):
        response = await self.client.post(
            "/api/incidents",
            headers={"x-csrf-token": "csrf-token"},
            json={"title": "Issue", "message": "Details", "links": [{"label": "Ticket", "url": "javascript:alert(1)"}]},
        )

        self.assertEqual(response.status_code, 422)
        self.gateway_class.assert_not_called()


if __name__ == "__main__":
    unittest.main()
