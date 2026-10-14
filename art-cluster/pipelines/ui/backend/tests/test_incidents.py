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

    async def test_create_prunes_the_oldest_resolved_item_at_history_limit(self):
        resolved = {
            **INCIDENT,
            "id": "old-resolved",
            "status": "resolved",
            "resolvedAt": "2026-01-01T00:00:00Z",
            "updated": "2026-01-01T00:00:00Z",
        }
        current = [{**INCIDENT, "id": f"active-{index}"} for index in range(19)] + [resolved]
        self.allow_admin()
        self.gateway.kube_json.side_effect = None
        self.gateway.kube_json = AsyncMock(side_effect=[{"metadata": {"resourceVersion": "12"}, "data": {"incidents.json": json.dumps({"incidents": current})}}, {"metadata": {"resourceVersion": "13"}}])
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "New issue", "message": "Scans may fail"},
            )

        self.assertEqual(response.status_code, 201)
        incidents = response.json()["incidents"]
        self.assertEqual(len(incidents), 20)
        self.assertNotIn("old-resolved", {incident["id"] for incident in incidents})
        self.assertEqual(incidents[-1]["title"], "New issue")

    async def test_create_rejects_history_at_limit_when_every_incident_is_active(self):
        current = [{**INCIDENT, "id": f"active-{index}"} for index in range(20)]
        self.allow_admin()
        calls = []

        async def read_only_kube_json(method, _path, **_kwargs):
            calls.append(method)
            return {"metadata": {"resourceVersion": "12"}, "data": {"incidents.json": json.dumps({"incidents": current})}}

        self.gateway.kube_json = AsyncMock(side_effect=read_only_kube_json)
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "New issue", "message": "Scans may fail"},
            )

        self.assertEqual(response.status_code, 409)
        self.assertIn("Resolve an incident", response.json()["detail"])
        self.assertEqual(calls, ["GET"])

    async def test_create_rejects_data_that_exceeds_configmap_size_budget(self):
        long_url = "https://example.com/" + "a" * 10000
        current = [
            {**INCIDENT, "id": f"large-{index}", "links": [{"label": "Ticket", "url": long_url}] * 10}
            for index in range(10)
        ]
        calls = []

        async def read_only_kube_json(method, _path, **_kwargs):
            calls.append(method)
            return {"metadata": {"resourceVersion": "12"}, "data": {"incidents.json": json.dumps({"incidents": current})}}

        self.gateway.kube_json = AsyncMock(side_effect=read_only_kube_json)
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "New issue", "message": "Scans may fail"},
            )

        self.assertEqual(response.status_code, 413)
        self.assertIn("history is too large", response.json()["detail"])
        self.assertEqual(calls, ["GET"])

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

    async def test_stale_configmap_write_returns_conflict(self):
        async def kube_json(method, _path, **_kwargs):
            if method == "GET":
                return CONFIGMAP
            raise UpstreamError(409, "Incident data changed; refresh and try again")

        self.gateway.kube_json = AsyncMock(side_effect=kube_json)
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "New issue", "message": "Scans may fail"},
            )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json(), {"detail": "Incident data changed; refresh and try again"})

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
