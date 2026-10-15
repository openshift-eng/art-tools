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
        incident = response.json()["incidents"][0]
        self.assertEqual(incident["id"], "kerberos-1")
        self.assertTrue(incident["showOnBanner"])
        self.assertEqual(incident["history"], [{"timestamp": INCIDENT["started"], "note": "Incident created."}])

    async def test_create_records_start_and_update_times(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "New issue", "message": "Scans may fail", "links": INCIDENT["links"]},
            )

        self.assertEqual(response.status_code, 201)
        created = response.json()["incidents"][-1]
        self.assertTrue(created["id"])
        self.assertEqual(created["started"], created["updated"])
        self.assertTrue(created["showOnBanner"])
        self.assertEqual(created["history"][0]["note"], "Incident created.")
        self.assertEqual(created["history"][0]["timestamp"], created["started"])
        datetime.fromisoformat(created["started"].replace("Z", "+00:00"))
        self.assertEqual(created["links"][0]["url"], "https://issues.example/ART-1")
        patch_call = next(call for call in self.gateway.kube_json.await_args_list if call.args[0] == "PATCH")
        self.assertEqual(patch_call.kwargs["json"]["metadata"]["resourceVersion"], "12")

    async def test_create_can_keep_an_incident_off_the_banner(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "Internal incident record", "message": "Recorded for history", "showOnBanner": False},
            )

        self.assertEqual(response.status_code, 201)
        self.assertFalse(response.json()["incidents"][-1]["showOnBanner"])

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
        self.gateway.kube_json = AsyncMock(
            side_effect=[
                {"metadata": {"resourceVersion": "12"}, "data": {"incidents.json": json.dumps({"incidents": current})}},
                {"metadata": {"resourceVersion": "13"}},
            ]
        )
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

    async def test_create_prunes_soft_deleted_items_before_resolved_history(self):
        deleted = {
            **INCIDENT,
            "id": "soft-deleted",
            "status": "deleted",
            "deletedAt": "2026-01-01T00:00:00Z",
            "updated": "2026-01-01T00:00:00Z",
        }
        current = [{**INCIDENT, "id": f"active-{index}"} for index in range(19)] + [deleted]
        self.gateway.kube_json = AsyncMock(
            side_effect=[
                {"metadata": {"resourceVersion": "12"}, "data": {"incidents.json": json.dumps({"incidents": current})}},
                {"metadata": {"resourceVersion": "13"}},
            ]
        )
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post(
                "/api/incidents",
                headers={"x-csrf-token": "csrf-token"},
                json={"title": "New issue", "message": "Scans may fail"},
            )

        self.assertEqual(response.status_code, 201)
        self.assertNotIn("soft-deleted", {incident["id"] for incident in response.json()["incidents"]})

    async def test_delete_soft_deletes_and_keeps_incident_in_history(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.delete("/api/incidents/kerberos-1", headers={"x-csrf-token": "csrf-token"})

        self.assertEqual(response.status_code, 200)
        deleted = response.json()["incidents"][0]
        self.assertEqual(deleted["status"], "deleted")
        self.assertTrue(deleted["deletedAt"])
        self.assertEqual(deleted["id"], INCIDENT["id"])
        self.assertEqual(deleted["history"][-1]["note"], "Incident marked deleted.")
        self.assertEqual(deleted["history"][-1]["timestamp"], deleted["deletedAt"])

    async def test_create_rejects_history_at_limit_when_every_incident_is_active(self):
        current = [{**INCIDENT, "id": f"active-{index}"} for index in range(20)]
        self.allow_admin()
        calls = []

        async def read_only_kube_json(method, _path, **_kwargs):
            calls.append(method)
            return {
                "metadata": {"resourceVersion": "12"},
                "data": {"incidents.json": json.dumps({"incidents": current})},
            }

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
            return {
                "metadata": {"resourceVersion": "12"},
                "data": {"incidents.json": json.dumps({"incidents": current})},
            }

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
                json={
                    "title": "Updated issue",
                    "message": "Still intermittent",
                    "status": "identified",
                    "showOnBanner": False,
                    "updateNote": "  Kerberos renewal is still intermittent.  ",
                    "history": [{"timestamp": "caller-supplied", "note": "Not an accepted event"}],
                },
            )

        self.assertEqual(response.status_code, 200)
        updated = response.json()["incidents"][0]
        self.assertEqual(updated["title"], "Updated issue")
        self.assertEqual(updated["status"], "identified")
        self.assertIsNone(updated["resolvedAt"])
        self.assertNotEqual(updated["updated"], INCIDENT["updated"])
        self.assertEqual(updated["history"][-1]["note"], "Kerberos renewal is still intermittent.")
        self.assertEqual(updated["history"][-1]["timestamp"], updated["updated"])
        self.assertEqual(len(updated["history"]), 2)
        self.assertFalse(updated["showOnBanner"])

    async def test_update_preserves_banner_opt_out_when_field_is_omitted(self):
        configmap = {
            "metadata": {"resourceVersion": "12"},
            "data": {"incidents.json": json.dumps({"incidents": [{**INCIDENT, "showOnBanner": False}]})},
        }
        self.gateway.kube_json = AsyncMock(side_effect=[configmap, {"metadata": {"resourceVersion": "13"}}])
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.put(
                "/api/incidents/kerberos-1",
                headers={"x-csrf-token": "csrf-token"},
                json={
                    "title": INCIDENT["title"],
                    "message": INCIDENT["message"],
                    "updateNote": "Legacy editor updated this incident.",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["incidents"][0]["showOnBanner"])

    async def test_update_trims_old_history_entries_before_configmap_size_budget_check(self):
        current_incident = {
            **INCIDENT,
            "history": [
                {"timestamp": INCIDENT["started"], "note": f"Entry {index:04d}: {'x' * 488}"} for index in range(1560)
            ],
            "description": "x" * 2156,
        }
        configmap = {
            "metadata": {"resourceVersion": "12"},
            "data": {"incidents.json": json.dumps({"incidents": [current_incident]})},
        }
        calls = []

        async def kube_json(method, _path, **_kwargs):
            calls.append(method)
            if method == "GET":
                return configmap
            return {"metadata": {"resourceVersion": "13"}}

        self.gateway.kube_json = AsyncMock(side_effect=kube_json)
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.put(
                "/api/incidents/kerberos-1",
                headers={"x-csrf-token": "csrf-token"},
                json={
                    "title": INCIDENT["title"],
                    "message": INCIDENT["message"],
                    "description": "x" * 2156,
                    "links": INCIDENT["links"],
                    "updateNote": "New update",
                },
            )

        self.assertEqual(response.status_code, 200)
        history = response.json()["incidents"][0]["history"]
        self.assertEqual(len(history), api.MAX_INCIDENT_HISTORY_ENTRIES)
        first_retained = 1560 - api.MAX_INCIDENT_HISTORY_ENTRIES + 1
        self.assertTrue(history[0]["note"].startswith(f"Entry {first_retained:04d}:"))
        self.assertEqual(history[-1]["note"], "New update")
        self.assertEqual(calls, ["GET", "PATCH"])

    async def test_close_records_resolved_status_and_close_time(self):
        self.allow_admin()
        with patch.object(api.Path, "read_text", return_value='{"incidents":[]}'):
            response = await self.client.post("/api/incidents/kerberos-1/close", headers={"x-csrf-token": "csrf-token"})

        self.assertEqual(response.status_code, 200)
        closed = response.json()["incidents"][0]
        self.assertEqual(closed["status"], "resolved")
        self.assertEqual(closed["updated"], closed["resolvedAt"])
        datetime.fromisoformat(closed["resolvedAt"].replace("Z", "+00:00"))
        self.assertEqual(closed["history"][-1]["note"], "Incident closed.")
        self.assertEqual(closed["history"][-1]["timestamp"], closed["resolvedAt"])

    async def test_update_requires_a_nonblank_note_of_at_most_500_characters(self):
        for note in ("   ", "x" * 501):
            with self.subTest(note_length=len(note)):
                response = await self.client.put(
                    "/api/incidents/kerberos-1",
                    headers={"x-csrf-token": "csrf-token"},
                    json={"title": "Updated issue", "message": "Still intermittent", "updateNote": note},
                )
                self.assertEqual(response.status_code, 422)
        self.gateway_class.assert_not_called()

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
