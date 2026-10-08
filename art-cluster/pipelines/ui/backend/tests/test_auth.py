import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from pipeline_ui import app as api
from pipeline_ui.cluster import UpstreamError

NAMESPACE = "art-test-tenant"
RUN = {
    "metadata": {"namespace": NAMESPACE, "name": "run-1", "uid": "run-uid"},
    "spec": {"pipelineRef": {"name": "test"}},
    "status": {},
}


async def items(values=(), error=None):
    if error:
        raise error
    for value in values:
        yield value


async def stream_text(response):
    return "".join([chunk async for chunk in response.body_iterator])


class AuthenticationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateway = MagicMock()
        self.gateway.__aenter__.return_value = self.gateway
        self.gateway.kube_json = AsyncMock(return_value={"metadata": {"name": "alice"}})
        self.gateway.run = AsyncMock(return_value=RUN)
        self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items()
        self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items()
        self.gateway_class = self.enterContext(patch.object(api, "Gateway", return_value=self.gateway))
        self.enterContext(patch.object(api, "NAMESPACES", (NAMESPACE,)))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api.app),
            base_url="https://ui.example",
            headers={"x-forwarded-access-token": "test-token", "x-forwarded-user": "unverified-user"},
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_session_validates_token_and_returns_cluster_identity(self):
        response = await self.client.get("/api/session")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["user"], "alice")
        self.assertTrue(response.json()["csrfToken"])
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertIn(api.CSRF_COOKIE, response.cookies)
        self.gateway_class.assert_called_once_with("test-token")
        self.gateway.kube_json.assert_awaited_once_with("GET", "/apis/user.openshift.io/v1/users/~")
        self.gateway.__aexit__.assert_awaited_once()

    async def test_session_reuses_csrf_cookie_after_validation(self):
        self.client.cookies.set(api.CSRF_COOKIE, "existing-csrf")
        response = await self.client.get("/api/session")
        self.assertEqual(response.json()["csrfToken"], "existing-csrf")
        self.gateway.kube_json.assert_awaited_once()

    async def test_missing_token_is_not_a_valid_session(self):
        del self.client.headers["x-forwarded-access-token"]
        response = await self.client.get("/api/session")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertNotIn("set-cookie", response.headers)
        self.gateway_class.assert_not_called()

    async def test_expired_or_forbidden_token_does_not_issue_csrf_cookie(self):
        for status in (401, 403):
            with self.subTest(status=status):
                self.gateway.kube_json.side_effect = UpstreamError(status, "Rejected")
                response = await self.client.get("/api/session")
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.json(), {"detail": "Rejected"})
                self.assertEqual(response.headers["cache-control"], "no-store")
                self.assertNotIn("set-cookie", response.headers)

    async def test_session_network_failure_is_not_an_authentication_failure(self):
        self.gateway.kube_json.side_effect = httpx.ConnectError("Unavailable")
        response = await self.client.get("/api/session")
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertNotIn("set-cookie", response.headers)

    async def test_history_and_child_lookups_propagate_authentication_failure(self):
        for path in ("/api/runs", "/api/pipelines/latest-runs", f"/api/runs/{NAMESPACE}/run-1/children"):
            for source in ("archive", "live"):
                with self.subTest(path=path, source=source):
                    self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items()
                    self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items()
                    method = self.gateway.list_records if source == "archive" else self.gateway.list_kube
                    method.side_effect = lambda *_args, **_kwargs: items(error=UpstreamError(401, "Unauthorized"))
                    response = await self.client.get(path)
                    self.assertEqual(response.status_code, 401)
                    self.assertEqual(response.json(), {"detail": "Unauthorized"})

    async def test_permission_errors_still_allow_partial_results(self):
        self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items(error=UpstreamError(403, "Forbidden"))
        self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items(error=UpstreamError(403, "Forbidden"))
        response = await self.client.get("/api/runs", params={"namespace": NAMESPACE})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["errors"]), 2)

    async def test_stream_scanner_authentication_failure_terminates_without_done(self):
        self.gateway.run.side_effect = [RUN, UpstreamError(401, "Unauthorized")]
        request = SimpleNamespace(headers=self.client.headers, is_disconnected=AsyncMock(return_value=False))
        response = await api.run_log_stream(request, NAMESPACE, "run-1")
        text = await asyncio.wait_for(stream_text(response), timeout=2)
        self.assertIn('event: failure\ndata: {"status": 401, "message": "Unauthorized"}', text)
        self.assertNotIn("event: done", text)
        self.assertEqual(self.gateway.__aexit__.await_count, 2)

    async def test_stream_step_authentication_failure_cancels_other_watchers(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()
        taskrun = {
            "metadata": {"name": "task-1", "uid": "task-uid"},
            "status": {"podName": "pod-1", "steps": [{"name": "expired"}, {"name": "waiting"}]},
        }
        self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items((taskrun,))

        async def follow(_namespace, _pod, container):
            if container == "step-expired":
                await started.wait()
                raise UpstreamError(401, "Unauthorized")
            try:
                started.set()
                await asyncio.Event().wait()
                yield "unreachable"
            finally:
                cancelled.set()

        self.gateway.follow_step_logs = follow
        request = SimpleNamespace(headers=self.client.headers, is_disconnected=AsyncMock(return_value=False))
        response = await api.run_log_stream(request, NAMESPACE, "run-1")
        text = await asyncio.wait_for(stream_text(response), timeout=2)
        self.assertIn('event: failure\ndata: {"status": 401, "message": "Unauthorized"}', text)
        self.assertNotIn("event: done", text)
        self.assertTrue(cancelled.is_set())
        self.assertEqual(self.gateway.__aexit__.await_count, 2)


if __name__ == "__main__":
    unittest.main()
