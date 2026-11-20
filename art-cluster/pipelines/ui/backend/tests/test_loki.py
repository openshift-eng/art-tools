import io
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import httpx
from pipeline_ui import app as api
from pipeline_ui import loki
from pipeline_ui.cluster import UpstreamError

UID = UUID("3d0fdc60-326b-482c-b3a9-ca653c99a5be")
NAMESPACE = "art-test-tenant"
NAME = "pipeline-test"
RUN = {
    "metadata": {"name": NAME, "uid": str(UID)},
    "status": {"startTime": "2026-11-19T20:00:00Z", "completionTime": "2026-11-19T20:20:00Z"},
}


def loki_response(streams):
    return httpx.Response(200, json={"status": "success", "data": {"resultType": "streams", "result": streams}})


class LokiTests(unittest.IsolatedAsyncioTestCase):
    async def test_export_pages_entire_run_and_revisits_ties(self):
        start, end = loki.run_window(RUN)
        rows = [
            (start + index // 3, index, json.dumps({"message": f"line {index:05d}", "stream": "stdout"}))
            for index in range(11244)
        ]
        boundaries = []

        def handle(request):
            self.assertEqual(request.url.path, "/api/logs/v1/application/loki/api/v1/query_range")
            self.assertEqual(request.headers["Authorization"], "Bearer viewer")
            params = request.url.params
            self.assertIn(f'kubernetes_namespace_name="{NAMESPACE}"', params["query"])
            self.assertIn(f'run_uid="{UID}"', params["query"])
            self.assertEqual(params["direction"], "forward")
            self.assertEqual(int(params["end"]), end)
            boundary = int(params["start"])
            boundaries.append(boundary)
            page = [entry for entry in rows if entry[0] >= boundary][: int(params["limit"])]
            return loki_response(
                [
                    {
                        "stream": {"kubernetes_container_name": "step-scan"},
                        "values": [[str(ts), line] for ts, _, line in page],
                    }
                ]
            )

        output = io.BytesIO()
        client_class = httpx.AsyncClient
        with patch.object(
            loki.httpx,
            "AsyncClient",
            side_effect=lambda **kwargs: client_class(transport=httpx.MockTransport(handle), **kwargs),
        ):
            await loki.write_console("viewer", NAMESPACE, UID, RUN, output)

        lines = output.getvalue().decode().splitlines()
        self.assertEqual([line.split(": ")[-1] for line in lines], [f"line {index:05d}" for index in range(11244)])
        self.assertTrue(all("step-scan stdout:" in line for line in lines))
        self.assertEqual(boundaries[1], rows[loki.PAGE_LIMIT - 1][0])
        self.assertEqual(boundaries, sorted(set(boundaries)))

    async def test_full_page_at_one_timestamp_is_reported_as_incomplete(self):
        start, _ = loki.run_window(RUN)

        def handle(_request):
            return loki_response([{"stream": {}, "values": [[str(start), "duplicated"] for _ in range(3)]}])

        output = io.BytesIO()
        client = httpx.AsyncClient(base_url=loki.LOKI_API, transport=httpx.MockTransport(handle))
        with patch.object(loki, "PAGE_LIMIT", 3), patch.object(loki.httpx, "AsyncClient", return_value=client):
            with self.assertRaisesRegex(UpstreamError, "Export would be incomplete"):
                await loki.write_console("viewer", NAMESPACE, UID, RUN, output)
        self.assertEqual(output.getvalue(), b"")


class FullConsoleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateway = MagicMock()
        self.gateway.__aenter__.return_value = self.gateway
        self.gateway.run = AsyncMock(side_effect=UpstreamError(404, "PipelineRun pruned"))
        self.gateway.archived_run = AsyncMock(return_value=({"name": "archive"}, RUN))
        self.enterContext(patch.object(api, "Gateway", return_value=self.gateway))
        self.enterContext(patch.object(api, "NAMESPACES", (NAMESPACE,)))
        self.write_console = self.enterContext(patch.object(loki, "write_console", new_callable=AsyncMock))
        self.path = f"/api/runs/{NAMESPACE}/{NAME}/logs/full?uid={UID}"
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api.app),
            base_url="https://ui.example",
            headers={"x-forwarded-access-token": "viewer"},
        )
        self.addAsyncCleanup(self.client.aclose)

    async def test_inline_console_uses_exact_archived_run_and_viewer(self):
        async def write_console(token, namespace, uid, run, file):
            self.assertEqual((token, namespace, uid, run), ("viewer", NAMESPACE, UID, RUN))
            file.write(b"scan-sources output\n")

        self.write_console.side_effect = write_console
        response = await self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"scan-sources output\n")
        self.assertTrue(response.headers["content-type"].startswith("text/plain"))
        self.assertNotIn("content-disposition", response.headers)
        self.gateway.archived_run.assert_awaited_once_with(NAMESPACE, NAME, str(UID))

    async def test_loki_denial_is_a_structured_error(self):
        detail = "Loki application log access is required for this namespace"
        self.write_console.side_effect = UpstreamError(403, detail)
        response = await self.client.get(self.path)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json(), {"detail": detail})

    async def test_failed_later_page_does_not_send_partial_text(self):
        async def write_console(_token, _namespace, _uid, _run, file):
            file.write(b"first page\n")
            raise UpstreamError(502, "Loki log query failed")

        self.write_console.side_effect = write_console
        response = await self.client.get(self.path)
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json(), {"detail": "Loki log query failed"})
