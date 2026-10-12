import base64
import importlib.util
import json
import os
import unittest
from unittest.mock import patch

import httpx
from pipeline_ui import cluster


def record(index):
    run = {"metadata": {"name": f"run-{index}", "uid": f"uid-{index}"}}
    return {"name": f"record-{index}", "data": {"value": base64.b64encode(json.dumps(run).encode()).decode()}}


class ResultsPaginationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(patch.object(cluster, "KUBE_CA", True))
        self.enterContext(patch.object(cluster, "RESULTS_CA", True))
        self.enterContext(patch.object(cluster, "RESULTS_PAGE_SIZE", 1000))
        self.gateway = cluster.Gateway("test-token")
        await self.gateway.results.aclose()
        self.requests = []
        self.pages = {}
        self.gateway.results = httpx.AsyncClient(
            base_url=cluster.RESULTS_API, transport=httpx.MockTransport(self.respond)
        )

    async def asyncTearDown(self):
        await self.gateway.__aexit__()

    def respond(self, request):
        self.requests.append(request)
        return httpx.Response(200, json=self.pages[request.url.params.get("page_token")])

    async def test_entire_history_across_large_pages_and_both_token_formats(self):
        records = [record(index) for index in range(2001)]
        self.pages = {
            None: {"records": records[:1000], "next_page_token": "page-2"},
            "page-2": {"records": records[1000:2000], "nextPageToken": "page-3"},
            "page-3": {"records": records[2000:]},
        }
        query = "data_type == PIPELINE_RUN && data.metadata.labels['tekton.dev/pipeline'] == 'build-fbc'"
        runs = [run async for _record, run in self.gateway.list_records("art-test-tenant", filter_text=query)]

        self.assertEqual([run["metadata"]["uid"] for run in runs], [f"uid-{index}" for index in range(2001)])
        self.assertEqual(
            [request.url.params.get("page_token") for request in self.requests], [None, "page-2", "page-3"]
        )
        for request in self.requests:
            self.assertEqual(request.url.path, f"{cluster.RESULTS_PREFIX}/art-test-tenant/results/-/records")
            self.assertEqual(request.url.params["page_size"], "1000")
            self.assertEqual(request.url.params["filter"], query)
            self.assertEqual(request.url.params["order_by"], "create_time desc")

    async def test_empty_page_with_continuation_still_loads_later_records(self):
        self.pages = {
            None: {"records": [], "next_page_token": "page-2"},
            "page-2": {"records": [record(1)]},
        }
        runs = [
            run
            async for _record, run in self.gateway.list_records(
                "art-test-tenant", filter_text="data_type == PIPELINE_RUN"
            )
        ]

        self.assertEqual([run["metadata"]["uid"] for run in runs], ["uid-1"])
        self.assertEqual(len(self.requests), 2)

    async def test_malformed_record_does_not_hide_valid_later_history(self):
        self.pages = {
            None: {"records": [{"data": {"value": "bm90LWpzb24="}}], "next_page_token": "page-2"},
            "page-2": {"records": [record(1)]},
        }
        runs = [
            run
            async for _record, run in self.gateway.list_records(
                "art-test-tenant", filter_text="data_type == PIPELINE_RUN"
            )
        ]

        self.assertEqual([run["metadata"]["uid"] for run in runs], ["uid-1"])

    async def test_each_load_reads_current_records(self):
        self.pages = {None: {"records": [record(1)]}}
        first = [
            run
            async for _record, run in self.gateway.list_records(
                "art-test-tenant", filter_text="data_type == PIPELINE_RUN"
            )
        ]
        self.pages = {None: {"records": [record(2)]}}
        second = [
            run
            async for _record, run in self.gateway.list_records(
                "art-test-tenant", filter_text="data_type == PIPELINE_RUN"
            )
        ]

        self.assertEqual(first[0]["metadata"]["uid"], "uid-1")
        self.assertEqual(second[0]["metadata"]["uid"], "uid-2")
        self.assertEqual(len(self.requests), 2)


class ResultsPageSizeTests(unittest.TestCase):
    def settings(self, value=None):
        spec = importlib.util.spec_from_file_location("cluster_settings", cluster.__file__)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {}, clear=True):
            if value is not None:
                os.environ["RESULTS_PAGE_SIZE"] = value
            spec.loader.exec_module(module)
        return module

    def test_default_is_1000(self):
        self.assertEqual(self.settings().RESULTS_PAGE_SIZE, 1000)

    def test_accepts_configured_page_sizes_and_boundaries(self):
        for value in ("1", "200", "1000", "10000"):
            with self.subTest(value=value):
                self.assertEqual(self.settings(value).RESULTS_PAGE_SIZE, int(value))

    def test_invalid_configuration_fails_at_startup(self):
        for value in ("0", "-1", "10001", "many", "1.5", ""):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "RESULTS_PAGE_SIZE must be an integer between 1 and 10000"):
                    self.settings(value)


if __name__ == "__main__":
    unittest.main()
