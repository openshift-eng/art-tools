import unittest
from copy import deepcopy
from unittest.mock import MagicMock, patch

import httpx
from pipeline_ui import app as api
from pipeline_ui.cluster import UpstreamError
from pipeline_ui.health import PIPELINES, build_health, health_run


def run(name, pipeline, *, parent=None, status="Succeeded", created="2026-10-08T10:00:00Z", **values):
    return {
        "namespace": "art-acm-tenant",
        "name": name,
        "uid": f"uid-{name}",
        "pipeline": pipeline,
        "status": status,
        "message": "",
        "created": created,
        "started": created,
        "completed": None if status == "Running" else created,
        "source": "live",
        "group": "acm-2.15",
        "assembly": "stream",
        "target": "",
        "parent": parent,
        "rebuiltFrom": None,
        "triggered": [],
        "dryRun": False,
        **values,
    }


def complete_chain(prefix="", **values):
    names = [f"{prefix}{name}" for name in ("scan", "images", "bundle", "fbc-419", "fbc-420")]
    return [
        run(names[0], PIPELINES[0], **values),
        run(names[1], PIPELINES[1], parent=names[0], **values),
        run(names[2], PIPELINES[2], parent=names[1], **values),
        run(names[3], PIPELINES[3], parent=names[2], target="4.19", **values),
        run(names[4], PIPELINES[3], parent=names[2], target="4.20", **values),
    ]


class HealthTests(unittest.TestCase):
    def test_every_stage_and_fbc_target_must_succeed(self):
        runs = complete_chain()
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Succeeded")
        self.assertEqual(len(result["currentChain"]["stages"][3]["runs"]), 2)
        self.assertEqual(result["lastSuccessfulChain"]["root"]["uid"], "uid-scan")
        for status, health in (("Running", "Unknown"), ("Failed", "Failed"), ("Cancelled", "Cancelled")):
            with self.subTest(status=status):
                runs[-1]["status"] = status
                self.assertEqual(build_health(runs)[0]["health"], health)

    def test_scan_without_builds_never_establishes_green(self):
        for status in ("Succeeded", "Running"):
            result = build_health([run("scan", PIPELINES[0], status=status)])[0]
            self.assertEqual(result["health"], "Unknown")
            self.assertIsNone(result["currentChain"])

    def test_later_scan_only_does_not_hide_a_failed_build(self):
        runs = complete_chain()[:2]
        runs[1]["status"] = "Failed"
        runs.append(run("later-scan", PIPELINES[0], created="2026-10-08T12:00:00Z"))
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Failed")
        self.assertEqual(result["latestScan"]["name"], "later-scan")
        self.assertEqual(result["currentChain"]["failures"][0]["uid"], "uid-images")

    def test_new_active_build_preserves_previous_green(self):
        runs = complete_chain() + complete_chain("new-", created="2026-10-08T12:00:00Z")[:2]
        runs[-1]["status"] = "Running"
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Succeeded")
        self.assertEqual(result["currentChain"]["status"], "Running")
        self.assertEqual(result["lastCompletedChain"]["root"]["name"], "scan")

    def test_new_early_failure_replaces_previous_green(self):
        runs = complete_chain() + [run("new-scan", PIPELINES[0], status="Failed", created="2026-10-08T12:00:00Z")]
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Failed")
        self.assertEqual(result["lastSuccessfulChain"]["root"]["name"], "scan")

    def test_image_retry_recovers_chain_and_retains_failure(self):
        runs = complete_chain()
        runs[1]["status"] = "Failed"
        retry = run("retry-images", PIPELINES[1], rebuiltFrom=runs[1]["uid"], created="2026-10-08T12:00:00Z")
        runs[2]["parent"] = retry["name"]
        result = build_health(runs + [retry])[0]
        self.assertEqual(result["health"], "Succeeded")
        self.assertEqual(result["currentChain"]["stages"][1]["runs"][0]["uid"], retry["uid"])
        self.assertTrue(result["currentChain"]["recovered"])
        self.assertIn("Failed", [item["status"] for item in result["currentChain"]["attempts"]])

    def test_fbc_retry_replaces_only_its_target(self):
        runs = complete_chain()
        runs[-1]["status"] = "Failed"
        retry = run(
            "retry-fbc", PIPELINES[3], rebuiltFrom=runs[-1]["uid"], target="4.20", created="2026-10-08T12:00:00Z"
        )
        result = build_health(runs + [retry])[0]
        self.assertEqual(result["health"], "Succeeded")
        self.assertEqual(
            {item["uid"] for item in result["currentChain"]["stages"][3]["runs"]}, {"uid-fbc-419", retry["uid"]}
        )

    def test_running_retry_preserves_the_last_completed_result(self):
        for original_status in ("Succeeded", "Failed"):
            with self.subTest(original_status=original_status):
                runs = complete_chain()
                runs[-1]["status"] = original_status
                retry = run(
                    "retry-fbc",
                    PIPELINES[3],
                    status="Running",
                    rebuiltFrom=runs[-1]["uid"],
                    target="4.20",
                    created="2026-10-08T12:00:00Z",
                )
                result = build_health(runs + [retry])[0]
                self.assertEqual(result["health"], original_status)
                self.assertEqual(result["currentChain"]["status"], "Running")

    def test_prior_retry_descendant_failures_remain_in_history(self):
        runs = complete_chain()
        runs[2]["status"] = "Failed"
        retry = run("retry-images", PIPELINES[1], rebuiltFrom=runs[1]["uid"], created="2026-10-08T12:00:00Z")
        bundle = run("retry-bundle", PIPELINES[2], parent=retry["name"], created="2026-10-08T12:30:00Z")
        runs[-2]["parent"] = bundle["name"]
        runs[-1]["parent"] = bundle["name"]
        result = build_health(runs + [retry, bundle])[0]
        self.assertEqual(result["health"], "Succeeded")
        self.assertIn(
            "uid-bundle", [item["uid"] for item in result["currentChain"]["attempts"] if item["status"] == "Failed"]
        )

    def test_retry_chain_with_archived_originals(self):
        runs = complete_chain()
        runs[1].update(status="Failed", source="archive")
        first = run(
            "retry-one", PIPELINES[1], status="Failed", rebuiltFrom=runs[1]["uid"], created="2026-10-08T11:00:00Z"
        )
        second = run("retry-two", PIPELINES[1], rebuiltFrom=first["uid"], created="2026-10-08T12:00:00Z")
        runs[2]["parent"] = second["name"]
        self.assertEqual(build_health(runs + [first, second])[0]["health"], "Succeeded")

    def test_missing_triggered_target_does_not_establish_green(self):
        runs = complete_chain()
        runs[2]["triggered"] = ["fbc-419", "fbc-420", "missing-fbc-421"]
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Unknown")
        self.assertEqual(result["currentChain"]["status"], "Incomplete")
        self.assertTrue(result["currentChain"]["issues"])

    def test_trigger_annotation_can_link_child_without_parent_label(self):
        runs = complete_chain()
        runs[0]["triggered"] = ["images"]
        runs[1]["parent"] = None
        self.assertEqual(build_health(runs)[0]["health"], "Succeeded")

    def test_reused_parent_name_is_ambiguous(self):
        runs = complete_chain()
        reused = {**runs[2], "uid": "reused-bundle-uid", "created": "2026-10-08T12:00:00Z"}
        result = build_health(runs + [reused])[0]
        self.assertEqual(result["health"], "Unknown")
        self.assertTrue(result["currentChain"]["issues"])

    def test_dry_run_cannot_establish_green(self):
        self.assertEqual(build_health(complete_chain(dryRun=True))[0]["health"], "Unknown")

    def test_missing_stage_is_incomplete_and_orphans_are_not_merged(self):
        runs = complete_chain()
        runs[2]["parent"] = "unrelated-image-build"
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Unknown")
        self.assertEqual(result["currentChain"]["status"], "Incomplete")

    def test_namespaces_groups_and_assemblies_are_isolated(self):
        runs = (
            complete_chain()
            + complete_chain(namespace="other-tenant")
            + complete_chain("other-", group="acm-2.17")
            + complete_chain("assembly-", assembly="test")
        )
        self.assertEqual(len(build_health(runs)), 4)
        runs[-1]["status"] = "Failed"
        results = build_health(runs)
        self.assertEqual(sum(item["health"] == "Succeeded" for item in results), 3)

    def test_incomplete_history_prevents_confirming_green_but_preserves_known_failures(self):
        runs = complete_chain()
        result = build_health(runs, {"art-acm-tenant"})[0]
        self.assertEqual(result["health"], "Unknown")
        self.assertTrue(result["incompleteHistory"])
        runs[-1]["status"] = "Failed"
        self.assertEqual(build_health(runs, {"art-acm-tenant"})[0]["health"], "Failed")

    def test_unknown_conditions_and_nonstandard_success_reason(self):
        raw = {
            "metadata": {},
            "spec": {"params": [{"name": "group", "value": "acm-2.15"}]},
            "status": {"conditions": [{"type": "Succeeded", "status": "True", "reason": "Completed"}]},
        }
        summary = {"status": "Completed"}
        self.assertEqual(health_run(raw, summary)["status"], "Succeeded")
        raw["status"]["conditions"][0].update(status="False", reason="PipelineRunTimeout")
        self.assertEqual(health_run(raw, summary)["status"], "Failed")
        raw["status"]["conditions"] = []
        self.assertEqual(health_run(raw, summary)["status"], "Unknown")


async def items(values=(), error=None):
    if error:
        raise error
    for value in values:
        yield value


class HealthApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateway = MagicMock()
        self.gateway.__aenter__.return_value = self.gateway
        self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items()
        self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items()
        self.enterContext(patch.object(api, "Gateway", return_value=self.gateway))
        self.enterContext(patch.object(api, "NAMESPACES", ("art-acm-tenant",)))
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api.app),
            base_url="https://ui.example",
            headers={"x-forwarded-access-token": "test-token"},
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_endpoint_requires_login_and_configured_namespace(self):
        response = await self.client.get("/api/pipeline-health", params={"namespace": "other-tenant"})
        self.assertEqual(response.status_code, 404)
        del self.client.headers["x-forwarded-access-token"]
        response = await self.client.get("/api/pipeline-health")
        self.assertEqual(response.status_code, 401)

    async def test_authentication_failures_propagate_from_either_source(self):
        for source in ("list_records", "list_kube"):
            with self.subTest(source=source):
                self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items()
                self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items()
                getattr(self.gateway, source).side_effect = lambda *_args, **_kwargs: items(
                    error=UpstreamError(401, "Unauthorized")
                )
                response = await self.client.get("/api/pipeline-health")
                self.assertEqual(response.status_code, 401)

    async def test_partial_history_is_reported(self):
        self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items(error=UpstreamError(503, "Unavailable"))
        response = await self.client.get("/api/pipeline-health", params={"namespace": "art-acm-tenant"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["errors"][0]["source"], "archive")
        self.assertEqual(response.headers["cache-control"], "no-store")

    async def test_live_data_replaces_archived_snapshot_by_uid(self):
        raw = {
            "metadata": {"namespace": "art-acm-tenant", "name": "scan", "uid": "uid-scan"},
            "spec": {"pipelineRef": {"name": PIPELINES[0]}, "params": [{"name": "group", "value": "acm-2.15"}]},
            "status": {"conditions": [{"type": "Succeeded", "status": "Unknown", "reason": "Running"}]},
        }
        archived = deepcopy(raw)
        raw["status"]["conditions"][0].update(status="True", reason="Succeeded")
        self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items(((None, archived),))
        self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items((raw,))
        response = await self.client.get("/api/pipeline-health")
        self.assertEqual(response.status_code, 200)
        scan = response.json()["items"][0]["latestScan"]
        self.assertEqual(scan["source"], "live")
        self.assertEqual(scan["status"], "Succeeded")


if __name__ == "__main__":
    unittest.main()
