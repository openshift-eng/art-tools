import json
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
    def test_partial_lp_failure_makes_completed_chain_warning(self):
        runs = complete_chain()
        runs[1]['status'] = 'Warning'
        result = build_health(runs)[0]
        self.assertEqual(result['health'], 'Warning')
        self.assertEqual(result['lastCompletedChain']['status'], 'Warning')
        self.assertIsNone(result['lastSuccessfulChain'])
        runs[-1]['status'] = 'Failed'
        self.assertEqual(build_health(runs)[0]['health'], 'Failed')

    def test_successful_retry_recovers_warning_and_running_retry_preserves_it(self):
        runs = complete_chain()
        runs[1]['status'] = 'Warning'
        retry = run(
            'retry-images', PIPELINES[1], status='Running', rebuiltFrom=runs[1]['uid'], created='2026-10-08T12:00:00Z'
        )
        runs[2]['parent'] = retry['name']
        result = build_health(runs + [retry])[0]
        self.assertEqual(result['health'], 'Warning')
        self.assertEqual(result['currentChain']['status'], 'Running')
        retry.update(status='Succeeded', completed=retry['created'])
        self.assertEqual(build_health(runs + [retry])[0]['health'], 'Succeeded')

    def test_lp_warning_survives_native_health_normalization(self):
        raw = {
            'metadata': {'annotations': {'art.openshift.io/rebase-warning': 'Partial LP build failure'}},
            'spec': {'pipelineRef': {'name': 'build-layered-products'}},
            'status': {'conditions': [{'type': 'Succeeded', 'status': 'True', 'reason': 'Succeeded'}]},
        }
        self.assertEqual(health_run(raw, {'status': 'Warning'})['status'], 'Warning')
        raw['status']['conditions'][0].update(status='False', reason='Failed')
        self.assertEqual(health_run(raw, {'status': 'Failed'})['status'], 'Failed')

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

    def test_direct_scan_branches_contribute_to_health(self):
        for pipeline in PIPELINES[2:]:
            for status in ("Failed", "Cancelled", "Running", "Succeeded"):
                with self.subTest(pipeline=pipeline, status=status):
                    runs = complete_chain()
                    runs.append(run("direct", pipeline, parent="scan", status=status, target="4.20"))
                    result = build_health(runs)[0]
                    chain = result["currentChain"]
                    self.assertEqual(chain["issues"], [])
                    self.assertIn(
                        "uid-direct", [item["uid"] for item in chain["stages"][PIPELINES.index(pipeline)]["runs"]]
                    )
                    self.assertEqual(chain["status"], status)
                    self.assertEqual(result["health"], "Unknown" if status == "Running" else status)

    def test_direct_scan_only_work_is_visible_without_establishing_e2e_green(self):
        for pipeline in PIPELINES[2:]:
            with self.subTest(pipeline=pipeline):
                runs = [run("scan", PIPELINES[0], triggered=["direct"]), run("direct", pipeline)]
                result = build_health(runs)[0]
                self.assertEqual(result["health"], "Unknown")
                self.assertEqual(result["currentChain"]["status"], "Incomplete")
                self.assertEqual(result["currentChain"]["issues"], [])

    def test_scan_retry_recovers_direct_bundle_or_fbc_and_keeps_failure_history(self):
        for pipeline in PIPELINES[2:]:
            with self.subTest(pipeline=pipeline):
                runs = complete_chain()
                original = run("direct", pipeline, parent="scan", status="Failed", target="4.20", source="archive")
                runs.append(original)
                runs.append(run("later-scan", PIPELINES[0], created="2026-10-08T12:00:00Z"))
                retry = run(
                    "scan-retry",
                    pipeline,
                    parent="scan",
                    status="Running",
                    rebuiltFrom=original["uid"],
                    target="4.20",
                    created="2026-10-08T12:01:00Z",
                )
                runs.append(retry)
                result = build_health(runs)[0]
                self.assertEqual(result["health"], "Failed")
                self.assertEqual(result["currentChain"]["status"], "Running")
                self.assertEqual(result["currentChain"]["active"][0]["uid"], retry["uid"])
                retry.update(status="Succeeded", completed="2026-10-08T12:05:00Z")
                result = build_health(runs)[0]
                chain = result["currentChain"]
                self.assertEqual(result["health"], "Succeeded")
                self.assertEqual(chain["issues"], [])
                self.assertTrue(chain["recovered"])
                self.assertIn(
                    original["uid"], [item["uid"] for item in chain["attempts"] if item["status"] == "Failed"]
                )
                self.assertNotIn(original["uid"], [item["uid"] for stage in chain["stages"] for item in stage["runs"]])
                self.assertEqual(result["latestScan"]["name"], "later-scan")

    def test_invalid_pipeline_order_still_reports_an_issue(self):
        for parent in ("images", "bundle"):
            with self.subTest(parent=parent):
                runs = complete_chain()
                pipeline = PIPELINES[3] if parent == "images" else PIPELINES[1]
                runs.append(run("invalid", pipeline, parent=parent))
                chain = build_health(runs)[0]["currentChain"]
                self.assertEqual(chain["issues"], ["Unexpected pipeline order in the build chain"])
                self.assertEqual(chain["status"], "Incomplete")

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

    def test_scan_triggered_fbc_recovery_preserves_original_chain(self):
        runs = complete_chain()
        runs[-1]["status"] = "Failed"
        runs[2]["triggered"] = [runs[3]["name"], runs[4]["name"]]
        retry = run(
            "retry-fbc",
            PIPELINES[3],
            parent=runs[2]["name"],
            rebuiltFrom=runs[-1]["uid"],
            target="4.20",
            created="2026-10-08T12:00:00Z",
        )
        later_scan = health_run(
            {
                "metadata": {"annotations": {"art.openshift.io/recovered-abc": retry["name"]}},
                "spec": {"params": [{"name": "group", "value": "acm-2.15"}]},
                "status": {"conditions": [{"type": "Succeeded", "status": "True"}]},
            },
            run("later-scan", PIPELINES[0], created="2026-10-08T12:00:00Z"),
        )
        result = build_health(runs + [later_scan, retry])[0]
        self.assertEqual(result["health"], "Succeeded")
        self.assertEqual(result["latestScan"]["name"], "later-scan")
        self.assertEqual(result["currentChain"]["root"]["name"], "scan")
        self.assertEqual(result["currentChain"]["issues"], [])
        self.assertIn(runs[-1]["uid"], {item["uid"] for item in result["currentChain"]["attempts"]})

    def test_bundle_recovery_uses_its_new_fbc_children_for_all_targets(self):
        runs = complete_chain()
        runs[2]["status"] = "Failed"
        retry = run(
            "retry-bundle",
            PIPELINES[2],
            parent="images",
            rebuiltFrom=runs[2]["uid"],
            created="2026-10-08T12:00:00Z",
        )
        children = [
            run(
                f"retry-fbc-{target}", PIPELINES[3], parent=retry["name"], target=target, created="2026-10-08T13:00:00Z"
            )
            for target in ("4.19", "4.20")
        ]
        retry["triggered"] = [child["name"] for child in children]
        self.assertNotEqual(build_health(runs + [retry] + children[:1])[0]["health"], "Succeeded")
        result = build_health(runs + [retry] + children)[0]
        self.assertEqual(result["health"], "Succeeded")
        self.assertEqual(
            {item["uid"] for item in result["currentChain"]["stages"][3]["runs"]}, {child["uid"] for child in children}
        )

    def fbc_scan_recovery(self, status="Succeeded"):
        runs = complete_chain()
        runs[-1].update(status="Failed", operatorNvrs=["operator-1-1"])
        scan = run("recovery-scan", PIPELINES[0], created="2026-10-08T12:00:00Z")
        recovery = run(
            "recovered-fbc",
            PIPELINES[3],
            status=status,
            recoveryScan=scan["name"],
            operatorNvrs=["operator-1-1"],
            target="4.20",
            created="2026-10-08T12:01:00Z",
            source="archive",
        )
        return runs + [scan, recovery]

    def batched_bundle_recovery(self):
        runs = complete_chain()
        runs[2].update(status="Failed", operatorNvrs=["operator-a-1-1"])
        other = run("other-bundle", PIPELINES[2], parent="images", status="Failed", operatorNvrs=["operator-b-1-1"])
        retry = run(
            "recovered-bundles",
            PIPELINES[2],
            rebuiltFrom=runs[2]["uid"],
            rebuiltFromUids=[runs[2]["uid"], other["uid"]],
            operatorNvrs=["operator-a-1-1", "operator-b-1-1", "operator-c-1-1"],
            created="2026-10-08T12:00:00Z",
        )
        children = [
            run("recovered-fbc", PIPELINES[3], parent=retry["name"], target="4.20", created="2026-10-08T13:00:00Z")
        ]
        return runs + [other, retry] + children

    def test_explicit_batched_bundle_recovery_keeps_all_original_attempts(self):
        runs = self.batched_bundle_recovery()
        original = deepcopy(runs)
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Succeeded")
        chain = result["currentChain"]
        self.assertEqual([item["name"] for item in chain["stages"][2]["runs"]], ["recovered-bundles"])
        self.assertTrue({"uid-bundle", "uid-other-bundle"}.issubset(item["uid"] for item in chain["attempts"]))
        self.assertEqual(runs, original)

    def test_batched_bundle_recovery_does_not_clear_uncovered_failures(self):
        runs = self.batched_bundle_recovery()
        runs[-3]["operatorNvrs"] = ["operator-b-1-1", "not-rebuilt-1-1"]
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Failed")
        self.assertIn("uid-other-bundle", [item["uid"] for item in result["currentChain"]["failures"]])

    def test_batched_bundle_recovery_rejects_foreign_or_active_sources(self):
        for changes in (
            {"group": "other-group"},
            {"namespace": "other-namespace"},
            {"target": "4.19"},
            {"dryRun": True},
            {"completed": None, "status": "Running"},
            {"created": "2026-10-08T14:00:00Z", "completed": "2026-10-08T14:01:00Z"},
        ):
            with self.subTest(changes=changes):
                runs = self.batched_bundle_recovery()
                runs[-3].update(changes)
                result = next(item for item in build_health(runs) if item["group"] == "acm-2.15")
                self.assertNotEqual(result["health"], "Succeeded")

    def test_health_run_reads_multiple_retry_sources_and_rejects_malformed_lists(self):
        for value, expected in (
            (json.dumps(["uid-a", "uid-b", "uid-a"]), ["uid-a", "uid-b"]),
            ('[', []),
            ('"uid-a"', []),
            ('[null]', []),
            ('[""]', []),
        ):
            raw = {"metadata": {"annotations": {"art.openshift.io/rebuilt-from-uids": value}}}
            self.assertEqual(health_run(raw, {})["rebuiltFromUids"], expected)

    def test_unresolved_fbc_scan_recovery_matches_exact_inputs_and_keeps_history(self):
        for status in ("Succeeded", "Running", "Failed", "Cancelled"):
            with self.subTest(status=status):
                runs = self.fbc_scan_recovery(status)
                original = deepcopy(runs)
                result = build_health(runs)[0]
                chain = result["currentChain"]
                self.assertEqual(result["health"], "Failed" if status == "Running" else status)
                self.assertEqual(chain["status"], status)
                self.assertEqual(chain["root"]["name"], "scan")
                self.assertEqual(result["latestScan"]["name"], "recovery-scan")
                self.assertEqual(
                    {item["uid"] for item in chain["stages"][3]["runs"]}, {"uid-fbc-419", "uid-recovered-fbc"}
                )
                self.assertTrue(chain["recovered"])
                self.assertEqual(chain["stages"][3]["runs"][-1]["rebuiltFrom"], "uid-fbc-420")
                self.assertIn("uid-fbc-420", [item["uid"] for item in chain["attempts"]])
                self.assertEqual(runs, original)

    def test_fbc_scan_recovery_requires_matching_identity_inputs_and_scan(self):
        mismatches = (
            {"namespace": "another-tenant"},
            {"group": "acm-2.17"},
            {"assembly": "test"},
            {"target": "4.21"},
            {"target": ""},
            {"operatorNvrs": ["operator-2-1"]},
            {"operatorNvrs": ["operator-1-1", "extra-operator-1-1"]},
            {"operatorNvrs": []},
            {"dryRun": True},
            {"recoveryScan": "missing-scan"},
            {"recoveryScan": "bundle"},
            {"recoveryScan": None},
            {"rebuiltFrom": "missing-uid"},
            {"parent": "unrelated-bundle"},
            {"created": "2026-10-08T11:00:00Z"},
        )
        for mismatch in mismatches:
            with self.subTest(mismatch=mismatch):
                runs = self.fbc_scan_recovery()
                runs[-1].update(mismatch)
                result = next(item for item in build_health(runs) if item["group"] == "acm-2.15")
                self.assertEqual(result["health"], "Failed")
                self.assertFalse(result["currentChain"]["recovered"])

    def test_fbc_scan_recovery_rejects_ambiguous_sources_or_scan_names(self):
        for index in (-3, -2):
            with self.subTest(index=index):
                runs = self.fbc_scan_recovery()
                duplicate = {**runs[index], "uid": "duplicate-uid"}
                runs.append(duplicate)
                result = build_health(runs)[0]
                self.assertNotEqual(result["health"], "Succeeded")
                self.assertFalse(result["currentChain"]["recovered"])

    def test_fbc_scan_recovery_cannot_use_dry_run_or_unspecified_original_inputs(self):
        for changes in ({"dryRun": True}, {"operatorNvrs": []}):
            with self.subTest(changes=changes):
                runs = self.fbc_scan_recovery()
                runs[-3].update(changes)
                result = build_health(runs)[0]
                self.assertEqual(result["health"], "Failed")
                self.assertFalse(result["currentChain"]["recovered"])

    def test_latest_matching_fbc_recovery_supplies_effective_result(self):
        runs = self.fbc_scan_recovery()
        runs.append(
            {
                **runs[-1],
                "uid": "latest-recovery-uid",
                "name": "latest-recovery",
                "created": "2026-10-08T13:01:00Z",
                "completed": "2026-10-08T13:05:00Z",
                "status": "Failed",
            }
        )
        result = build_health(runs)[0]
        self.assertEqual(result["health"], "Failed")
        self.assertEqual(result["currentChain"]["failures"][0]["uid"], "latest-recovery-uid")
        self.assertEqual(result["lastSuccessfulChain"]["status"], "Succeeded")

    def test_health_run_reads_recovery_scan_and_normalizes_operator_nvr_set(self):
        raw = {
            "metadata": {"annotations": {"art.openshift.io/recovery-scan": "scan"}},
            "spec": {"params": [{"name": "operator-nvrs", "value": "operator-b-1-1, operator-a-1-1,operator-b-1-1"}]},
        }
        summary = health_run(raw, {})
        self.assertEqual(summary["recoveryScan"], "scan")
        self.assertEqual(summary["operatorNvrs"], ["operator-a-1-1", "operator-b-1-1"])

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

    def test_namespaces_groups_and_only_stream_assemblies_are_included(self):
        runs = (
            complete_chain()
            + complete_chain(namespace="other-tenant")
            + complete_chain("other-", group="acm-2.17")
            + complete_chain("assembly-", assembly="test")
        )
        self.assertEqual(len(build_health(runs)), 3)
        runs[-1]["status"] = "Failed"
        results = build_health(runs)
        self.assertEqual({item["assembly"] for item in results}, {"stream"})
        self.assertTrue(all(item["health"] == "Succeeded" for item in results))

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

    async def test_all_tenants_view_reports_one_source_permission_failure(self):
        raw = {
            "metadata": {"namespace": "art-acm-tenant", "name": "scan", "uid": "uid-scan"},
            "spec": {"pipelineRef": {"name": PIPELINES[0]}, "params": [{"name": "group", "value": "acm-2.15"}]},
            "status": {"conditions": [{"type": "Succeeded", "status": "True", "reason": "Succeeded"}]},
        }
        self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items(error=UpstreamError(403, "Forbidden"))
        self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items((raw,))
        response = await self.client.get("/api/pipeline-health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["errors"][0]["status"], 403)
        self.assertTrue(response.json()["items"][0]["incompleteHistory"])

    async def test_all_tenants_view_omits_fully_inaccessible_namespaces(self):
        self.gateway.list_records.side_effect = lambda *_args, **_kwargs: items(error=UpstreamError(403, "Forbidden"))
        self.gateway.list_kube.side_effect = lambda *_args, **_kwargs: items(error=UpstreamError(403, "Forbidden"))
        response = await self.client.get("/api/pipeline-health")
        self.assertEqual(response.json(), {"items": [], "errors": []})

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
