import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from pipeline_ui import app as api
from pipeline_ui.cluster import UpstreamError

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
        self.assertIsNone(response.json()["schedulerJob"])
        self.gateway.list_kube.assert_not_called()

    async def test_uid_only_older_rebuild_has_no_source_link_target(self):
        self.run["metadata"]["annotations"] = {"art.openshift.io/rebuilt-from": "source-uid"}

        response = await self.client.get(f"/api/runs/{NAMESPACE}/rebuilt-run")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["rebuiltFrom"])

    async def test_scheduler_job_annotation_survives_archival(self):
        self.run["metadata"]["annotations"]["art.openshift.io/scheduler-job"] = "art-scheduler-tenant/scheduler-12345"
        with patch.object(api, "find_run", new=AsyncMock(return_value=(self.run, "archive", None))):
            response = await self.client.get(f"/api/runs/{NAMESPACE}/rebuilt-run")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["schedulerJob"], {"namespace": "art-scheduler-tenant", "name": "scheduler-12345"}
        )
        self.gateway.list_kube.assert_not_called()

    async def test_legacy_scheduler_job_is_linked_only_when_its_owner_is_verified(self):
        name = "ocp4-scan-konflux-hp-29918426"
        self.run["metadata"]["name"] = f"{name}-4-20"
        self.run["spec"]["pipelineRef"]["name"] = "ocp4-scan-konflux"
        path = f"/api/runs/{NAMESPACE}/{name}-4-20"

        for owner in ("ocp4-scan-konflux-hp", "unrelated-cronjob", None):
            with self.subTest(owner=owner):

                async def list_jobs(*args, **kwargs):
                    if owner:
                        yield {
                            "metadata": {"ownerReferences": [{"kind": "CronJob", "name": owner, "controller": True}]}
                        }

                self.gateway.list_kube.side_effect = list_jobs
                response = await self.client.get(path)
                self.assertEqual(response.status_code, 200)
                expected = {"namespace": NAMESPACE, "name": name} if owner == "ocp4-scan-konflux-hp" else None
                self.assertEqual(response.json()["schedulerJob"], expected)
                self.gateway.list_kube.assert_called_with(
                    NAMESPACE, "jobs", api_group="batch/v1", field_selector=f"metadata.name={name}"
                )

        self.gateway.list_kube.side_effect = UpstreamError(403, "Forbidden")
        response = await self.client.get(path)
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["schedulerJob"])

    async def test_components_available_for_live_and_archived_details(self):
        self.run["metadata"]["annotations"].update(
            {
                "art.openshift.io/component-report": json.dumps({"version": 1, "partial": False}),
                "art.openshift.io/component-build-123": json.dumps(
                    {
                        "version": 1,
                        "type": "image",
                        "name": "operator",
                        "pipelineRuns": [],
                        "status": "Succeeded",
                        "checks": [
                            {
                                "kind": "conforma",
                                "name": "Conforma",
                                "blocking": True,
                                "status": "Failed",
                                "url": "https://konflux.example/check",
                                "reason": "Policy failed",
                            }
                        ],
                    }
                ),
            }
        )
        for source in ("live", "archive"):
            with patch.object(api, "find_run", new=AsyncMock(return_value=(self.run, source, None))):
                response = await self.client.get(f"/api/runs/{NAMESPACE}/rebuilt-run?uid=rebuilt-uid")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["source"], source)
            self.assertEqual(response.json()["components"][0]["status"], "Succeeded")
            self.assertEqual(response.json()["components"][0]["resultStatus"], "Failed")
            self.assertEqual(response.json()["components"][0]["checks"][0]["url"], "https://konflux.example/check")
            self.assertTrue(response.json()["componentReporting"]["available"])

    async def test_lp_warning_in_live_and_archived_run_details(self):
        warning = 'Images excluded after rebase failure: must-gather'
        self.run['metadata']['annotations']['art.openshift.io/rebase-warning'] = warning
        for pipeline in ('build-layered-products', 'olm-bundle-konflux', 'build-fbc', 'build'):
            self.run['spec']['pipelineRef']['name'] = pipeline
            for native_status, reason in (
                ('True', 'Succeeded'),
                ('True', 'Completed'),
                ('False', 'Failed'),
                ('False', 'Cancelled'),
                ('Unknown', 'Running'),
            ):
                self.run['status']['conditions'] = [{'type': 'Succeeded', 'status': native_status, 'reason': reason}]
                for source in ('live', 'archive'):
                    with (
                        self.subTest(pipeline=pipeline, status=native_status, source=source, reason=reason),
                        patch.object(api, 'find_run', new=AsyncMock(return_value=(self.run, source, None))),
                    ):
                        response = await self.client.get(f'/api/runs/{NAMESPACE}/rebuilt-run?uid=rebuilt-uid')
                    expected_warning = pipeline == 'build-layered-products' and native_status == 'True'
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json()['status'], 'Warning' if expected_warning else reason)
                    self.assertEqual(response.json()['message'], warning if expected_warning else '')

    async def test_success_without_valid_lp_warning_stays_successful(self):
        self.run['spec']['pipelineRef']['name'] = 'build-layered-products'
        self.run['status']['conditions'] = [{'type': 'Succeeded', 'status': 'True', 'reason': 'Succeeded'}]
        for warning in (None, '', '  ', [], {}):
            with self.subTest(warning=warning):
                self.run['metadata']['annotations']['art.openshift.io/rebase-warning'] = warning
                self.assertEqual(api.run_summary(self.run, 'live')['status'], 'Succeeded')


if __name__ == "__main__":
    unittest.main()
