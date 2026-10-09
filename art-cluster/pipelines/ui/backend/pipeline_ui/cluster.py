"""Request-scoped clients for OpenShift and Tekton Results."""

import asyncio
import base64
import json
import os
from urllib.parse import quote

import httpx

KUBE_API = os.getenv("KUBE_API_URL", "https://kubernetes.default.svc").rstrip("/")
RESULTS_API = os.getenv("RESULTS_API_URL", "https://tekton-results-api-service.openshift-pipelines.svc:8080").rstrip(
    "/"
)
KUBE_CA = os.getenv("KUBE_CA_FILE", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
RESULTS_CA = os.getenv("RESULTS_CA_FILE", "/var/run/secrets/service-ca/service-ca.crt")
RESULTS_PREFIX = "/apis/results.tekton.dev/v1alpha2/parents"


class UpstreamError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class Gateway:
    def __init__(self, token: str):
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        self.kube = httpx.AsyncClient(base_url=KUBE_API, headers=headers, verify=KUBE_CA, timeout=30)
        self.results = httpx.AsyncClient(base_url=RESULTS_API, headers=headers, verify=RESULTS_CA, timeout=45)
        self._session_check = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        if self._session_check is not None:
            if not self._session_check.done():
                self._session_check.cancel()
            await asyncio.gather(self._session_check, return_exceptions=True)
        await self.kube.aclose()
        await self.results.aclose()

    @staticmethod
    def _check(response: httpx.Response) -> httpx.Response:
        if response.is_error:
            try:
                body = response.json()
                message = (body.get("message") or body.get("error")) if isinstance(body, dict) else None
                message = message or response.reason_phrase
            except ValueError:
                message = response.reason_phrase
            raise UpstreamError(response.status_code, str(message))
        return response

    async def kube_json(self, method: str, path: str, **kwargs) -> dict:
        response = await self.kube.request(method, path, **kwargs)
        return self._check(response).json()

    async def results_json(self, path: str, **kwargs) -> dict:
        response = await self.results.get(path, **kwargs)
        return (await self._check_results(response)).json()

    async def _check_results(self, response: httpx.Response) -> httpx.Response:
        if response.status_code == 401:
            # Results returns 401 for both expired tokens and namespace RBAC denials.
            # Validate the session before allowing callers to skip an inaccessible tenant.
            if self._session_check is None:
                self._session_check = asyncio.create_task(self.kube_json("GET", "/apis/user.openshift.io/v1/users/~"))
            await self._session_check
            raise UpstreamError(403, "You do not have access to this Tekton Results history")
        return self._check(response)

    async def pipeline(self, namespace: str, name: str) -> dict:
        return await self.kube_json("GET", f"/apis/tekton.dev/v1/namespaces/{quote(namespace)}/pipelines/{quote(name)}")

    async def run(self, namespace: str, name: str) -> dict:
        return await self.kube_json(
            "GET", f"/apis/tekton.dev/v1/namespaces/{quote(namespace)}/pipelineruns/{quote(name)}"
        )

    async def pod(self, namespace: str, name: str) -> dict:
        return await self.kube_json("GET", f"/api/v1/namespaces/{quote(namespace)}/pods/{quote(name)}")

    async def list_events(self, namespace: str, name: str):
        token = ""
        while True:
            params = {"limit": 200, "fieldSelector": f"involvedObject.name={name}"}
            if token:
                params["continue"] = token
            data = await self.kube_json("GET", f"/api/v1/namespaces/{quote(namespace)}/events", params=params)
            for event in data.get("items", []):
                yield event
            token = data.get("metadata", {}).get("continue", "")
            if not token:
                return

    async def list_kube(self, namespace: str, resource: str, *, label_selector: str | None = None):
        token = ""
        while True:
            params = {"limit": 500}
            if token:
                params["continue"] = token
            if label_selector:
                params["labelSelector"] = label_selector
            data = await self.kube_json(
                "GET", f"/apis/tekton.dev/v1/namespaces/{quote(namespace)}/{resource}", params=params
            )
            for item in data.get("items", []):
                yield item
            token = data.get("metadata", {}).get("continue", "")
            if not token:
                return

    async def list_records(self, namespace: str, *, filter_text: str):
        token = ""
        while True:
            params = {"filter": filter_text, "page_size": 200, "order_by": "create_time desc"}
            if token:
                params["page_token"] = token
            data = await self.results_json(f"{RESULTS_PREFIX}/{quote(namespace)}/results/-/records", params=params)
            for record in data.get("records", []):
                try:
                    run = json.loads(base64.b64decode(record["data"]["value"]))
                except (KeyError, ValueError, TypeError):
                    continue
                yield record, run
            token = data.get("next_page_token") or data.get("nextPageToken") or ""
            if not token:
                return

    async def archived_run(self, namespace: str, name: str, uid: str | None = None):
        # Tekton Results permits multiple runs with the same name after pruning.
        escaped = name.replace("\\", "\\\\").replace("'", "\\'")
        query = f"data_type == PIPELINE_RUN && data.metadata.name == '{escaped}'"
        async for record, run in self.list_records(namespace, filter_text=query):
            if uid is None or run.get("metadata", {}).get("uid") == uid:
                return record, run
        raise UpstreamError(404, "PipelineRun was not found in Tekton Results")

    async def log_text(self, client: httpx.AsyncClient, path: str, *, params: dict | None = None):
        chunks = []
        length = 0
        truncated = False
        async with client.stream("GET", path, params=params) as response:
            if response.is_error:
                await response.aread()
            if client is self.results:
                await self._check_results(response)
            else:
                self._check(response)
            async for chunk in response.aiter_bytes():
                remaining = 8_000_000 - length
                if len(chunk) > remaining:
                    chunks.append(chunk[:remaining])
                    truncated = True
                    break
                chunks.append(chunk)
                length += len(chunk)
        return b"".join(chunks).decode("utf-8", errors="replace"), truncated

    async def archived_logs(self, record_name: str):
        log_name = record_name.replace("/records/", "/logs/", 1)
        return await self.log_text(self.results, f"{RESULTS_PREFIX}/{log_name}")

    async def live_logs(self, namespace: str, run_name: str):
        taskruns = []
        async for taskrun in self.list_kube(namespace, "taskruns", label_selector=f"tekton.dev/pipelineRun={run_name}"):
            taskruns.append(taskrun)
        taskruns.sort(key=lambda task: task.get("metadata", {}).get("creationTimestamp", ""))
        sections = []
        truncated = False
        for taskrun in taskruns:
            pod = taskrun.get("status", {}).get("podName")
            if not pod:
                continue
            for step in taskrun.get("status", {}).get("steps", []):
                container = step.get("container") or f"step-{step.get('name', '')}"
                try:
                    contents, clipped = await self.log_text(
                        self.kube,
                        f"/api/v1/namespaces/{quote(namespace)}/pods/{quote(pod)}/log",
                        params={"container": container},
                    )
                except UpstreamError as error:
                    if error.status in (400, 404):
                        continue
                    raise
                sections.append(f"## {taskrun['metadata']['name']} / {step.get('name', container)}\n{contents}")
                truncated = truncated or clipped
        return "\n\n".join(sections), truncated

    async def follow_step_logs(self, namespace: str, pod: str, container: str):
        path = f"/api/v1/namespaces/{quote(namespace)}/pods/{quote(pod)}/log"
        async with self.kube.stream(
            "GET", path, params={"container": container, "follow": "true"}, timeout=None
        ) as response:
            if response.is_error:
                await response.aread()
            self._check(response)
            async for chunk in response.aiter_text():
                yield chunk
