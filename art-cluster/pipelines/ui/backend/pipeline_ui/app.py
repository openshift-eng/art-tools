"""ART Pipelines UI API and static frontend."""

import asyncio
import json
import os
import secrets
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .cluster import Gateway, UpstreamError
from .rebuild import InvalidRun, build_run, parameter_form, pipeline_name

NAMESPACES = tuple(
    namespace.strip() for namespace in os.getenv("PIPELINE_NAMESPACES", "").split(",") if namespace.strip()
)
CSRF_COOKIE = "__Host-art-pipelines-csrf"
app = FastAPI(title="ART Pipelines UI", docs_url=None, redoc_url=None, openapi_url=None)


class SourceRun(BaseModel):
    name: str
    uid: str


class CreateRun(BaseModel):
    namespace: str
    pipeline: str
    resourceVersion: str
    values: dict[str, object] = Field(default_factory=dict)
    workspaces: list[dict] = Field(default_factory=list)
    sourceRun: SourceRun | None = None


@app.exception_handler(UpstreamError)
async def upstream_error(_request: Request, error: UpstreamError):
    return JSONResponse(status_code=error.status, content={"detail": error.message})


@app.exception_handler(httpx.RequestError)
async def network_error(_request: Request, _error: httpx.RequestError):
    return JSONResponse(status_code=502, content={"detail": "Cluster service is temporarily unavailable"})


def user_token(request: Request) -> str:
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        raise HTTPException(status_code=401, detail="OpenShift login is required")
    return token


def allowed_namespace(namespace: str) -> str:
    if namespace not in NAMESPACES:
        raise HTTPException(status_code=404, detail="Namespace is not configured")
    return namespace


def selected_namespaces(namespace: str | None) -> tuple[str, ...]:
    return (allowed_namespace(namespace),) if namespace else NAMESPACES


def check_csrf(request: Request) -> None:
    cookie = request.cookies.get(CSRF_COOKIE)
    header = request.headers.get("x-csrf-token")
    if not cookie or not header or not secrets.compare_digest(cookie, header):
        raise HTTPException(status_code=403, detail="Refresh the page before starting a run")


def run_summary(run: dict, source: str) -> dict:
    metadata = run.get("metadata", {})
    status = run.get("status", {})
    condition = (status.get("conditions") or [{}])[0]
    reason = condition.get("reason") or "Unknown"
    if condition.get("status") == "Unknown":
        reason = "Running"
    return {
        "namespace": metadata.get("namespace"),
        "name": metadata.get("name"),
        "uid": metadata.get("uid"),
        "pipeline": pipeline_name(run),
        "status": reason,
        "message": condition.get("message", ""),
        "created": metadata.get("creationTimestamp"),
        "started": status.get("startTime"),
        "completed": status.get("completionTime"),
        "source": source,
    }


def pipeline_summary(pipeline: dict) -> dict:
    metadata = pipeline.get("metadata", {})
    return {
        "namespace": metadata.get("namespace"),
        "name": metadata.get("name"),
        "description": pipeline.get("spec", {}).get("description", ""),
        "parameterCount": len(pipeline.get("spec", {}).get("params", [])),
    }


def event_summary(event: dict) -> dict:
    metadata = event.get("metadata", {})
    involved = event.get("involvedObject", {})
    return {
        "uid": metadata.get("uid"),
        "kind": involved.get("kind"),
        "object": involved.get("name"),
        "type": event.get("type", "Normal"),
        "reason": event.get("reason", ""),
        "message": event.get("message", ""),
        "count": event.get("count", 1),
        "firstSeen": event.get("firstTimestamp") or event.get("eventTime"),
        "lastSeen": event.get("lastTimestamp") or event.get("eventTime") or metadata.get("creationTimestamp"),
    }


async def read_run_summaries(gateway: Gateway, namespace: str, errors: list, selected: bool) -> list[dict]:
    items = {}
    try:
        async for _record, run in gateway.list_records(namespace, filter_text="data_type == PIPELINE_RUN"):
            summary = run_summary(run, "archive")
            if summary["uid"]:
                items[summary["uid"]] = summary
    except UpstreamError as error:
        if error.status != 403 or selected:
            errors.append({"namespace": namespace, "source": "archive", "message": error.message})
    try:
        async for run in gateway.list_kube(namespace, "pipelineruns"):
            summary = run_summary(run, "live")
            if summary["uid"]:
                items[summary["uid"]] = summary
    except UpstreamError as error:
        if error.status != 403 or selected:
            errors.append({"namespace": namespace, "source": "live", "message": error.message})
    return list(items.values())


async def find_run(gateway: Gateway, namespace: str, name: str, uid: str | None = None):
    uid = uid or None
    try:
        run = await gateway.run(namespace, name)
        if uid is None or run.get("metadata", {}).get("uid") == uid:
            return run, "live", None
    except UpstreamError as error:
        if error.status != 404:
            raise
    record, run = await gateway.archived_run(namespace, name, uid)
    return run, "archive", record["name"]


@app.get("/api/health")
async def health():
    return {"ready": True}


@app.get("/api/session")
async def session(request: Request, response: Response):
    user_token(request)
    csrf = request.cookies.get(CSRF_COOKIE) or secrets.token_urlsafe(32)
    response.set_cookie(CSRF_COOKIE, csrf, secure=True, httponly=False, samesite="strict", path="/")
    return {"csrfToken": csrf, "user": request.headers.get("x-forwarded-user", "")}


@app.get("/api/namespaces")
async def namespaces(request: Request):
    async with Gateway(user_token(request)) as gateway:

        async def visible(namespace: str):
            try:
                await gateway.kube_json(
                    "GET", f"/apis/tekton.dev/v1/namespaces/{namespace}/pipelines", params={"limit": 1}
                )
                return namespace
            except UpstreamError as error:
                if error.status == 403:
                    return None
                raise

        found = await asyncio.gather(*(visible(namespace) for namespace in NAMESPACES))
    return {"namespaces": [namespace for namespace in found if namespace]}


@app.get("/api/pipelines")
async def pipelines(request: Request, namespace: str | None = None, q: str = ""):
    names = selected_namespaces(namespace)
    async with Gateway(user_token(request)) as gateway:

        async def read(current: str):
            try:
                return [pipeline_summary(item) async for item in gateway.list_kube(current, "pipelines")]
            except UpstreamError as error:
                if error.status == 403 and namespace is None:
                    return []
                raise

        groups = await asyncio.gather(*(read(name) for name in names))
    items = [item for group in groups for item in group if q.lower() in item["name"].lower()]
    return {"items": sorted(items, key=lambda item: (item["namespace"], item["name"]))}


@app.get("/api/pipelines/latest-runs")
async def latest_pipeline_runs(request: Request, namespace: str | None = None):
    names = selected_namespaces(namespace)
    errors = []
    async with Gateway(user_token(request)) as gateway:
        groups = await asyncio.gather(
            *(read_run_summaries(gateway, name, errors, namespace is not None) for name in names)
        )
    latest = {}
    for group in groups:
        for run in group:
            if not run["pipeline"]:
                continue
            key = (run["namespace"], run["pipeline"])
            previous = latest.get(key)
            if previous is None or (run["created"] or "", run["source"] == "live") > (
                previous["created"] or "",
                previous["source"] == "live",
            ):
                latest[key] = run
    return {"items": list(latest.values()), "errors": errors}


@app.get("/api/pipelines/{namespace}/{name}")
async def pipeline_detail(request: Request, namespace: str, name: str):
    allowed_namespace(namespace)
    async with Gateway(user_token(request)) as gateway:
        pipeline = await gateway.pipeline(namespace, name)
    return {
        **pipeline_summary(pipeline),
        "parameters": pipeline.get("spec", {}).get("params", []),
        "workspaces": pipeline.get("spec", {}).get("workspaces", []),
        "resourceVersion": pipeline["metadata"]["resourceVersion"],
    }


@app.get("/api/pipelines/{namespace}/{name}/form")
async def run_form(
    request: Request, namespace: str, name: str, source_run: str | None = None, source_uid: str | None = None
):
    allowed_namespace(namespace)
    async with Gateway(user_token(request)) as gateway:
        pipeline = await gateway.pipeline(namespace, name)
        source = None
        if source_run:
            source, _, _ = await find_run(gateway, namespace, source_run, source_uid)
            if pipeline_name(source) != name:
                raise HTTPException(status_code=400, detail="Source run belongs to another Pipeline")
    return parameter_form(pipeline, source)


@app.get("/api/runs")
async def runs(
    request: Request,
    namespace: str | None = None,
    pipeline: str = "",
    q: str = "",
    status: str = "",
    since: str = "",
    until: str = "",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
):
    names = selected_namespaces(namespace)
    errors = []
    async with Gateway(user_token(request)) as gateway:
        groups = await asyncio.gather(
            *(read_run_summaries(gateway, name, errors, namespace is not None) for name in names)
        )
    items = [item for group in groups for item in group]
    if pipeline:
        items = [item for item in items if item["pipeline"] == pipeline]
    if q:
        items = [
            item
            for item in items
            if q.lower() in (item["name"] or "").lower() or q.lower() in (item["pipeline"] or "").lower()
        ]
    if status:
        items = [item for item in items if item["status"].lower() == status.lower()]
    if since:
        items = [item for item in items if (item["created"] or "") >= since]
    if until:
        items = [item for item in items if (item["created"] or "") <= until]
    items.sort(key=lambda item: item["created"] or "", reverse=True)
    start = (page - 1) * page_size
    return {"items": items[start : start + page_size], "total": len(items), "page": page, "errors": errors}


@app.get("/api/runs/{namespace}/{name}")
async def run_detail(request: Request, namespace: str, name: str, uid: str | None = None):
    allowed_namespace(namespace)
    async with Gateway(user_token(request)) as gateway:
        run, source, _ = await find_run(gateway, namespace, name, uid)
    metadata = run.get("metadata", {})
    labels = metadata.get("labels") or {}
    return {
        **run_summary(run, source),
        "labels": labels,
        "annotations": metadata.get("annotations") or {},
        "parentPipelineRun": labels.get("art.openshift.io/parent-pipelinerun"),
        "parameters": run.get("spec", {}).get("params", []),
        "workspaces": run.get("spec", {}).get("workspaces", []),
        "tasks": run.get("status", {}).get("childReferences", []),
    }


@app.get("/api/runs/{namespace}/{name}/children")
async def run_children(request: Request, namespace: str, name: str):
    allowed_namespace(namespace)
    label = "art.openshift.io/parent-pipelinerun"
    escaped = name.replace("\\", "\\\\").replace("'", "\\'")
    query = f"data_type == PIPELINE_RUN && data.metadata.labels['{label}'] == '{escaped}'"

    async def archived_children(gateway: Gateway):
        found = []
        try:
            async for _record, run in gateway.list_records(namespace, filter_text=query):
                if (run.get("metadata", {}).get("labels") or {}).get(label) == name:
                    found.append(run_summary(run, "archive"))
        except UpstreamError as error:
            return found, {"source": "archive", "message": error.message}
        return found, None

    async def live_children(gateway: Gateway):
        found = []
        try:
            async for run in gateway.list_kube(namespace, "pipelineruns", label_selector=f"{label}={name}"):
                found.append(run_summary(run, "live"))
        except UpstreamError as error:
            return found, {"source": "live", "message": error.message}
        return found, None

    async with Gateway(user_token(request)) as gateway:
        (archived, archive_error), (live, live_error) = await asyncio.gather(
            archived_children(gateway), live_children(gateway)
        )
    items = {item["uid"]: item for item in archived + live if item["uid"]}
    children = sorted(items.values(), key=lambda item: item["created"] or "", reverse=True)
    return {"items": children, "errors": [error for error in (archive_error, live_error) if error]}


@app.get("/api/runs/{namespace}/{name}/logs")
async def run_logs(request: Request, namespace: str, name: str, uid: str | None = None):
    allowed_namespace(namespace)
    async with Gateway(user_token(request)) as gateway:
        run, source, record_name = await find_run(gateway, namespace, name, uid)
        if source == "live":
            logs, truncated = await gateway.live_logs(namespace, name)
            if logs:
                return {"source": "live", "text": logs, "truncated": truncated}
            if not run.get("status", {}).get("completionTime"):
                return {"source": "live", "text": "Logs are not available yet.", "truncated": False}
            try:
                record, _ = await gateway.archived_run(namespace, name, run["metadata"]["uid"])
                record_name = record["name"]
            except UpstreamError as error:
                if error.status == 404:
                    return {"source": "live", "text": "Logs are not available.", "truncated": False}
                raise
        if not run.get("status", {}).get("completionTime"):
            return {"source": "archive", "text": "Logs are not available yet.", "truncated": False}
        try:
            logs, truncated = await gateway.archived_logs(record_name)
        except UpstreamError as error:
            if error.status in (404, 500):
                return {"source": "archive", "text": "Archived logs are not available.", "truncated": False}
            raise
    return {"source": "archive", "text": logs, "truncated": truncated}


@app.get("/api/runs/{namespace}/{name}/logs/stream")
async def run_log_stream(request: Request, namespace: str, name: str, uid: str | None = None):
    allowed_namespace(namespace)
    token = user_token(request)
    async with Gateway(token) as gateway:
        run, source, _ = await find_run(gateway, namespace, name, uid)

    def event(kind: str, payload: dict) -> str:
        return f"event: {kind}\ndata: {json.dumps(payload)}\n\n"

    async def stream():
        if source != "live" or run.get("status", {}).get("completionTime"):
            yield event("done", {})
            return

        queue = asyncio.Queue(maxsize=100)
        watchers = {}
        seen = set()
        async with Gateway(token) as gateway:

            async def follow(key: str, title: str, pod: str, container: str):
                length = 0
                retry = False
                try:
                    async for chunk in gateway.follow_step_logs(namespace, pod, container):
                        if not chunk:
                            continue
                        encoded = chunk.encode("utf-8")
                        remaining = 8_000_000 - length
                        clipped = len(encoded) > remaining
                        if clipped:
                            chunk = encoded[:remaining].decode("utf-8", errors="ignore")
                        if chunk:
                            await queue.put(("chunk", {"key": key, "title": title, "text": chunk}))
                        length += len(chunk.encode("utf-8"))
                        if clipped:
                            await queue.put(("truncated", {}))
                            break
                except UpstreamError as error:
                    if error.status in (400, 404) and length == 0:
                        retry = True
                    else:
                        await queue.put(("failure", {"message": error.message}))
                except httpx.RequestError:
                    if length == 0:
                        retry = True
                    else:
                        await queue.put(("failure", {"message": "Log stream temporarily unavailable"}))
                finally:
                    if not asyncio.current_task().cancelling():
                        await queue.put(("closed", {"key": key, "retry": retry}))

            yield "retry: 1000\n\n"
            yield event("reset", {})
            loop = asyncio.get_running_loop()
            next_scan = 0
            current = run
            retry_stream = False
            try:
                while not await request.is_disconnected():
                    if loop.time() >= next_scan:
                        try:
                            current = await gateway.run(namespace, name)
                            if uid and current.get("metadata", {}).get("uid") != uid:
                                break
                            async for taskrun in gateway.list_kube(
                                namespace, "taskruns", label_selector=f"tekton.dev/pipelineRun={name}"
                            ):
                                metadata = taskrun.get("metadata", {})
                                owners = metadata.get("ownerReferences", [])
                                if owners and not any(
                                    owner.get("uid") == current["metadata"]["uid"] for owner in owners
                                ):
                                    continue
                                pod = taskrun.get("status", {}).get("podName")
                                if not pod:
                                    continue
                                for step in taskrun.get("status", {}).get("steps", []):
                                    container = step.get("container") or f"step-{step.get('name', '')}"
                                    key = f"{metadata['uid']}/{container}"
                                    if key not in seen:
                                        seen.add(key)
                                        title = f"## {metadata['name']} / {step.get('name', container)}"
                                        watchers[key] = asyncio.create_task(follow(key, title, pod, container))
                        except UpstreamError as error:
                            if error.status != 404:
                                yield event("failure", {"message": error.message})
                                retry_stream = error.status >= 500
                            break
                        except httpx.RequestError:
                            yield event("failure", {"message": "Cluster service is temporarily unavailable"})
                            retry_stream = True
                            break
                        next_scan = loop.time() + 2

                    try:
                        kind, payload = await asyncio.wait_for(queue.get(), timeout=max(0.01, next_scan - loop.time()))
                    except TimeoutError:
                        yield ": keepalive\n\n"
                    else:
                        if kind == "closed":
                            watchers.pop(payload["key"], None)
                            if payload["retry"]:
                                seen.discard(payload["key"])
                        else:
                            yield event(kind, payload)
                    if current.get("status", {}).get("completionTime") and not watchers and queue.empty():
                        break
            finally:
                for watcher in watchers.values():
                    watcher.cancel()
                await asyncio.gather(*watchers.values(), return_exceptions=True)
            if not retry_stream:
                yield event("done", {})

    return StreamingResponse(
        stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.get("/api/runs/{namespace}/{name}/events")
async def run_events(request: Request, namespace: str, name: str, uid: str | None = None):
    allowed_namespace(namespace)
    async with Gateway(user_token(request)) as gateway:
        run, source, _ = await find_run(gateway, namespace, name, uid)
        run_uid = run.get("metadata", {}).get("uid")
        targets = [("PipelineRun", name, run_uid)]
        if source == "live":
            tasks = [
                task
                async for task in gateway.list_kube(
                    namespace, "taskruns", label_selector=f"tekton.dev/pipelineRun={name}"
                )
            ]
            for task in tasks:
                metadata = task.get("metadata", {})
                owners = metadata.get("ownerReferences", [])
                if owners and not any(owner.get("uid") == run_uid for owner in owners):
                    continue
                targets.append(("TaskRun", metadata["name"], metadata["uid"]))
                pod_name = task.get("status", {}).get("podName")
                if pod_name:
                    try:
                        pod = await gateway.pod(namespace, pod_name)
                        targets.append(("Pod", pod_name, pod["metadata"]["uid"]))
                    except UpstreamError as error:
                        if error.status not in (403, 404):
                            raise

        async def read_events(kind: str, object_name: str, object_uid: str):
            return [
                event_summary(event)
                async for event in gateway.list_events(namespace, object_name)
                if event.get("involvedObject", {}).get("kind") == kind
                and event.get("involvedObject", {}).get("uid") == object_uid
            ]

        groups = await asyncio.gather(*(read_events(*target) for target in targets))
    items = [event for group in groups for event in group]
    items.sort(key=lambda event: (event["lastSeen"] or "", event["uid"] or ""), reverse=True)
    return {"items": items, "source": "cluster"}


@app.post("/api/runs", status_code=201)
async def create_run(request: Request, body: CreateRun):
    check_csrf(request)
    allowed_namespace(body.namespace)
    async with Gateway(user_token(request)) as gateway:
        pipeline = await gateway.pipeline(body.namespace, body.pipeline)
        if pipeline["metadata"]["resourceVersion"] != body.resourceVersion:
            raise HTTPException(status_code=409, detail="Pipeline changed; refresh the form and review its parameters")
        source = None
        if body.sourceRun:
            source, _, _ = await find_run(gateway, body.namespace, body.sourceRun.name, body.sourceRun.uid)
        try:
            manifest = build_run(pipeline, body.values, source_run=source, workspaces=body.workspaces)
        except InvalidRun as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        created = await gateway.kube_json(
            "POST", f"/apis/tekton.dev/v1/namespaces/{body.namespace}/pipelineruns", json=manifest
        )
    return run_summary(created, "live")


static_dir = Path(os.getenv("STATIC_DIR", Path(__file__).resolve().parents[2] / "frontend" / "dist"))
if static_dir.is_dir():
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="frontend")
