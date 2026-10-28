"""ART Pipelines UI API and static frontend."""

import asyncio
import json
import logging
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import AnyHttpUrl, BaseModel, Field, ValidationError, field_validator, model_validator

from .cluster import Gateway, UpstreamError
from .components import component_details
from .health import build_health, health_run
from .rebuild import (
    REBUILT_FROM_ANNOTATION,
    REBUILT_FROM_NAME_ANNOTATION,
    InvalidRun,
    build_run,
    parameter_form,
    pipeline_name,
)

NAMESPACES = tuple(
    namespace.strip() for namespace in os.getenv("PIPELINE_NAMESPACES", "").split(",") if namespace.strip()
)
PIPELINE_ORDER = (
    ("schedule-layered-products-scan",),
    ("layered-products-scan",),
    ("build-layered-products",),
    ("olm-bundle-konflux",),
    ("build-fbc",),
    ("release-from-fbc", "binary-release", "binary-release-konflux"),
    ("trigger-prod-release",),
)
PIPELINE_PRIORITY = {name: priority for priority, names in enumerate(PIPELINE_ORDER) for name in names}
CSRF_COOKIE = "__Host-art-pipelines-csrf"
INCIDENTS_FILE = Path(os.getenv("INCIDENTS_FILE", "/etc/incidents/incidents.json"))
MAX_INCIDENTS = 20
MAX_INCIDENT_HISTORY_ENTRIES = 50
MAX_INCIDENTS_JSON_BYTES = 900 * 1024
logger = logging.getLogger(__name__)
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


class IncidentLink(BaseModel):
    label: str = Field(min_length=1, max_length=80)
    url: AnyHttpUrl


class IncidentHistoryEntry(BaseModel):
    timestamp: str = ""
    note: str = Field(min_length=1, max_length=500)
    details: str = Field(default="", max_length=4000)


class Incident(BaseModel):
    id: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=160)
    message: str = Field(min_length=1, max_length=240)
    severity: Literal["info", "warning", "danger"] = "warning"
    status: Literal["investigating", "identified", "monitoring", "resolved", "deleted"]
    started: str | None = None
    updated: str | None = None
    resolvedAt: str | None = None
    deletedAt: str | None = None
    description: str = Field(default="", max_length=4000)
    links: list[IncidentLink] = Field(default_factory=list, max_length=10)
    showOnBanner: bool = True
    history: list[IncidentHistoryEntry] = Field(default_factory=list, max_length=MAX_INCIDENT_HISTORY_ENTRIES)

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_record(cls, value):
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        normalized.setdefault("showOnBanner", True)
        if "history" not in normalized:
            normalized["history"] = [
                {
                    "timestamp": normalized.get("started") or normalized.get("updated") or "",
                    "note": "Incident created.",
                }
            ]
        return normalized

    @field_validator("history", mode="before")
    @classmethod
    def retain_recent_history(cls, value):
        return value[-MAX_INCIDENT_HISTORY_ENTRIES:] if isinstance(value, list) else value


class IncidentCreate(BaseModel):
    title: str = Field(min_length=1, max_length=160)
    message: str = Field(min_length=1, max_length=240)
    severity: Literal["info", "warning", "danger"] = "warning"
    status: Literal["investigating", "identified", "monitoring"] = "investigating"
    description: str = Field(default="", max_length=4000)
    links: list[IncidentLink] = Field(default_factory=list, max_length=10)
    showOnBanner: bool = True


class IncidentUpdate(BaseModel):
    title: str = Field(min_length=1, max_length=160)
    message: str = Field(min_length=1, max_length=240)
    severity: Literal["info", "warning", "danger"] = "warning"
    status: Literal["investigating", "identified", "monitoring"] = "investigating"
    description: str = Field(default="", max_length=4000)
    links: list[IncidentLink] = Field(default_factory=list, max_length=10)
    showOnBanner: bool | None = None
    updateNote: str = Field(min_length=1, max_length=500)

    @field_validator("updateNote", mode="before")
    @classmethod
    def trim_update_note(cls, value):
        return value.strip() if isinstance(value, str) else value


@app.exception_handler(UpstreamError)
async def upstream_error(_request: Request, error: UpstreamError):
    return JSONResponse(
        status_code=error.status, content={"detail": error.message}, headers={"Cache-Control": "no-store"}
    )


@app.exception_handler(httpx.RequestError)
async def network_error(_request: Request, _error: httpx.RequestError):
    return JSONResponse(
        status_code=502,
        content={"detail": "Cluster service is temporarily unavailable"},
        headers={"Cache-Control": "no-store"},
    )


def user_token(request: Request) -> str:
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        raise HTTPException(
            status_code=401, detail="OpenShift login is required", headers={"Cache-Control": "no-store"}
        )
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


def pipeline_sort_key(pipeline: dict) -> tuple[str, int, str]:
    name = pipeline["name"]
    return (pipeline["namespace"], PIPELINE_PRIORITY.get(name, len(PIPELINE_ORDER)), name)


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


async def read_run_summaries(
    gateway: Gateway, namespace: str, errors: list, selected: bool, *, include_health: bool = False
) -> list[dict]:
    items = {}
    try:
        async for _record, run in gateway.list_records(namespace, filter_text="data_type == PIPELINE_RUN"):
            summary = run_summary(run, "archive")
            if include_health:
                summary = health_run(run, summary)
            if summary["uid"]:
                items[summary["uid"]] = summary
    except UpstreamError as error:
        if error.status == 401:
            raise
        if error.status != 403 or selected or include_health:
            errors.append(
                {"namespace": namespace, "source": "archive", "message": error.message, "status": error.status}
            )
    try:
        async for run in gateway.list_kube(namespace, "pipelineruns"):
            summary = run_summary(run, "live")
            if include_health:
                summary = health_run(run, summary)
            if summary["uid"]:
                items[summary["uid"]] = summary
    except UpstreamError as error:
        if error.status == 401:
            raise
        if error.status != 403 or selected or include_health:
            errors.append({"namespace": namespace, "source": "live", "message": error.message, "status": error.status})
    return list(items.values())


@app.get("/api/pipeline-health")
async def pipeline_health(request: Request, response: Response, namespace: str | None = None):
    names = selected_namespaces(namespace)
    errors = []
    async with Gateway(user_token(request)) as gateway:
        groups = await asyncio.gather(
            *(read_run_summaries(gateway, name, errors, namespace is not None, include_health=True) for name in names)
        )
    visible = {run["namespace"] for group in groups for run in group}
    errors = [error for error in errors if namespace or error["status"] != 403 or error["namespace"] in visible]
    response.headers["Cache-Control"] = "no-store"
    return {
        "items": build_health([run for group in groups for run in group], {error["namespace"] for error in errors}),
        "errors": errors,
    }


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


@app.get("/api/incidents")
async def incidents(request: Request, response: Response):
    response.headers["Cache-Control"] = "no-store"
    try:
        data = json.loads(INCIDENTS_FILE.read_text())
    except FileNotFoundError:
        data = {"incidents": []}
    except (OSError, json.JSONDecodeError) as error:
        logger.warning("Could not read incident data from %s: %s", INCIDENTS_FILE, error)
        raise HTTPException(status_code=503, detail="Incident information is temporarily unavailable") from error

    if not isinstance(data, dict) or not isinstance(data.get("incidents"), list):
        logger.warning("Incident data in %s must contain an incidents list", INCIDENTS_FILE)
        raise HTTPException(status_code=503, detail="Incident information is temporarily unavailable")

    items = []
    for entry in data["incidents"]:
        try:
            items.append(Incident.model_validate(entry).model_dump(mode="json"))
        except (ValidationError, TypeError) as error:
            logger.warning("Ignoring invalid incident entry in %s: %s", INCIDENTS_FILE, error)
    async with Gateway(user_token(request)) as gateway:
        can_manage = await can_manage_incidents(gateway)
        if can_manage:
            _configmap, current_items = await read_incident_configmap(gateway)
            items = [item.model_dump(mode="json") for item in current_items]
    return {"incidents": items, "canManage": can_manage}


INCIDENT_CONFIGMAP_PATH = "/api/v1/namespaces/art-pipelines-ui/configmaps/art-pipelines-incidents"


def incident_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def append_incident_history(history: list[IncidentHistoryEntry], entry: IncidentHistoryEntry):
    return [*history, entry][-MAX_INCIDENT_HISTORY_ENTRIES:]


async def can_manage_incidents(gateway: Gateway) -> bool:
    try:
        review = await gateway.kube_json(
            "POST",
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            json={
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "spec": {
                    "resourceAttributes": {
                        "namespace": "art-pipelines-ui",
                        "verb": "patch",
                        "resource": "configmaps",
                        "name": "art-pipelines-incidents",
                    }
                },
            },
        )
        return bool(review.get("status", {}).get("allowed"))
    except UpstreamError as error:
        if error.status != 403:
            logger.warning("Could not check incident management permission: %s", error.message)
        return False


async def read_incident_configmap(gateway: Gateway):
    configmap = await gateway.kube_json("GET", INCIDENT_CONFIGMAP_PATH)
    try:
        content = json.loads(configmap.get("data", {}).get("incidents.json", '{"incidents":[]}'))
        if not isinstance(content, dict) or not isinstance(content.get("incidents"), list):
            raise ValueError("incidents.json must contain an incidents list")
        items = [Incident.model_validate(item) for item in content["incidents"]]
    except (json.JSONDecodeError, TypeError, ValueError, ValidationError) as error:
        logger.warning("Cannot update malformed incident ConfigMap: %s", error)
        raise HTTPException(
            status_code=503, detail="Incident data must be corrected before it can be edited"
        ) from error
    return configmap, items


async def write_incident_configmap(gateway: Gateway, configmap: dict, items: list[Incident]):
    while len(items) > MAX_INCIDENTS:
        removable = [
            (index, item.deletedAt or item.resolvedAt or item.updated or item.started or "")
            for index, item in enumerate(items)
            if item.status in ("deleted", "resolved")
        ]
        if not removable:
            raise HTTPException(
                status_code=409,
                detail=f"There are already {MAX_INCIDENTS} active incidents. Resolve an incident before creating another.",
            )
        deleted_items = [entry for entry in removable if items[entry[0]].status == "deleted"]
        oldest_index = min(deleted_items or removable, key=lambda entry: entry[1])[0]
        items.pop(oldest_index)

    serialized = [item.model_dump(mode="json") for item in items]
    incidents_json = json.dumps({"incidents": serialized}, indent=2, ensure_ascii=False)
    if len(incidents_json.encode("utf-8")) > MAX_INCIDENTS_JSON_BYTES:
        raise HTTPException(
            status_code=413,
            detail="Incident history is too large to save. Remove old resolved incidents or shorten their details.",
        )
    await gateway.kube_json(
        "PATCH",
        INCIDENT_CONFIGMAP_PATH,
        headers={"Content-Type": "application/merge-patch+json"},
        json={
            "metadata": {"resourceVersion": configmap["metadata"]["resourceVersion"]},
            "data": {"incidents.json": incidents_json},
        },
    )
    return {"incidents": serialized, "canManage": True}


@app.post("/api/incidents", status_code=201)
async def create_incident(request: Request, body: IncidentCreate):
    check_csrf(request)
    async with Gateway(user_token(request)) as gateway:
        configmap, items = await read_incident_configmap(gateway)
        now = incident_timestamp()
        items.append(
            Incident(
                id=str(uuid4()),
                started=now,
                updated=now,
                history=[IncidentHistoryEntry(timestamp=now, note="Incident created.", details=body.description)],
                **body.model_dump(),
            )
        )
        return await write_incident_configmap(gateway, configmap, items)


@app.put("/api/incidents/{incident_id}")
async def update_incident(request: Request, incident_id: str, body: IncidentUpdate):
    check_csrf(request)
    async with Gateway(user_token(request)) as gateway:
        configmap, items = await read_incident_configmap(gateway)
        for index, incident in enumerate(items):
            if incident.id == incident_id:
                now = incident_timestamp()
                changes = body.model_dump(exclude={"showOnBanner", "updateNote"})
                changes["links"] = body.links
                changes.update(
                    {
                        "showOnBanner": incident.showOnBanner if body.showOnBanner is None else body.showOnBanner,
                        "history": append_incident_history(
                            incident.history,
                            IncidentHistoryEntry(timestamp=now, note=body.updateNote, details=body.description),
                        ),
                        "updated": now,
                        "resolvedAt": None,
                    }
                )
                updated = incident.model_copy(update=changes)
                items[index] = updated
                return await write_incident_configmap(gateway, configmap, items)
    raise HTTPException(status_code=404, detail="Incident was not found")


@app.post("/api/incidents/{incident_id}/close")
async def close_incident(request: Request, incident_id: str):
    check_csrf(request)
    async with Gateway(user_token(request)) as gateway:
        configmap, items = await read_incident_configmap(gateway)
        for index, incident in enumerate(items):
            if incident.id == incident_id:
                if incident.status != "resolved":
                    now = incident_timestamp()
                    items[index] = incident.model_copy(
                        update={
                            "status": "resolved",
                            "updated": now,
                            "resolvedAt": now,
                            "history": append_incident_history(
                                incident.history, IncidentHistoryEntry(timestamp=now, note="Incident closed.")
                            ),
                        }
                    )
                return await write_incident_configmap(gateway, configmap, items)
    raise HTTPException(status_code=404, detail="Incident was not found")


@app.delete("/api/incidents/{incident_id}")
async def delete_incident(request: Request, incident_id: str):
    check_csrf(request)
    async with Gateway(user_token(request)) as gateway:
        configmap, items = await read_incident_configmap(gateway)
        for index, incident in enumerate(items):
            if incident.id == incident_id:
                if incident.status != "deleted":
                    now = incident_timestamp()
                    items[index] = incident.model_copy(
                        update={
                            "status": "deleted",
                            "updated": now,
                            "deletedAt": now,
                            "history": append_incident_history(
                                incident.history, IncidentHistoryEntry(timestamp=now, note="Incident marked deleted.")
                            ),
                        }
                    )
                return await write_incident_configmap(gateway, configmap, items)
    raise HTTPException(status_code=404, detail="Incident was not found")


@app.get("/api/session")
async def session(request: Request, response: Response):
    async with Gateway(user_token(request)) as gateway:
        user = await gateway.kube_json("GET", "/apis/user.openshift.io/v1/users/~")
    csrf = request.cookies.get(CSRF_COOKIE) or secrets.token_urlsafe(32)
    response.headers["Cache-Control"] = "no-store"
    response.set_cookie(CSRF_COOKIE, csrf, secure=True, httponly=False, samesite="strict", path="/")
    return {"csrfToken": csrf, "user": user["metadata"]["name"]}


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
    return {"items": sorted(items, key=pipeline_sort_key)}


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
    annotations = metadata.get("annotations") or {}
    rebuilt_from_name = annotations.get(REBUILT_FROM_NAME_ANNOTATION)
    rebuilt_from_uid = annotations.get(REBUILT_FROM_ANNOTATION)
    return {
        **run_summary(run, source),
        **component_details(run),
        "labels": labels,
        "annotations": annotations,
        "parentPipelineRun": labels.get("art.openshift.io/parent-pipelinerun"),
        "rebuiltFrom": (
            {"name": rebuilt_from_name, "uid": rebuilt_from_uid} if rebuilt_from_name and rebuilt_from_uid else None
        ),
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
            if error.status == 401:
                raise
            return found, {"source": "archive", "message": error.message}
        return found, None

    async def live_children(gateway: Gateway):
        found = []
        try:
            async for run in gateway.list_kube(namespace, "pipelineruns", label_selector=f"{label}={name}"):
                found.append(run_summary(run, "live"))
        except UpstreamError as error:
            if error.status == 401:
                raise
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
                return {"source": "archive", "text": "Logs are not available.", "truncated": False}
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
                        await queue.put(("failure", {"status": error.status, "message": error.message}))
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
                                yield event("failure", {"status": error.status, "message": error.message})
                                if error.status == 401:
                                    return
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
                            if kind == "failure" and payload.get("status") == 401:
                                return
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
