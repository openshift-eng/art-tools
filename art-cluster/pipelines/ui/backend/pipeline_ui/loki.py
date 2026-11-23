"""Export the log entries retained by Loki for a PipelineRun."""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import BinaryIO
from uuid import UUID

import httpx

from .cluster import RESULTS_CA, UpstreamError

LOKI_API = os.getenv(
    "LOKI_API_URL", "https://logging-loki-gateway-http.openshift-logging.svc.cluster.local:8080/api/logs/v1/application"
).rstrip("/")
PAGE_LIMIT = 2000
NANOSECONDS = 1_000_000_000


def run_window(run: dict) -> tuple[int, int]:
    status = run.get("status", {})
    started = status.get("startTime")
    completed = status.get("completionTime")
    if not started or not completed:
        raise UpstreamError(409, "A started and completed PipelineRun is required for console export")
    start_time = datetime.fromisoformat(started)
    # Use the same forwarding margin as the deployed Tekton Results Loki queries.
    end_time = datetime.fromisoformat(completed) + timedelta(minutes=10)
    return (
        int(start_time.timestamp()) * NANOSECONDS + start_time.microsecond * 1000,
        int(end_time.timestamp()) * NANOSECONDS + end_time.microsecond * 1000,
    )


def query_for_run(namespace: str, uid: UUID) -> str:
    return (
        f'{{log_type="application", kubernetes_namespace_name={json.dumps(namespace)}}}'
        ' | json run_uid="kubernetes.labels.tekton_dev_pipelineRunUID"'
        f' | run_uid="{uid}"'
    )


def console_line(timestamp: int, line: str, labels: dict) -> bytes:
    seconds, nanos = divmod(timestamp, NANOSECONDS)
    stamp = datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        entry = json.loads(line)
    except ValueError:
        entry = {}
    if not isinstance(entry, dict):
        entry = {}
    message = entry.get("message")
    if not isinstance(message, str):
        message = line
    kubernetes = entry.get("kubernetes")
    container = kubernetes.get("container_name") if isinstance(kubernetes, dict) else None
    stream = entry.get("stream") or labels.get("stream")
    context = " ".join(
        value for value in (container or labels.get("kubernetes_container_name"), stream) if isinstance(value, str)
    )
    prefix = f"{context}: " if context else ""
    return f"{stamp}.{nanos:09d}Z {prefix}{message}\n".encode()


async def page(client: httpx.AsyncClient, query: str, start: int, end: int) -> list[tuple[int, str, dict]]:
    response = await client.get(
        "/loki/api/v1/query_range",
        params={"query": query, "start": str(start), "end": str(end), "limit": PAGE_LIMIT, "direction": "forward"},
    )
    if response.status_code in (401, 403):
        raise UpstreamError(response.status_code, "Loki application log access is required for this namespace")
    if response.is_error:
        raise UpstreamError(502, f"Loki log query failed ({response.status_code})")
    try:
        data = response.json()
        if data["status"] != "success" or data["data"]["resultType"] != "streams":
            raise ValueError("Unexpected query result")
        entries = [
            (int(value[0]), value[1], stream["stream"])
            for stream in data["data"]["result"]
            for value in stream["values"]
        ]
        if len(entries) > PAGE_LIMIT or any(
            not start <= timestamp < end or not isinstance(line, str) or not isinstance(labels, dict)
            for timestamp, line, labels in entries
        ):
            raise ValueError("Invalid entries in query result")
    except (KeyError, TypeError, ValueError, IndexError) as error:
        raise UpstreamError(502, "Loki returned an invalid log response") from error
    return sorted(entries, key=lambda entry: entry[0])


async def write_console(token: str, namespace: str, uid: UUID, run: dict, output: BinaryIO) -> None:
    start, end = run_window(run)
    query = query_for_run(namespace, uid)
    async with httpx.AsyncClient(
        base_url=LOKI_API,
        headers={"Authorization": f"Bearer {token}"},
        verify=RESULTS_CA,
        timeout=30,
    ) as client:
        while start < end:
            entries = await page(client, query, start, end)
            # A full page may split its final timestamp. Re-query that timestamp as a whole.
            boundary = entries[-1][0] if len(entries) == PAGE_LIMIT else end
            if boundary == start:
                raise UpstreamError(502, "Loki returned a full page at one timestamp. Export would be incomplete.")
            for timestamp, line, labels in entries:
                if timestamp == boundary:
                    break
                output.write(console_line(timestamp, line, labels))
            start = boundary
    if output.tell() == 0:
        raise UpstreamError(404, "No console logs for this PipelineRun are available in Loki")
