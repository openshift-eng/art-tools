"""Read component build results from live or archived ART PipelineRuns."""

import json
from urllib.parse import urlsplit

PREFIX = "art.openshift.io/component-build-"
REPORT = "art.openshift.io/component-report"
STATUSES = {"Pending", "Running", "Succeeded", "Failed", "Cancelled", "Unknown", "Skipped", "Reused", "DryRun"}
ACTIVE = {"Pending", "Running"}


def _json(value):
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return None


def _status(value, terminal):
    result = value if isinstance(value, str) and value in STATUSES else "Unknown"
    return "Unknown" if terminal and result in ACTIVE else result


def _url(value):
    try:
        parsed = urlsplit(value) if isinstance(value, str) else None
        return value if parsed and parsed.scheme == "https" and parsed.netloc else None
    except ValueError:
        return None


def component_details(run):
    annotations = run.get("metadata", {}).get("annotations") or {}
    marker = _json(annotations.get(REPORT))
    status = run.get("status") or {}
    terminal = bool(status.get("completionTime")) or any(
        c.get("type") == "Succeeded" and c.get("status") in ("True", "False") for c in status.get("conditions", [])
    )
    partial = bool(isinstance(marker, dict) and marker.get("partial"))
    components = []
    for key, value in annotations.items():
        if not key.startswith(PREFIX):
            continue
        entry = _json(value)
        if (
            not isinstance(entry, dict)
            or entry.get("version") != 1
            or entry.get("type") not in ("image", "bundle", "fbc")
            or not isinstance(entry.get("name"), str)
            or not isinstance(entry.get("pipelineRuns"), list)
        ):
            partial = True
            continue
        builds = []
        for build in entry["pipelineRuns"]:
            if not isinstance(build, dict) or not isinstance(build.get("name"), str):
                partial = True
                continue
            builds.append(
                {
                    "name": build["name"],
                    "namespace": build.get("namespace") if isinstance(build.get("namespace"), str) else "",
                    "url": _url(build.get("url")),
                    "status": _status(build.get("status"), terminal),
                    "attempt": build.get("attempt") if type(build.get("attempt")) is int else None,
                    "reason": str(build.get("reason") or ""),
                }
            )
        outcome = _status(entry.get("status"), terminal)
        reason = str(entry.get("reason") or "")
        if terminal and outcome == "Unknown":
            partial = True
            reason = reason or "ART run ended before a final build result was recorded"
        components.append(
            {
                "id": key,
                "type": entry["type"],
                "name": entry["name"],
                "displayName": entry.get("displayName") if isinstance(entry.get("displayName"), str) else entry["name"],
                "status": outcome,
                "reason": reason,
                "pipelineRuns": sorted(builds, key=lambda b: b["attempt"] or 0),
            }
        )
    available = REPORT in annotations or bool(components) or any(k.startswith(PREFIX) for k in annotations)
    if REPORT in annotations and (not isinstance(marker, dict) or marker.get("version") != 1):
        partial = True
    reason = str(marker.get("reason") or "") if isinstance(marker, dict) else ""
    if (
        terminal
        and available
        and not components
        and not (isinstance(marker, dict) and marker.get("state") == "skipped")
    ):
        partial = True
        reason = reason or "ART run ended before component build results were recorded"
    return {
        "components": sorted(components, key=lambda c: (c["name"], c["type"])),
        "componentReporting": {"available": available, "terminal": terminal, "partial": partial, "reason": reason},
    }
