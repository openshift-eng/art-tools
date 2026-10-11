"""Read versioned component reports from live or archived ART PipelineRuns."""

import json
from urllib.parse import urlsplit

PREFIX = "art.openshift.io/component-build-"
REPORT = "art.openshift.io/component-report"
STATUSES = {"Pending", "Running", "Succeeded", "Failed", "Cancelled", "Unknown", "Skipped", "Reused", "DryRun"}
FAILURES = {"Failed", "Cancelled"}
ACTIVE = {"Pending", "Running"}


def _json(value):
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return None


def summarize(stages):
    statuses = {s["status"] for s in stages}
    for status in ("Failed", "Cancelled", "Running", "Pending", "Unknown"):
        if status in statuses:
            return status
    if statuses == {"DryRun"}:
        return "DryRun"
    if "Reused" in statuses:
        return "Reused"
    if "Succeeded" in statuses:
        return "Succeeded"
    return "Skipped" if statuses else "—"


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
        if not isinstance(entry, dict) or entry.get("version") != 1:
            partial = True
            continue
        if (
            not isinstance(entry.get("type"), str)
            or entry.get("type") not in ("image", "bundle", "fbc")
            or not isinstance(entry.get("name"), str)
        ):
            partial = True
            continue
        if not isinstance(entry.get("stages"), dict) or not isinstance(entry.get("resources"), list):
            partial = True
            continue
        stages = []
        for name, stage in entry["stages"].items():
            if not isinstance(stage, dict) or stage.get("category") not in ("build", "check", "release", "preparation"):
                partial = True
                continue
            stage = {**stage, "name": name, "required": stage.get("required") is not False}
            native = stage.get("status")
            stage["status"] = native if isinstance(native, str) and native in STATUSES else "Unknown"
            stage["reason"] = str(stage.get("reason") or "")
            if terminal and stage["status"] in ACTIVE:
                stage.update(
                    status="Unknown", reason=stage.get("reason") or "ART run ended before a final status was recorded"
                )
                partial = True
            if stage["status"] == "Unknown" and terminal:
                partial = True
            stages.append(stage)
        resources = []
        for resource in entry["resources"]:
            if not isinstance(resource, dict) or not isinstance(resource.get("name"), str):
                partial = True
                continue
            resource = dict(resource)
            url = resource.get("url")
            try:
                valid_url = isinstance(url, str) and urlsplit(url).scheme == "https" and bool(urlsplit(url).netloc)
            except ValueError:
                valid_url = False
            if not valid_url:
                resource["url"] = None
            native = resource.get("status")
            resource["status"] = native if isinstance(native, str) and native in STATUSES else "Unknown"
            if terminal and resource["status"] in ACTIVE:
                resource["status"] = "Unknown"
            for field in ("kind", "role", "namespace", "reason"):
                resource[field] = resource.get(field) if isinstance(resource.get(field), str) else ""
            resource["attempt"] = resource.get("attempt") if isinstance(resource.get("attempt"), int) else None
            resources.append(resource)
        required = [s for s in stages if s["required"]]
        overall = "DryRun" if entry.get("dryRun") is True else summarize(required)
        build = [s for s in stages if s["category"] == "build"]
        if overall == "Succeeded" and build and summarize(build) in ("Reused", "Skipped"):
            overall = summarize(build)
        components.append(
            {
                "type": entry["type"],
                "name": entry["name"],
                "displayName": entry.get("displayName") if isinstance(entry.get("displayName"), str) else entry["name"],
                "nvr": entry.get("nvr") if isinstance(entry.get("nvr"), str) else None,
                "operatorNvr": entry.get("operatorNvr") if isinstance(entry.get("operatorNvr"), str) else None,
                "fbcExcluded": entry.get("fbcExcluded") is True,
                "members": [
                    member
                    for member in entry.get("members", [])
                    if isinstance(member, dict)
                    and isinstance(member.get("name"), str)
                    and isinstance(member.get("containerImage"), str)
                ]
                if isinstance(entry.get("members"), list)
                else [],
                "id": key,
                "stages": stages,
                "resources": resources,
                "status": overall,
                "hasFailures": any(s["status"] in FAILURES for s in stages)
                or any(r.get("status") in FAILURES for r in resources),
                "summaries": {
                    category: summarize([s for s in stages if s["category"] == category])
                    for category in ("build", "check", "release")
                },
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
        reason = reason or "ART run ended before component details were recorded"
    return {
        "components": sorted(components, key=lambda c: (c["name"], c["type"])),
        "componentReporting": {
            "available": available,
            "terminal": terminal,
            "partial": partial,
            "reason": reason,
            "complete": bool(terminal and available and not partial),
        },
    }
