"""Read component build results from live or archived ART PipelineRuns."""

import json
from urllib.parse import urlsplit

PREFIX = "art.openshift.io/component-build-"
REPORT = "art.openshift.io/component-report"
STATUSES = {"Pending", "Running", "Succeeded", "Failed", "Cancelled", "Unknown", "Skipped", "Reused", "DryRun"}
ACTIVE = {"Pending", "Running"}


def _json(value):
    """Decode an annotation without letting invalid JSON break run details."""
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return None


def _status(value, terminal):
    """Normalize build outcomes and mark unfinished terminal-run reports unknown."""
    result = value if isinstance(value, str) and value in STATUSES else "Unknown"
    return "Unknown" if terminal and result in ACTIVE else result


def _url(value):
    """Return only HTTPS links suitable for opening from the component table."""
    try:
        parsed = urlsplit(value) if isinstance(value, str) else None
        return value if parsed and parsed.scheme == "https" and parsed.netloc else None
    except ValueError:
        return None


def _checks(value, terminal):
    """Normalize checks and distinguish report gaps from blocking uncertainty."""
    if not isinstance(value, list):
        return [], True, True
    checks = []
    partial = False
    blocking_incomplete = False
    for check in value:
        if (
            not isinstance(check, dict)
            or check.get('kind') not in ('conforma', 'custom')
            or not isinstance(check.get('name'), str)
            or type(check.get('blocking')) is not bool
        ):
            partial = True
            blocking_incomplete |= not isinstance(check, dict) or check.get('blocking') is not False
            continue
        status = _status(check.get('status'), terminal)
        if status not in {'Pending', 'Running', 'Succeeded', 'Failed', 'Cancelled', 'Skipped', 'DryRun'}:
            status = 'Unknown'
            partial = True
        checks.append(
            {
                'kind': check['kind'],
                'name': check['name'],
                'blocking': check['blocking'],
                'status': status,
                'url': _url(check.get('url')),
                'reason': str(
                    check.get('reason')
                    or ('ART run ended before a final check result was recorded' if status == 'Unknown' else '')
                ),
            }
        )
    return checks, partial, blocking_incomplete


def _result_status(status, checks):
    """Combine the build and blocking checks while preserving optional failures."""
    if status != 'Succeeded':
        return status
    blocking = {check['status'] for check in checks if check['blocking']}
    if blocking & {'Failed', 'Cancelled'}:
        return 'Failed'
    if blocking & {'Unknown', 'Skipped'}:
        return 'Unknown'
    if blocking & ACTIVE:
        return 'Running'
    return status


def component_details(run):
    """Read versioned component reports from a live or archived ART run."""
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
            checks, check_partial, _ = _checks(build.get('checks', []), terminal)
            partial |= check_partial
            builds.append(
                {
                    "name": build["name"],
                    "namespace": build.get("namespace") if isinstance(build.get("namespace"), str) else "",
                    "url": _url(build.get("url")),
                    "status": _status(build.get("status"), terminal),
                    "attempt": build.get("attempt") if type(build.get("attempt")) is int else None,
                    "reason": str(build.get("reason") or ""),
                    "checks": checks,
                }
            )
        outcome = _status(entry.get("status"), terminal)
        checks, check_partial, blocking_incomplete = _checks(entry.get('checks', []), terminal)
        partial |= check_partial
        result_status = _result_status(outcome, checks)
        if blocking_incomplete and result_status in {'Succeeded', 'Running'}:
            result_status = 'Unknown'
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
                "resultStatus": result_status,
                "checks": checks,
                "checksRecorded": 'checks' in entry,
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
