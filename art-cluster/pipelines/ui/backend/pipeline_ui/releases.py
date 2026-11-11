"""Observe release preparation and shipment CI without changing either system."""

import asyncio
import base64
import os
import re
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, quote, urlsplit

import yaml
import httpx

from .cluster import UpstreamError
from .gitlab_read import GitLabReadError

SHIPMENT_PATH = re.compile(r"^shipment/([^/]+)/([^/]+)/([^/]+)/(stage|prod)/([^/]+\.ya?ml)$")
ACTIVE = {"created", "waiting_for_resource", "preparing", "pending", "running", "scheduled", "canceling"}
RUN_HOSTS = {host.strip() for host in os.getenv(
    "RELEASES_PIPELINE_UI_HOSTS",
    "art-pipelines-ui-art-pipelines-ui.apps.artc2023.pc3z.p1.openshiftapps.com",
).split(",") if host.strip()}
_shipment_cache = OrderedDict()


def ci_status(value):
    if value in ACTIVE:
        return "Running"
    return {"success": "Succeeded", "failed": "Failed", "canceled": "Cancelled", "skipped": "Skipped", "manual": "Manual action"}.get(value, "Unknown")


def preparation_run(raw):
    metadata = raw.get("metadata") or {}
    spec = raw.get("spec") or {}
    if spec.get("pipelineRef", {}).get("name") != "release-from-fbc":
        return None
    params = {item["name"]: item.get("value") for item in spec.get("params", [])}
    group, assembly = params.get("group", ""), params.get("assembly", "")
    if not isinstance(group, str) or not group or not isinstance(assembly, str):
        return None
    condition = next((c for c in raw.get("status", {}).get("conditions", []) if c.get("type") == "Succeeded"), {})
    status = {"True": "Succeeded", "False": "Failed", "Unknown": "Running"}.get(condition.get("status"), "Unknown")
    if "cancel" in condition.get("reason", "").lower():
        status = "Cancelled"
    return {
        "namespace": metadata.get("namespace"), "name": metadata.get("name"), "uid": metadata.get("uid"),
        "group": group, "assembly": assembly, "status": status,
        "created": metadata.get("creationTimestamp"), "started": raw.get("status", {}).get("startTime"),
        "completed": raw.get("status", {}).get("completionTime"),
        "rebuiltFrom": metadata.get("annotations", {}).get("art.openshift.io/rebuilt-from"),
        "dryRun": str(params.get("dry-run", "false")).lower() == "true",
        "testMode": str(params.get("test-mode", "false")).lower() == "true",
    }


async def preparation_runs(gateway, errors, namespace):
    found = {}
    for source in ("archive", "live"):
        try:
            if source == "archive":
                values = gateway.list_records(namespace, filter_text="data_type == PIPELINE_RUN && data.spec.pipelineRef.name == 'release-from-fbc'")
                async for _record, raw in values:
                    run = preparation_run(raw)
                    if run and run["uid"] and run["namespace"] == namespace:
                        found[run["uid"]] = {**run, "source": source}
            else:
                async for raw in gateway.list_kube(namespace, "pipelineruns", label_selector="tekton.dev/pipeline=release-from-fbc"):
                    run = preparation_run(raw)
                    if run and run["uid"] and run["namespace"] == namespace:
                        found[run["uid"]] = {**run, "source": source}
        except UpstreamError as error:
            if error.status == 401:
                raise
            errors.append(f"{namespace}: preparation {source} history is unavailable")
        except (httpx.RequestError, OSError, ValueError):
            errors.append(f"{namespace}: preparation {source} history is unavailable")
    return sorted(found.values(), key=lambda run: (run["created"] or "", run["uid"]), reverse=True)


def creator_run(description):
    match = re.search(r"^Created by job:\s*(https://\S+)", description or "", re.MULTILINE)
    if not match:
        return None
    parsed = urlsplit(match[1])
    if parsed.hostname not in RUN_HOSTS or parsed.username:
        return None
    path, _, query = parsed.fragment.partition("?")
    parts = path.strip("/").split("/")
    if len(parts) != 3 or parts[0] != "run" or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", parts[1]):
        return None
    return {"namespace": parts[1], "name": parts[2], "uid": parse_qs(query).get("uid", [None])[0]}


def attach_preparation(releases, runs, *, include_orphans=True):
    used = set()
    claims = {}
    candidates_by_release = {}
    for release in releases:
        link = release.pop("creator", None)
        candidates = [run for run in runs if link and run["name"] == link["name"]
                      and (not link["uid"] or run["uid"] == link["uid"])
                      and run["namespace"] == release["namespace"] == link["namespace"]
                      and (run["group"], run["assembly"]) == (release["group"], release["assembly"])
                      and not run["dryRun"]]
        candidates_by_release[id(release)] = (link, candidates)
    creator_uids = {candidates[0]["uid"] for _link, candidates in candidates_by_release.values() if len(candidates) == 1}
    for release in releases:
        link, candidates = candidates_by_release[id(release)]
        release["preparation"] = {"status": "Unknown", "attempts": [], "message": "No explicit preparation link"}
        if len(candidates) != 1:
            if link:
                release["preparation"]["message"] = "Preparation run is missing or its name is ambiguous"
            continue
        root = candidates[0]
        family = {root["uid"]: root}
        changed = True
        while changed:
            changed = False
            for run in runs:
                if run["uid"] in family or (run["namespace"], run["group"], run["assembly"]) != (root["namespace"], root["group"], root["assembly"]) or run["dryRun"]:
                    continue
                # A retry that explicitly created another MR starts a separate release history.
                if run["uid"] in creator_uids:
                    continue
                if run.get("rebuiltFrom") in family or any(r.get("rebuiltFrom") == run["uid"] for r in family.values()):
                    family[run["uid"]] = run
                    changed = True
        attempts = sorted(family.values(), key=lambda r: (r["created"] or "", r["uid"]), reverse=True)
        release["preparation"] = {"status": attempts[0]["status"], "attempts": attempts, "message": "Linked by MR creator URL and explicit retry references"}
        used.update(family)
        claims.setdefault(root["uid"], []).append(release)
    for claimed in claims.values():
        if len({release['mr']['iid'] for release in claimed}) > 1:
            for release in claimed:
                release["preparation"] = {"status": "Unknown", "attempts": [], "message": "Preparation is referenced by multiple MRs; association is ambiguous"}
    if include_orphans:
        remaining = [run for run in runs if run["uid"] not in used]
        # Keep unrelated attempts separate. Only explicit retry UIDs join attempts.
        while remaining:
            latest = remaining.pop(0)
            family = [latest]
            changed = True
            while changed:
                changed = False
                uids = {r["uid"] for r in family}
                parents = {r.get("rebuiltFrom") for r in family}
                for run in list(remaining):
                    if (run["namespace"], run["group"], run["assembly"]) == (latest["namespace"], latest["group"], latest["assembly"]) and (run["uid"] in parents or run.get("rebuiltFrom") in uids):
                        family.append(run)
                        remaining.remove(run)
                        changed = True
            releases.append({
                "id": f"run:{latest['uid']}", "namespace": latest["namespace"], "group": latest["group"], "assembly": latest["assembly"],
                "updated": latest["created"], "mr": None, "shipments": [], "issues": [],
                "preparation": {"status": latest["status"], "attempts": family, "message": "No shipment MR linked to this preparation attempt"},
                "ci": None, "testMode": latest["testMode"], "dryRun": latest["dryRun"],
            })
    return sorted(releases, key=lambda release: release.get("updated") or "", reverse=True)


async def release_catalog(gateway, namespaces, errors, *, selected=False):
    """Derive capabilities and group ownership from viewer-readable tenant pipelines."""
    semaphore = asyncio.Semaphore(4)

    async def load(namespace):
        async with semaphore:
            try:
                pipeline = await gateway.pipeline(namespace, "release-from-fbc")
            except UpstreamError as error:
                if error.status == 404 or (error.status == 403 and not selected):
                    return None
                if error.status in (401, 403):
                    raise
                errors.append(f"{namespace}: release pipeline is unavailable")
                return None
            except (httpx.RequestError, OSError, ValueError):
                errors.append(f"{namespace}: release pipeline is unavailable")
                return None
            parameter = next((p for p in pipeline.get("spec", {}).get("params", []) if p.get("name") == "group"), {})
            groups = {g for g in [parameter.get("default"), *parameter.get("enum", [])] if isinstance(g, str) and g}
            runs = await preparation_runs(gateway, errors, namespace)
            return {"namespace": namespace, "groups": groups, "runs": runs}

    return [tenant for tenant in await asyncio.gather(*(load(ns) for ns in namespaces)) if tenant]


def group_owners(catalog):
    owners = {}
    for tenant in catalog:
        for group in tenant["groups"]:
            owners.setdefault(group, set()).add(tenant["namespace"])
    return owners


def scoped_shipments(records, owners, creator=None):
    """Do not expose a mixed or unowned MR through a shared service credential."""
    scoped = []
    for record in records:
        namespaces = owners.get(record["group"], set())
        if creator:
            namespace = creator.get("namespace") if creator.get("namespace") in namespaces else None
        else:
            namespace = next(iter(namespaces)) if len(namespaces) == 1 else None
        if not namespace:
            return []
        scoped.append({**record, "namespace": namespace})
    return scoped


async def pipeline_is_current(reader, mr, pipeline):
    if not pipeline or not pipeline.get("sha") or not mr.get("sha"):
        return False
    if pipeline["sha"] == mr["sha"]:
        return True
    # Merged-results pipelines use a synthetic commit, whose source parent must match.
    if pipeline.get("ref") != f"refs/merge-requests/{mr['iid']}/merge":
        return False
    project = pipeline.get("project_id", mr["project_id"])
    commit = await reader.object(f"/projects/{int(project)}/repository/commits/{quote(pipeline['sha'], safe='')}")
    return mr["sha"] in commit.get("parent_ids", [])


async def shipment_records(reader, mr, owners):
    cache_key = (*reader.cache_key, mr["iid"], mr.get("source_project_id"), mr.get("sha"))
    cached = _shipment_cache.get(cache_key)
    if cached and time.monotonic() - cached[0] < 300:
        return cached[1] if all(record["group"] in owners for record in cached[1]) else []
    path = f"{reader.project_path}/merge_requests/{int(mr['iid'])}"
    diffs = await reader.all(f"{path}/diffs")
    files = [d["new_path"] for d in diffs if not d.get("deleted_file") and SHIPMENT_PATH.fullmatch(d.get("new_path", ""))]
    if any(SHIPMENT_PATH.fullmatch(filename).group(2) not in owners for filename in files):
        return []
    if len(files) > 50:
        raise GitLabReadError("Shipment has more than 50 files; evidence is incomplete")
    records = []
    for filename in files:
        match = SHIPMENT_PATH.fullmatch(filename)
        product, group, application, _env, basename = match.groups()
        file = await reader.object(f"/projects/{int(mr['source_project_id'])}/repository/files/{quote(filename, safe='')}?ref={quote(mr['sha'], safe='')}")
        try:
            raw = base64.b64decode(file["content"], validate=True)
            if len(raw) > 2_000_000:
                raise ValueError("oversize shipment")
            shipment = yaml.safe_load(raw)["shipment"]
            metadata = shipment["metadata"]
            if (metadata["product"], metadata["group"], metadata["application"]) != (product, group, application):
                raise ValueError("path metadata mismatch")
            if not basename.startswith(str(metadata["assembly"]) + "."):
                raise ValueError("assembly path mismatch")
            records.append({
                "path": filename, "product": product, "group": group, "assembly": str(metadata["assembly"]),
                "application": application, "kind": "fbc" if metadata.get("fbc") else ("metadata" if ".metadata." in basename else "image"),
                "environments": {env: {
                    "releasePlan": (shipment.get("environments", {}).get(env) or {}).get("releasePlan"),
                    "pipelineUrl": reader.safe_url(((shipment.get("environments", {}).get(env) or {}).get("result") or {}).get("pipeline")),
                    "advisoryUrl": safe_https(((shipment.get("environments", {}).get(env) or {}).get("advisory") or {}).get("url")),
                } for env in ("stage", "prod")},
            })
        except (KeyError, TypeError, ValueError, yaml.YAMLError) as error:
            raise GitLabReadError("Shipment file could not be verified against its path metadata") from error
    _shipment_cache[cache_key] = (time.monotonic(), records)
    _shipment_cache.move_to_end(cache_key)
    while len(_shipment_cache) > 512:
        _shipment_cache.popitem(last=False)
    return records


def safe_https(value):
    if not isinstance(value, str):
        return None
    parsed = urlsplit(value)
    return value if parsed.scheme == "https" and parsed.netloc and not parsed.username else None


def mr_view(reader, mr):
    return {
        "iid": mr["iid"], "projectId": mr["project_id"], "url": reader.safe_url(mr.get("web_url")),
        "title": mr.get("title", ""), "state": mr.get("state", "unknown"), "draft": mr.get("draft", False),
        "sha": mr.get("sha"), "labels": mr.get("labels", []), "mergeStatus": mr.get("detailed_merge_status"),
    }


async def discover_releases(reader, days, errors, owners):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    mrs = {}
    for params in ({"state": "opened"}, {"state": "all", "updated_after": cutoff}):
        data, truncated = await reader.pages(f"{reader.project_path}/merge_requests", {
            **params, "scope": "all", "target_branch": "main", "order_by": "updated_at", "sort": "desc",
        }, max_pages=3)
        mrs.update((mr["iid"], mr) for mr in data)
        if truncated:
            errors.append("MR discovery is limited to 300 records per query; some history may be missing")
    semaphore = asyncio.Semaphore(6)

    async def load(summary):
        async with semaphore:
            try:
                # Verified paths and viewer-readable pipeline groups establish tenant membership.
                records = await shipment_records(reader, summary, owners)
                if not records or not all(record["group"] in owners for record in records):
                    return []
                mr = await reader.object(f"{reader.project_path}/merge_requests/{int(summary['iid'])}")
                if mr.get("sha") != summary.get("sha"):
                    records = await shipment_records(reader, mr, owners)
                creator = creator_run(mr.get("description"))
                records = scoped_shipments(records, owners, creator)
                if not records:
                    return []
                head = mr.get("head_pipeline")
                current = await pipeline_is_current(reader, mr, head) if head else False
                identities = sorted({(file["namespace"], file["group"], file["assembly"]) for file in records})
                return [{
                    "id": f"mr:{mr['project_id']}:{mr['iid']}:{namespace}:{group}:{assembly}", "namespace": namespace,
                    "group": group, "assembly": assembly, "updated": mr.get("updated_at"), "mr": mr_view(reader, mr),
                    "creator": creator,
                    "shipments": [r for r in records if (r["namespace"], r["group"], r["assembly"]) == (namespace, group, assembly)],
                    "ci": {"status": ci_status(head.get("status")), "current": current, "url": reader.safe_url(head.get("web_url")), "id": head.get("id")} if head else None,
                    "testMode": "<!-- release-from-fbc-test-mode -->" in (mr.get("description") or ""), "dryRun": False, "issues": [],
                } for namespace, group, assembly in identities]
            except GitLabReadError as error:
                errors.append(f"MR !{summary['iid']}: {error}")
                return []
    groups = await asyncio.gather(*(load(mr) for mr in mrs.values()))
    return [release for group in groups for release in group]


def job_view(reader, job, environment=None):
    return {"id": job.get("id"), "name": job.get("name", "Unknown job"), "stage": job.get("stage"),
            "status": ci_status(job.get("status")), "url": reader.safe_url(job.get("web_url")),
            "allowFailure": job.get("allow_failure", False), "environment": environment,
            "started": job.get("started_at"), "completed": job.get("finished_at")}


def effective_jobs(jobs):
    latest = {}
    for job in sorted(jobs, key=lambda j: j.get("id") or 0):
        latest[(job.get("environment"), job["name"])] = job
    return list(latest.values())


def aggregate_jobs(jobs):
    if not jobs:
        return "Unknown"
    required = [j for j in effective_jobs(jobs) if not j.get("allowFailure")]
    if not required:
        return "Optional"
    for status in ("Failed", "Cancelled", "Running", "Manual action", "Unknown"):
        if any(j["status"] == status for j in required):
            return status
    return "Succeeded" if all(j["status"] == "Succeeded" for j in required) else "Skipped"


async def release_detail(reader, iid, expected_sha, owners):
    path = f"{reader.project_path}/merge_requests/{iid}"
    mr = await reader.object(path)
    records = scoped_shipments(await shipment_records(reader, mr, owners), owners, creator_run(mr.get("description")))
    if not records or mr.get("target_branch") != "main":
        raise UpstreamError(404, "Shipment MR was not found in an accessible release tenant")
    issues, jobs, stages = [], [], []
    approvals = {"status": "Unknown", "rules": []}
    try:
        state = await reader.object(f"{path}/approval_state")
        if not isinstance(state.get("rules"), list):
            raise GitLabReadError("GitLab approval rules are unavailable")
        rules = [{"name": rule.get("name", "Unnamed rule"), "required": rule.get("approvals_required", 0),
                  "approved": rule.get("approved") is True,
                  "approvedBy": [user.get("username") for user in rule.get("approved_by", []) if user.get("username")]}
                 for rule in state["rules"]]
        approvals = {"status": "Waiting" if any(rule["required"] and not rule["approved"] for rule in rules) else "Succeeded", "rules": rules}
    except GitLabReadError as error:
        issues.append(str(error))
    head = mr.get("head_pipeline")
    current = False
    pipelines = []
    try:
        pipelines = await reader.all(f"{path}/pipelines")
        if head:
            current = await pipeline_is_current(reader, mr, head)
            prefix = f"/projects/{int(head.get('project_id', mr['project_id']))}/pipelines/{int(head['id'])}"
            parent_jobs = [job_view(reader, job) for job in await reader.all(f"{prefix}/jobs", {"include_retried": "true"})]
            jobs.extend(parent_jobs)
            stages.append({"name": "Validation", "status": aggregate_jobs([j for j in parent_jobs if j["name"] in {"validate-job", "generate-config-job"}])})
            bridges = effective_jobs([job_view(reader, bridge) | {"downstream": bridge.get("downstream_pipeline")} for bridge in await reader.all(f"{prefix}/bridges")])
            for name, env, label in (("stage-job", "stage", "Stage release"), ("prod-job", "prod", "Production")):
                bridge = next((b for b in bridges if b["name"] == name), None)
                env_jobs = []
                status = bridge["status"] if bridge else "Not started"
                downstream = bridge.get("downstream") if bridge else None
                if downstream:
                    child_project = int(downstream.get("project_id", mr["project_id"]))
                    if child_project != mr["project_id"]:
                        raise GitLabReadError("Unexpected downstream project; child evidence was not loaded")
                    child = f"/projects/{child_project}/pipelines/{int(downstream['id'])}"
                    env_jobs = [job_view(reader, job, env) for job in await reader.all(f"{child}/jobs", {"include_retried": "true"})]
                    jobs.extend(env_jobs)
                    # Include both the bridge and child jobs so missing or failed watch work cannot be green.
                    status = aggregate_jobs([bridge, *env_jobs]) if env_jobs else "Unknown"
                elif status == "Succeeded":
                    status = "Unknown"
                    issues.append(f"{label} bridge succeeded but its downstream evidence is missing")
                stages.append({"name": label, "status": status, "url": reader.safe_url((downstream or {}).get("web_url")), "jobCount": len(effective_jobs(env_jobs))})
        else:
            stages = [{"name": name, "status": "Not started"} for name in ("Validation", "Stage release", "Production")]
    except GitLabReadError as error:
        issues.append(str(error))
    if mr.get("sha") != expected_sha:
        issues.append("The MR revision changed since the list was loaded; refresh the release list")
    if head and not current:
        issues.append("The latest pipeline is not verified against the current source revision; job evidence is historical")
    # Detect changes during the multi-request read; do not present an old snapshot as current.
    latest = await reader.object(path, fresh=True)
    if latest.get("sha") != mr.get("sha"):
        current = False
        issues.append("The MR changed while status was loading; refresh to read the new revision")
    stages = [{"name": name, "status": "Unknown", **next((s for s in stages if s["name"] == name), {})}
              for name in ("Validation", "Stage release", "Production")]
    if head and not current:
        stages = [{**stage, "observedStatus": stage["status"], "status": "Historical"} for stage in stages]
    return {"mr": mr_view(reader, mr), "shipments": records, "stages": stages, "approvals": approvals,
            "jobs": jobs, "current": current, "issues": issues,
            "pipelines": [{"id": p.get("id"), "status": ci_status(p.get("status")), "sha": p.get("sha"), "url": reader.safe_url(p.get("web_url")), "created": p.get("created_at")} for p in pipelines]}
