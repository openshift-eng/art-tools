import os
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from kubernetes import client, config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ART Pipeline Dashboard")

CONSOLE_BASE = "https://console-openshift-console.apps.artc2023.pc3z.p1.openshiftapps.com"
NAMESPACE = os.environ.get("WATCH_NAMESPACE", "art-cd")
TEMPLATE_PATH = Path(__file__).parent / "templates" / "index.html"


def get_k8s_client():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.CustomObjectsApi()


def parse_time(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def get_status(pr):
    conditions = pr.get("status", {}).get("conditions", [])
    for c in conditions:
        if c.get("type") == "Succeeded":
            if c.get("status") == "True":
                return "Succeeded"
            if c.get("status") == "False":
                return "Cancelled" if c.get("reason") == "Cancelled" else "Failed"
            return "Running"
    return "Pending"


def duration_str(start, end):
    if not start:
        return ""
    s = parse_time(start)
    e = parse_time(end) if end else datetime.now(timezone.utc)
    if not s or not e:
        return ""
    total = int((e - s).total_seconds())
    if total < 0:
        return ""
    h, rem = divmod(total, 3600)
    m, sec = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m}m"
    if m > 0:
        return f"{m}m{sec}s"
    return f"{sec}s"


def build_node(pr):
    meta = pr.get("metadata", {})
    name = meta.get("name", "")
    labels = meta.get("labels", {})
    annotations = meta.get("annotations", {})
    start_time = pr.get("status", {}).get("startTime") or meta.get("creationTimestamp")
    pipeline = labels.get("tekton.dev/pipeline", "unknown")
    status = get_status(pr)
    completion = pr.get("status", {}).get("completionTime")

    triggered = {}
    for k, v in annotations.items():
        if k.startswith("art.openshift.io/triggered-"):
            triggered[k.replace("art.openshift.io/triggered-", "")] = v

    return {
        "name": name,
        "pipeline": pipeline,
        "status": status,
        "startTime": start_time or "",
        "completionTime": completion or "",
        "duration": duration_str(start_time, completion),
        "consoleUrl": f"{CONSOLE_BASE}/k8s/ns/{NAMESPACE}/tekton.dev~v1~PipelineRun/{name}",
        "parentPipelineRun": labels.get("art.openshift.io/parent-pipelinerun", ""),
        "parentPipeline": labels.get("art.openshift.io/parent-pipeline", ""),
        "triggered": triggered,
    }


def build_chain(root_name, nodes, edges):
    visited = set()
    queue = [root_name]
    chain_nodes, chain_edges = [], []
    while queue:
        current = queue.pop(0)
        if current in visited or current not in nodes:
            continue
        visited.add(current)
        chain_nodes.append(nodes[current])
        for e in edges:
            if e["from"] == current:
                chain_edges.append(e)
                queue.append(e["to"])
            if e["to"] == current:
                queue.append(e["from"])
    return {"root": root_name, "nodes": chain_nodes, "edges": chain_edges}


@app.get("/api/pipelines")
def get_pipelines(
    hours: int = Query(default=24, ge=1, le=168),
    search: str = Query(default=""),
    status: str = Query(default=""),
    limit: int = Query(default=5, ge=1, le=50),
):
    api = get_k8s_client()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    result = api.list_namespaced_custom_object(
        group="tekton.dev", version="v1", namespace=NAMESPACE, plural="pipelineruns"
    )

    nodes = {}
    for pr in result.get("items", []):
        node = build_node(pr)
        st = parse_time(node["startTime"])
        if st and st < cutoff:
            continue
        if search and search.lower() not in node["name"].lower():
            continue
        if status and node["status"] != status:
            continue
        nodes[node["name"]] = node

    edges = []
    children_set = set()
    for name, node in nodes.items():
        parent = node["parentPipelineRun"]
        if parent and parent in nodes:
            edges.append({"from": parent, "to": name})
            children_set.add(name)
        for _, child_name in node["triggered"].items():
            if child_name in nodes:
                edge = {"from": name, "to": child_name}
                if edge not in edges:
                    edges.append(edge)
                children_set.add(child_name)

    roots = [n for n in nodes if n not in children_set]

    chains, seen = [], set()
    for r in sorted(roots, key=lambda x: nodes[x].get("startTime", ""), reverse=True):
        if r not in seen:
            chain = build_chain(r, nodes, edges)
            for cn in chain["nodes"]:
                seen.add(cn["name"])
            if chain["nodes"]:
                chains.append(chain)
    standalone = [nodes[n] for n in nodes if n not in seen]

    multi_chains = [c for c in chains if len(c["nodes"]) > 1]
    single_chains = [c for c in chains if len(c["nodes"]) == 1]

    standalone_by_pipeline = defaultdict(list)
    for c in single_chains:
        standalone_by_pipeline[c["nodes"][0]["pipeline"]].append(c["nodes"][0])
    for s in standalone:
        standalone_by_pipeline[s["pipeline"]].append(s)

    for runs in standalone_by_pipeline.values():
        runs.sort(key=lambda x: x.get("startTime", ""), reverse=True)

    return {
        "chains": multi_chains[:limit],
        "totalChains": len(multi_chains),
        "standaloneByPipeline": {
            k: {"runs": v[:limit], "total": len(v)}
            for k, v in standalone_by_pipeline.items()
        },
        "meta": {
            "totalRuns": len(nodes),
            "pipelines": sorted(set(n["pipeline"] for n in nodes.values())),
            "hours": hours,
            "limit": limit,
        },
    }


@app.get("/", response_class=HTMLResponse)
def index():
    return TEMPLATE_PATH.read_text()
