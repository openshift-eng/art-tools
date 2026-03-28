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

_k8s_custom = None
_k8s_core = None


def get_clients():
    global _k8s_custom, _k8s_core
    if _k8s_custom is None:
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        _k8s_custom = client.CustomObjectsApi()
        _k8s_core = client.CoreV1Api()
    return _k8s_custom, _k8s_core


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


def fetch_all_nodes(hours, search, status_filter):
    custom_api, _ = get_clients()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    result = custom_api.list_namespaced_custom_object(
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
        if status_filter:
            allowed = set(status_filter.split(","))
            if node["status"] not in allowed:
                continue
        nodes[node["name"]] = node
    return nodes


def compute_edges(nodes):
    edges = []
    children_set = set()
    # First pass: authoritative parent edges from the child's own label
    declared_parents = {}
    for name, node in nodes.items():
        parent = node["parentPipelineRun"]
        if parent and parent in nodes:
            edges.append({"from": parent, "to": name})
            children_set.add(name)
            declared_parents[name] = parent
    # Second pass: triggered annotations, but only if the child hasn't
    # declared a *different* parent (prevents rebuilds from absorbing
    # the previous run's children into their chain)
    for name, node in nodes.items():
        for _, child_name in node["triggered"].items():
            if child_name not in nodes:
                continue
            real_parent = declared_parents.get(child_name)
            if real_parent and real_parent != name:
                continue
            edge = {"from": name, "to": child_name}
            if edge not in edges:
                edges.append(edge)
            children_set.add(child_name)
    return edges, children_set


def find_chain(target, nodes, edges):
    """Find the full chain containing target node."""
    connected = set()
    queue = [target]
    while queue:
        current = queue.pop(0)
        if current in connected or current not in nodes:
            continue
        connected.add(current)
        for e in edges:
            if e["from"] == current:
                queue.append(e["to"])
            if e["to"] == current:
                queue.append(e["from"])
    if len(connected) <= 1:
        return None
    chain_nodes = [nodes[n] for n in connected]
    chain_edges = [e for e in edges if e["from"] in connected and e["to"] in connected]
    root = None
    children = {e["to"] for e in chain_edges}
    for n in connected:
        if n not in children:
            root = n
            break
    return {"root": root or target, "nodes": chain_nodes, "edges": chain_edges}


@app.get("/api/runs")
def get_runs(
    hours: int = Query(default=24, ge=1, le=168),
    search: str = Query(default=""),
    status: str = Query(default=""),
    pipeline: str = Query(default=""),
):
    nodes = fetch_all_nodes(hours, search, status)
    all_runs = sorted(nodes.values(), key=lambda x: x.get("startTime", ""), reverse=True)
    if pipeline:
        all_runs = [r for r in all_runs if r["pipeline"] == pipeline]
    pipelines = sorted(set(n["pipeline"] for n in nodes.values()))
    pipeline_counts = defaultdict(int)
    for n in nodes.values():
        pipeline_counts[n["pipeline"]] += 1
    return {
        "runs": all_runs,
        "pipelines": [{"name": p, "count": pipeline_counts[p]} for p in pipelines],
        "total": len(all_runs),
    }


@app.get("/api/run/{name}")
def get_run_detail(name: str, hours: int = Query(default=24, ge=1, le=168)):
    nodes = fetch_all_nodes(hours, "", "")
    if name not in nodes:
        return {"error": "not found"}
    node = nodes[name]
    edges, _ = compute_edges(nodes)
    chain = find_chain(name, nodes, edges)

    logs = None
    if node["status"] in ("Failed", "Cancelled"):
        logs = get_failure_logs(name)

    return {"run": node, "chain": chain, "logs": logs}


def get_failure_logs(pr_name, tail=80):
    """Get logs from the failed task in a PipelineRun."""
    custom_api, core_api = get_clients()
    try:
        trs = custom_api.list_namespaced_custom_object(
            group="tekton.dev", version="v1", namespace=NAMESPACE, plural="taskruns",
            label_selector=f"tekton.dev/pipelineRun={pr_name}",
        )
        for tr in trs.get("items", []):
            tr_status = get_status(tr)
            if tr_status not in ("Failed", "Cancelled"):
                continue
            tr_name = tr["metadata"]["name"]
            pod_name = tr.get("status", {}).get("podName", "")
            task_name = tr["metadata"].get("labels", {}).get("tekton.dev/pipelineTask", tr_name)
            if not pod_name:
                continue
            try:
                log = core_api.read_namespaced_pod_log(
                    name=pod_name, namespace=NAMESPACE,
                    container="step-main", tail_lines=tail,
                )
            except Exception:
                try:
                    log = core_api.read_namespaced_pod_log(
                        name=pod_name, namespace=NAMESPACE, tail_lines=tail,
                    )
                except Exception as e:
                    log = f"Could not fetch logs: {e}"
            return {"task": task_name, "pod": pod_name, "lines": log}
    except Exception as e:
        logger.warning("Failed to fetch logs for %s: %s", pr_name, e)
    return None


@app.get("/", response_class=HTMLResponse)
def index():
    return TEMPLATE_PATH.read_text()
