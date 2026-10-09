"""Reconstruct scan-to-FBC build chains from explicit PipelineRun relationships."""

from collections import defaultdict

PIPELINES = ("layered-products-scan", "build-layered-products", "olm-bundle-konflux", "build-fbc")
LABEL_PREFIX = "art.openshift.io/"


def health_run(run: dict, summary: dict) -> dict:
    metadata = run.get("metadata", {})
    labels = metadata.get("labels") or {}
    annotations = metadata.get("annotations") or {}
    params = {param["name"]: param.get("value") for param in run.get("spec", {}).get("params", [])}
    condition = next(
        (item for item in run.get("status", {}).get("conditions", []) if item.get("type") == "Succeeded"), {}
    )
    outcome = {"True": "Succeeded", "Unknown": "Running", "False": "Failed"}.get(condition.get("status"), "Unknown")
    if outcome == "Failed" and "cancel" in (condition.get("reason") or "").lower():
        outcome = "Cancelled"
    return {
        **summary,
        "status": outcome,
        "group": labels.get(f"{LABEL_PREFIX}group") or params.get("group") or "",
        "assembly": labels.get(f"{LABEL_PREFIX}assembly") or params.get("assembly") or "stream",
        "target": labels.get(f"{LABEL_PREFIX}ocp-version") or params.get("ocp-target-version") or "",
        "parent": labels.get(f"{LABEL_PREFIX}parent-pipelinerun"),
        "rebuiltFrom": annotations.get(f"{LABEL_PREFIX}rebuilt-from"),
        "triggered": [
            value
            for key, value in annotations.items()
            if any(
                key == f"{LABEL_PREFIX}triggered-{name}" or key.startswith(f"{LABEL_PREFIX}triggered-{name}-")
                for name in PIPELINES[1:]
            )
        ],
        "dryRun": str(params.get("dry-run", "false")).lower() == "true",
    }


def build_health(runs: list[dict], incomplete_namespaces: set[str] | None = None) -> list[dict]:
    """A group is green only when a complete, unambiguous, non-dry-run chain succeeded."""
    incomplete_namespaces = incomplete_namespaces or set()
    runs = [run for run in runs if run["pipeline"] in PIPELINES and run["group"] and run["uid"]]
    by_uid = {run["uid"]: run for run in runs}
    by_name = defaultdict(list)
    retries = defaultdict(list)
    children = defaultdict(dict)
    issues = defaultdict(set)

    def identity(run):
        return run["namespace"], run["group"], run["assembly"]

    def order(run):
        return run["created"] or "", run["uid"]

    for run in runs:
        by_name[(run["namespace"], run["name"])].append(run)
    for run in runs:
        if run["rebuiltFrom"]:
            source = by_uid.get(run["rebuiltFrom"])
            if source and identity(source) == identity(run) and source["pipeline"] == run["pipeline"]:
                retries[source["uid"]].append(run)
            else:
                issues[run["uid"]].add("Retry source is missing or belongs to another group")
        if run["parent"] and run["pipeline"] != PIPELINES[0]:
            parents = by_name[(run["namespace"], run["parent"])]
            if len(parents) == 1 and identity(parents[0]) == identity(run):
                children[parents[0]["uid"]][run["uid"]] = run
            elif len(parents) > 1:
                for parent in parents:
                    issues[parent["uid"]].add("Parent name was reused; the relationship is ambiguous")
        for name in run["triggered"]:
            targets = by_name[(run["namespace"], name)]
            if len(targets) == 1 and identity(targets[0]) == identity(run):
                target = targets[0]
                if target["parent"] and target["parent"] != run["name"]:
                    issues[run["uid"]].add("Triggered run has a different parent")
                else:
                    children[run["uid"]][target["uid"]] = target
            else:
                issues[run["uid"]].add("A triggered PipelineRun is missing or ambiguous")

    family_cache = {}

    def family(run):
        if run["uid"] not in family_cache:
            found = {}
            pending = [run]
            while pending:
                item = pending.pop()
                if item["uid"] in found:
                    continue
                found[item["uid"]] = item
                pending.extend(retries[item["uid"]])
            family_cache[run["uid"]] = sorted(found.values(), key=order, reverse=True)
        return family_cache[run["uid"]]

    def chain(root, before=None):
        stages = {name: {} for name in PIPELINES}
        attempts = {}
        chain_issues = set()
        pending = [root]
        visited = set()
        while pending:
            original = pending.pop()
            related = [item for item in family(original) if before is None or (item["created"] or "") < before]
            effective = dict(related[0])
            if before and (not effective["completed"] or effective["completed"] >= before):
                effective.update(status="Running", completed=None)
            if effective["uid"] in visited:
                continue
            visited.add(effective["uid"])
            attempts.update((item["uid"], item) for item in related)
            chain_issues.update(issues[effective["uid"]])
            stages[effective["pipeline"]][effective["uid"]] = effective
            if effective["pipeline"] == PIPELINES[-1]:
                continue
            for child in children[effective["uid"]].values():
                if before and (child["created"] or "") >= before:
                    continue
                if PIPELINES.index(child["pipeline"]) == PIPELINES.index(effective["pipeline"]) + 1:
                    pending.append(child)
                else:
                    chain_issues.add("Unexpected pipeline order in the build chain")
        effective_runs = [run for stage in stages.values() for run in stage.values()]
        failed = [run for run in effective_runs if run["status"] == "Failed"]
        cancelled = [run for run in effective_runs if run["status"] == "Cancelled"]
        active = [run for run in effective_runs if run["status"] == "Running"]
        complete = all(stages.values()) and all(run["status"] == "Succeeded" for run in effective_runs)
        if any(run["dryRun"] for run in effective_runs):
            chain_issues.add("Dry-run execution does not establish E2E health")
        if failed:
            status = "Failed"
        elif cancelled:
            status = "Cancelled"
        elif active:
            status = "Running"
        elif complete and not chain_issues:
            status = "Succeeded"
        elif any(stages[name] for name in PIPELINES[1:]):
            status = "Incomplete"
        else:
            status = "Scan only"
        history_pending = list(attempts.values())
        history_seen = set()
        while history_pending:
            item = history_pending.pop()
            if item["uid"] in history_seen or (before and (item["created"] or "") >= before):
                continue
            history_seen.add(item["uid"])
            attempts[item["uid"]] = item
            history_pending.extend(children[item["uid"]].values())
            history_pending.extend(retries[item["uid"]])
        return {
            "root": root,
            "status": status,
            "completed": max((run["completed"] for run in effective_runs if run["completed"]), default=None),
            "stages": [
                {"pipeline": name, "runs": sorted(stages[name].values(), key=lambda run: (run["target"], order(run)))}
                for name in PIPELINES
            ],
            "failures": sorted(failed + cancelled, key=order),
            "active": sorted(active, key=order),
            "attempts": sorted(attempts.values(), key=order),
            "recovered": any(run["rebuiltFrom"] for run in attempts.values()),
            "issues": sorted(chain_issues),
        }

    grouped_runs = defaultdict(list)
    for run in runs:
        grouped_runs[identity(run)].append(run)
    result = []
    for (namespace, group, assembly), values in grouped_runs.items():
        scans = sorted((run for run in values if run["pipeline"] == PIPELINES[0]), key=order, reverse=True)
        if not scans:
            continue
        roots = [run for run in scans if not run["rebuiltFrom"] or run["rebuiltFrom"] not in by_uid]
        chains = [chain(run) for run in roots]
        meaningful = [
            item
            for item in chains
            if any(stage["runs"] for stage in item["stages"][1:]) or item["status"] in ("Failed", "Cancelled")
        ]
        current = meaningful[0] if meaningful else None
        completed_versions = []
        for item in meaningful:
            completed_versions.append(item)
            retry_times = sorted(
                {run["created"] for run in item["attempts"] if run["rebuiltFrom"] and run["created"]}, reverse=True
            )
            completed_versions.extend(chain(item["root"], before=timestamp) for timestamp in retry_times)
        last_completed = next(
            (item for item in completed_versions if item["status"] in ("Succeeded", "Failed", "Cancelled")), None
        )
        last_successful = next((item for item in completed_versions if item["status"] == "Succeeded"), None)
        health = last_completed["status"] if last_completed else "Unknown"
        if namespace in incomplete_namespaces and health == "Succeeded":
            health = "Unknown"
        result.append(
            {
                "namespace": namespace,
                "group": group,
                "assembly": assembly,
                "health": health,
                "latestScan": scans[0],
                "currentChain": current,
                "lastCompletedChain": last_completed,
                "lastSuccessfulChain": last_successful,
                "history": meaningful[:10],
                "incompleteHistory": namespace in incomplete_namespaces,
                "active": any(run["status"] == "Running" for run in values),
            }
        )
    return sorted(
        result,
        key=lambda item: (
            item["health"] != "Failed",
            not item["active"],
            item["namespace"],
            item["group"],
            item["assembly"],
        ),
    )
