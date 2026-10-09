"""Build a new PipelineRun from a current Pipeline and optional prior run."""

from copy import deepcopy

TENANT_PIPELINERUN_TIMEOUT = "6h"


class InvalidRun(ValueError):
    pass


def pipeline_name(run: dict) -> str | None:
    return (run.get("spec", {}).get("pipelineRef") or {}).get("name") or run.get("metadata", {}).get("labels", {}).get(
        "tekton.dev/pipeline"
    )


def parameter_form(pipeline: dict, source_run: dict | None = None) -> dict:
    source_values = {
        param["name"]: param.get("value") for param in (source_run or {}).get("spec", {}).get("params", [])
    }
    parameters = []
    names = set()
    for definition in pipeline.get("spec", {}).get("params", []):
        name = definition["name"]
        names.add(name)
        source = "run" if name in source_values else "default" if "default" in definition else "required"
        parameters.append(
            {
                "name": name,
                "type": definition.get("type", "string"),
                "description": definition.get("description", ""),
                "enum": definition.get("enum"),
                "value": source_values.get(name, definition.get("default")),
                "source": source,
            }
        )
    return {
        "namespace": pipeline["metadata"]["namespace"],
        "pipeline": pipeline["metadata"]["name"],
        "resourceVersion": pipeline["metadata"]["resourceVersion"],
        "parameters": parameters,
        "removedParameters": sorted(set(source_values) - names),
        "workspaces": deepcopy((source_run or {}).get("spec", {}).get("workspaces", [])),
        "workspaceDefinitions": deepcopy(pipeline.get("spec", {}).get("workspaces", [])),
        "sourceRun": (
            {"name": source_run["metadata"]["name"], "uid": source_run["metadata"]["uid"]} if source_run else None
        ),
    }


def _validate_value(name: str, value, definition: dict) -> None:
    kind = definition.get("type", "string")
    if kind == "string" and not isinstance(value, str):
        raise InvalidRun(f"Parameter {name} must be a string")
    if kind == "array" and (not isinstance(value, list) or any(not isinstance(item, str) for item in value)):
        raise InvalidRun(f"Parameter {name} must be an array of strings")
    if kind == "object" and (not isinstance(value, dict) or any(not isinstance(item, str) for item in value.values())):
        raise InvalidRun(f"Parameter {name} must be an object of strings")
    if kind not in ("string", "array", "object"):
        raise InvalidRun(f"Unsupported parameter type for {name}: {kind}")
    if definition.get("enum") and value not in definition["enum"]:
        raise InvalidRun(f"Parameter {name} must match one of its allowed values")


def build_run(
    pipeline: dict,
    values: dict,
    *,
    source_run: dict | None = None,
    workspaces: list | None = None,
) -> dict:
    namespace = pipeline["metadata"]["namespace"]
    name = pipeline["metadata"]["name"]
    if source_run and (source_run["metadata"]["namespace"] != namespace or pipeline_name(source_run) != name):
        raise InvalidRun("The source run does not reference this Pipeline")
    definitions = {param["name"]: param for param in pipeline.get("spec", {}).get("params", [])}
    unknown = set(values) - set(definitions)
    if unknown:
        raise InvalidRun(f"Parameters are no longer defined by this Pipeline: {', '.join(sorted(unknown))}")
    params = []
    for param_name, definition in definitions.items():
        value = values.get(param_name, definition.get("default"))
        if value is None:
            raise InvalidRun(f"Parameter {param_name} is required")
        _validate_value(param_name, value, definition)
        params.append({"name": param_name, "value": value})
    spec = {"pipelineRef": {"name": name}, "params": params}
    if source_run:
        for field in ("taskRunTemplate", "taskRunSpecs", "timeouts", "podTemplate", "computeResources"):
            if field in source_run.get("spec", {}):
                spec[field] = deepcopy(source_run["spec"][field])
        if "serviceAccountName" in source_run.get("spec", {}):
            spec.setdefault("taskRunTemplate", {})["serviceAccountName"] = source_run["spec"]["serviceAccountName"]
    else:
        spec["taskRunTemplate"] = {"serviceAccountName": "pipeline"}
    if namespace.endswith("-tenant"):
        timeouts = spec.setdefault("timeouts", {})
        timeouts["pipeline"] = TENANT_PIPELINERUN_TIMEOUT
    bindings = workspaces if workspaces is not None else (source_run or {}).get("spec", {}).get("workspaces", [])
    declared = {workspace["name"]: workspace for workspace in pipeline.get("spec", {}).get("workspaces", [])}
    provided = {workspace.get("name") for workspace in bindings}
    if len(provided) != len(bindings) or any(item not in declared for item in provided):
        raise InvalidRun("Workspace bindings must have unique names declared by the current Pipeline")
    missing = [name for name, workspace in declared.items() if not workspace.get("optional") and name not in provided]
    if missing:
        raise InvalidRun(f"Required workspace bindings are missing: {', '.join(missing)}")
    if bindings:
        spec["workspaces"] = deepcopy(bindings)
    metadata = {"namespace": namespace, "generateName": f"{name[:48]}-"}
    if source_run:
        metadata["annotations"] = {"art.openshift.io/rebuilt-from": source_run["metadata"]["uid"]}
    return {"apiVersion": "tekton.dev/v1", "kind": "PipelineRun", "metadata": metadata, "spec": spec}
