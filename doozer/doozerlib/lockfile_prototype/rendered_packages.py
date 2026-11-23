"""
Resolve package lists rendered by image build scripts before DNF installs them.
"""

import json
import logging
import re
import shlex
from pathlib import Path

from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.dynamic_packages import DynamicPackageResolver, DynamicPackageSet
from doozerlib.lockfile_prototype.models import RepoEntry


async def resolve_rendered_packages(
    source_dir: Path,
    entries: list[dict],
    parents: list[str],
    container: ContainerImageHelper,
    repos: list[RepoEntry] | None = None,
    logger: logging.Logger | None = None,
    arches: list[str] | None = None,
    skipped_stages: set[int] | None = None,
) -> dict[int, DynamicPackageSet]:
    """
    Resolve copied package templates in the stages that render them.

    The Python prefix is selected from the base image's OS release and the
    case statement in the copied configuration script, just as in the build.
    Fail if it cannot be determined rather than omit required RPMs.
    """
    stages: list[list[dict]] = []
    stage_headers: list[str] = []
    for entry in entries:
        if entry["instruction"] == "FROM":
            stages.append([])
            stage_headers.append(entry["value"])
        elif stages:
            stages[-1].append(entry)

    result: dict[int, DynamicPackageSet] = {}
    stage_states: dict[str, tuple[dict[str, Path], set[str], dict[str, str]]] = {}
    for stage, instructions in enumerate(stages):
        header = shlex.split(stage_headers[stage])
        header = [token for token in header if not token.startswith("--platform=")]
        parent = header[0].lower() if header else ""
        alias = header[2].lower() if len(header) >= 3 and header[1].lower() == "as" else ""
        parent_copied, parent_unresolved, parent_env = stage_states.get(parent, ({}, set(), {}))
        variables = parent_env.copy()
        env_variables = parent_env.copy()
        copied = parent_copied.copy()
        unresolved = parent_unresolved.copy()
        snapshots: list[tuple[dict[str, str], dict[str, Path], set[str], str]] = []
        for entry in instructions:
            value = entry["value"]
            if entry["instruction"] in ("ENV", "ARG"):
                match = re.match(r"(\w+)=(.*)$", value.strip())
                if match and "$" not in match.group(2):
                    variables[match.group(1)] = match.group(2).strip("\"'")
                    if entry["instruction"] == "ENV":
                        env_variables[match.group(1)] = variables[match.group(1)]
            elif entry["instruction"] == "COPY":
                try:
                    copy_value = value.replace("\\\n", " ").strip()
                    match = re.match(r"^((?:--\S+\s+)*)\s*(\[.*)$", copy_value, re.DOTALL)
                    if match:
                        operands = json.loads(match.group(2))
                        if not isinstance(operands, list) or not all(isinstance(token, str) for token in operands):
                            continue
                        tokens = shlex.split(match.group(1)) + operands
                    else:
                        tokens = shlex.split(copy_value)
                except ValueError:
                    continue
                from_stage = any(token == "--from" or token.startswith("--from=") for token in tokens)
                tokens = [token for token in tokens if not token.startswith("--")]
                if len(tokens) < 2:
                    continue
                if from_stage:
                    destination = tokens[-1]
                    for source in tokens[:-1]:
                        target = (
                            f"{destination.rstrip('/')}/{Path(source).name}"
                            if destination.endswith("/")
                            else destination
                        )
                        affected = {
                            path for path in copied if path == target or path.startswith(target.rstrip("/") + "/")
                        }
                        for path in affected:
                            copied.pop(path)
                        unresolved.update(affected or {target})
                    continue
                for token in tokens[:-1]:
                    token = re.sub(r"\$\{(\w+)\}|\$(\w+)", lambda m: variables.get(m.group(1) or m.group(2), ""), token)
                    if not Path(token).parts or token.startswith("/") or ".." in Path(token).parts:
                        continue
                    for candidate in source_dir.glob(token):
                        path = candidate.resolve()
                        if not path.is_relative_to(source_dir.resolve()) or not path.is_file():
                            continue
                        destination = tokens[-1]
                        copied[
                            (destination.rstrip("/") + "/" + path.name) if destination.endswith("/") else destination
                        ] = path
                        target = (
                            (destination.rstrip("/") + "/" + path.name) if destination.endswith("/") else destination
                        )
                        unresolved.discard(target)
            elif entry["instruction"] == "RUN":
                snapshots.append((variables.copy(), copied.copy(), unresolved.copy(), value))

        if alias:
            stage_states[alias] = (copied.copy(), unresolved.copy(), env_variables.copy())
        if skipped_stages and stage in skipped_stages:
            continue

        for variables, copied, unresolved, run_text in snapshots:
            script_text = "\n".join(
                path.read_text()
                for destination, path in copied.items()
                if path.suffix == ".sh"
                and re.search(rf"(?<![\w.-]){re.escape(Path(destination).name)}(?![\w.-])", run_text)
            )
            commands = run_text + "\n" + script_text
            if "render_package_list" not in commands:
                continue

            def referenced(destination: str) -> bool:
                """
                Check whether commands refer to a copied container path.
                """
                expanded = re.sub(
                    r"\$\{(\w+)\}|\$(\w+)",
                    lambda match: variables.get(match.group(1) or match.group(2), match.group(0)),
                    commands,
                )
                return destination in expanded or any(
                    value == Path(destination).name and re.search(rf"\$\{{?{re.escape(name)}\b", commands)
                    for name, value in variables.items()
                )

            for destination in unresolved:
                if referenced(destination):
                    raise RuntimeError(f"Cannot resolve rendered package list copied from another stage: {destination}")

            templates: list[Path] = []
            arch_templates: dict[str, list[Path]] = {}
            for destination, path in copied.items():
                if path.suffix == ".sh":
                    continue
                if referenced(destination):
                    templates.append(path)
                elif "$(arch)" in commands:
                    for arch in arches or []:
                        if any(value and Path(destination).name == f"{value}-{arch}" for value in variables.values()):
                            arch_templates.setdefault(arch, []).append(path)
            if not templates and not arch_templates:
                continue

            prefix = ""
            all_templates = templates + [path for paths in arch_templates.values() for path in paths]
            if any("@PYTHON_PACKAGE_PREFIX@" in path.read_text() for path in all_templates):
                image = parents[stage] if stage < len(parents) else ""
                version = ""
                for os_release_path in ("/etc/os-release", "/usr/lib/os-release") if image and "/" in image else ():
                    os_release = await container.read_file_from_image(image, os_release_path)
                    version = DynamicPackageResolver._parse_os_release(os_release).get("VERSION_ID", "")
                    if version:
                        break
                if not version:
                    repo_majors = {
                        match.group(1)
                        for repo in repos or []
                        for match in re.finditer(r"\brhel-(\d+)(?:\b|_)", repo.repoid, re.IGNORECASE)
                    }
                    if len(repo_majors) == 1:
                        version = repo_majors.pop()
                        if logger:
                            logger.warning(
                                "Cannot read OS release from %s; inferring RHEL %s from repository IDs for stage %s",
                                image,
                                version,
                                stage,
                            )
                major = version.split(".", 1)[0]
                config = next((path for path in copied.values() if path.name == "python-config.sh"), None)
                if not major or config is None:
                    raise RuntimeError(f"Cannot determine Python package prefix for stage {stage}")
                match = re.search(
                    rf"(?m)^\s*{re.escape(major)}\)\s*\n\s*PYTHON_VERSION=([\d.]+)\s*\n\s*;;", config.read_text()
                )
                if not match:
                    raise RuntimeError(f"Unsupported Python package prefix for OS version {version} in stage {stage}")
                prefix = "python" + match.group(1)

            def read_packages(paths: list[Path]) -> list[str]:
                packages: set[str] = set()
                for path in paths:
                    for line in path.read_text().splitlines():
                        line = line.strip()
                        if line and not line.startswith("#"):
                            packages.add(line.replace("@PYTHON_PACKAGE_PREFIX@", prefix))
                return sorted(packages)

            stage_result = result.setdefault(stage, DynamicPackageSet(common=[], arch_specific={}))
            stage_result.common = sorted(set(stage_result.common) | set(read_packages(templates)))
            for arch, paths in arch_templates.items():
                stage_result.arch_specific[arch] = sorted(
                    set(stage_result.arch_specific.get(arch, [])) | set(read_packages(paths))
                )
    return result
