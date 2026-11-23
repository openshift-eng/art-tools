"""
Resolve package lists rendered by image build scripts before DNF installs them.
"""

import logging
import re
import shlex
from pathlib import Path

from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.dynamic_packages import DynamicPackageResolver
from doozerlib.lockfile_prototype.models import RepoEntry


async def resolve_rendered_packages(
    source_dir: Path,
    entries: list[dict],
    parents: list[str],
    container: ContainerImageHelper,
    repos: list[RepoEntry] | None = None,
    logger: logging.Logger | None = None,
) -> dict[int, list[str]]:
    """
    Resolve copied package templates in the stages that render them.

    The Python prefix is selected from the base image's OS release and the
    case statement in the copied configuration script, just as in the build.
    Fail if it cannot be determined rather than omit required RPMs.
    """
    stages: list[list[dict]] = []
    for entry in entries:
        if entry["instruction"] == "FROM":
            stages.append([])
        elif stages:
            stages[-1].append(entry)

    result: dict[int, list[str]] = {}
    for stage, instructions in enumerate(stages):
        variables: dict[str, str] = {}
        copied: dict[str, Path] = {}
        runs = []
        for entry in instructions:
            value = entry["value"]
            if entry["instruction"] in ("ENV", "ARG"):
                match = re.match(r"(\w+)=(.*)$", value.strip())
                if match and "$" not in match.group(2):
                    variables[match.group(1)] = match.group(2).strip("\"'")
            elif entry["instruction"] == "COPY" and not value.lstrip().startswith("--"):
                try:
                    tokens = shlex.split(value.replace("\\\n", " "))
                except ValueError:
                    continue
                if len(tokens) < 2:
                    continue
                for token in tokens[:-1]:
                    token = re.sub(r"\$\{(\w+)\}|\$(\w+)", lambda m: variables.get(m.group(1) or m.group(2), ""), token)
                    path = (source_dir / token).resolve()
                    if token and path.is_relative_to(source_dir.resolve()) and path.is_file():
                        destination = tokens[-1]
                        copied[
                            (destination.rstrip("/") + "/" + path.name) if destination.endswith("/") else destination
                        ] = path
            elif entry["instruction"] == "RUN":
                runs.append(value)

        run_text = "\n".join(runs)
        script_text = "\n".join(
            path.read_text()
            for destination, path in copied.items()
            if path.suffix == ".sh"
            and re.search(rf"(?<![\w.-]){re.escape(Path(destination).name)}(?![\w.-])", run_text)
        )
        commands = run_text + "\n" + script_text
        if "render_package_list" not in commands:
            continue

        templates: list[Path] = []
        for destination, path in copied.items():
            if path.suffix == ".sh":
                continue
            referenced = destination in commands or any(
                value == path.name and re.search(rf"\$\{{?{re.escape(name)}\b", commands)
                for name, value in variables.items()
            )
            if referenced:
                templates.append(path)
        if not templates:
            continue

        prefix = ""
        if any("@PYTHON_PACKAGE_PREFIX@" in path.read_text() for path in templates):
            image = parents[stage] if stage < len(parents) else ""
            if not image or "/" not in image:
                raise RuntimeError(f"Cannot determine base image for rendered package lists in stage {stage}")
            version = ""
            for os_release_path in ("/etc/os-release", "/usr/lib/os-release"):
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

        packages: set[str] = set()
        for path in templates:
            for line in path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    packages.add(line.replace("@PYTHON_PACKAGE_PREFIX@", prefix))
        result[stage] = sorted(packages)
    return result
