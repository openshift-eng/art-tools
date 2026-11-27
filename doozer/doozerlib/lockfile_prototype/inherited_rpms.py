"""
Track inherited RPM versions in wildcard-versionlocked Containerfile stages.
"""

import json
import re
import shlex
from collections.abc import Iterator
from pathlib import Path

from doozerlib.lockfile_prototype.models import RepoEntry, RpmInventory
from doozerlib.lockfile_prototype.rendered_packages import parse_copy_tokens
from doozerlib.lockfile_prototype.utils import parse_evr
from doozerlib.repodata import Rpm

_PACKAGE_MANAGERS = {"dnf", "dnf5", "microdnf", "yum", "rpm-ostree"}
_MUTATIONS = {
    "install",
    "update",
    "upgrade",
    "reinstall",
    "downgrade",
    "distro-sync",
    "remove",
    "erase",
    "swap",
    "groupinstall",
    "groupremove",
    "override",
    "uninstall",
}
_SHELL_COMMANDS = re.compile(r"""(?:[^;&|\n"'#]+|"[^"]*"|'[^']*')+|\#[^\n]*""")


def stage_references(entries: list[dict]) -> dict[int, int]:
    """
    Map stage aliases and numeric FROM references to their earlier stages.
    """
    aliases: dict[str, int] = {}
    references: dict[int, int] = {}
    stage = -1
    for entry in entries:
        if entry["instruction"] != "FROM":
            continue
        stage += 1
        tokens = [token.lower() for token in shlex.split(entry["value"]) if not token.startswith("--")]
        if tokens[0] in aliases:
            references[stage] = aliases[tokens[0]]
        aliases[str(stage)] = stage
        if len(tokens) >= 3 and tokens[-2] == "as":
            aliases[tokens[-1]] = stage
    return references


def _run_commands(content: str) -> Iterator[list[str]]:
    """
    Yield literal commands in source order, including JSON-form RUN commands.
    """
    content = content.replace("\\\n", " ").strip()
    if content.startswith("["):
        try:
            command = json.loads(content)
        except ValueError:
            pass
        else:
            if isinstance(command, list) and all(isinstance(arg, str) for arg in command):
                yield command
                return
    for match in _SHELL_COMMANDS.finditer(content):
        if match.group().startswith("#"):
            continue
        try:
            tokens = shlex.split(match.group())
        except ValueError:
            continue
        if tokens:
            yield tokens


def _package_operations(tokens: list[str]) -> Iterator[str]:
    """
    Recognize literal package changes and versionlock commands on the stage root.
    """
    while tokens and (
        tokens[0] in {"if", "then", "else", "do", "!", "exec", "command"} or re.match(r"\w+=", tokens[0])
    ):
        tokens = tokens[1:]
    if not tokens:
        return
    program = Path(tokens[0]).name
    args = tokens[1:]
    if any(arg == "--installroot" or arg.startswith("--installroot=") for arg in args):
        return
    if program in _PACKAGE_MANAGERS:
        operation = next((arg for arg in args if arg in _MUTATIONS | {"versionlock"}), None)
        if operation == "versionlock":
            options = args[args.index(operation) + 1 :]
            if options[:2] == ["add", "*"]:
                yield "lock"
            elif any(option in {"clear", "delete", "reset"} for option in options):
                yield "unlock"
        elif operation:
            bypasses_lock = any(
                arg in {"--noplugins", "--disableplugin", "--disable-plugin"}
                or arg.startswith(("--disableplugin=", "--disable-plugin="))
                for arg in args
            )
            yield "unprotected_change" if bypasses_lock or program not in {"dnf", "dnf5", "yum"} else "change"
    elif program == "rpm" and any(
        arg in {"--install", "--upgrade", "--freshen", "--erase"} or re.fullmatch(r"-[iUFe][a-zA-Z]*", arg)
        for arg in args
    ):
        yield "unprotected_change"


def stage_package_operations(source_dir: Path, entries: list[dict], references: dict[int, int]) -> dict[int, list[str]]:
    """
    Read RPM operations in RUN commands and directly invoked build-context scripts.
    """
    root = source_dir.resolve()
    scripts: dict[int, dict[str, Path]] = {}
    operations: dict[int, list[str]] = {}
    stage = -1
    for entry in entries:
        instruction, value = entry["instruction"], entry["value"]
        if instruction == "FROM":
            stage += 1
            scripts[stage] = scripts.get(references.get(stage, -1), {}).copy()
            operations[stage] = []
        elif instruction == "COPY" and stage >= 0:
            try:
                tokens = parse_copy_tokens(value)
            except ValueError:
                continue
            operands = [token for token in tokens if not token.startswith("--")]
            if len(operands) < 2:
                continue
            destination = operands[-1]
            for source in operands[:-1]:
                target = f"{destination.rstrip('/')}/{Path(source).name}" if destination.endswith("/") else destination
                if any(token.startswith("--from=") for token in tokens):
                    scripts[stage].pop(target, None)
                else:
                    scripts[stage][target] = root / source
        elif instruction == "RUN" and stage >= 0:
            mounts: dict[str, Path] = {}
            for command in _run_commands(value):
                while command and command[0].startswith("--"):
                    flag = command.pop(0)
                    if flag.startswith("--mount="):
                        options = dict(part.split("=", 1) for part in flag[8:].split(",") if "=" in part)
                        target = options.get("target") or options.get("dst") or options.get("destination")
                        if options.get("type", "bind") == "bind" and "from" not in options and target:
                            mounts[target.rstrip("/")] = root / options.get("source", options.get("src", "."))
                if not command:
                    continue
                operations[stage].extend(_package_operations(command))
                script = command[0]
                if Path(script).name in {"sh", "bash"}:
                    script = next((arg for arg in command[1:] if not arg.startswith("-")), "")
                if script:
                    path = scripts[stage].get(script)
                    if path is None:
                        for target, source_dir_path in mounts.items():
                            if script == target:
                                path = source_dir_path
                                break
                            if script.startswith(target + "/"):
                                path = source_dir_path / script[len(target) + 1 :]
                                break
                    if path is None and not script.startswith("/") and script.endswith(".sh"):
                        path = root / script
                    if path is not None:
                        path = path.resolve()
                        if path.is_relative_to(root) and path.is_file():
                            for script_command in _run_commands(path.read_text(errors="replace")):
                                operations[stage].extend(_package_operations(script_command))
    return operations


def preserved_versions(inventory: RpmInventory) -> dict[str, str]:
    """
    Collect inherited EVRs across all arches, including arches missing a package.
    """
    versions: dict[str, set[str]] = {}
    for packages in inventory.values():
        for name, evrs in packages.items():
            versions.setdefault(name, set()).update(evrs)
    for name, evrs in versions.items():
        if len(evrs) != 1:
            raise RuntimeError(f"Conflicting inherited RPM versions for {name}: {sorted(evrs)}")
    return {name: next(iter(evrs)) for name, evrs in versions.items()}


def constrain_repo(repo: RepoEntry, packages: list[Rpm], versions: dict[str, str]) -> RepoEntry:
    """
    Exclude repository candidates that conflict with retained inherited EVRs.
    """
    excluded = sorted(
        {
            package.nevra
            for package in packages
            if package.name in versions
            and (str(package.epoch), package.version, package.release) != parse_evr(versions[package.name])
        }
    )
    if not excluded:
        return repo
    options = repo.options.copy()
    previous = [str(options.pop(key)) for key in ("exclude", "excludepkgs") if key in options]
    options["excludepkgs"] = " ".join(previous + excluded)
    return repo.model_copy(update={"options": options})
