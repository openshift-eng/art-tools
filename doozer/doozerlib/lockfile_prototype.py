"""
Alternative lockfile generator using rpm-lockfile-prototype for DNF resolution.

Uses the rpm-lockfile-prototype CLI tool to resolve RPM dependencies from
top-level Dockerfile packages, producing a lockfile without needing a
previous build's SBOM.
"""

import json
import logging
import os
import re
import shlex
from enum import Enum
from pathlib import Path

import yaml
from artcommonlib import logutil
from artcommonlib.exectools import cmd_gather_async
from dockerfile_parse import DockerfileParser

from doozerlib.constants import BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL
from doozerlib.image import ImageMetadata
from doozerlib.repos import Repos

DEFAULT_RPM_LOCKFILE_NAME = "rpms.lock.yaml"


class LockfileBackend(str, Enum):
    ART_INTERNAL = "art-internal"
    RPM_LOCKFILE_PROTOTYPE = "rpm-lockfile-prototype"


def _resolve_bash_expansion(text: str, variables: dict[str, str]) -> str:
    """
    Resolve bash-style variable expansions in text.

    Supports ${VAR}, $VAR, ${VAR:-default}, and ${VAR:+value}.
    Unresolved variables are replaced with empty string.

    Arg(s):
        text (str): Text containing variable references.
        variables (dict[str, str]): Variable name to value mapping.
    Return Value(s):
        str: Text with variables resolved.
    """

    def _replace_plus(match: re.Match) -> str:
        var_name = match.group(1)
        value = match.group(2)
        return value if variables.get(var_name) else ""

    def _replace_minus(match: re.Match) -> str:
        var_name = match.group(1)
        default = match.group(2)
        return variables.get(var_name) or default

    def _replace_var(match: re.Match) -> str:
        return variables.get(match.group(1), "")

    for _ in range(10):
        prev = text
        text = re.sub(r"\$\{(\w+):\+([^}]*)\}", _replace_plus, text)
        text = re.sub(r"\$\{(\w+):-([^}]*)\}", _replace_minus, text)
        text = re.sub(r"\$\{(\w+)\}", _replace_var, text)
        text = re.sub(r"\$(\w+)", _replace_var, text)
        if text == prev:
            break
    return text


def _collect_stage_vars(entries: list[dict], inherited_vars: dict[str, str] | None = None) -> dict[str, str]:
    """
    Collect ARG and ENV variable definitions from DockerfileParser
    structure entries.

    ARG values with defaults and ENV values are collected. ENV values
    can reference previously defined variables via bash expansion.

    Arg(s):
        entries (list[dict]): DockerfileParser structure entries with
            "instruction" and "value" keys.
        inherited_vars (dict[str, str] | None): Variables inherited from
            global scope (ARGs before first FROM).
    Return Value(s):
        dict[str, str]: Collected variable name-to-value mapping.
    """
    variables: dict[str, str] = dict(inherited_vars or {})

    for entry in entries:
        instruction = entry["instruction"]
        value = entry["value"]

        if instruction == "ARG":
            arg_match = re.match(r"^(\w+)(?:=(.*))?$", value.strip())
            if arg_match:
                var_name = arg_match.group(1)
                default_value = arg_match.group(2)
                if default_value is not None:
                    default_value = default_value.strip()
                    if (
                        len(default_value) >= 2
                        and default_value[0] in ("\"", "'")
                        and default_value[-1] == default_value[0]
                    ):
                        default_value = default_value[1:-1]
                    variables[var_name] = _resolve_bash_expansion(default_value, variables)

        elif instruction == "ENV":
            env_match = re.match(r"^(\w+)(?:=|\s+)(.*)", value.strip())
            if env_match:
                var_name = env_match.group(1)
                val = env_match.group(2).strip()
                if len(val) >= 2 and val[0] in ("\"", "'") and val[-1] == val[0]:
                    val = val[1:-1]
                variables[var_name] = _resolve_bash_expansion(val, variables)

    return variables


_ARCH_KEYWORDS = (
    "$(arch)",
    "$HOSTTYPE",
    "${HOSTTYPE}",
    "$(uname -m)",
    "$(uname -p)",
    "$ARCH",
    "${ARCH}",
)

_ARCH_VALUE_RE = re.compile(
    r"(?:\$\(arch\)|\$HOSTTYPE|\$\{HOSTTYPE\}|\$\(uname\s+-[mp]\)|\$ARCH\b|\$\{ARCH\})"
    r"[\"']?\s*==?\s*[\"']?(\w+)[\"']?"
)


def _has_arch_test(text: str) -> bool:
    """
    Return True if text contains a known architecture-testing expression.
    """
    return any(kw in text for kw in _ARCH_KEYWORDS)


def _extract_arch_values(text: str) -> list[str]:
    """
    Extract architecture names from conditional expressions.
    """
    return _ARCH_VALUE_RE.findall(text)


def _split_shell_commands(text: str) -> list[str]:
    """
    Split shell text on && and ; delimiters, respecting $(...) subshell
    nesting so that semicolons inside command substitutions are not
    treated as command separators.

    Arg(s):
        text (str): Shell command text (e.g. a RUN body).
    Return Value(s):
        list[str]: List of individual commands.
    """
    commands: list[str] = []
    current: list[str] = []
    depth = 0
    i = 0
    while i < len(text):
        if text[i : i + 2] == "$(":
            depth += 1
            current.append("$(")
            i += 2
        elif text[i] == ")" and depth > 0:
            depth -= 1
            current.append(")")
            i += 1
        elif depth == 0 and text[i : i + 2] == "&&":
            commands.append("".join(current))
            current = []
            i += 2
        elif depth == 0 and text[i] == ";":
            commands.append("".join(current))
            current = []
            i += 1
        else:
            current.append(text[i])
            i += 1
    if current:
        commands.append("".join(current))
    return commands


def _extract_subshell_packages(subshell_body: str) -> str:
    """
    Extract package names from echo commands inside a $(...) subshell.

    Handles patterns like:
        $(if [ "$(uname -m)" != "s390x" ]; then echo -n mstflint; fi)

    Returns the extracted package names as a space-separated string,
    ignoring the arch condition. The missing-package retry logic in
    the lockfile generator handles arch unavailability.

    Arg(s):
        subshell_body (str): Content inside $(...).
    Return Value(s):
        str: Space-separated package names found in echo commands.
    """
    packages: list[str] = []
    for match in re.finditer(r"\becho\s+(?:-\w+\s+)*([\w\s-]+)", subshell_body):
        tokens = match.group(1).strip().split()
        for token in tokens:
            if token and not token.startswith("-") and re.match(r"^[\w][\w.\-]*$", token):
                packages.append(token)
    return " ".join(packages)


def _extract_packages_from_run_commands(
    run_values: list[str],
    env_vars: dict[str, str] | None = None,
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Extract package names from yum/dnf install commands in RUN bodies.

    Resolves shell variables defined inline in RUN commands, plus any
    ARG/ENV variables passed via env_vars. Supports bash parameter
    expansion patterns like ${VAR:-default} and ${VAR:+value}.

    Detects arch-conditional blocks (if [ $(arch) = X ]) and returns
    packages inside them separately so they can be resolved only for
    the matching architecture.

    Arg(s):
        run_values (list[str]): RUN command bodies (instruction prefix
            already stripped by DockerfileParser).
        env_vars (dict[str, str] | None): Variables from ARG/ENV directives.
    Return Value(s):
        tuple[list[str], dict[str, list[str]]]:
            - Sorted unique list of common package names (all arches).
            - Dict mapping arch to sorted unique package names for that arch only.
    """
    packages: set[str] = set()
    arch_packages: dict[str, set[str]] = {}
    variables = dict(env_vars or {})

    for run_body in run_values:
        commands = _split_shell_commands(run_body)

        shell_vars: dict[str, str] = {}
        arch_shell_vars: dict[str, dict[str, str]] = {}
        arch_context: list[str] | None = None
        cond_stack: list[str] = []

        for cmd in commands:
            cmd_stripped = cmd.strip()

            if cmd_stripped.startswith("if") and _has_arch_test(cmd_stripped):
                arches = _extract_arch_values(cmd_stripped)
                if arches:
                    arch_context = arches
                    cond_stack.append("arch")
                    continue

            if re.match(r"^if\s", cmd_stripped):
                cond_stack.append("unknown")

            if re.match(r"^fi\b", cmd_stripped):
                if cond_stack:
                    kind = cond_stack.pop()
                    if kind == "arch":
                        arch_context = None
                else:
                    arch_context = None
                continue

            if re.match(r"^elif\b", cmd_stripped) and cond_stack and cond_stack[-1] == "arch":
                if _has_arch_test(cmd_stripped):
                    arches = _extract_arch_values(cmd_stripped)
                    if arches:
                        arch_context = arches

            if re.match(r"^else\b", cmd_stripped):
                if not cond_stack or cond_stack[-1] == "arch":
                    arch_context = None

            sub_commands = re.split(r"\|\|", cmd_stripped)
            for sub_cmd in sub_commands:
                sub_cmd = re.sub(r"^(if\s+!\s*|if\s+|then|else|elif|do)\s*", "", sub_cmd.strip())

                if sub_cmd.startswith("if") and _has_arch_test(sub_cmd):
                    arches = _extract_arch_values(sub_cmd)
                    if arches:
                        arch_context = arches
                        cond_stack.append("arch")
                        continue

                if re.match(r"^if\s", sub_cmd):
                    cond_stack.append("unknown")
                    continue

                if sub_cmd.startswith("[[") and cond_stack and cond_stack[-1] == "arch" and _has_arch_test(sub_cmd):
                    arches = _extract_arch_values(sub_cmd)
                    if arches:
                        arch_context = arches
                    continue

                subshell_var = re.match(r"(\w+)=\$\((.+)\)$", sub_cmd, re.DOTALL)
                if subshell_var:
                    var_name = subshell_var.group(1)
                    extracted = _extract_subshell_packages(subshell_var.group(2))
                    if extracted:
                        shell_vars[var_name] = extracted
                    continue

                var_match = re.match(r"(\w+)=([\"'])(.*?)\2", sub_cmd, re.DOTALL)
                unquoted_match = None
                if not var_match:
                    unquoted_match = re.match(r"(\w+)=(\S+)$", sub_cmd)
                if var_match or unquoted_match:
                    var_name = (var_match or unquoted_match).group(1)
                    var_value = var_match.group(3) if var_match else unquoted_match.group(2)
                    in_unknown_cond = any(k == "unknown" for k in cond_stack)
                    if arch_context:
                        per_arch = arch_shell_vars.setdefault(var_name, {})
                        for arch in arch_context:
                            prev_val = per_arch.get(arch, "")
                            if prev_val:
                                resolved = _resolve_bash_expansion(
                                    var_value, {**variables, **shell_vars, var_name: prev_val}
                                )
                            else:
                                resolved = var_value
                            per_arch[arch] = resolved
                    elif in_unknown_cond and var_name in shell_vars:
                        old_val = shell_vars[var_name]
                        new_val = _resolve_bash_expansion(var_value, {**variables, **shell_vars})
                        shell_vars[var_name] = f"{old_val} {new_val}"
                    else:
                        shell_vars[var_name] = _resolve_bash_expansion(var_value, {**variables, **shell_vars})
                    continue

                install_match = re.match(r"(yum|dnf)\s+.*?\b(?:install|update|upgrade)\b(.*)$", sub_cmd, re.IGNORECASE)
                if not install_match:
                    continue

                args_str = re.sub(r"\d*>[>&]*[/\w]\S*", "", install_match.group(2)).strip()
                common_vars = {**variables, **shell_vars}
                combined_arch_vals = {k: " ".join(per_arch.values()) for k, per_arch in arch_shell_vars.items()}
                all_vars = {
                    **common_vars,
                    **{k: _resolve_bash_expansion(v, common_vars) for k, v in combined_arch_vals.items()},
                }
                resolved = _resolve_bash_expansion(args_str, all_vars)
                tokens = shlex.split(resolved)
                tokens = [sub for token in tokens for sub in token.split()]

                arch_var_names = set(arch_shell_vars.keys())

                arch_resolved_tokens: set[str] = set()
                if arch_var_names and not arch_context:
                    used_arch_vars = {}
                    for var_name, per_arch in arch_shell_vars.items():
                        var_ref = re.compile(r"\$\{?" + re.escape(var_name) + r"\}?\b")
                        if var_ref.search(args_str):
                            used_arch_vars[var_name] = per_arch

                    if used_arch_vars:
                        no_arch_vars = {vn: "" for vn in used_arch_vars}
                        common_resolved = _resolve_bash_expansion(args_str, {**common_vars, **no_arch_vars})
                        common_tokens_set = set(shlex.split(common_resolved))

                        all_arches = set()
                        for per_arch in used_arch_vars.values():
                            all_arches.update(per_arch.keys())
                        for arch in all_arches:
                            arch_var_vals = {vn: pa.get(arch, "") for vn, pa in used_arch_vars.items()}
                            arch_resolved = _resolve_bash_expansion(args_str, {**common_vars, **arch_var_vals})
                            arch_tokens = shlex.split(arch_resolved)
                            arch_tokens = [sub for t in arch_tokens for sub in t.split()]
                            for token in arch_tokens:
                                token = re.split(r"\s+(?:>=|<=|==|!=|>|<)\s+", token)[0].strip()
                                if (
                                    token
                                    and not token.startswith("-")
                                    and "$" not in token
                                    and not token.endswith("-")
                                    and "/" not in token
                                    and "*" not in token
                                    and token not in (">=", "<=", "==", ">", "<", "!=")
                                    and token not in common_tokens_set
                                ):
                                    arch_packages.setdefault(arch, set()).add(token)
                                    arch_resolved_tokens.add(token)

                skip_next = False
                for token in tokens:
                    if skip_next:
                        skip_next = False
                        continue
                    if token in (">=", "<=", "==", ">", "<", "!="):
                        skip_next = True
                        continue
                    token = re.split(r"\s+(?:>=|<=|==|!=|>|<)\s+", token)[0].strip()
                    if token.startswith("-"):
                        continue
                    if "$" in token:
                        continue
                    if not token or not token.strip() or token.endswith("-"):
                        continue
                    if "/" in token or "*" in token:
                        continue
                    if token in arch_resolved_tokens:
                        continue
                    if arch_context:
                        for arch in arch_context:
                            arch_packages.setdefault(arch, set()).add(token)
                    else:
                        packages.add(token)

    arch_result = {arch: sorted(pkgs) for arch, pkgs in sorted(arch_packages.items())}
    return sorted(packages), arch_result


def _extract_update_targets(run_values: list[str]) -> list[str]:
    """
    Extract package names from yum/dnf update/upgrade commands.

    These are packages already installed in the base image that need
    newer versions. They must be listed separately as upgradePackages
    in rpms.in.yaml so DNF includes them in the lockfile.

    Arg(s):
        run_values (list[str]): RUN command bodies.
    Return Value(s):
        list[str]: Sorted unique package names from update/upgrade commands.
    """
    targets: set[str] = set()
    for run_body in run_values:
        commands = _split_shell_commands(run_body)
        for cmd in commands:
            cmd = cmd.strip()
            match = re.match(r"(yum|dnf)\s+.*?\b(?:update|upgrade)\b(.*)$", cmd, re.IGNORECASE)
            if not match:
                continue
            args_str = re.sub(r"\d*>[>&]*\S*", "", match.group(2)).strip()
            try:
                tokens = shlex.split(args_str)
            except ValueError:
                tokens = args_str.split()
            for token in tokens:
                if token.startswith("-") or not token or "$" in token:
                    continue
                if re.match(r"^[\w][\w.\-]*$", token):
                    targets.add(token)
    return sorted(targets)


def _token_uses_arch_var(
    original_args: str,
    resolved_token: str,
    common_vars: dict[str, str],
    arch_shell_vars: dict[str, tuple[list[str], str]],
) -> bool:
    """
    Check if a resolved token came from an arch-conditional variable.

    Compares the full resolution (with arch vars) against common-only
    resolution.  If the token only appears in the full resolution, it
    depends on an arch-conditional variable.
    """
    common_resolved = _resolve_bash_expansion(original_args, common_vars)
    try:
        common_tokens = set(shlex.split(common_resolved))
    except ValueError:
        return False
    return resolved_token not in common_tokens


def _has_update_in_runs(run_values: list[str]) -> bool:
    """
    Check if any RUN command body contains a yum/dnf/microdnf update command.

    Arg(s):
        run_values (list[str]): RUN command bodies (instruction prefix
            already stripped by DockerfileParser).
    Return Value(s):
        bool: True if an update command was found.
    """
    for run_body in run_values:
        commands = re.split(r"&&|;", run_body)
        for cmd in commands:
            cmd = cmd.strip()
            if re.match(r"(yum|dnf|microdnf)\s+.*?\bupdate\b", cmd, re.IGNORECASE):
                return True
    return False


def _build_copy_map(
    stage_entries: list[dict],
    env_vars: dict[str, str] | None = None,
) -> dict[str, str]:
    """
    Build a mapping from container destination paths to source paths
    from COPY/ADD instructions in a Dockerfile stage.

    Handles both file and directory destinations:
        COPY hack/foo.sh /tmp   -> /tmp/foo.sh -> hack/foo.sh
        COPY hack/foo.sh /opt/renamed.sh -> /opt/renamed.sh -> hack/foo.sh

    Skips COPY --from=... (inter-stage copies) since those don't come
    from the source tree.

    Arg(s):
        stage_entries (list[dict]): DockerfileParser structure entries.
        env_vars (dict[str, str] | None): Variables for resolving
            COPY paths that contain ARG/ENV references.
    Return Value(s):
        dict[str, str]: Container path to source-relative path mapping.
    """
    variables = dict(env_vars or {})
    copy_map: dict[str, str] = {}
    for entry in stage_entries:
        if entry["instruction"] not in ("COPY", "ADD"):
            continue
        value = entry["value"]
        if "--from=" in value:
            continue
        if variables:
            value = _resolve_bash_expansion(value, variables)
        try:
            parts = shlex.split(value)
        except ValueError:
            continue
        non_flag_parts = [p for p in parts if not p.startswith("--")]
        if len(non_flag_parts) < 2:
            continue
        sources = non_flag_parts[:-1]
        dest = non_flag_parts[-1]
        for src in sources:
            src_name = Path(src).name
            # Map assuming dest is a directory: COPY hack/foo.sh /tmp -> /tmp/foo.sh
            dir_dest = dest.rstrip("/") + "/" + src_name
            copy_map[dir_dest] = src
            # Map assuming dest is an exact file path (only when dest doesn't end with /)
            if not dest.endswith("/"):
                copy_map[dest] = src
    return copy_map


def _extract_packages_from_scripts(
    run_values: list[str],
    source_dir: Path | None = None,
    copy_map: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
) -> tuple[list[str], list[str], dict[str, list[str]]]:
    """
    Find shell scripts invoked in RUN commands and extract yum/dnf
    install/update packages from them.

    Detects patterns like:
        RUN /src/install-deps.sh
        RUN ./scripts/setup.sh
        RUN . /cachi2/cachi2.env && /src/install-deps.sh
        RUN /bin/bash /tmp/dockerfile_install_support.sh
        RUN bash /opt/scripts/setup.sh

    Uses copy_map to trace container paths back to source files when
    the script was COPY'd into the image (e.g. COPY hack/foo.sh /tmp).

    Arg(s):
        run_values (list[str]): RUN command bodies.
        source_dir (Path | None): Source tree root to locate script files.
        copy_map (dict[str, str] | None): Container path to source path
            mapping from COPY/ADD instructions.
    Return Value(s):
        tuple[list[str], list[str], dict[str, list[str]]]:
            - Package names from install commands in scripts.
            - Update target names from update commands in scripts.
            - Dict mapping arch to sorted unique package names for that arch only.
    """
    if not source_dir:
        return [], [], {}

    logger = logging.getLogger(__name__)
    # Match script invocations: absolute (/path/to.sh), relative
    # (./path/to.sh), or bare (name.sh relying on PATH) — optionally
    # preceded by an interpreter (bash, sh, /bin/bash, etc.)
    script_pattern = re.compile(
        r"(?:^|&&\s*|;\s*)"
        r"(?:(?:(?:/usr)?/bin/)?(?:ba)?sh\s+)?"
        r"(/\S+\.sh|\.\/\S+\.sh|(?<!\S)[\w.-]+\.sh)"
    )
    copy_map = copy_map or {}
    all_packages: set[str] = set()
    all_updates: set[str] = set()
    all_arch_packages: dict[str, set[str]] = {}

    for run_body in run_values:
        for match in script_pattern.finditer(run_body):
            script_path = match.group(1)
            if script_path.startswith("./"):
                candidate = source_dir / script_path[2:]
            elif script_path.startswith("/src/"):
                candidate = source_dir / script_path[5:]
            elif script_path in copy_map:
                candidate = source_dir / copy_map[script_path]
            elif "/" not in script_path:
                found = None
                for container_path, src_path in copy_map.items():
                    if Path(container_path).name == script_path:
                        found = source_dir / src_path
                        break
                if found:
                    candidate = found
                else:
                    logger.debug(f"Skipping bare script {script_path}: not found in COPY map")
                    continue
            else:
                logger.debug(f"Skipping script {script_path}: unsupported path prefix and not in COPY map")
                continue

            try:
                candidate = candidate.resolve()
                if not candidate.is_relative_to(source_dir.resolve()):
                    logger.debug(f"Skipping script {script_path}: path traversal outside source_dir")
                    continue
            except (OSError, ValueError):
                continue

            if not candidate.exists():
                logger.warning(f"Script {script_path} referenced in Dockerfile but not found at {candidate}")
                continue

            logger.info(f"Extracting packages from script: {candidate}")

            try:
                script_content = candidate.read_text()
            except OSError:
                continue

            raw_lines = [line for line in script_content.splitlines() if not line.strip().startswith("#")]
            joined_lines: list[str] = []
            for line in raw_lines:
                stripped = line.rstrip()
                if joined_lines and joined_lines[-1].endswith("\\"):
                    joined_lines[-1] = joined_lines[-1][:-1] + " " + stripped.lstrip()
                else:
                    joined_lines.append(stripped)
            script_body = " && ".join(line.strip() for line in joined_lines if line.strip())

            pkgs, script_arch_pkgs = _extract_packages_from_run_commands([script_body])
            all_packages.update(pkgs)
            for arch, arch_pkgs in script_arch_pkgs.items():
                all_arch_packages.setdefault(arch, set()).update(arch_pkgs)

            updates = _extract_update_targets([script_body])
            all_updates.update(updates)

            file_pkgs, file_arch_pkgs = _extract_packages_from_file_installs(
                [script_body], copy_map, source_dir, env_vars=env_vars
            )
            all_packages.update(file_pkgs)
            for arch, arch_pkgs in file_arch_pkgs.items():
                all_arch_packages.setdefault(arch, set()).update(arch_pkgs)

    arch_result = {arch: sorted(pkgs) for arch, pkgs in sorted(all_arch_packages.items())}
    return sorted(all_packages), sorted(all_updates), arch_result


_KNOWN_ARCHES = ("x86_64", "aarch64", "ppc64le", "s390x")

_ARCH_PATH_KEYWORDS = ("$(arch)", "$(uname -m)", "$(uname -p)")


def _read_packages_from_file(source_file: Path) -> list[str]:
    """
    Read package names from a file (one per line, comments skipped).
    Allows file paths (e.g. /usr/sbin/udevadm) since DNF can resolve
    them via provides.
    """
    packages: list[str] = []
    for line in source_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pkg_name = line.split()[0]
        if not pkg_name.startswith("-") and "*" not in pkg_name:
            packages.append(pkg_name)
    return packages


def _resolve_file_from_copy_map(
    resolved_path: str,
    copy_map: dict[str, str],
    source_dir: Path,
) -> Path | None:
    """
    Look up a container path in copy_map and return the source file.
    Falls back to looking for the basename directly in source_dir
    when copy_map uses globs (e.g. COPY ${PKGS_LIST}* /tmp/).
    """
    source_file = None
    if resolved_path in copy_map:
        source_file = source_dir / copy_map[resolved_path]
    else:
        basename = Path(resolved_path).name
        for container_path, src_path in copy_map.items():
            if container_path.endswith("/" + basename):
                source_file = source_dir / src_path
                break
        if not source_file:
            candidate = source_dir / basename
            if candidate.exists():
                source_file = candidate
    if not source_file:
        return None
    try:
        source_file = source_file.resolve()
        if not source_file.is_relative_to(source_dir.resolve()):
            return None
    except (OSError, ValueError):
        return None
    return source_file if source_file.exists() else None


def _extract_packages_from_file_installs(
    run_values: list[str],
    copy_map: dict[str, str],
    source_dir: Path,
    env_vars: dict[str, str] | None = None,
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Extract package names from install commands that read packages from
    a file via stdin redirect or pipe.

    Supported patterns:
        xargs dnf install < /tmp/pkgs.txt
        grep -vE '^(#|$)' /tmp/pkgs.txt | xargs dnf install -y
        cat /tmp/pkgs.txt | xargs dnf install -y

    When the file path contains an arch keyword like $(arch), resolves
    it per-architecture and returns arch-specific packages separately.

    Arg(s):
        run_values (list[str]): RUN command bodies or joined script lines.
        copy_map (dict[str, str]): Container path to source path mapping.
        source_dir (Path): Source tree root.
        env_vars (dict[str, str] | None): Variables for path resolution.
    Return Value(s):
        tuple[list[str], dict[str, list[str]]]:
            - Sorted unique package names (common to all arches).
            - Dict mapping arch to sorted unique package names for that arch only.
    """
    logger = logging.getLogger(__name__)
    variables = dict(env_vars or {})
    redirect_re = re.compile(r"(?:xargs\s+\S+\s+)?(?:dnf|yum)\s+.*?\b(?:install)\b.*?<\s*(\S+)")
    pipe_re = re.compile(r"(?:grep|cat)\s+.*?(\S+)\s*\|\s*(?:xargs\s+\S+\s+)?(?:dnf|yum)\s+.*?\b(?:install)\b")
    packages: set[str] = set()
    arch_packages: dict[str, set[str]] = {}

    for run_body in run_values:
        for cmd in _split_shell_commands(run_body):
            cmd_clean = re.sub(r"^(if\s+!\s*|if\s+|then|else|elif|do)\s*", "", cmd.strip())
            match = redirect_re.search(cmd_clean)
            if not match:
                match = pipe_re.search(cmd_clean)
            if not match:
                continue
            raw_path = match.group(1)
            resolved_path = _resolve_bash_expansion(raw_path, variables)
            if not resolved_path:
                continue

            has_arch_keyword = any(kw in resolved_path for kw in _ARCH_PATH_KEYWORDS)
            if not has_arch_keyword and "$" in resolved_path:
                continue

            if has_arch_keyword:
                for arch in _KNOWN_ARCHES:
                    arch_path = resolved_path
                    for kw in _ARCH_PATH_KEYWORDS:
                        arch_path = arch_path.replace(kw, arch)
                    source_file = _resolve_file_from_copy_map(arch_path, copy_map, source_dir)
                    if not source_file:
                        continue
                    logger.info(f"Extracting arch-specific packages from {source_file} for {arch}")
                    try:
                        arch_packages.setdefault(arch, set()).update(_read_packages_from_file(source_file))
                    except OSError:
                        continue
            else:
                source_file = _resolve_file_from_copy_map(resolved_path, copy_map, source_dir)
                if not source_file:
                    logger.debug(f"File install path {resolved_path} not resolved from COPY map")
                    continue
                logger.info(f"Extracting packages from file install: {source_file}")
                try:
                    packages.update(_read_packages_from_file(source_file))
                except OSError:
                    continue

    arch_result = {arch: sorted(pkgs) for arch, pkgs in sorted(arch_packages.items())}
    return sorted(packages), arch_result


def analyze_dockerfile_stages(
    dockerfile_path: Path,
    source_dir: Path | None = None,
) -> tuple[list[list[str]], list[bool], list[dict[str, list[str]]], list[list[str]]]:
    """
    Parse a Dockerfile and return per-stage install packages and update flags.

    Uses DockerfileParser for instruction parsing, backslash-continuation
    joining, and stage boundary detection. Collects ARG definitions before
    the first FROM as global variables, then per-stage ARG/ENV definitions.
    All variables are used to resolve package names in install commands.

    Also detects shell scripts invoked in RUN commands and extracts
    packages from them if the script file exists in source_dir.

    Packages inside arch-conditional blocks (if [ $(arch) = X ]) are
    returned separately so they can be resolved only for the matching
    architecture.

    Arg(s):
        dockerfile_path (Path): Path to the Dockerfile.
        source_dir (Path | None): Source tree root to locate script files
            referenced in RUN commands.
    Return Value(s):
        tuple[list[list[str]], list[bool], list[dict[str, list[str]]], list[list[str]]]:
            - Per-stage sorted unique package lists from install commands.
            - Per-stage flags indicating whether an update command was found.
            - Per-stage dicts mapping arch to arch-specific package lists.
            - Per-stage sorted unique update target lists.
    """
    dfp = DockerfileParser(str(dockerfile_path))
    entries = dfp.structure

    pre_from_entries: list[dict] = []
    stage_entry_lists: list[list[dict]] = []
    current_stage: list[dict] = []
    seen_from = False

    for entry in entries:
        if entry["instruction"] == "FROM":
            if seen_from and current_stage:
                stage_entry_lists.append(current_stage)
            seen_from = True
            current_stage = [entry]
        elif seen_from:
            current_stage.append(entry)
        else:
            pre_from_entries.append(entry)

    if current_stage:
        stage_entry_lists.append(current_stage)

    global_args = _collect_stage_vars(pre_from_entries)

    stages_packages: list[list[str]] = []
    stages_has_update: list[bool] = []
    stages_arch_packages: list[dict[str, list[str]]] = []
    stages_update_targets: list[list[str]] = []

    for stage_entries in stage_entry_lists:
        stage_vars = _collect_stage_vars(stage_entries, inherited_vars=global_args)
        run_values = [e["value"] for e in stage_entries if e["instruction"] == "RUN"]
        common, arch_specific = _extract_packages_from_run_commands(run_values, env_vars=stage_vars)
        copy_map = _build_copy_map(stage_entries, env_vars=stage_vars)
        script_pkgs, script_updates, script_arch_pkgs = _extract_packages_from_scripts(
            run_values, source_dir=source_dir, copy_map=copy_map, env_vars=stage_vars
        )
        file_pkgs, file_arch_pkgs = (
            _extract_packages_from_file_installs(run_values, copy_map, source_dir, env_vars=stage_vars)
            if source_dir
            else ([], {})
        )
        common = sorted(set(common + script_pkgs + file_pkgs))
        stages_packages.append(common)
        for extra_arch_pkgs in (script_arch_pkgs, file_arch_pkgs):
            for arch, arch_pkgs in extra_arch_pkgs.items():
                existing = set(arch_specific.get(arch, []))
                existing.update(arch_pkgs)
                arch_specific[arch] = sorted(existing)
        stages_arch_packages.append(arch_specific)
        stages_has_update.append(_has_update_in_runs(run_values))
        update_targets = _extract_update_targets(run_values)
        stages_update_targets.append(sorted(set(update_targets + script_updates)))

    return stages_packages, stages_has_update, stages_arch_packages, stages_update_targets


def build_rpms_in_yaml(
    repos: list[dict],
    arches: list[str],
    packages: list[str],
    arch_specific_packages: dict[str, list[str]] | None = None,
    upgrade_packages: list[str] | None = None,
) -> dict:
    """
    Build the rpms.in.yaml structure for rpm-lockfile-prototype.

    Arg(s):
        repos (list[dict]): Repo dicts with "name" and "baseurl" keys.
        arches (list[str]): Target architectures.
        packages (list[str]): Common package names for all arches.
        arch_specific_packages (dict[str, list[str]] | None): Per-arch packages.
        upgrade_packages (list[str] | None): Packages to upgrade. These are
            already installed in the base image and need newer versions
            from repos. Listed separately so DNF includes them even when
            --image mode would skip them as already satisfied.
    Return Value(s):
        dict: rpms.in.yaml structure ready for YAML serialization.
    """
    repo_entries = [{"repoid": repo["name"], "baseurl": repo["baseurl"]} for repo in repos]

    package_entries: list[str | dict] = list(packages)
    if arch_specific_packages:
        for arch, arch_pkgs in arch_specific_packages.items():
            for pkg in arch_pkgs:
                if pkg not in packages:
                    package_entries.append(
                        {
                            "name": pkg,
                            "arches": {"only": arch},
                        }
                    )

    result: dict = {
        "arches": arches,
        "contentOrigin": {"repos": repo_entries},
        "packages": package_entries,
    }
    if upgrade_packages:
        result["upgradePackages"] = list(upgrade_packages)
    return result


class RpmLockfilePrototypeGenerator:
    """
    Generates RPM lockfiles using rpm-lockfile-prototype as the DNF
    resolution engine. Produces an ephemeral rpms.in.yaml from image
    config and Dockerfile, delegates resolution to the external tool.
    """

    def __init__(
        self,
        repos: Repos,
        runtime=None,
        logger: logging.Logger | None = None,
    ):
        self.repos = repos
        self.runtime = runtime
        self.downstream_parents: list[str] = []
        self.logger = logger or logutil.get_logger(__name__)

    def _build_env(self) -> dict:
        """
        Build environment for subprocess with registry auth if available.
        """
        env = os.environ.copy()
        registry_auth = os.environ.get("QUAY_AUTH_FILE") or os.environ.get("REGISTRY_AUTH_FILE")
        if registry_auth:
            env["REGISTRY_AUTH_FILE"] = registry_auth
        return env

    async def _resolve_to_digest(self, pullspec: str) -> str:
        """
        If pullspec uses a tag, resolve it to a digest via skopeo inspect.
        Returns the pullspec with @sha256:... instead of :tag.
        If already a digest, returns as-is.
        """
        if "@sha256:" in pullspec:
            return pullspec

        env = self._build_env()
        cmd = ["skopeo", "inspect", "--no-tags", f"docker://{pullspec}"]
        self.logger.info(f"Resolving tag to digest: {pullspec}")

        rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=env)

        if rc != 0:
            self.logger.warning(f"Failed to resolve digest for {pullspec}, using tag: {stderr[:200]}")
            return pullspec

        digest = json.loads(stdout).get("Digest", "")
        if not digest:
            self.logger.warning(f"No digest found for {pullspec}, using tag")
            return pullspec

        repo = pullspec.rsplit(":", 1)[0]
        resolved = f"{repo}@{digest}"
        self.logger.info(f"Resolved to: {resolved}")
        return resolved

    async def _get_installed_packages(self, image_pullspec: str) -> list[str]:
        """
        Query the list of installed RPM package names from a container image.

        Arg(s):
            image_pullspec (str): Fully-qualified image pullspec (digest preferred).
        Return Value(s):
            list[str]: Sorted unique package names installed in the image.
        """
        # brew.registry.redhat.io requires special auth; registry-proxy is accessible in CI
        query_pullspec = image_pullspec.replace(BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL)
        cmd = [
            "podman",
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--entrypoint",
            "rpm",
            query_pullspec,
            "-qa",
            "--qf",
            r"%{NAME}\n",
        ]
        env = self._build_env()
        rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=env)
        if rc != 0:
            raise RuntimeError(f"Failed to query installed packages from {query_pullspec}: {stderr}")
        # gpg-pubkey is an RPM pseudo-package for imported GPG keys — not installable via DNF
        return sorted(
            set(line.strip() for line in stdout.splitlines() if line.strip() and line.strip() != "gpg-pubkey")
        )

    async def _read_file_from_image(self, image_pullspec: str, filepath: str) -> str:
        """
        Read a file from a container image via podman.

        Arg(s):
            image_pullspec (str): Fully-qualified image pullspec.
            filepath (str): Absolute path to file inside the image.
        Return Value(s):
            str: File contents, or empty string on failure.
        """
        query_pullspec = image_pullspec.replace(BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL)
        cmd = [
            "podman",
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "--entrypoint",
            "cat",
            query_pullspec,
            filepath,
        ]
        env = self._build_env()
        rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=env)
        if rc != 0:
            self.logger.warning(f"Failed to read {filepath} from {query_pullspec}: {stderr[:200]}")
            return ""
        return stdout

    async def _resolve_cat_packages(
        self,
        dockerfile_path: Path,
        downstream_parents: list[str],
    ) -> dict[int, list[str]]:
        """
        Find $(cat /filepath) patterns in Dockerfile install commands and
        resolve them by reading file contents from base images via podman.

        Arg(s):
            dockerfile_path (Path): Path to Dockerfile.
            downstream_parents (list[str]): Per-stage base image pullspecs.
        Return Value(s):
            dict[int, list[str]]: Stage number to extra package names found
                via cat-file resolution.
        """
        cat_pattern = re.compile(r"\$\(\s*cat\s+(/\S+)\s*\)")
        dfp = DockerfileParser(str(dockerfile_path))
        entries = dfp.structure

        stage_runs: list[list[str]] = []
        current_runs: list[str] = []
        seen_from = False

        for entry in entries:
            if entry["instruction"] == "FROM":
                if seen_from:
                    stage_runs.append(current_runs)
                seen_from = True
                current_runs = []
            elif entry["instruction"] == "RUN" and seen_from:
                current_runs.append(entry["value"])
        if current_runs:
            stage_runs.append(current_runs)

        extra_packages: dict[int, list[str]] = {}

        for stage_num, runs in enumerate(stage_runs):
            image_pullspec = downstream_parents[stage_num] if stage_num < len(downstream_parents) else None
            if not image_pullspec or "/" not in image_pullspec:
                continue

            cat_files: set[str] = set()
            for run_body in runs:
                for match in cat_pattern.finditer(run_body):
                    cat_files.add(match.group(1))

            if not cat_files:
                continue

            stage_pkgs: list[str] = []
            for filepath in cat_files:
                content = await self._read_file_from_image(image_pullspec, filepath)
                if not content:
                    continue
                self.logger.info(f"Resolved $(cat {filepath}) from base image: {content.strip()}")
                try:
                    tokens = shlex.split(content.strip())
                except ValueError:
                    tokens = content.strip().split()
                for token in tokens:
                    token = token.strip()
                    if token and not token.startswith("-") and re.match(r"^[\w][\w.\-]*$", token):
                        stage_pkgs.append(token)

            if stage_pkgs:
                extra_packages[stage_num] = sorted(set(stage_pkgs))

        return extra_packages

    _KNOWN_ARCHES = ("x86_64", "aarch64", "ppc64le", "s390x")

    @staticmethod
    def _templatize_baseurl(baseurl: str) -> str:
        """
        Replace any known architecture string in a baseurl with $basearch.

        Some repos are configured with the same hardcoded URL (e.g. x86_64)
        for all arches. A naive replace of the requesting arch would miss
        these. This scans for any known arch string in the URL path and
        replaces it.

        Arg(s):
            baseurl (str): Concrete repo URL.
        Return Value(s):
            str: URL with the arch path component replaced by $basearch.
        """
        for arch in RpmLockfilePrototypeGenerator._KNOWN_ARCHES:
            if f"/{arch}/" in baseurl:
                return baseurl.replace(f"/{arch}/", "/$basearch/")
        return baseurl

    def _get_repoid_for_content_set(self, repo, repo_name: str, first_arch: str) -> str:
        """
        Derive a repoid that matches what cachi2 DNF options use.

        The Konflux prefetch config keys repos by content_set name per arch,
        so the lockfile repoid must match. Since rpm-lockfile-prototype
        applies DNF variable substitution on the repoid, we templatize the
        arch portion with $basearch so each per-arch resolution produces
        the correct content_set name.

        Arg(s):
            repo: Repo object from the Repos collection.
            repo_name (str): ocp-build-data repo name (fallback).
            first_arch (str): Architecture used to obtain the content_set name.
        Return Value(s):
            str: Repoid suitable for rpms.in.yaml (may contain $basearch).
        """
        try:
            content_set_id = repo.content_set(first_arch)
        except ValueError:
            content_set_id = None

        if content_set_id is None:
            return f"{repo_name}-$basearch"

        if not content_set_id:
            return repo_name

        if first_arch in content_set_id:
            return content_set_id.replace(first_arch, "$basearch")
        return content_set_id

    def _build_repo_list(self, enabled_repos: set[str], arches: list[str]) -> list[dict]:
        """
        Build repo list from Repos object for rpms.in.yaml.

        Repos where all arches resolve to the same URL (e.g. rhel-9-rt-rpms
        with all arches pointing to x86_64) keep the literal URL so that
        rpm-lockfile-prototype can download the metadata without hitting
        non-existent arch paths. DNF will skip packages that don't match
        the target arch.

        Uses content_set names as repoids so that the lockfile output matches
        the cachi2 prefetch DNF options (which key by content_set per arch).

        Arg(s):
            enabled_repos (set[str]): Repo names to include.
            arches (list[str]): Target architectures.
        Return Value(s):
            list[dict]: Repo dicts with "name" and "baseurl".
        """
        repo_list = []
        first_arch = arches[0]
        for repo_name in sorted(enabled_repos):
            try:
                repo = self.repos[repo_name]
            except ValueError:
                continue
            try:
                baseurl = repo.baseurl(repotype="unsigned", arch=first_arch)
            except ValueError:
                self.logger.warning(f"Repo {repo_name} has no baseurl for {first_arch}, skipping")
                continue

            arch_urls = set()
            for arch in arches:
                try:
                    arch_urls.add(repo.baseurl(repotype="unsigned", arch=arch))
                except ValueError:
                    pass

            if len(arch_urls) <= 1:
                baseurl_template = baseurl
            else:
                baseurl_template = self._templatize_baseurl(baseurl)

            repoid = self._get_repoid_for_content_set(repo, repo_name, first_arch)
            repo_list.append({"name": repoid, "baseurl": baseurl_template})
        return repo_list

    @staticmethod
    def _parse_missing_packages(error_text: str) -> set[str]:
        """
        Parse missing package names from rpm-lockfile-prototype error output.

        Arg(s):
            error_text (str): stderr or error message from rpm-lockfile-prototype.
        Return Value(s):
            set[str]: Set of package names that were not found.
        """
        missing: set[str] = set()
        for line in error_text.splitlines():
            m = re.search(r"missing packages:\s*(.+)", line.strip())
            if m:
                missing.update(pkg.strip() for pkg in m.group(1).split(","))
        return missing

    async def _run_prototype(
        self,
        in_file_path: Path,
        out_file_path: Path,
        image_pullspec: str | None = None,
        distgit_key: str = "",
    ) -> None:
        """
        Run rpm-lockfile-prototype for a single invocation.
        """
        cmd = ["rpm-lockfile-prototype"]
        if image_pullspec:
            cmd.extend(["--image", image_pullspec])
        else:
            cmd.append("--bare")
        cmd.extend(["--outfile", str(out_file_path), str(in_file_path)])

        env = self._build_env()
        rc, _, stderr = await cmd_gather_async(cmd, check=False, env=env)

        if rc != 0:
            raise RuntimeError(f"rpm-lockfile-prototype failed for {distgit_key} (exit code {rc}): {stderr}")

    async def generate_lockfile(
        self,
        image_meta: ImageMetadata,
        dest_dir: Path,
        filename: str = DEFAULT_RPM_LOCKFILE_NAME,
    ) -> None:
        """
        Generate an RPM lockfile using rpm-lockfile-prototype.

        For multi-stage Dockerfiles, runs the tool once per stage with the
        correct stageNum so each stage resolves against its own base image
        rpmdb. Results are merged into a single lockfile.

        Arg(s):
            image_meta (ImageMetadata): Image metadata with repo/arch config.
            dest_dir (Path): Directory containing the Dockerfile and where
                the lockfile will be written.
            filename (str): Output lockfile name.
        """
        if not image_meta.is_lockfile_generation_enabled():
            self.logger.debug(f"Lockfile generation disabled for {image_meta.distgit_key}")
            return

        enabled_repos = image_meta.get_enabled_repos()
        if not enabled_repos:
            self.logger.info(f"No enabled repos for {image_meta.distgit_key}, skipping")
            return

        arches = image_meta.get_arches()
        repo_list = self._build_repo_list(enabled_repos, arches)
        distgit_key = image_meta.distgit_key

        # Get config RPMs
        config_rpms = []
        lockfile_rpms = image_meta.config.konflux.cachi2.lockfile.get("rpms", [])
        if lockfile_rpms is not None:
            try:
                config_rpms = list(lockfile_rpms)
            except TypeError:
                config_rpms = []

        dockerfile_path = dest_dir / "Dockerfile"
        out_file_path = dest_dir / filename

        if not dockerfile_path.exists():
            all_packages = sorted(set(config_rpms))
            if not all_packages:
                self.logger.warning(f"{distgit_key}: no Dockerfile and no config rpms, skipping")
                return
            in_yaml = build_rpms_in_yaml(repo_list, arches, all_packages)
            in_file_path = dest_dir / "rpms.in.yaml"
            try:
                with open(in_file_path, "w") as f:
                    yaml.safe_dump(in_yaml, f, sort_keys=False)
                await self._run_prototype(in_file_path, out_file_path, distgit_key=distgit_key)
                self.logger.info(f"{distgit_key}: lockfile written to {out_file_path}")
            finally:
                if in_file_path.exists():
                    in_file_path.unlink()
            return

        stages_packages, stages_has_update, stages_arch_packages, stages_update_targets = analyze_dockerfile_stages(
            dockerfile_path, source_dir=dest_dir
        )
        if config_rpms and stages_packages:
            stages_packages[-1] = sorted(set(stages_packages[-1] + config_rpms))

        cat_packages = await self._resolve_cat_packages(dockerfile_path, self.downstream_parents)
        for stage_num, pkgs in cat_packages.items():
            if stage_num < len(stages_packages):
                stages_packages[stage_num] = sorted(set(stages_packages[stage_num] + pkgs))
                self.logger.info(
                    f"{distgit_key}: stage {stage_num}: added {len(pkgs)} packages from $(cat ...) resolution"
                )

        stages_to_resolve = [
            (stage_num, pkgs)
            for stage_num, pkgs in enumerate(stages_packages)
            if pkgs
            or (stage_num < len(stages_arch_packages) and stages_arch_packages[stage_num])
            or (stage_num < len(stages_has_update) and stages_has_update[stage_num])
        ]

        if not stages_to_resolve:
            self.logger.info(f"{distgit_key}: no packages or updates in any Dockerfile stage, writing empty lockfile")
            empty_lockfile = {"lockfileVersion": 1, "lockfileVendor": "redhat", "arches": []}
            with open(out_file_path, "w") as f:
                yaml.safe_dump(empty_lockfile, f, sort_keys=False)
            return

        self.logger.info(f"{distgit_key}: found {len(stages_to_resolve)} stage(s) to resolve")

        stage_lockfiles: list[dict] = []

        last_stage_num = stages_to_resolve[-1][0]

        for stage_num, packages in stages_to_resolve:
            image_pullspec = self.downstream_parents[stage_num] if stage_num < len(self.downstream_parents) else None
            if image_pullspec and "/" not in image_pullspec:
                self.logger.debug(f"{distgit_key}: stage {stage_num} parent is a stage alias, using --bare")
                image_pullspec = None
            if image_pullspec:
                image_pullspec = await self._resolve_to_digest(image_pullspec)

            if not packages:
                if not image_pullspec:
                    self.logger.warning(
                        f"{distgit_key}: stage {stage_num} is update-only but no base image available, skipping"
                    )
                    continue
                self.logger.info(
                    f"{distgit_key}: stage {stage_num} is update-only, querying installed packages from base image"
                )
                packages = await self._get_installed_packages(image_pullspec)
                if not packages:
                    self.logger.warning(
                        f"{distgit_key}: no packages found in base image for stage {stage_num}, skipping"
                    )
                    continue
                # For update-only stages, resolve with --bare so the tool returns the latest
                # available versions from the repos, not constrained by the existing image rpmdb
                # (--image would see all packages already installed and return nothing)
                image_pullspec = None

            if stage_num == last_stage_num and image_pullspec:
                self.logger.info(
                    f"{distgit_key}: final stage {stage_num} — using --bare so all packages "
                    f"appear in lockfile for hermetic builds"
                )
                image_pullspec = None

            in_file_path = dest_dir / f"rpms.stage{stage_num}.in.yaml"
            stage_out_path = dest_dir / f"rpms.stage{stage_num}.lock.yaml"
            remaining_packages = list(packages)
            arch_pkgs = stages_arch_packages[stage_num] if stage_num < len(stages_arch_packages) else {}
            update_targets = stages_update_targets[stage_num] if stage_num < len(stages_update_targets) else []

            try:
                while True:
                    self.logger.info(
                        f"{distgit_key}: resolving stage {stage_num} "
                        f"({len(remaining_packages)} packages, image={image_pullspec or 'bare'})"
                    )
                    in_yaml = build_rpms_in_yaml(
                        repo_list,
                        arches,
                        remaining_packages,
                        arch_specific_packages=arch_pkgs,
                        upgrade_packages=update_targets if image_pullspec else None,
                    )
                    with open(in_file_path, "w") as f:
                        yaml.safe_dump(in_yaml, f, sort_keys=False)

                    try:
                        await self._run_prototype(
                            in_file_path, stage_out_path, image_pullspec=image_pullspec, distgit_key=distgit_key
                        )
                        break
                    except RuntimeError as e:
                        missing = self._parse_missing_packages(str(e))
                        if not missing:
                            raise
                        self.logger.warning(
                            f"{distgit_key}: stage {stage_num}: retrying without arch-unavailable packages: "
                            f"{sorted(missing)}"
                        )
                        remaining_packages = [p for p in remaining_packages if p not in missing]
                        for arch in list(arch_pkgs.keys()):
                            arch_pkgs[arch] = [p for p in arch_pkgs[arch] if p not in missing]
                            if not arch_pkgs[arch]:
                                del arch_pkgs[arch]
                        if not remaining_packages and not arch_pkgs:
                            self.logger.warning(
                                f"{distgit_key}: stage {stage_num}: no packages remaining after filtering, skipping"
                            )
                            break

                with open(stage_out_path) as f:
                    stage_lockfiles.append(yaml.safe_load(f))

            finally:
                if in_file_path.exists():
                    in_file_path.unlink()
                if stage_out_path.exists():
                    stage_out_path.unlink()

        if len(stage_lockfiles) == 1:
            final = stage_lockfiles[0]
        else:
            final = self._merge_lockfiles(stage_lockfiles)

        final["arches"] = [
            arch_entry
            for arch_entry in final.get("arches", [])
            if arch_entry.get("packages") or arch_entry.get("source")
        ]

        if image_meta.is_cross_arch_enabled():
            self.logger.info(f"{distgit_key}: cross-architecture lockfile inclusion enabled, merging packages")
            all_packages: dict[str, dict] = {}
            all_modules: dict[str, dict] = {}
            for arch_entry in final.get("arches", []):
                for pkg in arch_entry.get("packages", []):
                    url = pkg.get("url", "")
                    if url not in all_packages:
                        all_packages[url] = pkg
                for mod in arch_entry.get("module_metadata", []):
                    key = f"{mod.get('name', '')}:{mod.get('stream', '')}:{mod.get('version', '')}"
                    if key not in all_modules:
                        all_modules[key] = mod
            merged_packages = list(all_packages.values())
            merged_modules = list(all_modules.values())
            for arch_entry in final.get("arches", []):
                arch_entry["packages"] = merged_packages
                if merged_modules:
                    arch_entry["module_metadata"] = merged_modules

        with open(out_file_path, "w") as f:
            yaml.safe_dump(final, f, sort_keys=False)

        self.logger.info(f"{distgit_key}: lockfile written to {out_file_path}")

    @staticmethod
    def _merge_lockfiles(lockfiles: list[dict]) -> dict:
        """
        Merge multiple stage lockfiles into one, deduplicating packages.
        """
        if len(lockfiles) == 1:
            return lockfiles[0]

        merged: dict = {
            "lockfileVersion": 1,
            "lockfileVendor": "redhat",
            "arches": [],
        }

        all_arches: dict[str, dict[str, dict]] = {}
        all_module_metadata: dict[str, dict[str, dict]] = {}

        for lockfile in lockfiles:
            for arch_entry in lockfile.get("arches", []):
                arch = arch_entry["arch"]
                if arch not in all_arches:
                    all_arches[arch] = {}
                    all_module_metadata[arch] = {}
                for pkg in arch_entry.get("packages", []):
                    key = pkg.get("url", pkg.get("name", ""))
                    if key not in all_arches[arch]:
                        all_arches[arch][key] = pkg
                for mod in arch_entry.get("module_metadata", []):
                    mod_key = mod.get("name", "") + ":" + mod.get("stream", "")
                    if mod_key not in all_module_metadata[arch]:
                        all_module_metadata[arch][mod_key] = mod

        for arch in sorted(all_arches.keys()):
            packages = sorted(all_arches[arch].values(), key=lambda p: p.get("url", p.get("name", "")))
            modules = sorted(all_module_metadata[arch].values(), key=lambda m: m.get("name", ""))
            merged["arches"].append(
                {
                    "arch": arch,
                    "packages": packages,
                    "module_metadata": modules,
                }
            )

        return merged
