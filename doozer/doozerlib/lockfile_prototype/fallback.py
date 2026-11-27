"""
Fallback resolution for unreachable base images.

When base and child images are built together in the same rebase batch,
the base image doesn't exist in the registry yet so podman can't pull
it to read generated files (e.g. /more-pkgs). These functions reconstruct
file contents by parsing parent Dockerfiles for echo/sed/printf redirect
commands that generate the target file.
"""

import re
import shlex
from pathlib import Path

from dockerfile_parse import DockerfileParser

_MAX_EXPANSION_DEPTH = 10
_RE_CONDITIONAL_SET = re.compile(r"\$\{(\w+):\+([^}]*)\}")
_RE_CONDITIONAL_DEFAULT = re.compile(r"\$\{(\w+):-([^}]*)\}")
_RE_BRACED_VAR = re.compile(r"\$\{(\w+)\}")
_RE_PLAIN_VAR = re.compile(r"\$(\w+)")
_RPM_OPERATION = re.compile(
    r"(?m)(?:^|&&|;)\s*(?:do\s+)?(?P<tool>dnf5?|yum|microdnf|rpm(?:-ostree)?)\s+(?P<args>[^\n;&|]+)"
)
_RPM_CHANGES = {
    "install",
    "upgrade",
    "update",
    "reinstall",
    "downgrade",
    "distro-sync",
    "groupinstall",
    "remove",
    "erase",
    "swap",
    "override",
    "uninstall",
    "groupremove",
    "autoremove",
    "--install",
    "--erase",
    "--upgrade",
    "--freshen",
}
_RPMDB_PATHS = tuple(Path(path) for path in ("/usr/share/rpm", "/usr/lib/sysimage/rpm", "/var/lib/rpm"))


def find_versionlocked_stages(source_dir: Path, entries: list[dict]) -> tuple[set[int], dict[int, int]]:
    """
    Find literal wildcard versionlocks and unchanged stages based on an earlier stage.
    """
    root = source_dir.resolve()
    stage = -1
    aliases: dict[str, int] = {}
    references: dict[int, int] = {}
    copied: dict[str, str] = {}
    commands: dict[int, list[str]] = {}
    changed_stages: set[int] = set()
    rpmdb_copies: set[int] = set()
    for entry in entries:
        instruction = entry["instruction"]
        value = entry["value"].replace("\\\n", " ")
        if instruction == "FROM":
            stage += 1
            commands[stage] = []
            copied = {}
            image, *alias = [token.lower() for token in value.split() if not token.startswith("--")]
            if image in aliases:
                references[stage] = aliases[image]
            aliases[str(stage)] = stage
            if len(alias) == 2 and alias[0] == "as":
                aliases[alias[1]] = stage
        elif instruction == "COPY" and stage >= 0:
            source, _, destination = value.strip().partition(" ")
            if source.endswith(".sh") and destination.strip():
                copied[destination.strip()] = source
            target = Path(value.rstrip().split()[-1])
            if not target.is_absolute() or any(
                database.is_relative_to(target) or target.is_relative_to(database) for database in _RPMDB_PATHS
            ):
                rpmdb_copies.add(stage)
        elif instruction == "ADD" and stage >= 0:
            rpmdb_copies.add(stage)
        elif instruction == "RUN" and stage >= 0:
            changed_stages.add(stage)
            commands[stage].append(value)
            scripts = re.finditer(
                r"(?:^|&&|;|--mount=\S+\s+)\s*(?:bash\s+)?([\w./-]+\.sh)(?=\s|$|[;&|])"
                r"|\bsed\s+[^|]+\s([\w./-]+\.sh)\s*\|\s*bash\b",
                value,
            )
            for match in scripts:
                script = match[1] or match[2]
                source = copied.get(script, script)
                for target in re.findall(r"--mount=type=bind,target=([^,\s]+)(?=\s)", value):
                    source = source.removeprefix(target.rstrip("/") + "/")
                path = (root / source).resolve()
                if path.is_relative_to(root) and path.is_file():
                    commands[stage].append(path.read_text(errors="replace"))

    locked = {
        stage
        for stage, bodies in commands.items()
        if stage not in rpmdb_copies and _preserves_installed_rpms("\n".join(bodies))
    }
    unchanged = {stage: parent for stage, parent in references.items() if stage not in changed_stages | rpmdb_copies}
    return locked, unchanged


def _preserves_installed_rpms(commands: str) -> bool:
    """
    Require every literal RPM mutation to be protected by a wildcard versionlock.
    """
    seen_lock = locked = False
    for match in _RPM_OPERATION.finditer(commands.replace("\\\n", " ")):
        tool = match["tool"]
        try:
            args = shlex.split(match["args"])
        except ValueError:
            return False
        if "versionlock" in args and tool in {"dnf", "dnf5", "yum"}:
            operation = args[args.index("versionlock") + 1 :]
            locked = operation == ["add", "*"] or (locked and operation[:1] == ["list"])
            seen_lock |= locked
        elif (operations := _RPM_CHANGES.intersection(args)) or (
            tool == "rpm" and any(re.fullmatch(r"-[iUFe][a-zA-Z]*", arg) for arg in args)
        ):
            bypasses_lock = any(arg.startswith(("--noplugins", "--disableplugin", "--disable-plugin")) for arg in args)
            if (
                not locked
                or bypasses_lock
                or tool not in {"dnf", "dnf5", "yum"}
                or operations - {"install", "reinstall", "update", "upgrade"}
                or "--allowerasing" in args
            ):
                return False
    return seen_lock


def _resolve_bash_expansion(text: str, variables: dict[str, str]) -> str:
    """
    Resolve bash-style variable expansions in text.

    Supports ${VAR:+value}, ${VAR:-default}, ${VAR}, and $VAR.
    Unresolved variables are replaced with empty string.

    Arg(s):
        text (str): Text containing variable references.
        variables (dict[str, str]): Variable name to value mapping.
    Return Value(s):
        str: Text with variables resolved.
    """
    for _ in range(_MAX_EXPANSION_DEPTH):
        prev = text
        text = _RE_CONDITIONAL_SET.sub(lambda m: m.group(2) if variables.get(m.group(1)) else "", text)
        text = _RE_CONDITIONAL_DEFAULT.sub(lambda m: variables.get(m.group(1)) or m.group(2), text)
        text = _RE_BRACED_VAR.sub(lambda m: variables.get(m.group(1), ""), text)
        text = _RE_PLAIN_VAR.sub(lambda m: variables.get(m.group(1), ""), text)
        if text == prev:
            break
    return text


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] in ("\"", "'") and value[-1] == value[0]:
        return value[1:-1]
    return value


def _collect_stage_vars(entries: list[dict], inherited_vars: dict[str, str] | None = None) -> dict[str, str]:
    """
    Collect ARG and ENV variable definitions from DockerfileParser entries.

    Arg(s):
        entries (list[dict]): DockerfileParser structure entries.
        inherited_vars (dict[str, str] | None): Variables from prior scope.
    Return Value(s):
        dict[str, str]: Accumulated variable name-value pairs.
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
                    variables[var_name] = _resolve_bash_expansion(_strip_quotes(default_value.strip()), variables)

        elif instruction == "ENV":
            env_match = re.match(r"^(\w+)(?:=|\s+)(.*)", value.strip())
            if env_match:
                var_name = env_match.group(1)
                variables[var_name] = _resolve_bash_expansion(_strip_quotes(env_match.group(2).strip()), variables)

    return variables


def extract_generated_file_content(
    parent_dir: Path,
    container_path: str,
) -> str:
    """
    Parse Dockerfiles in parent_dir for RUN commands that write to
    container_path. Resolves ARG/ENV variables to reconstruct the
    file content.

    Supports patterns like:
        echo "pkg1 pkg2" > /filepath
        sed 's/.../.../g' <<<"..." > /filepath
        printf "..." > /filepath

    Arg(s):
        parent_dir (Path): Parent image's build directory.
        container_path (str): Absolute path inside the container.
    Return Value(s):
        str: Reconstructed file content, or empty string.
    """
    escaped_path = re.escape(container_path)
    redirect_re = re.compile(rf">\s*{escaped_path}\s*$", re.MULTILINE)

    for df_name in ("Dockerfile", "Dockerfile.base", "Containerfile"):
        df_path = parent_dir / df_name
        if not df_path.is_file():
            continue

        with open(df_path, "rb") as fh:
            dfp = DockerfileParser(fileobj=fh)
            entries = dfp.structure

        variables: dict[str, str] = {}
        for entry in entries:
            if entry["instruction"] in ("ARG", "ENV"):
                variables = _collect_stage_vars([entry], inherited_vars=variables)
            elif entry["instruction"] == "RUN":
                run_body = entry["value"]
                if not redirect_re.search(run_body):
                    continue
                # Handle here-string: sed 's/x/y/g' <<<"content" > /path
                heredoc_match = re.search(r'<<<\s*"([^"]*)"', run_body)
                if heredoc_match:
                    raw_content = heredoc_match.group(1)
                    resolved = _resolve_bash_expansion(raw_content, variables)
                    sed_match = re.search(r"sed\s+'s/([^/]+)/([^/]*)/g'", run_body)
                    if sed_match:
                        resolved = resolved.replace(sed_match.group(1), sed_match.group(2))
                    return resolved
                # Handle echo/printf: echo "content" > /path
                echo_match = re.search(r'(?:echo|printf)\s+["\']?([^"\'>\n]+)["\']?\s*>', run_body)
                if echo_match:
                    raw_content = echo_match.group(1).strip()
                    return _resolve_bash_expansion(raw_content, variables)

    return ""
