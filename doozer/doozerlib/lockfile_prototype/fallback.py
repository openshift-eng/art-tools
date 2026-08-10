"""
Fallback resolution for unreachable base images.

When base and child images are built together in the same rebase batch,
the base image doesn't exist in the registry yet so podman can't pull
it to read generated files (e.g. /more-pkgs). These functions reconstruct
file contents by parsing parent Dockerfiles for echo/sed/printf redirect
commands that generate the target file.
"""

import re
from pathlib import Path

from dockerfile_parse import DockerfileParser

_MAX_EXPANSION_DEPTH = 10
_RE_CONDITIONAL_SET = re.compile(r"\$\{(\w+):\+([^}]*)\}")
_RE_CONDITIONAL_DEFAULT = re.compile(r"\$\{(\w+):-([^}]*)\}")
_RE_BRACED_VAR = re.compile(r"\$\{(\w+)\}")
_RE_PLAIN_VAR = re.compile(r"\$(\w+)")


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
