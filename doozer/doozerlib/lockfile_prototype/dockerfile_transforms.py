"""
Dockerfile text-level transformations for rpm-lockfile-prototype builds.

Applied during rebase when the lockfile backend is rpm-lockfile-prototype
to fix incompatibilities between package names in install commands and the
actual names recorded in the rpmdb (e.g. virtual provides, package renames).
"""

import json
import logging
import re
import unicodedata
from io import StringIO
from pathlib import Path

import bashlex
import bashlex.errors
from dockerfile_parse import DockerfileParser

_RUN_INSTRUCTION_RE = re.compile(r"^(?P<prefix>[ \t]*RUN[ \t]+)(?P<body>.*)$", re.DOTALL | re.IGNORECASE)
_INSTALLROOT_OPTION_RE = re.compile(r"^--installroot$")
_INSTALLROOT_ARGUMENT_RE = re.compile(r"^--installroot=(?P<root>[^\s;&|\\]+)$")
_PACKAGE_MANAGER_RE = re.compile(r"^(?:microdnf|dnf|yum)$")
_RPM_GPG_KEY_PATH = "/etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release"


def add_installroot_gpg_key_import(df_content: str) -> str:
    """
    Import the Red Hat RPM GPG key before installing into an empty installroot.

    The RPM database in a newly created installroot has no imported GPG keys,
    even though the key file exists in the image filesystem. Existing roots,
    such as bootc roots, are left unchanged when they contain any files.

    Arg(s):
        df_content (str): Raw Dockerfile text.
    Return Value(s):
        str: Transformed Dockerfile text with conditional GPG key imports.
    """

    normalized_content = unicodedata.normalize("NFC", df_content)
    parser = DockerfileParser(fileobj=StringIO(normalized_content))
    lines = normalized_content.splitlines(keepends=True)
    line_offsets = [0]
    for line in lines:
        line_offsets.append(line_offsets[-1] + len(line))

    replacements: list[tuple[int, int, str]] = []
    for entry in parser.structure:
        if entry["instruction"] != "RUN":
            continue

        startline = entry["startline"]
        endline = entry["endline"]
        start_offset = line_offsets[startline]
        end_offset = line_offsets[endline + 1]
        original = normalized_content[start_offset:end_offset]
        transformed = _transform_shell_run_instruction(original)
        if transformed != original:
            replacements.append((start_offset, end_offset, transformed))

    for start_offset, end_offset, replacement in reversed(replacements):
        normalized_content = normalized_content[:start_offset] + replacement + normalized_content[end_offset:]

    return normalized_content


def _transform_shell_run_instruction(instruction: str) -> str:
    """
    Transform package-manager commands in one shell-form RUN instruction.

    Arg(s):
        instruction (str): Raw Dockerfile RUN instruction.
    Return Value(s):
        str: RUN instruction with conditional installroot key imports.
    """
    instruction_match = _RUN_INSTRUCTION_RE.fullmatch(instruction)
    if instruction_match is None:
        return instruction

    prefix = instruction_match.group("prefix")
    body = instruction_match.group("body")
    command_start, shell_body = _strip_run_options(body)
    if _is_exec_form_run(shell_body):
        return instruction

    transformed_body = _transform_shell_run_body(shell_body)
    if transformed_body == shell_body:
        return instruction
    return prefix + body[:command_start] + transformed_body


def _strip_run_options(body: str) -> tuple[int, str]:
    """
    Find the shell command after Dockerfile-specific RUN options.

    Arg(s):
        body (str): Text following the RUN instruction keyword.
    Return Value(s):
        tuple[int, str]: Offset and text of the shell command.
    """
    offset = 0
    while True:
        while offset < len(body) and body[offset] in " \t":
            offset += 1
        if not body.startswith("--", offset):
            return offset, body[offset:]

        option_end = offset
        while option_end < len(body) and body[option_end] not in " \t\n":
            option_end += 1
        offset = option_end


def _is_exec_form_run(body: str) -> bool:
    """
    Return whether a RUN body uses Docker's JSON array form.

    Arg(s):
        body (str): RUN body after Dockerfile-specific options.
    Return Value(s):
        bool: True when the body parses as a JSON array.
    """
    try:
        parsed = json.loads(body.strip())
    except json.JSONDecodeError:
        return False
    return isinstance(parsed, list)


def _transform_shell_run_body(shell_body: str) -> str:
    """
    Add GPG key imports before executable package-manager commands.

    Arg(s):
        shell_body (str): Shell-form RUN body.
    Return Value(s):
        str: Transformed shell command text.
    """
    try:
        nodes = bashlex.parse(shell_body, strictmode=False)
    except bashlex.errors.ParsingError:
        return shell_body

    command_nodes: list = []
    for node in nodes:
        _append_command_nodes(node, command_nodes)

    insertions: list[tuple[int, str]] = []
    for command in command_nodes:
        parts = getattr(command, "parts", [])
        if not parts or parts[0].kind != "word":
            continue
        command_name = parts[0].word.rsplit("/", 1)[-1]
        if _PACKAGE_MANAGER_RE.fullmatch(command_name) is None:
            continue

        root = _find_installroot(parts[1:])
        if root is None:
            continue

        guard = _installroot_gpg_key_guard(root)
        if shell_body[: command.pos[0]].endswith(guard):
            continue
        insertions.append((command.pos[0], guard))

    for position, guard in reversed(sorted(insertions)):
        shell_body = shell_body[:position] + guard + shell_body[position:]
    return shell_body


def _append_command_nodes(node, command_nodes: list) -> None:
    """
    Collect shell command nodes from a bashlex syntax tree.

    Arg(s):
        node: Current bashlex syntax-tree node.
        command_nodes (list): List receiving command nodes.
    """
    if node.kind == "command":
        command_nodes.append(node)
    for attribute in ("parts", "list"):
        for child in getattr(node, attribute, []) or []:
            _append_command_nodes(child, command_nodes)


def _find_installroot(parts: list) -> str | None:
    """
    Find an installroot path in package-manager argument nodes.

    Arg(s):
        parts (list): Bashlex word nodes after the package-manager command.
    Return Value(s):
        str | None: Installroot path, or None when the option is absent.
    """
    for index, part in enumerate(parts):
        if part.kind != "word":
            continue
        argument = unicodedata.normalize("NFC", part.word)
        argument_match = _INSTALLROOT_ARGUMENT_RE.fullmatch(argument)
        if argument_match is not None:
            return argument_match.group("root")
        if _INSTALLROOT_OPTION_RE.fullmatch(argument) and index + 1 < len(parts):
            next_part = parts[index + 1]
            if next_part.kind == "word":
                return unicodedata.normalize("NFC", next_part.word)
    return None


def _installroot_gpg_key_guard(root: str) -> str:
    """
    Build a shell guard that imports the Red Hat RPM GPG key for an empty root.

    Arg(s):
        root (str): Installroot path.
    Return Value(s):
        str: Shell guard followed by a command separator.
    """
    return (
        f"if [ -d {root} ] && [ -z \"$(ls -A {root})\" ]; then rpm --root {root} --import {_RPM_GPG_KEY_PATH}; fi && "
    )


def strip_bare_updates(df_content: str) -> str:
    """
    Remove bare dnf/yum update commands from a Dockerfile.

    In hermetic builds the lockfile pins exact RPM versions, so bare
    updates are redundant. They also fail because the build container
    cannot reach external repos (e.g. cdn-ubi.redhat.com).

    Only strips updates without named packages. Named updates like
    ``dnf update -y openssl`` are left intact.

    Arg(s):
        df_content (str): Raw Dockerfile text.
    Return Value(s):
        str: Transformed Dockerfile text with bare updates removed.
    """
    bare_update_re = re.compile(
        r"\b(?:microdnf|dnf|yum)\s+(?:-y\s+)?(?:update|upgrade)(?:\s+-y)?\s*(?:\\\n\s*&&\s*|&&\s*|;\s*|\n|(?=$))",
    )
    return bare_update_re.sub("", df_content)


def strip_bare_updates_from_scripts(
    dest_dir: Path,
    logger: logging.Logger | None = None,
) -> None:
    """
    Walk dest_dir for shell scripts and strip bare yum/dnf update
    commands from each. Scripts invoked from Dockerfile RUN commands
    (e.g. install-python-deps-ocp.sh) can contain bare updates that
    fail in hermetic builds.

    Arg(s):
        dest_dir (Path): Build directory containing source files.
        logger (logging.Logger | None): Logger instance.
    """
    for script in dest_dir.rglob("*.sh"):
        if not script.is_file():
            continue
        original = script.read_text()
        modified = strip_bare_updates(original)
        if modified != original:
            script.write_text(modified)
            if logger:
                logger.debug(f"Stripped bare updates from {script.relative_to(dest_dir)}")


def transform_reinstall_commands(df_content: str) -> str:
    """
    Make microdnf/dnf/yum reinstall commands fail-safe for hermetic builds.

    In hermetic builds the installed NEVRA may not be available in the
    lockfile repos, so ``reinstall`` can fail with "Installed package
    not available". Rather than stripping the command entirely (which
    drops semantically important re-extractions like ``reinstall
    tzdata``), wrap each reinstall invocation in ``(cmd || true)`` so
    it succeeds when the NEVRA matches and degrades gracefully when it
    does not.

    Arg(s):
        df_content (str): Raw Dockerfile text.
    Return Value(s):
        str: Transformed Dockerfile text with reinstall commands wrapped.
    """
    reinstall_re = re.compile(
        r"(\b(?:microdnf|dnf|yum)\s+(?:-\w+\s+)*reinstall\b[^&|;\\\n]*(?:\\\n[^&|;\\\n]*)*)"
        r"(\s*&&\s*|\s*;\s*)?",
    )

    def _wrap(m: re.Match) -> str:
        cmd = m.group(1).rstrip()
        if cmd.endswith("\\"):
            cmd = cmd[:-1].rstrip()
        sep = m.group(2)
        rest = m.string[m.end() :]
        if not sep and rest.lstrip().startswith("||"):
            return m.group(0)
        if sep:
            return f"({cmd} || true) {sep.lstrip()}"
        return f"({cmd} || true)"

    return reinstall_re.sub(_wrap, df_content)


def fix_rpm_verify_commands(df_content: str) -> str:
    """
    Transform rpm -V commands in Dockerfile RUN instructions so that
    package names are resolved to their actual installed names at build
    time via rpm --whatprovides.

    rpm -V fails when a package is installed under a different name via
    a virtual provide (e.g. bind-utils installed as bind9.18-utils in
    RHEL 9). yum install bind-utils succeeds because DNF resolves the
    virtual provide, but the rpmdb entry is named bind9.18-utils, so
    rpm -V bind-utils fails with "package bind-utils is not installed".

    Transforms every occurrence of:
        rpm -V [--flags] $PKGS
    to:
        rpm -V [--flags] $(for _art_pkg in $PKGS; do
            rpm -q --qf '%{NAME}\\n' --whatprovides "$_art_pkg" 2>/dev/null | head -1
            || echo "$_art_pkg"; done)

    The shell loop resolves each package name/path to its installed RPM
    name before verification, so the correct name is always used.

    Arg(s):
        df_content (str): Raw Dockerfile text.
    Return Value(s):
        str: Transformed Dockerfile text with rpm -V commands fixed.
    """
    rpm_v_re = re.compile(
        r"\brpm\s+-V\b"
        r"((?:[ \t]+--[\w-]+(?:=\S+)?)*)"  # optional --flags (group 1)
        r"((?:[ \t]+(?!--)(?![ \t])[^ \t\n&|;\\]+)+)"  # package args (group 2), same line only
    )

    def _replace(m: re.Match) -> str:
        flags = m.group(1)  # e.g. " --nogroup --nosize --nofiledigest --nomtime --nomode"
        pkgs = m.group(2).strip()  # e.g. "$INSTALL_PKGS" or "bind-utils wget"
        # rpm -q errors ("no package provides ...") go to stdout, not stderr,
        # so piping through head -1 always exits 0 and || never triggers.
        # Use variable assignment + exit code chain instead:
        # 1. Try rpm -q by name (handles name-version like llvm-toolset-19.1.7)
        # 2. Try rpm -q --whatprovides (handles virtual provides like bind-utils)
        # 3. Fall back to original name
        resolve_loop = (
            "$(for _art_pkg in " + pkgs + "; do "
            '_art_name=$(rpm -q --qf \'%{NAME}\\n\' "$_art_pkg" 2>/dev/null) || '
            '_art_name=$(rpm -q --qf \'%{NAME}\\n\' --whatprovides "$_art_pkg" 2>/dev/null) || '
            '_art_name=$_art_pkg; echo "$_art_name" | head -1; done)'
        )
        return "rpm -V" + flags + " " + resolve_loop

    return rpm_v_re.sub(_replace, df_content)
