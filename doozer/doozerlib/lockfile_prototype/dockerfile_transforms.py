"""
Dockerfile text-level transformations for rpm-lockfile-prototype builds.

Applied during rebase when the lockfile backend is rpm-lockfile-prototype
to fix incompatibilities between package names in install commands and the
actual names recorded in the rpmdb (e.g. virtual provides, package renames).
"""

import logging
import re
from pathlib import Path


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
