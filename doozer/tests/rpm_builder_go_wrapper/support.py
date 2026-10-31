"""Test harness for doozerlib/rpm_builder_go_wrapper.sh.

This is the RPM-side sibling of tests/golang_builder_fips_wrapper/, and
works the same way: the shim is a shell script, so it can't be unit tested
by importing it. We run the *real* script as a subprocess and stub out the
one real binary it shells out to.

The one structural difference from the container suite is where that binary
has to live. The container shim finds the compiler as ``go.real`` on PATH;
this one resolves it relative to its own location
(``${SCRIPT_DIR}/go.real``, with SCRIPT_DIR derived from BASH_SOURCE),
because of how it is deployed: rpm_builder.py inlines the whole script into
the generated spec file's %build section, which at build time writes it to
/tmp/goshim/go and symlinks the real compiler alongside it as
/tmp/goshim/go.real. So the harness reproduces that layout -- a copy of the
script in a scratch directory with the stub beside it -- rather than
putting the stub on PATH.

Unlike the container shim, this script has no SHIM_TEST short-circuit, so
there is nothing to avoid: every run exercises the whole script end to end.

Because nothing here touches the network, a real Go toolchain, or even a
real `go` binary, a few hundred invocations run in well under a second.
"""

import atexit
import json
import os
import shutil
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

BEHAVIOR_PATH = Path(__file__).resolve().parent / "behavior.yaml"

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "doozerlib" / "rpm_builder_go_wrapper.sh"

# A real `go` binary answers the probes in plain shell -- no interpreter
# startup needed -- so only the final, shim-rewritten compiler invocation
# (the fall-through at the bottom) pays for a python process, and only one
# per test. That one process is deliberately the bare `python3` resolved
# from the test's own PATH rather than sys.executable: under `uv run`,
# sys.executable is the project's venv interpreter, which carries a much
# heavier site-packages set and is an order of magnitude slower to start.
# The shebang must be the first two bytes of the file, so this is
# intentionally built flush-left rather than via textwrap.dedent, which
# can't find a common indent once the embedded python snippet (itself
# flush-left) is mixed in with the indented shell.
#
# Three probe spellings are answered rather than the two you might expect.
# `env GOVERSION` is the version probe the shim branches on, but the shim
# also dumps a bare `go.real env` into its log -- and if that fell through
# to the compiler branch it would emit a second JSON blob and the harness
# would parse the wrong call.
#
# Before answering a probe the stub appends "<query> <GOTOOLCHAIN-or-sentinel>"
# to FAKE_GO_PROBE_LOG -- a plain shell variable expansion, no subprocess --
# so a test can check what GOTOOLCHAIN looked like at the exact moment the
# shim asked for the version, rather than trusting that reading the script
# top-to-bottom got the ordering right.
_FAKE_GO_REAL = (
    "#!/bin/sh\n"
    'case "$1 $2" in\n'
    '  "env GOVERSION")\n'
    "    printf '%s %s\\n' GOVERSION \"${GOTOOLCHAIN:-__UNSET__}\" >> \"$FAKE_GO_PROBE_LOG\"\n"
    '    echo "${FAKE_GO_REAL_VERSION:-go1.24.0}"\n'
    "    exit 0\n"
    "    ;;\n"
    '  "env GOHOSTARCH")\n'
    "    printf '%s %s\\n' GOHOSTARCH \"${GOTOOLCHAIN:-__UNSET__}\" >> \"$FAKE_GO_PROBE_LOG\"\n"
    '    echo "${FAKE_GO_HOSTARCH:-amd64}"\n'
    "    exit 0\n"
    "    ;;\n"
    '  "env ")\n'
    # The shim's own logging dump: `go.real env | grep -e CGO_ENABLED ...`.
    # Answer it like a real `go env` would, so the grep has something to
    # match, but keep it off the compiler branch.
    "    printf '%s %s\\n' ENV \"${GOTOOLCHAIN:-__UNSET__}\" >> \"$FAKE_GO_PROBE_LOG\"\n"
    '    echo "CGO_ENABLED=\'${CGO_ENABLED}\'"\n'
    '    echo "GOEXPERIMENT=\'${GOEXPERIMENT}\'"\n'
    '    echo "GOFIPS140=\'${GOFIPS140}\'"\n'
    "    exit 0\n"
    "    ;;\n"
    "esac\n"
    "exec python3 -c '\n"
    "import json, os, sys\n"
    'print(json.dumps({"args": sys.argv[1:], "env": dict(os.environ)}))\n'
    'sys.exit(int(os.environ.get("FAKE_GO_EXIT_CODE", "0")))\n'
    "' \"$@\"\n"
)

# Neither the stub nor the script under test changes between tests, so the
# /tmp/goshim-alike directory is built once per session. The only thing
# that varies between runs is the environment the harness launches the
# script with.
_SHIM_DIR = tempfile.mkdtemp(prefix="rpm_goshim_")
_SHIM_SCRIPT = Path(_SHIM_DIR) / "go"
_STUB_PATH = Path(_SHIM_DIR) / "go.real"
_SHIM_SCRIPT.write_text(SCRIPT_PATH.read_text())
_STUB_PATH.write_text(_FAKE_GO_REAL)
_STUB_PATH.chmod(_STUB_PATH.stat().st_mode | stat.S_IEXEC)
atexit.register(shutil.rmtree, _SHIM_DIR, ignore_errors=True)


@dataclass
class ShimResult:
    returncode: int
    stderr: str
    real_call: Optional[Dict]  # None when go.real's compiler branch was never reached
    probed_toolchain: Dict[str, Optional[str]]  # {"GOVERSION": ..., "ENV": ...}

    @property
    def invoked_real_go(self) -> bool:
        """True once the shim got far enough to hand a command to go.real."""
        return self.real_call is not None

    @property
    def final_args(self) -> Optional[List[str]]:
        """The argv the shim ultimately handed to the compiler, after rewriting."""
        return self.real_call["args"] if self.real_call else None

    def final_env(self, name: str, default=None):
        """A single environment variable as seen by the compiler invocation."""
        return self.real_call["env"].get(name, default) if self.real_call else default

    @property
    def probed_goversion_toolchain(self) -> Optional[str]:
        """GOTOOLCHAIN as seen by the script at the moment it asked
        `go.real env GOVERSION` -- i.e. before any later rewriting, and
        independent of whatever GOTOOLCHAIN ends up being for the final
        compiler invocation."""
        return self.probed_toolchain.get("GOVERSION")


def run_shim(
    args: List[str],
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
    go_real_version: str = "go1.24.0",
    go_hostarch: str = "amd64",
) -> ShimResult:
    """Run the real wrapper script end-to-end with `go.real` stubbed out.

    `env` should contain exactly the variables a real caller would set --
    for this shim that means what rpm_builder.py bakes into the generated
    /tmp/goshim/go, plus whatever the RPM's own %build section exports.
    Nothing is inherited from the test process's own environment, so tests
    can't accidentally pass because of a stray variable left lying around
    on the machine running them.

    `go_real_version` is what the golang package in the Brew buildroot
    reports via `go env GOVERSION`. It is the only version-like input this
    script reads: it pins GOTOOLCHAIN=local, so go.mod's `go` directive
    cannot pull in a different toolchain behind its back.
    """
    probe_log_fd, probe_log_path = tempfile.mkstemp(prefix="rpm_goshim_probe_")
    os.close(probe_log_fd)
    try:
        full_env = {
            "PATH": "/usr/bin:/bin",
            "LC_ALL": "C",
            "FAKE_GO_REAL_VERSION": go_real_version,
            "FAKE_GO_HOSTARCH": go_hostarch,
            "FAKE_GO_PROBE_LOG": probe_log_path,
        }
        full_env.update(env or {})

        proc = subprocess.run(
            ["bash", str(_SHIM_SCRIPT), *args],
            env=full_env,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=5,
        )

        probed_toolchain = {}
        for line in Path(probe_log_path).read_text().splitlines():
            query, _, toolchain = line.partition(" ")
            probed_toolchain[query] = None if toolchain == "__UNSET__" else toolchain
    finally:
        os.unlink(probe_log_path)

    real_call = json.loads(proc.stdout) if proc.stdout.strip() else None
    return ShimResult(
        returncode=proc.returncode,
        stderr=proc.stderr,
        real_call=real_call,
        probed_toolchain=probed_toolchain,
    )


def load_behavior_cases() -> List[Dict]:
    """Load behavior.yaml -- the actual specification this suite tests.

    Each entry is a complete, self-contained statement of one requirement:
    given this environment and this command line, the shim must produce
    this outcome. The YAML is the spec; test_rpm_builder_go_wrapper.py is
    just a generic interpreter for it.
    """
    cases = yaml.safe_load(BEHAVIOR_PATH.read_text())
    names = [c["name"] for c in cases]
    duplicates = {n for n in names if names.count(n) > 1}
    assert not duplicates, f"duplicate case names in behavior.yaml: {duplicates}"
    return cases


def extract_tags(args: List[str]) -> Optional[List[str]]:
    """The comma-separated value of -tags/--tags/-tags=.../--tags=..., split
    into its individual tags, or None if the final command line has no tags
    flag at all."""
    for i, arg in enumerate(args):
        if arg in ("-tags", "--tags"):
            return args[i + 1].split(",")
        if arg.startswith("-tags=") or arg.startswith("--tags="):
            return arg.split("=", 1)[1].split(",")
    return None
