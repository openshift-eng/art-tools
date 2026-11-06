"""Test harness for doozerlib/golang_builder_FIPS_wrapper.sh.

The shim is a shell script, not Python, so it can't be unit tested by
importing it. Instead we run the *real* script as a subprocess, for a true
end-to-end test -- the only thing replaced is ``go.real``, the one real
binary the script shells out to (the Dockerfile that deploys this shim is
responsible for making sure a real compiler is reachable under that name;
our stand-in makes that promise true in the test sandbox without needing an
actual Go toolchain).

The script has a built-in ``SHIM_TEST=1`` short-circuit that fakes the Go
version and prints a summary instead of calling ``go.real`` at all. We
deliberately avoid it here: it skips both the real "go env" detection calls
and the real final invocation, which is exactly the part of the script this
suite wants to exercise. So SHIM_TEST is left unset, and ``go.real`` is
stubbed on PATH instead:

* ``go.real env GOVERSION`` / ``go.real env GOHOSTARCH`` -- the two calls
  the script makes to probe the toolchain -- return canned values, and also
  record what GOTOOLCHAIN looked like at that exact moment (see
  ``probed_goversion_toolchain`` below -- this is what lets a test prove the
  script's own ``GOTOOLCHAIN=local`` forcing already took effect before the
  version probe, rather than just trusting that reading the script's source
  top-to-bottom got the ordering right).
* Any other invocation is treated as the final compiler call the script
  makes once it has finished rewriting the command line and environment.
  The stub records the argv and environment it was handed, as JSON, on
  stdout, instead of building anything.

Note what this harness deliberately does *not* model: go.mod's `go`
directive and a repo's `.go-version` file. Neither is ever read by this
script -- its only input is `go.real env GOVERSION`, i.e. go_real_version
below. A case that wants to make that point (e.g. "a stale go.mod doesn't
change the outcome") does so by simply not needing a go.mod to produce the
documented result, not by adding a knob that would misrepresent the script
as caring.

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

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "doozerlib" / "golang_builder_FIPS_wrapper.sh"

# A real `go` binary always answers these two probes in plain shell -- no
# interpreter startup needed -- so only the final, shim-rewritten compiler
# invocation (the "else" branch) pays for a python process, and only one
# per test instead of three. That one process is deliberately the bare
# `python3` resolved from the test's own PATH rather than sys.executable:
# under `uv run`, sys.executable is the project's venv interpreter, which
# carries a much heavier site-packages set and is an order of magnitude
# slower to start than a plain interpreter -- a cost paid on every single
# test otherwise. The shebang must be the first two bytes of the file, so
# this is intentionally built flush-left rather than via textwrap.dedent,
# which can't find a common indent once the embedded python snippet
# (itself flush-left) is mixed in with the indented shell.
# Before answering a probe, it appends "<query> <GOTOOLCHAIN-or-sentinel>" to
# FAKE_GO_PROBE_LOG -- a plain shell variable expansion, no subprocess -- so
# the harness can later check what GOTOOLCHAIN looked like at the exact
# moment the script asked for the version, not just at the end.
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
    "esac\n"
    "exec python3 -c '\n"
    "import json, os, sys\n"
    'print(json.dumps({"args": sys.argv[1:], "env": dict(os.environ)}))\n'
    'sys.exit(int(os.environ.get("FAKE_GO_EXIT_CODE", "0")))\n'
    "' \"$@\"\n"
)

# The stub never changes between tests, so it's written to disk once per
# test session rather than once per test -- the only thing that varies
# between runs is the environment the harness launches the real script
# with, not the stub itself.
_STUB_DIR = tempfile.mkdtemp(prefix="go_real_stub_")
_STUB_PATH = Path(_STUB_DIR) / "go.real"
_STUB_PATH.write_text(_FAKE_GO_REAL)
_STUB_PATH.chmod(_STUB_PATH.stat().st_mode | stat.S_IEXEC)
atexit.register(shutil.rmtree, _STUB_DIR, ignore_errors=True)


@dataclass
class ShimResult:
    returncode: int
    stderr: str
    real_call: Optional[Dict]  # None when go.real's compiler branch was never reached
    probed_toolchain: Dict[str, Optional[str]]  # {"GOVERSION": ..., "GOHOSTARCH": ...}

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

    `env` should contain exactly the variables a real caller would set (an
    ART Dockerfile, OpenShift CI, or a developer's shell) -- nothing is
    inherited from the test process's own environment, so tests can't
    accidentally pass because of a stray variable left lying around on the
    machine running them.

    `go_real_version` is deliberately named for what it is: the version the
    *installed toolchain* reports via `go env GOVERSION`. It is the only
    version-like input this script ever reads -- see the module docstring
    for why go.mod and .go-version aren't separate knobs here.
    """
    probe_log_fd, probe_log_path = tempfile.mkstemp(prefix="go_real_probe_")
    os.close(probe_log_fd)
    try:
        full_env = {
            "PATH": f"{_STUB_DIR}:/usr/bin:/bin",
            "LC_ALL": "C",
            "FAKE_GO_REAL_VERSION": go_real_version,
            "FAKE_GO_HOSTARCH": go_hostarch,
            "FAKE_GO_PROBE_LOG": probe_log_path,
        }
        full_env.update(env or {})

        proc = subprocess.run(
            ["bash", str(SCRIPT_PATH), *args],
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


# Shorthand for the environment of a typical ART product build: doozer sets
# __doozer_group to the OCP group being built, which is what turns on the
# default compliance policy (as opposed to a plain developer build, which is
# exempt from everything -- see exempt_all in the script).
def art_build_env(group: str = "openshift-4.20", **extra: str) -> Dict[str, str]:
    return {"__doozer_group": group, **extra}


def load_behavior_cases() -> List[Dict]:
    """Load behavior.yaml -- the actual specification this suite tests.

    Each entry is a complete, self-contained statement of one requirement:
    given this environment and this command line, the shim must produce
    this outcome. The YAML is the spec; test_golang_builder_fips_wrapper.py
    is just a generic interpreter for it.
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
