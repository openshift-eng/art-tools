"""Test harness for doozerlib/golang_builder_FIPS_wrapper.sh (main, pre-PR #3435).

The shim is a shell script, not Python, so it can't be unit tested by
importing it. Instead we run the *real* script as a subprocess, for a true
end-to-end test -- the only thing replaced is ``go.real``, the one real
binary the script shells out to (the Dockerfile that deploys this shim is
responsible for making sure a real compiler is reachable under that name;
our stand-in makes that promise true in the test sandbox without needing an
actual Go toolchain).

The script has a built-in ``SHIM_TEST=1`` short-circuit that fakes the host
arch and prints a summary instead of calling ``go.real`` at all. We
deliberately avoid it here: it skips both the real "go env" detection call
and the real final invocation, which is exactly the part of the script this
suite wants to exercise. So SHIM_TEST is left unset, and ``go.real`` is
stubbed on PATH instead:

* ``go.real env GOHOSTARCH`` -- the only probe this version of the script
  makes (there is no Go-version detection on main at all -- that's added by
  PR #3435) -- returns a canned value.
* Any other invocation is treated as the final compiler call the script
  makes once it has finished rewriting the command line and environment.
  The stub records the argv and environment it was handed, as JSON, on
  stdout, instead of building anything.

Because nothing here touches the network, a real Go toolchain, or even a
real `go` binary, a few hundred invocations run in well under a second.
"""

import atexit
import json
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

# A real `go` binary always answers this probe in plain shell -- no
# interpreter startup needed -- so only the final, shim-rewritten compiler
# invocation (the "else" branch) pays for a python process, and only one per
# test. That one process is deliberately the bare `python3` resolved from
# the test's own PATH rather than sys.executable: under `uv run`,
# sys.executable is the project's venv interpreter, which carries a much
# heavier site-packages set and is an order of magnitude slower to start
# than a plain interpreter -- a cost paid on every single test otherwise.
# The shebang must be the first two bytes of the file, so this is
# intentionally built flush-left rather than via textwrap.dedent, which
# can't find a common indent once the embedded python snippet (itself
# flush-left) is mixed in with the indented shell.
_FAKE_GO_REAL = (
    "#!/bin/sh\n"
    'case "$1 $2" in\n'
    '  "env GOHOSTARCH")\n'
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


def run_shim(
    args: List[str],
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
    go_hostarch: str = "amd64",
) -> ShimResult:
    """Run the real wrapper script end-to-end with `go.real` stubbed out.

    `env` should contain exactly the variables a real caller would set (an
    ART Dockerfile, OpenShift CI, or a developer's shell) -- nothing is
    inherited from the test process's own environment, so tests can't
    accidentally pass because of a stray variable left lying around on the
    machine running them.
    """
    full_env = {
        "PATH": f"{_STUB_DIR}:/usr/bin:/bin",
        "LC_ALL": "C",
        "FAKE_GO_HOSTARCH": go_hostarch,
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

    real_call = json.loads(proc.stdout) if proc.stdout.strip() else None
    return ShimResult(returncode=proc.returncode, stderr=proc.stderr, real_call=real_call)


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
