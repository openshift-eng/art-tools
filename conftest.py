import asyncio
import subprocess

import pytest

# Functional test suites deliberately spawn the real CLI against real external services --
# that's their entire point, unlike unit tests. Never apply the guard there; marking them
# subprocess_calls_legit would be wrong too, since that marker promises no network access.
_EXEMPT_DIR_NAMES = {'functional_tests', 'tests_functional'}


class SubprocessBlocked(RuntimeError):
    """A unit test tried to spawn a real subprocess without opting in via subprocess_calls_legit."""


def subprocess_calls_legit(reason: str):
    """Allow a test to spawn a real subprocess instead of failing on SubprocessBlocked.

    Unit tests must not depend on real external processes, network access, or shared
    machine state -- that's what makes them fast, deterministic, and safe to run
    anywhere. This decorator is a narrow, audited exception, not a general escape hatch.
    Use it only when ALL of the following are true:

    - The subprocess IS the thing under test, not an incidental side effect of a code
      path you forgot to mock. Example: a test that verifies a real shell wrapper
      script behaves correctly, or a `git`-wrapper function whose contract can only be
      verified by actually running `git`. If the subprocess call is just something the
      code under test happens to do on the way to the behavior you actually care about,
      mock it instead -- that's almost always the right fix.
    - Everything the child process touches lives under a test-owned tmp directory
      (e.g. pytest's `tmp_path`), never real system state, the developer's home
      directory, or anything outside the test's own sandbox.
    - Nothing the child process does can reach the network. No `git clone`/`fetch` of a
      real remote, no package manager, no `gcloud`/`oc`/`brew`/`rhpkg` touching a real
      service. If you're not sure, assume it can and don't use this decorator.
    - The call is fast and deterministic -- no dependence on timing, retries, or
      external availability. A flaky test under this decorator is a bug in the
      decorator's use, not in CI.

    If a test fails under the subprocess guard and you're unsure which bucket it's in,
    default to mocking (look at how sibling tests in the same file already mock
    exectools.cmd_gather/cmd_assert/cmd_assert_async) rather than reaching for this.

    :param reason: one-line justification for the audit trail, e.g.
        "runs `git init` against tmp_path to verify the real repo layout; no network".
    """
    return pytest.mark.subprocess_calls_legit(reason=reason)


_real_popen_init = subprocess.Popen.__init__
_real_create_subprocess_exec = asyncio.subprocess.create_subprocess_exec
_real_create_subprocess_shell = asyncio.subprocess.create_subprocess_shell


def _blocked_popen_init(self, args, *popen_args, **popen_kwargs):
    raise SubprocessBlocked(
        f'real subprocess spawn blocked in unit tests: {args!r}. '
        f'Mock the call, or mark the test with subprocess_calls_legit(reason=...) if spawning '
        f'a real process is genuinely the point of the test.'
    )


async def _blocked_create_subprocess_exec(program, *args, **kwargs):
    raise SubprocessBlocked(
        f'real subprocess spawn blocked in unit tests: {(program, *args)!r}. '
        f'Mock the call, or mark the test with subprocess_calls_legit(reason=...) if spawning '
        f'a real process is genuinely the point of the test.'
    )


async def _blocked_create_subprocess_shell(cmd, *args, **kwargs):
    raise SubprocessBlocked(
        f'real subprocess spawn blocked in unit tests: {cmd!r}. '
        f'Mock the call, or mark the test with subprocess_calls_legit(reason=...) if spawning '
        f'a real process is genuinely the point of the test.'
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Block real subprocess spawns for this test item, before any of its fixtures run.

    Using a setup/teardown hook pair rather than an autouse fixture matters: fixture scope
    ordering means a function-scoped autouse fixture installs its patches *after*
    session/module/class-scoped fixtures have already run, so those broader-scoped fixtures
    would be able to spawn real subprocesses unnoticed. These hooks run before every fixture
    of any scope for the item, and the matching teardown hook runs after all of them.
    """
    if _is_exempt(item) or item.get_closest_marker('subprocess_calls_legit') is not None:
        return
    subprocess.Popen.__init__ = _blocked_popen_init
    asyncio.subprocess.create_subprocess_exec = _blocked_create_subprocess_exec
    asyncio.subprocess.create_subprocess_shell = _blocked_create_subprocess_shell


def pytest_runtest_teardown() -> None:
    subprocess.Popen.__init__ = _real_popen_init
    asyncio.subprocess.create_subprocess_exec = _real_create_subprocess_exec
    asyncio.subprocess.create_subprocess_shell = _real_create_subprocess_shell


def _is_exempt(item: pytest.Item) -> bool:
    return any(part in _EXEMPT_DIR_NAMES for part in item.path.parts)
