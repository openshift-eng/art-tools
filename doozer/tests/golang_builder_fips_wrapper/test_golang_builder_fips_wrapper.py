"""End-to-end behavior test for doozerlib/golang_builder_FIPS_wrapper.sh.

This shim replaces `go` in the PATH of golang builder images. It inspects
the compiler invocation it was handed and, when running an ART product
build, rewrites it to be FIPS-compliant before handing it to the real `go`
binary (installed alongside it as `go.real`).

The actual specification lives in behavior.yaml, as a list of concrete
examples: given this environment and this command line, the shim must
produce this outcome. This file is a single, generic interpreter for that
spec -- it has no opinions of its own about what the shim should do, so
reading behavior.yaml top to bottom is the right way to learn how the shim
actually behaves. See support.py for how each case is actually run (the
real script, as a subprocess, with only `go.real` stubbed out).
"""

import pytest

from .support import extract_tags, load_behavior_cases, run_shim

CASES = load_behavior_cases()


def _case_id(case):
    return case["name"]


@pytest.mark.parametrize("case", CASES, ids=_case_id)
def test_shim_behavior(case, tmp_path):
    description = case["description"].strip()

    cwd = None
    for rel_path in case.get("cwd_files", []):
        target = tmp_path / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("")
        cwd = str(tmp_path)

    result = run_shim(
        case["args"],
        env=case.get("env", {}),
        cwd=cwd,
        go_real_version=case.get("go_real_version", "go1.24.0"),
        go_hostarch=case.get("go_hostarch", "amd64"),
    )

    expect = case["expect"]
    expect_invoked = expect.get("invoked", True)
    assert result.invoked_real_go == expect_invoked, description

    expect_exit_code = expect.get("exit_code", 0 if expect_invoked else 1)
    assert result.returncode == expect_exit_code, description

    if "stderr_contains" in expect:
        assert expect["stderr_contains"] in result.stderr, description

    if "args" in expect:
        assert result.final_args == expect["args"], description

    for token in expect.get("args_contains", []):
        assert token in result.final_args, f"{description}: expected {token!r} in {result.final_args}"

    for token in expect.get("args_not_contains", []):
        assert token not in result.final_args, f"{description}: did not expect {token!r} in {result.final_args}"

    if expect.get("no_tags"):
        assert extract_tags(result.final_args) is None, description

    tags_include = expect.get("tags_include", [])
    tags_exclude = expect.get("tags_exclude", [])
    if tags_include or tags_exclude:
        tags = extract_tags(result.final_args) or []
        for tag in tags_include:
            assert tag in tags, f"{description}: expected tag {tag!r} in {tags}"
        for tag in tags_exclude:
            assert tag not in tags, f"{description}: did not expect tag {tag!r} in {tags}"

    for name, value in expect.get("env", {}).items():
        assert result.final_env(name) == value, f"{description}: {name}"

    if "probed_goversion_toolchain" in expect:
        assert result.probed_goversion_toolchain == expect["probed_goversion_toolchain"], description
