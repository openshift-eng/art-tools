import asyncio
import os
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from pyartcd.pipelines.open_reconciliation_prs_layered import ReconcileCIUpstreamLayeredPipeline
from pyartcd.runtime import Runtime


@pytest.fixture
def pipeline():
    runtime = mock.MagicMock(spec=Runtime)
    runtime.logger = mock.MagicMock()
    runtime.doozer_working = "/workspace/doozer_working"
    return ReconcileCIUpstreamLayeredPipeline(runtime, group="mta-8.1")


@mock.patch("pyartcd.pipelines.open_reconciliation_prs_layered.exectools.cmd_gather_async", new_callable=AsyncMock)
def test_run_doozer_command_uses_quay_auth_file_for_registry_auth(cmd_gather_async, pipeline):
    cmd_gather_async.return_value = (0, "stdout", "stderr")

    with mock.patch.dict(os.environ, {"QUAY_AUTH_FILE": "/path/to/auth.json", "EXISTING_VAR": "value"}, clear=True):
        result = asyncio.run(pipeline._run_doozer_command("--group mta-8.1", "images:streams prs open"))

    assert result == (0, "stdout", "stderr")
    env = cmd_gather_async.await_args.kwargs["env"]
    assert env["QUAY_AUTH_FILE"] == "/path/to/auth.json"
    assert env["REGISTRY_AUTH_FILE"] == "/path/to/auth.json"
    assert env["EXISTING_VAR"] == "value"


@mock.patch("pyartcd.pipelines.open_reconciliation_prs_layered.exectools.cmd_gather_async", new_callable=AsyncMock)
def test_run_doozer_command_without_quay_auth_file_preserves_anonymous_behavior(cmd_gather_async, pipeline):
    cmd_gather_async.return_value = (0, "stdout", "stderr")

    with mock.patch.dict(
        os.environ, {"REGISTRY_AUTH_FILE": "/path/to/inherited-auth.json", "EXISTING_VAR": "value"}, clear=True
    ):
        asyncio.run(pipeline._run_doozer_command("--group mta-8.1", "images:streams prs open"))

    env = cmd_gather_async.await_args.kwargs["env"]
    assert "QUAY_AUTH_FILE" not in env
    assert "REGISTRY_AUTH_FILE" not in env
    assert env["EXISTING_VAR"] == "value"


def test_open_reconciliation_prs_preserves_non_base_members(pipeline):
    pipeline._run_doozer_command = AsyncMock(return_value=(0, "", ""))

    with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "token"}, clear=True):
        result = asyncio.run(pipeline._open_reconciliation_prs("--group mta-8.1"))

    assert result == 0
    pr_args = pipeline._run_doozer_command.await_args.args[2]
    assert "--preserve-non-base-members" in pr_args
