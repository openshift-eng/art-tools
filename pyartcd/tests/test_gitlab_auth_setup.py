import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from artcommonlib.github_auth import build_git_auth_env
from pyartcd.pipelines.binary_release_konflux import BinaryReleaseKonfluxPipeline
from pyartcd.pipelines.build_microshift_bootc import BuildMicroShiftBootcPipeline
from pyartcd.pipelines.prepare_release_lp import PrepareReleaseLPPipeline
from pyartcd.pipelines.release_from_fbc import ReleaseFromFbcPipeline


@pytest.mark.parametrize(
    "setup_method",
    [
        ReleaseFromFbcPipeline.setup_shipment_repo,
        BinaryReleaseKonfluxPipeline.setup_shipment_repo,
        PrepareReleaseLPPipeline._setup_shipment_repo,
    ],
)
def test_shipment_repo_setup_uses_clean_url_and_gitlab_askpass(setup_method):
    token = "glpat_pipeline_setup_test_secret"
    push_url = "https://gitlab.example.com/user/ocp-shipment-data.git"
    pull_url = "https://gitlab.example.com/org/ocp-shipment-data.git"
    repo = SimpleNamespace(setup=AsyncMock(), fetch_switch_branch=AsyncMock())
    pipeline = SimpleNamespace(
        create_mr=True,
        shipment_data_repo=repo,
        shipment_data_repo_push_url=push_url,
        shipment_data_repo_pull_url=pull_url,
        gitlab_token=token,
    )

    asyncio.run(setup_method(pipeline))

    repo.setup.assert_awaited_once_with(
        remote_url=push_url,
        upstream_remote_url=pull_url,
        remote_auth_envs={"origin": build_git_auth_env(token, username="oauth2")},
    )
    repo.fetch_switch_branch.assert_awaited_once_with("main")


def test_build_microshift_setup_uses_clean_url_and_gitlab_askpass():
    token = "glpat_microshift_setup_test_secret"
    push_url = "https://gitlab.example.com/user/ocp-shipment-data.git"
    pull_url = "https://gitlab.example.com/org/ocp-shipment-data.git"
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir) / "shipment-data"
        repo = SimpleNamespace(setup=AsyncMock(), fetch_switch_branch=AsyncMock())
        pipeline = SimpleNamespace(
            _logger=MagicMock(),
            _shipment_data_repo_dir=repo_path,
            runtime=SimpleNamespace(dry_run=False),
            shipment_data_repo_push_url=push_url,
            shipment_data_repo_pull_url=pull_url,
            gitlab_token=token,
        )

        with patch("pyartcd.pipelines.build_microshift_bootc.GitRepository", return_value=repo):
            asyncio.run(BuildMicroShiftBootcPipeline._setup_shipment_data_repo(pipeline))

    repo.setup.assert_awaited_once_with(
        remote_url=push_url,
        upstream_remote_url=pull_url,
        remote_auth_envs={"origin": build_git_auth_env(token, username="oauth2")},
    )
    repo.fetch_switch_branch.assert_awaited_once_with("main")
