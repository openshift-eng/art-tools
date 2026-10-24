import asyncio
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import ShipmentConfig
from gitlab.exceptions import GitlabCreateError
from pyartcd.git import GitRepository
from pyartcd.lp_shipment import (
    FbcRefreshRecord,
    ShipmentMRActiveStageError,
    ShipmentMRProductionError,
    ShipmentMRScopeError,
    ShipmentMRValidationError,
    _identity,
    add_fbc_refresh_comment,
    add_superseded_mr_comment,
    close_superseded_shipment_mr,
    create_shipment_mr_with_retry,
    get_shipment_mr_url,
    inspect_shipment_mr_ci_state,
    reconcile_shipment_mr,
    resolve_shipment_release_plans,
    set_shipment_mr_draft,
    update_shipment_mr_url,
    validate_shipment_mr,
    validate_shipment_mr_ci_state,
    validate_shipment_mr_for_operation,
    validate_shipment_mr_reuse_state,
)

YAML = new_roundtrip_yaml_handler()


def test_resolve_shipment_release_plans_from_main_config():
    """Return both registered plans from the canonical shipment config."""
    with TemporaryDirectory() as directory:
        config_path = Path(directory, "config.yaml")
        config_path.write_text(
            """applications:
  quay-3-18:
    environments:
      stage:
        releasePlan: quay-advisory-stage-3-18
      prod:
        releasePlan: quay-advisory-prod-3-18
"""
        )

        assert resolve_shipment_release_plans(GitRepository(directory), "quay-3-18") == (
            "quay-advisory-stage-3-18",
            "quay-advisory-prod-3-18",
        )


@pytest.mark.parametrize(
    "config",
    [
        "applications: {}\n",
        """applications:
  quay-3-18:
    environments:
      stage:
        releasePlan: n/a
      prod:
        releasePlan: quay-advisory-prod-3-18
""",
    ],
)
def test_resolve_shipment_release_plans_rejects_unresolved_config(config):
    """Never bake an absent or placeholder ReleasePlan into shipment YAML."""
    with TemporaryDirectory() as directory:
        Path(directory, "config.yaml").write_text(config)

        with pytest.raises(ValueError, match="Cannot create a layered-product shipment MR"):
            resolve_shipment_release_plans(GitRepository(directory), "quay-3-18")


@patch("pyartcd.lp_shipment.validate_shipment_mr_ci_state")
@patch("pyartcd.lp_shipment.validate_shipment_mr_reuse_state", new_callable=AsyncMock)
@patch("pyartcd.lp_shipment.validate_shipment_mr")
def test_validate_shipment_mr_for_operation_restores_main(mock_validate, mock_reuse_state, mock_ci_state):
    """Restore main after inspecting an old MR branch for reuse safety."""
    mr = MagicMock()
    state = MagicMock()
    mock_validate.return_value = mr
    mock_ci_state.return_value = state
    repo = MagicMock(spec=GitRepository)
    repo.fetch_switch_branch = AsyncMock()

    result = asyncio.run(
        validate_shipment_mr_for_operation(
            MagicMock(),
            repo,
            "https://gitlab.example/group/project/-/merge_requests/42",
            "https://gitlab.example/group/project.git",
            "https://gitlab.example/group/project.git",
            "quay",
            "quay-3.18",
            "3.18.2",
            allowed_states=("opened", "closed"),
            allow_active_stage=True,
        )
    )

    assert result == (mr, state)
    mock_reuse_state.assert_awaited_once_with(repo, mr, "quay", "quay-3.18", "3.18.2")
    repo.fetch_switch_branch.assert_awaited_once_with("main")


@patch("pyartcd.lp_shipment.validate_shipment_mr_reuse_state", new_callable=AsyncMock)
@patch("pyartcd.lp_shipment.validate_shipment_mr")
def test_validate_shipment_mr_for_operation_restores_main_after_scope_failure(mock_validate, mock_reuse_state):
    """Do not leave a stale MR checkout behind when content validation fails."""
    mock_validate.return_value = MagicMock()
    mock_reuse_state.side_effect = ShipmentMRScopeError("wrong release")
    repo = MagicMock(spec=GitRepository)
    repo.fetch_switch_branch = AsyncMock()

    with pytest.raises(ShipmentMRScopeError, match="wrong release"):
        asyncio.run(
            validate_shipment_mr_for_operation(
                MagicMock(),
                repo,
                "https://gitlab.example/group/project/-/merge_requests/42",
                "https://gitlab.example/group/project.git",
                "https://gitlab.example/group/project.git",
                "quay",
                "quay-3.18",
                "3.18.2",
                allowed_states=("opened", "closed"),
                allow_active_stage=True,
            )
        )

    repo.fetch_switch_branch.assert_awaited_once_with("main")


@patch("pyartcd.lp_shipment.asyncio.sleep", new_callable=AsyncMock)
def test_create_shipment_mr_retries_transient_missing_branch(mock_sleep):
    """Retry when GitLab has not recognized a newly pushed branch yet."""
    source_project = MagicMock()
    expected_mr = MagicMock()
    source_project.mergerequests.create.side_effect = [
        GitlabCreateError({'source_branch': ['does not exist']}, 400, None),
        expected_mr,
    ]
    attributes = {'source_branch': 'prepare-shipment-test'}

    result = asyncio.run(create_shipment_mr_with_retry(source_project, attributes, MagicMock()))

    assert result is expected_mr
    assert source_project.mergerequests.create.call_count == 2
    mock_sleep.assert_awaited_once_with(5)


@patch("pyartcd.lp_shipment.asyncio.sleep", new_callable=AsyncMock)
def test_create_shipment_mr_does_not_retry_other_errors(mock_sleep):
    """Keep unrelated GitLab MR creation failures immediate."""
    source_project = MagicMock()
    source_project.mergerequests.create.side_effect = GitlabCreateError({'title': ['is invalid']}, 400, None)

    with pytest.raises(GitlabCreateError):
        asyncio.run(
            create_shipment_mr_with_retry(
                source_project,
                {'source_branch': 'prepare-shipment-test'},
                MagicMock(),
            )
        )

    source_project.mergerequests.create.assert_called_once()
    mock_sleep.assert_not_awaited()


def test_add_superseded_mr_comment_records_replacement_and_run():
    """Lead with the warning and link the replacement and initiating run."""
    gitlab_client = MagicMock()

    add_superseded_mr_comment(
        gitlab_client,
        "https://gitlab.example.com/group/project/-/merge_requests/10",
        "https://gitlab.example.com/group/project/-/merge_requests/11",
        operation="release-from-fbc",
        run_url="https://jenkins.example.com/job/release/42/",
    )

    gitlab_client.add_mr_comment.assert_called_once()
    old_mr_url, body = gitlab_client.add_mr_comment.call_args.args
    assert old_mr_url.endswith("/merge_requests/10")
    assert body.startswith("**DO NOT USE THIS MR FOR RELEASE.**")
    assert "[shipment MR !11](https://gitlab.example.com/group/project/-/merge_requests/11)" in body
    assert "[release-from-fbc run](https://jenkins.example.com/job/release/42/)" in body


def test_add_superseded_mr_comment_supports_pipeline_run_url():
    """Use the same neutral run link for a Tekton PipelineRun."""
    gitlab_client = MagicMock()

    add_superseded_mr_comment(
        gitlab_client,
        "https://gitlab.example.com/group/project/-/merge_requests/10",
        "https://gitlab.example.com/group/project/-/merge_requests/11",
        operation="prepare-release-lp",
        run_url="https://console.example.com/tekton.dev~v1~PipelineRun/example",
    )

    body = gitlab_client.add_mr_comment.call_args.args[1]
    assert "[prepare-release-lp run](https://console.example.com/tekton.dev~v1~PipelineRun/example)" in body


def test_add_superseded_mr_comment_without_run_url():
    """Retain the operation name when no initiating run URL is available."""
    gitlab_client = MagicMock()

    add_superseded_mr_comment(
        gitlab_client,
        "https://gitlab.example.com/group/project/-/merge_requests/10",
        "https://gitlab.example.com/group/project/-/merge_requests/11",
        operation="release-from-fbc",
    )

    body = gitlab_client.add_mr_comment.call_args.args[1]
    assert body.endswith("during a forced replacement via `release-from-fbc`.")


def test_add_superseded_mr_comment_failure_is_nonfatal(caplog):
    """Keep the persisted replacement authoritative if its audit comment fails."""
    gitlab_client = MagicMock()
    gitlab_client.add_mr_comment.side_effect = RuntimeError("GitLab unavailable")

    add_superseded_mr_comment(
        gitlab_client,
        "https://gitlab.example.com/group/project/-/merge_requests/10",
        "https://gitlab.example.com/group/project/-/merge_requests/11",
        operation="release-from-fbc",
    )

    assert "Failed to comment on superseded shipment MR" in caplog.text


def test_add_fbc_refresh_comment_records_pullspec_mapping_and_run():
    """Record each source-to-refreshed mapping without rewriting MR history."""
    gitlab_client = MagicMock()
    records = [
        FbcRefreshRecord("4.19", ("quay.io/old:419",), ("quay.io/new:419",)),
        FbcRefreshRecord("4.20", ("quay.io/old:420",), ("quay.io/new:420",)),
    ]

    add_fbc_refresh_comment(
        gitlab_client,
        "https://gitlab.example.com/group/project/-/merge_requests/42",
        "1.8.18",
        records,
        operation="release-from-fbc",
        run_url="https://jenkins.example.com/job/release/42/",
    )

    mr_url, body = gitlab_client.add_mr_comment.call_args.args
    assert mr_url.endswith("/merge_requests/42")
    assert "using assembly `1.8.18`" in body
    assert "| 4.19 | `quay.io/old:419` | `quay.io/new:419` |" in body
    assert "| 4.20 | `quay.io/old:420` | `quay.io/new:420` |" in body
    assert "[release-from-fbc run](https://jenkins.example.com/job/release/42/)" in body


def test_add_fbc_refresh_comment_supports_assembly_driven_builds():
    """Describe prepare-release output without inventing a previous FBC."""
    gitlab_client = MagicMock()

    add_fbc_refresh_comment(
        gitlab_client,
        "https://gitlab.example.com/group/project/-/merge_requests/42",
        "2.17.3",
        [FbcRefreshRecord("4.20", (), ("quay.io/new:420",))],
        operation="prepare-release-lp",
    )

    body = gitlab_client.add_mr_comment.call_args.args[1]
    assert "| 4.20 | _assembly-driven build_ | `quay.io/new:420` |" in body
    assert body.endswith("Initiated by `prepare-release-lp`.")


def test_add_fbc_refresh_comment_failure_is_nonfatal(caplog):
    """Keep a successful shipment update authoritative if commenting fails."""
    gitlab_client = MagicMock()
    gitlab_client.add_mr_comment.side_effect = RuntimeError("GitLab unavailable")

    add_fbc_refresh_comment(
        gitlab_client,
        "https://gitlab.example.com/group/project/-/merge_requests/42",
        "1.8.18",
        [FbcRefreshRecord("4.20", ("quay.io/old:420",), ("quay.io/new:420",))],
        operation="release-from-fbc",
    )

    assert "Failed to comment on refreshed FBCs" in caplog.text


def test_close_superseded_shipment_mr_closes_open_mr():
    """Close an open MR before creating its forced replacement."""
    mr = MagicMock(state='opened')

    close_superseded_shipment_mr(mr, dry_run=False)

    assert mr.state_event == 'close'
    assert mr.state == 'closed'
    mr.save.assert_called_once_with()


def test_close_superseded_shipment_mr_leaves_closed_mr_unchanged():
    """Do not attempt to close an MR that was already abandoned."""
    mr = MagicMock(state='closed')

    close_superseded_shipment_mr(mr, dry_run=False)

    mr.save.assert_not_called()


def test_close_superseded_shipment_mr_dry_run_does_not_save():
    """Report closure intent without mutating GitLab during a dry run."""
    mr = MagicMock(state='opened', web_url='https://gitlab.example/mr/42')

    close_superseded_shipment_mr(mr, dry_run=True)

    mr.save.assert_not_called()


def _shipment(*, fbc=False, nvr=None, release_notes=True):
    """Build a minimal layered-product shipment mapping for tests."""
    shipment = {
        'metadata': {
            'product': 'openshift-logging',
            'application': 'fbc-logging-6-5' if fbc else 'logging-6-5',
            'group': 'logging-6.5',
            'assembly': '6.5.2',
            'fbc': fbc,
        },
        'environments': {
            'stage': {'releasePlan': 'stage-plan'},
            'prod': {'releasePlan': 'prod-plan'},
        },
        'snapshot': {
            'spec': {'application': 'fbc-logging-6-5' if fbc else 'logging-6-5', 'components': []},
            'nvrs': [nvr] if nvr else ['logging-container-6.5.2-1.el9'],
        },
    }
    if release_notes:
        shipment['data'] = {'releaseNotes': {'type': 'RHBA'}}
    return {'shipment': shipment}


def test_get_shipment_mr_url():
    """Return the configured MR pointer and tolerate a missing assembly."""
    config = {'releases': {'6.5.2': {'assembly': {'group': {'shipment': {'mr': 'https://example/mr/1'}}}}}}
    assert get_shipment_mr_url(config, '6.5.2') == 'https://example/mr/1'
    assert get_shipment_mr_url({}, '6.5.2') is None


def test_fbc_identity_uses_component_and_ocp_target():
    """Distinguish FBC shipments by both operator and target OCP version."""
    first = _identity(_shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19'))
    second = _identity(_shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-2.ocp4.20'))
    other_operator = _identity(_shipment(fbc=True, nvr='loki-operator-fbc-6.5.2-1.ocp4.19'))
    assert first != second
    assert first != other_operator


def test_validate_shipment_mr():
    """Accept an open MR with the configured source and target repositories."""
    client = MagicMock()
    client._parse_mr_url.return_value = ('hybrid-platforms/art/ocp-shipment-data', '42')
    mr = MagicMock(state='opened', source_project_id=10, target_branch='main', labels=[])
    client.get_mr_from_url.return_value = mr
    client.get_project.return_value.path_with_namespace = 'openshift-eng/ocp-shipment-data'

    assert (
        validate_shipment_mr(
            client,
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42',
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data.git',
            'https://gitlab.example/openshift-eng/ocp-shipment-data.git',
        )
        is mr
    )


def test_validate_shipment_mr_rejects_closed_mr():
    """Reject a closed shipment MR and direct the operator to use force."""
    client = MagicMock()
    client._parse_mr_url.return_value = ('hybrid-platforms/art/ocp-shipment-data', '42')
    client.get_mr_from_url.return_value = MagicMock(state='closed')

    with pytest.raises(ShipmentMRValidationError, match='--force'):
        validate_shipment_mr(
            client,
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42',
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data.git',
            'https://gitlab.example/openshift-eng/ocp-shipment-data.git',
        )


def test_validate_shipment_mr_rejects_prod_release_label():
    """Reject an open MR after shipment CI records production success."""
    client = MagicMock()
    client._parse_mr_url.return_value = ('hybrid-platforms/art/ocp-shipment-data', '42')
    mr = MagicMock(
        state='opened',
        source_project_id=10,
        target_branch='main',
        labels=['stage-release-success', 'prod-release-success'],
    )
    client.get_mr_from_url.return_value = mr
    client.get_project.return_value.path_with_namespace = 'openshift-eng/ocp-shipment-data'

    with pytest.raises(ShipmentMRProductionError, match='prod-release-success') as exc_info:
        validate_shipment_mr(
            client,
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42',
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data.git',
            'https://gitlab.example/openshift-eng/ocp-shipment-data.git',
        )
    assert 'manual recovery' in str(exc_info.value)


def _shipment_ci_graph(
    *,
    parent_status='manual',
    stage_status='success',
    downstream_stage_status='success',
    stage_job_status='success',
    prod_status='manual',
    prod_downstream=None,
    prod_started_at=None,
    downstream_prod_status='success',
    prod_job_status='success',
):
    """Build a minimal python-gitlab object graph for Shipment CI tests."""
    client = MagicMock()
    client._parse_mr_url.return_value = ('hybrid-platforms/art/ocp-shipment-data', '42')

    project = SimpleNamespace(id=10, pipelines=MagicMock())
    parent_pipeline = SimpleNamespace(
        id=100,
        status=parent_status,
        web_url='https://gitlab.example/pipelines/100',
        bridges=MagicMock(),
    )
    stage_downstream = {'id': 200, 'project_id': 10}
    stage_bridge = SimpleNamespace(name='stage-job', status=stage_status, downstream_pipeline=stage_downstream)
    prod_bridge = SimpleNamespace(
        name='prod-job', status=prod_status, started_at=prod_started_at, downstream_pipeline=prod_downstream
    )
    parent_pipeline.bridges.list.return_value = [stage_bridge, prod_bridge]

    stage_pipeline = SimpleNamespace(
        id=200,
        status=downstream_stage_status,
        web_url='https://gitlab.example/pipelines/200',
        jobs=MagicMock(),
    )
    stage_pipeline.jobs.list.return_value = [SimpleNamespace(name='shipment-watch-stage', status=stage_job_status)]

    prod_pipeline = SimpleNamespace(
        id=300,
        status=downstream_prod_status,
        web_url='https://gitlab.example/pipelines/300',
        jobs=MagicMock(),
    )
    prod_pipeline.jobs.list.return_value = [SimpleNamespace(name='shipment-watch-prod', status=prod_job_status)]

    def get_pipeline(pipeline_id):
        if pipeline_id == 100:
            return parent_pipeline
        return stage_pipeline if pipeline_id == 200 else prod_pipeline

    project.pipelines.get.side_effect = get_pipeline
    client.get_project.return_value = project
    mr = SimpleNamespace(pipelines=MagicMock())
    mr.pipelines.list.return_value = [SimpleNamespace(id=100)]
    return client, mr, parent_pipeline, stage_pipeline


def test_inspect_shipment_mr_ci_state_accepts_terminal_stage_and_manual_prod():
    """Allow the normal reuse window after stage and before manual prod starts."""
    client, mr, parent, stage = _shipment_ci_graph()
    project = client.get_project.return_value

    state = inspect_shipment_mr_ci_state(
        client, 'https://gitlab.example/project/-/merge_requests/42', mr, project=project
    )

    assert state.active_stage == ()
    assert state.prod_attempts == ()
    assert state.active_prod == ()
    mr.pipelines.list.assert_called_once_with(get_all=True)
    parent.bridges.list.assert_called_once_with(get_all=True)
    stage.jobs.list.assert_called_once_with(get_all=True, include_retried=True)
    client.get_project.assert_not_called()


def test_inspect_shipment_mr_ci_state_reports_active_prod_downstream():
    """Report active production work without changing MR-reuse decisions."""
    client, mr, _, _ = _shipment_ci_graph(
        prod_status='running',
        prod_downstream={'id': 300, 'project_id': 10},
        downstream_prod_status='running',
        prod_job_status='running',
    )

    state = inspect_shipment_mr_ci_state(client, 'https://gitlab.example/project/-/merge_requests/42', mr)

    assert state.prod_attempts
    assert state.active_prod
    assert any('shipment-watch-prod' in description for description in state.active_prod)


@pytest.mark.parametrize(
    'status', ['created', 'waiting_for_resource', 'preparing', 'pending', 'running', 'scheduled', 'canceling']
)
def test_validate_shipment_mr_ci_state_rejects_active_stage(status):
    """Reject every nonterminal stage state during in-place reuse."""
    client, mr, _, _ = _shipment_ci_graph(stage_status=status, downstream_stage_status=status, stage_job_status=status)

    with pytest.raises(ShipmentMRActiveStageError, match='active stage work'):
        validate_shipment_mr_ci_state(
            client,
            'https://gitlab.example/project/-/merge_requests/42',
            mr,
            allow_active_stage=False,
        )


def test_validate_shipment_mr_ci_state_allows_active_stage_for_force():
    """Allow force replacement to isolate a new MR while old stage continues."""
    client, mr, _, _ = _shipment_ci_graph(
        parent_status='running',
        stage_status='running',
        downstream_stage_status='running',
        stage_job_status='running',
    )

    state = validate_shipment_mr_ci_state(
        client,
        'https://gitlab.example/project/-/merge_requests/42',
        mr,
        allow_active_stage=True,
    )

    assert state.active_stage
    assert not state.prod_attempts


def test_validate_shipment_mr_ci_state_reports_active_stage_with_created_prod_bridge():
    """Treat a production bridge awaiting stage completion as untouched."""
    client, mr, _, _ = _shipment_ci_graph(
        parent_status='running',
        stage_status='running',
        downstream_stage_status='running',
        stage_job_status='running',
        prod_status='created',
    )

    state = inspect_shipment_mr_ci_state(client, 'https://gitlab.example/project/-/merge_requests/42', mr)

    assert state.active_stage
    assert state.prod_attempts == ()
    assert state.active_prod == ()

    with pytest.raises(ShipmentMRActiveStageError, match='active stage work'):
        validate_shipment_mr_ci_state(
            client,
            'https://gitlab.example/project/-/merge_requests/42',
            mr,
            allow_active_stage=False,
        )


@pytest.mark.parametrize('status', ['pending', 'running', 'success', 'failed'])
def test_validate_shipment_mr_ci_state_rejects_any_prod_attempt(status):
    """Block replacement once the production bridge leaves its untouched state."""
    client, mr, _, _ = _shipment_ci_graph(prod_status=status)

    with pytest.raises(ShipmentMRProductionError, match='Manual release recovery'):
        validate_shipment_mr_ci_state(
            client,
            'https://gitlab.example/project/-/merge_requests/42',
            mr,
            allow_active_stage=True,
        )


def test_validate_shipment_mr_ci_state_allows_canceled_unstarted_prod_bridge():
    """Ignore GitLab cancellation of a prod bridge that never started."""
    client, mr, _, _ = _shipment_ci_graph(prod_status='canceled', prod_started_at=None)

    state = validate_shipment_mr_ci_state(
        client,
        'https://gitlab.example/project/-/merge_requests/42',
        mr,
        allow_active_stage=True,
    )

    assert state.prod_attempts == ()
    assert state.active_prod == ()


def test_validate_shipment_mr_ci_state_rejects_canceled_started_prod_bridge():
    """Block a canceled prod bridge when GitLab reports that it started."""
    client, mr, _, _ = _shipment_ci_graph(
        prod_status='canceled',
        prod_started_at='2026-10-09T13:45:40.642Z',
    )

    with pytest.raises(ShipmentMRProductionError, match='Manual release recovery'):
        validate_shipment_mr_ci_state(
            client,
            'https://gitlab.example/project/-/merge_requests/42',
            mr,
            allow_active_stage=True,
        )


def test_validate_shipment_mr_ci_state_fails_closed_when_canceled_prod_bridge_omits_started_at():
    """Fail closed when GitLab omits canceled-bridge execution evidence."""
    client, mr, parent, _ = _shipment_ci_graph(prod_status='canceled')
    prod_bridge = parent.bridges.list.return_value[1]
    del prod_bridge.started_at

    with pytest.raises(RuntimeError, match='has no started_at field'):
        validate_shipment_mr_ci_state(
            client,
            'https://gitlab.example/project/-/merge_requests/42',
            mr,
            allow_active_stage=True,
        )


@pytest.mark.parametrize('prod_status', ['created', 'manual', 'canceled'])
def test_validate_shipment_mr_ci_state_rejects_prod_downstream_from_untouched_bridge(prod_status):
    """Treat any associated production child pipeline as an attempted release."""
    client, mr, _, _ = _shipment_ci_graph(
        prod_status=prod_status,
        prod_downstream={'id': 300, 'project_id': 10},
    )

    with pytest.raises(ShipmentMRProductionError, match='production pipeline history'):
        validate_shipment_mr_ci_state(
            client,
            'https://gitlab.example/project/-/merge_requests/42',
            mr,
            allow_active_stage=True,
        )


def test_inspect_shipment_mr_ci_state_fails_closed_on_unknown_status():
    """Refuse mutation when GitLab returns a state the implementation cannot classify."""
    client, mr, _, _ = _shipment_ci_graph(stage_status='mystery')

    with pytest.raises(RuntimeError, match='unknown GitLab CI status'):
        inspect_shipment_mr_ci_state(client, 'https://gitlab.example/project/-/merge_requests/42', mr)


def test_validate_shipment_mr_allows_closed_for_replacement_inspection():
    """Inspect a closed, unmerged MR when force replacement is requested."""
    client = MagicMock()
    client._parse_mr_url.return_value = ('hybrid-platforms/art/ocp-shipment-data', '42')
    mr = MagicMock(state='closed', source_project_id=10, target_branch='main', labels=[])
    client.get_mr_from_url.return_value = mr
    client.get_project.return_value.path_with_namespace = 'openshift-eng/ocp-shipment-data'

    result = validate_shipment_mr(
        client,
        'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42',
        'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data.git',
        'https://gitlab.example/openshift-eng/ocp-shipment-data.git',
        allowed_states=('opened', 'closed'),
    )

    assert result is mr


def test_validate_shipment_mr_rejects_merged_for_replacement():
    """Never automate a replacement for a merged shipment record."""
    client = MagicMock()
    client._parse_mr_url.return_value = ('hybrid-platforms/art/ocp-shipment-data', '42')
    client.get_mr_from_url.return_value = MagicMock(state='merged')

    with pytest.raises(ShipmentMRProductionError, match='merged'):
        validate_shipment_mr(
            client,
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42',
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data.git',
            'https://gitlab.example/openshift-eng/ocp-shipment-data.git',
            allowed_states=('opened', 'closed'),
        )


def test_set_shipment_mr_draft_clears_stage_success_label():
    """Reset stale stage success when preparing an allowed MR rerun."""
    mr = MagicMock(title='Shipment for logging 6.5.2', labels=['stage-release-success', 'reviewed'])

    set_shipment_mr_draft(mr, dry_run=False)

    assert mr.title == 'Draft: Shipment for logging 6.5.2'
    assert mr.labels == ['reviewed']
    mr.save.assert_called_once_with()


def test_validate_shipment_mr_reuse_state_rejects_prod_advisory():
    """Reject reuse when an image file records a production advisory."""
    with TemporaryDirectory() as directory:
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        path = Path(directory, 'shipment/openshift-logging/logging-6.5/logging-6-5/prod/6.5.2.image.yaml')
        path.parent.mkdir(parents=True)
        existing = _shipment()
        existing['shipment']['environments']['prod']['advisory'] = {'url': 'prod-advisory'}
        YAML.dump(existing, path)
        mr = MagicMock(source_branch='prepare-shipment-6.5.2-20260817161645')
        mr.changes.return_value = {'changes': [{'new_path': str(path.relative_to(directory))}]}

        with pytest.raises(ShipmentMRProductionError, match='prod advisory') as exc_info:
            asyncio.run(validate_shipment_mr_reuse_state(repo, mr, 'openshift-logging', 'logging-6.5', '6.5.2'))
        assert 'manual recovery' in str(exc_info.value)


def test_validate_shipment_mr_reuse_state_rejects_prod_fbc_result():
    """Reject reuse when an FBC file records a production pipeline result."""
    with TemporaryDirectory() as directory:
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        path = Path(directory, 'shipment/openshift-logging/logging-6.5/fbc-logging-6-5/prod/6.5.2.fbc.yaml')
        path.parent.mkdir(parents=True)
        existing = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19')
        existing['shipment']['environments']['prod']['result'] = {'pipeline': 'prod-ci'}
        YAML.dump(existing, path)
        mr = MagicMock(source_branch='prepare-shipment-6.5.2-20260817161645')
        mr.changes.return_value = {'changes': [{'new_path': str(path.relative_to(directory))}]}

        with pytest.raises(ShipmentMRProductionError, match='prod pipeline result') as exc_info:
            asyncio.run(validate_shipment_mr_reuse_state(repo, mr, 'openshift-logging', 'logging-6.5', '6.5.2'))
        assert 'manual recovery' in str(exc_info.value)


def test_validate_shipment_mr_reuse_state_rejects_wrong_product():
    """Reject an MR whose shipment files belong to another product."""
    with TemporaryDirectory() as directory:
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        path = Path(directory, 'shipment/openshift-logging/logging-6.5/logging-6-5/prod/6.5.2.image.yaml')
        path.parent.mkdir(parents=True)
        YAML.dump(_shipment(), path)
        mr = MagicMock(source_branch='prepare-shipment-6.5.2-20260817161645')
        mr.changes.return_value = {'changes': [{'new_path': str(path.relative_to(directory))}]}

        with pytest.raises(ShipmentMRScopeError, match='refusing to modify an unrelated MR') as exc_info:
            asyncio.run(validate_shipment_mr_reuse_state(repo, mr, 'oadp', 'logging-6.5', '6.5.2'))
        message = str(exc_info.value)
        assert "product 'oadp'" in message
        assert str(path.relative_to(directory)) in message
        assert '--force' in message


def test_update_shipment_mr_url_creates_explicit_stream_assembly():
    """Create a minimal explicit-stream assembly for a direct release."""
    with TemporaryDirectory() as directory:
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        repo.commit_push = AsyncMock(return_value=True)
        Path(directory, 'releases.yml').write_text('releases: {}\n')

        asyncio.run(
            update_shipment_mr_url(
                repo,
                'logging-6.5',
                '6.5.2',
                'https://gitlab.example/mr/42',
                None,
                create_as_stream=True,
            )
        )

        result = YAML.load(Path(directory, 'releases.yml'))
        assembly = result['releases']['6.5.2']['assembly']
        assert assembly['type'] == 'stream'
        assert assembly['group']['shipment']['mr'] == 'https://gitlab.example/mr/42'
        repo.commit_push.assert_awaited_once()


def test_update_shipment_mr_url_preserves_full_standard_assembly():
    """Add the MR pointer without replacing standard assembly fields."""
    with TemporaryDirectory() as directory:
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        repo.commit_push = AsyncMock(return_value=True)
        Path(directory, 'releases.yml').write_text(
            "releases:\n"
            "  2.17.3:\n"
            "    assembly:\n"
            "      type: standard\n"
            "      basis:\n"
            "        fbc_pullspecs: quay.io/example/fbc\n"
            "      members:\n"
            "        images: []\n"
        )

        asyncio.run(
            update_shipment_mr_url(
                repo,
                'acm-2.17',
                '2.17.3',
                'https://gitlab.example/mr/42',
                None,
                create_as_stream=False,
            )
        )

        assembly = YAML.load(Path(directory, 'releases.yml'))['releases']['2.17.3']['assembly']
        assert assembly['type'] == 'standard'
        assert assembly['basis']['fbc_pullspecs'] == 'quay.io/example/fbc'
        assert assembly['members']['images'] == []
        assert assembly['group']['shipment']['mr'] == 'https://gitlab.example/mr/42'


def test_update_shipment_mr_url_rejects_concurrent_pointer_change():
    """Refuse to overwrite a pointer changed by another release run."""
    with TemporaryDirectory() as directory:
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        repo.commit_push = AsyncMock(return_value=True)
        Path(directory, 'releases.yml').write_text(
            "releases:\n"
            "  6.5.2:\n"
            "    assembly:\n"
            "      group:\n"
            "        shipment:\n"
            "          mr: https://gitlab.example/mr/concurrent\n"
        )

        try:
            asyncio.run(
                update_shipment_mr_url(
                    repo,
                    'logging-6.5',
                    '6.5.2',
                    'https://gitlab.example/mr/new',
                    'https://gitlab.example/mr/old',
                    create_as_stream=True,
                )
            )
        except RuntimeError as exc:
            assert 'changed concurrently' in str(exc)
        else:
            raise AssertionError("Expected a concurrent pointer update to be rejected")
        repo.commit_push.assert_not_awaited()


def test_reconcile_recreates_existing_fbc_from_scratch():
    """Discard existing FBC content and write only the current generated data."""
    with TemporaryDirectory() as directory:
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        repo.log_diff = AsyncMock()
        repo.commit_push = AsyncMock(return_value=True)

        async def write_file(relative_path, content):
            """Write reconciled content in the temporary repository."""
            destination = Path(directory, relative_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content)
            return destination

        repo.write_file = AsyncMock(side_effect=write_file)
        path = Path(
            directory,
            'shipment/openshift-logging/logging-6.5/fbc-logging-6-5/prod/',
            '6.5.2.fbc.ocp4.19.2026081716164501.yaml',
        )
        path.parent.mkdir(parents=True)
        existing = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19')
        existing['shipment']['environments']['stage']['result'] = {'pipeline': 'stage-ci'}
        existing['shipment']['metadata']['group'] = 'manually-edited'
        existing['shipment']['manual'] = {'field': 'discarded'}
        YAML.dump(existing, path)

        desired = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-2.ocp4.19', release_notes=False)
        mr = MagicMock(source_branch='prepare-shipment-6.5.2-20260817161645')
        mr.changes.return_value = {'changes': [{'new_path': str(path.relative_to(directory)), 'new_file': True}]}

        changed = asyncio.run(
            reconcile_shipment_mr(
                repo,
                mr,
                {'fbc01': ShipmentConfig(**desired)},
                include_fbc_ocp_version=True,
                dry_run=False,
            )
        )

        assert changed
        result = YAML.load(path)
        assert result['shipment']['snapshot']['nvrs'] == ['cluster-logging-operator-fbc-6.5.2-2.ocp4.19']
        assert result == desired
        repo.commit_push.assert_awaited_once()


def test_reconcile_creates_new_shipment_directory_inside_repository(monkeypatch):
    """Create directories for new shipments relative to the repository root."""
    with TemporaryDirectory() as directory, TemporaryDirectory() as working_directory:
        monkeypatch.chdir(working_directory)
        repo = GitRepository(directory)
        repo.fetch_switch_branch = AsyncMock()
        repo.log_diff = AsyncMock()
        repo.commit_push = AsyncMock(return_value=True)

        async def write_file(relative_path, content):
            """Assert that reconciliation created the repository directory."""
            destination = Path(directory, relative_path)
            assert destination.parent.is_dir()
            destination.write_text(content)
            return destination

        repo.write_file = AsyncMock(side_effect=write_file)
        mr = MagicMock(source_branch='prepare-shipment-6.5.2-20260817161645')
        mr.changes.return_value = {'changes': []}
        desired = _shipment(
            fbc=True,
            nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19',
            release_notes=False,
        )

        changed = asyncio.run(
            reconcile_shipment_mr(
                repo,
                mr,
                {'fbc01': ShipmentConfig(**desired)},
                include_fbc_ocp_version=True,
                dry_run=False,
            )
        )

        expected = Path(
            directory,
            'shipment/openshift-logging/logging-6.5/fbc-logging-6-5/prod/',
            '6.5.2.fbc.ocp4.19.2026081716164501.yaml',
        )
        assert changed
        assert expected.is_file()
        assert not Path(working_directory, 'shipment').exists()
        repo.commit_push.assert_awaited_once()


@pytest.fixture
def reconciliation_repo(tmp_path):
    """Provide shipment files and a repository that records writes and pushes."""
    repo = GitRepository(tmp_path)
    repo.fetch_switch_branch = AsyncMock()
    repo.log_diff = AsyncMock()
    repo.commit_push = AsyncMock(return_value=True)
    changes = []
    mr = MagicMock(
        source_branch='prepare-shipment-6.5.2-20260817161645',
        labels=['stage-release-success', 'reviewed'],
    )
    mr.changes.return_value = {'changes': changes}

    async def write_file(relative_path, content):
        destination = tmp_path / relative_path
        destination.write_text(content)
        return destination

    repo.write_file = AsyncMock(side_effect=write_file)

    def add_file(config, filename, *, new_file=True):
        metadata = config['shipment']['metadata']
        path = (
            tmp_path
            / 'shipment'
            / metadata['product']
            / metadata['group']
            / metadata['application']
            / 'prod'
            / filename
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        YAML.dump(config, path)
        path.write_text('# Preserve formatting and comments on unchanged shipments.\n' + path.read_text())
        changes.append({'new_path': str(path.relative_to(tmp_path)), 'new_file': new_file})
        return path

    return repo, mr, add_file


@pytest.mark.parametrize('fbc', [False, True])
@pytest.mark.parametrize('dry_run', [False, True])
def test_reconcile_preserves_unchanged_stage_outputs(reconciliation_repo, fbc, dry_run):
    """An unchanged shipment keeps its exact file, outputs, and successful MR status."""
    repo, mr, add_file = reconciliation_repo
    desired = _shipment(
        fbc=fbc,
        nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19' if fbc else None,
        release_notes=not fbc,
    )
    existing = deepcopy(desired)
    stage = existing['shipment']['environments']['stage']
    if fbc:
        stage['result'] = {'pipeline': 'stage-ci'}
    else:
        stage['advisory'] = {'url': 'stage-advisory', 'internal_url': 'internal-stage-advisory'}
    kind = 'fbc' if fbc else 'image'
    path = add_file(existing, f'6.5.2.{kind}.2026081716164507.yaml')
    original = path.read_bytes()

    changed = asyncio.run(
        reconcile_shipment_mr(
            repo, mr, {kind: ShipmentConfig(**desired)}, include_fbc_ocp_version=True, dry_run=dry_run
        )
    )

    assert not changed
    assert path.read_bytes() == original
    assert mr.labels == ['stage-release-success', 'reviewed']
    mr.save.assert_not_called()
    repo.write_file.assert_not_awaited()
    repo.commit_push.assert_not_awaited()


@pytest.mark.parametrize('dry_run', [False, True])
@pytest.mark.parametrize(
    'change', ['fbc-build', 'fbc-plan', 'image-build', 'image-notes', 'image-plan', 'manual-field', 'invalid-stage']
)
def test_reconcile_invalidates_only_changed_shipment(reconciliation_repo, change, dry_run):
    """Any changed input invalidates stage outputs while retaining the matching filename."""
    repo, mr, add_file = reconciliation_repo
    fbc = change.startswith('fbc') or change == 'manual-field'
    existing = _shipment(
        fbc=fbc,
        nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19' if fbc else None,
        release_notes=not fbc,
    )
    desired = deepcopy(existing)
    existing['shipment']['environments']['stage']['result'] = {'pipeline': 'stage-ci'}
    if not fbc:
        existing['shipment']['environments']['stage']['advisory'] = {'url': 'stage-advisory'}
    if change.endswith('build'):
        if fbc:
            desired['shipment']['snapshot']['nvrs'] = ['cluster-logging-operator-fbc-6.5.2-2.ocp4.19']
        else:
            desired['shipment']['snapshot']['nvrs'].append('logging-bundle-container-6.5.2-1.el9')
    elif change.endswith('plan'):
        desired['shipment']['environments']['stage']['releasePlan'] = 'updated-stage-plan'
    elif change == 'image-notes':
        desired['shipment']['data']['releaseNotes']['description'] = 'Updated release notes'
    elif change == 'invalid-stage':
        existing['shipment']['environments']['stage'] = None
    else:
        existing['shipment']['manual'] = {'field': 'discarded'}
    kind = 'fbc' if fbc else 'image'
    path = add_file(existing, f'6.5.2.{kind}.2026081716164507.yaml')

    async def commit_push(*_args, **_kwargs):
        assert mr.labels == ['reviewed']
        mr.save.assert_called_once_with()
        return True

    repo.commit_push.side_effect = commit_push

    changed = asyncio.run(
        reconcile_shipment_mr(
            repo, mr, {kind: ShipmentConfig(**desired)}, include_fbc_ocp_version=False, dry_run=dry_run
        )
    )

    assert changed
    assert YAML.load(path) == desired
    repo.write_file.assert_awaited_once()
    if dry_run:
        assert mr.labels == ['stage-release-success', 'reviewed']
        mr.save.assert_not_called()
        repo.commit_push.assert_not_awaited()
    else:
        repo.commit_push.assert_awaited_once()


def test_reconcile_image_change_preserves_nine_unchanged_fbcs(reconciliation_repo):
    """Adding an image bundle must not clear existing results from unchanged FBCs."""
    repo, mr, add_file = reconciliation_repo
    desired_image = _shipment()
    existing_image = deepcopy(desired_image)
    existing_image['shipment']['environments']['stage']['advisory'] = {'url': 'stage-advisory'}
    image_path = add_file(existing_image, '6.5.2.image.20260817161645.yaml')
    desired_image['shipment']['snapshot']['nvrs'].append('logging-bundle-container-6.5.2-1.el9')
    desired_image['shipment']['data']['releaseNotes']['description'] = 'Updated release notes'
    shipments = {'image': ShipmentConfig(**desired_image)}
    originals = {}
    for i, minor in enumerate((12, 14, 15, 16, 17, 18, 19, 20, 21), start=1):
        desired = _shipment(fbc=True, nvr=f'cluster-logging-operator-fbc-6.5.2-1.ocp4.{minor}', release_notes=False)
        existing = deepcopy(desired)
        existing['shipment']['environments']['stage']['result'] = {'pipeline': f'stage-ci-{minor}'}
        path = add_file(existing, f'6.5.2.fbc.20260817161645{i:02d}.yaml')
        originals[path] = path.read_bytes()
        shipments[f'fbc{i:02d}'] = ShipmentConfig(**desired)

    changed = asyncio.run(reconcile_shipment_mr(repo, mr, shipments, include_fbc_ocp_version=False, dry_run=False))

    assert changed
    assert YAML.load(image_path) == desired_image
    for path, content in originals.items():
        assert path.read_bytes() == content
    repo.write_file.assert_awaited_once()
    assert mr.labels == ['reviewed']
    mr.save.assert_called_once_with()
    repo.commit_push.assert_awaited_once()


@pytest.mark.parametrize('new_file', [False, True])
def test_reconcile_adds_and_removes_shipments_without_renaming_retained_files(reconciliation_repo, new_file):
    """New FBC filenames avoid retained paths even when their identity sorts first."""
    repo, mr, add_file = reconciliation_repo
    retained = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.20', release_notes=False)
    existing = deepcopy(retained)
    existing['shipment']['environments']['stage']['result'] = {'pipeline': 'stage-ci'}
    path = add_file(existing, '6.5.2.fbc.2026081716164501.yaml')
    original = path.read_bytes()
    obsolete = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.21', release_notes=False)
    obsolete_path = add_file(obsolete, '6.5.2.fbc.2026081716164503.yaml', new_file=new_file)
    added = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19', release_notes=False)
    unrelated = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.22', release_notes=False)
    unrelated['shipment']['metadata']['product'] = 'another-product'
    unrelated_path = add_file(unrelated, '6.5.2.fbc.2026081716164501.yaml')
    unrelated_original = unrelated_path.read_bytes()

    async def restore(_repo, relative_path):
        assert _repo is repo
        assert relative_path == str(obsolete_path.relative_to(repo._directory))
        obsolete_path.write_text('restored from main\n')

    with patch('pyartcd.lp_shipment._restore_from_main', new_callable=AsyncMock, side_effect=restore) as restore_main:
        changed = asyncio.run(
            reconcile_shipment_mr(
                repo,
                mr,
                {'fbc01': ShipmentConfig(**added), 'fbc02': ShipmentConfig(**retained)},
                include_fbc_ocp_version=False,
                dry_run=False,
            )
        )

    assert changed
    assert path.read_bytes() == original
    assert unrelated_path.read_bytes() == unrelated_original
    assert YAML.load(path.with_name('6.5.2.fbc.2026081716164502.yaml')) == added
    if new_file:
        assert not obsolete_path.exists()
        restore_main.assert_not_awaited()
    else:
        assert obsolete_path.read_text() == 'restored from main\n'
        restore_main.assert_awaited_once()
    repo.write_file.assert_awaited_once()
    repo.commit_push.assert_awaited_once()


def _quay_shipment(group, assembly, *, unified=False):
    config = _shipment(
        fbc=unified, nvr='quay-fbc-unified-4.22-1.ocp4.22' if unified else None, release_notes=not unified
    )
    app = 'quay-fbc-unified' if unified else group.replace('.', '-')
    config['shipment']['metadata'].update(product='quay', group=group, assembly=assembly, application=app)
    config['shipment']['snapshot']['spec']['application'] = app
    return config


def test_combined_scope_validation_checks_every_release_and_production_state(reconciliation_repo):
    repo, mr, add_file = reconciliation_repo
    scopes = [('quay', 'quay-3.17', '3.17.7'), ('quay', 'quay-3.18', '3.18.3')]
    for _, group, assembly in scopes:
        add_file(_quay_shipment(group, assembly), f'{assembly}.image.20260817161645.yaml')
    asyncio.run(validate_shipment_mr_reuse_state(repo, mr, *scopes[0], scopes=scopes, require_all_scopes=True))
    prod = _quay_shipment('quay-3.18', '3.18.3', unified=True)
    prod['shipment']['environments']['prod']['result'] = {'pipeline': 'https://prod/pipeline'}
    add_file(prod, '3.18.3.fbc.20260817161645.yaml')
    with pytest.raises(ShipmentMRProductionError):
        asyncio.run(validate_shipment_mr_reuse_state(repo, mr, *scopes[0], scopes=scopes))


def test_combined_scope_validation_rejects_unselected_release(reconciliation_repo):
    repo, mr, add_file = reconciliation_repo
    scopes = [('quay', 'quay-3.18', '3.18.3')]
    add_file(_quay_shipment('quay-3.18', '3.18.3'), '3.18.3.image.yaml')
    add_file(_quay_shipment('quay-3.17', '3.17.7'), '3.17.7.image.yaml')
    with pytest.raises(ShipmentMRScopeError, match='outside the selected scopes'):
        asyncio.run(validate_shipment_mr_reuse_state(repo, mr, *scopes[0], scopes=scopes))


def test_combined_scope_validation_requires_all_scopes_for_reuse(reconciliation_repo):
    repo, mr, add_file = reconciliation_repo
    scopes = [('quay', 'quay-3.17', '3.17.7'), ('quay', 'quay-3.18', '3.18.3')]
    add_file(_quay_shipment('quay-3.17', '3.17.7'), '3.17.7.image.yaml')
    with pytest.raises(ShipmentMRScopeError, match='missing selected release scopes'):
        asyncio.run(validate_shipment_mr_reuse_state(repo, mr, *scopes[0], scopes=scopes, require_all_scopes=True))


def test_ordinary_mode_cannot_modify_combined_mr_even_with_force_marker(reconciliation_repo):
    from pyartcd.lp_shipment import UNIFIED_MR_MARKER, ShipmentMRUnifiedError

    repo, mr, _ = reconciliation_repo
    mr.description = UNIFIED_MR_MARKER
    with pytest.raises(ShipmentMRUnifiedError, match='use --create-unified-fbcs'):
        asyncio.run(validate_shipment_mr_reuse_state(repo, mr, 'quay', 'quay-3.18', '3.18.3'))


def test_combined_reconciliation_preserves_unchanged_images_and_replaces_only_changed_fbc(reconciliation_repo):
    repo, mr, add_file = reconciliation_repo
    scopes = [('quay', 'quay-3.17', '3.17.7'), ('quay', 'quay-3.18', '3.18.3')]
    desired = {}
    originals = {}
    for index, (_, group, assembly) in enumerate(scopes):
        image = _quay_shipment(group, assembly)
        existing = deepcopy(image)
        existing['shipment']['environments']['stage']['advisory'] = {'url': 'https://stage/advisory'}
        path = add_file(existing, f'{assembly}.image.20260817161645.yaml')
        originals[path] = path.read_bytes()
        desired[f'image{index:02d}'] = ShipmentConfig(**image)
    fbc = _quay_shipment('quay-3.18', '3.18.3', unified=True)
    existing = deepcopy(fbc)
    existing['shipment']['snapshot']['nvrs'] = ['quay-fbc-unified-4.22-0.ocp4.22']
    existing['shipment']['environments']['stage']['result'] = {'pipeline': 'https://stage/fbc'}
    path = add_file(existing, '3.18.3.fbc.2026081716164501.yaml')
    desired['fbc01'] = ShipmentConfig(**fbc)
    assert asyncio.run(
        reconcile_shipment_mr(repo, mr, desired, include_fbc_ocp_version=True, dry_run=False, scopes=scopes)
    )
    assert all(path.read_bytes() == original for path, original in originals.items())
    assert YAML.load(path)['shipment']['snapshot']['nvrs'] == fbc['shipment']['snapshot']['nvrs']
    assert 'result' not in YAML.load(path)['shipment']['environments']['stage']
    repo.write_file.assert_awaited_once()
    assert mr.labels == ['reviewed']
