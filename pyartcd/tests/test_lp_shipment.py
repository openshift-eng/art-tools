import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import ShipmentConfig
from pyartcd.git import GitRepository
from pyartcd.lp_shipment import (
    ShipmentMRActiveStageError,
    ShipmentMRProductionError,
    ShipmentMRScopeError,
    ShipmentMRValidationError,
    _identity,
    get_shipment_mr_url,
    inspect_shipment_mr_ci_state,
    reconcile_shipment_mr,
    set_shipment_mr_draft,
    update_shipment_mr_url,
    validate_shipment_mr,
    validate_shipment_mr_ci_state,
    validate_shipment_mr_reuse_state,
)

YAML = new_roundtrip_yaml_handler()


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
    prod_bridge = SimpleNamespace(name='prod-job', status=prod_status, downstream_pipeline=prod_downstream)
    parent_pipeline.bridges.list.return_value = [stage_bridge, prod_bridge]

    stage_pipeline = SimpleNamespace(
        id=200,
        status=downstream_stage_status,
        web_url='https://gitlab.example/pipelines/200',
        jobs=MagicMock(),
    )
    stage_pipeline.jobs.list.return_value = [SimpleNamespace(name='shipment-watch-stage', status=stage_job_status)]

    def get_pipeline(pipeline_id):
        return parent_pipeline if pipeline_id == 100 else stage_pipeline

    project.pipelines.get.side_effect = get_pipeline
    client.get_project.return_value = project
    mr = SimpleNamespace(pipelines=MagicMock())
    mr.pipelines.list.return_value = [SimpleNamespace(id=100)]
    return client, mr, parent_pipeline, stage_pipeline


def test_inspect_shipment_mr_ci_state_accepts_terminal_stage_and_manual_prod():
    """Allow the normal reuse window after stage and before manual prod starts."""
    client, mr, parent, stage = _shipment_ci_graph()

    state = inspect_shipment_mr_ci_state(client, 'https://gitlab.example/project/-/merge_requests/42', mr)

    assert state.active_stage == ()
    assert state.prod_attempts == ()
    mr.pipelines.list.assert_called_once_with(get_all=True)
    parent.bridges.list.assert_called_once_with(get_all=True)
    stage.jobs.list.assert_called_once_with(get_all=True, include_retried=True)


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


@pytest.mark.parametrize('status', ['created', 'pending', 'running', 'success', 'failed', 'canceled'])
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


def test_validate_shipment_mr_ci_state_rejects_prod_downstream_from_manual_bridge():
    """Treat any associated production child pipeline as an attempted release."""
    client, mr, _, _ = _shipment_ci_graph(
        prod_status='manual',
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
