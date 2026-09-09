import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import ShipmentConfig
from pyartcd.git import GitRepository
from pyartcd.lp_shipment import (
    _identity,
    _reconcile_config,
    get_shipment_mr_url,
    reconcile_shipment_mr,
    update_shipment_mr_url,
    validate_shipment_mr,
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


def test_reconcile_config_preserves_downstream_environment_fields():
    """Replace generated fields without overwriting downstream CI results."""
    existing = _shipment()
    existing['shipment']['environments']['stage']['result'] = {'pipeline': 'stage-ci'}
    existing['shipment']['environments']['prod']['advisory'] = {'url': 'advisory-url'}
    existing['shipment']['snapshot']['nvrs'] = ['manually-edited']
    desired = _shipment(release_notes=False)
    desired['shipment']['environments']['stage']['releasePlan'] = 'new-stage-plan'

    result = _reconcile_config(existing, desired)

    assert result['shipment']['snapshot'] == desired['shipment']['snapshot']
    assert 'data' not in result['shipment']
    assert result['shipment']['environments']['stage'] == {
        'releasePlan': 'new-stage-plan',
        'result': {'pipeline': 'stage-ci'},
    }
    assert result['shipment']['environments']['prod']['advisory'] == {'url': 'advisory-url'}


def test_reconcile_config_clears_stale_fbc_pipeline_results_when_nvr_changes():
    """Clear pipeline URLs for both environments when an FBC NVR changes."""
    existing = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19')
    existing['shipment']['environments']['stage']['result'] = {'pipeline': 'stage-ci'}
    existing['shipment']['environments']['prod']['result'] = {
        'pipeline': 'prod-ci',
        'other': 'preserved',
    }
    existing['shipment']['environments']['prod']['advisory'] = {'url': 'advisory-url'}
    desired = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-2.ocp4.19')

    result = _reconcile_config(existing, desired)

    assert 'result' not in result['shipment']['environments']['stage']
    assert result['shipment']['environments']['prod']['result'] == {'other': 'preserved'}
    assert result['shipment']['environments']['prod']['advisory'] == {'url': 'advisory-url'}


def test_reconcile_config_preserves_fbc_pipeline_results_when_nvr_is_unchanged():
    """Retain completed FBC pipeline URLs when the generated NVR is unchanged."""
    existing = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19')
    existing['shipment']['environments']['stage']['result'] = {'pipeline': 'stage-ci'}
    desired = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19')

    result = _reconcile_config(existing, desired)

    assert result['shipment']['environments']['stage']['result'] == {'pipeline': 'stage-ci'}


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
    mr = MagicMock(state='opened', source_project_id=10, target_branch='main')
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

    try:
        validate_shipment_mr(
            client,
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/42',
            'https://gitlab.example/hybrid-platforms/art/ocp-shipment-data.git',
            'https://gitlab.example/openshift-eng/ocp-shipment-data.git',
        )
    except ValueError as exc:
        assert '--force' in str(exc)
    else:
        raise AssertionError("Expected a closed MR to be rejected")


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


def test_reconcile_existing_fbc_clears_stale_pipeline_result():
    """Keep the filename but clear its pipeline result when the FBC NVR changes."""
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
            '6.5.2.fbc.2026081716164501.yaml',
        )
        path.parent.mkdir(parents=True)
        existing = _shipment(fbc=True, nvr='cluster-logging-operator-fbc-6.5.2-1.ocp4.19')
        existing['shipment']['environments']['prod']['result'] = {'pipeline': 'prod-ci'}
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
        assert 'result' not in result['shipment']['environments']['prod']
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
