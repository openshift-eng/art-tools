"""Tests for layered-product production pre-validation."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import yaml
from elliottlib.cli.konflux_release_validate_lp_prod_cli import ValidateLpProdCli, find_pruned_entries
from elliottlib.shipment_model import ShipmentConfig
from elliottlib.shipment_utils import ShipmentMRCIState


def _shipment_config(product='openshift-logging', fbc=True, nvr='logging-fbc-6.6.1-1.ocp4.20') -> dict:
    """Return a minimal valid shipment configuration."""
    return {
        'shipment': {
            'metadata': {
                'product': product,
                'application': 'fbc-test',
                'group': 'logging-6.6',
                'assembly': '6.6.1',
                'fbc': fbc,
            },
            'environments': {
                'stage': {'releasePlan': 'logging-stage-fbc'},
                'prod': {'releasePlan': 'logging-prod-fbc'},
            },
            'snapshot': {
                'spec': {
                    'application': 'fbc-test',
                    'components': [
                        {
                            'name': 'logging-fbc',
                            'containerImage': 'quay.io/example/logging-fbc@sha256:abc',
                            'source': {'git': {'url': 'https://example.com/fbc.git', 'revision': 'deadbeef'}},
                        }
                    ],
                },
                'nvrs': [nvr],
            },
        }
    }


def _channel(package: str, channel: str, *entries: str) -> dict:
    return {
        'schema': 'olm.channel',
        'package': package,
        'name': channel,
        'entries': [{'name': entry} for entry in entries],
    }


def test_find_pruned_entries_reports_missing_version_and_channel():
    production = [
        {'schema': 'olm.package', 'name': 'cluster-logging'},
        _channel('cluster-logging', 'stable-6.6', 'logging.v6.6.0', 'logging.v6.6.1'),
        _channel('cluster-logging', 'stable-6.5', 'logging.v6.5.3'),
    ]
    fragment = [
        {'schema': 'olm.package', 'name': 'cluster-logging'},
        _channel('cluster-logging', 'stable-6.6', 'logging.v6.6.0'),
    ]

    assert find_pruned_entries(production, fragment) == {
        ('cluster-logging', 'stable-6.5'): {'logging.v6.5.3'},
        ('cluster-logging', 'stable-6.6'): {'logging.v6.6.1'},
    }


def test_find_pruned_entries_ignores_other_products():
    production = [
        _channel('cluster-logging', 'stable', 'logging.v6.6.1'),
        _channel('mta-operator', 'stable', 'mta.v8.1.0'),
    ]
    fragment = [_channel('cluster-logging', 'stable', 'logging.v6.6.1')]

    assert find_pruned_entries(production, fragment) == {}


def test_find_pruned_entries_uses_unknown_schema_name_as_package():
    production = [_channel('oadp-operator', 'v1.4', 'oadp-operator.v1.4.10')]
    fragment = [{'schema': 'example.future.schema', 'name': 'oadp-operator'}]

    assert find_pruned_entries(production, fragment) == {
        ('oadp-operator', 'v1.4'): {'oadp-operator.v1.4.10'},
    }


def test_find_pruned_entries_rejects_known_channel_without_package():
    fragment = [{'schema': 'olm.channel', 'name': 'v1.4', 'entries': []}]

    with pytest.raises(ValueError, match='without package/name'):
        find_pruned_entries([], fragment)


def test_ocp_is_noop_before_external_checks(tmp_path):
    config_path = tmp_path / 'ocp.fbc.yaml'
    config_path.write_text(yaml.safe_dump(_shipment_config(product='ocp', nvr='ocp-fbc-4.20.1-1.ocp4.20')))
    validator = ValidateLpProdCli((str(config_path),), 'https://gitlab.example/project/-/merge_requests/1', None)
    validator._validate_gitlab_concurrency = MagicMock()
    validator._new_konflux_client = MagicMock()
    validator._validate_fbc_fragments = AsyncMock()

    asyncio.run(validator.run())

    validator._validate_gitlab_concurrency.assert_not_called()
    validator._new_konflux_client.assert_not_called()
    validator._validate_fbc_fragments.assert_not_awaited()


def test_gitlab_concurrency_blocks_same_product_active_prod():
    validator = ValidateLpProdCli(('unused',), 'https://gitlab.example/project/-/merge_requests/1', None)
    client = MagicMock()
    client._parse_mr_url.return_value = ('project', '1')
    project = MagicMock(id=10)
    mr = MagicMock(source_project_id=10)
    project.mergerequests.get.return_value = mr
    client.get_project.return_value = project
    client.list_merge_requests.return_value = [
        SimpleNamespace(iid=2, web_url='https://gitlab.example/project/-/merge_requests/2')
    ]

    with (
        patch('elliottlib.cli.konflux_release_validate_lp_prod_cli.GitLabClient.from_url', return_value=client),
        patch(
            'elliottlib.cli.konflux_release_validate_lp_prod_cli.get_shipment_config_records',
            return_value=[MagicMock()],
        ) as get_records,
        patch(
            'elliottlib.cli.konflux_release_validate_lp_prod_cli.inspect_shipment_mr_ci_state',
            return_value=ShipmentMRCIState((), ('attempted',), ('https://gitlab.example/pipelines/10 is running',)),
        ) as inspect_state,
    ):
        with pytest.raises(RuntimeError, match='same-product|layered product'):
            validator._validate_gitlab_concurrency('openshift-logging')
        get_records.assert_called_once_with(
            mr,
            project,
            kinds=None,
            product='openshift-logging',
            environment='prod',
        )
        client.list_merge_requests.assert_called_once_with(
            'project', state='opened', project=project, target_branch='main'
        )
        client.get_project.assert_called_once_with('project')
        project.mergerequests.get.assert_called_once_with(2)
        inspect_state.assert_called_once_with(
            client,
            'https://gitlab.example/project/-/merge_requests/2',
            mr,
            project=project,
        )


def test_gitlab_concurrency_reuses_source_project():
    validator = ValidateLpProdCli(('unused',), 'https://gitlab.example/project/-/merge_requests/1', None)
    client = MagicMock()
    client._parse_mr_url.return_value = ('project', '1')
    project = MagicMock(id=10)
    source_project = MagicMock(id=20)
    project.mergerequests.get.side_effect = [
        MagicMock(source_project_id=20),
        MagicMock(source_project_id=20),
    ]
    client.get_project.side_effect = [project, source_project]
    client.list_merge_requests.return_value = [
        SimpleNamespace(iid=2, web_url='https://gitlab.example/project/-/merge_requests/2'),
        SimpleNamespace(iid=3, web_url='https://gitlab.example/project/-/merge_requests/3'),
    ]

    with (
        patch('elliottlib.cli.konflux_release_validate_lp_prod_cli.GitLabClient.from_url', return_value=client),
        patch(
            'elliottlib.cli.konflux_release_validate_lp_prod_cli.get_shipment_config_records',
            return_value=[],
        ) as get_records,
        patch('elliottlib.cli.konflux_release_validate_lp_prod_cli.inspect_shipment_mr_ci_state') as inspect_state,
    ):
        validator._validate_gitlab_concurrency('openshift-logging')

    assert get_records.call_count == 2
    assert all(record_call.args[1] is source_project for record_call in get_records.call_args_list)
    client.get_project.assert_has_calls([call('project'), call(20)])
    assert client.get_project.call_count == 2
    inspect_state.assert_not_called()


def test_konflux_concurrency_ignores_other_product_and_terminal_release():
    validator = ValidateLpProdCli(('unused',), 'https://gitlab.example/project/-/merge_requests/1', None)
    client = MagicMock()
    client.list_releases = AsyncMock(
        return_value=[
            {'metadata': {'name': 'mta-prod-8-1-fbc-20260101000000'}},
            {
                'metadata': {'name': 'openshift-logging-prod-6-6-fbc-20260101000000'},
                'status': {'conditions': [{'type': 'Released', 'status': 'True', 'reason': 'Succeeded'}]},
            },
        ]
    )

    asyncio.run(validator._validate_konflux_concurrency('openshift-logging', client))


def test_konflux_concurrency_blocks_active_same_product():
    validator = ValidateLpProdCli(('unused',), 'https://gitlab.example/project/-/merge_requests/1', None)
    client = MagicMock()
    client.list_releases = AsyncMock(
        return_value=[{'metadata': {'name': 'openshift-logging-prod-6-6-fbc-20260101000000'}}]
    )
    client.resource_url.return_value = 'https://konflux.example/releases/logging'

    with pytest.raises(RuntimeError, match='https://konflux.example/releases/logging'):
        asyncio.run(validator._validate_konflux_concurrency('openshift-logging', client))


def test_fbc_validation_reports_pruned_entry():
    config = ShipmentConfig.model_validate(_shipment_config())
    validator = ValidateLpProdCli(('shipment/logging.fbc.yaml',), 'https://gitlab.example/mr/1', '/tmp/auth.json')
    production = [_channel('cluster-logging', 'stable-6.6', 'logging.v6.6.0', 'logging.v6.6.1')]
    fragment = [_channel('cluster-logging', 'stable-6.6', 'logging.v6.6.0')]

    with patch(
        'elliottlib.cli.konflux_release_validate_lp_prod_cli.render',
        new=AsyncMock(side_effect=[production, fragment]),
    ):
        with pytest.raises(RuntimeError, match=r'cluster-logging.*stable-6\.6.*logging\.v6\.6\.1'):
            asyncio.run(validator._validate_fbc_fragments([config]))
