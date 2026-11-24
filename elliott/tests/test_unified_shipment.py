"""Contract tests for group-less unified shipments and effective input comparison."""

from copy import deepcopy

import pytest
from elliottlib.shipment_model import ShipmentConfig
from elliottlib.unified_shipment import effective_inputs
from pydantic import ValidationError


@pytest.fixture
def shipment():
    return {
        'shipment': {
            'metadata': {'product': 'mta', 'application': 'mta-fbc-unified', 'fbc': True, 'unified': True},
            'environments': {
                'stage': {'releasePlan': 'mta-fbc-stage-unified'},
                'prod': {'releasePlan': 'mta-fbc-prod-unified'},
            },
            'snapshot': {
                'spec': {
                    'application': 'mta-fbc-unified',
                    'components': [
                        {
                            'name': 'mta-fbc-unified-4-18',
                            'containerImage': 'quay.io/example/fbc@sha256:' + 'a' * 64,
                            'source': {
                                'git': {'url': 'https://github.com/openshift-priv/art-fbc', 'revision': 'b' * 40}
                            },
                        }
                    ],
                },
                'nvrs': ['mta-fbc-unified-4.18-20261123.ocp4.18'],
            },
            'unified': {
                'assemblies': [
                    {
                        'group': 'mta-8.2',
                        'assembly': '8.2.3',
                        'revision': 'c' * 40,
                        'imageShipmentMR': 'https://gitlab.example/art/shipments/-/merge_requests/1',
                    }
                ]
            },
        },
    }


def test_unified_has_no_execution_assembly_or_extra_provenance_locator(shipment):
    parsed = ShipmentConfig.model_validate(shipment)
    assert parsed.shipment.metadata.group is None
    assert parsed.shipment.metadata.assembly is None
    assert parsed.model_dump(exclude_none=True)['shipment']['unified'] == shipment['shipment']['unified']


@pytest.mark.parametrize('field,value', [('group', 'mta-8.2'), ('assembly', '8.2.3'), ('fbc', False)])
def test_reject_single_execution_context(shipment, field, value):
    shipment['shipment']['metadata'][field] = value
    with pytest.raises(ValidationError):
        ShipmentConfig.model_validate(shipment)


@pytest.mark.parametrize('field', ['generation', 'fingerprint'])
def test_no_obsolete_shipment_fields(shipment, field):
    shipment['shipment']['unified']['assemblies'][0][field] = 'old'
    with pytest.raises(ValidationError):
        ShipmentConfig.model_validate(shipment)


def test_ordinary_shipments_still_require_context(shipment):
    shipment['shipment']['metadata'].pop('unified')
    shipment['shipment'].pop('unified')
    with pytest.raises(ValidationError):
        ShipmentConfig.model_validate(shipment)
    shipment['shipment']['metadata'].update(group='mta-8.2', assembly='8.2.3')
    assert ShipmentConfig.model_validate(shipment).shipment.metadata.assembly == '8.2.3'


def test_effective_inputs_detect_manual_pin_change_ignore_bookkeeping():
    nvr = 'operator-container-8.2.3-1'
    releases = {
        'releases': {
            '8.2.3': {
                'assembly': {
                    'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': nvr}}}]},
                    'group': {
                        'fbc_builds': {
                            '4.18': {
                                'operator': {
                                    'nvr': 'operator-fbc-8.2.3-1.ocp4.18',
                                    'pullspec': 'quay.io/example/fbc@sha256:' + 'a' * 64,
                                    'operand_nvrs': [nvr],
                                }
                            }
                        }
                    },
                }
            }
        }
    }
    original = effective_inputs(releases, '8.2.3', '4.18')
    changed = deepcopy(releases)
    changed['releases']['8.2.3']['assembly']['group']['shipment'] = {'mr': 'another pointer'}
    assert effective_inputs(changed, '8.2.3', '4.18') == original
    changed['releases']['8.2.3']['assembly']['members']['images'][0]['metadata']['is']['nvr'] = 'other-8.2.3-2'
    with pytest.raises(ValueError, match='stale FBC pins'):
        effective_inputs(changed, '8.2.3', '4.18')


def test_provenance_comparison_uses_content_not_branch_commit(shipment):
    from elliottlib.unified_shipment import validate_contributor_inputs

    contributors = ShipmentConfig.model_validate(shipment).shipment.unified.assemblies
    nvr = 'operator-container-8.2.3-1'
    assembly = {
        'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': nvr}}}]},
        'group': {
            'shipment': {'mr': contributors[0].imageShipmentMR},
            'fbc_builds': {
                '4.18': {
                    'operator': {
                        'nvr': 'operator-fbc-8.2.3-1.ocp4.18',
                        'pullspec': 'quay.io/example/fbc@sha256:' + 'a' * 64,
                        'operand_nvrs': [nvr],
                    }
                }
            },
        },
    }
    releases = {'releases': {'8.2.3': {'assembly': assembly}}}
    provenance = {
        'ocp_target_version': '4.18',
        'inputs': [
            {
                'group': 'mta-8.2',
                'assembly': '8.2.3',
                'assembly_group_revision': 'c' * 40,
                'nvr': 'operator-fbc-8.2.3-1.ocp4.18',
                'pullspec': 'quay.io/example/fbc@sha256:' + 'a' * 64,
            }
        ],
    }
    current = deepcopy(releases)
    current['releases']['unrelated'] = {'assembly': {'type': 'stream'}}

    def load(group, revision):
        return releases if revision else current

    validate_contributor_inputs(provenance, contributors, load)
    current['releases']['8.2.3']['assembly']['group']['fbc_builds']['4.18']['operator']['pullspec'] = (
        'quay.io/example/fbc@sha256:' + 'd' * 64
    )
    with pytest.raises(ValueError, match='changed since unified'):
        validate_contributor_inputs(provenance, contributors, load)


def test_effective_inputs_include_inherited_pins():
    nvr = 'operator-container-8.2.3-1'
    releases = {
        'releases': {
            'parent': {
                'assembly': {
                    'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': nvr}}}]},
                }
            },
            '8.2.3': {
                'assembly': {
                    'basis': {'assembly': 'parent'},
                    'group': {
                        'fbc_builds': {
                            '4.18': {
                                'operator': {
                                    'nvr': 'operator-fbc-8.2.3-1.ocp4.18',
                                    'pullspec': 'quay.io/example/fbc@sha256:' + 'a' * 64,
                                    'operand_nvrs': [nvr],
                                }
                            }
                        }
                    },
                }
            },
        }
    }
    assert effective_inputs(releases, '8.2.3', '4.18')['image_nvrs'] == [nvr]
    releases['releases']['parent']['assembly']['members']['images'][0]['metadata']['is']['nvr'] = 'changed-8.2.3-2'
    with pytest.raises(ValueError, match='stale FBC pins'):
        effective_inputs(releases, '8.2.3', '4.18')


def test_json_schema_preserves_ordinary_context_requirement(shipment):
    from jsonschema import Draft202012Validator
    from jsonschema.exceptions import ValidationError as SchemaValidationError

    validator = Draft202012Validator(ShipmentConfig.model_json_schema())
    validator.validate(shipment)
    ordinary = deepcopy(shipment)
    ordinary['shipment']['metadata'].pop('unified')
    ordinary['shipment'].pop('unified')
    with pytest.raises(SchemaValidationError):
        validator.validate(ordinary)
    ordinary['shipment']['metadata'].update(group='mta-8.2', assembly='8.2.3')
    validator.validate(ordinary)


@pytest.mark.parametrize('mode', ['unchanged', 'bookkeeping', 'catalog_changed'])
def test_production_freeze_preserves_content_allows_results(shipment, mode):
    from types import SimpleNamespace
    from unittest.mock import Mock, patch

    from elliottlib.unified_shipment import validate_production_freeze

    config = ShipmentConfig.model_validate(shipment).shipment
    old = deepcopy(shipment['shipment'])
    if mode == 'bookkeeping':
        old['environments']['stage']['result'] = {'pipeline': 'https://example/old-stage'}
    if mode == 'catalog_changed':
        old['snapshot']['spec']['components'][0]['containerImage'] = 'quay.io/example/fbc@sha256:' + 'd' * 64
    client = Mock()
    client._parse_mr_url.return_value = ('art/shipments', '1')
    client.get_mr_from_url.return_value = SimpleNamespace(sha='current')
    with (
        patch('elliottlib.unified_shipment.production_pipelines', return_value=[(SimpleNamespace(sha='old'), None)]),
        patch(
            'elliottlib.unified_shipment.shipment_files',
            side_effect=lambda project, mr, ref: [
                ('shipment/test.yaml', old if ref == 'old' else shipment['shipment'])
            ],
        ),
    ):
        if mode == 'catalog_changed':
            with pytest.raises(RuntimeError, match='content changed after production'):
                validate_production_freeze(
                    config, 'shipment/test.yaml', client, 'https://gitlab.example/art/shipments/-/merge_requests/1'
                )
        else:
            validate_production_freeze(
                config, 'shipment/test.yaml', client, 'https://gitlab.example/art/shipments/-/merge_requests/1'
            )


@pytest.mark.parametrize(
    'status,historic_matches', [('success', True), ('failed', True), ('pending', True), ('success', False)]
)
def test_image_prod_requires_successful_matching_watch_job(shipment, status, historic_matches):
    import json
    from types import SimpleNamespace
    from unittest.mock import Mock, patch

    from elliottlib.unified_shipment import validate_image_production

    config = ShipmentConfig.model_validate(shipment).shipment
    pin, bundle = 'operator-container-8.2.3-1', 'operator-bundle-8.2.3-1'
    records = {
        'releases': {
            '8.2.3': {
                'assembly': {
                    'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': pin}}}]},
                    'group': {
                        'fbc_builds': {
                            '4.18': {
                                'operator': {
                                    'nvr': 'operator-fbc-8.2.3-1.ocp4.18',
                                    'pullspec': 'quay.io/example/fbc@sha256:' + 'a' * 64,
                                    'operand_nvrs': [pin],
                                }
                            }
                        }
                    },
                }
            }
        }
    }
    image = {
        'metadata': {'product': 'mta', 'group': 'mta-8.2', 'assembly': '8.2.3', 'fbc': False},
        'snapshot': {'nvrs': [pin, bundle]},
    }
    historic = deepcopy(image)
    if not historic_matches:
        historic['snapshot']['nvrs'] = ['different-content']
    client = Mock()
    client._parse_mr_url.return_value = ('art/shipments', '1')
    client.get_mr_from_url.return_value = SimpleNamespace(sha='current')
    child = Mock()
    child.jobs.list.return_value = [SimpleNamespace(name='image-watch-prod', status=status)]
    provenance = {'inputs': [{'group': 'mta-8.2', 'assembly': '8.2.3', 'bundle_nvr': bundle}]}
    with (
        patch('elliottlib.unified_shipment.get_github_client_for_org') as github,
        patch('elliottlib.unified_shipment.production_pipelines', return_value=[(SimpleNamespace(sha='old'), child)]),
        patch(
            'elliottlib.unified_shipment.shipment_files',
            side_effect=lambda project, mr, ref: [('shipment/image.yaml', historic if ref == 'old' else image)],
        ),
    ):
        github.return_value.get_repo.return_value.get_contents.return_value.decoded_content = json.dumps(
            records
        ).encode()
        if status == 'success' and historic_matches:
            validate_image_production(config, provenance, client)
        else:
            with pytest.raises(RuntimeError, match='production incomplete'):
                validate_image_production(config, provenance, client)


@pytest.mark.parametrize('environment', ['stage', 'prod'])
def test_stage_can_precede_image_prod_but_prod_checks_readiness(shipment, environment):
    import asyncio
    from unittest.mock import AsyncMock, Mock, patch

    from elliottlib.unified_shipment import validate_unified_release

    config = ShipmentConfig.model_validate(shipment).shipment
    provenance = {'ocp_target_version': '4.18'}
    labels = {'com.redhat.art.nvr': config.snapshot.nvrs[0], 'org.opencontainers.image.revision': 'b' * 40}

    async def inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with (
        patch('elliottlib.unified_shipment.load_pinned_provenance', return_value=provenance),
        patch('elliottlib.unified_shipment.validate_current_inputs') as current,
        patch('elliottlib.unified_shipment.validate_image_production') as images,
        patch('elliottlib.unified_shipment.validate_production_freeze') as freeze,
        patch('artcommonlib.exectools.to_thread', side_effect=inline),
        patch(
            'elliottlib.unified_shipment.oc_image_info_for_arch_async',
            new_callable=AsyncMock,
            return_value={'config': {'config': {'Labels': labels}}},
        ),
    ):
        asyncio.run(
            validate_unified_release(
                config,
                environment,
                gitlab_client=Mock(),
                mr_url='https://gitlab.example/art/shipments/-/merge_requests/1',
                config_path='shipment/unified.yaml',
            )
        )
    current.assert_called_once()
    freeze.assert_called_once()
    assert images.call_count == (1 if environment == 'prod' else 0)


def test_unified_runtime_has_no_single_assembly(shipment):
    from unittest.mock import Mock

    from elliottlib.cli.konflux_release_cli import initialize_release_runtime

    runtime = Mock(group=None)
    runtime.shipment_gitdata.load_yaml_file.return_value = shipment
    initialize_release_runtime(runtime, 'shipment/test.yaml')
    runtime.initialize.assert_called_once_with(no_group=True, build_system='konflux')
    assert runtime.product == 'mta'
    assert runtime.assembly is None


def test_malformed_unified_json_schema_is_rejected(shipment):
    from jsonschema import Draft202012Validator
    from jsonschema.exceptions import ValidationError as SchemaValidationError

    shipment['shipment'].pop('unified')
    with pytest.raises(SchemaValidationError):
        Draft202012Validator(ShipmentConfig.model_json_schema()).validate(shipment)
