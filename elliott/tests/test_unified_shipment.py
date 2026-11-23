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
