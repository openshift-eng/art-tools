from copy import deepcopy

import pytest
from validator.schema import releases_schema


def assembly():
    return {
        'releases': {
            '3.18.5': {
                'assembly': {
                    'group': {
                        'fbc_builds': {
                            '4.22': {
                                'quay-operator': {
                                    'nvr': 'quay-operator-fbc-3.18.5-1.ocp4.22',
                                    'pullspec': 'quay.io/test/fbc@sha256:' + 'a' * 64,
                                    'operand_nvrs': ['quay-operator-container-3.18.5-1'],
                                }
                            },
                        }
                    }
                }
            }
        }
    }


def test_assembly_can_record_immutable_fbc_inputs():
    assert not releases_schema.validate(None, assembly())


@pytest.mark.parametrize('field,value', [('nvr', ''), ('pullspec', 'quay.io/test/fbc:latest'), ('operand_nvrs', [])])
def test_assembly_fbc_records_reject_incomplete_inputs(field, value):
    config = deepcopy(assembly())
    config['releases']['3.18.5']['assembly']['group']['fbc_builds']['4.22']['quay-operator'][field] = value
    assert releases_schema.validate(None, config)
