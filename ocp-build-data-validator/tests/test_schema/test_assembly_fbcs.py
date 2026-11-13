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


@pytest.mark.parametrize(
    'pullspecs',
    [
        {'4.17': 'quay.io/example/catalog@sha256:abc'},
        {'4.18': ['quay.io/example/first:tag', 'quay.io/example/second:tag']},
        ['quay.io/example/catalog:tag'],
    ],
)
def test_assembly_accepts_generated_basis_fbc_pullspecs(pullspecs):
    config = {'releases': {'6.4.5': {'assembly': {'type': 'standard', 'basis': {'fbc_pullspecs': pullspecs}}}}}
    assert not releases_schema.validate('releases.yml', config)


@pytest.mark.parametrize('pullspecs', [42, {'4.18': [7]}, {'4.18': ''}])
def test_assembly_rejects_invalid_basis_fbc_pullspecs(pullspecs):
    config = {'releases': {'6.4.5': {'assembly': {'type': 'standard', 'basis': {'fbc_pullspecs': pullspecs}}}}}
    assert releases_schema.validate('releases.yml', config)


@pytest.mark.parametrize('field,value', [('nvr', ''), ('pullspec', 'quay.io/test/fbc:latest'), ('operand_nvrs', [])])
def test_assembly_fbc_records_reject_incomplete_inputs(field, value):
    config = deepcopy(assembly())
    config['releases']['3.18.5']['assembly']['group']['fbc_builds']['4.22']['quay-operator'][field] = value
    assert releases_schema.validate(None, config)
