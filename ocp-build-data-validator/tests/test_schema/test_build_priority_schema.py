import pytest
from validator import schema, support


def test_build_priority_policy_dispatch():
    path = 'config/build-priority.yml'
    assert support.get_artifact_type(path) == 'build-priority'
    assert (
        schema.validate(
            path,
            {
                'enabled': False,
                'rules': [{'product': 'ocp', 'versions': ['4.19', '5.0'], 'priority': 1}],
                'default_priority': 2,
            },
        )
        == ''
    )


@pytest.mark.parametrize(
    'config',
    [
        {'enabled': 'true'},
        {'rules': [{'product': 'ocp', 'priority': 11}]},
        {'rules': [{'product': 'ocp', 'min_version': '4.18.0', 'priority': 1}]},
        {'rules': [{'product': 'unknown', 'priority': 1}]},
    ],
)
def test_validator_rejects_invalid_priority_policy(config):
    assert schema.validate('config/build-priority.yml', config)
