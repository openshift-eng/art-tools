from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from artcommonlib.build_priority import BUILD_PRIORITY_POLICY_URL, BuildPriorityPolicy, load_build_priority_policy
from pydantic import ValidationError


def incident_policy(**overrides):
    return BuildPriorityPolicy.model_validate(
        {
            'enabled': True,
            'rules': [
                {'product': 'ocp', 'versions': ['4.19', '4.20', '4.21', '4.22', '5.0'], 'priority': 1},
                {'product': 'ocp', 'priority': 3},
            ],
            'default_priority': 2,
            **overrides,
        }
    )


@pytest.mark.parametrize('version', ['4.19', '4.20', '4.21', '4.22', '5.0'])
def test_development_cutoff_streams(version):
    assert incident_policy().priority_for('ocp', f'openshift-{version}') == '1'


@pytest.mark.parametrize('version', ['4.9', '4.13', '4.15', '4.17', '4.18', '4.23', '5.1'])
def test_other_ocp_streams(version):
    assert incident_policy().priority_for('ocp', f'openshift-{version}') == '3'


@pytest.mark.parametrize('product,group', [('rhacm2', 'acm-2.17'), ('openshift-logging', 'logging-6.6')])
def test_layered_products(product, group):
    assert incident_policy().priority_for(product, group) == '2'


def test_disabled_and_missing_flag_retain_existing_behavior():
    assert incident_policy(enabled=False).priority_for('ocp', 'openshift-4.20') is None
    assert BuildPriorityPolicy.model_validate({}).priority_for('ocp', 'openshift-4.20') is None


def test_min_version_uses_numeric_comparison():
    policy = incident_policy(rules=[{'product': 'ocp', 'min_version': '4.18', 'priority': 1}])
    assert policy.priority_for('ocp', 'openshift-4.9') == '2'
    assert policy.priority_for('ocp', 'openshift-4.18') == '1'
    assert policy.priority_for('ocp', 'openshift-4.20') == '1'
    assert policy.priority_for('ocp', 'openshift-5.0') == '1'
    assert policy.priority_for('ocp', 'unversioned') == '2'


def test_incident_override_precedes_general_rule():
    policy = incident_policy(
        rules=[
            {'product': 'rhacm2', 'versions': ['2.17'], 'priority': 1},
            {'product': 'rhacm2', 'priority': 3},
        ]
    )
    assert policy.priority_for('rhacm2', 'acm-2.17') == '1'
    assert policy.priority_for('rhacm2', 'acm-2.16') == '3'


@pytest.mark.parametrize(
    'config',
    [
        None,
        {'enabled': 'false'},
        {'enabled': 1},
        {'enabeld': True},
        {'default_priority': 0},
        {'default_priority': 11},
        {'default_priority': True},
        {'default_priority': '2'},
        {'rules': [{'product': 'unknown', 'priority': 2}]},
        {'rules': [{'product': 'ocp', 'priority': 0}]},
        {'rules': [{'product': 'ocp', 'priority': 11}]},
        {'rules': [{'product': 'ocp', 'priority': True}]},
        {'rules': [{'product': 'ocp', 'priority': 2, 'versions': []}]},
        {'rules': [{'product': 'ocp', 'priority': 2, 'versions': [4.20]}]},
        {'rules': [{'product': 'ocp', 'priority': 2, 'min_version': '4.18.0'}]},
    ],
)
def test_invalid_policy_is_rejected_even_when_disabled(config):
    with pytest.raises(ValidationError):
        BuildPriorityPolicy.model_validate(config)


class TestLoadBuildPriorityPolicy(IsolatedAsyncioTestCase):
    def setUp(self):
        patcher = patch('artcommonlib.build_priority.aiohttp.ClientSession')
        self.session_class = patcher.start()
        self.addCleanup(patcher.stop)
        self.session = MagicMock()
        self.session_class.return_value.__aenter__.return_value = self.session
        self.response = MagicMock()
        self.response.status = 200
        self.response.text = AsyncMock(return_value='enabled: true\ndefault_priority: 2\n')
        self.session.get.return_value.__aenter__.return_value = self.response

    async def test_fetches_canonical_main_snapshot(self):
        policy = await load_build_priority_policy()
        self.session.get.assert_called_once_with(BUILD_PRIORITY_POLICY_URL)
        self.response.raise_for_status.assert_called_once()
        assert policy.priority_for('rhacm2', 'acm-2.17') == '2'

    async def test_missing_file_retains_normal_priorities(self):
        self.response.status = 404
        policy = await load_build_priority_policy()
        assert not policy.enabled
        self.response.text.assert_not_awaited()

    async def test_http_failure_is_not_treated_as_disabled(self):
        self.response.raise_for_status.side_effect = aiohttp.ClientError('server unavailable')
        with pytest.raises(aiohttp.ClientError):
            await load_build_priority_policy()

    async def test_bad_policy_is_not_treated_as_disabled(self):
        self.response.text.return_value = 'enabled: "false"'
        with pytest.raises(ValidationError):
            await load_build_priority_policy()
