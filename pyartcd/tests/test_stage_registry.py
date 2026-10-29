import unittest
from unittest.mock import AsyncMock, patch

from pyartcd.stage_registry import (
    STAGE_REGISTRY,
    check_bundle_in_stage_registry,
    resolve_stage_release_plan,
    stage_registry_auth_file,
)

PULLSPEC = 'quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:abc123'
REPO = 'rhacm2/acm-operator-bundle'


@patch.dict('os.environ', {'REGISTRY_AUTH_FILE': '', 'QUAY_AUTH_FILE': ''})
class TestStageRegistryAuthFile(unittest.TestCase):
    def test_no_auth_file(self):
        self.assertIsNone(stage_registry_auth_file())

    def test_registry_auth_file_wins(self):
        with patch.dict('os.environ', {'REGISTRY_AUTH_FILE': '/a.json', 'QUAY_AUTH_FILE': '/b.json'}):
            self.assertEqual(stage_registry_auth_file(), '/a.json')

    def test_falls_back_to_quay_auth_file(self):
        with patch.dict('os.environ', {'QUAY_AUTH_FILE': '/b.json'}):
            self.assertEqual(stage_registry_auth_file(), '/b.json')


@patch.dict('os.environ', {'REGISTRY_AUTH_FILE': '', 'QUAY_AUTH_FILE': ''})
class TestCheckBundleInStageRegistry(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.stage_registry.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_looks_up_by_digest(self, mock_cmd):
        mock_cmd.return_value = (0, '{}', '')

        result = await check_bundle_in_stage_registry(REPO, PULLSPEC, 'op-1')

        self.assertTrue(result)
        mock_cmd.assert_awaited_once_with(
            ['skopeo', 'inspect', '--raw', f'docker://{STAGE_REGISTRY}/{REPO}@sha256:abc123'],
            check=False,
        )

    @patch('pyartcd.stage_registry.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_manifest_unknown_is_missing(self, mock_cmd):
        mock_cmd.return_value = (1, '', 'reading manifest sha256:abc123: manifest unknown')

        self.assertFalse(await check_bundle_in_stage_registry(REPO, PULLSPEC, 'op-1'))

    @patch('pyartcd.stage_registry.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_unauthorized_is_inconclusive(self, mock_cmd):
        mock_cmd.return_value = (1, '', 'unauthorized: Please login to the Red Hat Registry')

        self.assertIsNone(await check_bundle_in_stage_registry(REPO, PULLSPEC, 'op-1'))

    @patch('pyartcd.stage_registry.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_exception_is_inconclusive(self, mock_cmd):
        mock_cmd.side_effect = Exception('connection refused')

        self.assertIsNone(await check_bundle_in_stage_registry(REPO, PULLSPEC, 'op-1'))

    async def test_missing_inputs_are_inconclusive(self):
        self.assertIsNone(await check_bundle_in_stage_registry(None, PULLSPEC, 'op-1'))
        self.assertIsNone(await check_bundle_in_stage_registry(REPO, '', 'op-1'))
        self.assertIsNone(await check_bundle_in_stage_registry(REPO, 'quay.io/x:latest', 'op-1'))
        self.assertIsNone(await check_bundle_in_stage_registry(REPO, 'quay.io/x@md5:abc', 'op-1'))

    @patch('pyartcd.stage_registry.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_uses_quay_auth_file(self, mock_cmd):
        mock_cmd.return_value = (0, '{}', '')

        with patch.dict('os.environ', {'QUAY_AUTH_FILE': '/tmp/quay-authfile/config.json'}):
            await check_bundle_in_stage_registry(REPO, PULLSPEC, 'op-1')

        self.assertEqual(mock_cmd.await_args.args[0][3:5], ['--authfile', '/tmp/quay-authfile/config.json'])


class TestResolveStageReleasePlan(unittest.TestCase):
    def test_acm_group_resolves_plan(self):
        self.assertEqual(resolve_stage_release_plan('acm-5.0', {'product': 'rhacm2'}), 'acm-advisory-stage-auto-5-0')

    def test_openshift_group_resolves_plan(self):
        self.assertEqual(resolve_stage_release_plan('openshift-4.18', {}), 'ocp-art-advisory-stage-auto-4-18')

    def test_group_without_plan(self):
        self.assertIsNone(resolve_stage_release_plan('oadp-1.5', {}))

    def test_version_from_group_config_when_group_name_has_none(self):
        with patch(
            'pyartcd.stage_registry.resolve_konflux_fbc_stage_release_plan', return_value='plan'
        ) as mock_resolve:
            plan = resolve_stage_release_plan('somegroup', {'product': 'rhacm2', 'version': '5.0.0'})

        self.assertEqual(plan, 'plan')
        mock_resolve.assert_called_once_with('rhacm2', 5, 0)


if __name__ == '__main__':
    unittest.main()
