import unittest
from unittest.mock import patch, AsyncMock
from flexmock import flexmock
from elliottlib.cli import get_golang_versions_cli
from elliottlib import errata as erratalib
from elliottlib import util as utillib
from artcommonlib import rhcos
from elliottlib.cli.common import cli, Runtime
from click.testing import CliRunner


class TestGetGolangVersionsCli(unittest.IsolatedAsyncioTestCase):
    def test_get_golang_versions_advisory(self):
        runner = CliRunner()
        advisory_id = 123
        content_type = 'not docker'
        flexmock(Runtime).should_receive("initialize").and_return(None)
        nvrs = [('foo', 'v1', 'r'), ('bar', 'v1', 'r'), ('runc', 'v1', 'r'), ('podman', 'v1', 'r')]
        go_nvr_map = 'foobar'
        logger = get_golang_versions_cli._LOGGER
        flexmock(erratalib). \
            should_receive("get_all_advisory_nvrs"). \
            with_args(advisory_id). \
            and_return(nvrs)
        flexmock(erratalib). \
            should_receive("get_erratum_content_type"). \
            with_args(advisory_id). \
            and_return(content_type)
        flexmock(utillib). \
            should_receive("get_golang_rpm_nvrs"). \
            with_args([('runc', 'v1', 'r'), ('podman', 'v1', 'r')], logger).and_return(go_nvr_map)
        flexmock(utillib). \
            should_receive("pretty_print_nvrs_go").with_args(go_nvr_map)

        result = runner.invoke(cli, ['go', '--advisory', advisory_id, '--components', 'runc,podman'])
        self.assertEqual(result.exit_code, 0)

    def test_get_golang_versions_nvrs(self):
        runner = CliRunner()
        flexmock(Runtime).should_receive("initialize").and_return(None)
        go_map_1 = {'1.15': {('podman', '1.9.3', '3.rhaos4.6.el8')}}
        go_map_2 = {'1.16': {('podman-container', '3.0.1', '6.el8')}}

        logger = get_golang_versions_cli._LOGGER
        flexmock(utillib). \
            should_receive("get_golang_rpm_nvrs"). \
            with_args([('podman', '1.9.3', '3.rhaos4.6.el8')], logger).and_return(go_map_1)
        flexmock(utillib). \
            should_receive("get_golang_container_nvrs"). \
            with_args([('podman-container', '3.0.1', '6.el8')], logger).and_return(go_map_2)
        go_map_1.update(go_map_2)
        flexmock(utillib). \
            should_receive("pretty_print_nvrs_go").with_args(go_map_1, report=False)

        result = runner.invoke(cli, ['go', '--nvrs', 'podman-container-3.0.1-6.el8,podman-1.9.3-3.rhaos4.6.el8'])
        self.assertEqual(result.exit_code, 0)

    @patch('elliottlib.util.get_nvrs_from_release')
    def test_get_golang_versions_release(self, mock_get_nvrs_from_release: AsyncMock):
        runner = CliRunner()
        flexmock(Runtime).should_receive("initialize").and_return(None)
        payload_nvrs = {'ose-cli-container': ('3.0.1', '6.el8')}
        go_map = {'1.15': {('ose-cli-container', '3.0.1', '6.el8')}}

        logger = get_golang_versions_cli._LOGGER
        flexmock(rhcos). \
            should_receive("get_container_configs"). \
            and_return([{'name': 'machine-os-content'}])
        mock_get_nvrs_from_release.return_value = payload_nvrs
        flexmock(utillib). \
            should_receive("get_golang_container_nvrs"). \
            with_args([('ose-cli-container', '3.0.1', '6.el8')], logger).and_return(go_map)
        flexmock(utillib). \
            should_receive("pretty_print_nvrs_go").with_args(go_map, report=False)

        result = runner.invoke(cli, ['--group=openshift-4.14', 'go', '--release',
                                     'registry.ci.openshift.org/ocp/release:4.14.0-0.nightly-2023-04-24-145153'])
        self.assertEqual(result.exit_code, 0)


if __name__ == '__main__':
    unittest.main()
