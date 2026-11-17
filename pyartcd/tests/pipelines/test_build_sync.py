from unittest import TestCase
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from pyartcd.pipelines.build_sync import BuildSyncPipeline, build_sync


class TestBuildSyncRetirement(TestCase):
    def pipeline(self, build_system='konflux'):
        return BuildSyncPipeline(
            runtime=MagicMock(),
            version='4.22',
            assembly='stream',
            publish=False,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            emergency_ignore_issues=False,
            retrigger_current_nightly=False,
            doozer_data_gitref=None,
            images=None,
            exclude_arches=None,
            skip_multiarch_payload=False,
            embargo_permit_ack=False,
            build_system=build_system,
        )

    def test_rejects_brew_payloads(self):
        with self.assertRaisesRegex(ValueError, 'Brew build-sync has been retired'):
            self.pipeline('brew')

    @patch('pyartcd.pipelines.build_sync.uses_konflux_imagestream_override')
    def test_preserves_konflux_imagestream_names_and_counter(self, override):
        for enabled, expected in [(True, '4.22-art-latest'), (False, '4.22-konflux-art-latest')]:
            with self.subTest(override=enabled):
                override.return_value = enabled
                pipeline = self.pipeline()
                self.assertEqual(pipeline.is_base_name, expected)
                self.assertEqual(pipeline.fail_count_name, 'count:build-sync-failure:konflux:stream:4.22')

    @patch('pyartcd.pipelines.build_sync.jenkins.init_jenkins')
    def test_cli_rejects_brew_before_connecting_to_jenkins(self, init_jenkins):
        result = CliRunner().invoke(build_sync, ['--version', '4.22', '--build-system', 'brew'])
        self.assertEqual(result.exit_code, 2)
        self.assertIn("'brew' is not 'konflux'", result.output)
        init_jenkins.assert_not_called()
