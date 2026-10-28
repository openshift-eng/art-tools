import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from pyartcd.pipelines.layered_products_scan_konflux import LayeredProductsScanPipeline
from pyartcd.runtime import Runtime


def _make_operator(name: str = 'oadp-operator', nvr: str = 'oadp-operator-1.5-1') -> MagicMock:
    op = MagicMock(spec=KonfluxBuildRecord)
    op.name = name
    op.nvr = nvr
    return op


def _make_bundle(
    name: str = 'oadp-operator-bundle',
    nvr: str = 'oadp-operator-bundle-1.5-1',
    outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
    operator_nvr: str = 'oadp-operator-1.5-1',
) -> MagicMock:
    bundle = MagicMock(spec=KonfluxBundleBuildRecord)
    bundle.name = name
    bundle.nvr = nvr
    bundle.outcome = outcome
    bundle.operator_nvr = operator_nvr
    return bundle


def _make_fbc(
    name: str = 'oadp-operator-fbc',
    nvr: str = 'oadp-operator-fbc-1.5-1',
    outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
) -> MagicMock:
    fbc = MagicMock(spec=KonfluxFbcBuildRecord)
    fbc.name = name
    fbc.nvr = nvr
    fbc.outcome = outcome
    return fbc


class TestLayeredProductsScanPipeline(unittest.IsolatedAsyncioTestCase):
    """Tests for source-change detection (the original scan-sources flow).

    The scan_for_missing_bundles_fbcs step is mocked out here because these
    tests focus exclusively on the source-change scan and build triggers.
    """

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock scan_for_missing_bundles_fbcs so these source-change-focused
        # tests don't need to set up KonfluxDb / doozer operator-list mocks.
        self._scan_patcher = patch.object(
            LayeredProductsScanPipeline, 'scan_for_missing_bundles_fbcs', new_callable=AsyncMock
        )
        self._scan_patcher.start()
        self.addCleanup(self._scan_patcher.stop)

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_no_changes_detected(self, mock_cmd_gather, mock_jenkins):
        scan_output = yaml.dump({'images': [], 'rpms': []})
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()
        mock_jenkins.start_layered_products.assert_not_called()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_changed_images_trigger_build(self, mock_cmd_gather, mock_jenkins):
        self.runtime.dry_run = False

        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'oadp-velero-restic-restore-helper', 'changed': True},
                    {'name': 'oadp-operator', 'changed': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        mock_jenkins.start_layered_products.assert_called_once_with(
            group='oadp-1.4',
            assembly='stream',
            image_list=['oadp-velero-restic-restore-helper', 'oadp-operator'],
        )

    def test_tekton_changed_images_trigger_generic_build_pipeline(self):
        self.runtime.dry_run = False
        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='quay-3.16',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )
        pipeline.changes = {'images': ['quay-operator']}
        pipeline.report = {'images': [{'name': 'quay-operator', 'changed': True}]}

        with (
            patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.is_tekton_context', return_value=True),
            patch(
                'pyartcd.pipelines.layered_products_scan_konflux.tekton.start_pipeline_run',
                return_value='build-layered-products-quay-3-16-test',
            ) as mock_start_pipeline_run,
            patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.annotate_current_pipelinerun'),
        ):
            pipeline.handle_source_changes()

        mock_start_pipeline_run.assert_called_once_with(
            pipeline_name='build-layered-products',
            params={
                'group': 'quay-3.16',
                'assembly': 'stream',
                'image-list': 'quay-operator',
            },
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_images_with_disabled_dependency_are_filtered(self, mock_cmd_gather, mock_jenkins):
        """When an image has has_disabled_dependency flag, it should be excluded from the build trigger."""
        self.runtime.dry_run = False

        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'oadp-velero-restic-restore-helper', 'changed': True},
                    {'name': 'oadp-operator', 'changed': True, 'has_disabled_dependency': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        mock_jenkins.start_layered_products.assert_called_once_with(
            group='oadp-1.4',
            assembly='stream',
            image_list=['oadp-velero-restic-restore-helper'],
        )

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_all_images_have_disabled_dependency(self, mock_cmd_gather, mock_jenkins):
        """When all changed images have disabled dependencies, no build should be triggered."""
        self.runtime.dry_run = False

        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'oadp-operator', 'changed': True, 'has_disabled_dependency': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        mock_jenkins.start_layered_products.assert_not_called()

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_report_stored_for_filtering(self, mock_cmd_gather, mock_jenkins):
        """Verify that the raw report is stored for use in handle_source_changes."""
        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'test-image', 'changed': True},
                ],
                'rpms': [],
            }
        )
        mock_cmd_gather.return_value = (0, scan_output, '')

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.4',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        await pipeline.run()

        self.assertIn('images', pipeline.report)
        self.assertEqual(len(pipeline.report['images']), 1)
        self.assertEqual(pipeline.report['images'][0]['name'], 'test-image')


class TestScanForMissingBundlesFbcs(unittest.IsolatedAsyncioTestCase):
    """Tests for the scan-operator logic integrated into the LP scan pipeline."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.layered_products_scan_konflux.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

        # Mock load_group_config so load_ocp_target_versions doesn't try
        # to run doozer (which re-raises on failure after Fix 2).
        config_patcher = patch(
            'pyartcd.pipelines.layered_products_scan_konflux.load_group_config',
            new_callable=AsyncMock,
            return_value={},
        )
        config_patcher.start()
        self.addCleanup(config_patcher.stop)

    def _make_pipeline(self, group='oadp-1.5', assembly='stream'):
        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group=group,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly=assembly,
            data_gitref='',
            image_list='',
        )
        return pipeline

    async def test_no_operators_found(self):
        """When no operators are found, the scan should return without triggering builds."""
        pipeline = self._make_pipeline()

        with patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value=set()):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertEqual(pipeline.operators_without_bundles, [])
        self.assertEqual(pipeline.operators_without_fbcs, [])

    async def test_no_successful_builds_found(self):
        """When operators exist but none have successful builds, nothing is triggered."""
        pipeline = self._make_pipeline()

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[]),
        ):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertEqual(pipeline.operators_without_bundles, [])
        self.assertEqual(pipeline.operators_without_fbcs, [])

    async def test_missing_bundle_triggers_build(self):
        """Operator with no bundle should be added to operators_without_bundles."""
        pipeline = self._make_pipeline()
        operator = _make_operator()

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[operator]),
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=None),
            patch.object(pipeline, 'trigger_bundle_builds') as mock_trigger,
        ):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertIn(operator, pipeline.operators_without_bundles)
        mock_trigger.assert_called_once_with([operator])

    async def test_failed_bundle_triggers_build(self):
        """Operator with failed bundle should be added to operators_without_bundles."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        failed_bundle = _make_bundle(outcome=KonfluxBuildOutcome.FAILURE)

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[operator]),
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=failed_bundle),
            patch.object(pipeline, 'trigger_bundle_builds') as mock_trigger,
        ):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertIn(operator, pipeline.operators_without_bundles)
        mock_trigger.assert_called_once()

    async def test_missing_fbc_triggers_build(self):
        """Operator with successful bundle but missing FBC should trigger FBC build."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        bundle = _make_bundle()

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[operator]),
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_fbc_exists', new_callable=AsyncMock, return_value=None),
            patch.object(pipeline, 'trigger_fbc_builds') as mock_trigger_fbc,
        ):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertNotIn(operator, pipeline.operators_without_bundles)
        self.assertIn(operator, pipeline.operators_without_fbcs)
        mock_trigger_fbc.assert_called_once_with([operator])

    async def test_all_bundles_and_fbcs_exist(self):
        """When all operators have bundles and FBCs, no builds are triggered."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        bundle = _make_bundle()
        fbc = _make_fbc()

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[operator]),
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_fbc_exists', new_callable=AsyncMock, return_value=fbc),
            patch.object(pipeline, 'trigger_bundle_builds') as mock_trigger_bundle,
            patch.object(pipeline, 'trigger_fbc_builds') as mock_trigger_fbc,
        ):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertEqual(pipeline.operators_without_bundles, [])
        self.assertEqual(pipeline.operators_without_fbcs, [])
        mock_trigger_bundle.assert_not_called()
        mock_trigger_fbc.assert_not_called()

    async def test_pending_bundle_skipped(self):
        """Operator with pending bundle should not trigger any action."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        pending_bundle = _make_bundle(outcome=KonfluxBuildOutcome.PENDING)

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[operator]),
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=pending_bundle),
            patch.object(pipeline, 'trigger_bundle_builds') as mock_trigger_bundle,
            patch.object(pipeline, 'trigger_fbc_builds') as mock_trigger_fbc,
        ):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertEqual(pipeline.operators_without_bundles, [])
        self.assertEqual(pipeline.operators_without_fbcs, [])
        mock_trigger_bundle.assert_not_called()
        mock_trigger_fbc.assert_not_called()

    async def test_version_extraction(self):
        """Version should be extracted correctly from group name."""
        pipeline = self._make_pipeline(group='oadp-1.5')
        self.assertEqual(pipeline.version, '1.5')

        pipeline2 = self._make_pipeline(group='logging-6.7')
        self.assertEqual(pipeline2.version, '6.7')

        pipeline3 = self._make_pipeline(group='quay-3.16')
        self.assertEqual(pipeline3.version, '3.16')

    async def test_trigger_errors_raise_runtime_error(self):
        """If triggering builds fails, a RuntimeError should be raised."""
        pipeline = self._make_pipeline()
        operator = _make_operator()

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[operator]),
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=None),
            patch.object(pipeline, 'trigger_bundle_builds', side_effect=RuntimeError('Jenkins failure')),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                await pipeline.scan_for_missing_bundles_fbcs()
            self.assertIn('Jenkins failure', str(ctx.exception))

    async def test_check_operator_exceptions_are_collected_and_raised(self):
        """Exceptions from check_operator (e.g. BigQuery errors) must not be silently swallowed."""
        pipeline = self._make_pipeline()
        operator = _make_operator()

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[operator]),
            patch.object(
                pipeline, 'check_operator', new_callable=AsyncMock, side_effect=RuntimeError('BigQuery timeout')
            ),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                await pipeline.scan_for_missing_bundles_fbcs()
            self.assertIn('BigQuery timeout', str(ctx.exception))

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_ocp_target_versions_loaded(self, mock_load_group_config):
        """OCP_TARGET_VERSIONS should be loaded from group config and stored on the pipeline."""
        mock_load_group_config.return_value = {'OCP_TARGET_VERSIONS': ['4.14', '4.15']}
        pipeline = self._make_pipeline()

        await pipeline.load_ocp_target_versions()

        self.assertEqual(pipeline.ocp_target_versions, ['4.14', '4.15'])

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_version_prefer_group_config_over_group_name(self, mock_load_group_config):
        """group.yml version must win over MAJOR.MINOR from the group name (FBC NVR source)."""
        mock_load_group_config.return_value = {'version': '1.4.4', 'OCP_TARGET_VERSIONS': ['4.19']}
        pipeline = self._make_pipeline(group='kueue-1.4')
        self.assertEqual(pipeline.version, '1.4')  # provisional from group name

        await pipeline.load_ocp_target_versions()

        self.assertEqual(pipeline.version, '1.4.4')
        self.assertEqual(pipeline.ocp_target_versions, ['4.19'])

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_version_fallback_to_group_name(self, mock_load_group_config):
        """When group.yml omits version, keep MAJOR.MINOR derived from the group name."""
        mock_load_group_config.return_value = {'OCP_TARGET_VERSIONS': ['4.14']}
        pipeline = self._make_pipeline(group='oadp-1.5')
        self.assertEqual(pipeline.version, '1.5')

        await pipeline.load_ocp_target_versions()

        self.assertEqual(pipeline.version, '1.5')

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_ocp_target_versions_not_set(self, mock_load_group_config):
        """When OCP_TARGET_VERSIONS is not in group config, ocp_target_versions should be empty."""
        mock_load_group_config.return_value = {'some_other_key': 'value'}
        pipeline = self._make_pipeline()

        await pipeline.load_ocp_target_versions()

        self.assertEqual(pipeline.ocp_target_versions, [])

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_ocp_target_versions_load_failure_raises(self, mock_load_group_config):
        """If loading group config fails, the exception should propagate."""
        mock_load_group_config.side_effect = RuntimeError('doozer failed')
        pipeline = self._make_pipeline()

        with self.assertRaises(RuntimeError) as ctx:
            await pipeline.load_ocp_target_versions()
        self.assertIn('doozer failed', str(ctx.exception))

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_ocp_target_versions_string_wrapped_in_list(self, mock_load_group_config):
        """A bare string OCP_TARGET_VERSIONS should be wrapped in a list."""
        mock_load_group_config.return_value = {'OCP_TARGET_VERSIONS': '4.14'}
        pipeline = self._make_pipeline()

        await pipeline.load_ocp_target_versions()

        self.assertEqual(pipeline.ocp_target_versions, ['4.14'])

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_ocp_target_versions_invalid_entries_filtered(self, mock_load_group_config):
        """Invalid OCP_TARGET_VERSIONS entries should be filtered out."""
        mock_load_group_config.return_value = {'OCP_TARGET_VERSIONS': ['4.14', 'bad', '4.15', '4.14.1', '', '5.0']}
        pipeline = self._make_pipeline()

        await pipeline.load_ocp_target_versions()

        self.assertEqual(pipeline.ocp_target_versions, ['4.14', '4.15', '5.0'])

    @patch('pyartcd.pipelines.layered_products_scan_konflux.load_group_config', new_callable=AsyncMock)
    async def test_scan_loads_ocp_target_versions(self, mock_load_group_config):
        """scan_for_missing_bundles_fbcs should call load_ocp_target_versions."""
        mock_load_group_config.return_value = {'version': '1.5.8', 'OCP_TARGET_VERSIONS': ['4.14']}
        pipeline = self._make_pipeline()

        with (
            patch.object(pipeline, 'load_operator_names', new_callable=AsyncMock, return_value={'oadp-operator'}),
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[]),
        ):
            await pipeline.scan_for_missing_bundles_fbcs()

        self.assertEqual(pipeline.ocp_target_versions, ['4.14'])
        self.assertEqual(pipeline.version, '1.5.8')


class TestDeduplication(unittest.IsolatedAsyncioTestCase):
    """Tests for the deduplication logic: operators already being rebuilt
    due to source changes should be skipped in the bundle/FBC scan."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.layered_products_scan_konflux.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

        # Mock load_group_config so load_ocp_target_versions doesn't try
        # to run doozer (which re-raises on failure after Fix 2).
        config_patcher = patch(
            'pyartcd.pipelines.layered_products_scan_konflux.load_group_config',
            new_callable=AsyncMock,
            return_value={},
        )
        config_patcher.start()
        self.addCleanup(config_patcher.stop)

    def _make_pipeline(self, group='oadp-1.5'):
        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group=group,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )
        return pipeline

    async def test_operator_already_rebuilding_is_skipped(self):
        """An operator whose name appears in rebuilding_images should be skipped."""
        pipeline = self._make_pipeline()
        pipeline.rebuilding_images = ['oadp-operator']

        operator = _make_operator(name='oadp-operator')

        mock_bundle_db = MagicMock()
        mock_fbc_db = MagicMock()

        await pipeline.check_operator(mock_bundle_db, mock_fbc_db, operator)

        self.assertNotIn(operator, pipeline.operators_without_bundles)
        self.assertNotIn(operator, pipeline.operators_without_fbcs)

    async def test_non_rebuilding_operator_is_checked(self):
        """Operators NOT in rebuilding_images should still be checked for bundles/FBCs."""
        pipeline = self._make_pipeline()
        pipeline.rebuilding_images = ['some-other-image']

        operator = _make_operator(name='oadp-operator')

        with (
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=None),
        ):
            mock_bundle_db = MagicMock()
            mock_fbc_db = MagicMock()
            await pipeline.check_operator(mock_bundle_db, mock_fbc_db, operator)

        self.assertIn(operator, pipeline.operators_without_bundles)

    async def test_handle_source_changes_sets_rebuilding_images(self):
        """handle_source_changes should populate rebuilding_images for dedup."""
        pipeline = self._make_pipeline()
        pipeline.changes = {'images': ['oadp-operator', 'oadp-velero']}
        pipeline.report = {
            'images': [
                {'name': 'oadp-operator', 'changed': True},
                {'name': 'oadp-velero', 'changed': True},
            ]
        }

        pipeline.handle_source_changes()

        self.assertEqual(pipeline.rebuilding_images, ['oadp-operator', 'oadp-velero'])

    async def test_handle_source_changes_excludes_disabled_from_rebuilding(self):
        """Images with has_disabled_dependency should not be in rebuilding_images."""
        pipeline = self._make_pipeline()
        pipeline.changes = {'images': ['oadp-operator', 'oadp-velero']}
        pipeline.report = {
            'images': [
                {'name': 'oadp-operator', 'changed': True},
                {'name': 'oadp-velero', 'changed': True, 'has_disabled_dependency': True},
            ]
        }

        pipeline.handle_source_changes()

        self.assertEqual(pipeline.rebuilding_images, ['oadp-operator'])

    @patch.dict(os.environ, {'KUBECONFIG': '/path/to/kubeconfig'})
    @patch('pyartcd.pipelines.layered_products_scan_konflux.KonfluxDb')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.exectools.cmd_gather_async')
    async def test_full_run_dedup_integration(self, mock_cmd_gather, mock_jenkins, mock_konflux_db):
        """Integration test: operator changed by source scan should be skipped in bundle/FBC scan."""
        self.runtime.dry_run = True

        # scan-sources returns oadp-operator as changed
        scan_output = yaml.dump(
            {
                'images': [
                    {'name': 'oadp-operator', 'changed': True},
                    {'name': 'oadp-velero', 'changed': True},
                ],
                'rpms': [],
            }
        )

        # load_operator_names returns both operators
        operator_list_output = 'oadp-operator\toadp-operator-bundle\noadp-velero\toadp-velero-bundle'

        mock_cmd_gather.side_effect = [
            (0, scan_output, ''),  # scan-sources
            (0, operator_list_output, ''),  # olm-bundle:list-olm-operators
        ]

        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.5',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

        # Mock operator build queries to return builds for both operators
        op1 = _make_operator(name='oadp-operator', nvr='oadp-operator-1.5-1')
        op2 = _make_operator(name='oadp-velero', nvr='oadp-velero-1.5-1')

        with (
            patch.object(pipeline, 'get_latest_operator_builds', new_callable=AsyncMock, return_value=[op1, op2]),
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=None),
            patch.object(pipeline, 'trigger_bundle_builds') as mock_trigger_bundle,
        ):
            await pipeline.run()

        # Both operators have source changes, so both should be skipped in bundle/FBC check
        self.assertEqual(pipeline.rebuilding_images, ['oadp-operator', 'oadp-velero'])
        # Neither should be in operators_without_bundles (dedup kicked in)
        self.assertEqual(pipeline.operators_without_bundles, [])
        mock_trigger_bundle.assert_not_called()


class TestCheckOperator(unittest.IsolatedAsyncioTestCase):
    """Tests for the check_operator method decision tree."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.layered_products_scan_konflux.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_pipeline(self):
        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.5',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )
        return pipeline

    async def test_check_operator_missing_bundle(self):
        """Operator with no bundle should be added to operators_without_bundles."""
        pipeline = self._make_pipeline()
        operator = _make_operator()

        with patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=None):
            await pipeline.check_operator(MagicMock(), MagicMock(), operator)

        self.assertIn(operator, pipeline.operators_without_bundles)
        self.assertNotIn(operator, pipeline.operators_without_fbcs)

    async def test_check_operator_failed_bundle(self):
        """Operator with failed bundle should be added to operators_without_bundles."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        failed_bundle = _make_bundle(outcome=KonfluxBuildOutcome.FAILURE)

        with patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=failed_bundle):
            await pipeline.check_operator(MagicMock(), MagicMock(), operator)

        self.assertIn(operator, pipeline.operators_without_bundles)

    async def test_check_operator_success_bundle_missing_fbc(self):
        """Operator with successful bundle but no FBC should be added to operators_without_fbcs."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        bundle = _make_bundle()

        with (
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_fbc_exists', new_callable=AsyncMock, return_value=None),
        ):
            await pipeline.check_operator(MagicMock(), MagicMock(), operator)

        self.assertNotIn(operator, pipeline.operators_without_bundles)
        self.assertIn(operator, pipeline.operators_without_fbcs)

    async def test_check_operator_success_bundle_and_fbc(self):
        """Operator with both bundle and FBC should not be tracked."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        bundle = _make_bundle()
        fbc = _make_fbc()

        with (
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_fbc_exists', new_callable=AsyncMock, return_value=fbc),
        ):
            await pipeline.check_operator(MagicMock(), MagicMock(), operator)

        self.assertNotIn(operator, pipeline.operators_without_bundles)
        self.assertNotIn(operator, pipeline.operators_without_fbcs)

    async def test_check_operator_pending_bundle_skipped(self):
        """Operator with pending bundle should not trigger any action."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        pending_bundle = _make_bundle(outcome=KonfluxBuildOutcome.PENDING)

        with patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=pending_bundle):
            await pipeline.check_operator(MagicMock(), MagicMock(), operator)

        self.assertEqual(len(pipeline.operators_without_bundles), 0)
        self.assertEqual(len(pipeline.operators_without_fbcs), 0)

    async def test_check_operator_failed_fbc(self):
        """Operator with failed FBC should be added to operators_without_fbcs."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        bundle = _make_bundle()
        failed_fbc = _make_fbc(outcome=KonfluxBuildOutcome.FAILURE)

        with (
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_fbc_exists', new_callable=AsyncMock, return_value=failed_fbc),
        ):
            await pipeline.check_operator(MagicMock(), MagicMock(), operator)

        self.assertIn(operator, pipeline.operators_without_fbcs)


class TestTriggerBuilds(unittest.TestCase):
    """Tests for trigger_bundle_builds and trigger_fbc_builds methods."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = False
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.layered_products_scan_konflux.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_pipeline(self):
        pipeline = LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.5',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )
        return pipeline

    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.annotate_current_pipelinerun')
    @patch(
        'pyartcd.pipelines.layered_products_scan_konflux.tekton.start_pipeline_run',
        return_value='olm-bundle-konflux-oadp-1-5-abc',
    )
    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.is_tekton_context', return_value=True)
    def test_trigger_bundle_builds_tekton(self, _mock_ctx, mock_start, mock_annotate):
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1'), _make_operator(nvr='op-2')]

        pipeline.trigger_bundle_builds(operators)

        mock_start.assert_called_once_with(
            pipeline_name='olm-bundle-konflux',
            params={
                'version': '1.5',
                'assembly': 'stream',
                'group': 'oadp-1.5',
                'operator-nvrs': 'op-1,op-2',
                'data-path': 'https://github.com/openshift-eng/ocp-build-data',
                'data-gitref': '',
                'dry-run': 'false',
            },
        )
        mock_annotate.assert_called_once_with(
            {'art.openshift.io/triggered-olm-bundle-konflux': 'olm-bundle-konflux-oadp-1-5-abc'}
        )

    def test_trigger_fbc_builds_no_target_versions_raises(self):
        """When ocp_target_versions is empty, trigger_fbc_builds raises RuntimeError."""
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1')]

        with self.assertRaises(RuntimeError) as ctx:
            pipeline.trigger_fbc_builds(operators)
        self.assertIn('No OCP_TARGET_VERSIONS configured', str(ctx.exception))

    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins.start_olm_bundle_konflux')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.is_tekton_context', return_value=False)
    def test_trigger_bundle_builds_jenkins(self, _mock_ctx, mock_jenkins_start):
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1'), _make_operator(nvr='op-2')]

        pipeline.trigger_bundle_builds(operators)

        mock_jenkins_start.assert_called_once_with(
            build_version='1.5',
            assembly='stream',
            operator_nvrs=['op-1', 'op-2'],
            group='oadp-1.5',
            doozer_data_path='https://github.com/openshift-eng/ocp-build-data',
            doozer_data_gitref='',
        )

    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.annotate_current_pipelinerun')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.start_pipeline_run')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.is_tekton_context', return_value=True)
    def test_trigger_fbc_builds_tekton_multiple_target_versions(self, _mock_ctx, mock_start, mock_annotate):
        """With multiple OCP target versions, a separate PipelineRun is created for each."""
        pipeline = self._make_pipeline()
        pipeline.ocp_target_versions = ['4.14', '4.15', '4.16']
        operators = [_make_operator(nvr='op-1')]

        mock_start.side_effect = [
            'build-fbc-oadp-1-5-v414',
            'build-fbc-oadp-1-5-v415',
            'build-fbc-oadp-1-5-v416',
        ]

        pipeline.trigger_fbc_builds(operators)

        self.assertEqual(mock_start.call_count, 3)
        for i, tv in enumerate(['4.14', '4.15', '4.16']):
            call_params = mock_start.call_args_list[i].kwargs['params']
            self.assertEqual(call_params['ocp-target-version'], tv)
            self.assertEqual(call_params['force'], 'true')

        self.assertEqual(mock_annotate.call_count, 3)
        mock_annotate.assert_any_call({'art.openshift.io/triggered-build-fbc-4.14': 'build-fbc-oadp-1-5-v414'})
        mock_annotate.assert_any_call({'art.openshift.io/triggered-build-fbc-4.15': 'build-fbc-oadp-1-5-v415'})
        mock_annotate.assert_any_call({'art.openshift.io/triggered-build-fbc-4.16': 'build-fbc-oadp-1-5-v416'})

    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.annotate_current_pipelinerun')
    @patch(
        'pyartcd.pipelines.layered_products_scan_konflux.tekton.start_pipeline_run',
        return_value='build-fbc-oadp-1-5-v414',
    )
    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.is_tekton_context', return_value=True)
    def test_trigger_fbc_builds_tekton_single_target_version(self, _mock_ctx, mock_start, mock_annotate):
        """With a single OCP target version, one PipelineRun is created with ocp-target-version."""
        pipeline = self._make_pipeline()
        # Simulate post-load_ocp_target_versions state (group.yml version, not group-name MAJOR.MINOR)
        pipeline.version = '1.5.8'
        pipeline.ocp_target_versions = ['4.14']
        operators = [_make_operator(nvr='op-1')]

        pipeline.trigger_fbc_builds(operators)

        mock_start.assert_called_once_with(
            pipeline_name='build-fbc',
            params={
                'version': '1.5.8',
                'assembly': 'stream',
                'group': 'oadp-1.5',
                'operator-nvrs': 'op-1',
                'data-path': 'https://github.com/openshift-eng/ocp-build-data',
                'data-gitref': '',
                'dry-run': 'false',
                'ocp-target-version': '4.14',
                'force': 'true',
            },
        )
        mock_annotate.assert_called_once_with({'art.openshift.io/triggered-build-fbc-4.14': 'build-fbc-oadp-1-5-v414'})

    def test_trigger_fbc_builds_jenkins_no_target_versions_raises(self):
        """Jenkins fallback: no target versions raises RuntimeError."""
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1')]

        with self.assertRaises(RuntimeError) as ctx:
            pipeline.trigger_fbc_builds(operators)
        self.assertIn('No OCP_TARGET_VERSIONS configured', str(ctx.exception))

    @patch('pyartcd.pipelines.layered_products_scan_konflux.jenkins.start_build_fbc')
    @patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.is_tekton_context', return_value=False)
    def test_trigger_fbc_builds_jenkins_multiple_target_versions(self, _mock_ctx, mock_jenkins_start):
        """Jenkins fallback: with target versions triggers one build-fbc per version."""
        pipeline = self._make_pipeline()
        pipeline.ocp_target_versions = ['4.14', '4.15']
        operators = [_make_operator(nvr='op-1')]

        pipeline.trigger_fbc_builds(operators)

        self.assertEqual(mock_jenkins_start.call_count, 2)
        mock_jenkins_start.assert_any_call(
            version='1.5',
            assembly='stream',
            operator_nvrs=['op-1'],
            dry_run=False,
            group='oadp-1.5',
            ocp_target_version='4.14',
            force_build=True,
        )
        mock_jenkins_start.assert_any_call(
            version='1.5',
            assembly='stream',
            operator_nvrs=['op-1'],
            dry_run=False,
            group='oadp-1.5',
            ocp_target_version='4.15',
            force_build=True,
        )

    def test_trigger_bundle_builds_dry_run(self):
        self.runtime.dry_run = True
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1')]

        with patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.start_pipeline_run') as mock_start:
            pipeline.trigger_bundle_builds(operators)
            mock_start.assert_not_called()

    def test_trigger_fbc_builds_dry_run(self):
        self.runtime.dry_run = True
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1')]

        with patch('pyartcd.pipelines.layered_products_scan_konflux.tekton.start_pipeline_run') as mock_start:
            pipeline.trigger_fbc_builds(operators)
            mock_start.assert_not_called()


class TestNaming(unittest.TestCase):
    """Tests for get_bundle_name and get_fbc_name helper methods."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        patcher = patch('pyartcd.pipelines.layered_products_scan_konflux.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_pipeline(self):
        return LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.5',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

    def test_get_bundle_name_default(self):
        pipeline = self._make_pipeline()
        self.assertEqual(pipeline.get_bundle_name('oadp-operator'), 'oadp-operator-bundle')

    def test_get_bundle_name_with_override(self):
        pipeline = self._make_pipeline()
        pipeline.bundle_names_by_operator = {'oadp-operator': 'my-custom-bundle'}
        self.assertEqual(pipeline.get_bundle_name('oadp-operator'), 'my-custom-bundle')

    def test_get_fbc_name(self):
        pipeline = self._make_pipeline()
        self.assertEqual(pipeline.get_fbc_name('oadp-operator'), 'oadp-operator-fbc')


class TestCheckFbcExists(unittest.IsolatedAsyncioTestCase):
    """Tests for check_fbc_exists, including per-version logic."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        patcher = patch('pyartcd.pipelines.layered_products_scan_konflux.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_pipeline(self):
        return LayeredProductsScanPipeline(
            runtime=self.runtime,
            group='oadp-1.5',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            assembly='stream',
            data_gitref='',
            image_list='',
        )

    async def test_per_version_all_exist(self):
        """When all OCP target versions have successful FBCs, return a SUCCESS FBC."""
        pipeline = self._make_pipeline()
        pipeline.ocp_target_versions = ['4.14', '4.15']

        operator = _make_operator()
        bundle = _make_bundle()

        fbc_414 = _make_fbc(nvr='oadp-operator-fbc-1.5-1.ocp4.14')
        fbc_415 = _make_fbc(nvr='oadp-operator-fbc-1.5-1.ocp4.15')

        mock_fbc_db = MagicMock()

        async def mock_search(**kwargs):
            extra_patterns = kwargs.get('extra_patterns', {})
            nvr_pattern = extra_patterns.get('nvr', '')
            where = kwargs.get('where', {})
            outcome = where.get('outcome')

            if outcome == KonfluxBuildOutcome.SUCCESS:
                if r'4\.14' in nvr_pattern:
                    yield fbc_414
                elif r'4\.15' in nvr_pattern:
                    yield fbc_415

        mock_fbc_db.search_builds_by_fields = mock_search

        result = await pipeline.check_fbc_exists(mock_fbc_db, operator, bundle)
        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, KonfluxBuildOutcome.SUCCESS)

    async def test_per_version_one_missing(self):
        """When one OCP target version is missing an FBC, return None."""
        pipeline = self._make_pipeline()
        pipeline.ocp_target_versions = ['4.14', '4.15']

        operator = _make_operator()
        bundle = _make_bundle()

        fbc_414 = _make_fbc(nvr='oadp-operator-fbc-1.5-1.ocp4.14')

        mock_fbc_db = MagicMock()

        async def mock_search(**kwargs):
            extra_patterns = kwargs.get('extra_patterns', {})
            nvr_pattern = extra_patterns.get('nvr', '')
            where = kwargs.get('where', {})
            outcome = where.get('outcome')

            if outcome == KonfluxBuildOutcome.SUCCESS:
                if r'4\.14' in nvr_pattern:
                    yield fbc_414
                # No FBC for 4.15 → nothing yielded
            # PENDING/FAILURE checks yield nothing too
            return

        mock_fbc_db.search_builds_by_fields = mock_search

        result = await pipeline.check_fbc_exists(mock_fbc_db, operator, bundle)
        self.assertIsNone(result)

    async def test_no_target_versions_original_behavior(self):
        """Without OCP target versions, use original single-check behaviour."""
        pipeline = self._make_pipeline()
        pipeline.ocp_target_versions = []

        operator = _make_operator()
        bundle = _make_bundle()
        fbc = _make_fbc()

        mock_fbc_db = MagicMock()

        async def mock_search(**kwargs):
            where = kwargs.get('where', {})
            outcome = where.get('outcome')
            if outcome == KonfluxBuildOutcome.SUCCESS:
                yield fbc

        mock_fbc_db.search_builds_by_fields = mock_search

        result = await pipeline.check_fbc_exists(mock_fbc_db, operator, bundle)
        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, KonfluxBuildOutcome.SUCCESS)

    async def test_per_version_missing_detected_despite_pending_other(self):
        """Missing 4.15 must be detected even when 4.14 is pending.

        Before the fix, _check_fbc_per_version returned immediately on a
        pending record, masking missing FBCs for later versions. The
        corrected method checks ALL versions and prioritises missing
        over pending.
        """
        pipeline = self._make_pipeline()
        pipeline.ocp_target_versions = ['4.14', '4.15']

        operator = _make_operator()
        bundle = _make_bundle()

        pending_414 = _make_fbc(nvr='oadp-operator-fbc-1.5-1.ocp4.14', outcome=KonfluxBuildOutcome.PENDING)

        mock_fbc_db = MagicMock()

        async def mock_search(**kwargs):
            extra_patterns = kwargs.get('extra_patterns', {})
            nvr_pattern = extra_patterns.get('nvr', '')
            where = kwargs.get('where', {})
            outcome = where.get('outcome')

            if outcome == KonfluxBuildOutcome.PENDING and r'4\.14' in nvr_pattern:
                yield pending_414
            # 4.14 has no SUCCESS, 4.15 has nothing at all → missing

        mock_fbc_db.search_builds_by_fields = mock_search

        result = await pipeline.check_fbc_exists(mock_fbc_db, operator, bundle)
        # Missing version detected → returns None (triggers rebuild)
        self.assertIsNone(result)

    async def test_per_version_all_pending_returns_pending(self):
        """When all versions have pending FBCs (none missing), return a pending record."""
        pipeline = self._make_pipeline()
        pipeline.ocp_target_versions = ['4.14', '4.15']

        operator = _make_operator()
        bundle = _make_bundle()

        pending_414 = _make_fbc(nvr='oadp-operator-fbc-1.5-1.ocp4.14', outcome=KonfluxBuildOutcome.PENDING)
        pending_415 = _make_fbc(nvr='oadp-operator-fbc-1.5-1.ocp4.15', outcome=KonfluxBuildOutcome.PENDING)

        mock_fbc_db = MagicMock()

        async def mock_search(**kwargs):
            extra_patterns = kwargs.get('extra_patterns', {})
            nvr_pattern = extra_patterns.get('nvr', '')
            where = kwargs.get('where', {})
            outcome = where.get('outcome')

            if outcome == KonfluxBuildOutcome.PENDING:
                if r'4\.14' in nvr_pattern:
                    yield pending_414
                elif r'4\.15' in nvr_pattern:
                    yield pending_415
            # No SUCCESS for either version

        mock_fbc_db.search_builds_by_fields = mock_search

        result = await pipeline.check_fbc_exists(mock_fbc_db, operator, bundle)
        # All versions have pending FBCs, none missing → returns pending record
        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, KonfluxBuildOutcome.PENDING)


if __name__ == '__main__':
    unittest.main()
