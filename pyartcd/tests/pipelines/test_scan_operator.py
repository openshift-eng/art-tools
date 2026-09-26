import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from pyartcd.pipelines.scan_operator import ScanOperatorPipeline
from pyartcd.runtime import Runtime


def _make_operator(name: str = 'test-operator', nvr: str = 'test-operator-1.0-1') -> MagicMock:
    op = MagicMock(spec=KonfluxBuildRecord)
    op.name = name
    op.nvr = nvr
    return op


def _make_bundle(
    name: str = 'test-operator-bundle',
    nvr: str = 'test-operator-bundle-1.0-1',
    outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
    operator_nvr: str = 'test-operator-1.0-1',
    image_pullspec: str = 'quay.io/redhat-prod/ocp-art-tenant/test-operator-bundle@sha256:abc123',
) -> MagicMock:
    bundle = MagicMock(spec=KonfluxBundleBuildRecord)
    bundle.name = name
    bundle.nvr = nvr
    bundle.outcome = outcome
    bundle.operator_nvr = operator_nvr
    bundle.image_pullspec = image_pullspec
    return bundle


def _make_fbc(
    name: str = 'test-operator-fbc',
    nvr: str = 'test-operator-fbc-1.0-1',
    outcome: KonfluxBuildOutcome = KonfluxBuildOutcome.SUCCESS,
) -> MagicMock:
    fbc = MagicMock(spec=KonfluxFbcBuildRecord)
    fbc.name = name
    fbc.nvr = nvr
    fbc.outcome = outcome
    return fbc


class TestScanOperatorPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.scan_operator.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_pipeline(self, version='4.18', assembly='stream'):
        pipeline = ScanOperatorPipeline(
            runtime=self.runtime,
            version=version,
            assembly=assembly,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
        )
        return pipeline

    async def test_check_operator_missing_bundle(self):
        """Operator with no bundle should be added to operators_without_bundles."""
        pipeline = self._make_pipeline()
        operator = _make_operator()

        with patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=None):
            await pipeline.check_operator(operator)

        self.assertIn(operator, pipeline.operators_without_bundles)
        self.assertNotIn(operator, pipeline.operators_without_fbcs)
        self.assertNotIn(operator, pipeline.operators_needing_stage_release)

    async def test_check_operator_failed_bundle(self):
        """Operator with failed bundle should be added to operators_without_bundles."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        failed_bundle = _make_bundle(outcome=KonfluxBuildOutcome.FAILURE)

        with patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=failed_bundle):
            await pipeline.check_operator(operator)

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
            await pipeline.check_operator(operator)

        self.assertNotIn(operator, pipeline.operators_without_bundles)
        self.assertIn(operator, pipeline.operators_without_fbcs)
        self.assertNotIn(operator, pipeline.operators_needing_stage_release)

    async def test_check_operator_pending_bundle_skipped(self):
        """Operator with pending bundle should not trigger any action."""
        pipeline = self._make_pipeline()
        operator = _make_operator()
        pending_bundle = _make_bundle(outcome=KonfluxBuildOutcome.PENDING)

        with patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=pending_bundle):
            await pipeline.check_operator(operator)

        self.assertEqual(len(pipeline.operators_without_bundles), 0)
        self.assertEqual(len(pipeline.operators_without_fbcs), 0)
        self.assertEqual(len(pipeline.operators_needing_stage_release), 0)

    async def test_check_operator_stage_release_missing(self):
        """Operator with bundle but image missing from stage registry should be added to operators_needing_stage_release."""
        pipeline = self._make_pipeline()
        pipeline._check_stage_release = True
        operator = _make_operator()
        bundle = _make_bundle()

        with (
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_stage_registry', new_callable=AsyncMock, return_value=False),
        ):
            await pipeline.check_operator(operator)

        self.assertNotIn(operator, pipeline.operators_without_bundles)
        self.assertNotIn(operator, pipeline.operators_without_fbcs)
        self.assertIn(operator, pipeline.operators_needing_stage_release)

    async def test_check_operator_stage_release_exists(self):
        """Operator with bundle and image in stage registry should continue to FBC check."""
        pipeline = self._make_pipeline()
        pipeline._check_stage_release = True
        operator = _make_operator()
        bundle = _make_bundle()

        with (
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_stage_registry', new_callable=AsyncMock, return_value=True),
            patch.object(pipeline, 'check_fbc_exists', new_callable=AsyncMock, return_value=None),
        ):
            await pipeline.check_operator(operator)

        self.assertNotIn(operator, pipeline.operators_needing_stage_release)
        self.assertIn(operator, pipeline.operators_without_fbcs)

    async def test_check_operator_no_stage_check_skips_registry(self):
        """When stage release checking is not applicable, skip the check entirely."""
        pipeline = self._make_pipeline()
        pipeline._check_stage_release = False
        operator = _make_operator()
        bundle = _make_bundle()

        with (
            patch.object(pipeline, 'check_bundle_exists', new_callable=AsyncMock, return_value=bundle),
            patch.object(pipeline, 'check_fbc_exists', new_callable=AsyncMock, return_value=_make_fbc()),
        ):
            await pipeline.check_operator(operator)

        self.assertEqual(len(pipeline.operators_needing_stage_release), 0)
        self.assertEqual(len(pipeline.operators_without_fbcs), 0)


class TestCheckStageRegistry(unittest.IsolatedAsyncioTestCase):
    """Tests for the check_stage_registry method."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.scan_operator.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_pipeline(self):
        pipeline = ScanOperatorPipeline(
            runtime=self.runtime,
            version='4.18',
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
        )
        pipeline._check_stage_release = True
        pipeline.delivery_repo_by_operator = {
            'test-operator': 'openshift4/ose-test-operator-bundle',
        }
        return pipeline

    @patch('pyartcd.pipelines.scan_operator.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_stage_registry_image_exists(self, mock_cmd):
        """Return True when skopeo finds the image in the stage registry."""
        pipeline = self._make_pipeline()
        operator = _make_operator(name='test-operator', nvr='test-operator-1.0-1')
        bundle = _make_bundle(image_pullspec='quay.io/redhat-prod/ocp-art-tenant/test-bundle@sha256:abc123')

        mock_cmd.return_value = (0, '{}', '')  # skopeo returns 0 = found

        result = await pipeline.check_stage_registry(operator, bundle)

        self.assertTrue(result)
        mock_cmd.assert_called_once_with(
            [
                'skopeo',
                'inspect',
                '--raw',
                'docker://registry.redhat.io/openshift4/ose-test-operator-bundle@sha256:abc123',
            ],
            check=False,
        )

    @patch('pyartcd.pipelines.scan_operator.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_stage_registry_image_missing(self, mock_cmd):
        """Return False when skopeo cannot find the image in the stage registry."""
        pipeline = self._make_pipeline()
        operator = _make_operator(name='test-operator', nvr='test-operator-1.0-1')
        bundle = _make_bundle(image_pullspec='quay.io/redhat-prod/ocp-art-tenant/test-bundle@sha256:abc123')

        mock_cmd.return_value = (1, '', 'manifest unknown')  # skopeo returns non-zero = not found

        result = await pipeline.check_stage_registry(operator, bundle)

        self.assertFalse(result)

    @patch('pyartcd.pipelines.scan_operator.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_stage_registry_inconclusive_error_returns_true(self, mock_cmd):
        """On non-manifest-unknown skopeo error, return True (inconclusive, don't re-trigger)."""
        pipeline = self._make_pipeline()
        operator = _make_operator(name='test-operator', nvr='test-operator-1.0-1')
        bundle = _make_bundle(image_pullspec='quay.io/redhat-prod/ocp-art-tenant/test-bundle@sha256:abc123')

        mock_cmd.return_value = (1, '', 'connection refused')

        result = await pipeline.check_stage_registry(operator, bundle)

        self.assertTrue(result)  # Inconclusive errors should not trigger re-release

    @patch('pyartcd.pipelines.scan_operator.exectools.cmd_gather_async', new_callable=AsyncMock)
    async def test_stage_registry_exception_returns_true(self, mock_cmd):
        """On unexpected exception, return True (don't re-trigger)."""
        pipeline = self._make_pipeline()
        operator = _make_operator(name='test-operator', nvr='test-operator-1.0-1')
        bundle = _make_bundle(image_pullspec='quay.io/redhat-prod/ocp-art-tenant/test-bundle@sha256:abc123')

        mock_cmd.side_effect = Exception('connection refused')

        result = await pipeline.check_stage_registry(operator, bundle)

        self.assertTrue(result)  # Err on the side of caution

    async def test_stage_registry_no_delivery_repo(self):
        """Return True when no delivery repo is configured for the operator."""
        pipeline = self._make_pipeline()
        pipeline.delivery_repo_by_operator = {}  # no delivery repo
        operator = _make_operator(name='test-operator', nvr='test-operator-1.0-1')
        bundle = _make_bundle(image_pullspec='quay.io/redhat-prod/ocp-art-tenant/test-bundle@sha256:abc123')

        result = await pipeline.check_stage_registry(operator, bundle)

        self.assertTrue(result)

    async def test_stage_registry_no_pullspec(self):
        """Return True when bundle has no image_pullspec."""
        pipeline = self._make_pipeline()
        operator = _make_operator(name='test-operator', nvr='test-operator-1.0-1')
        bundle = _make_bundle(image_pullspec='')

        result = await pipeline.check_stage_registry(operator, bundle)

        self.assertTrue(result)

    async def test_stage_registry_pullspec_without_digest(self):
        """Return True when bundle pullspec has no @ digest separator."""
        pipeline = self._make_pipeline()
        operator = _make_operator(name='test-operator', nvr='test-operator-1.0-1')
        bundle = _make_bundle(image_pullspec='quay.io/redhat-prod/test-bundle:latest')

        result = await pipeline.check_stage_registry(operator, bundle)

        self.assertTrue(result)


class TestTriggerBundleBuilds(unittest.TestCase):
    """Tests for the trigger_bundle_builds method with force_release parameter."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = False
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.scan_operator.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_pipeline(self):
        pipeline = ScanOperatorPipeline(
            runtime=self.runtime,
            version='4.18',
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
        )
        return pipeline

    @patch('pyartcd.pipelines.scan_operator.jenkins.start_olm_bundle_konflux')
    def test_trigger_bundle_builds_normal(self, mock_start):
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1'), _make_operator(nvr='op-2')]

        pipeline.trigger_bundle_builds(operators)

        mock_start.assert_called_once_with(
            build_version='4.18',
            assembly='stream',
            operator_nvrs=['op-1', 'op-2'],
            group='openshift-4.18',
            force_release=False,
        )

    @patch('pyartcd.pipelines.scan_operator.jenkins.start_olm_bundle_konflux')
    def test_trigger_bundle_builds_force_release(self, mock_start):
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1')]

        pipeline.trigger_bundle_builds(operators, force_release=True)

        mock_start.assert_called_once_with(
            build_version='4.18',
            assembly='stream',
            operator_nvrs=['op-1'],
            group='openshift-4.18',
            force_release=True,
        )

    def test_trigger_bundle_builds_dry_run(self):
        self.runtime.dry_run = True
        pipeline = self._make_pipeline()
        operators = [_make_operator(nvr='op-1')]

        # Should not raise and not call jenkins
        with patch('pyartcd.pipelines.scan_operator.jenkins.start_olm_bundle_konflux') as mock_start:
            pipeline.trigger_bundle_builds(operators, force_release=True)
            mock_start.assert_not_called()


class TestInitStageReleaseCheck(unittest.IsolatedAsyncioTestCase):
    """Tests for the _init_stage_release_check method."""

    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.dry_run = True
        self.runtime.working_dir = MagicMock()
        self.runtime.working_dir.__truediv__ = lambda self, x: MagicMock()

        # Mock KonfluxDb to prevent real BigQuery connections
        patcher = patch('pyartcd.pipelines.scan_operator.KonfluxDb')
        patcher.start()
        self.addCleanup(patcher.stop)

    @patch('pyartcd.pipelines.scan_operator.load_group_config', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.scan_operator.resolve_konflux_fbc_stage_release_plan')
    async def test_stage_release_check_enabled_with_plan(self, mock_resolve_plan, mock_load_config):
        """When a stage release plan exists, _check_stage_release should be True."""
        mock_load_config.return_value = {'product': 'ocp'}
        mock_resolve_plan.return_value = 'ocp-advisory-stage-4-18'

        pipeline = ScanOperatorPipeline(
            runtime=self.runtime,
            version='4.18',
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
        )
        await pipeline._init_stage_release_check()

        self.assertTrue(pipeline._check_stage_release)

    @patch('pyartcd.pipelines.scan_operator.load_group_config', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.scan_operator.resolve_konflux_fbc_stage_release_plan')
    async def test_stage_release_check_disabled_no_plan(self, mock_resolve_plan, mock_load_config):
        """When no stage release plan exists, _check_stage_release should be False."""
        mock_load_config.return_value = {'product': 'ocp'}
        mock_resolve_plan.return_value = None

        pipeline = ScanOperatorPipeline(
            runtime=self.runtime,
            version='4.18',
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
        )
        await pipeline._init_stage_release_check()

        self.assertFalse(pipeline._check_stage_release)

    async def test_stage_release_check_disabled_non_stream_assembly(self):
        """Non-stream assemblies should not have stage release checking."""
        pipeline = ScanOperatorPipeline(
            runtime=self.runtime,
            version='4.18',
            assembly='4.18.5',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
        )
        await pipeline._init_stage_release_check()

        self.assertFalse(pipeline._check_stage_release)
