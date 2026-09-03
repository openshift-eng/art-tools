import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from pyartcd.pipelines.olm_bundle_konflux import _stage_release_related_images


class TestStageReleaseRelatedImages(unittest.IsolatedAsyncioTestCase):
    """A stage release failure must be attributed to individual operators, not the whole job.

    pyartcd drops only the failed operators from the FBC trigger list, so it needs the per-operator
    `stage_release_related_images` records doozer writes to record.log.
    """

    def setUp(self):
        self._working_dir = TemporaryDirectory()
        self.addCleanup(self._working_dir.cleanup)
        self.runtime = mock.MagicMock(dry_run=False, doozer_working=self._working_dir.name)

    def _write_record_log(self, *records):
        lines = []
        for record in records:
            fields = "|".join(f"{k}={v}" for k, v in record.items())
            lines.append(f"stage_release_related_images|{fields}|")
        Path(self._working_dir.name, 'record.log').write_text("\n".join(lines) + "\n")

    async def _run(self):
        return await _stage_release_related_images(
            runtime=self.runtime,
            doozer_base_cmd=['doozer', '--group=rhacm2-2.16'],
            namespace='ns',
            final_kubeconfig='/path/to/kubeconfig',
            operator_nvrs=['operator-a-1-1', 'operator-b-1-1'],
        )

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_returns_empty_list_on_success(self, cmd_assert_async: mock.AsyncMock):
        self.assertEqual(await self._run(), [])
        cmd = cmd_assert_async.await_args.args[0]
        self.assertEqual(cmd[-2:], ['operator-a-1-1', 'operator-b-1-1'])
        self.assertIn('beta:bundle:stage-release-related-images', cmd)

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_returns_failed_operator_nvrs(self, cmd_assert_async: mock.AsyncMock):
        cmd_assert_async.side_effect = ChildProcessError('doozer failed')
        self._write_record_log(
            {'status': '1', 'operator_nvr': 'operator-a-1-1', 'operator': 'operator-a', 'message': 'boom'},
            {'status': '0', 'operator_nvr': 'operator-b-1-1', 'operator': 'operator-b', 'message': ''},
        )

        self.assertEqual(await self._run(), ['operator-a-1-1'])

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_reraises_when_no_per_operator_results(self, cmd_assert_async: mock.AsyncMock):
        """A crash before any operator ran (bad kubeconfig, etc.) is a job-wide failure."""
        cmd_assert_async.side_effect = ChildProcessError('doozer failed')

        with self.assertRaises(ChildProcessError):
            await self._run()

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_reraises_when_all_records_succeeded(self, cmd_assert_async: mock.AsyncMock):
        cmd_assert_async.side_effect = ChildProcessError('doozer failed')
        self._write_record_log(
            {'status': '0', 'operator_nvr': 'operator-a-1-1', 'operator': 'operator-a', 'message': ''},
        )

        with self.assertRaises(ChildProcessError):
            await self._run()

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async")
    async def test_passes_dry_run_through(self, cmd_assert_async: mock.AsyncMock):
        self.runtime.dry_run = True
        await self._run()
        self.assertIn('--dry-run', cmd_assert_async.await_args.args[0])


class TestOlmBundleKonfluxStageReleaseGating(unittest.IsolatedAsyncioTestCase):
    """Stage release should only run when bundles were actually built, unless --force-release is set."""

    def setUp(self):
        self._working_dir = TemporaryDirectory()
        self.addCleanup(self._working_dir.cleanup)
        self.runtime = mock.MagicMock(dry_run=False, doozer_working=self._working_dir.name)

    def _write_bundle_record(self, status='0', operator_nvr='op-1-1', bundle_nvr='op-bundle-1-1'):
        line = f"build_olm_bundle_konflux|status={status}|operator_nvr={operator_nvr}|bundle_nvr={bundle_nvr}|"
        Path(self._working_dir.name, 'record.log').write_text(line + "\n")

    def _get_original_func(self):
        from pyartcd.pipelines.olm_bundle_konflux import olm_bundle_konflux

        # Unwrap pass_runtime -> click_coroutine -> original async function
        return olm_bundle_konflux.callback.__wrapped__.__wrapped__

    async def _run_pipeline(
        self,
        mock_stage_release,
        mock_load_group_config,
        mock_locks,
        mock_jenkins,
        nvrs='operator-container-4.18.0-1',
        force_release=False,
        product='rhacm2',
        group_config_extra=None,
    ):
        async def run_with_lock(*, coro, **_kwargs):
            return await coro

        mock_locks.run_with_lock = mock.AsyncMock(side_effect=run_with_lock)
        group_cfg = {'product': product, 'vars': {'MAJOR': '2', 'MINOR': '16'}}
        if group_config_extra:
            group_cfg.update(group_config_extra)
        mock_load_group_config.return_value = group_cfg
        mock_jenkins.get_build_path_or_random.return_value = 'test'
        mock_jenkins.get_propagatable_params.return_value = {}

        await self._get_original_func()(
            runtime=self.runtime,
            version='4.18',
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            nvrs=nvrs,
            only=None,
            exclude=None,
            force=False,
            force_release=force_release,
            kubeconfig='/fake/kubeconfig',
            plr_template='',
            group='openshift-4.18',
        )

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.jenkins")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.locks")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.load_group_config", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux._stage_release_related_images", new_callable=mock.AsyncMock)
    async def test_stage_release_skipped_when_bundles_not_rebuilt(
        self,
        mock_stage_release,
        mock_load_group_config,
        mock_cmd_assert,
        mock_locks,
        mock_jenkins,
    ):
        """When all bundles are skipped (already exist), stage release should not run."""
        await self._run_pipeline(mock_stage_release, mock_load_group_config, mock_locks, mock_jenkins)

        mock_stage_release.assert_not_called()
        mock_jenkins.start_build_fbc.assert_called_once()

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.jenkins")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.locks")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.load_group_config", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux._stage_release_related_images", new_callable=mock.AsyncMock)
    async def test_stage_release_runs_when_force_release_set(
        self,
        mock_stage_release,
        mock_load_group_config,
        mock_cmd_assert,
        mock_locks,
        mock_jenkins,
    ):
        """When --force-release is set, stage release runs even for skipped bundles."""
        mock_stage_release.return_value = []
        await self._run_pipeline(
            mock_stage_release, mock_load_group_config, mock_locks, mock_jenkins, force_release=True
        )

        mock_stage_release.assert_called_once()

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.jenkins")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.locks")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.load_group_config", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux._stage_release_related_images", new_callable=mock.AsyncMock)
    async def test_stage_release_runs_when_bundles_actually_built(
        self,
        mock_stage_release,
        mock_load_group_config,
        mock_cmd_assert,
        mock_locks,
        mock_jenkins,
    ):
        """When bundles are actually built (record.log has entries), stage release runs."""
        mock_stage_release.return_value = []
        self._write_bundle_record(status='0', operator_nvr='op-1-1', bundle_nvr='op-bundle-1-1')

        await self._run_pipeline(mock_stage_release, mock_load_group_config, mock_locks, mock_jenkins, nvrs='')

        mock_stage_release.assert_called_once()

    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.jenkins")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.locks")
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.exectools.cmd_assert_async", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux.load_group_config", new_callable=mock.AsyncMock)
    @mock.patch("pyartcd.pipelines.olm_bundle_konflux._stage_release_related_images", new_callable=mock.AsyncMock)
    async def test_stage_release_skipped_when_no_plan_configured(
        self,
        mock_stage_release,
        mock_load_group_config,
        mock_cmd_assert,
        mock_locks,
        mock_jenkins,
    ):
        """Products with no configured stage release plan skip stage release even with --force-release and built bundles."""
        mock_stage_release.return_value = []
        self._write_bundle_record(status='0', operator_nvr='op-1-1', bundle_nvr='op-bundle-1-1')

        await self._run_pipeline(
            mock_stage_release,
            mock_load_group_config,
            mock_locks,
            mock_jenkins,
            nvrs='',
            force_release=True,
            product='ocp',
            group_config_extra={'vars': {'MAJOR': '5', 'MINOR': '1'}},
        )

        mock_stage_release.assert_not_called()
        mock_jenkins.start_build_fbc.assert_called_once()


if __name__ == '__main__':
    unittest.main()
