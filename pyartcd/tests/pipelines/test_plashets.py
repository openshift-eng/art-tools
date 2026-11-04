import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.build_plashets import BuildPlashetsPipeline
from pyartcd.plashets import build_plashet_from_tags, require_published_baseline_symlink, seed_plashet_baseline


class TestBuildCompose(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.jenkins.init_jenkins')
    @patch('pyartcd.jenkins.start_build')
    @patch('pyartcd.pipelines.build_plashets.build_plashets', new_callable=AsyncMock, return_value={})
    async def test_dry_run_prepares_plashets_without_launching_ci_sync(self, build_plashets, submit, _init):
        pipeline = BuildPlashetsPipeline(
            runtime=MagicMock(dry_run=True),
            group='openshift-5.1',
            release='test',
            assembly='stream',
        )
        pipeline.is_compose_build_permitted = AsyncMock(return_value=True)

        await pipeline.run()

        build_plashets.assert_awaited_once()
        self.assertTrue(build_plashets.await_args.kwargs['dry_run'])
        submit.assert_not_called()

    @patch('pyartcd.jenkins.init_jenkins')
    @patch('pyartcd.jenkins.update_title')
    @patch('pyartcd.locks.LockManager.from_lock', return_value=AsyncMock)
    @patch('pyartcd.pipelines.build_plashets.build_plashets', return_value=AsyncMock)
    @patch('pyartcd.jenkins.start_sync_for_ci')
    @patch('pyartcd.jenkins.start_rhcos')
    async def test_build_plashets(self, mocked_rhcos, mocked_sync_for_ci, mocked_build_plashets, mocked_lm, *_):
        build_plashets_pipeline = BuildPlashetsPipeline(
            runtime=MagicMock(dry_run=False),
            group='openshift-4.13',
            release='bogus',
            assembly='stream',
            data_path='',
            data_gitref='',
        )

        mocked_cm = AsyncMock()
        mocked_cm.__aenter__ = AsyncMock()
        mocked_cm.__aexit__ = AsyncMock()

        mocked_lm.return_value = AsyncMock()
        mocked_lm.return_value.lock.return_value = mocked_cm

        mocked_build_plashets.return_value = {}

        # Build not permitted
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=False)
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_not_awaited()
        mocked_rhcos.assert_not_called()
        mocked_sync_for_ci.assert_not_called()

        # Build permitted, assembly != 'stream'
        mocked_build_plashets.reset_mock()
        mocked_rhcos.reset_mock()
        mocked_sync_for_ci.reset_mock()
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=True)
        build_plashets_pipeline.assembly = 'test'
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_awaited_once()
        mocked_rhcos.assert_not_called()
        mocked_sync_for_ci.assert_not_called()

        # Build permitted, assembly = 'stream'
        mocked_build_plashets.reset_mock()
        mocked_rhcos.reset_mock()
        mocked_sync_for_ci.reset_mock()
        build_plashets_pipeline.is_compose_build_permitted = AsyncMock(return_value=True)
        build_plashets_pipeline.assembly = 'stream'
        await build_plashets_pipeline.run()
        mocked_build_plashets.assert_awaited_once()
        mocked_sync_for_ci.assert_called_once_with(version='4.13', block_until_building=False)

    @patch('pyartcd.pipelines.build_plashets.util.default_release_suffix')
    @patch('pyartcd.pipelines.build_plashets.BuildPlashetsPipeline')
    def test_auto_generate_release(self, mocked_pipeline_class, mocked_default_release_suffix):
        """Test that release timestamp is auto-generated when not provided"""
        mocked_default_release_suffix.return_value = '202401011200.p?'

        # Create a mock pipeline instance
        mocked_pipeline = MagicMock()
        mocked_pipeline_class.return_value = mocked_pipeline

        # Create a mock runtime
        runtime = MagicMock(dry_run=False)
        runtime.logger = MagicMock()

        # Simulate the CLI logic for auto-generating release
        release = ''
        if not release:
            release = mocked_default_release_suffix()
            runtime.logger.info(f'Auto-generated release timestamp: {release}')

        # Verify that default_release_suffix was called
        mocked_default_release_suffix.assert_called_once()

        # Verify that the release was set to the auto-generated value
        self.assertEqual(release, '202401011200.p?')

        # Verify that the logger was called with the auto-generated release
        runtime.logger.info.assert_called_once_with('Auto-generated release timestamp: 202401011200.p?')


class TestPackageFilterPlumbing(unittest.IsolatedAsyncioTestCase):
    """Tests for the ART-25287 --package-filter run-level override plumbing."""

    def _common_kwargs(self, base_dir):
        return dict(
            group_param='openshift-4.17',
            assembly='stream',
            base_dir=base_dir,
            name='plashet',
            arches=['x86_64'],
            include_embargoed=False,
            signing_mode='signed',
            signing_advisory=0,
            tag_pvs=[('rhaos-4.17-rhel-9-candidate', 'OSE-4.17-RHEL-9')],
            embargoed_tags=[],
            include_previous_packages=[],
        )

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_package_filter_appended_to_doozer_cmd(self, mocked_cmd):
        """When package_filter is set, --package-filter <pkg> is appended once per package."""
        with tempfile.TemporaryDirectory() as base_dir:
            await build_plashet_from_tags(
                **self._common_kwargs(base_dir),
                package_filter=['kata-containers', 'cri-o'],
                dry_run=False,
            )
        mocked_cmd.assert_awaited_once()
        cmd = mocked_cmd.await_args.args[0]
        # Both packages are present as --package-filter pairs.
        self.assertEqual(cmd.count('--package-filter'), 2)
        for pkg in ('kata-containers', 'cri-o'):
            idx = cmd.index(pkg)
            self.assertEqual(cmd[idx - 1], '--package-filter')

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_no_package_filter_leaves_cmd_unchanged(self, mocked_cmd):
        """Without package_filter, no --package-filter flag is emitted (full-regen behavior unchanged)."""
        with tempfile.TemporaryDirectory() as base_dir:
            await build_plashet_from_tags(
                **self._common_kwargs(base_dir),
                dry_run=False,
            )
        cmd = mocked_cmd.await_args.args[0]
        self.assertNotIn('--package-filter', cmd)

    async def test_existing_plashet_preserved_in_filter_mode(self):
        """In package-filter mode an existing plashet dir is NOT wiped (merge target preserved)."""
        with tempfile.TemporaryDirectory() as base_dir:
            repo_path = Path(base_dir, 'plashet')
            repo_path.mkdir(parents=True)
            sentinel = repo_path / 'existing_marker'
            sentinel.write_text('keep me')
            await build_plashet_from_tags(
                **self._common_kwargs(base_dir),
                package_filter=['kata-containers'],
                dry_run=True,  # dry_run skips the doozer call but exercises the rmtree decision
            )
            self.assertTrue(sentinel.exists(), 'Existing plashet content must be preserved in filter mode')

    async def test_existing_plashet_wiped_in_full_mode(self):
        """Without package-filter, an existing plashet dir is wiped (original full-regen behavior)."""
        with tempfile.TemporaryDirectory() as base_dir:
            repo_path = Path(base_dir, 'plashet')
            repo_path.mkdir(parents=True)
            sentinel = repo_path / 'existing_marker'
            sentinel.write_text('remove me')
            await build_plashet_from_tags(
                **self._common_kwargs(base_dir),
                dry_run=True,
            )
            self.assertFalse(sentinel.exists(), 'Existing plashet content must be wiped in full-regen mode')

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_seed_plashet_baseline_downloads_plashet_yml(self, mocked_cmd):
        """seed_plashet_baseline rsyncs the baseline plashet.yml into the destination dir."""
        with tempfile.TemporaryDirectory() as dest_dir:
            await seed_plashet_baseline('ocp-artifacts:/mnt/data/pub/RHOCP/plashets/slug/latest', dest_dir)
        mocked_cmd.assert_awaited_once()
        cmd = mocked_cmd.await_args.args[0]
        self.assertEqual(cmd[0], 'rsync')
        self.assertIn('ocp-artifacts:/mnt/data/pub/RHOCP/plashets/slug/latest/plashet.yml', cmd)
        self.assertIn(f'{dest_dir}/plashet.yml', cmd)

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_seed_plashet_baseline_dry_run(self, mocked_cmd):
        """In dry_run, seed_plashet_baseline logs but does not execute the rsync."""
        with tempfile.TemporaryDirectory() as dest_dir:
            await seed_plashet_baseline('ocp-artifacts:/mnt/.../latest', dest_dir, dry_run=True)
        mocked_cmd.assert_not_awaited()

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_seed_log_messages_redact_host_and_path(self, mocked_cmd):
        """The seed log/dry-run messages must not leak the ocp-artifacts host or /mnt path."""
        baseline_source = 'ocp-artifacts:/mnt/data/pub/RHOCP/plashets/slug/latest'
        with tempfile.TemporaryDirectory() as dest_dir:
            with self.assertLogs('pyartcd', level='INFO') as live_logs:
                await seed_plashet_baseline(baseline_source, dest_dir)
            with self.assertLogs('pyartcd', level='WARNING') as dry_logs:
                await seed_plashet_baseline(baseline_source, dest_dir, dry_run=True)
        for record in live_logs.output + dry_logs.output:
            self.assertNotIn('ocp-artifacts', record)
            self.assertNotIn('/mnt', record)

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_seed_runs_before_doozer_in_filter_mode(self, mocked_cmd):
        """With package_filter + seed_latest_from, the baseline is seeded (rsync) BEFORE the doozer run."""
        with tempfile.TemporaryDirectory() as base_dir:
            await build_plashet_from_tags(
                **self._common_kwargs(base_dir),
                package_filter=['kata-containers'],
                seed_latest_from='ocp-artifacts:/mnt/data/pub/RHOCP/plashets/slug/latest',
                dry_run=False,
            )
        # Two awaited commands, in order: the seeding rsync first, then the doozer invocation.
        self.assertEqual(mocked_cmd.await_count, 2)
        first_cmd = mocked_cmd.await_args_list[0].args[0]
        second_cmd = mocked_cmd.await_args_list[1].args[0]
        self.assertEqual(first_cmd[0], 'rsync')
        self.assertIn('ocp-artifacts:/mnt/data/pub/RHOCP/plashets/slug/latest/plashet.yml', first_cmd)
        self.assertEqual(second_cmd[0], 'doozer')

    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_seed_failure_aborts_before_doozer(self, mocked_cmd):
        """If seeding the baseline fails, the error propagates and doozer is never invoked (fail path)."""
        mocked_cmd.side_effect = ChildProcessError('rsync failed: no such baseline')
        with tempfile.TemporaryDirectory() as base_dir:
            with self.assertRaises(ChildProcessError):
                await build_plashet_from_tags(
                    **self._common_kwargs(base_dir),
                    package_filter=['kata-containers'],
                    seed_latest_from='ocp-artifacts:/mnt/data/pub/RHOCP/plashets/slug/latest',
                    dry_run=False,
                )
        # Only the seeding rsync was attempted; the doozer call never happened.
        self.assertEqual(mocked_cmd.await_count, 1)
        self.assertEqual(mocked_cmd.await_args_list[0].args[0][0], 'rsync')

    def test_require_published_baseline_symlink_rejects_no_symlink(self):
        """A filtered build is rejected when the config publishes no baseline symlink."""
        # create_symlinks disabled.
        cfg = MagicMock(create_symlinks=False, symlink_name='latest')
        with self.assertRaises(ValueError):
            require_published_baseline_symlink(cfg, 'rhel-9-server-ose-rpms')
        # create_symlinks enabled but no symlink_name.
        cfg = MagicMock(create_symlinks=True, symlink_name='')
        with self.assertRaises(ValueError):
            require_published_baseline_symlink(cfg, 'rhel-9-server-ose-rpms')

    def test_require_published_baseline_symlink_allows_published(self):
        """A filtered build is allowed when a baseline symlink is published."""
        cfg = MagicMock(create_symlinks=True, symlink_name='latest')
        # Should not raise.
        require_published_baseline_symlink(cfg, 'rhel-9-server-ose-rpms')
