import sys
import unittest
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.rebuild_golang_rpms import RebuildGolangRPMsPipeline

MODULE = 'pyartcd.pipelines.rebuild_golang_rpms'


class TestBumpRelease(TestCase):
    def setUp(self):
        if "specfile" not in sys.modules:
            self.skipTest("specfile is only available on Linux")
        runtime = MagicMock()
        self.pipeline = RebuildGolangRPMsPipeline(
            runtime, ocp_version="4.17", go_nvrs=["go1.16"], art_jira="JIRA-123", cves=None
        )

    def test_bump_up_case_1(self):
        fake_releases = "42.rhaos4.17.abcd"
        actual_r = self.pipeline.bump_release(fake_releases)
        expected_r = "43.rhaos4.17.abcd"
        self.assertEqual(actual_r, expected_r)

    def test_bump_up_case_2(self):
        fake_release = "42.1.rhaos4.17.abcd"
        actual_r = self.pipeline.bump_release(fake_release)
        expected_r = "43.rhaos4.17.abcd"
        self.assertEqual(actual_r, expected_r)

    def test_bump_up_case_3(self):
        fake_releases = "rhaos4.17.abcd"
        actual_r = self.pipeline.bump_release(fake_releases)
        expected_r = "1.rhaos4.17.abcd"
        self.assertEqual(actual_r, expected_r)

    def test_bump_up_case_4(self):
        fake_releases = "42.1"
        actual_r = self.pipeline.bump_release(fake_releases)
        expected_r = "43"
        self.assertEqual(actual_r, expected_r)


class TestBumpAndRebuildRpm(IsolatedAsyncioTestCase):
    def setUp(self):
        if "specfile" not in sys.modules:
            self.skipTest("specfile is only available on Linux")
        self.runtime = MagicMock()
        self.runtime.dry_run = False

    def _make_pipeline(self, force=False):
        return RebuildGolangRPMsPipeline(
            self.runtime,
            ocp_version="4.17",
            go_nvrs=["golang-1.22.0-1.el9"],
            art_jira="ART-1234",
            cves=None,
            force=force,
        )

    @patch(f'{MODULE}.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.os.path.isdir', return_value=True)
    @patch(f'{MODULE}.os.listdir', return_value=['foo.spec'])
    @patch(f'{MODULE}.Specfile')
    async def test_bump_commit_exists_skips_bump(
        self, mock_specfile, mock_listdir, mock_isdir, mock_gather, mock_assert
    ):
        """When a bump commit already exists, skip the bump and just trigger the build."""
        pipeline = self._make_pipeline(force=False)
        bump_msg = 'Bump and rebuild with latest golang, resolves ART-1234'
        mock_gather.return_value = (0, bump_msg, '')

        result = await pipeline.bump_and_rebuild_rpm('foo-rpm', '9', 'Test Author', 'test@redhat.com')

        self.assertTrue(result)
        mock_specfile.assert_not_called()
        build_calls = [c for c in mock_assert.call_args_list if 'rhpkg build' in str(c)]
        self.assertEqual(len(build_calls), 1)

    @patch(f'{MODULE}.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.os.path.isdir', return_value=True)
    @patch(f'{MODULE}.os.listdir', return_value=['foo.spec'])
    @patch(f'{MODULE}.Specfile')
    async def test_no_bump_commit_does_bump_and_build(
        self, mock_specfile, mock_listdir, mock_isdir, mock_gather, mock_assert
    ):
        """When no bump commit exists, bump the spec and trigger a build."""
        pipeline = self._make_pipeline(force=False)
        mock_gather.return_value = (0, 'some other commit message', '')

        mock_spec_instance = MagicMock()
        mock_spec_instance.release = '42.rhaos4.17.el9'
        mock_specfile.return_value = mock_spec_instance

        result = await pipeline.bump_and_rebuild_rpm('foo-rpm', '9', 'Test Author', 'test@redhat.com')

        self.assertTrue(result)
        mock_specfile.assert_called_once()
        mock_spec_instance.save.assert_called_once()
        commit_calls = [c for c in mock_assert.call_args_list if 'git commit' in str(c)]
        self.assertEqual(len(commit_calls), 1)
        build_calls = [c for c in mock_assert.call_args_list if 'rhpkg build' in str(c)]
        self.assertEqual(len(build_calls), 1)

    @patch(f'{MODULE}.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.os.path.isdir', return_value=True)
    @patch(f'{MODULE}.os.listdir', return_value=['foo.spec'])
    @patch(f'{MODULE}.Specfile')
    async def test_bump_commit_exists_with_force_skips_bump(
        self, mock_specfile, mock_listdir, mock_isdir, mock_gather, mock_assert
    ):
        """When a bump commit exists and --force is set, still skip the bump and build."""
        pipeline = self._make_pipeline(force=True)
        bump_msg = 'Bump and rebuild with latest golang, resolves ART-1234'
        mock_gather.return_value = (0, bump_msg, '')

        result = await pipeline.bump_and_rebuild_rpm('foo-rpm', '9', 'Test Author', 'test@redhat.com')

        self.assertTrue(result)
        mock_specfile.assert_not_called()
        build_calls = [c for c in mock_assert.call_args_list if 'rhpkg build' in str(c)]
        self.assertEqual(len(build_calls), 1)

    @patch(f'{MODULE}.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.os.path.isdir', return_value=True)
    async def test_git_log_failure_raises(self, mock_isdir, mock_gather, mock_assert):
        """When git log fails, raise a ValueError."""
        pipeline = self._make_pipeline(force=False)
        mock_gather.return_value = (1, '', 'error')

        with self.assertRaises(ValueError, msg='Cannot get last commit message'):
            await pipeline.bump_and_rebuild_rpm('foo-rpm', '9', 'Test Author', 'test@redhat.com')

    @patch(f'{MODULE}.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.os.path.isdir', return_value=True)
    @patch(f'{MODULE}.os.listdir', return_value=['foo.spec'])
    @patch(f'{MODULE}.Specfile')
    async def test_dry_run_skips_save_and_build(
        self, mock_specfile, mock_listdir, mock_isdir, mock_gather, mock_assert
    ):
        """In dry run mode, don't save the spec, push, or build."""
        self.runtime.dry_run = True
        pipeline = self._make_pipeline(force=False)
        mock_gather.return_value = (0, 'some other commit', '')

        mock_spec_instance = MagicMock()
        mock_spec_instance.release = '42.rhaos4.17.el9'
        mock_specfile.return_value = mock_spec_instance

        result = await pipeline.bump_and_rebuild_rpm('foo-rpm', '9', 'Test Author', 'test@redhat.com')

        self.assertTrue(result)
        mock_spec_instance.save.assert_not_called()
        commit_calls = [c for c in mock_assert.call_args_list if 'git commit' in str(c)]
        self.assertEqual(len(commit_calls), 0)
        build_calls = [c for c in mock_assert.call_args_list if 'rhpkg build' in str(c)]
        self.assertEqual(len(build_calls), 0)


class TestRunForceGolangVersionCheck(IsolatedAsyncioTestCase):
    def setUp(self):
        if "specfile" not in sys.modules:
            self.skipTest("specfile is only available on Linux")
        self.runtime = MagicMock()
        self.runtime.dry_run = False

    def _make_pipeline(self, force=False):
        p = RebuildGolangRPMsPipeline(
            self.runtime,
            ocp_version="4.17",
            go_nvrs=["golang-1.22.0-1.el9"],
            art_jira="ART-1234",
            cves=None,
            force=force,
            rpms=['foo-rpm'],
            all=False,
        )
        p.koji_session = MagicMock()
        return p

    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.move_golang_bugs', new_callable=AsyncMock)
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.bump_and_rebuild_rpm', new_callable=AsyncMock)
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.get_rpms')
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.get_art_built_rpms')
    @patch(f'{MODULE}.is_latest_and_available', new_callable=AsyncMock, return_value=True)
    @patch(f'{MODULE}.extract_and_validate_golang_nvrs')
    @patch(f'{MODULE}.elliottutil.get_golang_rpm_nvrs')
    async def test_force_rebuilds_rpms_on_latest_golang(
        self,
        mock_go_nvr_map,
        mock_extract,
        mock_available,
        mock_art_rpms,
        mock_get_rpms,
        mock_bump_rebuild,
        mock_move_bugs,
        mock_cmd_gather,
    ):
        """With --force, RPMs already on the latest golang version should still be rebuilt."""
        mock_extract.return_value = ('1.22.0', {'9': 'golang-1.22.0-1.el9'})
        mock_art_rpms.return_value = []
        mock_get_rpms.return_value = ['foo-rpm-1.0-1.el9']
        mock_go_nvr_map.return_value = {
            '1.22.0': [('foo-rpm', '1.0', '1.el9')],
        }
        mock_cmd_gather.side_effect = [
            (0, 'ART Bot', ''),
            (0, 'aos-team-art@redhat.com', ''),
        ]
        mock_bump_rebuild.return_value = True

        pipeline = self._make_pipeline(force=True)
        await pipeline.run()

        mock_bump_rebuild.assert_called_once_with('foo-rpm', '9', unittest.mock.ANY, unittest.mock.ANY)

    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.move_golang_bugs', new_callable=AsyncMock)
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.bump_and_rebuild_rpm', new_callable=AsyncMock)
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.get_rpms')
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.get_art_built_rpms')
    @patch(f'{MODULE}.is_latest_and_available', new_callable=AsyncMock, return_value=True)
    @patch(f'{MODULE}.extract_and_validate_golang_nvrs')
    @patch(f'{MODULE}.elliottutil.get_golang_rpm_nvrs')
    async def test_no_force_skips_rpms_on_latest_golang(
        self,
        mock_go_nvr_map,
        mock_extract,
        mock_available,
        mock_art_rpms,
        mock_get_rpms,
        mock_bump_rebuild,
        mock_move_bugs,
        mock_cmd_gather,
    ):
        """Without --force, RPMs already on the latest golang version should be skipped."""
        mock_extract.return_value = ('1.22.0', {'9': 'golang-1.22.0-1.el9'})
        mock_art_rpms.return_value = []
        mock_get_rpms.return_value = ['foo-rpm-1.0-1.el9']
        mock_go_nvr_map.return_value = {
            '1.22.0': [('foo-rpm', '1.0', '1.el9')],
        }

        pipeline = self._make_pipeline(force=False)
        await pipeline.run()

        mock_bump_rebuild.assert_not_called()

    @patch(f'{MODULE}.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch(f'{MODULE}.move_golang_bugs', new_callable=AsyncMock)
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.bump_and_rebuild_rpm', new_callable=AsyncMock)
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.get_rpms')
    @patch(f'{MODULE}.RebuildGolangRPMsPipeline.get_art_built_rpms')
    @patch(f'{MODULE}.is_latest_and_available', new_callable=AsyncMock, return_value=True)
    @patch(f'{MODULE}.extract_and_validate_golang_nvrs')
    @patch(f'{MODULE}.elliottutil.get_golang_rpm_nvrs')
    async def test_no_force_rebuilds_rpms_on_old_golang(
        self,
        mock_go_nvr_map,
        mock_extract,
        mock_available,
        mock_art_rpms,
        mock_get_rpms,
        mock_bump_rebuild,
        mock_move_bugs,
        mock_cmd_gather,
    ):
        """Without --force, RPMs on an older golang version should be rebuilt."""
        mock_extract.return_value = ('1.22.0', {'9': 'golang-1.22.0-1.el9'})
        mock_art_rpms.return_value = []
        mock_get_rpms.return_value = ['foo-rpm-1.0-1.el9']
        mock_go_nvr_map.return_value = {
            '1.21.0': [('foo-rpm', '1.0', '1.el9')],
        }
        mock_cmd_gather.side_effect = [
            (0, 'ART Bot', ''),
            (0, 'aos-team-art@redhat.com', ''),
        ]
        mock_bump_rebuild.return_value = True

        pipeline = self._make_pipeline(force=False)
        await pipeline.run()

        mock_bump_rebuild.assert_called_once_with('foo-rpm', '9', unittest.mock.ANY, unittest.mock.ANY)


if __name__ == '__main__':
    unittest.main()
