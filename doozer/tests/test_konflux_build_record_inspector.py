import logging
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from doozerlib.build_info import KonfluxBuildRecordInspector
from doozerlib.repodata import OutdatedRPMFinder


class TestKonfluxFindNonLatestRpms(IsolatedAsyncioTestCase):
    """Tests for KonfluxBuildRecordInspector.find_non_latest_rpms,
    specifically verifying that noarch RPMs are included when checking
    for outdated packages across all build architectures.
    """

    def setUp(self):
        self.runtime = MagicMock()
        self.logger = MagicMock(spec=logging.Logger)

        # Build record mock
        self.build_record = MagicMock(spec=KonfluxBuildRecord)
        self.build_record.nvr = 'test-image-v4.18.0-202501010000.p0'
        self.build_record.arches = ['x86_64', 'aarch64']
        self.build_record.image_pullspec = 'quay.io/test/image@sha256:abc123'
        self.build_record.name = 'test-image'
        self.build_record.build_id = '12345'
        self.build_record.embargoed = False
        self.build_record.version = 'v4.18.0'
        self.build_record.release = '202501010000.p0'
        self.build_record.el_target = 'el9'
        self.build_record.source_repo = 'https://github.com/openshift/test'
        self.build_record.commitish = 'abc123'
        self.build_record.build_pipeline_url = 'https://example.com/pipeline'

        # Image metadata mock
        self.image_meta = MagicMock()
        self.image_meta.logger = self.logger
        self.image_meta.distgit_key = 'test-image'
        self.image_meta.is_rpm_exempt.return_value = (False, None)
        self.image_meta.get_enabled_repos.return_value = ['rhel-9-baseos-rpms']

        self.runtime.image_map = {'test-image': self.image_meta}

        # Mock repos
        self.mock_repodata = MagicMock()
        self.mock_repo = MagicMock()
        self.mock_repo.get_repodata = AsyncMock(return_value=self.mock_repodata)
        self.runtime.repos = {'rhel-9-baseos-rpms': self.mock_repo}

        # Package RPM finder mock
        self.package_rpm_finder = MagicMock()

    def _make_rpm(self, name, epoch, version, release, arch):
        """Helper to create an RPM dict as returned by PackageRpmFinder."""
        return {
            'name': name,
            'epoch': epoch,
            'version': version,
            'release': release,
            'arch': arch,
        }

    async def test_noarch_rpms_included_in_each_build_arch(self):
        """Verify that noarch RPMs (e.g. ca-certificates, tzdata) are merged
        into each build arch's RPM list and passed to OutdatedRPMFinder.
        """
        rpms = [
            self._make_rpm('openssl-libs', 1, '3.0.7', '27.el9', 'x86_64'),
            self._make_rpm('openssl-libs', 1, '3.0.7', '27.el9', 'aarch64'),
            self._make_rpm('ca-certificates', 0, '2023.2.60_v7.0.306', '90.1.el9_2', 'noarch'),
            self._make_rpm('tzdata', 0, '2024a', '1.el9', 'noarch'),
        ]
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = rpms

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)

        with patch.object(OutdatedRPMFinder, 'find_non_latest_rpms', return_value=[]) as mock_find:
            await inspector.find_non_latest_rpms(self.package_rpm_finder)

            # Should be called once per build arch
            self.assertEqual(mock_find.call_count, 2)

            for call in mock_find.call_args_list:
                rpms_passed = call[0][0]
                rpm_names = [r['name'] for r in rpms_passed]
                # Each arch call must include the noarch RPMs
                self.assertIn('ca-certificates', rpm_names)
                self.assertIn('tzdata', rpm_names)

    async def test_noarch_only_rpms_still_checked(self):
        """If a build only has noarch RPMs installed, they should still be
        checked for each build architecture.
        """
        rpms = [
            self._make_rpm('ca-certificates', 0, '2023.2.60_v7.0.306', '90.1.el9_2', 'noarch'),
            self._make_rpm('filesystem', 0, '3.16', '2.el9', 'noarch'),
        ]
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = rpms

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)

        with patch.object(OutdatedRPMFinder, 'find_non_latest_rpms', return_value=[]) as mock_find:
            await inspector.find_non_latest_rpms(self.package_rpm_finder)

            # Should still be called for each build arch
            self.assertEqual(mock_find.call_count, 2)

            for call in mock_find.call_args_list:
                rpms_passed = call[0][0]
                rpm_names = [r['name'] for r in rpms_passed]
                self.assertIn('ca-certificates', rpm_names)
                self.assertIn('filesystem', rpm_names)

    async def test_arch_specific_rpms_not_mixed(self):
        """Verify that x86_64 RPMs are only in x86_64 calls and aarch64
        RPMs are only in aarch64 calls (noarch in both).
        """
        rpms = [
            self._make_rpm('openssl-libs', 1, '3.0.7', '27.el9', 'x86_64'),
            self._make_rpm('openssl-libs', 1, '3.0.7', '27.el9', 'aarch64'),
            self._make_rpm('ca-certificates', 0, '2023.2.60', '90.1.el9_2', 'noarch'),
        ]
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = rpms

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)

        calls_by_arch = {}

        def capture_find(rpms_list, repodatas, logger=None):
            # Identify which arch call this is based on the arch-specific RPMs
            arches_in_call = {r['arch'] for r in rpms_list if r['arch'] != 'noarch'}
            for a in arches_in_call:
                calls_by_arch[a] = [r['name'] for r in rpms_list]
            if not arches_in_call:
                # Only noarch RPMs
                calls_by_arch['noarch_only'] = [r['name'] for r in rpms_list]
            return []

        with patch.object(OutdatedRPMFinder, 'find_non_latest_rpms', side_effect=capture_find):
            await inspector.find_non_latest_rpms(self.package_rpm_finder)

        # x86_64 call should have x86_64 openssl-libs + ca-certificates noarch
        self.assertIn('x86_64', calls_by_arch)
        self.assertIn('openssl-libs', calls_by_arch['x86_64'])
        self.assertIn('ca-certificates', calls_by_arch['x86_64'])

        # aarch64 call should have aarch64 openssl-libs + ca-certificates noarch
        self.assertIn('aarch64', calls_by_arch)
        self.assertIn('openssl-libs', calls_by_arch['aarch64'])
        self.assertIn('ca-certificates', calls_by_arch['aarch64'])

    async def test_outdated_noarch_rpm_reported(self):
        """When a noarch RPM is outdated, it should appear in the results."""
        rpms = [
            self._make_rpm('openssl-libs', 1, '3.0.7', '27.el9', 'x86_64'),
            self._make_rpm('ca-certificates', 0, '2023.2.60', '90.1.el9_2', 'noarch'),
        ]
        self.build_record.arches = ['x86_64']
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = rpms

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)

        outdated = [
            (
                'ca-certificates-0:2023.2.60-90.1.el9_2.noarch',
                'ca-certificates-0:2024.2.69-100.1.el9_4.noarch',
                'rhel-9-baseos-rpms',
            ),
        ]

        with patch.object(OutdatedRPMFinder, 'find_non_latest_rpms', return_value=outdated):
            result = await inspector.find_non_latest_rpms(self.package_rpm_finder)

        self.assertIn('x86_64', result)
        self.assertEqual(len(result['x86_64']), 1)
        self.assertIn('ca-certificates', result['x86_64'][0][0])

    async def test_exempt_rpms_excluded_including_noarch(self):
        """RPMs exempted by scan-sources config should be excluded, including noarch ones."""
        rpms = [
            self._make_rpm('openssl-libs', 1, '3.0.7', '27.el9', 'x86_64'),
            self._make_rpm('ca-certificates', 0, '2023.2.60', '90.1.el9_2', 'noarch'),
        ]
        self.build_record.arches = ['x86_64']
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = rpms

        # Mark ca-certificates as exempt
        def mock_is_rpm_exempt(name):
            if name == 'ca-certificates':
                return (True, 'ca-*')
            return (False, None)

        self.image_meta.is_rpm_exempt.side_effect = mock_is_rpm_exempt

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)

        with patch.object(OutdatedRPMFinder, 'find_non_latest_rpms', return_value=[]) as mock_find:
            await inspector.find_non_latest_rpms(self.package_rpm_finder)

            # Only the non-exempt openssl-libs for x86_64 should remain
            rpms_passed = mock_find.call_args[0][0]
            rpm_names = [r['name'] for r in rpms_passed]
            self.assertIn('openssl-libs', rpm_names)
            self.assertNotIn('ca-certificates', rpm_names)

    async def test_no_enabled_repos_returns_empty(self):
        """When no repos are enabled, should return empty dict."""
        rpms = [
            self._make_rpm('ca-certificates', 0, '2023.2.60', '90.1.el9_2', 'noarch'),
        ]
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = rpms
        self.image_meta.get_enabled_repos.return_value = []

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)
        result = await inspector.find_non_latest_rpms(self.package_rpm_finder)

        self.assertEqual(result, {})

    async def test_no_installed_rpms_returns_empty(self):
        """When no RPMs are installed, should return empty dict."""
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = []

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)
        result = await inspector.find_non_latest_rpms(self.package_rpm_finder)

        self.assertEqual(result, {})

    async def test_four_arch_build_with_noarch(self):
        """Test with all four common build architectures to ensure noarch
        is merged into each one.
        """
        self.build_record.arches = ['x86_64', 'aarch64', 'ppc64le', 's390x']

        rpms = [
            self._make_rpm('glibc', 0, '2.34', '60.el9', 'x86_64'),
            self._make_rpm('glibc', 0, '2.34', '60.el9', 'aarch64'),
            self._make_rpm('glibc', 0, '2.34', '60.el9', 'ppc64le'),
            self._make_rpm('glibc', 0, '2.34', '60.el9', 's390x'),
            self._make_rpm('tzdata', 0, '2024a', '1.el9', 'noarch'),
        ]
        self.package_rpm_finder.get_brew_rpms_from_build_record.return_value = rpms

        inspector = KonfluxBuildRecordInspector(self.runtime, self.build_record)

        with patch.object(OutdatedRPMFinder, 'find_non_latest_rpms', return_value=[]) as mock_find:
            await inspector.find_non_latest_rpms(self.package_rpm_finder)

            # Called once per arch
            self.assertEqual(mock_find.call_count, 4)

            for call in mock_find.call_args_list:
                rpms_passed = call[0][0]
                rpm_names = [r['name'] for r in rpms_passed]
                # Every arch should have tzdata (noarch)
                self.assertIn('tzdata', rpm_names)
                # Every arch should have glibc (arch-specific)
                self.assertIn('glibc', rpm_names)
                # Each call should have exactly 2 RPMs
                self.assertEqual(len(rpms_passed), 2)
