import logging
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch
from artcommonlib.model import Model
from doozerlib import util


class TestUtil(unittest.TestCase):
    def test_isolate_nightly_name_components(self):
        self.assertEqual(
            util.isolate_nightly_name_components('4.1.0-0.nightly-2019-11-08-213727'), ('4.1', 'x86_64', False)
        )
        self.assertEqual(
            util.isolate_nightly_name_components('4.1.0-0.nightly-priv-2019-11-08-213727'), ('4.1', 'x86_64', True)
        )
        self.assertEqual(
            util.isolate_nightly_name_components('4.1.0-0.nightly-s390x-2019-11-08-213727'), ('4.1', 's390x', False)
        )
        self.assertEqual(
            util.isolate_nightly_name_components('4.9.0-0.nightly-arm64-priv-2021-06-08-213727'),
            ('4.9', 'aarch64', True),
        )

    def test_extract_version_fields(self):
        self.assertEqual(util.extract_version_fields('1.2.3'), [1, 2, 3])
        self.assertEqual(util.extract_version_fields('1.2'), [1, 2])
        self.assertEqual(util.extract_version_fields('v1.2.3'), [1, 2, 3])
        self.assertEqual(util.extract_version_fields('v1.2'), [1, 2])
        self.assertRaises(IOError, util.extract_version_fields, 'v1.2', 3)
        self.assertRaises(IOError, util.extract_version_fields, '1.2', 3)

    def test_bogus_arch_xlate(self):
        with self.assertRaises(Exception):
            go_arch_for_brew_arch("bogus")
        with self.assertRaises(Exception):
            brew_arch_for_go_arch("bogus")

    def test_get_release_name_for_assembly(self):
        releases_config = Model(
            {
                "releases": {
                    "4.12.99": {
                        "assembly": {
                            "type": "standard",
                            "basis": {
                                "assembly": "4.12.98",
                            },
                        },
                    },
                    "4.12.98": {
                        "assembly": {
                            "type": "standard",
                            "basis": {
                                "event": 12345,
                            },
                        },
                    },
                    "art0000": {
                        "assembly": {
                            "type": "custom",
                            "basis": {
                                "assembly": "4.12.99",
                            },
                        },
                    },
                    "art0001": {
                        "assembly": {
                            "type": "custom",
                            "basis": {
                                "assembly": "art0000",
                            },
                        },
                    },
                    "art0002": {
                        "assembly": {
                            "type": "custom",
                            "basis": {
                                "assembly": "art0001",
                                "patch_version": 23,
                            },
                        },
                    },
                    "art0003": {
                        "assembly": {
                            "type": "custom",
                            "basis": {
                                "assembly": "art0002",
                            },
                        },
                    },
                },
            }
        )

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "4.12.99")
        expected = "4.12.99"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "4.12.98")
        expected = "4.12.98"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0000")
        expected = "4.12.99-assembly.art0000"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0001")
        expected = "4.12.99-assembly.art0001"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0002")
        expected = "4.12.23-assembly.art0002"
        self.assertEqual(actual, expected)

        actual = util.get_release_name_for_assembly("openshift-4.12", releases_config, "art0003")
        expected = "4.12.23-assembly.art0003"
        self.assertEqual(actual, expected)

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_show_multiarch(self, gather_mock):
        gather_mock.return_value = (0, '{}', '')
        util.oc_image_info_show_multiarch('pullspec')
        gather_mock.assert_called_with(['oc', 'image', 'info', '-o', 'json', 'pullspec', '--show-multiarch'])

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_show_multiarch_caching(self, gather_mock):
        gather_mock.return_value = (0, '{}', '')
        util.oc_image_info_show_multiarch__caching('pullspec')
        gather_mock.assert_called_with(['oc', 'image', 'info', '-o', 'json', 'pullspec', '--show-multiarch'])

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_for_arch(self, gather_mock):
        gather_mock.return_value = (0, '{}', '')
        util.oc_image_info_for_arch('pullspec')
        gather_mock.assert_called_with(['oc', 'image', 'info', '-o', 'json', 'pullspec', '--filter-by-os=amd64'])

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_for_arch_with_custom_arch(self, gather_mock):
        gather_mock.return_value = (0, '{}', '')
        util.oc_image_info_for_arch('pullspec', go_arch='arm64')
        gather_mock.assert_called_with(['oc', 'image', 'info', '-o', 'json', 'pullspec', '--filter-by-os=arm64'])

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_for_arch_with_registry_config(self, gather_mock):
        gather_mock.return_value = (0, '{}', '')
        util.oc_image_info_for_arch('pullspec', registry_config='/path/to/config.json')
        gather_mock.assert_called_with(
            [
                'oc',
                'image',
                'info',
                '-o',
                'json',
                'pullspec',
                '--filter-by-os=amd64',
                '--registry-config=/path/to/config.json',
            ]
        )

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_for_arch_returns_dict(self, gather_mock):
        gather_mock.return_value = (0, '{"name": "test"}', '')
        result = util.oc_image_info_for_arch('pullspec')
        self.assertIsInstance(result, dict)
        self.assertEqual(result['name'], 'test')

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_for_arch_raises_on_list(self, gather_mock):
        # This should not happen with --filter-by-os, but test the assertion
        gather_mock.return_value = (0, '[{"name": "test"}]', '')
        with self.assertRaises(AssertionError):
            util.oc_image_info_for_arch('pullspec')

    @patch("artcommonlib.exectools.cmd_gather")
    def test_oc_image_info_for_arch_caching(self, gather_mock):
        gather_mock.return_value = (0, '{}', '')
        util.oc_image_info_for_arch__caching('pullspec')
        gather_mock.assert_called_with(['oc', 'image', 'info', '-o', 'json', 'pullspec', '--filter-by-os=amd64'])

    @patch("doozerlib.util.oc_image_info")
    def test_oc_image_info_for_arch_strict_false_manifest_unknown(self, oc_image_info_mock):
        # When strict=False and manifest unknown, should return None
        oc_image_info_mock.return_value = None
        result = util.oc_image_info_for_arch('pullspec', strict=False)
        self.assertIsNone(result)
        oc_image_info_mock.assert_called_once_with(
            'pullspec', '--filter-by-os=amd64', registry_config=None, strict=False
        )

    @patch("doozerlib.util.oc_image_info")
    def test_oc_image_info_for_arch_strict_false_other_error(self, oc_image_info_mock):
        # When strict=False but other error, should raise
        oc_image_info_mock.side_effect = IOError("oc image info failed (rc=1): error: network timeout")
        with self.assertRaises(IOError):
            util.oc_image_info_for_arch('pullspec', strict=False)

    @patch("doozerlib.util.oc_image_info")
    def test_oc_image_info_for_arch_strict_true_manifest_unknown(self, oc_image_info_mock):
        # When strict=True and manifest unknown, should raise
        oc_image_info_mock.side_effect = IOError(
            "oc image info failed (rc=1): error: manifest unknown: manifest unknown"
        )
        with self.assertRaises(IOError):
            util.oc_image_info_for_arch('pullspec', strict=True)

    def test_get_cincinnati_channels_ocp_4_1(self):
        # OCP 4.1 uses special channel names (prerelease, stable)
        channels = util.get_cincinnati_channels(4, 1)
        self.assertEqual(channels, ['prerelease-4.1', 'stable-4.1'])

    def test_get_cincinnati_channels_ocp_4_x(self):
        # OCP 4.2+ uses standard channel names
        channels = util.get_cincinnati_channels(4, 16)
        self.assertEqual(channels, ['candidate-4.16', 'fast-4.16', 'stable-4.16'])

        channels = util.get_cincinnati_channels(4, 22)
        self.assertEqual(channels, ['candidate-4.22', 'fast-4.22', 'stable-4.22'])

    def test_get_cincinnati_channels_ocp_5_x(self):
        # OCP 5.x uses standard channel names (same as 4.2+)
        channels = util.get_cincinnati_channels(5, 0)
        self.assertEqual(channels, ['candidate-5.0', 'fast-5.0', 'stable-5.0'])

        channels = util.get_cincinnati_channels(5, 5)
        self.assertEqual(channels, ['candidate-5.5', 'fast-5.5', 'stable-5.5'])

    def test_get_cincinnati_channels_string_versions(self):
        # Function should handle string inputs (converts to int)
        channels = util.get_cincinnati_channels('5', '0')
        self.assertEqual(channels, ['candidate-5.0', 'fast-5.0', 'stable-5.0'])

    def test_get_cincinnati_channels_rejects_ocp_3(self):
        # Cincinnati channels don't exist for OCP 3.x and earlier
        with self.assertRaises(ValueError) as ctx:
            util.get_cincinnati_channels(3, 11)
        self.assertIn('Cincinnati channels are only available for OCP 4.x and later', str(ctx.exception))
        self.assertIn('3.11', str(ctx.exception))


class TestIsPackageMain(unittest.TestCase):
    """Tests for ``_is_package_main``."""

    def _write(self, dir_path: pathlib.Path, name: str, content: str) -> pathlib.Path:
        p = dir_path / name
        p.write_text(content, encoding='utf-8')
        return p

    def test_simple_package_main(self):
        with tempfile.TemporaryDirectory() as td:
            f = self._write(pathlib.Path(td), 'main.go', 'package main\n\nfunc main() {}\n')
            self.assertTrue(util._is_package_main(f))

    def test_package_not_main(self):
        with tempfile.TemporaryDirectory() as td:
            f = self._write(pathlib.Path(td), 'lib.go', 'package mylib\n')
            self.assertFalse(util._is_package_main(f))

    def test_go_build_ignore(self):
        with tempfile.TemporaryDirectory() as td:
            content = '//go:build ignore\n\npackage main\n\nfunc main() {}\n'
            f = self._write(pathlib.Path(td), 'ignored.go', content)
            self.assertFalse(util._is_package_main(f))

    def test_plus_build_ignore(self):
        with tempfile.TemporaryDirectory() as td:
            content = '// +build ignore\n\npackage main\n\nfunc main() {}\n'
            f = self._write(pathlib.Path(td), 'ignored.go', content)
            self.assertFalse(util._is_package_main(f))

    def test_build_constraint_not_ignore(self):
        """A non-ignore build constraint should NOT prevent detection."""
        with tempfile.TemporaryDirectory() as td:
            content = '//go:build linux\n\npackage main\n\nfunc main() {}\n'
            f = self._write(pathlib.Path(td), 'linux_main.go', content)
            self.assertTrue(util._is_package_main(f))

    def test_nonexistent_file(self):
        self.assertFalse(util._is_package_main(pathlib.Path('/nonexistent/file.go')))


class TestGetEffectivePackage(unittest.TestCase):
    """Tests for ``_get_effective_package``."""

    def _write(self, dir_path: pathlib.Path, name: str, content: str) -> pathlib.Path:
        p = dir_path / name
        p.write_text(content, encoding='utf-8')
        return p

    def test_returns_package_name(self):
        with tempfile.TemporaryDirectory() as td:
            f = self._write(pathlib.Path(td), 'lib.go', 'package plugins\n')
            self.assertEqual(util._get_effective_package(f), 'plugins')

    def test_returns_main(self):
        with tempfile.TemporaryDirectory() as td:
            f = self._write(pathlib.Path(td), 'main.go', 'package main\n')
            self.assertEqual(util._get_effective_package(f), 'main')

    def test_returns_none_for_build_ignore(self):
        with tempfile.TemporaryDirectory() as td:
            content = '//go:build ignore\n\npackage main\n'
            f = self._write(pathlib.Path(td), 'ignored.go', content)
            self.assertIsNone(util._get_effective_package(f))

    def test_returns_none_for_unreadable(self):
        self.assertIsNone(util._get_effective_package(pathlib.Path('/nonexistent/file.go')))


class TestFindGoMainPackages(unittest.TestCase):
    """Tests for ``find_go_main_packages``."""

    def _write(self, dir_path: pathlib.Path, name: str, content: str) -> pathlib.Path:
        dir_path.mkdir(parents=True, exist_ok=True)
        p = dir_path / name
        p.write_text(content, encoding='utf-8')
        return p

    def test_finds_simple_main_package(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            cmd_dir = root / 'cmd' / 'myapp'
            self._write(cmd_dir, 'main.go', 'package main\n\nfunc main() {}\n')
            result = util.find_go_main_packages(root)
            self.assertEqual(result, [cmd_dir])

    def test_skips_vendor_dirs(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            self._write(root / 'vendor' / 'pkg', 'main.go', 'package main\n')
            result = util.find_go_main_packages(root)
            self.assertEqual(result, [])

    def test_skips_test_files(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            # Directory with only a test file declaring package main
            self._write(root / 'pkg', 'main_test.go', 'package main\n')
            result = util.find_go_main_packages(root)
            self.assertEqual(result, [])

    def test_skips_build_ignored_main(self):
        """A directory with only ``//go:build ignore`` main files should be skipped."""
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            plugins_dir = root / 'plugins'
            self._write(plugins_dir, 'example.go', '//go:build ignore\n\npackage main\n\nfunc main() {}\n')
            self._write(plugins_dir, 'minimum.go', 'package plugins\n')
            result = util.find_go_main_packages(root)
            self.assertEqual(result, [])

    def test_skips_mixed_package_directory(self):
        """A directory with both ``package main`` and another package (e.g.
        due to a build-constrained file that wasn't caught by the ignore
        check) should be skipped.
        """
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            mixed_dir = root / 'mixed'
            self._write(mixed_dir, 'main.go', 'package main\n\nfunc main() {}\n')
            self._write(mixed_dir, 'lib.go', 'package mixed\n')
            result = util.find_go_main_packages(root)
            self.assertEqual(result, [])

    def test_prometheus_plugins_scenario(self):
        """Reproduce the exact prometheus/plugins failure scenario:
        plugins/minimum.go is ``package plugins`` and there is a
        ``//go:build ignore`` file with ``package main``.
        """
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            plugins_dir = root / 'plugins'
            self._write(plugins_dir, 'minimum.go', 'package plugins\n\nvar _ = 1\n')
            self._write(
                plugins_dir,
                'example_plugin.go',
                '// +build ignore\n\npackage main\n\nimport _ "github.com/prometheus/prometheus/plugins"\n\nfunc main() {}\n',
            )
            # Also add a real main package elsewhere
            cmd_dir = root / 'cmd' / 'prometheus'
            self._write(cmd_dir, 'main.go', 'package main\n\nimport _ "github.com/prometheus/prometheus/plugins"\n\nfunc main() {}\n')

            result = util.find_go_main_packages(root)
            # plugins/ should NOT be in the result; cmd/prometheus/ should be
            self.assertIn(cmd_dir, result)
            self.assertNotIn(plugins_dir, result)

    def test_multiple_main_packages(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            cmd1 = root / 'cmd' / 'app1'
            cmd2 = root / 'cmd' / 'app2'
            self._write(cmd1, 'main.go', 'package main\n\nfunc main() {}\n')
            self._write(cmd2, 'main.go', 'package main\n\nfunc main() {}\n')
            result = util.find_go_main_packages(root)
            self.assertEqual(result, sorted([cmd1, cmd2]))


class TestInjectCoverageServer(unittest.TestCase):
    """Tests for ``inject_coverage_server``."""

    def _write(self, dir_path: pathlib.Path, name: str, content: str) -> pathlib.Path:
        dir_path.mkdir(parents=True, exist_ok=True)
        p = dir_path / name
        p.write_text(content, encoding='utf-8')
        return p

    def test_injects_into_main_packages(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            cmd_dir = root / 'cmd' / 'myapp'
            self._write(cmd_dir, 'main.go', 'package main\n\nfunc main() {}\n')

            logger = logging.getLogger('test')
            util.inject_coverage_server(root, logger)

            self.assertTrue((cmd_dir / 'coverage_server.go').exists())

    def test_does_not_inject_into_mixed_package(self):
        """Ensure coverage_server.go is NOT injected into directories that
        have conflicting package names (the prometheus/plugins scenario).
        """
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            plugins_dir = root / 'plugins'
            self._write(plugins_dir, 'minimum.go', 'package plugins\n')
            self._write(plugins_dir, 'example.go', '//go:build ignore\n\npackage main\n\nfunc main() {}\n')

            logger = logging.getLogger('test')
            util.inject_coverage_server(root, logger)

            self.assertFalse((plugins_dir / 'coverage_server.go').exists())


if __name__ == "__main__":
    unittest.main()
