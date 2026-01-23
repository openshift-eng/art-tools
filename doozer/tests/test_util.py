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


if __name__ == "__main__":
    unittest.main()
