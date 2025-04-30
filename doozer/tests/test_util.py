import unittest
from unittest.mock import patch

from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch
from artcommonlib.model import Model
from doozerlib import util


class TestUtil(unittest.TestCase):
    def test_isolate_nightly_name_components(self):
        self.assertEqual(util.isolate_nightly_name_components('4.1.0-0.nightly-2019-11-08-213727'), ('4.1', 'x86_64', False))
        self.assertEqual(util.isolate_nightly_name_components('4.1.0-0.nightly-priv-2019-11-08-213727'), ('4.1', 'x86_64', True))
        self.assertEqual(util.isolate_nightly_name_components('4.1.0-0.nightly-s390x-2019-11-08-213727'), ('4.1', 's390x', False))
        self.assertEqual(util.isolate_nightly_name_components('4.9.0-0.nightly-arm64-priv-2021-06-08-213727'), ('4.9', 'aarch64', True))

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
            },
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

    @patch("artcommonlib.exectools.cmd_assert")
    def test_oc_image_info_show_multiarch(self, assert_mock):
        assert_mock.return_value = ['{}', 0]
        util.oc_image_info_show_multiarch('pullspec')
        assert_mock.assert_called_once_with(
            ['oc', 'image', 'info', '-o', 'json', 'pullspec', '--show-multiarch'],
            retries=3,
        )

    @patch("artcommonlib.exectools.cmd_assert")
    def test_oc_image_info_show_multiarch_caching(self, assert_mock):
        assert_mock.return_value = ['{}', 0]
        util.oc_image_info_show_multiarch__caching('pullspec')
        assert_mock.assert_called_once_with(
            ['oc', 'image', 'info', '-o', 'json', 'pullspec', '--show-multiarch'],
            retries=3,
        )

    @patch("artcommonlib.exectools.cmd_assert")
    def test_oc_image_info_for_arch(self, assert_mock):
        assert_mock.return_value = ['{}', 0]
        util.oc_image_info_for_arch('pullspec')
        assert_mock.assert_called_once_with(
            ['oc', 'image', 'info', '-o', 'json', 'pullspec', '--filter-by-os=amd64'],
            retries=3,
        )

    @patch("artcommonlib.exectools.cmd_assert")
    def test_oc_image_info_for_arch_caching(self, assert_mock):
        assert_mock.return_value = ['{}', 0]
        util.oc_image_info_for_arch__caching('pullspec')
        assert_mock.assert_called_once_with(
            ['oc', 'image', 'info', '-o', 'json', 'pullspec', '--filter-by-os=amd64'],
            retries=3,
        )


if __name__ == "__main__":
    unittest.main()
