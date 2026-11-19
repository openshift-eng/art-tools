#!/usr/bin/env python
import logging
import unittest
from types import SimpleNamespace
from unittest import mock

from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from doozerlib import runtime


def stub_runtime():
    rt = runtime.Runtime(
        latest_parent_version=False,
        stage=False,
        branch='test-branch',
        rhpkg_config="",
    )
    rt._logger = logging.getLogger(__name__)
    rt.group_config = Model()
    return rt


class TestRuntime(unittest.TestCase):
    @staticmethod
    def _entry(key, data):
        return SimpleNamespace(key=key, data=Model(dict_to_model=data))

    def test_okd_group_config_overrides_product(self):
        group_config = {"product": "ocp", "okd": {"product": "okd"}}
        for variant, expected in ((BuildVariant.OCP, "ocp"), (BuildVariant.OKD, "okd")):
            with self.subTest(variant=variant):
                rt = stub_runtime()
                rt.variant = variant
                rt.get_replace_vars = mock.Mock(return_value={})
                rt.get_releases_config = mock.Mock(return_value={})
                rt._build_data_loader = mock.Mock()
                rt._build_data_loader.load_group_config.return_value = group_config

                self.assertEqual(rt.get_group_config().product, expected)

    def test_populate_image_name_maps_skips_non_olm_duplicates(self):
        rt = stub_runtime()

        rt._populate_image_name_maps(
            {
                "openshift-golang-builder": self._entry(
                    "openshift-golang-builder",
                    {"name": "openshift/golang-builder"},
                ),
                "openshift-golang-builder-94": self._entry(
                    "openshift-golang-builder-94",
                    {"name": "openshift/golang-builder"},
                ),
            }
        )

        self.assertEqual({}, rt.name_in_bundle_map)

    def test_populate_image_name_maps_registers_olm_related_images(self):
        rt = stub_runtime()

        rt._populate_image_name_maps(
            {
                "my-operator": self._entry(
                    "my-operator",
                    {
                        "name": "openshift/ose-my-operator",
                        "update-csv": {"manifests-dir": "manifests"},
                    },
                ),
                "my-operand": self._entry(
                    "my-operand",
                    {
                        "name": "openshift/ose-my-operand",
                        "dependents": ["my-operator"],
                    },
                ),
            }
        )

        self.assertEqual("my-operator", rt.name_in_bundle_map["my-operator"])
        self.assertEqual("my-operator", rt.name_in_bundle_map["ose-my-operator"])
        self.assertEqual("my-operand", rt.name_in_bundle_map["my-operand"])
        self.assertEqual("my-operand", rt.name_in_bundle_map["ose-my-operand"])

    def test_populate_image_name_maps_keeps_explicit_name_in_bundle(self):
        rt = stub_runtime()

        rt._populate_image_name_maps(
            {
                "custom-image": self._entry(
                    "custom-image",
                    {
                        "name": "openshift/custom-image",
                        "name_in_bundle": "explicit-bundle-name",
                    },
                ),
            }
        )

        self.assertEqual("custom-image", rt.name_in_bundle_map["explicit-bundle-name"])


if __name__ == "__main__":
    unittest.main()
