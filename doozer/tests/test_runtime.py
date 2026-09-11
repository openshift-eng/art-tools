#!/usr/bin/env python
import logging
import unittest
from types import SimpleNamespace

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.model import Model
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


class TestGetReplaceVars(unittest.TestCase):
    """Tests for Runtime.get_replace_vars() runtime_assembly precedence."""

    def _make_runtime(self, assembly=None, extra_vars=None, assembly_type=AssemblyTypes.STREAM):
        rt = stub_runtime()
        rt.assembly = assembly
        rt.assembly_type = assembly_type
        rt.extra_vars = extra_vars
        return rt

    def test_default_no_assembly(self):
        """Without an assembly, runtime_assembly defaults to ''."""
        rt = self._make_runtime(assembly=None)
        result = rt.get_replace_vars(None)
        self.assertEqual(result['runtime_assembly'], '')

    def test_default_with_assembly(self):
        """With an assembly and no group_config override, runtime_assembly equals the assembly name."""
        rt = self._make_runtime(assembly='4.17.3')
        result = rt.get_replace_vars(None)
        self.assertEqual(result['runtime_assembly'], '4.17.3')

    def test_group_vars_override_preserves_runtime_assembly(self):
        """If group_config.vars already sets runtime_assembly, that value is preserved."""
        rt = self._make_runtime(assembly='4.17.3')
        group_config = Model(dict_to_model={'vars': {'runtime_assembly': 'custom-assembly', 'MAJOR': 4}})
        result = rt.get_replace_vars(group_config)
        self.assertEqual(result['runtime_assembly'], 'custom-assembly')
        self.assertEqual(result['MAJOR'], 4)

    def test_cli_var_overrides_group_vars(self):
        """--var CLI override wins over group_config.vars for runtime_assembly."""
        rt = self._make_runtime(assembly='4.17.3', extra_vars=['runtime_assembly=cli-override'])
        group_config = Model(dict_to_model={'vars': {'runtime_assembly': 'custom-assembly'}})
        result = rt.get_replace_vars(group_config)
        self.assertEqual(result['runtime_assembly'], 'cli-override')

    def test_cli_var_overrides_default_assembly(self):
        """--var CLI override wins over the default self.assembly value."""
        rt = self._make_runtime(assembly='4.17.3', extra_vars=['runtime_assembly=cli-override'])
        result = rt.get_replace_vars(None)
        self.assertEqual(result['runtime_assembly'], 'cli-override')

    def test_no_group_vars_no_assembly(self):
        """With no group_config and no assembly, runtime_assembly defaults to ''."""
        rt = self._make_runtime(assembly=None)
        result = rt.get_replace_vars(Model())
        self.assertEqual(result['runtime_assembly'], '')

    def test_release_name_default(self):
        """release_name defaults to '' when not provided by group_config.vars."""
        rt = self._make_runtime(assembly=None)
        result = rt.get_replace_vars(None)
        self.assertEqual(result['release_name'], '')


if __name__ == "__main__":
    unittest.main()
