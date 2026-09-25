"""
Tests for the unified product mapping catalog.
"""

import unittest
from dataclasses import FrozenInstanceError, is_dataclass

from artcommonlib.product_catalog import (
    PRODUCT_CATALOG,
    find_product_config,
    get_kubeconfig_env_vars,
    get_product_config,
)
from artcommonlib.product_models import ConformaPolicies, ProductConfig, ReleaseTarget
from artcommonlib.variants import ProductId


class TestProductCatalog(unittest.TestCase):
    def test_product_config_is_a_frozen_dataclass(self):
        """Represent product configuration as an immutable dataclass."""
        config = get_product_config("ocp")

        self.assertIsInstance(config, ProductConfig)
        self.assertTrue(is_dataclass(config))
        with self.assertRaises(FrozenInstanceError):
            config.namespace = "mutated"

    def test_product_catalog_contains_only_dataclasses(self):
        """Keep the product catalog as an explicit collection of dataclasses."""
        self.assertIsInstance(PRODUCT_CATALOG, tuple)
        self.assertTrue(all(is_dataclass(config) for config in PRODUCT_CATALOG))

    def test_product_config_release_plans_are_immutable(self):
        """Prevent callers from mutating a product's release-plan mapping."""
        config = get_product_config("ocp")

        with self.assertRaises(TypeError):
            config.fbc_stage_release_plans[(5, 2)] = "unexpected-plan"

    def test_product_names_and_aliases_are_unique(self):
        """Keep canonical product names and aliases unambiguous."""
        names = [name.strip().lower() for config in PRODUCT_CATALOG for name in (config.product_name, *config.aliases)]

        self.assertEqual(len(names), len(set(names)))

    def test_product_config_uses_named_models_for_nested_values(self):
        """Use named models for release and policy configuration."""
        config = get_product_config("ocp")

        self.assertIsInstance(config.base_image_release, ReleaseTarget)
        self.assertEqual(config.base_image_release.release_plan, "ocp-art-images-base-silent")

        logging_config = get_product_config("openshift-logging")
        self.assertIsInstance(logging_config.conforma_stage_policies, ConformaPolicies)
        self.assertEqual(
            logging_config.conforma_stage_policies.image_policy,
            "rhtap-releng-tenant/registry-art-logging-stage",
        )

    def test_logging_alias_resolves_to_canonical_product(self):
        """Resolve the legacy logging product key to the canonical product."""
        config = get_product_config(" logging ")

        self.assertEqual(config.product_name, "openshift-logging")
        self.assertIs(config.product_id, ProductId.LOGGING)

    def test_installer_is_a_buildable_product(self):
        """Represent the OpenShift agent installer as a buildable product."""
        config = get_product_config("openshift_agent_installer")

        self.assertEqual(config.namespace, "art-installer-agent-tenant")
        self.assertEqual(config.kubeconfig_env, "ASSISTED_INSTALLER_SA_KUBECONFIG")
        self.assertIs(config.product_id, ProductId.AGENT_INSTALLER)

    def test_conforma_policy_can_omit_fbc_policy(self):
        """Represent a product with no Conforma FBC policy explicitly."""
        config = get_product_config("mirror_gui")

        self.assertEqual(config.conforma_stage_policies.image_policy, "rhtap-releng-tenant/registry-standard")
        self.assertIsNone(config.conforma_stage_policies.fbc_policy)

    def test_ocp_base_image_targets_are_preserved(self):
        """Preserve OCP production and EC base-image targets."""
        config = get_product_config("ocp")

        self.assertEqual(config.base_image_release.release_plan, "ocp-art-images-base-silent")
        self.assertEqual(config.base_image_release.application, "art-images-base")
        self.assertEqual(config.ec_base_image_release.release_plan, "ocp-art-images-base-silent-ec")
        self.assertEqual(config.ec_base_image_release.application, "art-images-base")

    def test_unknown_product_returns_none_from_optional_lookup(self):
        """Return None when an optional product lookup cannot resolve a key."""
        self.assertIsNone(find_product_config("unknown-product"))

    def test_kubeconfig_environment_names_are_unique(self):
        """Return unique kubeconfig environment names for diagnostics."""
        values = get_kubeconfig_env_vars()

        self.assertEqual(len(values), len(set(values)))
        self.assertIn("KONFLUX_SA_KUBECONFIG", values)
