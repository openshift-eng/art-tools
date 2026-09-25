"""
Tests for product identifiers.
"""

import unittest

from artcommonlib.product_catalog import get_product_id_for_product
from artcommonlib.variants import BuildVariant, ProductId, get_build_variant_for_product


class TestProductId(unittest.TestCase):
    def test_defines_all_product_ids(self):
        """Ensure all product identities are represented by one enum."""
        expected_product_ids = {
            "ocp": ProductId.OCP,
            "okd": ProductId.OKD,
            "rhacm2": ProductId.ACM,
            "cert-manager": ProductId.CERT_MANAGER,
            "cluster-observability-operator": ProductId.COO,
            "external-secrets-operator": ProductId.EXTERNAL_SECRETS,
            "multicluster-engine": ProductId.MCE,
            "openshift-logging": ProductId.LOGGING,
            "mta": ProductId.MTA,
            "microshift": ProductId.MICROSHIFT,
            "rhmtc": ProductId.MTC,
            "oadp": ProductId.OADP,
            "quay": ProductId.QUAY,
            "oc-mirror": ProductId.OC_MIRROR,
            "mirror_gui": ProductId.MIRROR_GUI,
            "openshift-opentelemetry-operator": ProductId.RHOSDT,
            "zero-trust-workload-identity-manager": ProductId.ZERO_TRUST,
            "supplemental-tools": ProductId.SUPPLEMENTAL_TOOLS,
            "openshift_agent_installer": ProductId.AGENT_INSTALLER,
        }

        self.assertIs(BuildVariant, ProductId)
        self.assertEqual({product_id.value for product_id in ProductId}, set(expected_product_ids))
        for value, expected_product_id in expected_product_ids.items():
            self.assertIs(ProductId(value), expected_product_id)

    def test_resolves_product_id_from_product(self):
        """Resolve and normalize a build-data product name."""
        self.assertIs(get_product_id_for_product(" Openshift-Logging "), ProductId.LOGGING)

    def test_resolves_product_id_from_product_alias(self):
        """Resolve a product alias through the shared product registry."""
        self.assertIs(get_product_id_for_product(" logging "), ProductId.LOGGING)

    def test_legacy_build_variant_lookup_wraps_product_id_lookup(self):
        """Keep the historical resolver as a compatibility wrapper."""
        self.assertIs(get_build_variant_for_product("logging"), ProductId.LOGGING)

    def test_raises_for_unknown_product(self):
        """Reject unknown products with the registry error."""
        with self.assertRaisesRegex(ValueError, "Unknown product 'unknown'"):
            get_product_id_for_product("unknown")

    def test_resolves_openshift_agent_installer_product_id(self):
        """Resolve the OpenShift agent installer product identifier."""
        self.assertIs(get_product_id_for_product("openshift_agent_installer"), ProductId.AGENT_INSTALLER)
