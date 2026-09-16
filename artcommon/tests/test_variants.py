"""
Tests for build variant definitions.
"""

import unittest

from artcommonlib.variants import BuildVariant, get_build_variant_for_product


class TestBuildVariant(unittest.TestCase):
    def test_defines_all_build_variants(self):
        """Ensure all supported OCP and layered-product variant values are available."""
        expected_variants = {
            "ocp": BuildVariant.OCP,
            "okd": BuildVariant.OKD,
            "rhacm2": BuildVariant.ACM,
            "cert-manager": BuildVariant.CERT_MANAGER,
            "cluster-observability-operator": BuildVariant.COO,
            "external-secrets-operator": BuildVariant.EXTERNAL_SECRETS,
            "multicluster-engine": BuildVariant.MCE,
            "openshift-logging": BuildVariant.LOGGING,
            "mta": BuildVariant.MTA,
            "microshift": BuildVariant.MICROSHIFT,
            "rhmtc": BuildVariant.MTC,
            "oadp": BuildVariant.OADP,
            "quay": BuildVariant.QUAY,
            "oc-mirror": BuildVariant.OC_MIRROR,
            "mirror-gui": BuildVariant.MIRROR_GUI,
            "zero-trust-workload-identity-manager": BuildVariant.ZERO_TRUST,
        }

        self.assertEqual({variant.value for variant in BuildVariant}, set(expected_variants))
        for value, expected_variant in expected_variants.items():
            self.assertIs(BuildVariant(value), expected_variant)

    def test_resolves_build_variant_from_product(self):
        """Resolve and normalize a build-data product name."""
        self.assertIs(get_build_variant_for_product(" Openshift-Logging "), BuildVariant.LOGGING)

    def test_returns_none_for_unsupported_product(self):
        """Leave products without a defined variant unmapped."""
        self.assertIsNone(get_build_variant_for_product("unknown"))
