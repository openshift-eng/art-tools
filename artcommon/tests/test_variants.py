"""
Tests for build variant definitions.
"""

import unittest

from artcommonlib.variants import BuildVariant


class TestBuildVariant(unittest.TestCase):
    def test_supports_all_non_ocp_build_variants(self):
        """Ensure all supported OCP and layered-product variant values are available."""
        expected_variants = {
            "ocp": BuildVariant.OCP,
            "okd": BuildVariant.OKD,
            "acm": BuildVariant.ACM,
            "cert-manager": BuildVariant.CERT_MANAGER,
            "coo": BuildVariant.COO,
            "external-secrets": BuildVariant.EXTERNAL_SECRETS,
            "mce": BuildVariant.MCE,
            "logging": BuildVariant.LOGGING,
            "mta": BuildVariant.MTA,
            "mtc": BuildVariant.MTC,
            "oadp": BuildVariant.OADP,
            "oc-mirror": BuildVariant.OC_MIRROR,
            "zero-trust": BuildVariant.ZERO_TRUST,
        }

        self.assertEqual({variant.value for variant in BuildVariant}, set(expected_variants))
        for value, expected_variant in expected_variants.items():
            self.assertIs(BuildVariant(value), expected_variant)
