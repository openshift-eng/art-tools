"""
Tests for build variant definitions.
"""

import unittest
from unittest.mock import patch

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
            "openshift-opentelemetry-operator": BuildVariant.RHOSDT,
            "zero-trust-workload-identity-manager": BuildVariant.ZERO_TRUST,
        }

        self.assertEqual({variant.value for variant in BuildVariant}, set(expected_variants))
        for value, expected_variant in expected_variants.items():
            self.assertIs(BuildVariant(value), expected_variant)

    def test_resolves_build_variant_from_product(self):
        """Resolve and normalize a build-data product name."""
        self.assertIs(get_build_variant_for_product(" Openshift-Logging "), BuildVariant.LOGGING)

    @patch("artcommonlib.variants.logger")
    def test_raises_and_logs_for_unsupported_product(self, mock_logger):
        """Reject products without a defined build variant."""
        message = "No build variant found for product unknown; add it to the BuildVariant enum"
        with self.assertRaisesRegex(ValueError, message):
            get_build_variant_for_product("unknown")
        mock_logger.error.assert_called_once_with(message)

    def test_raises_for_openshift_agent_installer(self):
        """Require an explicit build variant for the OpenShift agent installer."""
        with self.assertRaisesRegex(
            ValueError,
            "No build variant found for product openshift_agent_installer; add it to the BuildVariant enum",
        ):
            get_build_variant_for_product("openshift_agent_installer")
