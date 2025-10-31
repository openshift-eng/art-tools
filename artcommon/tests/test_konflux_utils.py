"""Tests for artcommonlib.konflux.utils module."""

import os
from unittest import TestCase
from unittest.mock import patch

from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.utils import (
    normalize_group_name_for_k8s,
    resolve_konflux_kubeconfig,
    resolve_konflux_namespace,
)


class TestNormalizeGroupNameForK8s(TestCase):
    """Test cases for normalize_group_name_for_k8s function"""

    def test_simple_oadp_group(self):
        """Test simple oadp group name normalization"""
        result = normalize_group_name_for_k8s("oadp-1.5")
        self.assertEqual(result, "oadp-1-5")

    def test_complex_group_name(self):
        """Test complex group name with mixed case, underscores, and dots"""
        result = normalize_group_name_for_k8s("Test_Group-1.5")
        self.assertEqual(result, "test-group-1-5")

    def test_empty_string(self):
        """Test empty string input"""
        result = normalize_group_name_for_k8s("")
        self.assertEqual(result, "")

    def test_consecutive_dashes(self):
        """Test collapse of consecutive dashes"""
        result = normalize_group_name_for_k8s("test--group---name")
        self.assertEqual(result, "test-group-name")

    def test_leading_trailing_special_chars(self):
        """Test trimming of leading/trailing non-alphanumeric characters"""
        result = normalize_group_name_for_k8s("-_test.group_-")
        self.assertEqual(result, "test-group")

    def test_only_special_chars(self):
        """Test string with only special characters"""
        result = normalize_group_name_for_k8s("_..-__")
        self.assertEqual(result, "")

    def test_mixed_alphanumeric_special(self):
        """Test mixed alphanumeric and special characters"""
        result = normalize_group_name_for_k8s("test@group#1.2$name")
        self.assertEqual(result, "test-group-1-2-name")

    def test_long_group_name_truncation(self):
        """Test truncation of very long group names"""
        long_name = "a" * 100  # 100 character string
        result = normalize_group_name_for_k8s(long_name)
        # Should be truncated to leave room for timestamp (max 63 - 18 - 1 = 44 chars)
        self.assertLessEqual(len(result), 44)
        self.assertTrue(result.startswith("a"))

    def test_uppercase_conversion(self):
        """Test uppercase to lowercase conversion"""
        result = normalize_group_name_for_k8s("OADP-1.5")
        self.assertEqual(result, "oadp-1-5")

    def test_numeric_group(self):
        """Test group name with numbers"""
        result = normalize_group_name_for_k8s("group123-4.56")
        self.assertEqual(result, "group123-4-56")


class TestResolveKonfluxKubeconfig(TestCase):
    """Test cases for resolve_konflux_kubeconfig function"""

    def test_provided_kubeconfig_takes_precedence(self):
        """Test that explicitly provided kubeconfig is returned"""
        result = resolve_konflux_kubeconfig("oadp-1.5", "/path/to/provided/kubeconfig")
        self.assertEqual(result, "/path/to/provided/kubeconfig")

    @patch.dict(os.environ, {"OADP_KONFLUX_SA_KUBECONFIG": "/path/to/oadp/kubeconfig"})
    def test_oadp_group_kubeconfig_resolution(self):
        """Test kubeconfig resolution for OADP groups"""
        result = resolve_konflux_kubeconfig("oadp-1.5")
        self.assertEqual(result, "/path/to/oadp/kubeconfig")

    @patch.dict(os.environ, {"KONFLUX_SA_KUBECONFIG": "/path/to/openshift/kubeconfig"})
    def test_openshift_group_kubeconfig_resolution(self):
        """Test kubeconfig resolution for OpenShift groups"""
        result = resolve_konflux_kubeconfig("openshift-4.17")
        self.assertEqual(result, "/path/to/openshift/kubeconfig")

    def test_no_mapping_found(self):
        """Test behavior when no mapping is found for group"""
        with patch.dict(os.environ, {}, clear=True):
            result = resolve_konflux_kubeconfig("unknown-group")
            self.assertIsNone(result)

    @patch.dict(os.environ, {}, clear=True)
    def test_env_var_not_set(self):
        """Test behavior when mapped env var is not set"""
        result = resolve_konflux_kubeconfig("oadp-1.5")
        self.assertIsNone(result)


class TestResolveKonfluxNamespace(TestCase):
    """Test cases for resolve_konflux_namespace function"""

    def test_provided_namespace_takes_precedence(self):
        """Test that explicitly provided namespace is returned"""
        result = resolve_konflux_namespace("oadp-1.5", "custom-namespace")
        self.assertEqual(result, "custom-namespace")

    def test_oadp_group_namespace_resolution(self):
        """Test namespace resolution for OADP groups"""
        result = resolve_konflux_namespace("oadp-1.5")
        self.assertEqual(result, "art-oadp-tenant")

    def test_openshift_group_namespace_resolution(self):
        """Test namespace resolution for OpenShift groups"""
        result = resolve_konflux_namespace("openshift-4.17")
        self.assertEqual(result, "ocp-art-tenant")

    def test_mta_group_namespace_resolution(self):
        """Test namespace resolution for MTA groups"""
        result = resolve_konflux_namespace("mta-7.0")
        self.assertEqual(result, "art-mta-tenant")

    def test_logging_group_namespace_resolution(self):
        """Test namespace resolution for logging groups"""
        result = resolve_konflux_namespace("logging-6.3")
        self.assertEqual(result, "art-logging-tenant")

    def test_no_mapping_found_uses_default(self):
        """Test that default namespace is used when no mapping is found"""
        result = resolve_konflux_namespace("unknown-group")
        self.assertEqual(result, KONFLUX_DEFAULT_NAMESPACE)

    def test_longest_prefix_match(self):
        """Test that longest matching prefix is preferred"""
        # openshift-4.17 matches "openshift-" prefix and should return ocp-art-tenant
        result = resolve_konflux_namespace("openshift-4.17")
        self.assertEqual(result, "ocp-art-tenant")
