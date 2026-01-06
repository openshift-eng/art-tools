import unittest
from unittest.mock import patch

from artcommonlib.rpm_utils import parse_nvr
from doozerlib.cli.config_plashet import compare_nvr_openshift_aware


class TestCompareNvrOpenshiftAware(unittest.TestCase):
    """Test cases for OpenShift-aware NVR comparison function."""

    def test_openshift_version_comparison_newer(self):
        """Test that newer OpenShift versions are correctly identified as newer."""
        test_cases = [
            # (nvre1, nvre2, expected_result, description)
            (
                "haproxy-2.8.10-1.rhaos4.21.el9",
                "haproxy-2.8.10-2.rhaos4.20.el9",
                1,
                "4.21 vs 4.20: 4.21 should be newer",
            ),
            (
                "pkg-1.0-1.rhaos4.22.el9",
                "pkg-1.0-10.rhaos4.21.el9",
                1,
                "4.22 vs 4.21: 4.22 should be newer even with lower build number",
            ),
            ("test-2.1-5.rhaos4.18.el8", "test-2.1-20.rhaos4.17.el8", 1, "4.18 vs 4.17: 4.18 should be newer"),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_openshift_version_comparison_older(self):
        """Test that older OpenShift versions are correctly identified as older."""
        test_cases = [
            (
                "haproxy-2.8.10-2.rhaos4.20.el9",
                "haproxy-2.8.10-1.rhaos4.21.el9",
                -1,
                "4.20 vs 4.21: 4.20 should be older",
            ),
            ("pkg-1.0-10.rhaos4.21.el9", "pkg-1.0-1.rhaos4.22.el9", -1, "4.21 vs 4.22: 4.21 should be older"),
            ("test-2.1-20.rhaos4.17.el8", "test-2.1-5.rhaos4.18.el8", -1, "4.17 vs 4.18: 4.17 should be older"),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_openshift_version_comparison_equal(self):
        """Test that equal OpenShift versions are correctly identified as equal."""
        test_cases = [
            ("haproxy-2.8.10-1.rhaos4.21.el9", "haproxy-2.8.10-1.rhaos4.21.el9", 0, "Identical NVRs should be equal"),
            # Note: Different build numbers with same OpenShift version should fall back to standard comparison
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_same_openshift_version_fallback_to_standard_comparison(self):
        """Test that same OpenShift versions fall back to standard RPM comparison."""
        test_cases = [
            (
                "pkg-1.0-3.rhaos4.21.el9",
                "pkg-1.0-2.rhaos4.21.el9",
                1,
                "Same OpenShift version: higher build number should be newer",
            ),
            (
                "pkg-1.0-2.rhaos4.21.el9",
                "pkg-1.0-3.rhaos4.21.el9",
                -1,
                "Same OpenShift version: lower build number should be older",
            ),
            ("pkg-1.0-1.rhaos4.20.el9", "pkg-1.0-1.rhaos4.20.el8", 1, "Same OpenShift version: el9 vs el8"),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_no_openshift_version_fallback_to_standard_comparison(self):
        """Test packages without OpenShift versions use standard comparison."""
        test_cases = [
            ("package-1.0.0-2.el9", "package-1.0.0-1.el9", 1, "No OpenShift version: higher release should be newer"),
            ("package-1.0.0-1.el9", "package-1.0.0-2.el9", -1, "No OpenShift version: lower release should be older"),
            ("package-1.0.0-1.el9", "package-1.0.0-1.el9", 0, "No OpenShift version: identical should be equal"),
            ("package-1.0.0-1.el9", "package-1.0.0-1.el8", 1, "No OpenShift version: el9 vs el8"),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_mixed_openshift_and_non_openshift_versions(self):
        """Test comparison between packages with and without OpenShift versions."""
        test_cases = [
            # When one has OpenShift version and one doesn't, fall back to standard comparison
            ("package-1.0.0-1.rhaos4.21.el9", "package-1.0.0-2.el9", -1, "OpenShift vs non-OpenShift: build 1 vs 2"),
            ("package-1.0.0-2.el9", "package-1.0.0-1.rhaos4.21.el9", 1, "Non-OpenShift vs OpenShift: build 2 vs 1"),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_different_package_names_raises_error(self):
        """Test that comparing different package names raises ValueError."""
        obj1 = parse_nvr("haproxy-2.8.10-1.rhaos4.21.el9")
        obj2 = parse_nvr("nginx-1.0.0-1.rhaos4.20.el9")

        with self.assertRaises(ValueError) as cm:
            compare_nvr_openshift_aware(obj1, obj2)

        self.assertIn("Package names don't match", str(cm.exception))
        self.assertIn("haproxy", str(cm.exception))
        self.assertIn("nginx", str(cm.exception))

    def test_different_epochs(self):
        """Test comparison with different epochs."""
        test_cases = [
            (
                "1:package-1.0.0-1.rhaos4.21.el9",
                "package-1.0.0-2.rhaos4.22.el9",
                1,
                "Epoch 1 vs no epoch: epoch takes precedence",
            ),
            (
                "package-1.0.0-1.rhaos4.21.el9",
                "1:package-1.0.0-2.rhaos4.22.el9",
                -1,
                "No epoch vs epoch 1: epoch takes precedence",
            ),
            (
                "2:package-1.0.0-1.rhaos4.21.el9",
                "1:package-1.0.0-2.rhaos4.22.el9",
                1,
                "Epoch 2 vs epoch 1: higher epoch wins",
            ),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_different_versions(self):
        """Test comparison with different package versions."""
        test_cases = [
            (
                "package-2.0.0-1.rhaos4.21.el9",
                "package-1.0.0-2.rhaos4.22.el9",
                1,
                "Version 2.0.0 vs 1.0.0: higher version wins",
            ),
            (
                "package-1.0.0-1.rhaos4.22.el9",
                "package-2.0.0-2.rhaos4.21.el9",
                -1,
                "Version 1.0.0 vs 2.0.0: lower version loses",
            ),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    def test_original_haproxy_bug_case(self):
        """Test the specific haproxy case that caused the original bug."""
        # This is the exact case from the error message
        tagged_obj = parse_nvr("haproxy-2.8.10-1.rhaos4.21.el9")
        released_obj = parse_nvr("haproxy-2.8.10-2.rhaos4.20.el9")

        result = compare_nvr_openshift_aware(tagged_obj, released_obj)

        # The 4.21 build should be considered NEWER than the 4.20 build
        self.assertEqual(result, 1, "haproxy 4.21 build should be newer than 4.20 build")

        # Verify the condition that was causing the skip
        should_skip = result < 0
        self.assertFalse(should_skip, "The 4.21 build should NOT be skipped")

    def test_edge_cases_with_openshift_versions(self):
        """Test edge cases with various OpenShift version patterns."""
        test_cases = [
            # Major version differences
            ("pkg-1.0-1.rhaos5.1.el9", "pkg-1.0-10.rhaos4.25.el9", 1, "rhaos5.1 vs rhaos4.25: major version 5 > 4"),
            ("pkg-1.0-10.rhaos4.25.el9", "pkg-1.0-1.rhaos5.1.el9", -1, "rhaos4.25 vs rhaos5.1: major version 4 < 5"),
            # Large minor version differences
            ("pkg-1.0-1.rhaos4.25.el9", "pkg-1.0-50.rhaos4.24.el9", 1, "rhaos4.25 vs rhaos4.24: minor version 25 > 24"),
            # Single digit vs double digit
            ("pkg-1.0-1.rhaos4.5.el9", "pkg-1.0-1.rhaos4.10.el9", -1, "rhaos4.5 vs rhaos4.10: 5 < 10"),
        ]

        for nvre1, nvre2, expected, description in test_cases:
            with self.subTest(nvre1=nvre1, nvre2=nvre2):
                obj1 = parse_nvr(nvre1)
                obj2 = parse_nvr(nvre2)
                result = compare_nvr_openshift_aware(obj1, obj2)
                self.assertEqual(result, expected, f"Failed: {description}")

    @patch('doozerlib.cli.config_plashet._rpmvercmp')
    def test_fallback_to_rpmvercmp_called(self, mock_rpmvercmp):
        """Test that _rpmvercmp is called for fallback cases."""
        mock_rpmvercmp.return_value = 1

        # Test case where OpenShift versions are the same (should fall back)
        obj1 = parse_nvr("pkg-1.0-3.rhaos4.21.el9")
        obj2 = parse_nvr("pkg-1.0-2.rhaos4.21.el9")

        result = compare_nvr_openshift_aware(obj1, obj2)

        # Should have called _rpmvercmp for the release comparison
        mock_rpmvercmp.assert_called_once_with("3.rhaos4.21.el9", "2.rhaos4.21.el9")
        self.assertEqual(result, 1)

    @patch('doozerlib.cli.config_plashet._rpmvercmp')
    def test_fallback_to_rpmvercmp_called_no_openshift_versions(self, mock_rpmvercmp):
        """Test that _rpmvercmp is called when no OpenShift versions are present."""
        mock_rpmvercmp.return_value = -1

        # Test case with no OpenShift versions
        obj1 = parse_nvr("pkg-1.0-1.el9")
        obj2 = parse_nvr("pkg-1.0-2.el9")

        result = compare_nvr_openshift_aware(obj1, obj2)

        # Should have called _rpmvercmp for the release comparison
        mock_rpmvercmp.assert_called_once_with("1.el9", "2.el9")
        self.assertEqual(result, -1)


class TestCompareNvrOpenshiftAwareWithTarget(unittest.TestCase):
    """Test cases for OpenShift-aware NVR comparison with target version scoping."""

    def test_target_version_scoping_match_target_prioritizes(self):
        """Test that target version gets priority when one package matches target."""
        # When using -g openshift-4.21, rhaos4.21 should beat rhaos4.20
        obj1 = parse_nvr("haproxy-2.8.10-1.rhaos4.21.el9")  # matches target 4.21
        obj2 = parse_nvr("haproxy-2.8.10-2.rhaos4.20.el9")  # doesn't match target
        target_version = (4, 21)

        result = compare_nvr_openshift_aware(obj1, obj2, target_version)
        self.assertEqual(result, 1, "rhaos4.21 should be newer than rhaos4.20 when target is (4, 21)")

    def test_target_version_scoping_no_match_standard_comparison(self):
        """Test that standard comparison is used when neither package matches target."""
        # When using -g openshift-4.21, but comparing rhaos4.19 vs rhaos4.20
        # Should fall back to standard comparison (higher build number wins)
        obj1 = parse_nvr("haproxy-2.8.10-1.rhaos4.19.el9")  # doesn't match target
        obj2 = parse_nvr("haproxy-2.8.10-2.rhaos4.20.el9")  # doesn't match target
        target_version = (4, 21)

        result = compare_nvr_openshift_aware(obj1, obj2, target_version)
        self.assertEqual(result, -1, "When neither matches target, higher build number should win")

    def test_target_version_scoping_reverse_match(self):
        """Test when second package matches target version."""
        # When using -g openshift-4.20, rhaos4.20 should beat rhaos4.21
        obj1 = parse_nvr("haproxy-2.8.10-1.rhaos4.21.el9")  # doesn't match target 4.20
        obj2 = parse_nvr("haproxy-2.8.10-2.rhaos4.20.el9")  # matches target 4.20
        target_version = (4, 20)

        result = compare_nvr_openshift_aware(obj1, obj2, target_version)
        self.assertEqual(
            result, 1, "rhaos4.21 should still be newer than rhaos4.20 (standard OpenShift version comparison)"
        )

    def test_no_target_version_original_behavior(self):
        """Test that original behavior is preserved when target_version is None."""
        obj1 = parse_nvr("haproxy-2.8.10-1.rhaos4.21.el9")
        obj2 = parse_nvr("haproxy-2.8.10-2.rhaos4.20.el9")

        # Should behave exactly like the original function
        result = compare_nvr_openshift_aware(obj1, obj2, None)
        self.assertEqual(result, 1, "Original behavior should be preserved")

    def test_both_match_target_standard_openshift_rules(self):
        """Test standard OpenShift comparison when both packages match target."""
        # When both packages are for the target version, use standard comparison
        obj1 = parse_nvr("haproxy-2.8.10-1.rhaos4.21.el9")
        obj2 = parse_nvr("haproxy-2.8.10-2.rhaos4.21.el9")
        target_version = (4, 21)

        result = compare_nvr_openshift_aware(obj1, obj2, target_version)
        self.assertEqual(result, -1, "When both match target, higher build number should win")


if __name__ == '__main__':
    unittest.main()
