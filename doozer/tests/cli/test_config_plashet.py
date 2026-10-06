import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from artcommonlib.rpm_utils import parse_nvr
from doozerlib.cli.config_plashet import are_signed, compare_nvr_openshift_aware


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
        self.assertEqual(result, -1, "rhaos4.20 should beat rhaos4.21 when target is (4, 20)")

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

    def test_target_version_always_beats_non_target(self):
        """Test that target version always beats non-target, regardless of version number."""
        # When using -g openshift-4.21, rhaos4.21 should beat rhaos4.22
        obj1 = parse_nvr("haproxy-2.8.10-1.rhaos4.22.el9")  # doesn't match target 4.21
        obj2 = parse_nvr("haproxy-2.8.10-1.rhaos4.21.el9")  # matches target 4.21
        target_version = (4, 21)

        result = compare_nvr_openshift_aware(obj1, obj2, target_version)
        self.assertEqual(result, -1, "rhaos4.21 should beat rhaos4.22 when target is (4, 21)")

        # Also test the reverse order
        result = compare_nvr_openshift_aware(obj2, obj1, target_version)
        self.assertEqual(result, 1, "rhaos4.21 should beat rhaos4.22 when target is (4, 21)")


class _MulticallResult:
    """Tiny helper to mimic Koji multicall result proxy objects."""

    def __init__(self, value):
        self.result = value


class _MulticallContext:
    """
    Mimic ``koji_api.multicall()`` context manager.

    On __enter__ returns a mock session whose method calls record themselves
    and return ``_MulticallResult`` proxies.  On __exit__ the recorded calls
    are "executed" by matching them to the ``responses`` dict.

    ``responses`` maps ``(method_name, key_kwarg_value)`` → return value.
    """

    def __init__(self, responses: dict):
        self._responses = responses
        self._proxy = MagicMock()
        # Wire up proxy method calls so they return _MulticallResult objects.
        self._proxy.getBuild = lambda nvr, strict=True: _MulticallResult(self._responses.get(("getBuild", nvr)))
        self._proxy.listRPMs = lambda buildID: _MulticallResult(self._responses.get(("listRPMs", buildID)))
        self._proxy.queryRPMSigs = lambda rpm_id: _MulticallResult(self._responses.get(("queryRPMSigs", rpm_id), []))

    def __enter__(self):
        return self._proxy

    def __exit__(self, *exc):
        return False


def _make_koji_api(responses: dict):
    """Return a mock koji session whose ``multicall()`` yields the given canned responses."""
    api = MagicMock()
    api.multicall.side_effect = lambda batch=5000: _MulticallContext(responses)
    return api


class TestAreSigned(unittest.TestCase):
    """Tests for the batch ``are_signed()`` function."""

    def setUp(self):
        # The module-level logger is None until the CLI initialises it;
        # inject a mock so tests don't crash on log calls.
        import doozerlib.cli.config_plashet as _mod

        self._orig_logger = _mod.logger
        _mod.logger = MagicMock()

    def tearDown(self):
        import doozerlib.cli.config_plashet as _mod

        _mod.logger = self._orig_logger

    def _make_config(self, packages_path="/mnt/redhat/brewroot/packages/{el_version}"):
        return SimpleNamespace(
            packages_path=packages_path,
            signing_keys=("fd431d51",),
        )

    # --- all NVRs already signed → all True ---------------------------------

    @patch("doozerlib.cli.config_plashet.get_brewroot_base_path")
    @patch("koji.pathinfo")
    def test_all_nvrs_signed(self, mock_pathinfo, mock_get_base):
        """When every RPM in every build has a signed copy on disk, all results are True."""
        base = Path("/brewroot/packages/el9/pkg-a/1.0/1.el9")
        mock_get_base.return_value = base
        mock_pathinfo.signed = lambda rpm, sigkey: f"data/signed/{sigkey}/{rpm['name']}"

        responses = {
            ("getBuild", "pkg-a-1.0-1.el9"): {"id": 100},
            ("listRPMs", 100): [{"id": 1, "name": "pkg-a-1.0-1.el9.x86_64.rpm"}],
            ("queryRPMSigs", 1): [{"sigkey": "fd431d51"}],
        }
        koji_api = _make_koji_api(responses)
        config = self._make_config()

        with patch.object(Path, "exists", return_value=True):
            result = are_signed(config, ["pkg-a-1.0-1.el9"], koji_api)

        self.assertEqual(result, {"pkg-a-1.0-1.el9": True})

    # --- some unsigned NVRs → returns correctly ------------------------------

    @patch("doozerlib.cli.config_plashet.get_brewroot_base_path")
    @patch("koji.pathinfo")
    def test_some_unsigned_nvrs(self, mock_pathinfo, mock_get_base):
        """Mix of signed and unsigned NVRs returns the correct mapping."""
        base_a = Path("/brewroot/packages/el9/pkg-a/1.0/1.el9")
        base_b = Path("/brewroot/packages/el9/pkg-b/2.0/1.el9")
        mock_get_base.side_effect = lambda _cfg, nvre: {
            "pkg-a-1.0-1.el9": base_a,
            "pkg-b-2.0-1.el9": base_b,
        }[nvre]
        mock_pathinfo.signed = lambda rpm, sigkey: f"data/signed/{sigkey}/{rpm['name']}"

        responses = {
            ("getBuild", "pkg-a-1.0-1.el9"): {"id": 100},
            ("getBuild", "pkg-b-2.0-1.el9"): {"id": 200},
            ("listRPMs", 100): [{"id": 1, "name": "pkg-a-1.0-1.el9.x86_64.rpm"}],
            ("listRPMs", 200): [{"id": 2, "name": "pkg-b-2.0-1.el9.x86_64.rpm"}],
            ("queryRPMSigs", 1): [{"sigkey": "fd431d51"}],
            ("queryRPMSigs", 2): [],  # No signatures
        }
        koji_api = _make_koji_api(responses)
        config = self._make_config()

        with patch.object(Path, "exists", return_value=True):
            result = are_signed(config, ["pkg-a-1.0-1.el9", "pkg-b-2.0-1.el9"], koji_api)

        self.assertTrue(result["pkg-a-1.0-1.el9"])
        self.assertFalse(result["pkg-b-2.0-1.el9"])

    # --- single-RPM build with no signature → returns False ------------------

    @patch("doozerlib.cli.config_plashet.get_brewroot_base_path")
    @patch("koji.pathinfo")
    def test_single_rpm_no_signature(self, mock_pathinfo, mock_get_base):
        """A build with exactly one RPM and no signature returns False.

        This covers the edge case fixed by the set.intersection(*keys_per_rpm)
        change — the old code used keys_per_rpm[0] followed by iteration which
        erroneously returned True for an empty-keys single-RPM build.
        """
        base = Path("/brewroot/packages/el9/solo/1.0/1.el9")
        mock_get_base.return_value = base
        mock_pathinfo.signed = lambda rpm, sigkey: f"data/signed/{sigkey}/{rpm['name']}"

        responses = {
            ("getBuild", "solo-1.0-1.el9"): {"id": 300},
            ("listRPMs", 300): [{"id": 3, "name": "solo-1.0-1.el9.x86_64.rpm"}],
            ("queryRPMSigs", 3): [],  # No signature at all
        }
        koji_api = _make_koji_api(responses)
        config = self._make_config()

        result = are_signed(config, ["solo-1.0-1.el9"], koji_api)

        self.assertFalse(result["solo-1.0-1.el9"])

    # --- empty RPM list → returns True (nothing to sign) ---------------------

    @patch("doozerlib.cli.config_plashet.get_brewroot_base_path")
    @patch("koji.pathinfo")
    def test_empty_rpm_list(self, mock_pathinfo, mock_get_base):
        """A build with no RPMs at all is considered signed (nothing to verify)."""
        base = Path("/brewroot/packages/el9/empty/1.0/1.el9")
        mock_get_base.return_value = base

        responses = {
            ("getBuild", "empty-1.0-1.el9"): {"id": 400},
            ("listRPMs", 400): [],  # No RPMs in the build
        }
        koji_api = _make_koji_api(responses)
        config = self._make_config()

        result = are_signed(config, ["empty-1.0-1.el9"], koji_api)

        self.assertTrue(result["empty-1.0-1.el9"])

    # --- missing brewroot path → returns False --------------------------------

    @patch("doozerlib.cli.config_plashet.get_brewroot_base_path")
    def test_missing_brewroot_path(self, mock_get_base):
        """When brewroot path does not exist, the NVR is marked False."""
        mock_get_base.return_value = None  # Path not found

        koji_api = MagicMock()
        config = self._make_config()

        result = are_signed(config, ["gone-1.0-1.el9"], koji_api)

        self.assertFalse(result["gone-1.0-1.el9"])
        # No multicall should have been made since all NVRs were rejected early.
        koji_api.multicall.assert_not_called()

    # --- empty input list → returns empty dict --------------------------------

    def test_empty_input_list(self):
        """Calling are_signed with no NVRs returns an empty dict immediately."""
        koji_api = MagicMock()
        config = self._make_config()

        result = are_signed(config, [], koji_api)

        self.assertEqual(result, {})

    # --- mixed: some paths missing, some signed, some unsigned ----------------

    @patch("doozerlib.cli.config_plashet.get_brewroot_base_path")
    @patch("koji.pathinfo")
    def test_mixed_path_missing_and_signing(self, mock_pathinfo, mock_get_base):
        """Combines missing paths, signed, and unsigned builds in one call."""
        base_ok = Path("/brewroot/packages/el9/ok-pkg/1.0/1.el9")

        def _base(cfg, nvre):
            if nvre == "ok-pkg-1.0-1.el9":
                return base_ok
            return None  # missing-1.0-1.el9 has no brewroot path

        mock_get_base.side_effect = _base
        mock_pathinfo.signed = lambda rpm, sigkey: f"data/signed/{sigkey}/{rpm['name']}"

        responses = {
            ("getBuild", "ok-pkg-1.0-1.el9"): {"id": 500},
            ("listRPMs", 500): [{"id": 5, "name": "ok-pkg-1.0-1.el9.x86_64.rpm"}],
            ("queryRPMSigs", 5): [{"sigkey": "fd431d51"}],
        }
        koji_api = _make_koji_api(responses)
        config = self._make_config()

        with patch.object(Path, "exists", return_value=True):
            result = are_signed(config, ["ok-pkg-1.0-1.el9", "missing-1.0-1.el9"], koji_api)

        self.assertTrue(result["ok-pkg-1.0-1.el9"])
        self.assertFalse(result["missing-1.0-1.el9"])


if __name__ == '__main__':
    unittest.main()
