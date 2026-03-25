from unittest.mock import Mock

import pytest
from doozerlib.lockfile import RpmInfo, RPMLockfileGenerator


class TestCrossArchVersionSetValidation:
    """Test suite for cross-architecture RPM version set validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_repos = Mock()
        self.generator = RPMLockfileGenerator(self.mock_repos)

    def test_valid_identical_version_sets_across_architectures(self):
        """Test validation passes when version sets are identical across architectures."""
        # Arrange: Multiple versions per package, but identical sets across architectures
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.5-4.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="audit-3.1.5-4.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="3.1.5",
                    release="4.el9",
                ),
                RpmInfo(
                    name="curl",
                    evr="0:7.76.1-29.el9",
                    checksum="sha3",
                    repoid="repo1",
                    size=300,
                    sourcerpm="curl-7.76.1-29.el9.src.rpm",
                    url="url3",
                    epoch=0,
                    version="7.76.1",
                    release="29.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha4",
                    repoid="repo2",
                    size=101,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url4",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.5-4.el9",
                    checksum="sha5",
                    repoid="repo2",
                    size=201,
                    sourcerpm="audit-3.1.5-4.el9.src.rpm",
                    url="url5",
                    epoch=0,
                    version="3.1.5",
                    release="4.el9",
                ),
                RpmInfo(
                    name="curl",
                    evr="0:7.76.1-29.el9",
                    checksum="sha6",
                    repoid="repo2",
                    size=301,
                    sourcerpm="curl-7.76.1-29.el9.src.rpm",
                    url="url6",
                    epoch=0,
                    version="7.76.1",
                    release="29.el9",
                ),
            ],
            "ppc64le": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha7",
                    repoid="repo3",
                    size=102,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url7",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.5-4.el9",
                    checksum="sha8",
                    repoid="repo3",
                    size=202,
                    sourcerpm="audit-3.1.5-4.el9.src.rpm",
                    url="url8",
                    epoch=0,
                    version="3.1.5",
                    release="4.el9",
                ),
                RpmInfo(
                    name="curl",
                    evr="0:7.76.1-29.el9",
                    checksum="sha9",
                    repoid="repo3",
                    size=302,
                    sourcerpm="curl-7.76.1-29.el9.src.rpm",
                    url="url9",
                    epoch=0,
                    version="7.76.1",
                    release="29.el9",
                ),
            ],
        }

        # Act & Assert: Should return None (no fallback needed)
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)
        assert result is None

    def test_version_set_mismatch_single_package_single_architecture(self):
        """Test validation fails when one package has different version sets across architectures."""
        # Arrange: audit-libs has different version on aarch64
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.5-4.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="audit-3.1.5-4.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="3.1.5",
                    release="4.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha3",
                    repoid="repo2",
                    size=101,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url3",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.5-6.el9",
                    checksum="sha4",
                    repoid="repo2",
                    size=202,
                    sourcerpm="audit-3.1.5-6.el9.src.rpm",
                    url="url4",
                    epoch=0,
                    version="3.1.5",
                    release="6.el9",
                ),
            ],
        }

        # Act: Should return mismatch data instead of raising exception
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Assert: Should return fallback recommendations
        assert result is not None
        assert "audit-libs" in result
        assert "x86_64" in result["audit-libs"]
        assert "aarch64" in result["audit-libs"]

    def test_version_set_mismatch_multiple_packages_multiple_architectures(self):
        """Test validation fails with detailed errors for multiple package mismatches across architectures."""
        # Arrange: Multiple packages with different version sets across multiple architectures
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="curl",
                    evr="0:7.76.1-29.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="curl-7.76.1-29.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="7.76.1",
                    release="29.el9",
                ),
                RpmInfo(
                    name="openssl-libs",
                    evr="1:3.0.7-25.el9",
                    checksum="sha3",
                    repoid="repo1",
                    size=300,
                    sourcerpm="openssl-3.0.7-25.el9.src.rpm",
                    url="url3",
                    epoch=1,
                    version="3.0.7",
                    release="25.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.5-6.el9",
                    checksum="sha4",
                    repoid="repo2",
                    size=101,
                    sourcerpm="audit-3.1.5-6.el9.src.rpm",
                    url="url4",
                    epoch=0,
                    version="3.1.5",
                    release="6.el9",
                ),
                RpmInfo(
                    name="curl",
                    evr="0:7.76.1-29.el9",
                    checksum="sha5",
                    repoid="repo2",
                    size=201,
                    sourcerpm="curl-7.76.1-29.el9.src.rpm",
                    url="url5",
                    epoch=0,
                    version="7.76.1",
                    release="29.el9",
                ),
                RpmInfo(
                    name="openssl-libs",
                    evr="1:3.0.7-27.el9",
                    checksum="sha6",
                    repoid="repo2",
                    size=301,
                    sourcerpm="openssl-3.0.7-27.el9.src.rpm",
                    url="url6",
                    epoch=1,
                    version="3.0.7",
                    release="27.el9",
                ),
            ],
            "s390x": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha7",
                    repoid="repo3",
                    size=102,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url7",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="curl",
                    evr="0:7.76.1-30.el9",
                    checksum="sha8",
                    repoid="repo3",
                    size=202,
                    sourcerpm="curl-7.76.1-30.el9.src.rpm",
                    url="url8",
                    epoch=0,
                    version="7.76.1",
                    release="30.el9",
                ),
                RpmInfo(
                    name="openssl-libs",
                    evr="1:3.0.7-25.el9",
                    checksum="sha9",
                    repoid="repo3",
                    size=302,
                    sourcerpm="openssl-3.0.7-25.el9.src.rpm",
                    url="url9",
                    epoch=1,
                    version="3.0.7",
                    release="25.el9",
                ),
            ],
        }

        # Act: Should return mismatch data for multiple packages
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Assert: Should return all mismatched packages
        assert result is not None
        assert "audit-libs" in result
        assert "curl" in result
        assert "openssl-libs" in result

        # Verify architecture data is present
        assert "x86_64" in result["audit-libs"]
        assert "aarch64" in result["audit-libs"]
        assert "s390x" in result["audit-libs"]

    def test_single_architecture_always_passes_validation(self):
        """Test validation always passes for single architecture scenarios."""
        # Arrange: Single architecture with multiple versions per package
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.2-2.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="audit-3.1.2-2.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="3.1.2",
                    release="2.el9",
                ),
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.5-4.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="audit-3.1.5-4.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="3.1.5",
                    release="4.el9",
                ),
                RpmInfo(
                    name="audit-libs",
                    evr="0:3.1.7-8.el9",
                    checksum="sha3",
                    repoid="repo1",
                    size=250,
                    sourcerpm="audit-3.1.7-8.el9.src.rpm",
                    url="url3",
                    epoch=0,
                    version="3.1.7",
                    release="8.el9",
                ),
            ],
        }

        # Act & Assert: Should return None (no fallback needed)
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)
        assert result is None

    def test_empty_rpm_list_passes_validation(self):
        """Test validation passes when RPM lists are empty."""
        # Arrange: Empty RPM lists for all architectures
        rpms_info_by_arch = {
            "x86_64": [],
            "aarch64": [],
            "ppc64le": [],
        }

        # Act & Assert: Should return None (no fallback needed)
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)
        assert result is None

    def test_architecture_specific_packages_allowed(self):
        """Test validation passes when packages exist on subset of architectures."""
        # Arrange: Package exists on x86_64 but not on aarch64
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="x86-only-package",
                    evr="0:1.0.0-1.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="x86-only-package-1.0.0-1.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="1.0.0",
                    release="1.el9",
                ),
            ],
            "aarch64": [],
        }

        # Act & Assert: Should return None (no fallback needed)
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)
        assert result is None

    def test_complex_epoch_version_release_combinations(self):
        """Test validation with complex EVR combinations including different epochs."""
        # Arrange: Complex EVR scenarios with epochs
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="complex-package",
                    evr="2:1.5.0-10.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="complex-package-1.5.0-10.el9.src.rpm",
                    url="url1",
                    epoch=2,
                    version="1.5.0",
                    release="10.el9",
                ),
                RpmInfo(
                    name="complex-package",
                    evr="1:2.0.0-5.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="complex-package-2.0.0-5.el9.src.rpm",
                    url="url2",
                    epoch=1,
                    version="2.0.0",
                    release="5.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="complex-package",
                    evr="2:1.5.0-10.el9",
                    checksum="sha3",
                    repoid="repo2",
                    size=101,
                    sourcerpm="complex-package-1.5.0-10.el9.src.rpm",
                    url="url3",
                    epoch=2,
                    version="1.5.0",
                    release="10.el9",
                ),
                RpmInfo(
                    name="complex-package",
                    evr="1:2.0.0-5.el9",
                    checksum="sha4",
                    repoid="repo2",
                    size=201,
                    sourcerpm="complex-package-2.0.0-5.el9.src.rpm",
                    url="url4",
                    epoch=1,
                    version="2.0.0",
                    release="5.el9",
                ),
            ],
        }

        # Act & Assert: Should not raise exception (identical version sets)
        self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

    def test_valid_latest_version_cross_arch_with_different_historical_versions(self):
        """Test validation passes when latest versions match across architectures despite different historical versions."""
        # Arrange: x86_64 has 2 versions, others have 1, but latest is same across all architectures
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="test-package",
                    evr="0:1.19.13-1.el9_2",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="test-package-1.19.13-1.el9_2.src.rpm",
                    url="url1",
                    epoch=0,
                    version="1.19.13",
                    release="1.el9_2",
                ),
                RpmInfo(
                    name="test-package",
                    evr="0:1.22.12-11.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="test-package-1.22.12-11.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="1.22.12",
                    release="11.el9",
                ),
            ],
            "ppc64le": [
                RpmInfo(
                    name="test-package",
                    evr="0:1.22.12-11.el9",
                    checksum="sha3",
                    repoid="repo2",
                    size=200,
                    sourcerpm="test-package-1.22.12-11.el9.src.rpm",
                    url="url3",
                    epoch=0,
                    version="1.22.12",
                    release="11.el9",
                ),
            ],
            "s390x": [
                RpmInfo(
                    name="test-package",
                    evr="0:1.22.12-11.el9",
                    checksum="sha4",
                    repoid="repo3",
                    size=200,
                    sourcerpm="test-package-1.22.12-11.el9.src.rpm",
                    url="url4",
                    epoch=0,
                    version="1.22.12",
                    release="11.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="test-package",
                    evr="0:1.22.12-11.el9",
                    checksum="sha5",
                    repoid="repo4",
                    size=200,
                    sourcerpm="test-package-1.22.12-11.el9.src.rpm",
                    url="url5",
                    epoch=0,
                    version="1.22.12",
                    release="11.el9",
                ),
            ],
        }

        # Act & Assert: Should pass validation (latest versions are identical across architectures)
        self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

    def test_error_message_formatting_with_sorted_versions(self):
        """Test that error messages format version sets in sorted order for consistency."""
        # Arrange: Version sets that will be unsorted to test sorting behavior
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="test-package",
                    evr="0:3.0.0-1.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="test-package-3.0.0-1.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="3.0.0",
                    release="1.el9",
                ),
                RpmInfo(
                    name="test-package",
                    evr="0:1.0.0-1.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="test-package-1.0.0-1.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="1.0.0",
                    release="1.el9",
                ),
                RpmInfo(
                    name="test-package",
                    evr="0:2.0.0-1.el9",
                    checksum="sha3",
                    repoid="repo1",
                    size=300,
                    sourcerpm="test-package-2.0.0-1.el9.src.rpm",
                    url="url3",
                    epoch=0,
                    version="2.0.0",
                    release="1.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="test-package",
                    evr="0:1.0.0-1.el9",
                    checksum="sha4",
                    repoid="repo2",
                    size=201,
                    sourcerpm="test-package-1.0.0-1.el9.src.rpm",
                    url="url4",
                    epoch=0,
                    version="1.0.0",
                    release="1.el9",
                ),
                RpmInfo(
                    name="test-package",
                    evr="0:4.0.0-1.el9",
                    checksum="sha5",
                    repoid="repo2",
                    size=401,
                    sourcerpm="test-package-4.0.0-1.el9.src.rpm",
                    url="url5",
                    epoch=0,
                    version="4.0.0",
                    release="1.el9",
                ),
            ],
        }

        # Act: Should return mismatch data
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Assert: Should return fallback data for latest versions only
        assert result is not None
        assert "test-package" in result
        # Verify that only latest versions are considered for fallback
        assert result["test-package"]["x86_64"] == "0:3.0.0-1.el9"
        assert result["test-package"]["aarch64"] == "0:4.0.0-1.el9"

    def test_mixed_single_and_multi_arch_packages_validation(self):
        """Test that single-arch packages are ignored while multi-arch version mismatches still trigger failures."""
        # Arrange: Mixed scenario with single-arch packages (ignored) and multi-arch packages (validated)
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="shared-lib",
                    evr="0:2.0.0-1.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="shared-lib-2.0.0-1.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="2.0.0",
                    release="1.el9",
                ),
                RpmInfo(
                    name="x86-only-driver",
                    evr="0:1.0.0-1.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="x86-only-driver-1.0.0-1.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="1.0.0",
                    release="1.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="shared-lib",
                    evr="0:2.0.0-2.el9",
                    checksum="sha3",
                    repoid="repo2",
                    size=101,
                    sourcerpm="shared-lib-2.0.0-2.el9.src.rpm",
                    url="url3",
                    epoch=0,
                    version="2.0.0",
                    release="2.el9",
                ),
                RpmInfo(
                    name="arm-specific-tool",
                    evr="0:1.5.0-1.el9",
                    checksum="sha4",
                    repoid="repo2",
                    size=300,
                    sourcerpm="arm-specific-tool-1.5.0-1.el9.src.rpm",
                    url="url4",
                    epoch=0,
                    version="1.5.0",
                    release="1.el9",
                ),
            ],
        }

        # Act: Should return mismatch data for shared-lib but ignore single-arch packages
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Assert: Should only include shared-lib in mismatch data
        assert result is not None
        assert "shared-lib" in result
        assert "x86-only-driver" not in result
        assert "arm-specific-tool" not in result
        assert result["shared-lib"]["x86_64"] == "0:2.0.0-1.el9"
        assert result["shared-lib"]["aarch64"] == "0:2.0.0-2.el9"

    def test_mixed_single_and_multi_arch_packages_with_matching_versions(self):
        """Test that single-arch packages are ignored and multi-arch packages with identical versions pass."""
        # Arrange: Mixed scenario with single-arch packages (ignored) and multi-arch packages (identical versions)
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="shared-lib",
                    evr="0:2.0.0-1.el9",
                    checksum="sha1",
                    repoid="repo1",
                    size=100,
                    sourcerpm="shared-lib-2.0.0-1.el9.src.rpm",
                    url="url1",
                    epoch=0,
                    version="2.0.0",
                    release="1.el9",
                ),
                RpmInfo(
                    name="x86-only-driver",
                    evr="0:1.0.0-1.el9",
                    checksum="sha2",
                    repoid="repo1",
                    size=200,
                    sourcerpm="x86-only-driver-1.0.0-1.el9.src.rpm",
                    url="url2",
                    epoch=0,
                    version="1.0.0",
                    release="1.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="shared-lib",
                    evr="0:2.0.0-1.el9",
                    checksum="sha3",
                    repoid="repo2",
                    size=101,
                    sourcerpm="shared-lib-2.0.0-1.el9.src.rpm",
                    url="url3",
                    epoch=0,
                    version="2.0.0",
                    release="1.el9",
                ),
                RpmInfo(
                    name="arm-specific-tool",
                    evr="0:1.5.0-1.el9",
                    checksum="sha4",
                    repoid="repo2",
                    size=300,
                    sourcerpm="arm-specific-tool-1.5.0-1.el9.src.rpm",
                    url="url4",
                    epoch=0,
                    version="1.5.0",
                    release="1.el9",
                ),
            ],
        }

        # Act: Should return None - single-arch packages ignored, shared-lib has identical versions
        result = self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Assert: Should return None (no mismatches)
        assert result is None


class TestRPMLockfileFallback:
    """Test suite for RPM lockfile version fallback recovery."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_repos = Mock()
        self.generator = RPMLockfileGenerator(self.mock_repos)

    def test_find_common_fallback_versions_simple_case(self):
        """Test fallback version selection picks lowest version."""
        arch_versions = {"x86_64": "0:1.0-3", "aarch64": "0:1.0-4"}
        result = self.generator._find_common_fallback_versions("pkg1", arch_versions)
        assert result == "0:1.0-3"

    def test_find_common_fallback_versions_complex_epoch(self):
        """Test fallback with epoch comparison."""
        arch_versions = {"x86_64": "2:1.0-1", "aarch64": "1:2.0-1"}
        result = self.generator._find_common_fallback_versions("pkg1", arch_versions)
        assert result == "1:2.0-1"

    def test_apply_fallback_versions_case_1_success(self):
        """Test fallback application when fallback version exists in all architectures."""
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo("pkg1", "0:1.0-2", "checksum1", "repo1", 1000, "src1", "url1", 0, "1.0", "2"),
                RpmInfo("pkg1", "0:1.0-3", "checksum2", "repo1", 1000, "src1", "url2", 0, "1.0", "3"),
            ],
            "aarch64": [
                RpmInfo("pkg1", "0:1.0-2", "checksum3", "repo1", 1000, "src1", "url3", 0, "1.0", "2"),
                RpmInfo("pkg1", "0:1.0-4", "checksum4", "repo1", 1000, "src1", "url4", 0, "1.0", "4"),
            ],
        }

        fallback_recommendations = {"pkg1": "0:1.0-2"}
        self.generator._apply_fallback_versions(rpms_info_by_arch, fallback_recommendations)

        for arch in rpms_info_by_arch:
            pkg_rpms = [rpm for rpm in rpms_info_by_arch[arch] if rpm.name == "pkg1"]
            assert len(pkg_rpms) == 1
            assert pkg_rpms[0].evr == "0:1.0-2"

    def test_apply_fallback_versions_case_2_error(self):
        """Test fallback application fails when fallback version doesn't exist."""
        rpms_info_by_arch = {
            "x86_64": [RpmInfo("pkg1", "0:1.0-3", "checksum1", "repo1", 1000, "src1", "url1", 0, "1.0", "3")],
            "aarch64": [RpmInfo("pkg1", "0:1.0-4", "checksum2", "repo1", 1000, "src1", "url2", 0, "1.0", "4")],
        }

        fallback_recommendations = {"pkg1": "0:1.0-2"}

        with pytest.raises(ValueError, match="Fallback version 0:1.0-2 not available"):
            self.generator._apply_fallback_versions(rpms_info_by_arch, fallback_recommendations)

    def test_apply_fallback_versions_partial_architecture_coverage(self):
        """Test fallback with package missing on some architectures."""
        rpms_info_by_arch = {
            "x86_64": [RpmInfo("pkg1", "0:1.0-2", "checksum1", "repo1", 1000, "src1", "url1", 0, "1.0", "2")],
            "aarch64": [RpmInfo("pkg2", "0:2.0-1", "checksum2", "repo1", 1000, "src2", "url2", 0, "2.0", "1")],
        }

        fallback_recommendations = {"pkg1": "0:1.0-2"}
        self.generator._apply_fallback_versions(rpms_info_by_arch, fallback_recommendations)

        x86_64_pkg1 = [rpm for rpm in rpms_info_by_arch["x86_64"] if rpm.name == "pkg1"]
        assert len(x86_64_pkg1) == 1
        assert x86_64_pkg1[0].evr == "0:1.0-2"

        aarch64_pkg1 = [rpm for rpm in rpms_info_by_arch["aarch64"] if rpm.name == "pkg1"]
        assert len(aarch64_pkg1) == 0

    def test_apply_fallback_versions_multi_arch_consistency(self):
        """Test fallback maintains consistency across all architectures."""
        rpms_info_by_arch = {
            "x86_64": [RpmInfo("pkg1", "0:1.0-2", "checksum1", "repo1", 1000, "src1", "url1", 0, "1.0", "2")],
            "aarch64": [RpmInfo("pkg1", "0:1.0-2", "checksum2", "repo1", 1000, "src1", "url2", 0, "1.0", "2")],
            "ppc64le": [RpmInfo("pkg1", "0:1.0-2", "checksum3", "repo1", 1000, "src1", "url3", 0, "1.0", "2")],
            "s390x": [RpmInfo("pkg1", "0:1.0-2", "checksum4", "repo1", 1000, "src1", "url4", 0, "1.0", "2")],
        }

        fallback_recommendations = {"pkg1": "0:1.0-2"}
        self.generator._apply_fallback_versions(rpms_info_by_arch, fallback_recommendations)

        for arch in rpms_info_by_arch:
            pkg_rpms = [rpm for rpm in rpms_info_by_arch[arch] if rpm.name == "pkg1"]
            assert len(pkg_rpms) == 1
            assert pkg_rpms[0].evr == "0:1.0-2"

    def test_apply_fallback_only_mismatched_packages_affected(self):
        """Test that only mismatched packages are modified during fallback."""
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo("pkg1", "0:1.0-2", "checksum1", "repo1", 1000, "src1", "url1", 0, "1.0", "2"),
                RpmInfo("pkg2", "0:2.0-1", "checksum2", "repo1", 1000, "src2", "url2", 0, "2.0", "1"),
            ],
            "aarch64": [
                RpmInfo("pkg1", "0:1.0-2", "checksum3", "repo1", 1000, "src1", "url3", 0, "1.0", "2"),
                RpmInfo("pkg2", "0:2.0-1", "checksum4", "repo1", 1000, "src2", "url4", 0, "2.0", "1"),
            ],
        }

        original_pkg2 = {arch: [rpm for rpm in rpms if rpm.name == "pkg2"] for arch, rpms in rpms_info_by_arch.items()}

        fallback_recommendations = {"pkg1": "0:1.0-2"}
        self.generator._apply_fallback_versions(rpms_info_by_arch, fallback_recommendations)

        for arch in rpms_info_by_arch:
            pkg2_rpms = [rpm for rpm in rpms_info_by_arch[arch] if rpm.name == "pkg2"]
            assert pkg2_rpms == original_pkg2[arch]
