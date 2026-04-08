from unittest.mock import Mock

import pytest
from doozerlib.lockfile import RpmInfo, RpmInfoCollector, RPMLockfileGenerator
from doozerlib.repodata import Repodata, Rpm


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

        # Act & Assert: Should not raise exception
        self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

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

        # Act & Assert: Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Verify error message contains specific details (now shows only latest versions)
        error_message = str(exc_info.value)
        assert "audit-libs" in error_message
        assert "x86_64:{0:3.1.5-4.el9}" in error_message
        assert "aarch64:{0:3.1.5-6.el9}" in error_message

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

        # Act & Assert: Should raise ValueError with multiple mismatches
        with pytest.raises(ValueError) as exc_info:
            self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Verify error message contains all mismatched packages
        error_message = str(exc_info.value)
        assert "audit-libs" in error_message
        assert "curl" in error_message
        assert "openssl-libs" in error_message

        # Verify architecture-specific version sets are listed
        assert "x86_64" in error_message
        assert "aarch64" in error_message
        assert "s390x" in error_message

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

        # Act & Assert: Should not raise exception
        self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

    def test_empty_rpm_list_passes_validation(self):
        """Test validation passes when RPM lists are empty."""
        # Arrange: Empty RPM lists for all architectures
        rpms_info_by_arch = {
            "x86_64": [],
            "aarch64": [],
            "ppc64le": [],
        }

        # Act & Assert: Should not raise exception
        self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

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

        # Act & Assert: Should not raise exception
        self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

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

        # Act & Assert: Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Verify error message shows only latest versions (not all historical versions)
        error_message = str(exc_info.value)
        assert "x86_64:{0:3.0.0-1.el9}" in error_message
        assert "aarch64:{0:4.0.0-1.el9}" in error_message

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

        # Act & Assert: Should fail due to shared-lib version mismatch but ignore single-arch packages
        with pytest.raises(ValueError) as exc_info:
            self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        error_message = str(exc_info.value)
        assert "shared-lib" in error_message
        assert "x86_64:{0:2.0.0-1.el9}" in error_message
        assert "aarch64:{0:2.0.0-2.el9}" in error_message
        assert "x86-only-driver" not in error_message
        assert "arm-specific-tool" not in error_message

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

        # Act: Should pass - single-arch packages ignored, shared-lib has identical versions
        try:
            self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)
            validation_passed = True
        except ValueError:
            validation_passed = False

        # Assert: Validation should pass with no exceptions
        assert validation_passed, (
            "Validation should pass when single-arch packages are present and multi-arch packages have identical versions"
        )


class TestCrossArchRepoDetection:
    """Test that cross-arch repo URL misconfiguration is detected and handled."""

    def setup_method(self):
        self.mock_repos = Mock()
        self.generator = RPMLockfileGenerator(self.mock_repos)

    def test_cross_arch_mismatch_is_warning_not_error(self):
        """When a version mismatch is caused by a cross-arch repo URL, it should warn, not raise."""
        # Simulate: openstack repo configured for aarch64/s390x but contains x86_64 RPMs
        self.generator.builder.cross_arch_repos = {
            ("openstack-16-rpms", "aarch64"): {"x86_64"},
            ("openstack-16-rpms", "s390x"): {"x86_64"},
        }

        # x86_64/ppc64le get higher version from openstack repo;
        # aarch64/s390x get lower version from base repo (openstack RPMs filtered out)
        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="python3-netifaces", evr="0:0.10.9-9.el8ost.1",
                    checksum="sha1", repoid="openstack-16-rpms", size=100,
                    sourcerpm="src.rpm", url="url1", epoch=0,
                    version="0.10.9", release="9.el8ost.1",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="python3-netifaces", evr="0:0.10.6-4.el8",
                    checksum="sha2", repoid="rhel-8-baseos-rpms", size=100,
                    sourcerpm="src.rpm", url="url2", epoch=0,
                    version="0.10.6", release="4.el8",
                ),
            ],
            "s390x": [
                RpmInfo(
                    name="python3-netifaces", evr="0:0.10.6-4.el8",
                    checksum="sha3", repoid="rhel-8-baseos-rpms", size=100,
                    sourcerpm="src.rpm", url="url3", epoch=0,
                    version="0.10.6", release="4.el8",
                ),
            ],
            "ppc64le": [
                RpmInfo(
                    name="python3-netifaces", evr="0:0.10.9-9.el8ost.1",
                    checksum="sha4", repoid="openstack-16-rpms", size=100,
                    sourcerpm="src.rpm", url="url4", epoch=0,
                    version="0.10.9", release="9.el8ost.1",
                ),
            ],
        }

        # Should NOT raise - just warn
        self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

    def test_real_mismatch_still_raises(self):
        """When a version mismatch is NOT caused by cross-arch repo issues, it should still raise."""
        # No cross-arch repos
        self.generator.builder.cross_arch_repos = {}

        rpms_info_by_arch = {
            "x86_64": [
                RpmInfo(
                    name="somepkg", evr="0:2.0-1.el9",
                    checksum="sha1", repoid="repo1", size=100,
                    sourcerpm="src.rpm", url="url1", epoch=0,
                    version="2.0", release="1.el9",
                ),
            ],
            "aarch64": [
                RpmInfo(
                    name="somepkg", evr="0:1.0-1.el9",
                    checksum="sha2", repoid="repo1", size=100,
                    sourcerpm="src.rpm", url="url2", epoch=0,
                    version="1.0", release="1.el9",
                ),
            ],
        }

        with pytest.raises(ValueError, match="RPM version set mismatches"):
            self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)


class TestDetectCrossArchRepo:
    """Test _detect_cross_arch_repo method on RpmInfoCollector."""

    def setup_method(self):
        self.mock_repos = Mock()
        self.collector = RpmInfoCollector(self.mock_repos)

    def test_detects_wrong_arch_repo(self):
        """Repo configured for aarch64 but containing x86_64 RPMs should be detected."""
        repodata = Repodata(
            name="openstack-rpms-aarch64",
            primary_rpms=[
                Rpm(name="pkg1", epoch=0, version="1.0", release="1.el8",
                    arch="x86_64", checksum="c1", size=100,
                    location="pkg1.rpm", sourcerpm="pkg1.src.rpm"),
                Rpm(name="pkg2", epoch=0, version="2.0", release="1.el8",
                    arch="x86_64", checksum="c2", size=200,
                    location="pkg2.rpm", sourcerpm="pkg2.src.rpm"),
            ],
        )

        self.collector._detect_cross_arch_repo("openstack-rpms", "aarch64", repodata)

        assert ("openstack-rpms", "aarch64") in self.collector.cross_arch_repos
        assert self.collector.cross_arch_repos[("openstack-rpms", "aarch64")] == {"x86_64"}

    def test_no_detection_for_correct_arch(self):
        """Repo with correct arch RPMs should not be flagged."""
        repodata = Repodata(
            name="baseos-rpms-aarch64",
            primary_rpms=[
                Rpm(name="pkg1", epoch=0, version="1.0", release="1.el8",
                    arch="aarch64", checksum="c1", size=100,
                    location="pkg1.rpm", sourcerpm="pkg1.src.rpm"),
            ],
        )

        self.collector._detect_cross_arch_repo("baseos-rpms", "aarch64", repodata)

        assert ("baseos-rpms", "aarch64") not in self.collector.cross_arch_repos

    def test_noarch_only_repo_is_fine(self):
        """Repo with only noarch packages should not be flagged."""
        repodata = Repodata(
            name="noarch-repo-aarch64",
            primary_rpms=[
                Rpm(name="pkg1", epoch=0, version="1.0", release="1.el8",
                    arch="noarch", checksum="c1", size=100,
                    location="pkg1.rpm", sourcerpm="pkg1.src.rpm"),
            ],
        )

        self.collector._detect_cross_arch_repo("noarch-repo", "aarch64", repodata)

        assert ("noarch-repo", "aarch64") not in self.collector.cross_arch_repos

    def test_empty_repo_is_fine(self):
        """Repo with no RPMs should not be flagged."""
        repodata = Repodata(name="empty-repo-aarch64", primary_rpms=[])

        self.collector._detect_cross_arch_repo("empty-repo", "aarch64", repodata)

        assert ("empty-repo", "aarch64") not in self.collector.cross_arch_repos
