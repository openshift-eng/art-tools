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

        # Verify error message contains specific details
        error_message = str(exc_info.value)
        assert "audit-libs" in error_message
        assert "x86_64:{0:3.1.2-2.el9,0:3.1.5-4.el9}" in error_message
        assert "aarch64:{0:3.1.2-2.el9,0:3.1.5-6.el9}" in error_message

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

    def test_partial_architecture_coverage_mismatch(self):
        """Test validation fails when packages exist on some but not all architectures."""
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

        # Act & Assert: Should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            self.generator._validate_cross_arch_version_sets(rpms_info_by_arch)

        # Verify error message contains package name and architecture details
        error_message = str(exc_info.value)
        assert "x86-only-package" in error_message
        assert "x86_64:{0:1.0.0-1.el9}" in error_message
        assert "aarch64:{}" in error_message

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

        # Verify error message has sorted version sets for readability
        error_message = str(exc_info.value)
        assert "x86_64:{0:1.0.0-1.el9,0:2.0.0-1.el9,0:3.0.0-1.el9}" in error_message
        assert "aarch64:{0:1.0.0-1.el9,0:4.0.0-1.el9}" in error_message
