import logging
from unittest.mock import MagicMock, Mock

from doozerlib.lockfile import RpmInfo, RPMLockfileGenerator
from doozerlib.repodata import Rpm


def _make_rpm_info(name, epoch, version, release, arch="x86_64", repoid="repo1", checksum="sha1", url_prefix="url"):
    """Helper to create RpmInfo instances for tests."""
    evr = f"{epoch}:{version}-{release}"
    return RpmInfo(
        name=name,
        evr=evr,
        checksum=checksum,
        repoid=repoid,
        size=100,
        sourcerpm=f"{name}-{version}-{release}.src.rpm",
        url=f"{url_prefix}/{name}-{version}-{release}.{arch}.rpm",
        epoch=epoch,
        version=version,
        release=release,
    )


def _make_rpm(name, epoch, version, release, arch="x86_64"):
    """Helper to create Rpm instances for loaded repos."""
    return Rpm(
        name=name,
        epoch=epoch,
        version=version,
        release=release,
        arch=arch,
        checksum=f"sha-{name}-{version}",
        size=100,
        location=f"Packages/{name}-{version}-{release}.{arch}.rpm",
        sourcerpm=f"{name}-{version}-{release}.src.rpm",
    )


class TestAlignCrossArchVersions:
    """Test suite for _align_cross_arch_versions method."""

    def setup_method(self):
        self.mock_repos = Mock()
        self.generator = RPMLockfileGenerator(self.mock_repos)

    def test_no_mismatches_returns_unchanged(self):
        """When all arches have the same latest version, input is returned as-is."""
        rpms_info_by_arch = {
            "x86_64": [
                _make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="x86_64", repoid="baseos-x86"),
            ],
            "aarch64": [
                _make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64", repoid="baseos-aarch64"),
            ],
        }
        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})
        assert result is rpms_info_by_arch

    def test_single_arch_returns_unchanged(self):
        """Single-arch scenarios have nothing to align."""
        rpms_info_by_arch = {
            "x86_64": [
                _make_rpm_info("curl", 0, "7.76.1", "30.el9", arch="x86_64"),
            ],
        }
        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})
        assert result is rpms_info_by_arch

    def test_empty_rpm_lists_returns_unchanged(self):
        """Empty RPM lists don't cause errors."""
        rpms_info_by_arch = {"x86_64": [], "aarch64": []}
        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})
        assert result is rpms_info_by_arch

    def test_arch_specific_package_not_aligned(self):
        """Packages existing on only one arch are left alone."""
        rpms_info_by_arch = {
            "x86_64": [
                _make_rpm_info("x86-only-driver", 0, "1.0", "1.el9", arch="x86_64"),
            ],
            "aarch64": [
                _make_rpm_info("arm-specific-tool", 0, "2.0", "1.el9", arch="aarch64"),
            ],
        }
        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})
        assert result is rpms_info_by_arch

    def test_basic_alignment_downgrades_higher_arch(self):
        """When x86_64 has v2 and aarch64 has v1, x86_64 should be downgraded to v1."""
        # x86_64 resolved to release 30.el9, aarch64 resolved to release 29.el9
        x86_rpm_v2 = _make_rpm_info("curl", 0, "7.76.1", "30.el9", arch="x86_64", repoid="baseos-x86")
        aarch64_rpm_v1 = _make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64", repoid="baseos-aarch64")

        rpms_info_by_arch = {
            "x86_64": [x86_rpm_v2],
            "aarch64": [aarch64_rpm_v1],
        }

        # Set up loaded repos so x86_64 has both v1 and v2
        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
            _make_rpm("curl", 0, "7.76.1", "30.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-x86"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        # x86_64 should now have the v1 (29.el9) version
        assert len(result["x86_64"]) == 1
        assert result["x86_64"][0].evr == "0:7.76.1-29.el9"
        # aarch64 should be unchanged
        assert len(result["aarch64"]) == 1
        assert result["aarch64"][0].evr == "0:7.76.1-29.el9"

    def test_alignment_preserves_pinned_entries(self):
        """NVR-pinned entries (lower versions alongside latest) are not removed."""
        # x86_64 has pinned v1 + latest v3; aarch64 has pinned v1 + latest v2
        x86_pinned = _make_rpm_info("rust-toolset", 0, "1.84.1", "1.el9", arch="x86_64", repoid="repo1")
        x86_latest = _make_rpm_info("rust-toolset", 0, "1.86.0", "1.el9", arch="x86_64", repoid="repo1")
        aarch64_pinned = _make_rpm_info("rust-toolset", 0, "1.84.1", "1.el9", arch="aarch64", repoid="repo1")
        aarch64_latest = _make_rpm_info("rust-toolset", 0, "1.85.0", "1.el9", arch="aarch64", repoid="repo1")

        rpms_info_by_arch = {
            "x86_64": [x86_pinned, x86_latest],
            "aarch64": [aarch64_pinned, aarch64_latest],
        }

        # x86_64 repos have the common version (1.85.0) available
        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("rust-toolset", 0, "1.84.1", "1.el9", arch="x86_64"),
            _make_rpm("rust-toolset", 0, "1.85.0", "1.el9", arch="x86_64"),
            _make_rpm("rust-toolset", 0, "1.86.0", "1.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "repo1-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        x86_evrs = {r.evr for r in result["x86_64"]}
        # Pinned version (1.84.1) should still be present
        assert "0:1.84.1-1.el9" in x86_evrs
        # Latest should be downgraded from 1.86.0 to 1.85.0
        assert "0:1.85.0-1.el9" in x86_evrs
        assert "0:1.86.0-1.el9" not in x86_evrs
        # aarch64 should be unchanged
        aarch64_evrs = {r.evr for r in result["aarch64"]}
        assert "0:1.84.1-1.el9" in aarch64_evrs
        assert "0:1.85.0-1.el9" in aarch64_evrs

    def test_alignment_multiple_packages(self):
        """Multiple packages with mismatches are all aligned."""
        rpms_info_by_arch = {
            "x86_64": [
                _make_rpm_info("curl", 0, "7.76.1", "30.el9", arch="x86_64"),
                _make_rpm_info("openssl-libs", 1, "3.0.7", "27.el9", arch="x86_64"),
            ],
            "aarch64": [
                _make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64"),
                _make_rpm_info("openssl-libs", 1, "3.0.7", "25.el9", arch="aarch64"),
            ],
        }

        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
            _make_rpm("curl", 0, "7.76.1", "30.el9", arch="x86_64"),
            _make_rpm("openssl-libs", 1, "3.0.7", "25.el9", arch="x86_64"),
            _make_rpm("openssl-libs", 1, "3.0.7", "27.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        x86_evrs = {r.name: r.evr for r in result["x86_64"]}
        assert x86_evrs["curl"] == "0:7.76.1-29.el9"
        assert x86_evrs["openssl-libs"] == "1:3.0.7-25.el9"

    def test_alignment_three_arches(self):
        """Alignment works across three architectures."""
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("audit-libs", 0, "3.1.5", "6.el9", arch="x86_64")],
            "aarch64": [_make_rpm_info("audit-libs", 0, "3.1.5", "4.el9", arch="aarch64")],
            "s390x": [_make_rpm_info("audit-libs", 0, "3.1.2", "2.el9", arch="s390x")],
        }

        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("audit-libs", 0, "3.1.2", "2.el9", arch="x86_64"),
            _make_rpm("audit-libs", 0, "3.1.5", "4.el9", arch="x86_64"),
            _make_rpm("audit-libs", 0, "3.1.5", "6.el9", arch="x86_64"),
        ]
        aarch64_repo = MagicMock()
        aarch64_repo.primary_rpms = [
            _make_rpm("audit-libs", 0, "3.1.2", "2.el9", arch="aarch64"),
            _make_rpm("audit-libs", 0, "3.1.5", "4.el9", arch="aarch64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": aarch64_repo,
            "repo1-s390x": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        # All arches should converge to 3.1.2-2.el9 (the lowest latest)
        for arch in ["x86_64", "aarch64", "s390x"]:
            assert len(result[arch]) == 1
            assert result[arch][0].evr == "0:3.1.2-2.el9", f"Failed for arch {arch}"

    def test_alignment_logs_downgrade_info(self, caplog):
        """Downgrade events are logged at INFO level."""
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("curl", 0, "7.76.1", "30.el9", arch="x86_64")],
            "aarch64": [_make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64")],
        }

        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
            _make_rpm("curl", 0, "7.76.1", "30.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        with caplog.at_level(logging.INFO):
            self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        assert "Cross-arch alignment" in caplog.text
        assert "curl" in caplog.text
        assert "x86_64" in caplog.text
        assert "downgraded" in caplog.text

    def test_alignment_warns_when_target_version_not_found(self, caplog):
        """When the target version isn't in loaded repos for any arch, the package is skipped entirely."""
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("curl", 0, "7.76.1", "30.el9", arch="x86_64")],
            "aarch64": [_make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64")],
        }

        # x86_64 repo does NOT have the target version (29.el9)
        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "30.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        with caplog.at_level(logging.WARNING):
            result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        assert "cannot find" in caplog.text
        assert "skipping alignment for this package" in caplog.text
        # Both arches retain original versions — package skipped entirely
        assert result["x86_64"][0].evr == "0:7.76.1-30.el9"
        assert result["aarch64"][0].evr == "0:7.76.1-29.el9"

    def test_alignment_with_epoch_differences(self):
        """Alignment works correctly when epochs differ."""
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("pkg", 2, "1.5.0", "10.el9", arch="x86_64")],
            "aarch64": [_make_rpm_info("pkg", 1, "2.0.0", "5.el9", arch="aarch64")],
        }

        # epoch 2 > epoch 1 for RPM comparison, so x86_64 has the higher version
        # target should be aarch64's version (1:2.0.0-5.el9)
        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("pkg", 1, "2.0.0", "5.el9", arch="x86_64"),
            _make_rpm("pkg", 2, "1.5.0", "10.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        assert result["x86_64"][0].evr == "1:2.0.0-5.el9"
        assert result["aarch64"][0].evr == "1:2.0.0-5.el9"

    def test_alignment_mixed_matched_and_mismatched_packages(self):
        """Only mismatched packages are aligned; matched ones stay unchanged."""
        rpms_info_by_arch = {
            "x86_64": [
                _make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
                _make_rpm_info("openssl-libs", 1, "3.0.7", "27.el9", arch="x86_64"),
            ],
            "aarch64": [
                _make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64"),  # Same version
                _make_rpm_info("openssl-libs", 1, "3.0.7", "25.el9", arch="aarch64"),  # Different
            ],
        }

        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("openssl-libs", 1, "3.0.7", "25.el9", arch="x86_64"),
            _make_rpm("openssl-libs", 1, "3.0.7", "27.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        x86_by_name = {r.name: r for r in result["x86_64"]}
        # curl was already matching — should be unchanged
        assert x86_by_name["curl"].evr == "0:7.76.1-29.el9"
        # openssl-libs should be downgraded
        assert x86_by_name["openssl-libs"].evr == "1:3.0.7-25.el9"

    def test_alignment_finds_version_from_correct_repo(self):
        """The replacement RpmInfo should come from the right repo with correct content_set."""
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("curl", 0, "7.76.1", "30.el9", arch="x86_64", repoid="baseos-x86")],
            "aarch64": [_make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64", repoid="baseos-aarch64")],
        }

        baseos_repo = MagicMock()
        baseos_repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "rhel-9-for-x86_64-baseos-rpms"
        mock_repo_obj.baseurl.return_value = "https://cdn.redhat.com/baseos/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "rhel-9-baseos-rpms-x86_64": baseos_repo,
            "rhel-9-baseos-rpms-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"rhel-9-baseos-rpms": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"rhel-9-baseos-rpms"})

        aligned_rpm = result["x86_64"][0]
        assert aligned_rpm.evr == "0:7.76.1-29.el9"
        assert aligned_rpm.repoid == "rhel-9-for-x86_64-baseos-rpms"
        assert "cdn.redhat.com" in aligned_rpm.url

    def test_alignment_prefers_rhel_over_rhocp_repo(self):
        """When the target version is in both RHEL and rhocp repos, RHEL wins."""
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("curl", 0, "7.76.1", "30.el9", arch="x86_64")],
            "aarch64": [_make_rpm_info("curl", 0, "7.76.1", "29.el9", arch="aarch64")],
        }

        baseos_repo = MagicMock()
        baseos_repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
        ]
        rhocp_repo = MagicMock()
        rhocp_repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
        ]

        baseos_repo_obj = MagicMock()
        baseos_repo_obj.content_set.return_value = "baseos-cs"
        baseos_repo_obj.baseurl.return_value = "https://baseos.example.com/"

        rhocp_repo_obj = MagicMock()
        rhocp_repo_obj.content_set.return_value = "rhocp-cs"
        rhocp_repo_obj.baseurl.return_value = "https://rhocp.example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "rhel-9-baseos-rpms-x86_64": baseos_repo,
            "rhocp-4.16-for-rhel-9-rpms-x86_64": rhocp_repo,
            "rhel-9-baseos-rpms-aarch64": MagicMock(primary_rpms=[]),
            "rhocp-4.16-for-rhel-9-rpms-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {
            "rhel-9-baseos-rpms": baseos_repo_obj,
            "rhocp-4.16-for-rhel-9-rpms": rhocp_repo_obj,
        }

        result = self.generator._align_cross_arch_versions(
            rpms_info_by_arch, {"rhel-9-baseos-rpms", "rhocp-4.16-for-rhel-9-rpms"}
        )

        # Should pick from baseos (RHEL), not rhocp
        aligned_rpm = result["x86_64"][0]
        assert aligned_rpm.repoid == "baseos-cs"

    def test_alignment_noarch_package_found(self):
        """noarch packages from repos should be found during alignment."""
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("python-setuptools", 0, "70.0", "2.el9", arch="x86_64")],
            "aarch64": [_make_rpm_info("python-setuptools", 0, "69.5", "1.el9", arch="aarch64")],
        }

        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("python-setuptools", 0, "69.5", "1.el9", arch="noarch"),
            _make_rpm("python-setuptools", 0, "70.0", "2.el9", arch="noarch"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        assert result["x86_64"][0].evr == "0:69.5-1.el9"

    def test_alignment_deduplicates_pinned_and_replaced(self):
        """When a pinned entry already holds the target EVR, no duplicate should appear after alignment."""
        # x86_64 has pinned v1 (the target) AND latest v2; aarch64 has only v1.
        # After alignment the latest on x86_64 gets replaced with v1, which
        # would duplicate the pinned v1 unless deduplication fires.
        x86_pinned = _make_rpm_info("rust-toolset", 0, "1.84.1", "1.el9", arch="x86_64", repoid="repo1")
        x86_latest = _make_rpm_info("rust-toolset", 0, "1.85.0", "1.el9", arch="x86_64", repoid="repo1")
        aarch64_latest = _make_rpm_info("rust-toolset", 0, "1.84.1", "1.el9", arch="aarch64", repoid="repo1")

        rpms_info_by_arch = {
            "x86_64": [x86_pinned, x86_latest],
            "aarch64": [aarch64_latest],
        }

        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("rust-toolset", 0, "1.84.1", "1.el9", arch="x86_64"),
            _make_rpm("rust-toolset", 0, "1.85.0", "1.el9", arch="x86_64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "repo1-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        # x86_64 should have exactly ONE entry for rust-toolset, not two
        x86_result = result["x86_64"]
        assert len(x86_result) == 1, f"Expected 1 entry but got {len(x86_result)}: {[r.evr for r in x86_result]}"
        assert x86_result[0].evr == "0:1.84.1-1.el9"
        # aarch64 should be unchanged
        assert len(result["aarch64"]) == 1
        assert result["aarch64"][0].evr == "0:1.84.1-1.el9"

    def test_alignment_skips_package_when_any_arch_missing_target(self):
        """When one arch can't resolve the target EVR, the entire package is skipped (pre-validation)."""
        # x86_64 has v3, aarch64 has v2, s390x has v1 (the target).
        # But the target v1 is NOT available in x86_64's repos.
        rpms_info_by_arch = {
            "x86_64": [_make_rpm_info("pkg", 0, "3.0", "1.el9", arch="x86_64")],
            "aarch64": [_make_rpm_info("pkg", 0, "2.0", "1.el9", arch="aarch64")],
            "s390x": [_make_rpm_info("pkg", 0, "1.0", "1.el9", arch="s390x")],
        }

        # x86_64 repo has v2 and v3 but NOT v1
        x86_repo = MagicMock()
        x86_repo.primary_rpms = [
            _make_rpm("pkg", 0, "2.0", "1.el9", arch="x86_64"),
            _make_rpm("pkg", 0, "3.0", "1.el9", arch="x86_64"),
        ]
        # aarch64 repo has v1 and v2
        aarch64_repo = MagicMock()
        aarch64_repo.primary_rpms = [
            _make_rpm("pkg", 0, "1.0", "1.el9", arch="aarch64"),
            _make_rpm("pkg", 0, "2.0", "1.el9", arch="aarch64"),
        ]

        mock_repo_obj = MagicMock()
        mock_repo_obj.content_set.return_value = "baseos-cs"
        mock_repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {
            "repo1-x86_64": x86_repo,
            "repo1-aarch64": aarch64_repo,
            "repo1-s390x": MagicMock(primary_rpms=[]),
        }
        self.generator.builder.repos._repos = {"repo1": mock_repo_obj}

        result = self.generator._align_cross_arch_versions(rpms_info_by_arch, {"repo1"})

        # All arches should keep their original versions since the package was skipped
        assert result["x86_64"][0].evr == "0:3.0-1.el9"
        assert result["aarch64"][0].evr == "0:2.0-1.el9"
        assert result["s390x"][0].evr == "0:1.0-1.el9"


class TestFindRpmByEvrInRepos:
    """Test suite for _find_rpm_by_evr_in_repos helper method."""

    def setup_method(self):
        self.mock_repos = Mock()
        self.generator = RPMLockfileGenerator(self.mock_repos)

    def test_find_existing_rpm(self):
        """Should find an RPM matching the exact name and EVR."""
        repo = MagicMock()
        repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="x86_64"),
        ]

        repo_obj = MagicMock()
        repo_obj.content_set.return_value = "baseos-cs"
        repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {"repo1-x86_64": repo}
        self.generator.builder.repos._repos = {"repo1": repo_obj}

        result = self.generator._find_rpm_by_evr_in_repos("curl", "0:7.76.1-29.el9", "x86_64", {"repo1"})

        assert result is not None
        assert result.name == "curl"
        assert result.evr == "0:7.76.1-29.el9"
        assert result.repoid == "baseos-cs"

    def test_returns_none_when_not_found(self):
        """Should return None when no matching RPM exists."""
        repo = MagicMock()
        repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "30.el9", arch="x86_64"),
        ]

        repo_obj = MagicMock()
        repo_obj.content_set.return_value = "baseos-cs"
        repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {"repo1-x86_64": repo}
        self.generator.builder.repos._repos = {"repo1": repo_obj}

        result = self.generator._find_rpm_by_evr_in_repos("curl", "0:7.76.1-29.el9", "x86_64", {"repo1"})

        assert result is None

    def test_returns_none_when_no_repos_loaded(self):
        """Should return None when loaded repos are empty."""
        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {}
        self.generator.builder.repos._repos = {}

        result = self.generator._find_rpm_by_evr_in_repos("curl", "0:7.76.1-29.el9", "x86_64", {"repo1"})

        assert result is None

    def test_skips_wrong_arch_rpms(self):
        """Should not match RPMs that are for a different architecture."""
        repo = MagicMock()
        repo.primary_rpms = [
            _make_rpm("curl", 0, "7.76.1", "29.el9", arch="aarch64"),
        ]

        repo_obj = MagicMock()
        repo_obj.content_set.return_value = "baseos-cs"
        repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {"repo1-x86_64": repo}
        self.generator.builder.repos._repos = {"repo1": repo_obj}

        result = self.generator._find_rpm_by_evr_in_repos("curl", "0:7.76.1-29.el9", "x86_64", {"repo1"})

        assert result is None

    def test_matches_noarch_packages(self):
        """noarch RPMs should match regardless of requested arch."""
        repo = MagicMock()
        repo.primary_rpms = [
            _make_rpm("python-setuptools", 0, "69.5", "1.el9", arch="noarch"),
        ]

        repo_obj = MagicMock()
        repo_obj.content_set.return_value = "baseos-cs"
        repo_obj.baseurl.return_value = "https://example.com/"

        self.generator.builder = MagicMock()
        self.generator.builder.loaded_repos = {"repo1-x86_64": repo}
        self.generator.builder.repos._repos = {"repo1": repo_obj}

        result = self.generator._find_rpm_by_evr_in_repos("python-setuptools", "0:69.5-1.el9", "x86_64", {"repo1"})

        assert result is not None
        assert result.evr == "0:69.5-1.el9"
