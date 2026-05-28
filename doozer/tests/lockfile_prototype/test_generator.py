"""
Tests for doozerlib.lockfile_prototype.generator (orchestration).
"""

import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import yaml
from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.generator import (
    RpmLockfilePrototypeGenerator,
    build_rpms_in_yaml,
)
from doozerlib.lockfile_prototype.models import (
    ArchResult,
    LockfileData,
    PackageEntry,
    RepoEntry,
    RpmsInConfig,
)
from doozerlib.lockfile_prototype.resolver import RpmResolver


class TestBuildRpmsInYaml(unittest.TestCase):
    def test_basic_structure(self):
        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            ),
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64", "ppc64le"],
            packages=["nfs-utils", "jq"],
        )
        self.assertIsInstance(result, RpmsInConfig)
        self.assertEqual(result.arches, ["x86_64", "ppc64le"])
        self.assertEqual(len(result.contentOrigin["repos"]), 1)
        self.assertEqual(result.contentOrigin["repos"][0].repoid, "rhel-9-baseos-rpms")
        self.assertEqual(result.packages, ["nfs-utils", "jq"])

    def test_arch_specific_packages(self):
        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            ),
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64", "ppc64le"],
            packages=["nfs-utils"],
            arch_specific_packages={"ppc64le": ["librtas"]},
        )
        self.assertIn("nfs-utils", result.packages)
        arch_entries = [p for p in result.packages if not isinstance(p, str)]
        self.assertEqual(len(arch_entries), 1)
        self.assertEqual(arch_entries[0].name, "librtas")
        self.assertEqual(arch_entries[0].arches["only"], "ppc64le")

    def test_multiple_repos(self):
        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            ),
            RepoEntry(
                repoid="rhel-9-appstream-rpms",
                baseurl="https://example.com/appstream/$basearch/os/",
            ),
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64"],
            packages=["httpd"],
        )
        self.assertEqual(len(result.contentOrigin["repos"]), 2)
        repoids = [r.repoid for r in result.contentOrigin["repos"]]
        self.assertEqual(repoids, ["rhel-9-baseos-rpms", "rhel-9-appstream-rpms"])

    def test_repo_options_flattened_in_serialization(self):
        """
        RepoEntry options must be flattened into the top-level dict
        during serialization so rpm-lockfile-prototype receives them
        as direct repo attributes (e.g. includepkgs, module_hotfixes).
        """
        repos = [
            RepoEntry(
                repoid="rhel-9-golang-rpms",
                baseurl="https://example.com/golang/$basearch/os/",
                options={"includepkgs": "golang*", "module_hotfixes": 1},
            ),
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64"],
            packages=["golang"],
        )
        dumped = result.model_dump(exclude_none=True)
        repo_dict = dumped["contentOrigin"]["repos"][0]
        self.assertEqual(repo_dict["repoid"], "rhel-9-golang-rpms")
        self.assertEqual(repo_dict["includepkgs"], "golang*")
        self.assertEqual(repo_dict["module_hotfixes"], 1)
        self.assertNotIn("options", repo_dict)


FAKE_LOCKFILE_DATA = LockfileData(
    lockfileVersion=1,
    lockfileVendor="redhat",
    arches=[
        ArchResult(
            arch="x86_64",
            packages=[
                PackageEntry(
                    url="https://example.com/nfs-utils-2.5.4-1.el9.x86_64.rpm",
                    repoid="rhel-9-baseos-rpms",
                    name="nfs-utils",
                    evr="2.5.4-1.el9",
                )
            ],
            source=[],
            module_metadata=[],
        )
    ],
)


class TestRpmLockfilePrototypeGenerator(unittest.TestCase):
    def _make_mock_repo(self, name: str, baseurl: str, content_set_name: str | None = None) -> MagicMock:
        repo = MagicMock()
        repo.name = name
        repo.baseurl.return_value = baseurl
        repo.content_set.return_value = content_set_name
        return repo

    def _make_mock_repos(self) -> MagicMock:
        repos = MagicMock()
        baseos = self._make_mock_repo(
            "rhel-9-baseos-rpms",
            "https://example.com/baseos/x86_64/os/",
            content_set_name="rhel-9-for-x86_64-baseos-rpms",
        )
        appstream = self._make_mock_repo(
            "rhel-9-appstream-rpms",
            "https://example.com/appstream/x86_64/os/",
            content_set_name="rhel-9-for-x86_64-appstream-rpms",
        )
        repo_map = {
            "rhel-9-baseos-rpms": baseos,
            "rhel-9-appstream-rpms": appstream,
        }
        repos.__getitem__ = lambda self_repos, key: repo_map[key]
        return repos

    def _make_mock_image_meta(self) -> MagicMock:
        meta = MagicMock()
        meta.distgit_key = "csi-driver-nfs"
        meta.get_arches.return_value = ["x86_64", "ppc64le"]
        meta.get_enabled_repos.return_value = {"rhel-9-baseos-rpms", "rhel-9-appstream-rpms"}
        meta.is_lockfile_generation_enabled.return_value = True

        lockfile_config = MagicMock()
        lockfile_config.get.return_value = ["keyutils"]
        meta.config.konflux.cachi2.lockfile = lockfile_config

        return meta

    def _make_mock_container(self) -> MagicMock:
        container = MagicMock(spec=ContainerImageHelper)
        container.resolve_to_digest = AsyncMock(side_effect=lambda p: p)
        container.get_installed_packages = AsyncMock(return_value=[])
        container.read_file_from_image = AsyncMock(return_value="")
        return container

    def _make_mock_resolver(self) -> MagicMock:
        resolver = MagicMock(spec=RpmResolver)
        resolver.resolve = AsyncMock(return_value=FAKE_LOCKFILE_DATA.model_copy(deep=True))
        return resolver

    def _make_generator(self) -> RpmLockfilePrototypeGenerator:
        return RpmLockfilePrototypeGenerator(
            repos=self._make_mock_repos(),
            container_helper=self._make_mock_container(),
            resolver=self._make_mock_resolver(),
        )

    def test_generate_lockfile_writes_result(self):
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils jq\n")

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

            self.assertTrue((dest_dir / "rpms.lock.yaml").exists())
        generator._resolver.resolve.assert_called_once()

    def test_generate_lockfile_no_temp_files_in_dest(self):
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils\n")

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

            in_files = list(dest_dir.glob("*.in.yaml"))
            stage_lock_files = list(dest_dir.glob("*.stage*.lock.yaml"))
            self.assertEqual(in_files, [])
            self.assertEqual(stage_lock_files, [])

    def test_generate_lockfile_fails_on_resolution_error(self):
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._resolver.resolve = AsyncMock(side_effect=RuntimeError("DNF dependency resolution failed"))

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils\n")

            with self.assertRaises(RuntimeError):
                asyncio.run(generator.generate_lockfile(meta, dest_dir))

    def test_generate_lockfile_skips_when_disabled(self):
        meta = self._make_mock_image_meta()
        meta.is_lockfile_generation_enabled.return_value = False
        generator = self._make_generator()

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            asyncio.run(generator.generate_lockfile(meta, dest_dir))
            self.assertFalse((dest_dir / "rpms.lock.yaml").exists())

    def test_generate_lockfile_writes_empty_when_no_packages(self):
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = []
        generator = self._make_generator()

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN echo hello\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))
            lockfile_path = dest_dir / "rpms.lock.yaml"
            self.assertTrue(lockfile_path.exists())
            with open(lockfile_path) as f:
                data = yaml.safe_load(f)
            self.assertEqual(data["lockfileVersion"], 1)
            self.assertEqual(data["arches"], [])

    def test_stage_alias_uses_bare_mode(self):
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = [
            "quay.io/test/builder@sha256:abc123",
            "build",
        ]

        captured_pullspecs: list[str | None] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_pullspecs.append(image_pullspec)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM quay.io/test/builder AS build\n"
                "RUN dnf install -y gcc\n"
                "\n"
                "FROM build\n"
                "RUN dnf install -y nfs-utils\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_pullspecs), 2)
        self.assertEqual(captured_pullspecs[0], "quay.io/test/builder@sha256:abc123")
        self.assertIsNone(captured_pullspecs[1])

    def test_builder_stage_skips_reinstall_when_final_stage_has_no_installs(self):
        """
        Multi-stage Dockerfile where only the builder stage installs
        packages (e.g. thanos). reinstallPackages should NOT be added
        to the builder stage — only the actual final Dockerfile stage
        should get reinstallPackages.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = [
            "quay.io/test/golang-builder@sha256:abc123",
            "quay.io/test/base@sha256:def456",
        ]
        generator._container.get_installed_packages = AsyncMock(return_value=["bash", "gcc", "golang", "glibc"])

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM golang-builder AS builder\n"
                "RUN yum install -y prometheus-promu\n"
                "\n"
                "FROM base-rhel9\n"
                "COPY --from=builder /bin/thanos /bin/thanos\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_configs), 1)
        # Builder stage should NOT have reinstallPackages
        self.assertEqual(captured_configs[0].reinstallPackages, [])
        # get_installed_packages should NOT be called for non-final stage
        generator._container.get_installed_packages.assert_not_called()

    def test_update_only_queries_base_image(self):
        """
        Update-only stages should pass installed packages as upgrade
        targets so DNF uses upgrade semantics and respects dependency
        constraints.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._container.get_installed_packages = AsyncMock(return_value=["bash", "coreutils", "glibc"])

        captured_configs: list[RpmsInConfig] = []
        captured_pullspecs: list[str | None] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            captured_pullspecs.append(image_pullspec)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum update -y && yum clean all\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        # Called for update-only stage + reinstall expansion
        self.assertGreaterEqual(generator._container.get_installed_packages.call_count, 1)
        self.assertEqual(len(captured_configs), 1)
        # Update-only stage uses --image mode with base image pullspec
        self.assertIsNotNone(captured_pullspecs[0])
        # All installed packages appear as both install and upgrade targets
        self.assertEqual(
            sorted(captured_configs[0].packages),
            ["bash", "coreutils", "glibc"],
        )
        self.assertEqual(sorted(captured_configs[0].upgradePackages), ["bash", "coreutils", "glibc"])

    def test_mixed_install_and_bare_update_uses_image_mode(self):
        """
        Stage with both yum install and bare yum update -y should resolve
        in --image mode with upgradePackages=["*"] so base image packages
        get upgraded to match build-time behavior.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        captured_configs: list[RpmsInConfig] = []
        captured_pullspecs: list[str | None] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            captured_pullspecs.append(image_pullspec)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN yum update -y && yum install -y aws-efs-utils && yum clean all\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_configs), 1)
        # --image mode: pullspec is preserved
        self.assertIsNotNone(captured_pullspecs[0])
        # Explicit install packages in the packages list
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        self.assertIn("aws-efs-utils", pkg_names)

    def test_update_only_no_image_skips(self):
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = []

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum update -y && yum clean all\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        generator._resolver.resolve.assert_not_called()

    def test_resolve_cat_packages_from_base_image(self):
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = []
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        async def mock_read_file(pullspec, filepath):
            if filepath == "/more-pkgs":
                return '"openvswitch3.5-devel" "openvswitch3.5-ipsec" "ovn25.09-vtep"'
            return ""

        generator._container.read_file_from_image = AsyncMock(side_effect=mock_read_file)

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN dnf install -y openssl && \\\n    eval \"dnf install -y $(cat /more-pkgs)\"\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        generator._container.read_file_from_image.assert_called_once()
        self.assertEqual(len(captured_configs), 1)
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        self.assertIn("openssl", pkg_names)
        self.assertIn("openvswitch3.5-devel", pkg_names)
        self.assertIn("openvswitch3.5-ipsec", pkg_names)
        self.assertIn("ovn25.09-vtep", pkg_names)

    def test_final_stage_uses_image_mode_when_pullspec_available(self):
        """
        For the final stage with a base image pullspec, resolution uses
        --image mode so DNF sees the base image's rpmdb. This ensures
        lockfile versions match build-time behavior.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        captured_configs: list[RpmsInConfig] = []
        captured_pullspecs: list[str | None] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            captured_pullspecs.append(image_pullspec)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf install -y libreswan openssl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_configs), 1)
        # --image mode: pullspec is preserved (not forced to None)
        self.assertIsNotNone(captured_pullspecs[0])
        # Only Dockerfile packages — base image packages come from rpmdb
        pkg_names = sorted(p if isinstance(p, str) else p.name for p in captured_configs[0].packages)
        self.assertIn("libreswan", pkg_names)
        self.assertIn("openssl", pkg_names)

    def test_bare_update_keeps_image_mode_without_upgrade_targets(self):
        """
        Bare yum/dnf update with --image mode should NOT expand base
        image packages as upgrade targets (many are virtual provides or
        renamed and cause resolution failures). Instead, dnf update is
        kept in the Dockerfile and runs at build time with cachi2 RPMs.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()

        captured_configs: list[RpmsInConfig] = []
        captured_pullspecs: list[str | None] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            captured_pullspecs.append(image_pullspec)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum update -y && dnf install -y libreswan\n")
            asyncio.run(
                generator.generate_lockfile(meta, dest_dir, downstream_parents=["quay.io/test/base@sha256:abc123"])
            )

        self.assertEqual(len(captured_configs), 1)
        # --image mode preserved
        self.assertIsNotNone(captured_pullspecs[0])
        # No upgrade targets — dnf update handled at build time
        self.assertEqual(captured_configs[0].upgradePackages, [])

    def test_reinstall_packages_also_passed_as_upgrade_targets(self):
        """
        Base image packages passed as reinstallPackages must also appear
        in upgradePackages so rpm-lockfile-prototype skips
        PackagesNotAvailableError (installed version not in repos) instead
        of crashing.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._container.get_installed_packages = AsyncMock(return_value=["sed", "glibc", "libnghttp2"])

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf install -y curl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_configs), 1)
        reinstall = captured_configs[0].reinstallPackages
        upgrade = captured_configs[0].upgradePackages
        # All reinstall packages must also be in upgrade targets
        for pkg in reinstall:
            self.assertIn(pkg, upgrade)
        # Dockerfile install packages are NOT in reinstall or upgrade
        self.assertNotIn("curl", reinstall)

    def test_reinstall_retry_removes_unavailable_packages(self):
        """
        When a reinstall/upgrade package fails resolution (e.g. package
        not found in repos at all), the retry loop must remove it from
        reinstallPackages and upgradePackages before retrying.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._container.get_installed_packages = AsyncMock(return_value=["nonexistent-pkg", "glibc"])

        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("No match for argument: nonexistent-pkg")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf install -y curl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        # Should have retried successfully
        self.assertEqual(call_count, 2)
        # Second call should not have "nonexistent-pkg" in reinstall or upgrade
        second_config = generator._resolver.resolve.call_args_list[1][0][0]
        self.assertNotIn("nonexistent-pkg", second_config.reinstallPackages)
        self.assertNotIn("nonexistent-pkg", second_config.upgradePackages)
        # "glibc" should still be present
        self.assertIn("glibc", second_config.reinstallPackages)
        self.assertIn("glibc", second_config.upgradePackages)

    def test_fallback_packages_used_when_image_unreachable(self):
        """
        When base image is unreachable but fallback_installed has data
        from parent lockfile, conflict detection should use the fallback
        packages instead of skipping.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["registry.redhat.io/openshift/art-images-base:unreachable-tag"]
        # Image not reachable: resolve_to_digest returns same pullspec (no digest)
        generator._container.resolve_to_digest = AsyncMock(
            return_value="registry.redhat.io/openshift/art-images-base:unreachable-tag"
        )
        # Provide fallback from parent lockfile

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf install -y libreswan\n")
            asyncio.run(
                generator.generate_lockfile(
                    meta, dest_dir, fallback_installed={0: ["glibc", "gnutls", "crypto-policies"]}
                )
            )

        self.assertEqual(len(captured_configs), 1)
        # Fallback packages should be included for conflict detection
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        self.assertIn("libreswan", pkg_names)
        self.assertIn("glibc", pkg_names)
        self.assertIn("gnutls", pkg_names)
        self.assertIn("crypto-policies", pkg_names)
        # get_installed_packages should NOT be called (image unreachable,
        # fallback used instead)
        generator._container.get_installed_packages.assert_not_called()

    def test_cat_file_resolved_from_parent_dockerfile_heredoc(self):
        """
        When base image is unreachable and $(cat /filepath) can't read
        from the image, fall back to parsing the parent's Dockerfile
        for RUN commands that generate the file via here-string + sed.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        # Image unreachable
        generator._container.resolve_to_digest = AsyncMock(return_value="registry.redhat.io/base:unreachable-tag")
        generator._container.read_file_from_image = AsyncMock(return_value="")

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            # Child Dockerfile uses $(cat /more-pkgs)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN dnf install -y openssl && eval \"dnf install -y $(cat /more-pkgs)\"\n"
            )
            # Parent build dir with Dockerfile.base that generates /more-pkgs
            parent_dir = Path(tmpdir) / "parent"
            parent_dir.mkdir()
            (parent_dir / "Dockerfile.base").write_text(
                "FROM ubi9\n"
                "ARG ovsver=3.5\n"
                "ARG ovnver=25.09\n"
                'RUN dnf install -y "openvswitch$ovsver" && \\\n'
                '    sed \'s/%/"/g\' <<<"%openvswitch$ovsver-devel% %ovn$ovnver-vtep%" > /more-pkgs\n'
            )

            asyncio.run(
                generator.generate_lockfile(
                    meta,
                    dest_dir,
                    downstream_parents=["registry.redhat.io/base:unreachable-tag"],
                    parent_source_dirs={0: parent_dir},
                )
            )

        self.assertEqual(len(captured_configs), 1)
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        self.assertIn("openssl", pkg_names)
        self.assertIn("openvswitch3.5-devel", pkg_names)
        self.assertIn("ovn25.09-vtep", pkg_names)

    def test_retry_on_missing_packages(self):
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("No match for argument: dmidecode")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils dmidecode\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

            self.assertEqual(generator._resolver.resolve.call_count, 2)
            self.assertTrue((dest_dir / "rpms.lock.yaml").exists())

    def test_resolve_stage_with_retry_no_upgrade_packages_when_bare(self):
        """
        When image_pullspec is None (bare/final stage), upgrade_packages must
        not be passed to build_rpms_in_yaml even if update_targets is non-empty.
        Passing upgrade_packages with --bare causes dnf.exceptions.PackagesNotInstalledError
        because the installed sack is empty.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]
        update_targets = ["bash", "glibc", "coreutils"]

        asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64"],
                packages=["nfs-utils"] + update_targets,
                arch_pkgs={},
                update_targets=update_targets,
                image_pullspec=None,
                distgit_key="test-image",
                stage_num=0,
            )
        )

        self.assertEqual(len(captured_configs), 1)
        # upgrade_packages must be empty (not passed) when image_pullspec is None
        self.assertEqual(
            captured_configs[0].upgradePackages,
            [],
            "upgrade_packages must be None/empty when image_pullspec is None (bare mode)",
        )

    def test_retry_removes_missing_package_from_upgrade_targets(self):
        """
        When rpm-lockfile-prototype fails with PackagesNotInstalledError
        for a package in upgradePackages, the retry loop must remove it
        from both remaining_packages and remaining_update_targets.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        captured_configs: list[RpmsInConfig] = []
        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            captured_configs.append(config)
            call_count += 1
            if call_count == 1:
                raise RuntimeError(
                    "dnf.exceptions.PackagesNotInstalledError: "
                    "No match for argument: policycoreutils-python-utils: "
                    "policycoreutils-python-utils"
                )
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64"],
                packages=["nfs-utils", "policycoreutils-python-utils"],
                arch_pkgs={},
                update_targets=["policycoreutils-python-utils"],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="test-image",
                stage_num=0,
            )
        )

        self.assertEqual(call_count, 2)
        self.assertNotIn("policycoreutils-python-utils", captured_configs[1].packages)
        self.assertNotIn("policycoreutils-python-utils", captured_configs[1].upgradePackages)

    def test_build_repo_list_keeps_literal_url_for_single_arch_repo(self):
        rt = MagicMock()
        rt.name = "rhel-9-rt-rpms"
        rt.baseurl.return_value = "https://example.com/e4s/rhel9/9.8/x86_64/rt/os/"
        rt.content_set.return_value = "rhel-9-for-x86_64-rt-rpms"

        repo_map = {"rhel-9-rt-rpms": rt}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos)
        result = generator._build_repo_list(enabled_repos={"rhel-9-rt-rpms"}, arches=["x86_64", "aarch64"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].repoid, "rhel-9-for-$basearch-rt-rpms")
        self.assertEqual(result[0].baseurl, "https://example.com/e4s/rhel9/9.8/x86_64/rt/os/")

    def test_templatize_baseurl_replaces_known_arch(self):
        generator = self._make_generator()
        url = "https://rhsm-pulp.corp.stage.redhat.com/content/e4s/rhel9/9.8/x86_64/rt/os/"
        result = generator._templatize_baseurl(url)
        self.assertEqual(result, "https://rhsm-pulp.corp.stage.redhat.com/content/e4s/rhel9/9.8/$basearch/rt/os/")

    def test_templatize_baseurl_no_arch_in_url(self):
        generator = self._make_generator()
        result = generator._templatize_baseurl("https://example.com/content/repo/os/")
        self.assertEqual(result, "https://example.com/content/repo/os/")

    def test_templatize_baseurl_already_has_basearch(self):
        generator = self._make_generator()
        result = generator._templatize_baseurl("https://example.com/baseos/$basearch/os/")
        self.assertEqual(result, "https://example.com/baseos/$basearch/os/")

    def test_build_repo_list_passes_extra_options(self):
        golang = MagicMock()
        golang.name = "rhel-9-golang-rpms"
        golang.baseurl.return_value = (
            "https://download.devel.redhat.com/brewroot/repos/rhaos-5.0-rhel-9-build/latest/x86_64/"
        )
        golang.content_set.return_value = "rhocp-5.0-for-rhel-9-x86_64-rpms"
        golang._data.conf.get.side_effect = lambda key, default=None: (
            {"includepkgs": "module-build-macros golang* goversioninfo", "module_hotfixes": 1}
            if key == "extra_options"
            else default
        )

        repo_map = {"rhel-9-golang-rpms": golang}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos)
        result = generator._build_repo_list(enabled_repos={"rhel-9-golang-rpms"}, arches=["x86_64"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].options["includepkgs"], "module-build-macros golang* goversioninfo")
        self.assertEqual(result[0].options["module_hotfixes"], 1)

    def test_build_repo_list_no_extra_options(self):
        rt = MagicMock()
        rt.name = "rhel-9-rt-rpms"
        rt.baseurl.return_value = "https://example.com/e4s/rhel9/9.8/x86_64/rt/os/"
        rt.content_set.return_value = "rhel-9-for-x86_64-rt-rpms"
        rt._data.conf.get.side_effect = lambda key, default=None: default if key == "extra_options" else default

        repo_map = {"rhel-9-rt-rpms": rt}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos)
        result = generator._build_repo_list(enabled_repos={"rhel-9-rt-rpms"}, arches=["x86_64"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].options, {})

    def test_build_repo_list_templatizes_multi_arch_url(self):
        baseos = MagicMock()
        baseos.name = "rhel-9-baseos-rpms"
        baseos.baseurl.side_effect = lambda repotype="unsigned", arch="x86_64": f"https://example.com/baseos/{arch}/os/"
        baseos.content_set.return_value = "rhel-9-for-x86_64-baseos-rpms"

        repo_map = {"rhel-9-baseos-rpms": baseos}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos)
        result = generator._build_repo_list(enabled_repos={"rhel-9-baseos-rpms"}, arches=["x86_64", "aarch64"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].repoid, "rhel-9-for-$basearch-baseos-rpms")
        self.assertEqual(result[0].baseurl, "https://example.com/baseos/$basearch/os/")


class TestDetermineStagePullspec(unittest.IsolatedAsyncioTestCase):
    """
    Tests for _determine_stage_pullspec fallback when base image is unreachable.
    """

    def _make_generator(self, downstream_parents: list[str]) -> RpmLockfilePrototypeGenerator:
        gen = RpmLockfilePrototypeGenerator.__new__(RpmLockfilePrototypeGenerator)
        gen.logger = MagicMock()
        gen._container = MagicMock()
        gen.downstream_parents = downstream_parents
        return gen

    async def test_reachable_image_resolves_to_digest(self):
        gen = self._make_generator(["registry.redhat.io/openshift/base:v1.0"])
        gen._container.resolve_to_digest = AsyncMock(return_value="registry.redhat.io/openshift/base@sha256:abc123")

        result = await gen._determine_stage_pullspec(0, "test-image")
        self.assertEqual(result, "registry.redhat.io/openshift/base@sha256:abc123")

    async def test_unreachable_image_falls_back_to_bare(self):
        gen = self._make_generator(["registry.redhat.io/openshift/base:v1.0-nonexistent"])
        gen._container.resolve_to_digest = AsyncMock(return_value="registry.redhat.io/openshift/base:v1.0-nonexistent")

        result = await gen._determine_stage_pullspec(0, "test-image")
        self.assertIsNone(result)
        gen.logger.warning.assert_called_once()

    async def test_already_digest_not_rechecked(self):
        pullspec = "registry.redhat.io/openshift/base@sha256:abc123"
        gen = self._make_generator([pullspec])
        gen._container.resolve_to_digest = AsyncMock(return_value=pullspec)

        result = await gen._determine_stage_pullspec(0, "test-image")
        self.assertEqual(result, pullspec)

    async def test_stage_alias_returns_none(self):
        gen = self._make_generator(["builder"])
        result = await gen._determine_stage_pullspec(0, "test-image")
        self.assertIsNone(result)


class TestCrossArchReconciliation(unittest.IsolatedAsyncioTestCase):
    """
    Tests for cross-architecture version reconciliation in
    RpmLockfilePrototypeGenerator.
    """

    def _make_lockfile(self, arch_packages: dict[str, list[tuple[str, str, str]]]) -> LockfileData:
        """
        Build a LockfileData from {arch: [(name, evr, url), ...]}.
        """
        arches = []
        for arch, pkgs in arch_packages.items():
            entries = [PackageEntry(name=name, evr=evr, url=url, repoid="test-repo") for name, evr, url in pkgs]
            arches.append(ArchResult(arch=arch, packages=entries))
        return LockfileData(arches=arches)

    def _make_generator(self) -> RpmLockfilePrototypeGenerator:
        gen = RpmLockfilePrototypeGenerator.__new__(RpmLockfilePrototypeGenerator)
        gen.logger = MagicMock()
        gen._resolver = MagicMock()
        gen._container = MagicMock()
        gen.downstream_parents = []
        return gen

    def test_detect_no_mismatches(self):
        lockfile = self._make_lockfile(
            {
                "x86_64": [("curl", "7.76-1.el9", "https://x86/curl.rpm")],
                "aarch64": [("curl", "7.76-1.el9", "https://arm/curl.rpm")],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_cross_arch_mismatches(lockfile)
        self.assertEqual(result, {})

    def test_detect_mismatches(self):
        lockfile = self._make_lockfile(
            {
                "x86_64": [("libeconf", "0.4.1-7.el9_8", "https://x86/libeconf.rpm")],
                "aarch64": [("libeconf", "0.4.1-5.el9", "https://arm/libeconf.rpm")],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_cross_arch_mismatches(lockfile)
        self.assertIn("libeconf", result)
        self.assertEqual(result["libeconf"]["x86_64"], "0.4.1-7.el9_8")
        self.assertEqual(result["libeconf"]["aarch64"], "0.4.1-5.el9")

    def test_detect_ignores_single_arch(self):
        lockfile = self._make_lockfile(
            {
                "x86_64": [("x86-only-pkg", "1.0-1.el9", "https://x86/pkg.rpm")],
                "aarch64": [("arm-only-pkg", "2.0-1.el9", "https://arm/pkg.rpm")],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_cross_arch_mismatches(lockfile)
        self.assertEqual(result, {})

    def test_compute_version_pins(self):
        mismatches = {
            "libeconf": {"x86_64": "0.4.1-7.el9_8", "aarch64": "0.4.1-5.el9"},
            "curl": {"x86_64": "7.76-2.el9", "s390x": "7.76-1.el9"},
        }
        gen = self._make_generator()
        pins = gen._compute_version_pins(mismatches)
        self.assertEqual(sorted(pins), ["curl-7.76-1.el9", "libeconf-0.4.1-5.el9"])

    async def test_reconciliation_skips_when_consistent(self):
        gen = self._make_generator()
        consistent = self._make_lockfile(
            {
                "x86_64": [("curl", "7.76-1.el9", "https://x86/curl.rpm")],
                "aarch64": [("curl", "7.76-1.el9", "https://arm/curl.rpm")],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(return_value=consistent)

        result = await gen._resolve_with_reconciliation(
            [],
            ["x86_64", "aarch64"],
            ["curl"],
            {},
            [],
            None,
            "test-image",
            0,
        )
        self.assertEqual(result, consistent)
        gen._resolve_stage_with_retry.assert_awaited_once()

    async def test_reconciliation_re_resolves_on_mismatch(self):
        gen = self._make_generator()
        mismatched = self._make_lockfile(
            {
                "x86_64": [("libeconf", "0.4.1-7.el9_8", "https://x86/libeconf-7.rpm")],
                "aarch64": [("libeconf", "0.4.1-5.el9", "https://arm/libeconf-5.rpm")],
            }
        )
        reconciled = self._make_lockfile(
            {
                "x86_64": [("libeconf", "0.4.1-5.el9", "https://x86/libeconf-5.rpm")],
                "aarch64": [("libeconf", "0.4.1-5.el9", "https://arm/libeconf-5.rpm")],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(side_effect=[mismatched, reconciled])

        result = await gen._resolve_with_reconciliation(
            [],
            ["x86_64", "aarch64"],
            ["curl"],
            {},
            [],
            None,
            "test-image",
            0,
        )
        self.assertEqual(result, reconciled)
        self.assertEqual(gen._resolve_stage_with_retry.await_count, 2)

        second_call_packages = gen._resolve_stage_with_retry.call_args_list[1][0][2]
        self.assertIn("libeconf-0.4.1-5.el9", second_call_packages)

    async def test_reconciliation_falls_back_on_error(self):
        gen = self._make_generator()
        mismatched = self._make_lockfile(
            {
                "x86_64": [("libeconf", "0.4.1-7.el9_8", "https://x86/libeconf.rpm")],
                "aarch64": [("libeconf", "0.4.1-5.el9", "https://arm/libeconf.rpm")],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(side_effect=[mismatched, RuntimeError("DNF depsolve error")])

        result = await gen._resolve_with_reconciliation(
            [],
            ["x86_64", "aarch64"],
            ["curl"],
            {},
            [],
            None,
            "test-image",
            0,
        )
        self.assertEqual(result, mismatched)
        self.assertEqual(gen._resolve_stage_with_retry.await_count, 2)

    async def test_reconciliation_falls_back_on_persistent_mismatch(self):
        gen = self._make_generator()
        mismatched = self._make_lockfile(
            {
                "x86_64": [("libeconf", "0.4.1-7.el9_8", "https://x86/libeconf.rpm")],
                "aarch64": [("libeconf", "0.4.1-5.el9", "https://arm/libeconf.rpm")],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(return_value=mismatched)

        result = await gen._resolve_with_reconciliation(
            [],
            ["x86_64", "aarch64"],
            ["curl"],
            {},
            [],
            None,
            "test-image",
            0,
        )
        self.assertEqual(result, mismatched)
        self.assertEqual(gen._resolve_stage_with_retry.await_count, 2)


class TestIsBuilddepRequirement(unittest.TestCase):
    def test_accepts_package_name(self):
        self.assertTrue(RpmLockfilePrototypeGenerator._is_builddep_requirement("gcc"))

    def test_accepts_package_with_dashes(self):
        self.assertTrue(RpmLockfilePrototypeGenerator._is_builddep_requirement("openssl-devel"))

    def test_rejects_rpmlib(self):
        self.assertFalse(RpmLockfilePrototypeGenerator._is_builddep_requirement("rpmlib(CompressedFileNames)"))

    def test_rejects_config(self):
        self.assertFalse(RpmLockfilePrototypeGenerator._is_builddep_requirement("config(pkcs11-helper)"))

    def test_rejects_file_path(self):
        self.assertFalse(RpmLockfilePrototypeGenerator._is_builddep_requirement("/usr/bin/perl"))

    def test_rejects_pkgconfig(self):
        self.assertFalse(RpmLockfilePrototypeGenerator._is_builddep_requirement("pkgconfig(openssl)"))

    def test_rejects_empty(self):
        self.assertFalse(RpmLockfilePrototypeGenerator._is_builddep_requirement(""))


class TestResolveBuilddepPackages(unittest.TestCase):
    def _make_gen(self):
        repos = MagicMock()
        return RpmLockfilePrototypeGenerator(repos=repos)

    def test_no_matching_srpm(self):
        gen = self._make_gen()
        with TemporaryDirectory() as tmpdir:
            result = asyncio.run(gen._resolve_builddep_packages(["pkcs11-helper*"], Path(tmpdir), "test-img"))
            self.assertEqual(result, [])

    def test_matching_srpm(self):
        gen = self._make_gen()
        with TemporaryDirectory() as tmpdir:
            srpm_path = Path(tmpdir) / "pkcs11-helper-1.26.0-3.el8.src.rpm"
            srpm_path.touch()

            async def mock_gather(cmd, check=True, env=None):
                return 0, "gcc\nopenssl-devel\nrpmlib(CompressedFileNames)\n/usr/bin/perl\nmake\n", ""

            import doozerlib.lockfile_prototype.generator as gen_mod

            original = gen_mod.cmd_gather_async
            gen_mod.cmd_gather_async = mock_gather
            try:
                result = asyncio.run(gen._resolve_builddep_packages(["pkcs11-helper*"], Path(tmpdir), "test-img"))
            finally:
                gen_mod.cmd_gather_async = original

            self.assertEqual(result, ["gcc", "make", "openssl-devel"])

    def test_matching_spec_file(self):
        gen = self._make_gen()
        with TemporaryDirectory() as tmpdir:
            spec_path = Path(tmpdir) / "pkcs11-helper.spec"
            spec_path.touch()

            async def mock_gather(cmd, check=True, env=None):
                return 0, "gcc\nmake\n", ""

            import doozerlib.lockfile_prototype.generator as gen_mod

            original = gen_mod.cmd_gather_async
            gen_mod.cmd_gather_async = mock_gather
            try:
                result = asyncio.run(gen._resolve_builddep_packages(["pkcs11-helper*"], Path(tmpdir), "test-img"))
            finally:
                gen_mod.cmd_gather_async = original

            self.assertEqual(result, ["gcc", "make"])
