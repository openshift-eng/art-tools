"""
Tests for doozerlib.lockfile_prototype.generator (orchestration).
"""

import asyncio
import tempfile
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import yaml
from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.generator import (
    RpmLockfilePrototypeGenerator,
    _is_local_rpm,
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


class TestIsLocalRpm(unittest.TestCase):
    def test_explicit_rpm_file(self):
        self.assertTrue(_is_local_rpm("foo.rpm"))
        self.assertTrue(_is_local_rpm("/tmp/bar-1.0.x86_64.rpm"))

    def test_path_glob(self):
        self.assertTrue(_is_local_rpm("/path/to/*.rpm"))
        self.assertTrue(_is_local_rpm("/opt/rpms/*"))

    def test_normal_packages(self):
        self.assertFalse(_is_local_rpm("nfs-utils"))
        self.assertFalse(_is_local_rpm("golang-*1.23*"))
        self.assertFalse(_is_local_rpm("python3-six"))

    def test_build_rpms_in_yaml_filters_local_rpms(self):
        """
        Local RPM file tokens extracted by the parser must be filtered
        out before reaching rpm-lockfile-prototype.
        """
        repos = [RepoEntry(repoid="baseos", baseurl="https://example.com/$basearch/")]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64"],
            packages=["nfs-utils", "/tmp/extras/*.rpm", "jq", "local.rpm"],
            arch_specific_packages={"x86_64": ["librtas", "/opt/rpms/*"]},
        )
        pkg_names = [p if isinstance(p, str) else p.name for p in result.packages]
        self.assertEqual(pkg_names, ["nfs-utils", "jq", "librtas"])

    def test_build_rpms_in_yaml_filters_local_rpms_from_reinstall_and_upgrade(self):
        """
        Local RPM tokens must also be filtered from reinstallPackages and
        upgradePackages, not just from packages and arch_specific_packages.
        """
        repos = [RepoEntry(repoid="baseos", baseurl="https://example.com/$basearch/")]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64"],
            packages=["curl"],
            reinstall_packages=["curl", "/tmp/extras/foo.rpm", "glibc"],
            upgrade_packages=["bash", "/opt/rpms/*", "openssl"],
        )
        self.assertEqual(result.reinstallPackages, ["curl", "glibc"])
        self.assertEqual(result.upgradePackages, ["bash", "openssl"])


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
            working_dir=Path(tempfile.mkdtemp()),
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

    def test_builder_stage_reinstalls_dockerfile_packages_and_adds_conflict_detection(self):
        """
        Multi-stage Dockerfile where only the builder stage installs
        packages (e.g. prometheus-promu). reinstallPackages should contain
        the Dockerfile packages so they appear in the lockfile even when
        already installed on some architectures. upgradePackages must NOT
        include them (they may not be installed in the base image).
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
        # reinstallPackages should contain the Dockerfile packages
        self.assertEqual(captured_configs[0].reinstallPackages, ["prometheus-promu"])
        # Dockerfile packages must NOT be in upgradePackages (they may
        # not be installed in the base image — upgrade would fail)
        self.assertNotIn("prometheus-promu", captured_configs[0].upgradePackages or [])
        # Base image packages should be queried for conflict detection
        generator._container.get_installed_packages.assert_called_once()
        # Base image packages should be in the install list
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        for pkg in ["bash", "gcc", "golang", "glibc"]:
            self.assertIn(pkg, pkg_names)

    def test_builder_stage_conflict_detection_includes_base_deps(self):
        """
        Multi-stage Dockerfile like openshift-enterprise-pod where the
        builder stage installs gcc and the builder's base image has
        gcc-c++ pre-installed. The lockfile should include gcc-c++ in
        the install list for conflict detection, and reinstallPackages
        should contain only the Dockerfile packages.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = [
            "quay.io/test/golang-builder@sha256:abc123",
            "quay.io/test/base@sha256:def456",
        ]
        generator._container.get_installed_packages = AsyncMock(
            return_value=["gcc", "gcc-c++", "glibc", "glibc-static", "libgcc"]
        )

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM golang-builder AS builder\n"
                "RUN dnf install -y gcc glibc-static\n"
                "\n"
                "FROM base-rhel9\n"
                "COPY --from=builder /bin/pause /usr/bin/pod\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_configs), 1)
        # reinstallPackages should contain only the Dockerfile packages
        self.assertEqual(sorted(captured_configs[0].reinstallPackages), ["gcc", "glibc-static"])
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        # gcc-c++ from the base image should be in the install list
        self.assertIn("gcc-c++", pkg_names)
        # Already-listed packages should not be duplicated
        self.assertEqual(pkg_names.count("gcc"), 1)
        self.assertEqual(pkg_names.count("glibc-static"), 1)

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

        self.assertEqual(generator._container.get_installed_packages.call_count, 1)
        self.assertEqual(len(captured_configs), 1)
        self.assertIsNotNone(captured_pullspecs[0])
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

    def test_bare_update_keeps_image_mode_reinstalls_dockerfile_packages(self):
        """
        Bare yum/dnf update with --image mode should NOT expand base
        image packages as upgrade targets (many are virtual provides or
        renamed and cause resolution failures). Dockerfile install
        packages must be reinstalled so they appear in the lockfile, and
        also promoted to upgradePackages as a fallback if reinstall fails.
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
        # Dockerfile packages reinstalled so they appear in lockfile
        self.assertEqual(captured_configs[0].reinstallPackages, ["libreswan"])
        # Dockerfile packages promoted to upgrade as reinstall fallback
        self.assertEqual(captured_configs[0].upgradePackages, ["libreswan"])

    def test_mixed_install_and_bare_update_reinstalls_dockerfile_packages(self):
        """
        When a stage has both explicit installs and a bare update
        (e.g. microdnf update -y && microdnf install -y openssl),
        Dockerfile install packages must appear in reinstallPackages
        so they end up in the lockfile even when already installed in
        the base image. Base image packages must NOT be reinstalled —
        they use upgrade semantics via upgradePackages to pick up
        latest versions without pinning.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._container.get_installed_packages = AsyncMock(return_value=["audit", "bash", "glibc"])

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN microdnf update -y && microdnf install -y openssl && microdnf clean all\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_configs), 1)
        # Dockerfile install packages must be reinstalled so they appear
        # in the lockfile even when already installed in the base image
        self.assertEqual(captured_configs[0].reinstallPackages, ["openssl"])
        # Explicit install package must be present in packages list
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        self.assertIn("openssl", pkg_names)
        # Base image packages must appear as upgrade targets so
        # dnf update picks up latest versions from repos
        self.assertEqual(sorted(captured_configs[0].upgradePackages), ["audit", "bash", "glibc", "openssl"])

    def test_dockerfile_package_already_in_base_image_not_reinstalled(self):
        """
        A Dockerfile package that's also already installed in the base
        image (e.g. python3-setuptools, part of both the explicit install
        list and the base image's package set) must NOT be reinstalled —
        only upgraded. Reinstalling it can trivially match the
        currently-installed EVR and win over the separate upgrade
        request, silently keeping an old version (e.g. a non-modular
        build from the base repos) instead of picking up the newer one
        the upgrade would have found.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        # python3-setuptools is already installed in the base image
        generator._container.get_installed_packages = AsyncMock(return_value=["bash", "glibc", "python3-setuptools"])

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN dnf upgrade -y && dnf install -y python3-setuptools git\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_configs), 1)
        # python3-setuptools must NOT be reinstalled — it's a base image
        # package and must go through upgrade semantics only
        self.assertNotIn("python3-setuptools", captured_configs[0].reinstallPackages)
        # git is purely a Dockerfile package (not in the base image) and
        # must still be reinstalled as before
        self.assertIn("git", captured_configs[0].reinstallPackages)
        # python3-setuptools must still be present as an upgrade target
        self.assertIn("python3-setuptools", captured_configs[0].upgradePackages)
        # And still present in the plain install list
        pkg_names = [p if isinstance(p, str) else p.name for p in captured_configs[0].packages]
        self.assertIn("python3-setuptools", pkg_names)

    def test_base_image_overlap_package_survives_retry_exhaustion_fallback(self):
        """
        When the main retry loop exhausts (unrelated packages keep
        failing) and falls back, a Dockerfile package that's also a base
        image package (only reachable via upgrade semantics, since it's
        excluded from reinstall) must still make it into the fallback's
        upgradePackages — not get wiped out by a blanket upgrade-targets
        clear before the fallback runs.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        # python3-setuptools is both a Dockerfile package and already
        # installed; the other 5 are pure base image noise that will each
        # fail resolution in turn, exhausting the main retry loop.
        failing_pkgs = [f"base-noise-{i}" for i in range(5)]
        generator._container.get_installed_packages = AsyncMock(return_value=["python3-setuptools", *failing_pkgs])

        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            pkg_names = [p if isinstance(p, str) else p.name for p in (config.packages or [])]
            pkg_names += config.reinstallPackages or []
            pkg_names += config.upgradePackages or []
            for pkg in failing_pkgs:
                if pkg in pkg_names:
                    raise RuntimeError(f"No match for argument: {pkg}")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN dnf upgrade -y && dnf install -y python3-setuptools git\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        # Upgrade targets survived (python3-setuptools is still there),
        # so upgrades_dropped must be False — the rebaser must keep the
        # dnf update command so the resolved upgrade RPMs are applied.
        self.assertFalse(generator.upgrades_dropped)
        # Fallback should have been reached (5 main-loop retries exhausted).
        # Some later calls are _recover_stripped_per_arch attempts for the
        # noise packages (unrelated to this assertion), so check across all
        # calls rather than assuming a fixed index: python3-setuptools must
        # have been offered as an upgrade target at some point post-fallback,
        # never as a reinstall target.
        all_configs = [c.args[0] for c in generator._resolver.resolve.call_args_list]
        self.assertTrue(
            any("python3-setuptools" in (c.upgradePackages or []) for c in all_configs),
            "python3-setuptools should appear in upgradePackages in at least one resolve call",
        )
        self.assertTrue(all("python3-setuptools" not in (c.reinstallPackages or []) for c in all_configs))

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

    def test_reinstall_retry_skips_retries_when_all_optional(self):
        """
        When a reinstall/upgrade package fails and all remaining reinstall
        packages are optional (strippable), the retry loop should exit
        early and fall back to resolving without reinstall packages.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._container.get_installed_packages = AsyncMock(return_value=["nonexistent-pkg", "glibc"])

        async def mock_resolve(config, image_pullspec=None):
            pkg_names = [p if isinstance(p, str) else p.name for p in (config.packages or [])]
            pkg_names += config.reinstallPackages or []
            pkg_names += config.upgradePackages or []
            if "nonexistent-pkg" in pkg_names:
                raise RuntimeError("No match for argument: nonexistent-pkg")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf install -y curl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        # First attempt fails, early exit + fallback succeeds, then per-arch
        # recovery is attempted for the stripped package (2 arches, both fail
        # since nonexistent-pkg is genuinely unavailable everywhere) = 4 calls.
        self.assertEqual(generator._resolver.resolve.call_count, 4)
        # Fallback should drop all optional reinstall packages
        fallback_config = generator._resolver.resolve.call_args_list[1][0][0]
        self.assertEqual(fallback_config.reinstallPackages, [])
        # Dockerfile package must still be in the install list
        pkg_names = [p if isinstance(p, str) else p.name for p in fallback_config.packages]
        self.assertIn("curl", pkg_names)

    def test_fallback_sets_upgrades_dropped_flag(self):
        """
        When the retry loop exhausts retries and the fallback clears
        upgrade targets, generator.upgrades_dropped must be True so
        the rebaser strips dnf update from the Dockerfile.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._container.get_installed_packages = AsyncMock(return_value=["bad-pkg", "glibc"])

        async def mock_resolve(config, image_pullspec=None):
            pkg_names = [p if isinstance(p, str) else p.name for p in (config.packages or [])]
            pkg_names += config.reinstallPackages or []
            pkg_names += config.upgradePackages or []
            if "bad-pkg" in pkg_names:
                raise RuntimeError("No match for argument: bad-pkg")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        self.assertFalse(generator.upgrades_dropped)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf install -y curl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertTrue(generator.upgrades_dropped)
        # Index 1 is the fallback call (index 0 fails, indices 2+ are the
        # per-arch recovery attempts for the stripped bad-pkg, which also
        # fail since it's genuinely unavailable everywhere).
        fallback_config = generator._resolver.resolve.call_args_list[1][0][0]
        self.assertEqual(fallback_config.upgradePackages, [])

    def test_fallback_keeps_upgrades_when_targets_remain(self):
        """
        When the retry loop exhausts but upgrade targets survive the
        stripping, upgrades_dropped must be False so the rebaser keeps
        the dnf update command and the resolved upgrade RPMs are applied
        at build time.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        # 5 bad packages exhaust the retry loop, 1 good upgrade target survives
        failing_pkgs = [f"bad-{i}" for i in range(5)]
        generator._container.get_installed_packages = AsyncMock(return_value=["glibc", *failing_pkgs])

        async def mock_resolve(config, image_pullspec=None):
            pkg_names = [p if isinstance(p, str) else p.name for p in (config.packages or [])]
            pkg_names += config.reinstallPackages or []
            pkg_names += config.upgradePackages or []
            for pkg in failing_pkgs:
                if pkg in pkg_names:
                    raise RuntimeError(f"No match for argument: {pkg}")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf upgrade -y && dnf install -y curl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        # glibc upgrade target survived → upgrades not dropped
        self.assertFalse(generator.upgrades_dropped)

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

        async def mock_resolve(config, image_pullspec=None):
            pkg_names = [p if isinstance(p, str) else p.name for p in (config.packages or [])]
            pkg_names += config.upgradePackages or []
            if "dmidecode" in pkg_names:
                raise RuntimeError("No match for argument: dmidecode")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils dmidecode\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

            # 1 failing attempt + 1 successful retry + 2 per-arch recovery
            # attempts for dmidecode (both fail, genuinely unavailable) = 4.
            self.assertEqual(generator._resolver.resolve.call_count, 4)
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

    def test_retry_warns_and_strips_required_dockerfile_package_missing(self):
        """
        When a required Dockerfile package (not in strippable_packages) is
        reported missing, _resolve_stage_with_retry must log a warning and
        strip it during retry rather than raising immediately.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("No match for argument: glibc-static")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        result = asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64"],
                packages=["gcc", "glibc-static", "bash"],
                arch_pkgs={},
                update_targets=[],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="openshift-enterprise-pod",
                stage_num=0,
                strippable_packages={"bash"},
            )
        )
        self.assertIsNotNone(result)
        self.assertEqual(call_count, 2)
        second_config = generator._resolver.resolve.call_args_list[1][0][0]
        self.assertNotIn("glibc-static", second_config.packages)
        self.assertIn("gcc", second_config.packages)

    def test_retry_strips_only_conflict_detection_packages(self):
        """
        When strippable_packages is set, only those packages are removed
        during retries. Original Dockerfile packages are preserved.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("No match for argument: libfoo-dev")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        result = asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64"],
                packages=["gcc", "glibc-static", "libfoo-dev"],
                arch_pkgs={},
                update_targets=[],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="test-image",
                stage_num=0,
                strippable_packages={"libfoo-dev", "libbar"},
            )
        )

        self.assertIsNotNone(result)
        self.assertEqual(call_count, 2)
        second_config = generator._resolver.resolve.call_args_list[1][0][0]
        # Strippable package removed
        self.assertNotIn("libfoo-dev", second_config.packages)
        # Required packages preserved
        self.assertIn("gcc", second_config.packages)
        self.assertIn("glibc-static", second_config.packages)

    def test_fallback_keeps_required_packages_in_reinstall(self):
        """
        When retry exhaustion fallback fires, required Dockerfile packages
        that overlap with reinstallPackages must be preserved in reinstall.
        Otherwise packages pre-installed in the base image disappear from
        the lockfile (dnf says 'already installed', skips them).
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        failing_pkgs = [f"base-pkg-{i}" for i in range(5)]
        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise RuntimeError(f"No match for argument: {failing_pkgs[call_count - 1]}")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        # Dockerfile packages: git, gzip, util-linux
        # Base image packages (reinstall + strippable): git, gzip, + 5 failing base pkgs
        # "git" and "gzip" are in both — they must stay in reinstall after fallback
        all_packages = ["git", "gzip", "util-linux", *failing_pkgs]
        reinstall = ["git", "gzip", *failing_pkgs]
        strippable = set(failing_pkgs)

        result = asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64", "aarch64"],
                packages=all_packages,
                arch_pkgs={},
                update_targets=[],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="openshift-enterprise-tests",
                stage_num=1,
                reinstall_packages=reinstall,
                strippable_packages=strippable,
            )
        )

        self.assertIsNotNone(result)
        # 5 retries (one failing pkg each) + 1 fallback = 6 calls
        self.assertEqual(call_count, 6)
        final_config = generator._resolver.resolve.call_args_list[5][0][0]
        # Required Dockerfile packages must remain in reinstallPackages
        self.assertIn("git", final_config.reinstallPackages)
        self.assertIn("gzip", final_config.reinstallPackages)
        # Strippable packages must be gone
        for pkg in failing_pkgs:
            self.assertNotIn(pkg, final_config.reinstallPackages)

    def test_fallback_disables_reinstall_to_upgrade_promotion(self):
        """
        When the retry loop exhausts and the fallback fires, reinstall
        packages must NOT be promoted to upgradePackages. Otherwise
        base.upgrade() raises PackagesNotInstalledError for packages
        that are in reinstall but not installed on all arches.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        failing_pkgs = [f"base-pkg-{i}" for i in range(5)]
        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise RuntimeError(f"No match for argument: {failing_pkgs[call_count - 1]}")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        # util-linux is a Dockerfile package AND a base image package.
        # It must stay in reinstallPackages (graceful skip if missing)
        # but NOT appear in upgradePackages (throws if not installed).
        all_packages = ["util-linux", "curl", *failing_pkgs]
        reinstall = ["util-linux", "curl", *failing_pkgs]
        strippable = set(failing_pkgs)

        result = asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64", "aarch64"],
                packages=all_packages,
                arch_pkgs={},
                update_targets=[],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="ose-vmware-vsphere-csi-driver",
                stage_num=0,
                reinstall_packages=reinstall,
                strippable_packages=strippable,
            )
        )

        self.assertIsNotNone(result)
        self.assertEqual(call_count, 6)
        fallback_config = generator._resolver.resolve.call_args_list[5][0][0]
        # Required packages must stay in reinstallPackages
        self.assertIn("util-linux", fallback_config.reinstallPackages)
        self.assertIn("curl", fallback_config.reinstallPackages)
        # upgradePackages must be empty — promotion disabled in fallback
        self.assertEqual(fallback_config.upgradePackages, [])

    def test_upgrade_targets_not_strippable_in_final_stage(self):
        """
        Dockerfile update targets (e.g. 'yum update -y python3-six') that
        are also base image packages must not be strippable. Otherwise the
        retry fallback drops them from reinstall and they disappear from
        the lockfile on arches where the update has no newer version.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        failing_pkgs = [f"base-pkg-{i}" for i in range(5)]
        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise RuntimeError(f"No match for argument: {failing_pkgs[call_count - 1]}")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        # python3-six is a Dockerfile update target AND a base image package.
        # It must NOT be strippable — it must survive the fallback in reinstall.
        result = asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64", "aarch64"],
                packages=["git", "python3-six", *failing_pkgs],
                arch_pkgs={},
                update_targets=["python3-six"],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="openshift-enterprise-tests",
                stage_num=1,
                reinstall_packages=["git", "python3-six", *failing_pkgs],
                strippable_packages=set(failing_pkgs),
            )
        )

        self.assertIsNotNone(result)
        self.assertEqual(call_count, 6)
        final_config = generator._resolver.resolve.call_args_list[5][0][0]
        # python3-six must stay in reinstallPackages (not strippable)
        self.assertIn("python3-six", final_config.reinstallPackages)
        self.assertIn("git", final_config.reinstallPackages)
        for pkg in failing_pkgs:
            self.assertNotIn(pkg, final_config.reinstallPackages)

    def test_fallback_strips_newly_discovered_missing_package(self):
        """
        A package that only surfaces as missing once the retry loop is
        already exhausted (e.g. dmidecode, only on x86_64) must still be
        stripped by the fallback instead of crashing the whole resolve
        with an unguarded RuntimeError.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        failing_pkgs = [f"base-pkg-{i}" for i in range(5)]
        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise RuntimeError(f"No match for argument: {failing_pkgs[call_count - 1]}")
            if call_count == 6:
                raise RuntimeError("No match for argument: dmidecode")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        result = asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64", "aarch64"],
                packages=["git", "dmidecode", *failing_pkgs],
                arch_pkgs={},
                update_targets=[],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="ose-baremetal-installer",
                stage_num=0,
                strippable_packages=set(failing_pkgs),
            )
        )

        self.assertIsNotNone(result)
        # 5 main-loop retries + 1 fallback failure on dmidecode + 1 successful fallback retry
        self.assertEqual(call_count, 7)
        final_config = generator._resolver.resolve.call_args_list[6][0][0]
        pkg_names = [p if isinstance(p, str) else p.name for p in final_config.packages]
        self.assertNotIn("dmidecode", pkg_names)
        self.assertIn("git", pkg_names)

    def test_fallback_raises_after_exhausting_its_own_retries(self):
        """
        If the fallback itself keeps hitting new missing packages beyond
        its retry budget, it must raise a clear RuntimeError rather than
        looping forever or crashing with a raw parse failure.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        # 5 distinct packages for the main loop, then 5 more distinct
        # packages that only ever surface during the fallback loop.
        main_failing = [f"base-pkg-{i}" for i in range(5)]
        fallback_failing = [f"arch-only-pkg-{i}" for i in range(5)]
        call_count = 0

        async def mock_resolve(config, image_pullspec=None):
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise RuntimeError(f"No match for argument: {main_failing[call_count - 1]}")
            raise RuntimeError(f"No match for argument: {fallback_failing[call_count - 6]}")

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(
                generator._resolve_stage_with_retry(
                    repo_list=repos,
                    arches=["x86_64", "aarch64"],
                    packages=["git"] + main_failing + fallback_failing,
                    arch_pkgs={},
                    update_targets=[],
                    image_pullspec="quay.io/test/base@sha256:abc123",
                    distgit_key="ose-baremetal-installer",
                    stage_num=0,
                    strippable_packages=set(main_failing),
                )
            )

        self.assertIn("exceeded", str(ctx.exception))
        # 5 main-loop attempts + 5 fallback attempts, no more calls after that
        self.assertEqual(call_count, 10)

    def test_bare_update_final_stage_triggers_per_arch_recovery(self):
        """
        End-to-end: final stage with a bare 'dnf upgrade -y' plus an
        explicit install (has_bare_update=True, is_update_only=False)
        strips an arch-specific base image package (e.g. dmidecode).
        Per-arch recovery must still be attempted for this stage shape,
        not just for is_update_only stages.
        """
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]
        generator._container.get_installed_packages = AsyncMock(return_value=["curl", "dmidecode"])
        generator._recover_stripped_per_arch = AsyncMock(return_value=None)

        async def mock_resolve(config, image_pullspec=None):
            pkg_names = [p if isinstance(p, str) else p.name for p in (config.packages or [])]
            pkg_names += config.upgradePackages or []
            if "dmidecode" in pkg_names:
                raise RuntimeError("No match for argument: dmidecode")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base-rhel9\nRUN dnf upgrade -y && dnf install -y curl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        generator._recover_stripped_per_arch.assert_awaited_once()
        stripped_arg = generator._recover_stripped_per_arch.call_args[0][2]
        self.assertIn("dmidecode", stripped_arg)

    def test_builder_stage_strips_unavailable_packages_silently(self):
        """
        End-to-end: builder stage where glibc-static (Dockerfile package)
        is not in repos. Builder stages use strippable=None (all packages
        strippable) so unavailable packages are silently stripped rather
        than raising.
        """
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None
        generator = self._make_generator()
        generator.downstream_parents = [
            "quay.io/test/golang-builder@sha256:abc123",
            "quay.io/test/base@sha256:def456",
        ]
        generator._container.get_installed_packages = AsyncMock(return_value=["gcc", "gcc-c++", "glibc", "libgcc"])

        async def mock_resolve(config, image_pullspec=None):
            pkg_names = [p if isinstance(p, str) else p.name for p in config.packages]
            if "glibc-static" in pkg_names:
                raise RuntimeError("No match for argument: glibc-static")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM golang-builder AS builder\n"
                "RUN dnf install -y gcc glibc-static\n"
                "\n"
                "FROM base-rhel9\n"
                "COPY --from=builder /bin/pause /usr/bin/pod\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))
            self.assertTrue((dest_dir / "rpms.lock.yaml").exists())

    def test_build_repo_list_keeps_literal_url_for_single_arch_repo(self):
        rt = MagicMock()
        rt.name = "rhel-9-rt-rpms"
        rt.baseurl.return_value = "https://example.com/e4s/rhel9/9.8/x86_64/rt/os/"
        rt.content_set.return_value = "rhel-9-for-x86_64-rt-rpms"

        repo_map = {"rhel-9-rt-rpms": rt}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))
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

        generator = RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))
        result = generator._build_repo_list(enabled_repos={"rhel-9-golang-rpms"}, arches=["x86_64"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].options["includepkgs"], "module-build-macros golang* goversioninfo")
        self.assertEqual(result[0].options["module_hotfixes"], 1)

    def test_build_repo_list_no_extra_options(self):
        rt = MagicMock()
        rt.name = "rhel-9-rt-rpms"
        rt.baseurl.return_value = "https://example.com/e4s/rhel9/9.8/x86_64/rt/os/"
        rt.content_set.return_value = "rhel-9-for-x86_64-rt-rpms"
        rt.cs_optional = False
        rt._data.conf.get.side_effect = lambda key, default=None: default if key == "extra_options" else default

        repo_map = {"rhel-9-rt-rpms": rt}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))
        result = generator._build_repo_list(enabled_repos={"rhel-9-rt-rpms"}, arches=["x86_64"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].options, {})

    def test_build_repo_list_templatizes_multi_arch_url(self):
        baseos = MagicMock()
        baseos.name = "rhel-9-baseos-rpms"
        baseos.baseurl.side_effect = lambda repotype="unsigned", arch="x86_64": (
            f"https://example.com/baseos/{arch}/os/"
        )
        baseos.content_set.return_value = "rhel-9-for-x86_64-baseos-rpms"

        repo_map = {"rhel-9-baseos-rpms": baseos}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))
        result = generator._build_repo_list(enabled_repos={"rhel-9-baseos-rpms"}, arches=["x86_64", "aarch64"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].repoid, "rhel-9-for-$basearch-baseos-rpms")
        self.assertEqual(result[0].baseurl, "https://example.com/baseos/$basearch/os/")

    def test_build_repo_list_optional_repo_gets_skip_if_unavailable(self):
        openstack = MagicMock()
        openstack.name = "openstack-16-for-rhel-8-rpms"
        openstack.baseurl.side_effect = lambda repotype="unsigned", arch="x86_64": {
            "x86_64": "https://example.com/layered/rhel8/x86_64/openstack/16.2/os/",
            "ppc64le": "https://example.com/layered/rhel8/ppc64le/openstack/16.2/os/",
            "aarch64": "https://example.com/layered/rhel8/x86_64/openstack/16.2/os/",
            "s390x": "https://example.com/layered/rhel8/x86_64/openstack/16.2/os/",
        }[arch]
        openstack.content_set.return_value = "openstack-16.2-for-rhel-8-x86_64-rpms"
        openstack.cs_optional = True
        openstack._data.conf.get.side_effect = lambda key, default=None: default if key == "extra_options" else default

        repo_map = {"openstack-16-for-rhel-8-rpms": openstack}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))
        result = generator._build_repo_list(
            enabled_repos={"openstack-16-for-rhel-8-rpms"},
            arches=["x86_64", "aarch64", "ppc64le", "s390x"],
        )
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].options.get("skip_if_unavailable"))

    def test_build_repo_list_non_optional_repo_no_skip_if_unavailable(self):
        baseos = MagicMock()
        baseos.name = "rhel-9-baseos-rpms"
        baseos.baseurl.side_effect = lambda repotype="unsigned", arch="x86_64": (
            f"https://example.com/baseos/{arch}/os/"
        )
        baseos.content_set.return_value = "rhel-9-for-x86_64-baseos-rpms"
        baseos.cs_optional = False
        baseos._data.conf.get.side_effect = lambda key, default=None: default if key == "extra_options" else default

        repo_map = {"rhel-9-baseos-rpms": baseos}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))
        result = generator._build_repo_list(enabled_repos={"rhel-9-baseos-rpms"}, arches=["x86_64", "aarch64"])
        self.assertEqual(len(result), 1)
        self.assertNotIn("skip_if_unavailable", result[0].options)

    def test_build_repo_list_optional_preserves_existing_extra_options(self):
        repo = MagicMock()
        repo.name = "optional-repo"
        repo.baseurl.return_value = "https://example.com/repo/x86_64/os/"
        repo.content_set.return_value = "optional-for-x86_64-rpms"
        repo.cs_optional = True
        repo._data.conf.get.side_effect = lambda key, default=None: (
            {"module_hotfixes": 1} if key == "extra_options" else default
        )

        repo_map = {"optional-repo": repo}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))
        result = generator._build_repo_list(enabled_repos={"optional-repo"}, arches=["x86_64"])
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].options.get("skip_if_unavailable"))
        self.assertEqual(result[0].options["module_hotfixes"], 1)


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

    async def test_reconciliation_removes_mismatched_from_update_targets(self):
        """
        When a mismatched package is an update target (e.g. python3-six
        from 'yum update -y python3-six'), the re-resolution must remove
        it from update_targets so the upgrade doesn't override the
        version pin.
        """
        gen = self._make_generator()
        mismatched = self._make_lockfile(
            {
                "x86_64": [("python3-six", "1.12.0-2.el8ost", "https://x86/six.rpm")],
                "aarch64": [("python3-six", "1.11.0-8.el8", "https://arm/six.rpm")],
            }
        )
        reconciled = self._make_lockfile(
            {
                "x86_64": [("python3-six", "1.11.0-8.el8", "https://x86/six-old.rpm")],
                "aarch64": [("python3-six", "1.11.0-8.el8", "https://arm/six.rpm")],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(side_effect=[mismatched, reconciled])

        result = await gen._resolve_with_reconciliation(
            [],
            ["x86_64", "aarch64"],
            ["git", "python3-six"],
            {},
            ["python3-six"],
            "quay.io/test/base@sha256:abc123",
            "openshift-enterprise-tests",
            1,
        )
        self.assertEqual(result, reconciled)
        second_call = gen._resolve_stage_with_retry.call_args_list[1]
        second_update_targets = second_call[0][4]
        self.assertNotIn("python3-six", second_update_targets)
        second_packages = second_call[0][2]
        self.assertTrue(any("python3-six-1.11.0-8.el8" in p for p in second_packages))

    def test_detect_partial_arch_upgrades_basic(self):
        """
        Upgrade target present on 1 of 4 arches is flagged as partial.
        """
        lockfile = self._make_lockfile(
            {
                "x86_64": [("libsemanage", "3.6-2.1.el9_4", "https://x86/libsemanage.rpm")],
                "aarch64": [],
                "ppc64le": [],
                "s390x": [],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_partial_arch_upgrades(
            lockfile,
            ["x86_64", "aarch64", "ppc64le", "s390x"],
            ["libsemanage"],
            set(),
        )
        self.assertEqual(result, {"libsemanage"})

    def test_detect_partial_arch_upgrades_ignores_stripped(self):
        """
        Package already in stripped_tracker should not be flagged.
        """
        lockfile = self._make_lockfile(
            {
                "x86_64": [("dmidecode", "3.3-4.el9", "https://x86/dmidecode.rpm")],
                "aarch64": [],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_partial_arch_upgrades(
            lockfile,
            ["x86_64", "aarch64"],
            ["dmidecode"],
            {"dmidecode"},
        )
        self.assertEqual(result, set())

    def test_detect_partial_arch_upgrades_all_arches_present(self):
        """
        Upgrade target present on all arches should not be flagged.
        """
        lockfile = self._make_lockfile(
            {
                "x86_64": [("curl", "7.76-2.el9", "https://x86/curl.rpm")],
                "aarch64": [("curl", "7.76-2.el9", "https://arm/curl.rpm")],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_partial_arch_upgrades(
            lockfile,
            ["x86_64", "aarch64"],
            ["curl"],
            set(),
        )
        self.assertEqual(result, set())

    def test_detect_partial_arch_upgrades_no_update_targets(self):
        """
        Empty update_targets produces empty result.
        """
        lockfile = self._make_lockfile(
            {
                "x86_64": [("curl", "7.76-2.el9", "https://x86/curl.rpm")],
                "aarch64": [],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_partial_arch_upgrades(
            lockfile,
            ["x86_64", "aarch64"],
            [],
            set(),
        )
        self.assertEqual(result, set())

    def test_detect_partial_arch_upgrades_non_upgrade_target_ignored(self):
        """
        Package present on only some arches but NOT an upgrade target
        should not be flagged.
        """
        lockfile = self._make_lockfile(
            {
                "x86_64": [("custom-pkg", "1.0-1.el9", "https://x86/custom.rpm")],
                "aarch64": [],
            }
        )
        result = RpmLockfilePrototypeGenerator._detect_partial_arch_upgrades(
            lockfile,
            ["x86_64", "aarch64"],
            ["other-pkg"],
            set(),
        )
        self.assertEqual(result, set())

    async def test_reconciliation_strips_partial_arch_upgrades(self):
        """
        When first pass has a partial-arch upgrade target, it should
        be stripped before mismatch detection, preventing the package
        from appearing in the final lockfile.
        """
        gen = self._make_generator()
        first_pass = self._make_lockfile(
            {
                "x86_64": [
                    ("curl", "7.76-1.el9", "https://x86/curl.rpm"),
                    ("libsemanage", "3.6-2.1.el9_4", "https://x86/libsemanage.rpm"),
                ],
                "aarch64": [
                    ("curl", "7.76-1.el9", "https://arm/curl.rpm"),
                ],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(return_value=first_pass)

        result = await gen._resolve_with_reconciliation(
            [],
            ["x86_64", "aarch64"],
            ["curl"],
            {},
            ["libsemanage"],
            "quay.io/test/base@sha256:abc123",
            "test-image",
            0,
        )
        x86_names = {p.name for p in result.arches[0].packages}
        arm_names = {p.name for p in result.arches[1].packages}
        self.assertNotIn("libsemanage", x86_names)
        self.assertIn("curl", x86_names)
        self.assertIn("curl", arm_names)
        gen.logger.warning.assert_called()


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
        return RpmLockfilePrototypeGenerator(repos=repos, working_dir=Path(tempfile.mkdtemp()))

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

    def test_spec_file_skipped_with_warning(self):
        gen = self._make_gen()
        with TemporaryDirectory() as tmpdir:
            spec_path = Path(tmpdir) / "tuned.spec"
            spec_path.touch()
            result = asyncio.run(gen._resolve_builddep_packages(["tuned.spec"], Path(tmpdir), "test-img"))
            self.assertEqual(result, [])


class TestExtractRhelVersionFromPullspec(unittest.TestCase):
    def test_rhel_8_golang_tag(self):
        ps = "registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.25-openshift-4.21"
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps), 8)

    def test_rhel_9_golang_tag(self):
        ps = "registry.ci.openshift.org/ocp/builder:rhel-9-golang-1.25"
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps), 9)

    def test_ubi_9_in_path_not_tag_returns_none(self):
        ps = "registry.access.redhat.com/ubi9/ubi-minimal:latest"
        self.assertIsNone(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps))

    def test_ubi_9_in_tag(self):
        ps = "registry.access.redhat.com/ubi9/ubi-minimal:ubi-9-minimal"
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps), 9)

    def test_nvr_el8_tag(self):
        ps = "registry.redhat.io/openshift/art-images-base:openshift-golang-builder-container-v1.25.9-202605121249.p2.g2aa6a05.el8"
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps), 8)

    def test_nvr_el9_tag(self):
        ps = "registry.redhat.io/openshift/art-images-base:openshift-golang-builder-container-v1.25.9-202605121249.p2.g2aa6a05.el9"
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps), 9)

    def test_digest_only_returns_none(self):
        ps = "quay.io/test/builder@sha256:abc123def456"
        self.assertIsNone(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps))

    def test_no_colon_returns_none(self):
        self.assertIsNone(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec("builder_stage"))

    def test_unrecognized_tag_returns_none(self):
        ps = "quay.io/test/builder:latest"
        self.assertIsNone(RpmLockfilePrototypeGenerator._extract_rhel_version_from_pullspec(ps))


class TestExtractRhelVersionFromRepos(unittest.TestCase):
    def test_rhel9_baseos_content_set(self):
        repos = [
            RepoEntry(repoid="rhel-9-for-x86_64-baseos-e4s-rpms__9_DOT_6", baseurl="https://example.com/baseos/"),
        ]
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_repos(repos), 9)

    def test_rhel8_content_set(self):
        repos = [
            RepoEntry(repoid="rhel-8-for-x86_64-baseos-rpms", baseurl="https://example.com/baseos/"),
        ]
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_repos(repos), 8)

    def test_no_rhel_in_repoid_returns_none(self):
        repos = [
            RepoEntry(repoid="custom-repo-rpms", baseurl="https://example.com/custom/"),
        ]
        self.assertIsNone(RpmLockfilePrototypeGenerator._extract_rhel_version_from_repos(repos))

    def test_empty_repos_returns_none(self):
        self.assertIsNone(RpmLockfilePrototypeGenerator._extract_rhel_version_from_repos([]))

    def test_first_rhel_repo_wins(self):
        repos = [
            RepoEntry(repoid="custom-repo", baseurl="https://example.com/custom/"),
            RepoEntry(repoid="rhel-9-for-x86_64-appstream-rpms", baseurl="https://example.com/appstream/"),
        ]
        self.assertEqual(RpmLockfilePrototypeGenerator._extract_rhel_version_from_repos(repos), 9)


class TestHasRhelVersionMismatch(unittest.TestCase):
    def _make_generator(self) -> RpmLockfilePrototypeGenerator:
        repos = MagicMock()
        return RpmLockfilePrototypeGenerator(
            repos=repos,
            working_dir=Path(tempfile.mkdtemp()),
            container_helper=MagicMock(spec=ContainerImageHelper),
            resolver=MagicMock(spec=RpmResolver),
        )

    def test_el8_builder_el9_repos_is_mismatch(self):
        gen = self._make_generator()
        gen.downstream_parents = [
            "registry.redhat.io/openshift/art-images-base:openshift-golang-builder-container-v1.25.9.el8",
            "quay.io/test/base:rhel-9",
        ]
        repos = [RepoEntry(repoid="rhel-9-for-x86_64-baseos-rpms", baseurl="https://example.com/")]
        self.assertTrue(gen._has_rhel_version_mismatch(0, repos, "test-img"))

    def test_el9_builder_el9_repos_no_mismatch(self):
        gen = self._make_generator()
        gen.downstream_parents = [
            "registry.ci.openshift.org/ocp/builder:rhel-9-golang-1.25",
        ]
        repos = [RepoEntry(repoid="rhel-9-for-x86_64-baseos-rpms", baseurl="https://example.com/")]
        self.assertFalse(gen._has_rhel_version_mismatch(0, repos, "test-img"))

    def test_undetectable_builder_returns_false(self):
        gen = self._make_generator()
        gen.downstream_parents = [
            "quay.io/test/builder@sha256:abc123",
        ]
        repos = [RepoEntry(repoid="rhel-9-for-x86_64-baseos-rpms", baseurl="https://example.com/")]
        self.assertFalse(gen._has_rhel_version_mismatch(0, repos, "test-img"))

    def test_undetectable_repos_returns_false(self):
        gen = self._make_generator()
        gen.downstream_parents = [
            "registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.25",
        ]
        repos = [RepoEntry(repoid="custom-repo", baseurl="https://example.com/")]
        self.assertFalse(gen._has_rhel_version_mismatch(0, repos, "test-img"))

    def test_stage_alias_returns_false(self):
        gen = self._make_generator()
        gen.downstream_parents = ["builder_stage"]
        repos = [RepoEntry(repoid="rhel-9-for-x86_64-baseos-rpms", baseurl="https://example.com/")]
        self.assertFalse(gen._has_rhel_version_mismatch(0, repos, "test-img"))

    def test_out_of_range_stage_returns_false(self):
        gen = self._make_generator()
        gen.downstream_parents = []
        repos = [RepoEntry(repoid="rhel-9-for-x86_64-baseos-rpms", baseurl="https://example.com/")]
        self.assertFalse(gen._has_rhel_version_mismatch(5, repos, "test-img"))


class TestRhelMismatchEndToEnd(unittest.TestCase):
    """
    End-to-end: builder stage with el8 pullspec + el9 repos has no repos
    that can soundly resolve its packages (mixing el8 and el9 repos in
    one rpm-lockfile-prototype call breaks module resolution), so the
    whole stage's packages must be skipped rather than resolved against
    the wrong RHEL major.
    """

    def _make_mock_repos(self) -> MagicMock:
        repos = MagicMock()
        baseos = MagicMock()
        baseos.name = "rhel-9-baseos-rpms"
        baseos.baseurl.return_value = "https://example.com/baseos/x86_64/os/"
        baseos.content_set.return_value = "rhel-9-for-x86_64-baseos-rpms"
        baseos.cs_optional = False
        baseos._data.conf.get.return_value = {}
        repo_map = {"rhel-9-baseos-rpms": baseos}
        repos.__getitem__ = lambda self_repos, key: repo_map[key]
        return repos

    def _make_mock_image_meta(self) -> MagicMock:
        meta = MagicMock()
        meta.distgit_key = "hive"
        meta.get_arches.return_value = ["x86_64"]
        meta.get_enabled_repos.return_value = {"rhel-9-baseos-rpms"}
        meta.is_lockfile_generation_enabled.return_value = True
        lockfile_config = MagicMock()
        lockfile_config.get.return_value = None
        meta.config.konflux.cachi2.lockfile = lockfile_config
        return meta

    def test_el8_builder_skips_entire_stage(self):
        container = MagicMock(spec=ContainerImageHelper)
        container.resolve_to_digest = AsyncMock(side_effect=lambda p: p.split(":")[0] + "@sha256:abc123")
        container.get_installed_packages = AsyncMock(return_value=["gcc", "glibc", "readline"])
        container.read_file_from_image = AsyncMock(return_value="")

        resolver = MagicMock(spec=RpmResolver)
        resolver.resolve = AsyncMock(return_value=FAKE_LOCKFILE_DATA.model_copy(deep=True))

        generator = RpmLockfilePrototypeGenerator(
            repos=self._make_mock_repos(),
            working_dir=Path(tempfile.mkdtemp()),
            container_helper=container,
            resolver=resolver,
        )
        generator.downstream_parents = [
            "registry.redhat.io/openshift/art-images-base:golang-builder-v1.25.el8",
            "quay.io/test/base:rhel-9-base",
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM golang-builder AS builder_el8\n"
                "RUN dnf install -y subscription-manager\n"
                "\n"
                "FROM base-rhel9\n"
                "COPY --from=builder_el8 /bin/app /usr/bin/app\n"
            )
            asyncio.run(generator.generate_lockfile(self._make_mock_image_meta(), dest_dir))

            # Stage 0 (el8 builder, el9-only repos) must be skipped entirely —
            # no repos exist that could soundly resolve its packages, so it's
            # dropped from the lockfile rather than pinning a mismatched RPM.
            # The final stage has no install/update commands, so no stage
            # ever reaches the resolver.
            resolver.resolve.assert_not_called()
            self.assertTrue((dest_dir / "rpms.lock.yaml").exists())
