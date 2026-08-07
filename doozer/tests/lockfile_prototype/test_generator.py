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
    _detect_stages_with_bare_updates,
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

    def test_generate_lockfile_writes_empty_when_no_stages(self):
        """
        When the Dockerfile has no FROM instructions (total_stages == 0),
        an empty lockfile is written.
        """
        meta = self._make_mock_image_meta()
        generator = self._make_generator()

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            # Dockerfile with no FROM → total_stages == 0
            (dest_dir / "Dockerfile").write_text("# No stages in this Dockerfile\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))
            lockfile_path = dest_dir / "rpms.lock.yaml"
            self.assertTrue(lockfile_path.exists())
            with open(lockfile_path) as f:
                data = yaml.safe_load(f)
            self.assertEqual(data["lockfileVersion"], 1)
            self.assertEqual(data["arches"], [])
        generator._resolver.resolve.assert_not_called()

    def test_stage_alias_uses_bare_mode(self):
        """
        When a stage references an alias (no "/"), resolution must
        use bare mode (image_pullspec=None).
        """
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = [
            "quay.io/test/builder@sha256:abc123",
            "build",
        ]

        captured_pullspecs: list[str | None] = []

        async def capture_resolve(config, image_pullspec=None, **kwargs):
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

    def test_resolve_cat_packages_from_base_image(self):
        """
        When $(cat /filepath) patterns appear in Dockerfile RUN commands,
        the extra packages resolved from the base image are passed in
        the packages field of rpms.in.yaml.
        """
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        async def mock_read_file(pullspec, filepath):
            if filepath == "/more-pkgs":
                return '"openvswitch3.5-devel" "openvswitch3.5-ipsec" "ovn25.09-vtep"'
            return ""

        generator._container.read_file_from_image = AsyncMock(side_effect=mock_read_file)

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None, **kwargs):
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
        # Cat-resolved packages are passed as extra packages
        self.assertIn("openvswitch3.5-devel", pkg_names)
        self.assertIn("openvswitch3.5-ipsec", pkg_names)
        self.assertIn("ovn25.09-vtep", pkg_names)

    def test_resolve_cat_packages_preserves_stage_indices(self):
        """
        When a Dockerfile has an empty first stage (no RUN commands),
        $(cat ...) packages in a later stage must still resolve against
        the correct parent. Previously, empty stages were skipped when
        building stage_runs, shifting later stages to wrong indices.
        """
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = [
            "quay.io/test/builder@sha256:aaa",
            "quay.io/test/base@sha256:bbb",
        ]

        async def mock_read_file(pullspec, filepath):
            if "bbb" in pullspec and filepath == "/more-pkgs":
                return "extra-pkg"
            return ""

        generator._container.read_file_from_image = AsyncMock(side_effect=mock_read_file)

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None, **kwargs):
            captured_configs.append(config)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM builder AS build\nCOPY --from=src /app /app\n\nFROM base\nRUN dnf install -y $(cat /more-pkgs)\n"
            )
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        # $(cat /more-pkgs) should resolve against stage 1 (base),
        # not stage 0 (builder)
        self.assertTrue(len(captured_configs) > 0)
        all_pkg_names = []
        for config in captured_configs:
            all_pkg_names.extend(p if isinstance(p, str) else p.name for p in config.packages)
        self.assertIn("extra-pkg", all_pkg_names)

    def test_final_stage_uses_image_mode_when_pullspec_available(self):
        """
        For the final stage with a base image pullspec, resolution uses
        --image mode so DNF sees the base image's rpmdb. This ensures
        lockfile versions match build-time behavior.
        """
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        captured_pullspecs: list[str | None] = []

        async def capture_resolve(config, image_pullspec=None, **kwargs):
            captured_pullspecs.append(image_pullspec)
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=capture_resolve)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN dnf install -y libreswan openssl\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(len(captured_pullspecs), 1)
        # --image mode: pullspec is preserved (not forced to None)
        self.assertIsNotNone(captured_pullspecs[0])

    def test_cat_file_resolved_from_parent_dockerfile_heredoc(self):
        """
        When base image is unreachable and $(cat /filepath) can't read
        from the image, fall back to parsing the parent's Dockerfile
        for RUN commands that generate the file via here-string + sed.
        """
        meta = self._make_mock_image_meta()
        generator = self._make_generator()
        # Image unreachable
        generator._container.resolve_to_digest = AsyncMock(return_value="registry.redhat.io/base:unreachable-tag")
        generator._container.read_file_from_image = AsyncMock(return_value="")

        captured_configs: list[RpmsInConfig] = []

        async def capture_resolve(config, image_pullspec=None, **kwargs):
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
        # Cat-resolved packages from parent source
        self.assertIn("openvswitch3.5-devel", pkg_names)
        self.assertIn("ovn25.09-vtep", pkg_names)

    def test_retry_on_missing_packages(self):
        """
        When the resolver reports a missing package in the extra packages
        list, the retry loop strips it and retries.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        async def mock_resolve(config, image_pullspec=None, **kwargs):
            pkg_names = [p if isinstance(p, str) else p.name for p in (config.packages or [])]
            if "dmidecode" in pkg_names:
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
                arches=["x86_64"],
                packages=["nfs-utils", "dmidecode"],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="test-image",
                stage_num=0,
            )
        )

        # 1 failing attempt + 1 successful retry = 2
        self.assertEqual(generator._resolver.resolve.call_count, 2)
        self.assertIsNotNone(result)

    def test_exclude_packages_retry_on_containerfile_missing_packages(self):
        """
        When packagesFromContainerfile fails due to unavailable packages
        (e.g. OKD-only centos-release-* packages extracted from a
        conditional RUN block), the retry adds them to excludePackages
        so the upstream tool skips them instead of failing.
        """
        generator = self._make_generator()
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        async def mock_resolve(config, image_pullspec=None, containerfile_path=None, **kwargs):
            if containerfile_path and not config.excludePackages:
                raise RuntimeError("No match for argument: centos-release-nfv-openvswitch")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        generator._resolver.resolve = AsyncMock(side_effect=mock_resolve)

        repos = [
            RepoEntry(
                repoid="rhel-9-baseos-rpms",
                baseurl="https://example.com/baseos/$basearch/os/",
            )
        ]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN dnf install -y nfs-utils centos-release-nfv-openvswitch\n"
            )

            result = asyncio.run(
                generator._resolve_stage_with_retry(
                    repo_list=repos,
                    arches=["x86_64"],
                    packages=[],
                    image_pullspec="quay.io/test/base@sha256:abc123",
                    distgit_key="test-image",
                    stage_num=0,
                    containerfile_path=str(dest_dir / "Dockerfile"),
                )
            )

            self.assertIsNotNone(result)
            # 1 failing attempt + 1 retry with excludePackages = 2
            self.assertEqual(generator._resolver.resolve.call_count, 2)
            # Second call must have the missing package in excludePackages
            second_config = generator._resolver.resolve.call_args_list[1][0][0]
            self.assertIn("centos-release-nfv-openvswitch", second_config.excludePackages)

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
        baseos.baseurl.side_effect = lambda repotype="unsigned", arch="x86_64": f"https://example.com/baseos/{arch}/os/"
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
        baseos.baseurl.side_effect = lambda repotype="unsigned", arch="x86_64": f"https://example.com/baseos/{arch}/os/"
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
            None,
            "test-image",
            0,
        )
        self.assertEqual(result, reconciled)
        self.assertEqual(gen._resolve_stage_with_retry.await_count, 2)

        second_call_packages = gen._resolve_stage_with_retry.call_args_list[1][0][2]
        self.assertIn("libeconf-0.4.1-5.el9", second_call_packages)

    async def test_reconciliation_excludes_unversioned_name_from_pinned_packages(self):
        """
        When the packages list contains an unversioned name that matches a
        mismatched package, the second resolution must exclude the unversioned
        name so DNF doesn't override the version pin.
        """
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
            ["curl", "libeconf"],
            None,
            "test-image",
            0,
        )
        self.assertEqual(result, reconciled)

        second_call_packages = gen._resolve_stage_with_retry.call_args_list[1][0][2]
        self.assertIn("libeconf-0.4.1-5.el9", second_call_packages)
        self.assertNotIn("libeconf", second_call_packages)
        self.assertIn("curl", second_call_packages)

    async def test_reconciliation_raises_on_resolution_error(self):
        gen = self._make_generator()
        mismatched = self._make_lockfile(
            {
                "x86_64": [("libeconf", "0.4.1-7.el9_8", "https://x86/libeconf.rpm")],
                "aarch64": [("libeconf", "0.4.1-5.el9", "https://arm/libeconf.rpm")],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(side_effect=[mismatched, RuntimeError("DNF depsolve error")])

        with self.assertRaises(RuntimeError) as ctx:
            await gen._resolve_with_reconciliation(
                [],
                ["x86_64", "aarch64"],
                ["curl"],
                None,
                "test-image",
                0,
            )
        self.assertIn("reconciliation failed", str(ctx.exception))
        self.assertIn("libeconf", str(ctx.exception))
        self.assertEqual(gen._resolve_stage_with_retry.await_count, 2)

    async def test_reconciliation_raises_on_persistent_mismatch(self):
        gen = self._make_generator()
        mismatched = self._make_lockfile(
            {
                "x86_64": [("libeconf", "0.4.1-7.el9_8", "https://x86/libeconf.rpm")],
                "aarch64": [("libeconf", "0.4.1-5.el9", "https://arm/libeconf.rpm")],
            }
        )
        gen._resolve_stage_with_retry = AsyncMock(return_value=mismatched)

        with self.assertRaises(RuntimeError) as ctx:
            await gen._resolve_with_reconciliation(
                [],
                ["x86_64", "aarch64"],
                ["curl"],
                None,
                "test-image",
                0,
            )
        self.assertIn("reconciliation failed", str(ctx.exception))
        self.assertIn("libeconf", str(ctx.exception))
        self.assertEqual(gen._resolve_stage_with_retry.await_count, 2)


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
        """
        Stage 0 (el8 builder) is skipped due to RHEL version mismatch.
        Stage 1 (final, base-rhel9) is still resolved even though it has
        no install commands — the upstream tool handles package extraction
        and the resolver is called for every non-skipped stage.
        """
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

            # Stage 0 (el8 builder, el9-only repos) must be skipped entirely.
            # Stage 1 (final) is resolved even with no install commands.
            resolver.resolve.assert_called_once()
            self.assertTrue((dest_dir / "rpms.lock.yaml").exists())


class TestDetectStagesWithBareUpdates(unittest.TestCase):
    """
    Tests for _detect_stages_with_bare_updates.
    """

    def _entries_from_dockerfile(self, content: str) -> list[dict]:
        """
        Parse Dockerfile content into DockerfileParser structure entries.
        """
        from dockerfile_parse import DockerfileParser

        with tempfile.TemporaryDirectory() as tmpdir:
            df_path = Path(tmpdir) / "Dockerfile"
            df_path.write_text(content)
            return DockerfileParser(str(df_path)).structure

    def test_bare_dnf_update(self):
        entries = self._entries_from_dockerfile("FROM base\nRUN dnf -y update && yum clean all\n")
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, {0})

    def test_bare_yum_update(self):
        entries = self._entries_from_dockerfile("FROM base\nRUN yum update -y && yum clean all\n")
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, {0})

    def test_bare_microdnf_update(self):
        entries = self._entries_from_dockerfile("FROM base\nRUN microdnf update && microdnf clean all\n")
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, {0})

    def test_bare_upgrade(self):
        entries = self._entries_from_dockerfile("FROM base\nRUN dnf upgrade -y && dnf clean all\n")
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, {0})

    def test_named_update_not_detected(self):
        entries = self._entries_from_dockerfile("FROM base\nRUN dnf update -y openssl && dnf clean all\n")
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, set())

    def test_install_only_not_detected(self):
        entries = self._entries_from_dockerfile("FROM base\nRUN dnf install -y nfs-utils\n")
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, set())

    def test_multi_stage_detects_correct_stage(self):
        entries = self._entries_from_dockerfile(
            "FROM builder AS build\n"
            "RUN dnf install -y gcc\n"
            "\n"
            "FROM base\n"
            "RUN dnf -y update && yum clean all\n"
            "RUN dnf install -y nfs-utils\n"
        )
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, {1})

    def test_no_run_commands(self):
        entries = self._entries_from_dockerfile("FROM base\nCOPY . /app\n")
        result = _detect_stages_with_bare_updates(entries)
        self.assertEqual(result, set())


class TestBareUpdateUpgradeResolution(unittest.TestCase):
    """
    Tests for bare update upgrade package resolution in generate_lockfile
    and _resolve_stage_with_retry.
    """

    def _make_mock_repos(self) -> MagicMock:
        repos = MagicMock()
        baseos = MagicMock()
        baseos.name = "rhel-9-baseos-rpms"
        baseos.baseurl.return_value = "https://example.com/baseos/x86_64/os/"
        baseos.content_set.return_value = "rhel-9-for-x86_64-baseos-rpms"
        baseos.cs_optional = False
        baseos._data.conf = {}
        repo_map = {"rhel-9-baseos-rpms": baseos}
        repos.__getitem__ = lambda self_repos, key: repo_map[key]
        return repos

    def _make_mock_image_meta(self) -> MagicMock:
        meta = MagicMock()
        meta.distgit_key = "ose-frr"
        meta.get_arches.return_value = ["x86_64"]
        meta.get_enabled_repos.return_value = {"rhel-9-baseos-rpms"}
        meta.is_lockfile_generation_enabled.return_value = True
        meta.is_cross_arch_enabled.return_value = False
        return meta

    def test_bare_update_passes_upgrade_packages(self):
        """
        When a non-final stage has a bare dnf update, base image packages
        should be passed as upgradePackages to the resolver.
        """
        container = MagicMock(spec=ContainerImageHelper)
        container.resolve_to_digest = AsyncMock(side_effect=lambda p: p + "@sha256:abc123")
        container.get_installed_packages = AsyncMock(return_value=["glibc", "openssl", "rpm"])
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
            "quay.io/test/builder:latest",
            "quay.io/test/base:latest",
        ]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM builder AS build\n"
                "RUN dnf -y update && yum clean all\n"
                "RUN dnf install -y gcc\n"
                "\n"
                "FROM base\n"
                "COPY --from=build /app /app\n"
            )

            asyncio.run(generator.generate_lockfile(self._make_mock_image_meta(), dest_dir))

        # Stage 0 has bare update — resolver should receive upgradePackages
        calls = resolver.resolve.call_args_list
        self.assertGreaterEqual(len(calls), 1)
        first_config = calls[0].args[0]
        self.assertIn("glibc", first_config.upgradePackages)
        self.assertIn("openssl", first_config.upgradePackages)

    def test_upgrade_packages_dropped_on_failure(self):
        """
        When upgrade packages from bare updates cause resolution failure,
        they are all dropped and upgrades_dropped is set to True.
        """
        call_count = 0

        async def mock_resolve(config, image_pullspec=None, **kwargs):
            nonlocal call_count
            call_count += 1
            if config.upgradePackages and "glibc" in config.upgradePackages:
                raise RuntimeError("No match for argument: glibc")
            return FAKE_LOCKFILE_DATA.model_copy(deep=True)

        container = MagicMock(spec=ContainerImageHelper)
        container.resolve_to_digest = AsyncMock(side_effect=lambda p: p)
        container.get_installed_packages = AsyncMock(return_value=[])
        container.read_file_from_image = AsyncMock(return_value="")

        resolver = MagicMock(spec=RpmResolver)
        resolver.resolve = AsyncMock(side_effect=mock_resolve)

        generator = RpmLockfilePrototypeGenerator(
            repos=self._make_mock_repos(),
            working_dir=Path(tempfile.mkdtemp()),
            container_helper=container,
            resolver=resolver,
        )

        repos = [RepoEntry(repoid="baseos", baseurl="https://example.com/$basearch/")]

        result = asyncio.run(
            generator._resolve_stage_with_retry(
                repo_list=repos,
                arches=["x86_64"],
                packages=[],
                image_pullspec="quay.io/test/base@sha256:abc123",
                distgit_key="test-image",
                stage_num=0,
                upgrade_packages=["glibc", "openssl", "rpm"],
            )
        )

        self.assertIsNotNone(result)
        self.assertTrue(generator.upgrades_dropped)
        # First call fails (glibc in upgradePackages), second succeeds without them
        self.assertEqual(call_count, 2)

    def test_bare_update_final_stage_disables_reinstall(self):
        """
        When the final stage has a bare dnf update, reinstallPackages
        must be cleared to avoid pinning installed EVRs which would
        silently suppress the upgrade.
        """
        container = MagicMock(spec=ContainerImageHelper)
        container.resolve_to_digest = AsyncMock(side_effect=lambda p: p + "@sha256:abc123")
        container.get_installed_packages = AsyncMock(return_value=["glibc", "openssl"])
        container.read_file_from_image = AsyncMock(return_value="")

        resolver = MagicMock(spec=RpmResolver)
        resolver.resolve = AsyncMock(return_value=FAKE_LOCKFILE_DATA.model_copy(deep=True))

        generator = RpmLockfilePrototypeGenerator(
            repos=self._make_mock_repos(),
            working_dir=Path(tempfile.mkdtemp()),
            container_helper=container,
            resolver=resolver,
        )
        generator.downstream_parents = ["quay.io/test/base:latest"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN dnf install -y gcc\nRUN dnf -y update && yum clean all\n"
            )

            asyncio.run(generator.generate_lockfile(self._make_mock_image_meta(), dest_dir))

        # Final stage has bare update — resolver should receive
        # upgradePackages but NOT reinstallPackages
        calls = resolver.resolve.call_args_list
        self.assertGreaterEqual(len(calls), 1)
        config = calls[0].args[0]
        self.assertIn("glibc", config.upgradePackages)
        self.assertEqual(config.reinstallPackages, [])

    def test_bare_update_stage_alias_drops_upgrades(self):
        """
        When a stage with a bare update uses a stage alias (no pullspec),
        upgrades_dropped must be set so the bare update is stripped.
        """
        container = MagicMock(spec=ContainerImageHelper)
        container.resolve_to_digest = AsyncMock(side_effect=lambda p: p + "@sha256:abc123")
        container.get_installed_packages = AsyncMock(return_value=[])
        container.read_file_from_image = AsyncMock(return_value="")

        resolver = MagicMock(spec=RpmResolver)
        resolver.resolve = AsyncMock(return_value=FAKE_LOCKFILE_DATA.model_copy(deep=True))

        generator = RpmLockfilePrototypeGenerator(
            repos=self._make_mock_repos(),
            working_dir=Path(tempfile.mkdtemp()),
            container_helper=container,
            resolver=resolver,
        )
        # Stage 0 is a stage alias (no "/"), stage 1 is real
        generator.downstream_parents = ["build", "quay.io/test/base:latest"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM scratch AS build\nRUN dnf -y update && yum clean all\n\nFROM base\nCOPY --from=build /app /app\n"
            )

            asyncio.run(generator.generate_lockfile(self._make_mock_image_meta(), dest_dir))

        self.assertTrue(generator.upgrades_dropped)
