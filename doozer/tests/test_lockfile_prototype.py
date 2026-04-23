"""
Tests for the rpm-lockfile-prototype integration.
"""

import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import yaml
from doozerlib.lockfile_prototype import (
    RpmLockfilePrototypeGenerator,
    build_rpms_in_yaml,
    extract_dockerfile_packages_per_stage,
)


class TestExtractDockerfilePackages(unittest.TestCase):
    def _write_dockerfile(self, tmpdir: str, content: str) -> Path:
        path = Path(tmpdir) / "Dockerfile"
        path.write_text(content)
        return path

    def test_per_stage_extraction(self):
        with TemporaryDirectory() as tmpdir:
            content = (
                "FROM builder AS build\n"
                "RUN dnf install -y gcc nmstate-devel git\n"
                "\n"
                "FROM base-rhel9\n"
                "RUN dnf install -y postgresql-server skopeo\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            result = extract_dockerfile_packages_per_stage(path)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0], ["gcc", "git", "nmstate-devel"])
            self.assertEqual(result[1], ["postgresql-server", "skopeo"])

    def test_per_stage_single_stage(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN yum -y install nfs-utils jq\n"
            path = self._write_dockerfile(tmpdir, content)
            result = extract_dockerfile_packages_per_stage(path)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], ["jq", "nfs-utils"])


class TestBuildRpmsInYaml(unittest.TestCase):
    def test_basic_structure(self):
        repos = [
            {
                "name": "rhel-9-baseos-rpms",
                "baseurl": "https://example.com/baseos/$basearch/os/",
            },
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64", "ppc64le"],
            packages=["nfs-utils", "jq"],
        )
        self.assertEqual(result["arches"], ["x86_64", "ppc64le"])
        self.assertEqual(len(result["contentOrigin"]["repos"]), 1)
        self.assertEqual(result["contentOrigin"]["repos"][0]["repoid"], "rhel-9-baseos-rpms")
        self.assertEqual(result["packages"], ["nfs-utils", "jq"])

    def test_arch_specific_packages(self):
        repos = [
            {
                "name": "rhel-9-baseos-rpms",
                "baseurl": "https://example.com/baseos/$basearch/os/",
            },
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64", "ppc64le"],
            packages=["nfs-utils"],
            arch_specific_packages={"ppc64le": ["librtas"]},
        )
        self.assertIn("nfs-utils", result["packages"])
        arch_entries = [p for p in result["packages"] if isinstance(p, dict)]
        self.assertEqual(len(arch_entries), 1)
        self.assertEqual(arch_entries[0]["name"], "librtas")
        self.assertEqual(arch_entries[0]["arches"]["only"], "ppc64le")

    def test_multiple_repos(self):
        repos = [
            {
                "name": "rhel-9-baseos-rpms",
                "baseurl": "https://example.com/baseos/$basearch/os/",
            },
            {
                "name": "rhel-9-appstream-rpms",
                "baseurl": "https://example.com/appstream/$basearch/os/",
            },
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64"],
            packages=["httpd"],
        )
        self.assertEqual(len(result["contentOrigin"]["repos"]), 2)
        repoids = [r["repoid"] for r in result["contentOrigin"]["repos"]]
        self.assertEqual(repoids, ["rhel-9-baseos-rpms", "rhel-9-appstream-rpms"])


class TestRpmLockfilePrototypeGenerator(unittest.TestCase):
    def _make_mock_repo(self, name: str, baseurl: str) -> MagicMock:
        repo = MagicMock()
        repo.name = name
        repo.baseurl.return_value = baseurl
        return repo

    def _make_mock_repos(self) -> MagicMock:
        repos = MagicMock()
        baseos = self._make_mock_repo(
            "rhel-9-baseos-rpms",
            "https://example.com/baseos/x86_64/os/",
        )
        appstream = self._make_mock_repo(
            "rhel-9-appstream-rpms",
            "https://example.com/appstream/x86_64/os/",
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

    async def _mock_cmd_gather_async(self, cmd, **kwargs):
        """
        Mock for cmd_gather_async that writes a fake lockfile to whatever
        --outfile path is in the command.
        """
        outfile_idx = cmd.index("--outfile") + 1
        outfile_path = Path(cmd[outfile_idx])
        lockfile = {
            "lockfileVersion": 1,
            "lockfileVendor": "redhat",
            "arches": [{"arch": "x86_64", "packages": [], "module_metadata": []}],
        }
        outfile_path.parent.mkdir(parents=True, exist_ok=True)
        with open(outfile_path, "w") as f:
            yaml.safe_dump(lockfile, f)
        return (0, "", "")

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_generate_lockfile_calls_prototype(self, mock_gather):
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils jq\n")
            mock_gather.side_effect = self._mock_cmd_gather_async

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        mock_gather.assert_called_once()
        call_args = mock_gather.call_args[0][0]
        self.assertEqual(call_args[0], "rpm-lockfile-prototype")

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_generate_lockfile_cleans_up_in_file(self, mock_gather):
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils\n")
            mock_gather.side_effect = self._mock_cmd_gather_async

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

            in_files = list(dest_dir.glob("*.in.yaml"))
            self.assertEqual(in_files, [])

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_generate_lockfile_fails_on_nonzero_exit(self, mock_gather):
        mock_gather.return_value = (1, "", "Error: missing package foo")

        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils\n")

            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(generator.generate_lockfile(meta, dest_dir))
            self.assertIn("rpm-lockfile-prototype failed", str(ctx.exception))

    def test_generate_lockfile_skips_when_disabled(self):
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()
        meta.is_lockfile_generation_enabled.return_value = False

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            asyncio.run(generator.generate_lockfile(meta, dest_dir))
            self.assertFalse((dest_dir / "rpms.lock.yaml").exists())
