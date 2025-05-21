import asyncio
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
from doozerlib.dnf import DnfManager
import unittest
import tempfile
import os
import yaml
from pathlib import Path
from doozerlib.dnf import RPMLockfileGenerator  # adjust import if needed


class TestRPMLockfileGenerator(unittest.TestCase):

    def test_generate_writes_correct_yaml_structure(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "rpm-lock.yaml"
            generator = RPMLockfileGenerator(str(output_path))

            arches_data = {
                "x86_64": {
                    "packages": [
                        {
                            "name": "bash",
                            "evr": "5.1-1",
                            "arch": "x86_64",
                            "repoid": "baseos",
                            "url": "http://example.com/bash.rpm",
                            "size": 1024,
                            "checksum": "sha1:abcdef123456",
                            "sourcerpm": "bash-5.1-1.src.rpm",
                        }
                    ],
                    "source": [],
                    "module_metadata": [],
                },
                "aarch64": {
                    "packages": [],
                    "source": ["custom_repo"],
                    "module_metadata": [{"name": "platform", "stream": "el8"}],
                },
            }

            generator.generate(arches_data)

            # File existence
            self.assertTrue(output_path.exists(), "Lockfile was not created")

            # Content correctness
            with open(output_path, "r") as f:
                content = yaml.safe_load(f)

            self.assertEqual(content["version"], 1)
            self.assertEqual(content["lockfileVendor"], "redhat")
            self.assertIn("x86_64", content["arches"])
            self.assertIn("aarch64", content["arches"])

            x86_pkgs = content["arches"]["x86_64"]["packages"]
            self.assertEqual(len(x86_pkgs), 1)
            self.assertEqual(x86_pkgs[0]["name"], "bash")
            self.assertEqual(x86_pkgs[0]["evr"], "5.1-1")

            aarch64_source = content["arches"]["aarch64"]["source"]
            self.assertEqual(aarch64_source, ["custom_repo"])
            self.assertEqual(content["arches"]["aarch64"]["module_metadata"][0]["name"], "platform")

    def test_generate_creates_parent_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_path = Path(tmpdir) / "nested" / "path" / "rpm-lock.yaml"
            generator = RPMLockfileGenerator(str(nested_path))

            generator.generate({
                "x86_64": {
                    "packages": [],
                    "source": [],
                    "module_metadata": [],
                }
            })

            self.assertTrue(nested_path.exists(), "Nested path was not created or file missing")

class TestDnfManager(TestCase):

    @patch("doozerlib.dnf.dnf.Base")
    def test_create_bases_on_init(self, mock_dnf_base):
        mock_base_instance = MagicMock()
        mock_dnf_base.return_value = mock_base_instance

        manager = DnfManager(
            repodir="/fake/repos",
            installroot_base="/fake/root",
            arches=["x86_64", "aarch64"]
        )

        self.assertIn("x86_64", manager._bases)
        self.assertIn("aarch64", manager._bases)
        self.assertIs(manager.get_base("x86_64"), mock_base_instance)

    def test_get_base_invalid_arch(self):
        manager = DnfManager(
            repodir="/fake/repos",
            installroot_base="/fake/root",
            arches=[]
        )

        with self.assertRaises(KeyError):
            manager.get_base("nonexistent")

    @patch("doozerlib.dnf.dnf.Base")
    @patch("doozerlib.dnf.dnf.module.module_base.ModuleBase")
    def test_install_packages_for_arches(self, mock_module_base_cls, mock_dnf_base_cls):
        import asyncio

        base_mock = MagicMock()
        base_mock.transaction.install_set = []
        base_mock.sack.query.return_value.filter.return_value = []
        base_mock.install_specs.return_value = None
        base_mock.resolve.return_value = None
        mock_dnf_base_cls.return_value = base_mock

        module_base_mock = MagicMock()
        module_base_mock.get_modules.return_value = [[]]
        mock_module_base_cls.return_value = module_base_mock

        manager = DnfManager(
            repodir="/fake/repos",
            installroot_base="/fake/root",
            arches=["x86_64"]
        )

        async def run_test():
            result = await manager.install_packages_for_arches(
                install_packages={"x86_64": ["bash"]},
                module_enable={"x86_64": set()},
                module_disable={"x86_64": set()},
                allow_erasing=False,
                no_sources=True
            )

            self.assertIn("x86_64", result)
            self.assertIsInstance(result["x86_64"], dict)
            self.assertSetEqual(set(result["x86_64"].keys()), {"packages", "source", "module_metadata"})

        asyncio.run(run_test())

    @patch("doozerlib.dnf.dnf.Base")
    @patch("doozerlib.dnf.dnf.module.module_base.ModuleBase")
    def test_install_packages_for_arches_returns_bash(self, mock_module_base, mock_base_cls):
        mock_pkg = MagicMock()
        mock_pkg.name = "bash"
        mock_pkg.e = "0"
        mock_pkg.v = "5.1"
        mock_pkg.r = "1"
        mock_pkg.a = "x86_64"
        mock_pkg.repoid = "baseos"
        mock_pkg.remote_location.return_value = "http://example.com/bash.rpm"
        mock_pkg.downloadsize = 1024
        mock_pkg.chksum = (1, b"\x12" * 32)
        mock_pkg.evr = "5.1-1"
        mock_pkg.sourcerpm = "bash-5.1-1.src.rpm"

        # Mock base setup
        mock_base = MagicMock()
        mock_base_cls.return_value = mock_base
        mock_base.sack.query.return_value.filter.return_value = [MagicMock()]
        mock_base.transaction.install_set = [mock_pkg]

        mock_module_base.return_value.get_modules.return_value = [[]]

        manager = DnfManager("/tmp/repos", "/tmp/root", ["x86_64"])

        result = asyncio.run(manager.install_packages_for_arches(
            {"x86_64": ["bash"]}
        ))

        pkg_info = result["x86_64"]["packages"][0]

        # Assert all fields
        expected = {
            "name": "bash",
            "evr": "5.1-1",
            "repoid": "baseos",
            "url": "http://example.com/bash.rpm",
            "size": 1024,
            "checksum": "sha1:1212121212121212121212121212121212121212121212121212121212121212",
            "sourcerpm": "bash-5.1-1.src.rpm",
        }

        for key, expected_value in expected.items():
            self.assertEqual(pkg_info[key], expected_value, f"{key} mismatch")
