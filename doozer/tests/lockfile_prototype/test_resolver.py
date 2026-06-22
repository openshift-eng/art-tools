"""
Tests for doozerlib.lockfile_prototype.resolver.
"""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from doozerlib.lockfile_prototype.models import (
    LockfileData,
    RpmsInConfig,
)
from doozerlib.lockfile_prototype.resolver import RpmResolver


class TestRpmResolver(unittest.TestCase):
    FAKE_LOCKFILE_DATA = {
        "lockfileVersion": 1,
        "lockfileVendor": "redhat",
        "arches": [
            {
                "arch": "x86_64",
                "packages": [
                    {
                        "url": "https://example.com/nfs-utils-2.5.4-1.el9.x86_64.rpm",
                        "repoid": "rhel-9-baseos-rpms",
                        "name": "nfs-utils",
                        "evr": "2.5.4-1.el9",
                    }
                ],
                "source": [],
                "module_metadata": [],
            }
        ],
    }

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_bare_mode(self, mock_gather):
        """
        Without image_pullspec, should pass --bare to the subprocess.
        """

        async def mock_cmd(cmd, **kwargs):
            self.assertIn("--bare", cmd)
            self.assertNotIn("--image", cmd)
            outfile_idx = cmd.index("--outfile") + 1
            with open(cmd[outfile_idx], "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["nfs-utils"],
        )
        result = asyncio.run(resolver.resolve(config))
        self.assertIsInstance(result, LockfileData)
        self.assertEqual(result.lockfileVersion, 1)

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_with_image(self, mock_gather):
        """
        With image_pullspec, should pass --image to the subprocess.
        """

        async def mock_cmd(cmd, **kwargs):
            self.assertIn("--image", cmd)
            image_idx = cmd.index("--image") + 1
            self.assertEqual(cmd[image_idx], "quay.io/test/img@sha256:abc")
            outfile_idx = cmd.index("--outfile") + 1
            with open(cmd[outfile_idx], "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        result = asyncio.run(resolver.resolve(config, image_pullspec="quay.io/test/img@sha256:abc"))
        self.assertIsInstance(result, LockfileData)
        self.assertEqual(result.lockfileVersion, 1)

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_failure(self, mock_gather):
        """
        Non-zero exit should raise RuntimeError.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", "DNF dependency resolution failed")

        mock_gather.side_effect = mock_fail
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["foo"],
        )
        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(resolver.resolve(config))
        self.assertIn("rpm-lockfile-prototype failed", str(ctx.exception))

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_uses_system_python(self, mock_gather):
        """
        Should invoke /usr/bin/python3 -c to use system Python.
        """

        async def mock_cmd(cmd, **kwargs):
            self.assertEqual(cmd[0], "/usr/bin/python3")
            self.assertEqual(cmd[1], "-c")
            outfile_idx = cmd.index("--outfile") + 1
            with open(cmd[outfile_idx], "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        asyncio.run(resolver.resolve(config))
        mock_gather.assert_called_once()

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_sets_dnf_cache_env(self, mock_gather):
        """
        RPM_LOCKFILE_PROTOTYPE_DNF_CACHE should be set in the subprocess
        env and point to the same directory across multiple resolve() calls.
        """
        captured_envs: list[dict] = []

        async def mock_cmd(cmd, **kwargs):
            captured_envs.append(dict(kwargs.get("env", {})))
            outfile_idx = cmd.index("--outfile") + 1
            with open(cmd[outfile_idx], "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["nfs-utils"],
        )
        asyncio.run(resolver.resolve(config))
        asyncio.run(resolver.resolve(config))

        self.assertEqual(len(captured_envs), 2)
        cache_dir_1 = captured_envs[0]["RPM_LOCKFILE_PROTOTYPE_DNF_CACHE"]
        cache_dir_2 = captured_envs[1]["RPM_LOCKFILE_PROTOTYPE_DNF_CACHE"]
        self.assertEqual(cache_dir_1, cache_dir_2)
        self.assertTrue(os.path.isdir(cache_dir_1))


class TestParseMissingPackages(unittest.TestCase):
    def test_cli_format(self):
        error = "missing packages: dmidecode\n"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"dmidecode"})

    def test_cli_format_multiple(self):
        error = "missing packages: dmidecode, microcode_ctl\n"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"dmidecode", "microcode_ctl"})

    def test_dnf_format(self):
        error = "No match for argument: dmidecode\nNo match for argument: microcode_ctl"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"dmidecode", "microcode_ctl"})

    def test_mixed_format(self):
        error = (
            "ERROR:dnf:No match for argument: dmidecode\nERROR:root:Problems in request:\nmissing packages: dmidecode\n"
        )
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"dmidecode"})

    def test_packages_not_installed_error_format(self):
        """
        DNF PackagesNotInstalledError outputs "No match for argument: <pkg>: <pkg>".
        The trailing colon-separated message must not pollute the package name.
        """
        error = (
            "dnf.exceptions.PackagesNotInstalledError: "
            "No match for argument: policycoreutils-python-utils: policycoreutils-python-utils"
        )
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"policycoreutils-python-utils"})

    def test_reinstall_not_available_format(self):
        """
        DNF PackagesNotAvailableError from base.reinstall() outputs
        "no package matched: <pkg>" when the installed version is not
        in the configured repos.
        """
        error = "dnf.exceptions.PackagesNotAvailableError: no package matched: git"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"git"})

    def test_no_match(self):
        error = "Some other error message\n"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, set())


class TestIsRpmdbCorrupt(unittest.TestCase):
    def test_detects_malformed_database(self):
        stderr = (
            "error: sqlite failure: CREATE TABLE IF NOT EXISTS 'Packages' "
            "(hnum INTEGER PRIMARY KEY AUTOINCREMENT,blob BLOB NOT NULL): "
            "database disk image is malformed"
        )
        self.assertTrue(RpmResolver._is_rpmdb_corrupt(stderr))

    def test_detects_failed_loading_rpmdb(self):
        stderr = "OSError: failed loading RPMDB\n"
        self.assertTrue(RpmResolver._is_rpmdb_corrupt(stderr))

    def test_no_false_positive(self):
        stderr = "No match for argument: foo\n"
        self.assertFalse(RpmResolver._is_rpmdb_corrupt(stderr))

    def test_empty_stderr(self):
        self.assertFalse(RpmResolver._is_rpmdb_corrupt(""))


class TestClearRpmdbCache(unittest.TestCase):
    def setUp(self):
        self.resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))

    def test_clears_cache_for_digest(self):
        """
        Should delete the cache directory matching the digest.
        """
        pullspec = "registry.example.com/repo@sha256:abc123def456"
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_cache = Path(tmpdir) / "rpmdbs"
            cache_entry = fake_cache / "amd64" / "sha256:abc123def456"
            cache_entry.mkdir(parents=True)
            (cache_entry / "Packages").touch()

            other_entry = fake_cache / "amd64" / "sha256:other"
            other_entry.mkdir(parents=True)
            (other_entry / "Packages").touch()

            with patch("doozerlib.lockfile_prototype.resolver.RPMDB_CACHE_PATH", fake_cache):
                cleared = self.resolver._clear_rpmdb_cache(pullspec)

            self.assertTrue(cleared)
            self.assertFalse(cache_entry.exists())
            self.assertTrue(other_entry.exists())

    def test_clears_across_arches(self):
        """
        Should delete cache entries for the digest across all arch subdirectories.
        """
        pullspec = "registry.example.com/repo@sha256:abc123def456"
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_cache = Path(tmpdir) / "rpmdbs"
            for arch in ("amd64", "arm64", "s390x"):
                entry = fake_cache / arch / "sha256:abc123def456"
                entry.mkdir(parents=True)
                (entry / "Packages").touch()

            with patch("doozerlib.lockfile_prototype.resolver.RPMDB_CACHE_PATH", fake_cache):
                cleared = self.resolver._clear_rpmdb_cache(pullspec)

            self.assertTrue(cleared)
            for arch in ("amd64", "arm64", "s390x"):
                self.assertFalse((fake_cache / arch / "sha256:abc123def456").exists())

    def test_no_digest_in_pullspec(self):
        """
        Should return False when pullspec has no digest.
        """
        cleared = self.resolver._clear_rpmdb_cache("registry.example.com/repo:latest")
        self.assertFalse(cleared)

    def test_cache_dir_missing(self):
        """
        Should return False when cache directory does not exist.
        """
        with patch("doozerlib.lockfile_prototype.resolver.RPMDB_CACHE_PATH", Path("/nonexistent/path")):
            cleared = self.resolver._clear_rpmdb_cache("registry.example.com/repo@sha256:abc123")
        self.assertFalse(cleared)


class TestResolveRpmdbCorruptionRetry(unittest.TestCase):
    FAKE_LOCKFILE_DATA = {
        "lockfileVersion": 1,
        "lockfileVendor": "redhat",
        "arches": [
            {
                "arch": "x86_64",
                "packages": [],
                "source": [],
                "module_metadata": [],
            }
        ],
    }

    CORRUPTION_STDERR = "error: sqlite failure: database disk image is malformed\nOSError: failed loading RPMDB\n"

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_retries_on_rpmdb_corruption(self, mock_gather, mock_clear):
        """
        First call fails with corruption, cache cleared, second call succeeds.
        """
        call_count = 0

        async def mock_cmd(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (1, "", self.CORRUPTION_STDERR)
            outfile_idx = cmd.index("--outfile") + 1
            with open(cmd[outfile_idx], "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        mock_clear.return_value = True

        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        result = asyncio.run(resolver.resolve(config, image_pullspec="registry.example.com/repo@sha256:abc123"))
        self.assertIsInstance(result, LockfileData)
        self.assertEqual(call_count, 2)
        mock_clear.assert_called_once()

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_raises_after_retry_fails(self, mock_gather, mock_clear):
        """
        Both calls fail with corruption — should raise RuntimeError.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", self.CORRUPTION_STDERR)

        mock_gather.side_effect = mock_fail
        mock_clear.return_value = True

        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(resolver.resolve(config, image_pullspec="registry.example.com/repo@sha256:abc123"))
        self.assertIn("rpm-lockfile-prototype failed", str(ctx.exception))

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_no_retry_without_image(self, mock_gather, mock_clear):
        """
        Bare mode (no image_pullspec) should not attempt cache clear.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", self.CORRUPTION_STDERR)

        mock_gather.side_effect = mock_fail

        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        with self.assertRaises(RuntimeError):
            asyncio.run(resolver.resolve(config))
        mock_clear.assert_not_called()

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_no_retry_on_other_errors(self, mock_gather, mock_clear):
        """
        Non-corruption errors should raise immediately without retry.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", "No match for argument: missing-pkg\n")

        mock_gather.side_effect = mock_fail

        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        with self.assertRaises(RuntimeError):
            asyncio.run(resolver.resolve(config, image_pullspec="registry.example.com/repo@sha256:abc123"))
        mock_clear.assert_not_called()
