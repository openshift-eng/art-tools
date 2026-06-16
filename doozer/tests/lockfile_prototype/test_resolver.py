"""
Tests for doozerlib.lockfile_prototype.resolver.
"""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import yaml
from doozerlib.lockfile_prototype.constants import (
    DEFAULT_RPM_LOCKFILE_NAME,
    RPM_LOCKFILE_CONTAINERFILE,
    RPM_LOCKFILE_IMAGE,
)
from doozerlib.lockfile_prototype.models import (
    LockfileData,
    RpmsInConfig,
)
from doozerlib.lockfile_prototype.resolver import RpmResolver


class TestEnsureImage(unittest.TestCase):
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_skips_build_when_image_exists(self, mock_gather):
        """
        When podman image exists returns 0, no build should happen.
        """
        call_log = []

        async def mock_cmd(cmd, **kwargs):
            call_log.append(cmd)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        asyncio.run(resolver._ensure_image())
        self.assertEqual(len(call_log), 1)
        self.assertEqual(call_log[0][:3], ["podman", "image", "exists"])

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_builds_when_image_missing(self, mock_gather):
        """
        When podman image exists returns non-zero, should build.
        """
        call_log = []

        async def mock_cmd(cmd, **kwargs):
            call_log.append(cmd)
            if cmd[1] == "image":
                return (1, "", "")
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        asyncio.run(resolver._ensure_image())
        self.assertEqual(len(call_log), 2)
        self.assertEqual(call_log[1][:2], ["podman", "build"])
        self.assertIn("-f", call_log[1])
        self.assertIn(str(RPM_LOCKFILE_CONTAINERFILE), call_log[1])

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_raises_on_build_failure(self, mock_gather):
        """
        Build failure should raise RuntimeError.
        """

        async def mock_cmd(cmd, **kwargs):
            if cmd[1] == "image":
                return (1, "", "")
            return (1, "", "STEP 2/7: RUN dnf install ... error")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(resolver._ensure_image())
        self.assertIn("Failed to build", str(ctx.exception))


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

    def _mock_podman_run(self, cmd, expect_bare=False, expect_image=None, **kwargs):
        """
        Helper to create a mock for podman run that writes fake lockfile output.
        Returns (rc, stdout, stderr).
        """
        self.assertEqual(cmd[0], "podman")
        self.assertEqual(cmd[1], "run")
        self.assertIn("--rm", cmd)

        if expect_bare:
            self.assertIn("--bare", cmd)
            self.assertNotIn("--image", cmd)
        if expect_image:
            img_idx = cmd.index("--image") + 1
            self.assertEqual(cmd[img_idx], expect_image)

        for i, arg in enumerate(cmd):
            if arg == "-v" and ":/work:" in cmd[i + 1]:
                host_tmpdir = cmd[i + 1].split(":")[0]
                break
        host_outfile = os.path.join(host_tmpdir, DEFAULT_RPM_LOCKFILE_NAME)
        with open(host_outfile, "w") as f:
            yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
        return (0, "", "")

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_bare_mode(self, mock_gather, mock_ensure):
        """
        Without image_pullspec, should pass --bare.
        """

        async def mock_cmd(cmd, **kwargs):
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["nfs-utils"],
        )
        result = asyncio.run(resolver.resolve(config))
        self.assertIsInstance(result, LockfileData)
        self.assertEqual(result.lockfileVersion, 1)

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_with_image(self, mock_gather, mock_ensure):
        """
        With image_pullspec, should pass --image.
        """

        async def mock_cmd(cmd, **kwargs):
            return self._mock_podman_run(cmd, expect_image="quay.io/test/img@sha256:abc")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        result = asyncio.run(resolver.resolve(config, image_pullspec="quay.io/test/img@sha256:abc"))
        self.assertIsInstance(result, LockfileData)
        self.assertEqual(result.lockfileVersion, 1)

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_failure(self, mock_gather, mock_ensure):
        """
        Non-zero exit should raise RuntimeError.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", "DNF dependency resolution failed")

        mock_gather.side_effect = mock_fail
        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["foo"],
        )
        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(resolver.resolve(config))
        self.assertIn("rpm-lockfile-prototype failed", str(ctx.exception))

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_uses_podman(self, mock_gather, mock_ensure):
        """
        Should invoke podman run, not system python.
        """

        async def mock_cmd(cmd, **kwargs):
            self.assertEqual(cmd[0], "podman")
            self.assertEqual(cmd[1], "run")
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        asyncio.run(resolver.resolve(config))
        mock_gather.assert_called_once()

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_mounts_dnf_cache(self, mock_gather, mock_ensure):
        """
        Should mount DNF cache and set env var.
        """
        captured_cmds = []

        async def mock_cmd(cmd, **kwargs):
            captured_cmds.append(cmd)
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["nfs-utils"],
        )
        asyncio.run(resolver.resolve(config))
        cmd = captured_cmds[0]
        dnf_cache_set = False
        for i, arg in enumerate(cmd):
            if arg == "-e" and i + 1 < len(cmd) and cmd[i + 1] == "RPM_LOCKFILE_PROTOTYPE_DNF_CACHE=/cache":
                dnf_cache_set = True
        self.assertTrue(dnf_cache_set)

    @patch.dict(os.environ, {"REGISTRY_AUTH_FILE": "/run/containers/auth.json"})
    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_mounts_auth_file(self, mock_gather, mock_ensure):
        """
        When REGISTRY_AUTH_FILE is set, should mount it and set env var.
        """
        captured_cmds = []

        async def mock_cmd(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        asyncio.run(resolver.resolve(config))
        cmd = captured_cmds[0]
        auth_mount = "/run/containers/auth.json:/auth/auth.json:ro,Z"
        self.assertTrue(
            any(arg == "-v" and cmd[i + 1] == auth_mount for i, arg in enumerate(cmd) if i + 1 < len(cmd)),
            f"Expected auth file mount {auth_mount} in command",
        )
        auth_env_set = any(
            arg == "-e" and i + 1 < len(cmd) and cmd[i + 1] == "REGISTRY_AUTH_FILE=/auth/auth.json"
            for i, arg in enumerate(cmd)
        )
        self.assertTrue(auth_env_set)

    def test_custom_image_parameter(self):
        """
        Image parameter should override default.
        """
        resolver = RpmResolver(image="quay.io/custom/rpm-lockfile:v1.0")
        self.assertEqual(resolver._image, "quay.io/custom/rpm-lockfile:v1.0")

    def test_default_image(self):
        """
        Default image should match constant.
        """
        resolver = RpmResolver()
        self.assertEqual(resolver._image, RPM_LOCKFILE_IMAGE)


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
        self.resolver = RpmResolver()

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

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_retries_on_rpmdb_corruption(self, mock_gather, mock_clear, mock_ensure):
        """
        First call fails with corruption, cache cleared, second call succeeds.
        """
        call_count = 0

        async def mock_cmd(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (1, "", self.CORRUPTION_STDERR)
            for i, arg in enumerate(cmd):
                if arg == "-v" and ":/work:" in cmd[i + 1]:
                    host_tmpdir = cmd[i + 1].split(":")[0]
                    break
            host_outfile = os.path.join(host_tmpdir, DEFAULT_RPM_LOCKFILE_NAME)
            with open(host_outfile, "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        mock_clear.return_value = True

        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        result = asyncio.run(resolver.resolve(config, image_pullspec="registry.example.com/repo@sha256:abc123"))
        self.assertIsInstance(result, LockfileData)
        self.assertEqual(call_count, 2)
        mock_clear.assert_called_once()

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_raises_after_retry_fails(self, mock_gather, mock_clear, mock_ensure):
        """
        Both calls fail with corruption — should raise RuntimeError.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", self.CORRUPTION_STDERR)

        mock_gather.side_effect = mock_fail
        mock_clear.return_value = True

        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(resolver.resolve(config, image_pullspec="registry.example.com/repo@sha256:abc123"))
        self.assertIn("rpm-lockfile-prototype failed", str(ctx.exception))

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_no_retry_without_image(self, mock_gather, mock_clear, mock_ensure):
        """
        Bare mode (no image_pullspec) should not attempt cache clear.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", self.CORRUPTION_STDERR)

        mock_gather.side_effect = mock_fail

        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        with self.assertRaises(RuntimeError):
            asyncio.run(resolver.resolve(config))
        mock_clear.assert_not_called()

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._ensure_image", new_callable=AsyncMock)
    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_no_retry_on_other_errors(self, mock_gather, mock_clear, mock_ensure):
        """
        Non-corruption errors should raise immediately without retry.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", "No match for argument: missing-pkg\n")

        mock_gather.side_effect = mock_fail

        resolver = RpmResolver()
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        with self.assertRaises(RuntimeError):
            asyncio.run(resolver.resolve(config, image_pullspec="registry.example.com/repo@sha256:abc123"))
        mock_clear.assert_not_called()
