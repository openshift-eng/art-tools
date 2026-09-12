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
from doozerlib.lockfile_prototype.constants import (
    DEFAULT_RPM_INFILE_NAME,
    DEFAULT_RPM_LOCKFILE_NAME,
    RPM_LOCKFILE_IMAGE,
)
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
            self.assertNotIn("--bare", cmd)
            img_idx = cmd.index("--image") + 1
            self.assertEqual(cmd[img_idx], expect_image)

        host_tmpdir = None
        for i, arg in enumerate(cmd):
            if arg == "-v" and ":/work:" in cmd[i + 1]:
                work_spec = cmd[i + 1]
                options = work_spec.split(":")[2] if len(work_spec.split(":")) > 2 else ""
                self.assertNotIn("ro", options.split(","), f"/work mount must be writable: {work_spec}")
                host_tmpdir = work_spec.split(":")[0]
                break
        self.assertIsNotNone(host_tmpdir, "No /work mount found in podman command")
        host_outfile = os.path.join(host_tmpdir, DEFAULT_RPM_LOCKFILE_NAME)
        with open(host_outfile, "w") as f:
            yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
        return (0, "", "")

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_bare_mode(self, mock_gather):
        """
        Without image_pullspec, should pass --bare.
        """

        async def mock_cmd(cmd, **kwargs):
            return self._mock_podman_run(cmd, expect_bare=True)

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
        With image_pullspec, should pass --image.
        """

        async def mock_cmd(cmd, **kwargs):
            return self._mock_podman_run(cmd, expect_image="quay.io/test/img@sha256:abc")

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
    def test_resolve_uses_podman(self, mock_gather):
        """
        Should invoke podman run, not system python.
        """

        async def mock_cmd(cmd, **kwargs):
            self.assertEqual(cmd[0], "podman")
            self.assertEqual(cmd[1], "run")
            return self._mock_podman_run(cmd, expect_bare=True)

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
    def test_resolve_mounts_dnf_cache(self, mock_gather):
        """
        Should mount DNF cache and set env var.
        """
        captured_cmds = []

        async def mock_cmd(cmd, **kwargs):
            captured_cmds.append(cmd)
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["nfs-utils"],
        )
        asyncio.run(resolver.resolve(config))
        cmd = captured_cmds[0]
        dnf_cache_env = any(
            arg == "-e" and i + 1 < len(cmd) and cmd[i + 1] == "RPM_LOCKFILE_PROTOTYPE_DNF_CACHE=/cache"
            for i, arg in enumerate(cmd)
        )
        self.assertTrue(dnf_cache_env, "RPM_LOCKFILE_PROTOTYPE_DNF_CACHE=/cache env not set")
        dnf_cache_mount = any(
            arg == "-v"
            and i + 1 < len(cmd)
            and cmd[i + 1].split(":")[1] == "/cache"
            and "ro" not in cmd[i + 1].split(":")[2].split(",")
            for i, arg in enumerate(cmd)
        )
        self.assertTrue(dnf_cache_mount, "No writable volume mount targeting /cache found in podman command")

    @patch.dict(os.environ, {"REGISTRY_AUTH_FILE": "/run/containers/auth.json"})
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_resolve_mounts_auth_file(self, mock_gather):
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
        auth_mount = "/run/containers/auth.json:/auth/auth.json:ro,z"
        self.assertTrue(
            any(arg == "-v" and cmd[i + 1] == auth_mount for i, arg in enumerate(cmd) if i + 1 < len(cmd)),
            f"Expected auth file mount {auth_mount} in command",
        )
        auth_env_set = any(
            arg == "-e" and i + 1 < len(cmd) and cmd[i + 1] == "REGISTRY_AUTH_FILE=/auth/auth.json"
            for i, arg in enumerate(cmd)
        )
        self.assertTrue(auth_env_set)

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_custom_image_parameter(self, mock_gather):
        """
        Image parameter should override default and be passed to podman run.
        """
        captured_cmds = []

        async def mock_cmd(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        custom_image = "quay.io/custom/rpm-lockfile:v1.0"
        resolver = RpmResolver(image=custom_image, working_dir=Path(tempfile.mkdtemp()))
        self.assertEqual(resolver._image, custom_image)
        config = RpmsInConfig(arches=["x86_64"], contentOrigin={"repos": []}, packages=[])
        asyncio.run(resolver.resolve(config))
        self.assertIn(custom_image, captured_cmds[0])

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_default_image(self, mock_gather):
        """
        Default image should match constant and be passed to podman run.
        """
        captured_cmds = []

        async def mock_cmd(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        self.assertEqual(resolver._image, RPM_LOCKFILE_IMAGE)
        config = RpmsInConfig(arches=["x86_64"], contentOrigin={"repos": []}, packages=[])
        asyncio.run(resolver.resolve(config))
        self.assertIn(RPM_LOCKFILE_IMAGE, captured_cmds[0])

    def test_rpmdb_cache_path_in_jenkins(self):
        """
        When JENKINS_HOME is set, _rpmdb_cache_path should point directly to
        JENKINS_CACHE_DIR/rpmdbs (preserving the pre-containerization path).
        """
        with patch.dict(os.environ, {"JENKINS_HOME": "/var/jenkins"}):
            resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        self.assertEqual(
            str(resolver._rpmdb_cache_path),
            "/mnt/jenkins-workspace/rpm-lockfile-cache/rpmdbs",
        )

    @patch("doozerlib.lockfile_prototype.resolver.Path.mkdir")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_rpmdb_cache_mount_in_jenkins(self, mock_gather, mock_mkdir):
        """
        In Jenkins, JENKINS_CACHE_DIR must be mounted at
        XDG_CACHE_HOME/rpm-lockfile-prototype so rpmdbs land at
        JENKINS_CACHE_DIR/rpmdbs (same path as before containerization).
        """
        captured_cmds = []

        async def mock_cmd(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            return self._mock_podman_run(cmd, expect_bare=True)

        mock_gather.side_effect = mock_cmd
        with patch.dict(os.environ, {"JENKINS_HOME": "/var/jenkins"}):
            resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
            config = RpmsInConfig(arches=["x86_64"], contentOrigin={"repos": []}, packages=[])
            asyncio.run(resolver.resolve(config))

        cmd = captured_cmds[0]
        expected_mount = "/mnt/jenkins-workspace/rpm-lockfile-cache:/rpmdb-cache/rpm-lockfile-prototype:z"
        self.assertTrue(
            any(arg == "-v" and cmd[i + 1] == expected_mount for i, arg in enumerate(cmd) if i + 1 < len(cmd)),
            f"Expected Jenkins RPMDB mount {expected_mount!r} in command",
        )
        self.assertTrue(
            any(
                arg == "-e" and i + 1 < len(cmd) and cmd[i + 1] == "XDG_CACHE_HOME=/rpmdb-cache"
                for i, arg in enumerate(cmd)
            ),
            "Expected XDG_CACHE_HOME=/rpmdb-cache in podman command",
        )

    def test_rpmdb_cache_path_outside_jenkins(self):
        """
        When JENKINS_HOME is not set, _rpmdb_cache_path should fall
        back to ~/.cache.
        """
        env = os.environ.copy()
        env.pop("JENKINS_HOME", None)
        env.pop("XDG_CACHE_HOME", None)
        with patch.dict(os.environ, env, clear=True):
            resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        expected = Path.home() / ".cache" / "rpm-lockfile-prototype" / "rpmdbs"
        self.assertEqual(resolver._rpmdb_cache_path, expected)

    def test_relative_xdg_cache_home_falls_back(self):
        """
        A relative XDG_CACHE_HOME must be rejected and fall back to ~/.cache
        so that podman -v never receives a relative mount source.
        """
        env = os.environ.copy()
        env.pop("JENKINS_HOME", None)
        env["XDG_CACHE_HOME"] = "."
        with patch.dict(os.environ, env, clear=True):
            resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        expected = Path.home() / ".cache" / "rpm-lockfile-prototype" / "rpmdbs"
        self.assertEqual(resolver._rpmdb_cache_path, expected)


class TestPackagesFromContainerfile(unittest.TestCase):
    FAKE_LOCKFILE_DATA = TestRpmResolver.FAKE_LOCKFILE_DATA

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_packages_from_containerfile_injected_into_config(self, mock_gather):
        """
        When containerfile_path and stage_num are provided, the config
        written to rpms.in.yaml must include packagesFromContainerfile
        with the file path and 1-indexed stageNum.
        """
        captured_configs: list[dict] = []
        captured_cmds: list[list] = []

        async def mock_cmd(cmd, **kwargs):
            captured_cmds.append(list(cmd))
            for i, arg in enumerate(cmd):
                if arg == "-v" and ":/work:" in cmd[i + 1]:
                    work_spec = cmd[i + 1]
                    options = work_spec.split(":")[2] if len(work_spec.split(":")) > 2 else ""
                    assert "ro" not in options.split(","), f"/work mount must be writable: {work_spec}"
                    host_tmpdir = work_spec.split(":")[0]
                    break
            with open(os.path.join(host_tmpdir, DEFAULT_RPM_INFILE_NAME)) as f:
                captured_configs.append(yaml.safe_load(f))
            with open(os.path.join(host_tmpdir, DEFAULT_RPM_LOCKFILE_NAME), "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=["extra-pkg"],
        )
        asyncio.run(
            resolver.resolve(
                config,
                image_pullspec="quay.io/test/img@sha256:abc",
                containerfile_path="/path/to/Dockerfile",
                stage_num=2,
            )
        )

        self.assertEqual(len(captured_configs), 1)
        pfc = captured_configs[0].get("packagesFromContainerfile")
        self.assertIsNotNone(pfc)
        # Container-side path — the host Containerfile is mounted at /work/Containerfile
        self.assertEqual(pfc["file"], "Containerfile")
        self.assertEqual(pfc["stageNum"], 2)

        # Host Containerfile must be mounted into the container
        cmd = captured_cmds[0]
        expected_mount = "/path/to/Dockerfile:/work/Containerfile:ro,Z"
        self.assertTrue(
            any(arg == "-v" and cmd[i + 1] == expected_mount for i, arg in enumerate(cmd) if i + 1 < len(cmd)),
            f"Expected Containerfile mount {expected_mount!r} in command",
        )

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_no_packages_from_containerfile_when_not_set(self, mock_gather):
        """
        Without containerfile_path, packagesFromContainerfile should not
        appear in the config.
        """
        captured_configs: list[dict] = []

        async def mock_cmd(cmd, **kwargs):
            for i, arg in enumerate(cmd):
                if arg == "-v" and ":/work:" in cmd[i + 1]:
                    work_spec = cmd[i + 1]
                    options = work_spec.split(":")[2] if len(work_spec.split(":")) > 2 else ""
                    assert "ro" not in options.split(","), f"/work mount must be writable: {work_spec}"
                    host_tmpdir = work_spec.split(":")[0]
                    break
            with open(os.path.join(host_tmpdir, DEFAULT_RPM_INFILE_NAME)) as f:
                captured_configs.append(yaml.safe_load(f))
            with open(os.path.join(host_tmpdir, DEFAULT_RPM_LOCKFILE_NAME), "w") as f:
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

        self.assertEqual(len(captured_configs), 1)
        self.assertIsNone(captured_configs[0].get("packagesFromContainerfile"))

    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_bare_context_is_written_to_config(self, mock_gather):
        """
        A bare context override must be preserved in the generated
        rpms.in.yaml input.
        """
        captured_configs: list[dict] = []

        async def mock_cmd(cmd, **kwargs):
            infile = cmd[-1]
            with open(infile) as f:
                captured_configs.append(yaml.safe_load(f))
            outfile_idx = cmd.index("--outfile") + 1
            with open(cmd[outfile_idx], "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            context={"bare": True},
            packages=["nfs-utils"],
        )
        asyncio.run(resolver.resolve(config))

        self.assertEqual(len(captured_configs), 1)
        self.assertEqual(captured_configs[0]["context"], {"bare": True})


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

    def test_glob_pattern_in_missing_packages(self):
        """
        DNF glob patterns like *-server-ose* can appear in missing packages
        errors. VALID_PKG_NAME must accept wildcards so the retry loop can
        strip them.
        """
        error = "missing packages: *-server-ose*"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"*-server-ose*"})

    def test_local_rpm_path_glob(self):
        """
        Local RPM path globs like /root/rpmbuild/RPMS/x86_64/pkcs11-helper*
        must be returned so the retry loop can add them to excludePackages.
        The upstream tool uses set subtraction to apply excludePackages, so
        the exact extracted string will be excluded on retry.
        """
        error = "ERROR:dnf:No match for argument: /root/rpmbuild/RPMS/x86_64/pkcs11-helper*"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"/root/rpmbuild/RPMS/x86_64/pkcs11-helper*"})

    def test_local_rpm_path_with_extension(self):
        """
        Local RPM paths ending in .rpm must also be returned.
        """
        error = "No match for argument: /root/rpmbuild/RPMS/x86_64/pkcs11-helper-2.3.el9.x86_64.rpm"
        missing = RpmResolver.parse_missing_packages(error)
        self.assertEqual(missing, {"/root/rpmbuild/RPMS/x86_64/pkcs11-helper-2.3.el9.x86_64.rpm"})

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

    def test_detects_no_such_file(self):
        stderr = (
            "shutil.Error: [('/home/jenkins/.cache/rpm-lockfile-prototype/rpmdbs/ppc64le/"
            "sha256:abc123/var/lib/rpm/rpmdb.sqlite', '/tmp/xyz/var/lib/rpm/rpmdb.sqlite', "
            "\"[Errno 2] No such file or directory: '/home/jenkins/.cache/rpm-lockfile-prototype/"
            "rpmdbs/ppc64le/sha256:abc123/var/lib/rpm/rpmdb.sqlite'\")]"
        )
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

            self.resolver._rpmdb_cache_path = fake_cache
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

            self.resolver._rpmdb_cache_path = fake_cache
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
        self.resolver._rpmdb_cache_path = Path("/nonexistent/path")
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
            for i, arg in enumerate(cmd):
                if arg == "-v" and ":/work:" in cmd[i + 1]:
                    work_spec = cmd[i + 1]
                    options = work_spec.split(":")[2] if len(work_spec.split(":")) > 2 else ""
                    assert "ro" not in options.split(","), f"/work mount must be writable: {work_spec}"
                    host_tmpdir = work_spec.split(":")[0]
                    break
            host_outfile = os.path.join(host_tmpdir, DEFAULT_RPM_LOCKFILE_NAME)
            with open(host_outfile, "w") as f:
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
    def test_retries_when_cache_already_gone(self, mock_gather, mock_clear):
        """
        Cache deleted by another process (_clear returns False) — should still retry.
        """
        cache_race_stderr = (
            "shutil.Error: [('/home/jenkins/.cache/rpm-lockfile-prototype/rpmdbs/ppc64le/"
            "sha256:abc123/var/lib/rpm/rpmdb.sqlite', '/tmp/xyz/var/lib/rpm/rpmdb.sqlite', "
            "\"[Errno 2] No such file or directory: '/home/jenkins/.cache/rpm-lockfile-prototype/"
            "rpmdbs/ppc64le/sha256:abc123/var/lib/rpm/rpmdb.sqlite'\")]"
        )
        call_count = 0

        async def mock_cmd(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (1, "", cache_race_stderr)
            for i, arg in enumerate(cmd):
                if arg == "-v" and ":/work:" in cmd[i + 1]:
                    work_spec = cmd[i + 1]
                    options = work_spec.split(":")[2] if len(work_spec.split(":")) > 2 else ""
                    assert "ro" not in options.split(","), f"/work mount must be writable: {work_spec}"
                    host_tmpdir = work_spec.split(":")[0]
                    break
            host_outfile = os.path.join(host_tmpdir, DEFAULT_RPM_LOCKFILE_NAME)
            with open(host_outfile, "w") as f:
                yaml.safe_dump(self.FAKE_LOCKFILE_DATA, f)
            return (0, "", "")

        mock_gather.side_effect = mock_cmd
        mock_clear.return_value = False

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

    @patch("doozerlib.lockfile_prototype.resolver.RpmResolver._clear_rpmdb_cache")
    @patch("doozerlib.lockfile_prototype.resolver.cmd_gather_async")
    def test_retry_raises_retry_error_not_original(self, mock_gather, mock_clear):
        """
        When cache corruption triggers a retry and the retry fails with a
        different error, the raised RuntimeError must contain the retry
        stderr so the outer retry loop can parse it.
        """
        retry_stderr = "dnf.exceptions.PackagesNotInstalledError: No match for argument: nfs-utils: nfs-utils"
        call_count = 0

        async def mock_cmd(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (1, "", self.CORRUPTION_STDERR)
            return (1, "", retry_stderr)

        mock_gather.side_effect = mock_cmd
        mock_clear.return_value = True

        resolver = RpmResolver(working_dir=Path(tempfile.mkdtemp()))
        config = RpmsInConfig(
            arches=["x86_64"],
            contentOrigin={"repos": []},
            packages=[],
        )
        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(resolver.resolve(config, image_pullspec="registry.example.com/repo@sha256:abc123"))
        self.assertIn("nfs-utils", str(ctx.exception))
