"""
Tests for doozerlib.lockfile_prototype.container_utils.
"""

import asyncio
import base64
import json
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper


class TestContainerImageHelper(unittest.TestCase):
    def test_parse_sbom_package_names_ignores_repository_metadata(self):
        """
        Ignore RPM PURLs that describe repository metadata rather than installed packages.
        """
        sbom = {
            "packages": [
                {
                    "name": "python3-dateutil",
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": (
                                "pkg:rpm/redhat/python3-dateutil@2.8.1-7.el9?"
                                "arch=noarch&repository_id=rhel-9-for-aarch64-baseos-e4s-rpms__9_DOT_6"
                            ),
                        }
                    ],
                },
                {
                    "name": "python3-six",
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": (
                                "pkg:rpm/redhat/python3-six@1.15.0-1.el9?"
                                "arch=noarch&upstream=python-six-1.15.0-1.el9.src.rpm"
                            ),
                        }
                    ],
                },
                {
                    "name": "python3-attrs",
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": (
                                "pkg:rpm/redhat/python3-attrs@21.4.0-1.el9?"
                                "arch=noarch&checksum=sha256:abc123&"
                                "repository_id=rhel-9-for-aarch64-appstream-e4s-rpms__9_DOT_6"
                            ),
                        }
                    ],
                },
            ]
        }

        helper = ContainerImageHelper()

        result = helper._parse_sbom_package_names(sbom)

        self.assertEqual(result, ["python3-attrs", "python3-six"])

    @patch("doozerlib.lockfile_prototype.container_utils.oc_image_info_for_arch_async", new_callable=AsyncMock)
    def test_resolve_to_digest_already_list_digest(self, mock_oc):
        """
        Pullspecs already pinned to the list digest should stay unchanged.
        """
        mock_oc.return_value = {
            "listDigest": "sha256:abc123",
            "digest": "sha256:platform456",
        }
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img@sha256:abc123"))
        self.assertEqual(result, "quay.io/test/img@sha256:abc123")
        mock_oc.assert_awaited_once()

    @patch("doozerlib.lockfile_prototype.container_utils.oc_image_info_for_arch_async", new_callable=AsyncMock)
    def test_resolve_to_digest_upgrades_platform_digest_to_list_digest(self, mock_oc):
        """
        A platform-instance digest should be upgraded to listDigest when available.
        """
        mock_oc.return_value = {
            "listDigest": "sha256:listaaa",
            "digest": "sha256:platformbbb",
        }
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img@sha256:platformbbb"))
        self.assertEqual(result, "quay.io/test/img@sha256:listaaa")

    @patch("doozerlib.lockfile_prototype.container_utils.oc_image_info_for_arch_async", new_callable=AsyncMock)
    def test_resolve_to_digest_tag_prefers_list_digest(self, mock_oc):
        """
        Multi-arch tag pullspecs should pin listDigest, not the platform digest.
        """
        mock_oc.return_value = {
            "listDigest": "sha256:listdigest",
            "digest": "sha256:platformdigest",
        }
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img:latest"))
        self.assertEqual(result, "quay.io/test/img@sha256:listdigest")

    @patch("doozerlib.lockfile_prototype.container_utils.oc_image_info_for_arch_async", new_callable=AsyncMock)
    def test_resolve_to_digest_single_arch_falls_back_to_digest(self, mock_oc):
        """
        Single-arch images with no listDigest should pin the platform digest.
        """
        mock_oc.return_value = {"digest": "sha256:def456"}
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img:latest"))
        self.assertEqual(result, "quay.io/test/img@sha256:def456")

    @patch("doozerlib.lockfile_prototype.container_utils.oc_image_info_for_arch_async", new_callable=AsyncMock)
    def test_resolve_to_digest_brew_registry_uses_proxy(self, mock_oc):
        """
        brew.registry.redhat.io pullspecs should be inspected via the registry proxy,
        but the returned pullspec should keep the original brew.registry domain.
        """
        mock_oc.return_value = {
            "listDigest": "sha256:abc123",
            "digest": "sha256:platform",
        }
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("brew.registry.redhat.io/rh-osbs/ubi8:8.6-754"))
        self.assertEqual(result, "brew.registry.redhat.io/rh-osbs/ubi8@sha256:abc123")
        inspect_pullspec = mock_oc.await_args.args[0]
        self.assertIn("registry-proxy.engineering.redhat.com", inspect_pullspec)

    @patch("doozerlib.lockfile_prototype.container_utils.oc_image_info_for_arch_async", new_callable=AsyncMock)
    def test_resolve_to_digest_inspect_fails(self, mock_oc):
        """
        If inspect fails, return the original pullspec (bare-mode fallback).
        """
        mock_oc.side_effect = ChildProcessError("connection refused")
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img:latest"))
        self.assertEqual(result, "quay.io/test/img:latest")

    @patch("doozerlib.lockfile_prototype.container_utils.oc_image_info_for_arch_async", new_callable=AsyncMock)
    def test_resolve_to_digest_no_digest_fields(self, mock_oc):
        """
        If inspect succeeds but returns no digest fields, keep the original pullspec.
        """
        mock_oc.return_value = {}
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img:latest"))
        self.assertEqual(result, "quay.io/test/img:latest")

    def test_repo_from_pullspec_strips_tag_and_digest(self):
        self.assertEqual(ContainerImageHelper._repo_from_pullspec("quay.io/test/img:latest"), "quay.io/test/img")
        self.assertEqual(
            ContainerImageHelper._repo_from_pullspec("quay.io/test/img@sha256:abc"),
            "quay.io/test/img",
        )
        self.assertEqual(
            ContainerImageHelper._repo_from_pullspec("registry.example.com:5000/ns/img:tag"),
            "registry.example.com:5000/ns/img",
        )

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages(self, mock_gather):
        """
        Should parse package names from an extracted RPM database.
        """

        async def mock_commands(cmd, **kwargs):
            if cmd[0] == "cosign":
                return (1, "", "found no attestations")
            if cmd[0] == "oras":
                return (1, "", "no SPDX attachment")
            if cmd[0] == "oc":
                path_arg = cmd[cmd.index("--path") + 1]
                _, destination = path_arg.rsplit(":", 1)
                Path(destination, "rpmdb.sqlite").touch()
                return (0, "", "")
            if cmd[0] == "rpm":
                return (0, "bash\ncoreutils\nbash\ngpg-pubkey\nglibc\n", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_commands
        pullspec = "quay.io/test/img@sha256:abc"
        logger = MagicMock()
        helper = ContainerImageHelper(logger=logger)
        result = asyncio.run(helper.get_installed_packages(pullspec, "x86_64"))
        self.assertEqual(result, ["bash", "coreutils", "glibc"])
        logger.info.assert_any_call(
            "Discovering installed packages for %s [arch=%s, platform=%s]",
            pullspec,
            "x86_64",
            "linux/amd64",
        )
        logger.info.assert_any_call(
            "No usable SBOM for %s [platform=%s]; extracting RPMDB",
            pullspec,
            "linux/amd64",
        )
        logger.info.assert_any_call("Found %d packages in RPMDB for %s [platform=%s]", 3, pullspec, "linux/amd64")
        commands = [call.args[0] for call in mock_gather.call_args_list]
        self.assertEqual([command[0] for command in commands], ["cosign", "oras", "oc", "rpm"])
        self.assertTrue(all(command[0] != "podman" for command in commands))

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_uses_spdx_attestation(self, mock_gather):
        """
        Should parse RPM package names from an SPDX attestation without running the image.
        """
        sbom = {
            "packages": [
                {
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": (
                                "pkg:rpm/redhat/bash@5.1.8-9.el9?arch=x86_64&upstream=bash-5.1.8-9.el9.src.rpm"
                            ),
                        }
                    ]
                },
                {
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": "pkg:rpm/redhat/gpg-pubkey@1-1.el9?arch=noarch",
                        }
                    ]
                },
                {
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": "pkg:rpm/redhat/bash-src@5.1.8-9.el9?arch=nosrc",
                        }
                    ]
                },
            ]
        }
        statement = {
            "_type": "https://in-toto.io/Statement/v0.1",
            "predicateType": "https://spdx.dev/Document",
            "predicate": sbom,
        }
        attestation = {
            "payloadType": "application/vnd.in-toto+json",
            "payload": base64.b64encode(json.dumps(statement).encode()).decode(),
        }

        async def mock_attestation(cmd, **kwargs):
            return (0, json.dumps(attestation), "")

        mock_gather.side_effect = mock_attestation
        pullspec = "quay.io/test/img@sha256:abc"
        logger = MagicMock()
        helper = ContainerImageHelper(logger=logger)
        result = asyncio.run(helper.get_installed_packages(pullspec, "x86_64"))

        self.assertEqual(result, ["bash"])
        logger.info.assert_any_call(
            "Found %d packages in SPDX attestation for %s [platform=%s]", 1, pullspec, "linux/amd64"
        )
        command = mock_gather.call_args.args[0]
        self.assertEqual(command[:3], ["cosign", "download", "attestation"])
        self.assertIn("--platform", command)
        self.assertEqual(command[command.index("--platform") + 1], "linux/amd64")
        self.assertEqual(command[command.index("--predicate-type") + 1], "https://spdx.dev/Document")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_uses_spdx_attachment(self, mock_gather):
        """
        Should parse a legacy SPDX attachment when no attestation exists.
        """
        sbom = {
            "packages": [
                {
                    "externalRefs": [
                        {
                            "referenceType": "purl",
                            "referenceLocator": (
                                "pkg:rpm/redhat/bash@5.1.8-9.el9?arch=x86_64&upstream=bash-5.1.8-9.el9.src.rpm"
                            ),
                        }
                    ]
                }
            ]
        }

        async def mock_commands(cmd, **kwargs):
            if cmd[0] == "cosign":
                return (1, "", "found no attestations")
            if cmd[0] == "oras" and "--descriptor" in cmd:
                return (0, json.dumps({"digest": "sha256:platformdigest"}), "")
            if cmd[0] == "oras" and cmd[1:3] == ["manifest", "fetch"]:
                manifest = {"layers": [{"mediaType": "text/spdx+json", "digest": "sha256:sbomdigest"}]}
                return (0, json.dumps(manifest), "")
            if cmd[0] == "oras" and cmd[1:3] == ["blob", "fetch"]:
                return (0, json.dumps(sbom), "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_commands
        pullspec = "quay.io/test/img@sha256:abc"
        logger = MagicMock()
        helper = ContainerImageHelper(logger=logger)
        with patch.object(helper.logger, "warning") as mock_warning:
            result = asyncio.run(helper.get_installed_packages(pullspec, "x86_64"))

        self.assertEqual(result, ["bash"])
        mock_warning.assert_not_called()
        logger.info.assert_any_call(
            "No usable SPDX attestation for %s [platform=%s]; checking legacy SPDX attachment",
            pullspec,
            "linux/amd64",
        )
        logger.info.assert_any_call(
            "Found %d packages in SPDX attachment for %s [platform=%s]", 1, pullspec, "linux/amd64"
        )
        commands = [call.args[0] for call in mock_gather.call_args_list]
        self.assertEqual([command[0] for command in commands], ["cosign", "oras", "oras", "oras"])
        self.assertIn("--platform", commands[1])
        self.assertTrue(any(command.endswith(":sha256-platformdigest.sbom") for command in commands[2]))
        self.assertTrue(any(command.endswith("@sha256:sbomdigest") for command in commands[3]))

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_falls_back_to_extracted_rpmdb_when_sbom_unavailable(self, mock_gather):
        """
        Should query an extracted RPM database when the image has no usable SBOM.
        """

        async def mock_commands(cmd, **kwargs):
            if cmd[0] == "cosign":
                return (1, "", "found no attestations")
            if cmd[0] == "oras":
                return (1, "", "no SPDX attachment")
            if cmd[0] == "oc":
                path_arg = cmd[cmd.index("--path") + 1]
                _, destination = path_arg.rsplit(":", 1)
                Path(destination, "rpmdb.sqlite").touch()
                return (0, "", "")
            if cmd[0] == "rpm":
                return (0, "bash\nglibc\n", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_commands
        pullspec = "quay.io/test/img@sha256:abc"
        logger = MagicMock()
        helper = ContainerImageHelper(logger=logger)
        with patch.object(helper.logger, "warning") as mock_warning:
            result = asyncio.run(helper.get_installed_packages(pullspec, "x86_64"))

        self.assertEqual(result, ["bash", "glibc"])
        mock_warning.assert_not_called()
        logger.info.assert_any_call(
            "No usable SBOM for %s [platform=%s]; extracting RPMDB",
            pullspec,
            "linux/amd64",
        )
        logger.info.assert_any_call("Found %d packages in RPMDB for %s [platform=%s]", 2, pullspec, "linux/amd64")
        commands = [call.args[0] for call in mock_gather.call_args_list]
        self.assertEqual([command[0] for command in commands], ["cosign", "oras", "oc", "rpm"])
        self.assertIn("--dbpath", commands[-1])
        self.assertTrue(all(command[0] != "podman" for command in commands))

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_uses_legacy_rpmdb_path(self, mock_gather):
        """
        Should try the legacy RPM database path when the modern path is unavailable.
        """

        async def mock_commands(cmd, **kwargs):
            if cmd[0] == "cosign":
                return (1, "", "found no attestations")
            if cmd[0] == "oras":
                return (1, "", "no SPDX attachment")
            if cmd[0] == "oc":
                path_arg = cmd[cmd.index("--path") + 1]
                source, destination = path_arg.rsplit(":", 1)
                if source == "/usr/lib/sysimage/rpm/":
                    return (1, "", "path not found")
                Path(destination, "Packages").touch()
                return (0, "", "")
            if cmd[0] == "rpm":
                return (0, "bash\n", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_commands
        helper = ContainerImageHelper()
        result = asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc", "x86_64"))

        self.assertEqual(result, ["bash"])
        oc_commands = [call.args[0] for call in mock_gather.call_args_list if call.args[0][0] == "oc"]
        self.assertEqual(len(oc_commands), 2)
        self.assertIn("/var/lib/rpm/:", oc_commands[1][oc_commands[1].index("--path") + 1])

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_uses_requested_architecture(self, mock_gather):
        """
        Should query the image using the platform matching the requested RPM architecture.
        """

        async def mock_commands(cmd, **kwargs):
            if cmd[0] == "cosign":
                return (1, "", "found no attestations")
            if cmd[0] == "oras":
                return (1, "", "no SPDX attachment")
            if cmd[0] == "oc":
                path_arg = cmd[cmd.index("--path") + 1]
                _, destination = path_arg.rsplit(":", 1)
                Path(destination, "rpmdb.sqlite").touch()
                return (0, "", "")
            if cmd[0] == "rpm":
                return (0, "bash\n", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_commands
        helper = ContainerImageHelper()
        asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc", "aarch64"))

        commands = [call.args[0] for call in mock_gather.call_args_list]
        oc_command = next(command for command in commands if command[0] == "oc")
        platform_index = oc_command.index("--filter-by-os")
        self.assertEqual(oc_command[platform_index + 1], "linux/arm64")
        self.assertTrue(all(command[0] != "podman" for command in commands))

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_fails(self, mock_gather):
        """
        Should return no packages when both SBOM and RPM database queries fail.
        """
        mock_gather.side_effect = ChildProcessError("Process failed")
        helper = ContainerImageHelper()
        result = asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc", "x86_64"))
        self.assertEqual(result, [])

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_read_file_from_image(self, mock_gather):
        """
        Should extract a file via oc image extract and read it.
        """

        async def mock_cmd(cmd, **kwargs):
            if cmd[0] == "oc":
                # Simulate extracting a file; dest dir already exists (tmpdir)
                path_arg = cmd[cmd.index("--path") + 1]
                filepath, dest = path_arg.rsplit(":", 1)
                extracted = os.path.join(dest, os.path.basename(filepath))
                with open(extracted, "w") as f:
                    f.write("package1 package2")
                return (0, "", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_cmd
        helper = ContainerImageHelper()
        result = asyncio.run(helper.read_file_from_image("quay.io/test/img@sha256:abc", "/etc/pkgs"))
        self.assertEqual(result, "package1 package2")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_read_file_from_image_fails(self, mock_gather):
        """
        Should return empty string on oc image extract failure.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", "no such file")

        mock_gather.side_effect = mock_fail
        helper = ContainerImageHelper()
        result = asyncio.run(helper.read_file_from_image("quay.io/test/img@sha256:abc", "/etc/missing"))
        self.assertEqual(result, "")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_read_file_from_image_glob_path(self, mock_gather):
        """
        Glob paths like /etc/*.repo should fall back to reading all extracted files.
        """

        async def mock_cmd(cmd, **kwargs):
            if cmd[0] == "oc":
                path_arg = cmd[cmd.index("--path") + 1]
                _, dest = path_arg.rsplit(":", 1)
                # oc extracts matching files — basename is literal "*.repo"
                with open(os.path.join(dest, "base.repo"), "w") as f:
                    f.write("[base]\n")
                with open(os.path.join(dest, "extras.repo"), "w") as f:
                    f.write("[extras]\n")
                return (0, "", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_cmd
        helper = ContainerImageHelper()
        result = asyncio.run(helper.read_file_from_image("quay.io/test/img@sha256:abc", "/etc/*.repo"))
        # Files are sorted alphabetically: base.repo then extras.repo
        self.assertEqual(result, "[base]\n[extras]\n")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_read_file_from_image_directory_path(self, mock_gather):
        """
        Directory paths like /etc/yum.repos.d/ should read all files in the dir.
        """

        async def mock_cmd(cmd, **kwargs):
            if cmd[0] == "oc":
                path_arg = cmd[cmd.index("--path") + 1]
                _, dest = path_arg.rsplit(":", 1)
                # oc extracts directory contents into tmpdir
                os.makedirs(os.path.join(dest, "subdir"), exist_ok=True)
                with open(os.path.join(dest, "subdir", "a.conf"), "w") as f:
                    f.write("a-content")
                with open(os.path.join(dest, "top.conf"), "w") as f:
                    f.write("top-content")
                return (0, "", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_cmd
        helper = ContainerImageHelper()
        result = asyncio.run(helper.read_file_from_image("quay.io/test/img@sha256:abc", "/etc/yum.repos.d/"))
        # rglob finds subdir/a.conf and top.conf; sorted order
        self.assertEqual(result, "a-contenttop-content")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_read_file_from_image_empty_extraction(self, mock_gather):
        """
        When oc image extract succeeds but no files are extracted, return "".
        """

        async def mock_cmd(cmd, **kwargs):
            if cmd[0] == "oc":
                # Succeed but don't create any files
                return (0, "", "")
            return (1, "", "unexpected command")

        mock_gather.side_effect = mock_cmd
        helper = ContainerImageHelper()
        result = asyncio.run(helper.read_file_from_image("quay.io/test/img@sha256:abc", "/etc/*.nonexistent"))
        self.assertEqual(result, "")
