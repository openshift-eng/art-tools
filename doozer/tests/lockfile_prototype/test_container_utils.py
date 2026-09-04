"""
Tests for doozerlib.lockfile_prototype.container_utils.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper


class TestContainerImageHelper(unittest.TestCase):
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
        Should parse rpm -qa output into sorted unique package names.
        """

        async def mock_podman(cmd, **kwargs):
            return (0, "bash\ncoreutils\nbash\ngpg-pubkey\nglibc\n", "")

        mock_gather.side_effect = mock_podman
        helper = ContainerImageHelper()
        result = asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc", "x86_64"))
        self.assertEqual(result, ["bash", "coreutils", "glibc"])

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_uses_requested_architecture(self, mock_gather):
        """
        Should query the image using the platform matching the requested RPM architecture.
        """

        async def mock_podman(cmd, **kwargs):
            return (0, "bash\n", "")

        mock_gather.side_effect = mock_podman
        helper = ContainerImageHelper()
        asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc", "aarch64"))

        command = mock_gather.call_args.args[0]
        platform_index = command.index("--platform")
        self.assertEqual(command[platform_index + 1], "linux/arm64")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_fails(self, mock_gather):
        """
        Should raise ChildProcessError on podman failure.
        """
        mock_gather.side_effect = ChildProcessError("Process failed")
        helper = ContainerImageHelper()
        with self.assertRaises(ChildProcessError):
            asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc", "x86_64"))

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_read_file_from_image(self, mock_gather):
        """
        Should return file contents from podman cat.
        """

        async def mock_podman(cmd, **kwargs):
            return (0, "package1 package2", "")

        mock_gather.side_effect = mock_podman
        helper = ContainerImageHelper()
        result = asyncio.run(helper.read_file_from_image("quay.io/test/img@sha256:abc", "/etc/pkgs"))
        self.assertEqual(result, "package1 package2")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_read_file_from_image_fails(self, mock_gather):
        """
        Should return empty string on failure.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", "no such file")

        mock_gather.side_effect = mock_fail
        helper = ContainerImageHelper()
        result = asyncio.run(helper.read_file_from_image("quay.io/test/img@sha256:abc", "/etc/missing"))
        self.assertEqual(result, "")
