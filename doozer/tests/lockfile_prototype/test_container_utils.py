"""
Tests for doozerlib.lockfile_prototype.container_utils.
"""

import asyncio
import unittest
from unittest.mock import patch

from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper


class TestContainerImageHelper(unittest.TestCase):
    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_resolve_to_digest_already_digest(self, mock_gather):
        """
        Pullspecs with @sha256: should be returned unchanged.
        """
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img@sha256:abc123"))
        self.assertEqual(result, "quay.io/test/img@sha256:abc123")
        mock_gather.assert_not_called()

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_resolve_to_digest_tag(self, mock_gather):
        """
        Tag-based pullspecs should be resolved via skopeo.
        """
        import json

        async def mock_skopeo(cmd, **kwargs):
            return (0, json.dumps({"Digest": "sha256:def456"}), "")

        mock_gather.side_effect = mock_skopeo
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img:latest"))
        self.assertEqual(result, "quay.io/test/img@sha256:def456")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_resolve_to_digest_brew_registry_uses_proxy(self, mock_gather):
        """
        brew.registry.redhat.io pullspecs should be inspected via the registry proxy,
        but the returned pullspec should keep the original brew.registry domain.
        """
        import json

        async def mock_skopeo(cmd, **kwargs):
            assert "registry-proxy.engineering.redhat.com" in cmd[-1], (
                f"Expected proxy URL in skopeo command, got {cmd}"
            )
            return (0, json.dumps({"Digest": "sha256:abc123"}), "")

        mock_gather.side_effect = mock_skopeo
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("brew.registry.redhat.io/rh-osbs/ubi8:8.6-754"))
        self.assertEqual(result, "brew.registry.redhat.io/rh-osbs/ubi8@sha256:abc123")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_resolve_to_digest_skopeo_fails(self, mock_gather):
        """
        If skopeo fails, return the original pullspec.
        """

        async def mock_fail(cmd, **kwargs):
            return (1, "", "connection refused")

        mock_gather.side_effect = mock_fail
        helper = ContainerImageHelper()
        result = asyncio.run(helper.resolve_to_digest("quay.io/test/img:latest"))
        self.assertEqual(result, "quay.io/test/img:latest")

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages(self, mock_gather):
        """
        Should parse rpm -qa output into sorted unique package names.
        """

        async def mock_podman(cmd, **kwargs):
            return (0, "bash\ncoreutils\nbash\ngpg-pubkey\nglibc\n", "")

        mock_gather.side_effect = mock_podman
        helper = ContainerImageHelper()
        result = asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc"))
        self.assertEqual(result, ["bash", "coreutils", "glibc"])

    @patch("doozerlib.lockfile_prototype.container_utils.cmd_gather_async")
    def test_get_installed_packages_fails(self, mock_gather):
        """
        Should raise ChildProcessError on podman failure.
        """
        mock_gather.side_effect = ChildProcessError("Process failed")
        helper = ContainerImageHelper()
        with self.assertRaises(ChildProcessError):
            asyncio.run(helper.get_installed_packages("quay.io/test/img@sha256:abc"))

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
