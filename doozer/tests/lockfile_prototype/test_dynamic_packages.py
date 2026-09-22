"""
Tests for dynamic package manifest resolution.
"""

import asyncio
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.dynamic_packages import (
    DynamicPackageResolver,
    discover_dynamic_package_script,
)
from doozerlib.lockfile_prototype.models import RepoEntry


class TestDynamicPackageResolver(unittest.TestCase):
    """
    Test dynamic package script and manifest resolution.
    """

    def _make_resolver(self, downstream_parents: list[str]) -> DynamicPackageResolver:
        """
        Create a resolver with a mocked container helper.

        Arg(s):
            downstream_parents (list[str]): Parent image pullspecs by stage.
        Return Value(s):
            DynamicPackageResolver: Resolver under test.
        """
        container = MagicMock(spec=ContainerImageHelper)
        container.read_file_from_image = AsyncMock(return_value="")
        return DynamicPackageResolver(
            container,
            downstream_parents,
            {},
            logging.getLogger(__name__),
        )

    def test_discovers_dynamic_script_from_dockerfile(self):
        """Discover a script that selects a manifest using OS-release values."""
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script_path = source_dir / "extensions" / "build.sh"
            script_path.parent.mkdir()
            script_path.write_text('extensions_yaml="extensions/${ID}-${VERSION_ID}.yaml"\n')
            entries = [
                {"instruction": "FROM", "value": "base"},
                {"instruction": "RUN", "value": "--mount=type=secret extensions/build.sh"},
            ]

            result = discover_dynamic_package_script(source_dir, entries)

        self.assertEqual(result, "extensions/build.sh")

    def test_does_not_discover_static_shell_script(self):
        """Ignore shell scripts that do not select an OS-specific manifest."""
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script_path = source_dir / "extensions" / "build.sh"
            script_path.parent.mkdir()
            script_path.write_text('extensions_yaml="extensions/static.yaml"\n')
            entries = [
                {"instruction": "FROM", "value": "base"},
                {"instruction": "RUN", "value": "extensions/build.sh"},
            ]

            result = discover_dynamic_package_script(source_dir, entries)

        self.assertIsNone(result)

    def test_resolves_manifest_from_os_release(self):
        """
        Resolve a manifest using the base image OS release and preserve
        architecture-specific packages.
        """
        resolver = self._make_resolver(["quay.io/test/base@sha256:abc123"])

        async def mock_read_file(pullspec: str, filepath: str) -> str:
            if filepath == "/etc/os-release":
                return 'ID="rhel"\nVERSION_ID="9"\n'
            return ""

        resolver._container.read_file_from_image = AsyncMock(side_effect=mock_read_file)

        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script_path = source_dir / "extensions" / "build.sh"
            script_path.parent.mkdir()
            script_path.write_text('extensions_yaml="extensions/${ID}-${VERSION_ID}.yaml"\n')
            (source_dir / "extensions" / "rhel-9.yaml").write_text(
                "extensions:\n"
                "  common:\n"
                "    packages: [common-package]\n"
                "  ppc-only:\n"
                "    architectures: [ppc64le]\n"
                "    packages: [ppc-package]\n"
            )
            entries = [
                {"instruction": "FROM", "value": "base"},
                {"instruction": "RUN", "value": "--mount=type=secret extensions/build.sh"},
            ]

            result = asyncio.run(
                resolver.resolve(
                    "extensions/build.sh",
                    source_dir,
                    ["x86_64", "ppc64le"],
                    entries,
                    [],
                )
            )

        self.assertEqual(result[0].common, ["common-package"])
        self.assertEqual(result[0].arch_specific, {"ppc64le": ["ppc-package"]})

    def test_falls_back_when_os_release_is_unavailable(self):
        """
        Use a unique source manifest when the base image cannot provide
        ``/etc/os-release``.
        """
        resolver = self._make_resolver(["quay.io/test/base@sha256:abc123"])

        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script_path = source_dir / "extensions" / "build.sh"
            script_path.parent.mkdir()
            script_path.write_text('extensions_yaml="extensions/${ID}-${VERSION_ID}.yaml"\n')
            (source_dir / "extensions" / "rhel-9.6.yaml").write_text(
                "extensions:\n  common:\n    packages: [common-package]\n"
            )
            entries = [
                {"instruction": "FROM", "value": "base"},
                {"instruction": "RUN", "value": "--mount=type=secret extensions/build.sh"},
            ]

            result = asyncio.run(
                resolver.resolve(
                    "extensions/build.sh",
                    source_dir,
                    ["x86_64"],
                    entries,
                    [],
                )
            )

        self.assertEqual(result[0].common, ["common-package"])

    def test_manifest_fallback_uses_repository_version_hint(self):
        """Repository content-set versions disambiguate multiple manifests."""
        resolver = self._make_resolver([])

        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            extensions_dir = source_dir / "extensions"
            extensions_dir.mkdir()
            (extensions_dir / "rhel-9.5.yaml").write_text("extensions: {}\n")
            (extensions_dir / "rhel-9.6.yaml").write_text("extensions: {}\n")
            repo_list = [
                RepoEntry(
                    repoid="rhel-9-for-$basearch-baseos-rpms__9_DOT_6",
                    baseurl="https://example.com/rhel-9.6/$basearch/os/",
                )
            ]

            result = resolver._resolve_manifest_file(
                source_dir,
                "extensions/${ID}-${VERSION_ID}.yaml",
                {},
                repo_list,
                "extensions/build.sh",
            )

        self.assertEqual(result.name, "rhel-9.6.yaml")
