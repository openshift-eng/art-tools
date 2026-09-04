"""
Tests for doozerlib.lockfile_prototype.lockfile_merger.
"""

import unittest

from doozerlib.lockfile_prototype.lockfile_merger import merge_lockfiles
from doozerlib.lockfile_prototype.models import (
    ArchResult,
    LockfileData,
    ModuleEntry,
    PackageEntry,
)


class TestMergeLockfiles(unittest.TestCase):
    def test_single_lockfile_merged(self):
        lockfile = LockfileData(
            lockfileVersion=1,
            arches=[
                ArchResult(arch="x86_64", packages=[]),
            ],
        )
        result = merge_lockfiles([lockfile])
        self.assertEqual(result.arches[0].arch, "x86_64")

    def test_dedupes_by_url(self):
        """
        Same URL across stages should appear once; different URLs preserved.
        """
        lockfile_a = LockfileData(
            arches=[
                ArchResult(
                    arch="x86_64",
                    packages=[
                        PackageEntry(url="https://example.com/foo-1.0.rpm", name="foo", evr="1.0", repoid="baseos"),
                    ],
                    module_metadata=[],
                ),
            ],
        )
        lockfile_b = LockfileData(
            arches=[
                ArchResult(
                    arch="x86_64",
                    packages=[
                        PackageEntry(url="https://example.com/foo-2.0.rpm", name="foo", evr="2.0", repoid="baseos"),
                        PackageEntry(url="https://example.com/bar-1.0.rpm", name="bar", evr="1.0", repoid="baseos"),
                    ],
                    module_metadata=[],
                ),
            ],
        )
        merged = merge_lockfiles([lockfile_a, lockfile_b])
        self.assertIsInstance(merged, LockfileData)
        urls = [p.url for p in merged.arches[0].packages]
        self.assertEqual(len(urls), 3)
        self.assertIn("https://example.com/foo-1.0.rpm", urls)
        self.assertIn("https://example.com/foo-2.0.rpm", urls)
        self.assertIn("https://example.com/bar-1.0.rpm", urls)

    def test_preserves_module_metadata(self):
        lockfile_a = LockfileData(
            arches=[
                ArchResult(
                    arch="x86_64",
                    packages=[],
                    module_metadata=[ModuleEntry(name="mod-a", stream="1.0")],
                ),
            ],
        )
        lockfile_b = LockfileData(
            arches=[
                ArchResult(
                    arch="x86_64",
                    packages=[],
                    module_metadata=[ModuleEntry(name="mod-b", stream="2.0")],
                ),
            ],
        )
        merged = merge_lockfiles([lockfile_a, lockfile_b])
        modules = merged.arches[0].module_metadata
        self.assertEqual(len(modules), 2)
        names = [m.name for m in modules]
        self.assertIn("mod-a", names)
        self.assertIn("mod-b", names)

    def test_multiple_arches(self):
        lockfile_a = LockfileData(
            arches=[
                ArchResult(
                    arch="x86_64",
                    packages=[PackageEntry(url="a.rpm", name="a", repoid="baseos")],
                    module_metadata=[],
                ),
            ],
        )
        lockfile_b = LockfileData(
            arches=[
                ArchResult(
                    arch="aarch64",
                    packages=[PackageEntry(url="b.rpm", name="b", repoid="baseos")],
                    module_metadata=[],
                ),
            ],
        )
        merged = merge_lockfiles([lockfile_a, lockfile_b])
        arches = [e.arch for e in merged.arches]
        self.assertEqual(arches, ["aarch64", "x86_64"])
