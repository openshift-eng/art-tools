import unittest

from artcommonlib import arch_util


class TestArchUtil(unittest.TestCase):
    def test_brew_arch_suffixes(self):
        expectations = {
            "x86_64": "",
            "amd64": "",
            "aarch64": "-aarch64",
            "arm64": "-aarch64",
        }
        for arch, suffix in expectations.items():
            self.assertEqual(arch_util.brew_suffix_for_arch(arch), suffix)

    def test_brew_arch_for_go_arch(self):
        expectations = {
            "x86_64": "amd64",
            "s390x": "s390x",
            "ppc64le": "ppc64le",
            "aarch64": "arm64",
            "multi": "multi",
        }

        for brew_arch, go_arch in expectations.items():
            self.assertEqual(arch_util.brew_arch_for_go_arch(go_arch), brew_arch)

        for brew_arch in expectations.keys():
            self.assertEqual(arch_util.brew_arch_for_go_arch(brew_arch), brew_arch)

        with self.assertRaises(Exception):
            self.assertEqual(arch_util.brew_arch_for_go_arch("wrong"), "bogus")

    def test_go_arch_suffixes(self):
        expectations = {
            "x86_64": "",
            "amd64": "",
            "aarch64": "-arm64",
            "arm64": "-arm64",
        }

        for arch, suffix in expectations.items():
            self.assertEqual(arch_util.go_suffix_for_arch(arch), suffix)

        for arch, suffix in expectations.items():
            self.assertEqual(arch_util.go_suffix_for_arch(arch, is_private=True), f"{suffix}-priv")

    def test_go_arch_for_brew_arch(self):
        expectations = {
            "amd64": "x86_64",
            "s390x": "s390x",
            "ppc64le": "ppc64le",
            "arm64": "aarch64",
            "multi": "multi",
        }

        for go_arch, brew_arch in expectations.items():
            self.assertEqual(arch_util.go_arch_for_brew_arch(brew_arch), go_arch)

        for go_arch in expectations.keys():
            self.assertEqual(arch_util.go_arch_for_brew_arch(go_arch), go_arch)

        with self.assertRaises(Exception):
            self.assertEqual(arch_util.go_arch_for_brew_arch("wrong"), "bogus")
