import unittest

from artcommonlib import arch_util


class TestArchUtil(unittest.TestCase):
    def test_brew_arch_suffixes(self):
        expectations = {
            "x86_64": "",
            "amd64": "",
            "aarch64": "-aarch64",
            "arm64": "-aarch64"
        }
        for arch, suffix in expectations.items():
            self.assertEqual(arch_util.brew_suffix_for_arch(arch), suffix)
