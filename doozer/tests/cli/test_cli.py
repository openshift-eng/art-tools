"""
Tests for the Doozer command-line interface.
"""

import unittest

from doozerlib.cli import cli


class TestCli(unittest.TestCase):
    def test_variant_option_accepts_layered_product(self):
        """Ensure the global variant option accepts a layered-product variant."""
        variant_option = next(option for option in cli.params if option.name == "variant")
        result = variant_option.type.convert("oadp", None, None)

        self.assertEqual(result, "oadp")
