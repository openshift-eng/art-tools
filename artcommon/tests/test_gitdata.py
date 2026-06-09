import sys
import unittest
from unittest.mock import MagicMock

# Stub out heavy transitive dependencies so we can import SafeFormatter
# without installing the full art-tools dependency tree.
for mod in [
    'ruamel',
    'ruamel.yaml',
    'ruamel.yaml.util',
    'artcommonlib.exectools',
    'artcommonlib.constants',
    'artcommonlib.github_auth',
    'artcommonlib.logutil',
    'artcommonlib.pushd',
    'artcommonlib.util',
    'future',
    'future.utils',
]:
    sys.modules.setdefault(mod, MagicMock())

from artcommonlib.gitdata import SafeFormatter  # noqa: E402


class TestSafeFormatter(unittest.TestCase):
    """Tests for SafeFormatter — a string.Formatter subclass that leaves
    unrecognised placeholders intact instead of raising."""

    def setUp(self):
        self.formatter = SafeFormatter()

    # --- named placeholders ---

    def test_named_substitution(self):
        result = self.formatter.format("release-{MAJOR}.{MINOR}", MAJOR=4, MINOR=17)
        self.assertEqual(result, "release-4.17")

    def test_missing_named_placeholder_kept(self):
        result = self.formatter.format("release-{MAJOR}.{MINOR}", MAJOR=4)
        self.assertEqual(result, "release-4.{MINOR}")

    def test_all_named_missing(self):
        result = self.formatter.format("{MAJOR}.{MINOR}")
        self.assertEqual(result, "{MAJOR}.{MINOR}")

    # --- positional / bare {} placeholders ---

    def test_bare_empty_braces_no_args(self):
        """Bare '{}' in input with no positional args must not raise."""
        result = self.formatter.format("streams_prs: {}", MAJOR=4, MINOR=17)
        self.assertEqual(result, "streams_prs: {}")

    def test_bare_braces_mixed_with_named(self):
        """YAML-like text containing both {} and {MAJOR}.{MINOR}."""
        yaml_text = "ci_alignment:\n  streams_prs: {}\ndistgit:\n  branch: rhaos-{MAJOR}.{MINOR}-rhel-9\n"
        result = self.formatter.format(yaml_text, MAJOR=5, MINOR=0)
        expected = "ci_alignment:\n  streams_prs: {}\ndistgit:\n  branch: rhaos-5.0-rhel-9\n"
        self.assertEqual(result, expected)

    def test_multiple_bare_braces(self):
        result = self.formatter.format("{} and {} and {MAJOR}", MAJOR=4)
        self.assertEqual(result, "{} and {} and 4")

    # --- edge cases ---

    def test_empty_string(self):
        result = self.formatter.format("")
        self.assertEqual(result, "")

    def test_no_placeholders(self):
        result = self.formatter.format("plain text", MAJOR=4)
        self.assertEqual(result, "plain text")

    def test_escaped_braces(self):
        result = self.formatter.format("escaped {{MAJOR}} literal")
        self.assertEqual(result, "escaped {MAJOR} literal")


if __name__ == "__main__":
    unittest.main()
