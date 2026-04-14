"""
Tests for version validators in doozer CLI.
"""

import unittest

import click
from doozerlib.cli import validate_semver_major_minor, validate_semver_major_minor_patch


class TestValidateSemverMajorMinorPatch(unittest.TestCase):
    """
    Tests for validate_semver_major_minor_patch.
    Always pads to 3 segments: v4 -> v4.0.0, v4.20 -> v4.20.0.
    """

    def _validate(self, version: str) -> str:
        """
        Helper to call the validator without a real Click context.
        """
        return validate_semver_major_minor_patch(ctx=None, param=None, version=version)

    def test_three_segments_preserved(self):
        self.assertEqual(self._validate("v4.20.0"), "v4.20.0")

    def test_three_segments_nonzero_patch(self):
        self.assertEqual(self._validate("v4.18.3"), "v4.18.3")

    def test_two_segments_padded(self):
        self.assertEqual(self._validate("v4.20"), "v4.20.0")

    def test_one_segment_padded(self):
        self.assertEqual(self._validate("v4"), "v4.0.0")

    def test_too_many_segments_raises(self):
        with self.assertRaises(click.BadParameter):
            self._validate("v4.20.0.1")

    def test_non_integer_raises(self):
        with self.assertRaises(click.BadParameter):
            self._validate("v4.abc")

    def test_no_prefix_three_segments(self):
        self.assertEqual(self._validate("4.20.0"), "4.20.0")


class TestValidateSemverMajorMinor(unittest.TestCase):
    """
    Tests for validate_semver_major_minor.
    Enforces exactly 2 segments, used by microshift-bootc (e.g. v4.20).
    """

    def _validate(self, version: str) -> str:
        """
        Helper to call the validator without a real Click context.
        """
        return validate_semver_major_minor(ctx=None, param=None, version=version)

    def test_two_segments(self):
        self.assertEqual(self._validate("v4.20"), "v4.20")

    def test_two_segments_no_prefix(self):
        self.assertEqual(self._validate("4.20"), "4.20")

    def test_one_segment_raises(self):
        with self.assertRaises(click.BadParameter):
            self._validate("v4")

    def test_three_segments_raises(self):
        with self.assertRaises(click.BadParameter):
            self._validate("v4.20.0")

    def test_non_integer_raises(self):
        with self.assertRaises(click.BadParameter):
            self._validate("v4.abc")
