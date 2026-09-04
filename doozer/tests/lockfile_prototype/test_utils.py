"""
Tests for doozerlib.lockfile_prototype.utils.
"""

import unittest
from unittest.mock import patch

from doozerlib.lockfile_prototype.utils import (
    build_env,
    compare_evr,
    format_version_pin,
    parse_evr,
    pick_minimum_evr,
)


class TestBuildEnv(unittest.TestCase):
    @patch.dict("os.environ", {"QUAY_AUTH_FILE": "/path/to/auth.json"}, clear=True)
    def test_quay_auth_file_sets_registry_auth(self):
        env = build_env()
        self.assertEqual(env["REGISTRY_AUTH_FILE"], "/path/to/auth.json")

    @patch.dict("os.environ", {"REGISTRY_AUTH_FILE": "/path/to/registry.json"}, clear=True)
    def test_registry_auth_file_used_as_fallback(self):
        env = build_env()
        self.assertEqual(env["REGISTRY_AUTH_FILE"], "/path/to/registry.json")

    @patch.dict("os.environ", {"QUAY_AUTH_FILE": "/quay", "REGISTRY_AUTH_FILE": "/registry"}, clear=True)
    def test_quay_auth_takes_precedence(self):
        env = build_env()
        self.assertEqual(env["REGISTRY_AUTH_FILE"], "/quay")

    @patch.dict("os.environ", {}, clear=True)
    def test_no_auth_files_no_registry_key(self):
        env = build_env()
        self.assertNotIn("REGISTRY_AUTH_FILE", env)


class TestParseEvr(unittest.TestCase):
    def test_without_epoch(self):
        self.assertEqual(parse_evr("0.4.1-5.el9"), ("0", "0.4.1", "5.el9"))

    def test_with_epoch(self):
        self.assertEqual(parse_evr("1:2.5.4-1.el9"), ("1", "2.5.4", "1.el9"))

    def test_zero_epoch_explicit(self):
        self.assertEqual(parse_evr("0:3.0-2.el9"), ("0", "3.0", "2.el9"))

    def test_complex_release(self):
        self.assertEqual(parse_evr("0.4.1-5.el9_8"), ("0", "0.4.1", "5.el9_8"))


class TestCompareEvr(unittest.TestCase):
    def test_equal(self):
        self.assertEqual(compare_evr("0.4.1-5.el9", "0.4.1-5.el9"), 0)

    def test_newer_release(self):
        self.assertEqual(compare_evr("0.4.1-7.el9_8", "0.4.1-5.el9"), 1)

    def test_older_release(self):
        self.assertEqual(compare_evr("0.4.1-5.el9", "0.4.1-7.el9_8"), -1)

    def test_newer_version(self):
        self.assertEqual(compare_evr("0.5.0-1.el9", "0.4.1-5.el9"), 1)

    def test_epoch_wins(self):
        self.assertEqual(compare_evr("1:0.1-1.el9", "0.4.1-5.el9"), 1)


class TestPickMinimumEvr(unittest.TestCase):
    def test_single(self):
        self.assertEqual(pick_minimum_evr(["0.4.1-5.el9"]), "0.4.1-5.el9")

    def test_picks_oldest(self):
        evrs = ["0.4.1-7.el9_8", "0.4.1-5.el9", "0.4.1-6.el9"]
        self.assertEqual(pick_minimum_evr(evrs), "0.4.1-5.el9")

    def test_with_epoch(self):
        evrs = ["1:2.0-3.el9", "0.9-1.el9"]
        self.assertEqual(pick_minimum_evr(evrs), "0.9-1.el9")


class TestFormatVersionPin(unittest.TestCase):
    def test_no_epoch(self):
        self.assertEqual(format_version_pin("libeconf", "0.4.1-5.el9"), "libeconf-0.4.1-5.el9")

    def test_with_epoch(self):
        self.assertEqual(format_version_pin("foo", "1:2.0-3.el9"), "foo-1:2.0-3.el9")

    def test_zero_epoch_omitted(self):
        self.assertEqual(format_version_pin("bar", "0:1.0-1.el9"), "bar-1.0-1.el9")
