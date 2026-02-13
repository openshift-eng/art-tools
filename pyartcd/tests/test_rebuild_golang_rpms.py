import sys
import unittest
from unittest import TestCase
from unittest.mock import MagicMock

from pyartcd.pipelines.rebuild_golang_rpms import RebuildGolangRPMsPipeline


class TestBumpRelease(TestCase):
    def setUp(self):
        if "specfile" not in sys.modules:
            self.skipTest("specfile is only available on Linux")
        runtime = MagicMock()
        self.pipeline = RebuildGolangRPMsPipeline(
            runtime, ocp_version="4.17", go_nvrs=["go1.16"], art_jira="JIRA-123", cves=None
        )

    def test_bump_up_case_1(self):
        fake_releases = "42.rhaos4.17.abcd"
        actual_r = self.pipeline.bump_release(fake_releases)
        expected_r = "43.rhaos4.17.abcd"
        self.assertEqual(actual_r, expected_r)

    def test_bump_up_case_2(self):
        fake_release = "42.1.rhaos4.17.abcd"
        actual_r = self.pipeline.bump_release(fake_release)
        expected_r = "43.rhaos4.17.abcd"
        self.assertEqual(actual_r, expected_r)

    def test_bump_up_case_3(self):
        fake_releases = "rhaos4.17.abcd"
        actual_r = self.pipeline.bump_release(fake_releases)
        expected_r = "1.rhaos4.17.abcd"
        self.assertEqual(actual_r, expected_r)

    def test_bump_up_case_4(self):
        fake_releases = "42.1"
        actual_r = self.pipeline.bump_release(fake_releases)
        expected_r = "43"
        self.assertEqual(actual_r, expected_r)


if __name__ == "__main__":
    unittest.main()
