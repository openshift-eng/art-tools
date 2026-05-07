import unittest

from artcommonlib.rpm_utils import parse_nvr, rpm_version_to_golang_v_semver


class TestRpmVersionToGolangVSemver(unittest.TestCase):
    def test_three_segments_unchanged(self):
        self.assertEqual(rpm_version_to_golang_v_semver("v1.25.8"), "v1.25.8")
        self.assertEqual(rpm_version_to_golang_v_semver("1.25.8"), "v1.25.8")

    def test_two_segments_padded(self):
        self.assertEqual(rpm_version_to_golang_v_semver("v1.24"), "v1.24.0")
        self.assertEqual(rpm_version_to_golang_v_semver("1.24"), "v1.24.0")

    def test_one_segment_padded(self):
        self.assertEqual(rpm_version_to_golang_v_semver("v1"), "v1.0.0")
        self.assertEqual(rpm_version_to_golang_v_semver("1"), "v1.0.0")

    def test_four_plus_truncates_to_three(self):
        self.assertEqual(rpm_version_to_golang_v_semver("v1.24.0.1"), "v1.24.0")

    def test_whitespace_stripped(self):
        self.assertEqual(rpm_version_to_golang_v_semver("  v1.22.12 \t"), "v1.22.12")

    def test_uppercase_v_prefix(self):
        self.assertEqual(rpm_version_to_golang_v_semver("V1.20.12"), "v1.20.12")

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            rpm_version_to_golang_v_semver("")
        with self.assertRaises(ValueError):
            rpm_version_to_golang_v_semver("   ")

    def test_empty_segment_raises(self):
        with self.assertRaises(ValueError):
            rpm_version_to_golang_v_semver("1..3")

    def test_via_parse_nvr_golang_builder(self):
        nvr = "openshift-golang-builder-container-v1.22.12-202603171846.p2.g3a22db8.el8"
        v = parse_nvr(nvr)["version"]
        self.assertEqual(rpm_version_to_golang_v_semver(v), "v1.22.12")


if __name__ == "__main__":
    unittest.main()
