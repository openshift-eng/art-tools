import unittest

from elliottlib import brew, util
from elliottlib.bzutil import Bug
from flexmock import flexmock


class TestUtil(unittest.TestCase):
    def test_isolate_timestamp_in_release(self):
        actual = util.isolate_timestamp_in_release("foo-4.7.0-202107021813.p0.g01c9f3f.el8")
        expected = "202107021813"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-202107021907.p0.g8b4b094")
        expected = "202107021907"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-202107021907.p0.g8b4b094")
        expected = "202107021907"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.8.0-202106152230.p0.g25122f5.assembly.stream")
        expected = "202106152230"
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-1.p0.g8b4b094")
        expected = None
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("foo-container-v4.7.0-202199999999.p0.g8b4b094")
        expected = None
        self.assertEqual(actual, expected)

        actual = util.isolate_timestamp_in_release("")
        expected = None
        self.assertEqual(actual, expected)

    def test_get_target_release(self):
        test_cases = [
            {
                "bugs": [flexmock(id=1, target_release="4.8.0")],
                "expected": ("", True),
            },
            {
                "bugs": [flexmock(id=2, target_release=[])],
                "expected": ("", True),
            },
            {
                "bugs": [flexmock(id=3, target_release=["4.8"])],
                "expected": ("4.8", False),
            },
            {
                "bugs": [flexmock(id=4, target_release=["4.8.0"])],
                "expected": ("4.8.0", False),
            },
            {
                "bugs": [flexmock(id=5, target_release=["4.8.0", "4.7.z"])],
                "expected": ("4.8.0", False),
            },
            # bugs belonging to different versions should raise error
            {
                "bugs": [flexmock(id=4, target_release=["4.8.0"]), flexmock(id=5, target_release=["4.7.z"])],
                "expected": ("", True),
            },
            {
                "bugs": [flexmock(id=4, target_release=["4.8.0"]), flexmock(id=5, target_release=["4.8.z"])],
                "expected": ("4.8.0", False),
            },
        ]

        for t in test_cases:
            expected_value, expected_err = t["expected"]
            if expected_err:
                with self.assertRaises(ValueError):
                    Bug.get_target_release(t["bugs"])
            else:
                actual = Bug.get_target_release(t["bugs"])
                self.assertEqual(expected_value, actual)

    def test_get_golang_container_nvrs(self):
        nvrs = [("ose-hypershift-container", "v4.11.0", "202206152147.p0.gaf0b009.assembly.stream")]
        flexmock(brew).should_receive("get_build_objects").and_return(
            [
                {
                    "id": 2052289,
                    "name": "ose-hypershift-container",
                    "nvr": "ose-hypershift-container-v4.11.0-202206152147.p0.gaf0b009.assembly.stream",
                    "release": "202206152147.p0.gaf0b009.assembly.stream",
                    "version": "v4.11.0",
                    "extra": {
                        "image": {
                            "parent_image_builds": {
                                "registry-proxy.engineering.redhat.com/rh-osbs/openshift-golang-builder@sha256:8e1cfc7198db25b97ce6f42e147b5c07d9725015ea971d04c91fe1249b565c80": {
                                    "id": 1977952,
                                    "nvr": "openshift-golang-builder-container-v1.18.0-202204191948.p0.el8.g4d4caca",
                                },
                                "registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-base:v4.11.0-202205301910.p0.gf1330f6.assembly.stream": {
                                    "id": 2031610,
                                    "nvr": "openshift-enterprise-base-container-v4.11.0-202205301910.p0.gf1330f6.assembly.stream",
                                },
                            }
                        }
                    },
                }
            ],
        )
        expected = {"openshift-golang-builder-container-v1.18.0-202204191948.p0.el8.g4d4caca": {nvrs[0]}}
        actual = util.get_golang_container_nvrs(nvrs, None)
        self.assertEqual(expected, actual)

    def test_get_golang_container_nvrs_builder(self):
        nvrs = [("openshift-golang-builder-container", "v1.18.0", "202204191948.p0.el8.g4d4caca")]
        flexmock(brew).should_receive("get_build_objects").and_return(
            [
                {
                    "id": 2052289,
                    "name": "openshift-golang-builder-container",
                    "release": "202204191948.p0.el8.g4d4caca",
                    "version": "v1.18.0",
                }
            ],
        )
        go_version = "1.18.0-2.module+el8.7.0+14880+f5e30240"
        flexmock(util).should_receive("golang_builder_version").and_return(go_version)
        expected = {go_version: {nvrs[0]}}
        actual = util.get_golang_container_nvrs(nvrs, None)
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()
