import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

import yaml
from artcommonlib import build_util, release_util, util
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.model import Model
from artcommonlib.release_util import SoftwareLifecyclePhase
from artcommonlib.util import (
    deep_merge,
    isolate_major_minor_in_group,
    normalize_group_name_for_k8s,
)


class TestUtil(unittest.TestCase):
    def test_convert_remote_git_to_https(self):
        # git@ to https
        self.assertEqual(
            util.convert_remote_git_to_https('git@github.com:openshift/aos-cd-jobs.git'),
            'https://github.com/openshift/aos-cd-jobs',
        )

        # https to https (no-op)
        self.assertEqual(
            util.convert_remote_git_to_https('https://github.com/openshift/aos-cd-jobs'),
            'https://github.com/openshift/aos-cd-jobs',
        )

        # https to https, remove suffix
        self.assertEqual(
            util.convert_remote_git_to_https('https://github.com/openshift/aos-cd-jobs.git'),
            'https://github.com/openshift/aos-cd-jobs',
        )

        # ssh to https
        self.assertEqual(
            util.convert_remote_git_to_https('ssh://ocp-build@github.com/openshift/aos-cd-jobs.git'),
            'https://github.com/openshift/aos-cd-jobs',
        )

    def test_convert_remote_git_to_ssh(self):
        # git@ to https
        self.assertEqual(
            util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
            'git@github.com:openshift/aos-cd-jobs.git',
        )

        # https to https (no-op)
        self.assertEqual(
            util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
            'git@github.com:openshift/aos-cd-jobs.git',
        )

        # https to https, remove suffix
        self.assertEqual(
            util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
            'git@github.com:openshift/aos-cd-jobs.git',
        )

        # ssh to https
        self.assertEqual(
            util.convert_remote_git_to_ssh('ssh://ocp-build@github.com/openshift/aos-cd-jobs.git'),
            'git@github.com:openshift/aos-cd-jobs.git',
        )

    def test_find_latest_builds(self):
        builds = [
            {
                "id": 13,
                "name": "a-container",
                "version": "v1.2.3",
                "release": "3.assembly.stream.el8",
                "tag_name": "tag1",
            },
            {
                "id": 12,
                "name": "a-container",
                "version": "v1.2.3",
                "release": "2.assembly.hotfix_a.el9",
                "tag_name": "tag1",
            },
            {
                "id": 11,
                "name": "a-container",
                "version": "v1.2.3",
                "release": "1.assembly.hotfix_a",
                "tag_name": "tag1",
            },
            {"id": 23, "name": "b-container", "version": "v1.2.3", "release": "3.assembly.test", "tag_name": "tag1"},
            {
                "id": 22,
                "name": "b-container",
                "version": "v1.2.3",
                "release": "2.assembly.hotfix_b",
                "tag_name": "tag1",
            },
            {"id": 21, "name": "b-container", "version": "v1.2.3", "release": "1.assembly.stream", "tag_name": "tag1"},
            {"id": 33, "name": "c-container", "version": "v1.2.3", "release": "3", "tag_name": "tag1"},
            {
                "id": 32,
                "name": "c-container",
                "version": "v1.2.3",
                "release": "2.assembly.hotfix_b",
                "tag_name": "tag1",
            },
            {"id": 31, "name": "c-container", "version": "v1.2.3", "release": "1", "tag_name": "tag1"},
        ]
        actual = build_util.find_latest_builds(builds, "stream")
        self.assertEqual([13, 21, 33], [b["id"] for b in actual])

        actual = build_util.find_latest_builds(builds, "hotfix_a")
        self.assertEqual([12, 21, 33], [b["id"] for b in actual])

        actual = build_util.find_latest_builds(builds, "hotfix_b")
        self.assertEqual([13, 22, 32], [b["id"] for b in actual])

        actual = build_util.find_latest_builds(builds, "test")
        self.assertEqual([13, 23, 33], [b["id"] for b in actual])

        actual = build_util.find_latest_builds(builds, None)
        self.assertEqual([13, 23, 33], [b["id"] for b in actual])

    def test_isolate_assembly_in_release(self):
        test_cases = [
            ('1.2.3-y.p.p1', None),
            ('1.2.3-y.p.p1.assembly', None),
            ('1.2.3-y.p.p1.assembly.x', 'x'),
            ('1.2.3-y.p.p1.assembly.xyz', 'xyz'),
            ('1.2.3-y.p.p1.assembly.xyz.el7', 'xyz'),
            ('1.2.3-y.p.p1.assembly.4.9.99.el7', '4.9.99'),
            ('1.2.3-y.p.p1.assembly.4.9.el700.hi', '4.9'),
            ('1.2.3-y.p.p1.assembly.art12398.el10', 'art12398'),
            ('1.2.3-y.p.p1.assembly.art12398.el10', 'art12398'),
            ('1.2.3-y.el9.p1.assembly.test', 'test'),
        ]

        for t in test_cases:
            actual = release_util.isolate_assembly_in_release(t[0])
            expected = t[1]
            self.assertEqual(actual, expected)

    def test_split_el_suffix_in_release(self):
        test_cases = [
            # (release_string, expected_prefix, expected_suffix)
            ('1.2.3-y.p.p1.assembly.4.9.99.el7', '1.2.3-y.p.p1.assembly.4.9.99', 'el7'),
            ('ansible-runner-http-1.0.0-2.el8ar', 'ansible-runner-http-1.0.0-2', 'el8'),
            ('1.2.3-y.p.p1.assembly.art12398.el199', '1.2.3-y.p.p1.assembly.art12398', 'el199'),
            ('1.2.3-y.p.p1.assembly.art12398', '1.2.3-y.p.p1.assembly.art12398', None),
            # OKD/SCOS test cases
            (
                '4.17.0-202407241200.p0.assembly.stream.gdeadbee.scos9',
                '4.17.0-202407241200.p0.assembly.stream.gdeadbee',
                'scos9',
            ),
            ('1.2.3-y.p.p1.assembly.4.9.99.scos8', '1.2.3-y.p.p1.assembly.4.9.99', 'scos8'),
            ('1.2.3-y.p.p1.assembly.art12398.scos10', '1.2.3-y.p.p1.assembly.art12398', 'scos10'),
        ]

        for t in test_cases:
            prefix, suffix = release_util.split_el_suffix_in_release(t[0])
            self.assertEqual(prefix, t[1])
            self.assertEqual(suffix, t[2])

    def test_isolate_el_version_in_release(self):
        test_cases = [
            ('container-selinux-2.167.0-1.module+el8.5.0+12397+bf23b712:2', 8),
            ('ansible-runner-http-1.0.0-2.el8ar', 8),
            ('1.2.3-y.p.p1.assembly.4.9.99.el7', 7),
            ('1.2.3-y.p.p1.assembly.4.9.el7', 7),
            ('1.2.3-y.p.p1.assembly.art12398.el199', 199),
            ('1.2.3-y.p.p1.assembly.art12398', None),
            ('1.2.3-y.p.p1.assembly.4.7.e.8', None),
            # OKD/SCOS test cases
            ('4.17.0-202407241200.p0.assembly.stream.gdeadbee.scos9', 9),
            ('1.2.3-y.p.p1.assembly.4.9.99.scos8', 8),
            ('1.2.3-y.p.p1.assembly.art12398.scos10', 10),
        ]

        for t in test_cases:
            actual = release_util.isolate_el_version_in_release(t[0])
            expected = t[1]
            self.assertEqual(actual, expected)

    def test_isolate_rhel_major_from_version(self):
        self.assertEqual(9, util.isolate_rhel_major_from_version('9.2'))
        self.assertEqual(8, util.isolate_rhel_major_from_version('8.6'))
        self.assertEqual(10, util.isolate_rhel_major_from_version('10.1'))
        self.assertEqual(None, util.isolate_rhel_major_from_version('invalid'))

    def test_isolate_rhel_major_from_distgit_branch(self):
        self.assertEqual(9, util.isolate_rhel_major_from_distgit_branch('rhaos-4.16-rhel-9'))
        self.assertEqual(8, util.isolate_rhel_major_from_distgit_branch('rhaos-4.16-rhel-8'))
        self.assertEqual(10, util.isolate_rhel_major_from_distgit_branch('rhaos-4.16-rhel-10'))
        self.assertEqual(None, util.isolate_rhel_major_from_distgit_branch('invalid'))

    def test_merge_objects(self):
        yaml_data = """
content:
  source:
    git:
      branch:
        target: release-4.16
    ci_alignment:
      streams_prs:
        ci_build_root:
          stream: rhel-9-golang-ci-build-root
distgit:
  branch: rhaos-4.16-rhel-9
from:
  builder:
  - stream: golang
  - stream: rhel-9-golang
  member: openshift-enterprise-base-rhel9
name: openshift/ose-machine-config-operator-rhel9
alternative_upstream:
- when: el8
  distgit:
    branch: rhaos-4.16-rhel-8
  from:
    member: openshift-enterprise-base
  name: openshift/ose-machine-config-operator
  content:
    source:
      ci_alignment:
        streams_prs:
          ci_build_root:
            stream: rhel-8-golang-ci-build-root
        """

        config = Model(yaml.safe_load(yaml_data))
        alt_config = config.alternative_upstream[0]
        merged_config = Model(deep_merge(config.primitive(), alt_config.primitive()))

        self.assertEqual(merged_config.name, 'openshift/ose-machine-config-operator')
        self.assertEqual(merged_config.distgit.branch, 'rhaos-4.16-rhel-8')
        self.assertEqual(merged_config.content.source.git.branch.target, 'release-4.16')
        self.assertEqual(merged_config.get('from').get('builder'), [{'stream': 'golang'}, {'stream': 'rhel-9-golang'}])
        self.assertEqual(merged_config.get('from').get('member'), 'openshift-enterprise-base')

    def test_isolate_major_minor_in_group(self):
        major, minor = isolate_major_minor_in_group('openshift-4.16')
        self.assertEqual(major, 4)
        self.assertEqual(minor, 16)

        major, minor = isolate_major_minor_in_group('invalid-4.16')
        self.assertEqual(major, None)
        self.assertEqual(minor, None)

        major, minor = isolate_major_minor_in_group('openshift-4.invalid')
        self.assertEqual(major, None)
        self.assertEqual(minor, None)

        major, minor = isolate_major_minor_in_group('openshift-invalid.16')
        self.assertEqual(major, None)
        self.assertEqual(minor, None)


class TestSoftwareLifecyclePhase(unittest.TestCase):
    def test_from_name_valid(self):
        self.assertEqual(SoftwareLifecyclePhase.from_name('eol'), SoftwareLifecyclePhase.EOL)
        self.assertEqual(SoftwareLifecyclePhase.from_name('pre-release'), SoftwareLifecyclePhase.PRE_RELEASE)
        self.assertEqual(SoftwareLifecyclePhase.from_name('signing'), SoftwareLifecyclePhase.SIGNING)
        self.assertEqual(SoftwareLifecyclePhase.from_name('release'), SoftwareLifecyclePhase.RELEASE)

    def test_from_name_invalid(self):
        with self.assertRaises(ValueError):
            SoftwareLifecyclePhase.from_name('invalid')

    def test_lt(self):
        self.assertTrue(SoftwareLifecyclePhase.PRE_RELEASE < SoftwareLifecyclePhase.SIGNING)
        self.assertTrue(SoftwareLifecyclePhase.SIGNING < SoftwareLifecyclePhase.RELEASE)
        self.assertTrue(SoftwareLifecyclePhase.RELEASE < SoftwareLifecyclePhase.EOL)
        self.assertTrue(SoftwareLifecyclePhase.EOL < 101)
        self.assertTrue(SoftwareLifecyclePhase.PRE_RELEASE < 1)

    def test_gt(self):
        self.assertTrue(SoftwareLifecyclePhase.RELEASE > SoftwareLifecyclePhase.SIGNING)
        self.assertTrue(SoftwareLifecyclePhase.SIGNING > SoftwareLifecyclePhase.PRE_RELEASE)
        self.assertTrue(SoftwareLifecyclePhase.EOL > SoftwareLifecyclePhase.RELEASE)
        self.assertTrue(SoftwareLifecyclePhase.RELEASE > 1)
        self.assertTrue(SoftwareLifecyclePhase.SIGNING > 0)

    def test_le(self):
        self.assertTrue(SoftwareLifecyclePhase.EOL <= SoftwareLifecyclePhase.EOL)
        self.assertTrue(SoftwareLifecyclePhase.PRE_RELEASE <= SoftwareLifecyclePhase.SIGNING)
        self.assertTrue(SoftwareLifecyclePhase.SIGNING <= SoftwareLifecyclePhase.RELEASE)
        self.assertTrue(SoftwareLifecyclePhase.EOL <= 100)
        self.assertTrue(SoftwareLifecyclePhase.PRE_RELEASE <= 0)

    def test_ge(self):
        self.assertTrue(SoftwareLifecyclePhase.RELEASE >= SoftwareLifecyclePhase.RELEASE)
        self.assertTrue(SoftwareLifecyclePhase.SIGNING >= SoftwareLifecyclePhase.PRE_RELEASE)
        self.assertTrue(SoftwareLifecyclePhase.EOL >= SoftwareLifecyclePhase.PRE_RELEASE)
        self.assertTrue(SoftwareLifecyclePhase.RELEASE >= 2)
        self.assertTrue(SoftwareLifecyclePhase.SIGNING >= 1)

    def test_eq(self):
        self.assertEqual(SoftwareLifecyclePhase.RELEASE, 2)
        self.assertEqual(SoftwareLifecyclePhase.RELEASE, SoftwareLifecyclePhase.RELEASE)
        self.assertNotEqual(SoftwareLifecyclePhase.RELEASE, SoftwareLifecyclePhase.PRE_RELEASE)
        self.assertEqual(SoftwareLifecyclePhase.SIGNING, 1)
        self.assertEqual(SoftwareLifecyclePhase.PRE_RELEASE, 0)
        self.assertEqual(SoftwareLifecyclePhase.EOL.value, 100)
        self.assertNotEqual(SoftwareLifecyclePhase.EOL.value, 101)

    def test_isolate_timestamp_in_release(self):
        actual = release_util.isolate_timestamp_in_release("foo-4.7.0-202107021813.p0.g01c9f3f.el8")
        expected = "202107021813"
        self.assertEqual(actual, expected)

        actual = release_util.isolate_timestamp_in_release("foo-container-v4.7.0-202107021907.p0.g8b4b094")
        expected = "202107021907"
        self.assertEqual(actual, expected)

        actual = release_util.isolate_timestamp_in_release("foo-container-v4.7.0-202107021907.p0.g8b4b094")
        expected = "202107021907"
        self.assertEqual(actual, expected)

        actual = release_util.isolate_timestamp_in_release(
            "foo-container-v4.8.0-202106152230.p0.g25122f5.assembly.stream"
        )
        expected = "202106152230"
        self.assertEqual(actual, expected)

        actual = release_util.isolate_timestamp_in_release("foo-container-v4.7.0-1.p0.g8b4b094")
        expected = None
        self.assertEqual(actual, expected)

        actual = release_util.isolate_timestamp_in_release("foo-container-v4.7.0-202199999999.p0.g8b4b094")
        expected = None
        self.assertEqual(actual, expected)

        actual = release_util.isolate_timestamp_in_release("")
        expected = None
        self.assertEqual(actual, expected)


class TestNormalizeGroupNameForK8s(unittest.TestCase):
    """Test cases for normalize_group_name_for_k8s function"""

    def test_simple_oadp_group(self):
        """Test simple oadp group name normalization"""
        result = normalize_group_name_for_k8s("oadp-1.5")
        self.assertEqual(result, "oadp-1-5")

    def test_complex_group_name(self):
        """Test complex group name with mixed case, underscores, and dots"""
        result = normalize_group_name_for_k8s("Test_Group-1.5")
        self.assertEqual(result, "test-group-1-5")

    def test_empty_string(self):
        """Test empty string input"""
        result = normalize_group_name_for_k8s("")
        self.assertEqual(result, "")

    def test_consecutive_dashes(self):
        """Test collapse of consecutive dashes"""
        result = normalize_group_name_for_k8s("test--group---name")
        self.assertEqual(result, "test-group-name")

    def test_leading_trailing_special_chars(self):
        """Test trimming of leading/trailing non-alphanumeric characters"""
        result = normalize_group_name_for_k8s("-_test.group_-")
        self.assertEqual(result, "test-group")

    def test_only_special_chars(self):
        """Test string with only special characters"""
        result = normalize_group_name_for_k8s("_..-__")
        self.assertEqual(result, "")

    def test_mixed_alphanumeric_special(self):
        """Test mixed alphanumeric and special characters"""
        result = normalize_group_name_for_k8s("test@group#1.2$name")
        self.assertEqual(result, "test-group-1-2-name")

    def test_long_group_name_truncation(self):
        """Test truncation of very long group names"""
        long_name = "a" * 100  # 100 character string
        result = normalize_group_name_for_k8s(long_name)
        # Should be truncated to leave room for timestamp (max 63 - 18 - 1 = 44 chars)
        self.assertLessEqual(len(result), 44)
        self.assertTrue(result.startswith("a"))

    def test_uppercase_conversion(self):
        """Test uppercase to lowercase conversion"""
        result = normalize_group_name_for_k8s("OADP-1.5")
        self.assertEqual(result, "oadp-1-5")

    def test_numeric_group(self):
        """Test group name with numbers"""
        result = normalize_group_name_for_k8s("group123-4.56")
        self.assertEqual(result, "group123-4-56")


class TestArtImageRepoHelpers(unittest.TestCase):
    """Tests for get_art_prod_image_repo_for_version"""

    def test_get_art_image_repo_ocp_4_dev(self):
        """Test OCP 4.x dev repository"""
        repo = util.get_art_prod_image_repo_for_version(4, "dev")
        self.assertEqual(repo, "quay.io/openshift-release-dev/ocp-v4.0-art-dev")

    def test_get_art_image_repo_ocp_5_dev(self):
        """Test OCP 5.x dev repository"""
        repo = util.get_art_prod_image_repo_for_version(5, "dev")
        self.assertEqual(repo, "quay.io/openshift-release-dev/ocp-v5.0-art-dev")

    def test_get_art_image_repo_ocp_5_dev_priv(self):
        """Test OCP 5.x private dev repository"""
        repo = util.get_art_prod_image_repo_for_version(5, "dev-priv")
        self.assertEqual(repo, "quay.io/openshift-release-dev/ocp-v5.0-art-dev-priv")

    def test_get_art_image_repo_ocp_4_prev(self):
        """Test OCP 4.x prev repository"""
        repo = util.get_art_prod_image_repo_for_version(4, "prev")
        self.assertEqual(repo, "quay.io/openshift-release-dev/ocp-v4.0-art-prev")

    def test_get_art_image_repo_ocp_4_test(self):
        """Test OCP 4.x test repository"""
        repo = util.get_art_prod_image_repo_for_version(4, "test")
        self.assertEqual(repo, "quay.io/openshift-release-dev/ocp-v4.0-art-test")

    def test_get_art_image_repo_rejects_ocp_3(self):
        """Test that OCP 3.x is rejected"""
        with self.assertRaises(ValueError) as ctx:
            util.get_art_prod_image_repo_for_version(3, "dev")
        self.assertIn("ART image repos only exist for OCP 4.x and later", str(ctx.exception))
        self.assertIn("3.x", str(ctx.exception))

    def test_get_art_image_repo_invalid_repo_type(self):
        """Test invalid repo_type parameter"""
        with self.assertRaises(ValueError) as ctx:
            util.get_art_prod_image_repo_for_version(4, "invalid")
        self.assertIn("Invalid repo_type", str(ctx.exception))
        self.assertIn("invalid", str(ctx.exception))


class TestKonfluxImagestreamOverride(unittest.TestCase):
    """Tests for uses_konflux_imagestream_override"""

    def test_versions_below_4_12(self):
        """Test versions below 4.12 return False"""
        self.assertFalse(util.uses_konflux_imagestream_override("4.11"))
        self.assertFalse(util.uses_konflux_imagestream_override("4.10"))
        self.assertFalse(util.uses_konflux_imagestream_override("4.0"))
        self.assertFalse(util.uses_konflux_imagestream_override("3.11"))
        self.assertFalse(util.uses_konflux_imagestream_override("3.0"))

    def test_version_4_12(self):
        """Test version 4.12 returns True (boundary)"""
        self.assertTrue(util.uses_konflux_imagestream_override("4.12"))

    def test_versions_above_4_12(self):
        """Test versions above 4.12 return True"""
        self.assertTrue(util.uses_konflux_imagestream_override("4.13"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.14"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.15"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.16"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.17"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.18"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.19"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.20"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.21"))
        self.assertTrue(util.uses_konflux_imagestream_override("4.22"))

    def test_ocp_5_versions(self):
        """Test all OCP 5.x versions return True"""
        self.assertTrue(util.uses_konflux_imagestream_override("5.0"))
        self.assertTrue(util.uses_konflux_imagestream_override("5.1"))
        self.assertTrue(util.uses_konflux_imagestream_override("5.10"))
        self.assertTrue(util.uses_konflux_imagestream_override("5.99"))

    def test_future_major_versions(self):
        """Test future major versions return True"""
        self.assertTrue(util.uses_konflux_imagestream_override("6.0"))
        self.assertTrue(util.uses_konflux_imagestream_override("10.0"))

    def test_invalid_versions(self):
        """Test invalid version strings raise ValueError"""
        with self.assertRaises(ValueError):
            util.uses_konflux_imagestream_override("invalid")
        with self.assertRaises(ValueError):
            util.uses_konflux_imagestream_override("4")
        with self.assertRaises(ValueError):
            util.uses_konflux_imagestream_override("4.12.1")
        with self.assertRaises(ValueError):
            util.uses_konflux_imagestream_override("")
        with self.assertRaises(ValueError):
            util.uses_konflux_imagestream_override("openshift-4.12")


class TestIsFutureReleaseDate(unittest.TestCase):
    """Tests for is_future_release_date function"""

    def test_future_date_yyyy_mm_dd_format(self):
        """Test future date in YYYY-MM-DD format"""
        future_date = "2099-12-31"
        result = util.is_future_release_date(future_date)
        self.assertTrue(result)

    def test_past_date_yyyy_mm_dd_format(self):
        """Test past date in YYYY-MM-DD format"""
        past_date = "2020-01-01"
        result = util.is_future_release_date(past_date)
        self.assertFalse(result)

    def test_future_date_yyyy_mmm_dd_format(self):
        """Test future date in YYYY-Mon-DD format (e.g., 2099-Dec-31)"""
        future_date = "2099-Dec-31"
        result = util.is_future_release_date(future_date)
        self.assertTrue(result)

    def test_past_date_yyyy_mmm_dd_format(self):
        """Test past date in YYYY-Mon-DD format (e.g., 2020-Jan-01)"""
        past_date = "2020-Jan-01"
        result = util.is_future_release_date(past_date)
        self.assertFalse(result)

    def test_invalid_date_format(self):
        """Test invalid date format returns False"""
        invalid_date = "not-a-date"
        result = util.is_future_release_date(invalid_date)
        self.assertFalse(result)

    def test_empty_string(self):
        """Test empty string returns False"""
        result = util.is_future_release_date("")
        self.assertFalse(result)

    def test_partial_date(self):
        """Test partial date returns False"""
        result = util.is_future_release_date("2024-01")
        self.assertFalse(result)

    def test_various_month_abbreviations(self):
        """Test various month abbreviations in YYYY-Mon-DD format"""
        # Test different month abbreviations
        test_cases = [
            ("2099-Jan-15", True),  # Future
            ("2099-Feb-15", True),
            ("2099-Mar-15", True),
            ("2020-Apr-15", False),  # Past
            ("2020-May-15", False),
            ("2020-Jun-15", False),
        ]
        for date_str, expected in test_cases:
            result = util.is_future_release_date(date_str)
            self.assertEqual(result, expected, f"Failed for date: {date_str}")


# Legacy group-based resolver tests removed - functions no longer exist
# Use product-based resolvers: resolve_konflux_kubeconfig_by_product() and resolve_konflux_namespace_by_product()
