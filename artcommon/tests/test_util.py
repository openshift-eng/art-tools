import unittest

import yaml

from artcommonlib import util, build_util, release_util
from artcommonlib.model import Model, MissingModel
from artcommonlib.util import deep_merge, isolate_major_minor_in_group


class TestUtil(unittest.TestCase):
    def test_convert_remote_git_to_https(self):
        # git@ to https
        self.assertEqual(util.convert_remote_git_to_https('git@github.com:openshift/aos-cd-jobs.git'),
                         'https://github.com/openshift/aos-cd-jobs')

        # https to https (no-op)
        self.assertEqual(util.convert_remote_git_to_https('https://github.com/openshift/aos-cd-jobs'),
                         'https://github.com/openshift/aos-cd-jobs')

        # https to https, remove suffix
        self.assertEqual(util.convert_remote_git_to_https('https://github.com/openshift/aos-cd-jobs.git'),
                         'https://github.com/openshift/aos-cd-jobs')

        # ssh to https
        self.assertEqual(util.convert_remote_git_to_https('ssh://ocp-build@github.com/openshift/aos-cd-jobs.git'),
                         'https://github.com/openshift/aos-cd-jobs')

    def test_convert_remote_git_to_ssh(self):
        # git@ to https
        self.assertEqual(util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
                         'git@github.com:openshift/aos-cd-jobs.git')

        # https to https (no-op)
        self.assertEqual(util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
                         'git@github.com:openshift/aos-cd-jobs.git')

        # https to https, remove suffix
        self.assertEqual(util.convert_remote_git_to_ssh('https://github.com/openshift/aos-cd-jobs'),
                         'git@github.com:openshift/aos-cd-jobs.git')

        # ssh to https
        self.assertEqual(util.convert_remote_git_to_ssh('ssh://ocp-build@github.com/openshift/aos-cd-jobs.git'),
                         'git@github.com:openshift/aos-cd-jobs.git')

    def test_find_latest_builds(self):
        builds = [
            {"id": 13, "name": "a-container", "version": "v1.2.3", "release": "3.assembly.stream.el8", "tag_name": "tag1"},
            {"id": 12, "name": "a-container", "version": "v1.2.3", "release": "2.assembly.hotfix_a.el9", "tag_name": "tag1"},
            {"id": 11, "name": "a-container", "version": "v1.2.3", "release": "1.assembly.hotfix_a", "tag_name": "tag1"},
            {"id": 23, "name": "b-container", "version": "v1.2.3", "release": "3.assembly.test", "tag_name": "tag1"},
            {"id": 22, "name": "b-container", "version": "v1.2.3", "release": "2.assembly.hotfix_b", "tag_name": "tag1"},
            {"id": 21, "name": "b-container", "version": "v1.2.3", "release": "1.assembly.stream", "tag_name": "tag1"},
            {"id": 33, "name": "c-container", "version": "v1.2.3", "release": "3", "tag_name": "tag1"},
            {"id": 32, "name": "c-container", "version": "v1.2.3", "release": "2.assembly.hotfix_b", "tag_name": "tag1"},
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
            ('1.2.3-y.el9.p1.assembly.test', 'test')
        ]

        for t in test_cases:
            actual = release_util.isolate_assembly_in_release(t[0])
            expected = t[1]
            self.assertEqual(actual, expected)

    def test_isolate_el_version_in_release(self):
        test_cases = [
            ('container-selinux-2.167.0-1.module+el8.5.0+12397+bf23b712:2', 8),
            ('ansible-runner-http-1.0.0-2.el8ar', 8),
            ('1.2.3-y.p.p1.assembly.4.9.99.el7', 7),
            ('1.2.3-y.p.p1.assembly.4.9.el7', 7),
            ('1.2.3-y.p.p1.assembly.art12398.el199', 199),
            ('1.2.3-y.p.p1.assembly.art12398', None),
            ('1.2.3-y.p.p1.assembly.4.7.e.8', None)
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
