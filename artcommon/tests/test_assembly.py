from datetime import date, datetime, timedelta, timezone
from unittest import TestCase

import yaml
from artcommonlib.assembly import (
    _merger,
    assembly_basis_event,
    assembly_config_struct,
    assembly_excluded_components,
    assembly_group_config,
    assembly_metadata_config,
    assembly_own_issues_config,
    assembly_permits,
    assembly_resolved,
    assembly_rhcos_config,
    assembly_targeted_fixes_only,
    assembly_validate_member_distgit_keys,
    check_assembly_overrides_expiry,
)
from artcommonlib.model import Missing, Model


class TestAssembly(TestCase):
    def setUp(self) -> None:
        releases_yml = """
releases:
  ART_1:
    assembly:
      members:
        rpms:
        - distgit_key: openshift-kuryr
          metadata:  # changes to make the metadata
            content:
              source:
                git:
                  url: git@github.com:jupierce/kuryr-kubernetes.git
                  branch:
                    target: 1_hash
      group:
        arches:
        - x86_64
        - ppc64le
        - s390x
        advisories:
          image: 11
          extras: 12

  ART_2:
    assembly:
      basis:
        brew_event: 5
      members:
        rpms:
        - distgit_key: openshift-kuryr
          metadata:  # changes to make the metadata
            content:
              source:
                git:
                  url: git@github.com:jupierce/kuryr-kubernetes.git
                  branch:
                    target: 2_hash
      group:
        arches:
        - x86_64
        - s390x
        advisories:
          image: 21

  ART_3:
    assembly:
      basis:
        assembly: ART_2
      group:
        advisories:
          image: 31

  ART_4:
    assembly:
      basis:
        assembly: ART_3
      group:
        advisories!:
          image: 41

  ART_5:
    assembly:
      basis:
        assembly: ART_4
      group:
        arches!:
        - s390x
        advisories!:
          image: 51

  ART_6:
    assembly:
      basis:
        assembly: ART_5
      members:
        rpms:
        - distgit_key: '*'
          metadata:
            content:
              source:
                git:
                  branch:
                    target: customer_6

  ART_7:
    assembly:
      basis:
        brew_event: 5
      members:
        images:
        - distgit_key: openshift-kuryr
          metadata:
            content:
              source:
                git:
                  url: git@github.com:jupierce/kuryr-kubernetes.git
                  branch:
                    target: 1_hash
            is: kuryr-nvr
            dependencies:
              rpms:
              - el7: some-nvr-1
                non_gc_tag: some-tag-1
      group:
        dependencies:
          rpms:
            - el7: some-nvr-3
              non_gc_tag: some-tag-3
      rhcos:
        machine-os-content:
          images:
            x86_64: registry.example.com/rhcos-x86_64:test
        dependencies:
          rpms:
            - el7: some-nvr-4
              non_gc_tag: some-tag-4
            - el8: some-nvr-5
              non_gc_tag: some-tag-4

  ART_8:
    assembly:
      basis:
        assembly: ART_7
      members:
        images:
        - distgit_key: openshift-kuryr
          metadata:
            is: kuryr-nvr2
            dependencies:
              rpms:
              - el7: some-nvr-2
                non_gc_tag: some-tag-2
      group:
        dependencies:
          rpms:
            - el7: some-nvr-4
              non_gc_tag: some-tag-4
      rhcos:
        machine-os-content:
          images: {}
        dependencies:
          rpms:
            - el8: some-nvr-6
              non_gc_tag: some-tag-6

  ART_INFINITE:
    assembly:
      basis:
        assembly: ART_INFINITE
      members:
        rpms:
        - distgit_key: '*'
          metadata:
            content:
              source:
                git:
                  branch:
                    target: customer_6

"""
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))

    def test_assembly_rhcos_config(self):
        rhcos_config = assembly_rhcos_config(self.releases_config, "ART_8")
        self.assertEqual(len(rhcos_config.dependencies.rpms), 3)

    def test_assembly_basis_event(self):
        self.assertEqual(assembly_basis_event(self.releases_config, 'ART_1'), None)
        self.assertEqual(assembly_basis_event(self.releases_config, 'ART_6'), 5)

        try:
            assembly_basis_event(self.releases_config, 'ART_INFINITE')
            self.fail('Expected ValueError on assembly infinite recursion')
        except ValueError:
            pass
        except Exception as e:
            self.fail(f'Expected ValueError on assembly infinite recursion but got: {type(e)}: {e}')

    def test_assembly_basis_time_invalid_1(self):
        releases_yml = """
releases:
  foo:
    assembly:
      basis:
        time: 2021-01-01T00:00:00Z
    type: standard
"""
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        with self.assertRaises(ValueError) as cm:
            assembly_basis_event(self.releases_config, 'foo', build_system='konflux')
        self.assertIn("Invalid time format for assembly", str(cm.exception))

    def test_assembly_basis_time_invalid_2(self):
        releases_yml = """
releases:
  foo:
    assembly:
      basis:
        time: not_a_valid_datetime
    type: standard
"""
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        with self.assertRaises(ValueError) as cm:
            assembly_basis_event(self.releases_config, 'foo', build_system='konflux')
        self.assertIn("Invalid isoformat string", str(cm.exception))

    def test_assembly_basis_time_valid(self):
        releases_yml = """
releases:
  foo:
    assembly:
      basis:
        time: "2021-01-01T00:00:00Z"
    type: standard
"""
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        self.assertEqual(
            assembly_basis_event(releases_config=self.releases_config, assembly='foo', build_system='konflux'),
            datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

    def test_asssembly_basis_time_with_brew_event_1(self):
        releases_yml = """
        releases:
          foo:
            assembly:
              basis:
                time: "2021-01-01T00:00:00Z"
                brew_event: 123456
            type: standard
        """
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        self.assertEqual(
            assembly_basis_event(releases_config=self.releases_config, assembly='foo', build_system='konflux'),
            datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

    def test_asssembly_basis_time_with_brew_event_2(self):
        releases_yml = """
        releases:
          foo:
            assembly:
              basis:
                time: "2021-01-01T00:00:00Z"
                brew_event: 123456
            type: standard
        """
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        self.assertEqual(
            assembly_basis_event(releases_config=self.releases_config, assembly='foo', build_system='brew'),
            123456,
        )

    def test_asssembly_basis_time_with_brew_event_3(self):
        releases_yml = """
        releases:
          foo:
            assembly:
              basis:
                time: "2021-01-01T00:00:00Z"
            type: standard
        """
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        self.assertEqual(
            assembly_basis_event(releases_config=self.releases_config, assembly='foo', build_system='brew'),
            datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

    def test_asssembly_basis_time_with_brew_event_4(self):
        releases_yml = """
        releases:
          foo:
            assembly:
              basis: {}
            type: standard
        """
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))

        with self.assertRaises(ValueError) as _:
            assembly_basis_event(
                releases_config=self.releases_config, assembly='foo', build_system='konflux', strict=True
            )

        with self.assertRaises(ValueError) as _:
            assembly_basis_event(releases_config=self.releases_config, assembly='foo', build_system='brew', strict=True)

    def test_assembly_group_config(self):
        group_config = Model(
            dict_to_model={
                'arches': [
                    'x86_64',
                ],
                'advisories': {
                    'image': 1,
                    'extras': 1,
                },
            }
        )

        config = assembly_group_config(self.releases_config, 'ART_1', group_config)
        self.assertEqual(len(config.arches), 3)

        config = assembly_group_config(self.releases_config, 'ART_2', group_config)
        self.assertEqual(len(config.arches), 2)

        # 3 inherits from 2 an only overrides advisory value
        config = assembly_group_config(self.releases_config, 'ART_3', group_config)
        self.assertEqual(len(config.arches), 2)
        self.assertEqual(config.advisories.image, 31)
        self.assertEqual(config.advisories.extras, 1)  # Extras never override, so should be from group_config

        # 4 inherits from 3, but sets "advsories!"
        config = assembly_group_config(self.releases_config, 'ART_4', group_config)
        self.assertEqual(len(config.arches), 2)
        self.assertEqual(config.advisories.image, 41)
        self.assertEqual(config.advisories.extras, Missing)

        # 5 inherits from 4, but sets "advsories!" (overriding 4's !) and "arches!"
        config = assembly_group_config(self.releases_config, 'ART_5', group_config)
        self.assertEqual(len(config.arches), 1)
        self.assertEqual(config.advisories.image, 51)

        config = assembly_group_config(self.releases_config, 'not_defined', group_config)
        self.assertEqual(len(config.arches), 1)

        config = assembly_group_config(self.releases_config, 'ART_7', group_config)
        self.assertEqual(len(config.dependencies.rpms), 1)

        config = assembly_group_config(self.releases_config, 'ART_8', group_config)
        self.assertEqual(len(config.dependencies.rpms), 2)

        try:
            assembly_group_config(self.releases_config, 'ART_INFINITE', group_config)
            self.fail('Expected ValueError on assembly infinite recursion')
        except ValueError:
            pass
        except Exception as e:
            self.fail(f'Expected ValueError on assembly infinite recursion but got: {type(e)}: {e}')

    def test_assembly_config_struct(self):
        release_configs = {
            "releases": {
                "child": {
                    "assembly": {
                        "basis": {
                            "assembly": "parent",
                        },
                    },
                },
                "parent": {
                    "assembly": {
                        "type": "custom",
                    },
                },
            },
        }
        actual = assembly_config_struct(Model(release_configs), "child", "type", "standard")
        self.assertEqual(actual, "custom")

        release_configs = {
            "releases": {
                "child": {
                    "assembly": {
                        "basis": {
                            "assembly": "parent",
                        },
                        "type": "candidate",
                    },
                },
                "parent": {
                    "assembly": {
                        "type": "custom",
                    },
                },
            },
        }
        actual = assembly_config_struct(Model(release_configs), "child", "type", "standard")
        self.assertEqual(actual, "candidate")

        release_configs = {
            "releases": {
                "child": {
                    "assembly": {
                        "basis": {
                            "assembly": "parent",
                        },
                    },
                },
                "parent": {
                    "assembly": {},
                },
            },
        }
        actual = assembly_config_struct(Model(release_configs), "child", "type", "standard")
        self.assertEqual(actual, "standard")

        release_configs = {
            "releases": {
                "child": {
                    "assembly": {
                        "basis": {
                            "assembly": "parent",
                        },
                    },
                },
                "parent": {
                    "assembly": {
                        "type": None,
                    },
                },
            },
        }
        actual = assembly_config_struct(Model(release_configs), "child", "type", "standard")
        self.assertEqual(actual, None)

        release_configs = {
            "releases": {
                "child": {
                    "assembly": {
                        "basis": {
                            "assembly": "parent",
                        },
                        "foo": {
                            "a": 1,
                            "b": 2,
                        },
                        "bar": [1, 2, 3],
                    },
                },
                "parent": {
                    "assembly": {
                        "foo": {
                            "b": 3,
                            "c": 4,
                        },
                        "bar": [0, 2, 4],
                    },
                },
            },
        }
        actual = assembly_config_struct(Model(release_configs), "child", "foo", {})
        self.assertEqual(
            actual,
            {
                "a": 1,
                "b": 2,
                "c": 4,
            },
        )
        actual = assembly_config_struct(Model(release_configs), "child", "bar", [])
        self.assertEqual(actual, [0, 1, 2, 3, 4])

    def test_asembly_metadata_config(self):
        meta_config = Model(
            dict_to_model={
                'owners': ['kuryr-team@redhat.com'],
                'content': {
                    'source': {
                        'git': {
                            'url': 'git@github.com:openshift-priv/kuryr-kubernetes.git',
                            'branch': {
                                'target': 'release-4.8',
                            },
                        },
                        'specfile': 'openshift-kuryr-kubernetes-rhel8.spec',
                    },
                },
                'name': 'openshift-kuryr',
            }
        )

        config = assembly_metadata_config(self.releases_config, 'ART_1', 'rpm', 'openshift-kuryr', meta_config)
        # Ensure no loss
        self.assertEqual(config.name, 'openshift-kuryr')
        self.assertEqual(len(config.owners), 1)
        self.assertEqual(config.owners[0], 'kuryr-team@redhat.com')
        # Check that things were overridden
        self.assertEqual(config.content.source.git.url, 'git@github.com:jupierce/kuryr-kubernetes.git')
        self.assertEqual(config.content.source.git.branch.target, '1_hash')

        config = assembly_metadata_config(self.releases_config, 'ART_5', 'rpm', 'openshift-kuryr', meta_config)
        # Ensure no loss
        self.assertEqual(config.name, 'openshift-kuryr')
        self.assertEqual(len(config.owners), 1)
        self.assertEqual(config.owners[0], 'kuryr-team@redhat.com')
        # Check that things were overridden
        self.assertEqual(config.content.source.git.url, 'git@github.com:jupierce/kuryr-kubernetes.git')
        self.assertEqual(config.content.source.git.branch.target, '2_hash')

        config = assembly_metadata_config(self.releases_config, 'ART_6', 'rpm', 'openshift-kuryr', meta_config)
        # Ensure no loss
        self.assertEqual(config.name, 'openshift-kuryr')
        self.assertEqual(len(config.owners), 1)
        self.assertEqual(config.owners[0], 'kuryr-team@redhat.com')
        # Check that things were overridden. 6 changes branches for all rpms
        self.assertEqual(config.content.source.git.url, 'git@github.com:jupierce/kuryr-kubernetes.git')
        self.assertEqual(config.content.source.git.branch.target, 'customer_6')

        config = assembly_metadata_config(self.releases_config, 'ART_8', 'image', 'openshift-kuryr', meta_config)
        # Ensure no loss
        self.assertEqual(config.name, 'openshift-kuryr')
        self.assertEqual(config.content.source.git.url, 'git@github.com:jupierce/kuryr-kubernetes.git')
        self.assertEqual(config.content.source.git.branch.target, '1_hash')
        # Ensure that 'is' comes from ART_8 and not ART_7
        self.assertEqual(config['is'], 'kuryr-nvr2')
        # Ensure that 'dependencies' were accumulate
        self.assertEqual(len(config.dependencies.rpms), 2)

        try:
            assembly_metadata_config(self.releases_config, 'ART_INFINITE', 'rpm', 'openshift-kuryr', meta_config)
            self.fail('Expected ValueError on assembly infinite recursion')
        except ValueError:
            pass
        except Exception as e:
            self.fail(f'Expected ValueError on assembly infinite recursion but got: {type(e)}: {e}')

    def test_assembly_excluded_components_no_assembly(self):
        self.assertEqual(assembly_excluded_components(self.releases_config, None, 'image'), set())
        self.assertEqual(assembly_excluded_components(self.releases_config, '', 'image'), set())
        self.assertEqual(assembly_excluded_components(None, 'ART_1', 'image'), set())

    def test_assembly_excluded_components_no_exclusions(self):
        self.assertEqual(assembly_excluded_components(self.releases_config, 'ART_7', 'image'), set())

    def test_assembly_excluded_components_basic(self):
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        images:
        - distgit_key: image-a
          exclude: true
        - distgit_key: image-b
          metadata:
            is: some-nvr
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        excluded = assembly_excluded_components(releases_config, 'test_assembly', 'image')
        self.assertEqual(excluded, {'image-a'})

    def test_assembly_excluded_components_explicit_false(self):
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        images:
        - distgit_key: image-a
          exclude: false
        - distgit_key: image-b
          exclude: true
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        excluded = assembly_excluded_components(releases_config, 'test_assembly', 'image')
        self.assertEqual(excluded, {'image-b'})

    def test_assembly_excluded_components_inheritance(self):
        releases_yml = """
releases:
  parent:
    assembly:
      members:
        images:
        - distgit_key: image-a
          exclude: true
        - distgit_key: image-b
          exclude: true
  child:
    assembly:
      basis:
        assembly: parent
      members:
        images:
        - distgit_key: image-c
          exclude: true
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        excluded = assembly_excluded_components(releases_config, 'child', 'image')
        self.assertEqual(excluded, {'image-a', 'image-b', 'image-c'})

    def test_assembly_excluded_components_child_overrides_parent(self):
        releases_yml = """
releases:
  parent:
    assembly:
      members:
        images:
        - distgit_key: image-a
          exclude: true
        - distgit_key: image-b
          exclude: true
  child:
    assembly:
      basis:
        assembly: parent
      members:
        images:
        - distgit_key: image-a
          exclude: false
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        excluded = assembly_excluded_components(releases_config, 'child', 'image')
        self.assertEqual(excluded, {'image-b'})

    def test_assembly_excluded_components_rpms(self):
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        rpms:
        - distgit_key: rpm-a
          exclude: true
        - distgit_key: rpm-b
          metadata:
            content: {}
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        excluded = assembly_excluded_components(releases_config, 'test_assembly', 'rpm')
        self.assertEqual(excluded, {'rpm-a'})
        # images should be unaffected
        self.assertEqual(assembly_excluded_components(releases_config, 'test_assembly', 'image'), set())

    def test_assembly_excluded_components_infinite_recursion(self):
        with self.assertRaises(ValueError):
            assembly_excluded_components(self.releases_config, 'ART_INFINITE', 'rpm')

    def test_assembly_resolved_empty(self):
        self.assertEqual(assembly_resolved(self.releases_config, None).primitive(), {})
        self.assertEqual(assembly_resolved(self.releases_config, '').primitive(), {})
        self.assertEqual(assembly_resolved(None, 'ART_1').primitive(), {})

    def test_assembly_resolved_no_inheritance(self):
        resolved = assembly_resolved(self.releases_config, 'ART_1')
        self.assertEqual(resolved.group.advisories.image, 11)
        self.assertEqual(resolved.group.advisories.extras, 12)
        self.assertEqual(len(resolved.group.arches), 3)
        self.assertIsNotNone(resolved.members)

    def test_assembly_resolved_with_inheritance(self):
        # ART_3 inherits from ART_2
        resolved = assembly_resolved(self.releases_config, 'ART_3')
        self.assertEqual(resolved.group.advisories.image, 31)
        # members inherited from ART_2
        self.assertIsNotNone(resolved.members)

    def test_assembly_resolved_bang_override(self):
        # ART_5 uses ! to completely replace arches and advisories
        resolved = assembly_resolved(self.releases_config, 'ART_5')
        self.assertEqual(len(resolved.group.arches), 1)
        self.assertEqual(resolved.group.arches[0], 's390x')
        self.assertEqual(resolved.group.advisories.image, 51)
        self.assertEqual(resolved.group.advisories.extras, Missing)

    def test_assembly_resolved_excludes_basis(self):
        resolved = assembly_resolved(self.releases_config, 'ART_3')
        self.assertEqual(resolved.basis, Missing)

    def test_assembly_resolved_includes_rhcos(self):
        resolved = assembly_resolved(self.releases_config, 'ART_8')
        self.assertEqual(len(resolved.rhcos.dependencies.rpms), 3)

    def test_assembly_resolved_infinite_recursion(self):
        with self.assertRaises(ValueError):
            assembly_resolved(self.releases_config, 'ART_INFINITE')

    def test_top_level_merge_operator_rejected(self):
        for suffix in ('!', '?', '-'):
            releases_yml = f"""
releases:
  bad_assembly:
    assembly:
      group{suffix}:
        advisories:
          image: 1
"""
            releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
            with self.assertRaises(ValueError, msg=f'suffix "{suffix}" should be rejected'):
                assembly_resolved(releases_config, 'bad_assembly')

    def test_merger(self):
        # First value dominates on primitive
        self.assertEqual(_merger(4, 5), 4)
        self.assertEqual(_merger('4', '5'), '4')
        self.assertEqual(_merger(None, '5'), None)
        self.assertEqual(_merger(True, None), True)
        self.assertEqual(_merger([1, 2], [2, 3]), [1, 2, 3])

        # Dicts are additive
        self.assertEqual(
            _merger({'x': 5}, None),
            {'x': 5},
        )

        self.assertEqual(
            _merger({'x': 5}, {'y': 6}),
            {'x': 5, 'y': 6},
        )

        # Depth does not matter
        self.assertEqual(
            _merger({'r': {'x': 5}}, {'r': {'y': 6}}),
            {'r': {'x': 5, 'y': 6}},
        )

        self.assertEqual(
            _merger({'r': {'x': 5, 'y': 7}}, {'r': {'y': 6}}),
            {'r': {'x': 5, 'y': 7}},
        )

        # ? key provides default only
        self.assertEqual(
            _merger({'r': {'x': 5, 'y?': 7}}, {'r': {'y': 6}}),
            {'r': {'x': 5, 'y': 6}},
        )

        # ! key dominates completely
        self.assertEqual(
            _merger({'r!': {'x': 5}}, {'r': {'y': 6}}),
            {'r': {'x': 5}},
        )

        # Lists are combined, dupes eliminated, and results sorted
        self.assertEqual(
            _merger({'r': [1, 2]}, {'r': [1, 3, 4]}),
            {'r': [1, 2, 3, 4]},
        )

        # ! key dominates completely
        self.assertEqual(
            _merger({'r!': [1, 2]}, {'r': [3, 4]}),
            {'r': [1, 2]},
        )

        # - key removes itself entirely
        self.assertEqual(
            _merger({'r-': [1, 2]}, {'r': [3, 4]}),
            {},
        )


class TestAssemblyOwnIssuesConfig(TestCase):
    def setUp(self):
        releases_yml = """
releases:
  parent_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
      issues:
        include:
          - id: OCPBUGS-100
        exclude:
          - id: OCPBUGS-200
  child_assembly:
    assembly:
      basis:
        assembly: parent_assembly
      issues:
        include:
          - id: OCPBUGS-300
        exclude:
          - id: OCPBUGS-400
  no_issues_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
"""
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))

    def test_own_issues_not_inherited(self):
        """Child assembly must NOT inherit parent's issues.include or issues.exclude."""
        config = assembly_own_issues_config(self.releases_config, "child_assembly")
        include_ids = [i["id"] for i in config.include]
        exclude_ids = [i["id"] for i in config.exclude]
        self.assertIn("OCPBUGS-300", include_ids)
        self.assertNotIn("OCPBUGS-100", include_ids)
        self.assertIn("OCPBUGS-400", exclude_ids)
        self.assertNotIn("OCPBUGS-200", exclude_ids)

    def test_no_issues_key_returns_empty(self):
        """Assembly with no issues key returns a Model with empty include/exclude."""
        config = assembly_own_issues_config(self.releases_config, "no_issues_assembly")
        self.assertEqual(list(config.include), [])
        self.assertEqual(list(config.exclude), [])

    def test_none_assembly_returns_empty(self):
        config = assembly_own_issues_config(self.releases_config, None)
        self.assertEqual(config.primitive(), {})

    def test_none_releases_config_returns_empty(self):
        config = assembly_own_issues_config(None, "child_assembly")
        self.assertEqual(config.primitive(), {})


class TestAssemblyTargetedFixesOnly(TestCase):
    def setUp(self):
        releases_yml = """
releases:
  targeted_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
      issues:
        targeted_fixes_only: true
        include:
          - id: OCPBUGS-1
  normal_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
  child_assembly:
    assembly:
      basis:
        assembly: targeted_assembly
"""
        self.releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))

    def test_targeted_assembly_returns_true(self):
        self.assertTrue(assembly_targeted_fixes_only(self.releases_config, "targeted_assembly"))

    def test_normal_assembly_returns_false(self):
        self.assertFalse(assembly_targeted_fixes_only(self.releases_config, "normal_assembly"))

    def test_not_inherited_from_parent(self):
        # child_assembly inherits from targeted_assembly but does NOT have its own targeted_fixes_only flag
        self.assertFalse(assembly_targeted_fixes_only(self.releases_config, "child_assembly"))

    def test_none_assembly_returns_false(self):
        self.assertFalse(assembly_targeted_fixes_only(self.releases_config, None))

    def test_none_releases_config_returns_false(self):
        self.assertFalse(assembly_targeted_fixes_only(None, "targeted_assembly"))


class TestAssemblyPermits(TestCase):
    def test_rhcos_conflicting_inherited_dependency_forbidden(self):
        """CONFLICTING_INHERITED_DEPENDENCY permits for rhcos component should be rejected."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
      permits:
        - code: CONFLICTING_INHERITED_DEPENDENCY
          component: rhcos
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        group_config = Model(dict_to_model={'software_lifecycle': {'phase': 'release'}})

        with self.assertRaises(ValueError) as cm:
            assembly_permits(releases_config, group_config, "test_assembly")

        self.assertIn("CONFLICTING_INHERITED_DEPENDENCY cannot be permitted for rhcos", str(cm.exception))
        self.assertIn("RPM advisories to claim newer versions", str(cm.exception))

    def test_rhcos_conflicting_inherited_dependency_wildcard_component_allowed(self):
        """CONFLICTING_INHERITED_DEPENDENCY permits for wildcard component should be allowed."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
      permits:
        - code: CONFLICTING_INHERITED_DEPENDENCY
          component: '*'
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        group_config = Model(dict_to_model={'software_lifecycle': {'phase': 'release'}})

        # Should not raise
        permits = assembly_permits(releases_config, group_config, "test_assembly")
        self.assertEqual(len(permits), 1)

    def test_rhcos_conflicting_inherited_dependency_other_component_allowed(self):
        """CONFLICTING_INHERITED_DEPENDENCY permits for non-rhcos components should be allowed."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
      permits:
        - code: CONFLICTING_INHERITED_DEPENDENCY
          component: some-image
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        group_config = Model(dict_to_model={'software_lifecycle': {'phase': 'release'}})

        # Should not raise
        permits = assembly_permits(releases_config, group_config, "test_assembly")
        self.assertEqual(len(permits), 1)

    def test_rhcos_other_issue_codes_allowed(self):
        """Other issue codes for rhcos component should be allowed."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      basis:
        time: "2026-01-01T00:00:00+00:00"
      permits:
        - code: CONFLICTING_GROUP_RPM_INSTALLED
          component: rhcos
        - code: INCONSISTENT_RHCOS_RPMS
          component: rhcos
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        group_config = Model(dict_to_model={'software_lifecycle': {'phase': 'release'}})

        # Should not raise
        permits = assembly_permits(releases_config, group_config, "test_assembly")
        self.assertEqual(len(permits), 2)


class TestAssemblyValidateMemberDistgitKeys(TestCase):
    def test_valid_keys_no_error(self):
        """Valid distgit_keys should pass validation without error."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        images:
        - distgit_key: openshift-kuryr
          metadata:
            is: some-nvr
        - distgit_key: openshift-apiserver
          metadata:
            is: another-nvr
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        known_keys = {'openshift-kuryr', 'openshift-apiserver', 'openshift-controller-manager'}
        # Should not raise
        assembly_validate_member_distgit_keys(releases_config, 'test_assembly', 'image', known_keys)

    def test_invalid_key_raises_error(self):
        """An invalid distgit_key should raise ValueError with the bad key name."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        images:
        - distgit_key: ose-kubernetes-nmstate-operator
          metadata:
            is: some-nvr
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        known_keys = {'openshift-kuryr', 'openshift-apiserver'}
        with self.assertRaises(ValueError) as cm:
            assembly_validate_member_distgit_keys(releases_config, 'test_assembly', 'image', known_keys)
        self.assertIn('ose-kubernetes-nmstate-operator', str(cm.exception))
        self.assertIn("does not match any known image definition", str(cm.exception))

    def test_fuzzy_suggestion_included(self):
        """When a close match exists, the error message should include a suggestion."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        images:
        - distgit_key: ose-kubernetes-nmstate-operator
          metadata:
            is: some-nvr
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        known_keys = {'openshift-kubernetes-nmstate-operator', 'openshift-kuryr'}
        with self.assertRaises(ValueError) as cm:
            assembly_validate_member_distgit_keys(releases_config, 'test_assembly', 'image', known_keys)
        self.assertIn("Did you mean", str(cm.exception))
        self.assertIn("openshift-kubernetes-nmstate-operator", str(cm.exception))

    def test_wildcard_key_skipped(self):
        """The wildcard '*' distgit_key should be skipped during validation."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        rpms:
        - distgit_key: '*'
          metadata:
            content:
              source:
                git:
                  branch:
                    target: custom_branch
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        known_keys = {'openshift-kuryr'}
        # Should not raise even though '*' is not in known_keys
        assembly_validate_member_distgit_keys(releases_config, 'test_assembly', 'rpm', known_keys)

    def test_no_assembly_no_error(self):
        """None or empty assembly should return without error."""
        releases_config = Model(dict_to_model={'releases': {}})
        assembly_validate_member_distgit_keys(releases_config, None, 'image', set())
        assembly_validate_member_distgit_keys(releases_config, '', 'image', set())

    def test_no_members_section_no_error(self):
        """An assembly without a members section should not raise."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      group:
        advisories:
          image: 1
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        assembly_validate_member_distgit_keys(releases_config, 'test_assembly', 'image', {'some-image'})

    def test_inheritance_validates_ancestor(self):
        """Validation should check distgit_keys in ancestor assemblies too."""
        releases_yml = """
releases:
  parent:
    assembly:
      members:
        images:
        - distgit_key: nonexistent-image
          metadata:
            is: some-nvr
  child:
    assembly:
      basis:
        assembly: parent
      members:
        images:
        - distgit_key: openshift-kuryr
          metadata:
            is: another-nvr
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        known_keys = {'openshift-kuryr'}
        with self.assertRaises(ValueError) as cm:
            assembly_validate_member_distgit_keys(releases_config, 'child', 'image', known_keys)
        self.assertIn('nonexistent-image', str(cm.exception))
        # The error should reference the parent assembly where the bad key lives
        self.assertIn("'parent'", str(cm.exception))

    def test_rpm_validation(self):
        """Validation should work for RPM members too."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        rpms:
        - distgit_key: nonexistent-rpm
          metadata:
            content: {}
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        known_keys = {'openshift-kuryr', 'openshift-clients'}
        with self.assertRaises(ValueError) as cm:
            assembly_validate_member_distgit_keys(releases_config, 'test_assembly', 'rpm', known_keys)
        self.assertIn('nonexistent-rpm', str(cm.exception))
        self.assertIn("does not match any known rpm definition", str(cm.exception))

    def test_exclude_only_entry_validated(self):
        """Even exclude-only entries should have their distgit_key validated."""
        releases_yml = """
releases:
  test_assembly:
    assembly:
      members:
        images:
        - distgit_key: nonexistent-image
          exclude: true
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        known_keys = {'openshift-kuryr'}
        with self.assertRaises(ValueError) as cm:
            assembly_validate_member_distgit_keys(releases_config, 'test_assembly', 'image', known_keys)
        self.assertIn('nonexistent-image', str(cm.exception))

    def test_infinite_recursion_detected(self):
        """Infinite recursion in assembly basis chain should be caught."""
        releases_yml = """
releases:
  loop_assembly:
    assembly:
      basis:
        assembly: loop_assembly
      members:
        images:
        - distgit_key: some-image
          metadata:
            is: some-nvr
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        with self.assertRaises(ValueError):
            assembly_validate_member_distgit_keys(releases_config, 'loop_assembly', 'image', {'some-image'})


class TestAssemblyOverridesExpiry(TestCase):
    """Tests for the 'until' expiry field on assembly overrides."""

    def _group_config(self):
        return Model(dict_to_model={'software_lifecycle': {'phase': 'release'}})

    def test_permit_until_not_expired(self):
        """Permit with future 'until' date should not raise."""
        future = (date.today() + timedelta(days=30)).isoformat()
        releases_yml = f"""
releases:
  test:
    assembly:
      permits:
        - code: MISMATCHED_SIBLINGS
          component: '*'
          until: "{future}"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        permits = assembly_permits(releases_config, self._group_config(), "test")
        self.assertEqual(len(permits), 1)

    def test_permit_until_expired(self):
        """Permit with past 'until' date should raise ValueError."""
        past = (date.today() - timedelta(days=5)).isoformat()
        releases_yml = f"""
releases:
  test:
    assembly:
      permits:
        - code: MISMATCHED_SIBLINGS
          component: '*'
          until: "{past}"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        with self.assertRaises(ValueError) as cm:
            assembly_permits(releases_config, self._group_config(), "test")
        self.assertIn("expired", str(cm.exception))
        self.assertIn("MISMATCHED_SIBLINGS", str(cm.exception))
        self.assertIn("5 day(s) overdue", str(cm.exception))

    def test_permit_until_invalid_format(self):
        """Permit with non-date 'until' should raise ValueError."""
        releases_yml = """
releases:
  test:
    assembly:
      permits:
        - code: MISMATCHED_SIBLINGS
          component: '*'
          until: "not-a-date"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        with self.assertRaises(ValueError) as cm:
            assembly_permits(releases_config, self._group_config(), "test")
        self.assertIn("Invalid 'until' date format", str(cm.exception))

    def test_permit_without_until_still_works(self):
        """Permits without 'until' should work as before."""
        releases_yml = """
releases:
  test:
    assembly:
      permits:
        - code: MISMATCHED_SIBLINGS
          component: '*'
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        permits = assembly_permits(releases_config, self._group_config(), "test")
        self.assertEqual(len(permits), 1)

    def test_member_image_until_expired(self):
        """Member image override with past 'until' should be reported."""
        past = (date.today() - timedelta(days=10)).isoformat()
        releases_yml = f"""
releases:
  test:
    assembly:
      members:
        images:
          - distgit_key: ose-network-operator
            why: "Pin for testing"
            until: "{past}"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        errors = check_assembly_overrides_expiry(releases_config, "test")
        self.assertEqual(len(errors), 1)
        self.assertIn("ose-network-operator", errors[0])
        self.assertIn("expired", errors[0])

    def test_member_rpm_until_expired(self):
        """Member RPM override with past 'until' should be reported."""
        past = (date.today() - timedelta(days=3)).isoformat()
        releases_yml = f"""
releases:
  test:
    assembly:
      members:
        rpms:
          - distgit_key: my-rpm
            why: "Pin NVR"
            until: "{past}"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        errors = check_assembly_overrides_expiry(releases_config, "test")
        self.assertEqual(len(errors), 1)
        self.assertIn("my-rpm", errors[0])

    def test_member_until_not_expired(self):
        """Member override with future 'until' should not be reported."""
        future = (date.today() + timedelta(days=30)).isoformat()
        releases_yml = f"""
releases:
  test:
    assembly:
      members:
        images:
          - distgit_key: ose-network-operator
            why: "Pin for testing"
            until: "{future}"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        errors = check_assembly_overrides_expiry(releases_config, "test")
        self.assertEqual(len(errors), 0)

    def test_no_until_no_error(self):
        """Overrides without 'until' should produce no errors."""
        releases_yml = """
releases:
  test:
    assembly:
      members:
        images:
          - distgit_key: ose-network-operator
            why: "Pin for testing"
      permits:
        - code: MISMATCHED_SIBLINGS
          component: '*'
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        errors = check_assembly_overrides_expiry(releases_config, "test")
        self.assertEqual(len(errors), 0)
        permits = assembly_permits(releases_config, self._group_config(), "test")
        self.assertEqual(len(permits), 1)

    def test_inherited_member_until_expired(self):
        """Expired 'until' on a member override in a parent assembly should be caught."""
        past = (date.today() - timedelta(days=7)).isoformat()
        releases_yml = f"""
releases:
  parent:
    assembly:
      members:
        images:
          - distgit_key: inherited-image
            why: "Temporary pin"
            until: "{past}"
  child:
    assembly:
      basis:
        assembly: parent
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        errors = check_assembly_overrides_expiry(releases_config, "child")
        self.assertEqual(len(errors), 1)
        self.assertIn("inherited-image", errors[0])

    def test_none_assembly_returns_empty(self):
        """None assembly should return empty list."""
        releases_config = Model(dict_to_model={})
        errors = check_assembly_overrides_expiry(releases_config, None)
        self.assertEqual(len(errors), 0)

    def test_multiple_expired_overrides(self):
        """Multiple expired overrides should all be reported."""
        past = (date.today() - timedelta(days=1)).isoformat()
        releases_yml = f"""
releases:
  test:
    assembly:
      members:
        images:
          - distgit_key: image-a
            why: "Pin A"
            until: "{past}"
          - distgit_key: image-b
            why: "Pin B"
            until: "{past}"
        rpms:
          - distgit_key: rpm-c
            why: "Pin C"
            until: "{past}"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        errors = check_assembly_overrides_expiry(releases_config, "test")
        self.assertEqual(len(errors), 3)

    def test_permit_until_today_not_expired(self):
        """Permit with 'until' set to today should not be expired (expires after today)."""
        today = date.today().isoformat()
        releases_yml = f"""
releases:
  test:
    assembly:
      permits:
        - code: MISMATCHED_SIBLINGS
          component: '*'
          until: "{today}"
"""
        releases_config = Model(dict_to_model=yaml.safe_load(releases_yml))
        permits = assembly_permits(releases_config, self._group_config(), "test")
        self.assertEqual(len(permits), 1)
