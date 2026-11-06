from collections import defaultdict
from unittest import TestCase
from unittest.mock import MagicMock, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.model import Model
from doozerlib.cli.release_gen_assembly import GenAssemblyCli
from flexmock import flexmock


class TestGenAssemblyCli(TestCase):
    def test_initialize_assembly_type(self):
        """
        Check that the correct assembly type is set, according to
        the gen-assembly name or `custom` flag
        """

        runtime = MagicMock()

        # If `custom` flag is set to true, assembly type should be CUSTOM
        gacli = GenAssemblyCli(runtime=runtime, custom=True)
        self.assertEqual(gacli.assembly_type, AssemblyTypes.CUSTOM)

        # For RC gen-assembly names, assembly type should be CANDIDATE
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='rc.0')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.CANDIDATE)

        # For FC gen-assembly names, assembly type should be CANDIDATE
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='fc.0')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.CANDIDATE)

        # For EC gen-assembly names, assembly type should be PREVIEW
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='ec.0')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.PREVIEW)

        # For any other non-standardard gen-assembly names, assembly type should be STANDARD
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='just-a-name')
        self.assertEqual(gacli.assembly_type, AssemblyTypes.STANDARD)

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_non_stream_assembly_name(self):
        """
        Check that assembly must be "stream" in order to populate an assembly definition from nightlies.
        The command should raise an error otherwise
        """

        gacli = flexmock(
            GenAssemblyCli(
                runtime=MagicMock(assembly='custom', arches=['amd64']),
                nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_no_nightlies_nor_standards(self):
        """
        At least one release (--nightly or --standard) must be specified.
        The command should raise an error otherwise
        """

        gacli = flexmock(
            GenAssemblyCli(
                runtime=MagicMock(assembly='stream'),
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_previous_and_auto_previous(self):
        """
        Only one among `--previous` and `--auto-previous` can be used.
        The command should raise an error otherwise
        """

        gacli = flexmock(
            GenAssemblyCli(
                runtime=MagicMock(assembly='stream', arches=['amd64']),
                nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
                auto_previous=True,
                previous_list='4.y.z',
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_custom_assembly(self):
        """
        Custom releases don't have previous list. The command should raise an error if `custom` is true,
        but one among `auto_previous`, `previous_list` or `in_flight` is specified
        """

        # Custom and auto_previous
        gacli = flexmock(
            GenAssemblyCli(
                runtime=MagicMock(assembly='stream', arches=['amd64']),
                nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
                custom=True,
                auto_previous=True,
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

        # Custom and previous_list
        gacli = flexmock(
            GenAssemblyCli(
                runtime=MagicMock(assembly='stream', arches=['amd64']),
                nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
                custom=True,
                previous_list='4.y.z',
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

        # Custom and in_flight
        gacli = flexmock(
            GenAssemblyCli(
                runtime=MagicMock(assembly='stream', arches=['amd64']),
                nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
                custom=True,
                in_flight=True,
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._validate_params()

    def test_nightly_release_pullspecs(self):
        """
        Check that for nightlies matching group param, two maps are populated:
        One that associates the group arch with the complete nightlies pullspecs,
        and one that associates the group arch with the nightlies names
        """

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.13'
        runtime.get_major_minor_fields.return_value = (4, 13)

        gacli = GenAssemblyCli(
            runtime=runtime,
            nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
        )
        gacli._get_release_pullspecs()
        self.assertEqual(
            gacli.release_pullspecs,
            {'x86_64': 'registry.ci.openshift.org/ocp/release:4.13.0-0.nightly-2022-12-01-153811'},
        )
        self.assertEqual(
            gacli.reference_releases_by_arch,
            {'x86_64': '4.13.0-0.nightly-2022-12-01-153811'},
        )

        gacli = GenAssemblyCli(
            runtime=runtime,
            nightlies=['4.13.0-0.nightly-2022-12-01-153811', '4.13.0-0.nightly-arm64-2022-12-05-151453'],
        )
        gacli._get_release_pullspecs()
        self.assertEqual(
            gacli.release_pullspecs,
            {
                'x86_64': 'registry.ci.openshift.org/ocp/release:4.13.0-0.nightly-2022-12-01-153811',
                'aarch64': 'registry.ci.openshift.org/ocp-arm64/release-arm64:4.13.0-0.nightly-arm64-2022-12-05-151453',
            },
        )
        self.assertEqual(
            gacli.reference_releases_by_arch,
            {
                'x86_64': '4.13.0-0.nightly-2022-12-01-153811',
                'aarch64': '4.13.0-0.nightly-arm64-2022-12-05-151453',
            },
        )

    def test_nightly_release_pullspecs_ocp5(self):
        runtime = MagicMock()
        runtime.get_minor_version.return_value = '5.0'
        runtime.get_major_minor_fields.return_value = (5, 0)

        gacli = GenAssemblyCli(
            runtime=runtime,
            nightlies=['5.0.0-0.nightly-2022-12-01-153811'],
        )
        gacli._get_release_pullspecs()
        self.assertEqual(
            gacli.release_pullspecs,
            {
                'x86_64': 'registry.ci.openshift.org/ocp/release-5:5.0.0-0.nightly-2022-12-01-153811',
            },
        )

    def test_multi_nighly_arch(self):
        """
        Only one nightly per group arch should be specified.
        The command should raise an error otherwise
        """

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.13'

        # 2 nightlies for x86_64
        gacli = GenAssemblyCli(
            runtime=runtime,
            nightlies=[
                '4.13.0-0.nightly-2022-12-01-153811',
                '4.13.0-0.nightly-2022-12-01-140621',
            ],
        )
        with self.assertRaises(ValueError):
            gacli._get_release_pullspecs()

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_group_standard_mismatch(self):
        """
        Specified standard releases must match the group `major.minor` parameter.
        The command should raise an error otherwise
        """

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.12'
        gacli = flexmock(
            GenAssemblyCli(
                runtime=runtime,
                standards=['4.11.18-x86_64'],
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._get_release_pullspecs()

    def test_standard_release_pullspecs(self):
        """
        Check that for standard releases matching group param, only one map is populated:
        the one that associates the group arch with the releases pullspecs.
        The one that associates the group arch with the nightlies names should be left empty
        """

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.11'
        gacli = GenAssemblyCli(
            runtime=runtime,
            standards=['4.11.18-x86_64'],
        )
        gacli._get_release_pullspecs()
        self.assertEqual(
            gacli.release_pullspecs,
            {'x86_64': 'quay.io/openshift-release-dev/ocp-release:4.11.18-x86_64'},
        )
        self.assertEqual(
            gacli.reference_releases_by_arch,
            {},
        )

    def test_multi_standard_arch(self):
        """
        Only one standard release per group arch should be specified.
        The command should raise an error otherwise
        """

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.11'
        gacli = GenAssemblyCli(
            runtime=runtime,
            standards=[
                '4.11.18-x86_64',
                '4.11.19-x86_64',
            ],
        )
        with self.assertRaises(ValueError):
            gacli._get_release_pullspecs()

    def test_get_advisories_release_jira_default(self):
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 11)
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.11.2')
        advisories, release_jira = gacli._get_advisories_release_jira()
        self.assertEqual(
            advisories,
            {
                'image': -1,
                'rpm': -1,
                'extras': -1,
                'metadata': -1,
            },
        )
        self.assertEqual(release_jira, "ART-0")

        runtime.get_major_minor_fields.return_value = (4, 14)
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.14.3')
        advisories, release_jira = gacli._get_advisories_release_jira()
        self.assertEqual(
            advisories,
            {
                'image': -1,
                'rpm': -1,
                'extras': -1,
                'metadata': -1,
            },
        )

        runtime.get_major_minor_fields.return_value = (3, 11)
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='3.11.2')
        advisories, release_jira = gacli._get_advisories_release_jira()
        self.assertEqual(
            advisories,
            {
                'image': -1,
                'rpm': -1,
                'extras': -1,
                'metadata': -1,
            },
        )

        runtime.get_major_minor_fields.return_value = (5, 1)
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='5.1.12')
        advisories, release_jira = gacli._get_advisories_release_jira()
        self.assertEqual(
            advisories,
            {
                'image': -1,
                'rpm': -1,
                'extras': -1,
                'metadata': -1,
            },
        )

        runtime.build_system = 'konflux'
        runtime.get_major_minor_fields.return_value = (4, 11)
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.11.2')
        advisories, release_jira = gacli._get_advisories_release_jira()
        self.assertEqual(
            advisories,
            {
                'rpm': -1,
                'rhcos': -1,
            },
        )
        self.assertEqual(release_jira, "ART-0")

    def test_get_advisories_release_jira_candidate_reuse(self):
        runtime = MagicMock(build_system='brew')
        advisories = {'image': 123, 'rpm': 456, 'extras': 789, 'metadata': 654}
        release_jira = "ART-123"
        runtime.get_releases_config.return_value = Model(
            {
                'releases': {
                    'rc.0': {
                        'assembly': {
                            'group': {
                                'advisories': advisories,
                                'release_jira': release_jira,
                            }
                        }
                    }
                }
            }
        )
        runtime.get_major_minor_fields.return_value = (4, 12)
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='rc.1')
        actual = gacli._get_advisories_release_jira()
        self.assertEqual(advisories, actual[0])
        self.assertEqual(release_jira, actual[1])

    def test_get_shipment_info(self):
        runtime = MagicMock(build_system='konflux')
        runtime.get_releases_config.return_value = Model({'releases': {}})
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='some-assembly')
        shipment = gacli._get_shipment_info()
        expected = {
            'advisories': [
                {'kind': 'image'},
                {'kind': 'extras'},
                {'kind': 'metadata'},
                {'kind': 'fbc'},
            ],
        }
        self.assertEqual(expected, shipment)

    def test_get_shipment_info_ec0(self):
        runtime = MagicMock(build_system='konflux')
        runtime.get_releases_config.return_value = Model({'releases': {}})
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='ec.0')
        shipment = gacli._get_shipment_info()
        expected = {
            'advisories': [
                {'kind': 'image'},
                {'kind': 'extras'},
                {'kind': 'metadata'},
                {'kind': 'fbc'},
            ],
            'env': 'stage',
        }
        self.assertEqual(expected, shipment)

    def test_get_shipment_info_ec1_from_ec0(self):
        runtime = MagicMock(build_system='konflux')
        shipment_info = {
            'advisories': [
                {'kind': 'image', 'live_id': 123},
                {'kind': 'extras', 'live_id': 456},
                {'kind': 'metadata', 'live_id': 789},
                {'kind': 'fbc'},
            ]
        }
        runtime.get_releases_config.return_value = Model(
            {'releases': {'ec.0': {'assembly': {'group': {'shipment': shipment_info}}}}}
        )
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='ec.1')
        self.assertEqual(shipment_info, gacli._get_shipment_info())

    def test_get_shipment_info_rc0_from_ec4(self):
        runtime = MagicMock(build_system='konflux')
        shipment_info = {
            'advisories': [
                {'kind': 'image', 'live_id': 123},
                {'kind': 'extras', 'live_id': 456},
                {'kind': 'metadata', 'live_id': 789},
                {'kind': 'fbc'},
            ]
        }
        runtime.get_releases_config.return_value = Model(
            {
                'releases': {
                    'ec.2': {'assembly': {'group': {'shipment': {}}}},
                    'ec.4': {'assembly': {'group': {'shipment': shipment_info}}},
                    'ec.1': {'assembly': {'group': {'shipment': {}}}},
                }
            }
        )
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='rc.0')
        self.assertEqual(shipment_info, gacli._get_shipment_info())

    def test_get_shipment_info_rc1_from_rc0(self):
        runtime = MagicMock(build_system='konflux')
        shipment_info = {
            'advisories': [
                {'kind': 'image', 'live_id': 123},
                {'kind': 'extras', 'live_id': 456},
                {'kind': 'metadata', 'live_id': 789},
                {'kind': 'fbc'},
            ]
        }
        runtime.get_releases_config.return_value = Model(
            {'releases': {'rc.0': {'assembly': {'group': {'shipment': shipment_info}}}}}
        )
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='rc.1')
        self.assertEqual(shipment_info, gacli._get_shipment_info())

    def test_get_shipment_info_rc1_no_rc0(self):
        runtime = MagicMock(build_system='konflux')
        runtime.get_releases_config.return_value = Model({'releases': {}})
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='rc.1')
        gacli.logger = MagicMock()
        expected = {
            'advisories': [
                {'kind': 'image'},
                {'kind': 'extras'},
                {'kind': 'metadata'},
                {'kind': 'fbc'},
            ],
            'env': 'stage',
        }
        self.assertEqual(expected, gacli._get_shipment_info())
        gacli.logger.warning.assert_called_once_with("No matching previous assembly found")


class TestGetRhcosContainerFallback(TestCase):
    """Tests for _get_rhcos_container manifest-list-tag label fallback logic."""

    def _make_gacli(self, arches=None, layered=True, rhcos_version='51.20261016.0'):
        """Create a GenAssemblyCli with mocked runtime for _get_rhcos_container tests."""
        if arches is None:
            arches = ['x86_64', 'aarch64']

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '5.1'
        runtime.group_config.arches = arches
        runtime.group_config.vars.RHCOS_EL_MAJOR = 9
        runtime.group_config.vars.RHCOS_EL_MINOR = 8
        runtime.group_config.rhcos.get.return_value = layered
        runtime.registry_config = '/fake/registry-config.json'

        gacli = GenAssemblyCli(runtime=runtime)
        gacli.rhcos_version = rhcos_version
        gacli.logger = MagicMock()

        # primary_rhcos_tag is set by _get_rhcos_container via rhcos.get_primary_container_name
        # We mock the functions that _get_rhcos_container calls
        return gacli

    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_primary_container_name', return_value='rhel-coreos')
    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_container_configs')
    @patch('doozerlib.cli.release_gen_assembly.util.oc_image_info_for_arch')
    def test_manifest_list_repo_from_primary_payload_tag(self, mock_oc_info, mock_get_configs, mock_primary):
        configured_repo = 'registry.example.com:5000/rhcos'
        default_repo = 'quay.io/openshift-release-dev/ocp-v4.0-art-dev'
        configurations = [
            ({}, default_repo),
            ({'payload_tags': None}, default_repo),
            ({'payload_tags': []}, default_repo),
            ({'payload_tags': [{'rhel_build_id_index': 'quay.io/other:latest'}]}, default_repo),
            ({'payload_tags': [{'primary': True}]}, default_repo),
            ({'payload_tags': [{'primary': True, 'rhel_build_id_index': None}]}, default_repo),
            ({'payload_tags': [{'primary': True, 'rhel_build_id_index': 123}]}, default_repo),
            ({'payload_tags': [{'primary': True, 'rhel_build_id_index': 'untagged'}]}, default_repo),
            ({'payload_tags': [{'primary': True, 'rhel_build_id_index': f'{configured_repo}:'}]}, default_repo),
            ({'payload_tags': [{'primary': True, 'rhel_build_id_index': configured_repo}]}, default_repo),
            (
                {
                    'payload_tags': [
                        {'primary': False, 'rhel_build_id_index': 'quay.io/other:latest'},
                        {'primary': True, 'rhel_build_id_index': f'{configured_repo}:5.1-9.8'},
                    ]
                },
                configured_repo,
            ),
        ]
        mock_get_configs.return_value = [Model({'name': 'rhel-coreos'})]
        for rhcos_config, expected_repo in configurations:
            with self.subTest(rhcos_config=rhcos_config):
                gacli = self._make_gacli()
                gacli.runtime.group_config.rhcos = Model({'layered_rhcos': True, **rhcos_config})
                gacli.rhcos_by_tag = defaultdict(dict, {'rhel-coreos': {'x86_64': 'quay.io/source@sha256:amd64'}})
                mock_oc_info.reset_mock()
                mock_oc_info.side_effect = [
                    {'config': {'config': {'Labels': {'coreos.build.manifest-list-tag': 'manifest-tag'}}}},
                    {'digest': 'sha256:arm64'},
                ]

                gacli._get_rhcos_container()

                self.assertEqual(mock_oc_info.call_args_list[1].args[0], f'{expected_repo}:manifest-tag')
                self.assertEqual(gacli.rhcos_by_tag['rhel-coreos']['aarch64'], f'{expected_repo}@sha256:arm64')

    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_primary_container_name', return_value='rhel-coreos')
    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_container_configs')
    @patch('doozerlib.cli.release_gen_assembly.util.oc_image_info_for_arch')
    def test_manifest_list_lookup_matches_variant(self, mock_oc_info, mock_get_configs, mock_primary):
        configurations = [
            {
                'name': 'rhel-coreos',
                'primary': True,
                'rhel_build_id_index': 'registry.example.com:5000/rhel9:5.1-9.8',
                'rhcos_index_tag': 'quay.io/art-images/rhel9:5.1-9.8-node-image',
            },
            {
                'name': 'rhel-coreos-10',
                'rhel_build_id_index': 'registry.example.com:5000/rhel10:5.1-10.2',
                'rhcos_index_tag': 'quay.io/art-images/rhel10:5.1-10.2-node-image',
            },
        ]
        for reverse_order in (False, True):
            for has_manifest_label in (False, True):
                with self.subTest(reverse_order=reverse_order, has_manifest_label=has_manifest_label):
                    gacli = self._make_gacli()
                    payload_tags = configurations[::-1] if reverse_order else configurations
                    gacli.runtime.group_config.rhcos = Model({'layered_rhcos': True, 'payload_tags': payload_tags})
                    mock_get_configs.return_value = gacli.runtime.group_config.rhcos.payload_tags
                    gacli.rhcos_by_tag = defaultdict(
                        dict,
                        {
                            config['name']: {'x86_64': f'quay.io/source@sha256:{config["name"]}'}
                            for config in payload_tags
                        },
                    )
                    mock_oc_info.reset_mock()
                    image_infos = []
                    expected_pullspecs = []
                    expected_digests = {}
                    for config in payload_tags:
                        rhel_major = 9 if config.get('primary') else 10
                        manifest_tag = (
                            config['rhcos_index_tag']
                            .rsplit(':', 1)[1]
                            .replace('-node-image', '-202610160945-node-image')
                        )
                        labels = {'release': f'202610160945.p2.g62965e8.assembly.stream.el{rhel_major}'}
                        if has_manifest_label:
                            labels['coreos.build.manifest-list-tag'] = f'label-{manifest_tag}'
                            expected_repo = config['rhel_build_id_index'].rsplit(':', 1)[0]
                        else:
                            expected_repo = config['rhcos_index_tag'].rsplit(':', 1)[0]
                        expected_tag = labels.get('coreos.build.manifest-list-tag', manifest_tag)
                        expected_pullspecs.append(f'{expected_repo}:{expected_tag}')
                        digest = f'sha256:arm64-el{rhel_major}'
                        expected_digests[config['name']] = f'{expected_repo}@{digest}'
                        image_infos.extend([{'config': {'config': {'Labels': labels}}}, {'digest': digest}])
                    mock_oc_info.side_effect = image_infos

                    gacli._get_rhcos_container()

                    self.assertEqual(mock_oc_info.call_count, 4)
                    self.assertEqual(
                        [image_call.args[0] for image_call in mock_oc_info.call_args_list[1::2]], expected_pullspecs
                    )
                    for name, pullspec in expected_digests.items():
                        self.assertEqual(gacli.rhcos_by_tag[name]['aarch64'], pullspec)

    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_primary_container_name', return_value='rhel-coreos')
    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_container_configs')
    @patch('doozerlib.cli.release_gen_assembly.util.oc_image_info_for_arch')
    def test_unmatched_variant_uses_primary_prefix(self, mock_oc_info, mock_get_configs, mock_primary):
        gacli = self._make_gacli()
        gacli.runtime.group_config.rhcos = Model(
            {
                'layered_rhcos': True,
                'payload_tags': [
                    {
                        'name': 'other-name',
                        'primary': True,
                        'rhcos_index_tag': 'quay.io/art-images:5.1-10.2-node-image',
                    }
                ],
            }
        )
        mock_get_configs.return_value = [Model({'name': 'rhel-coreos'})]
        gacli.rhcos_by_tag = defaultdict(dict, {'rhel-coreos': {'x86_64': 'quay.io/source@sha256:amd64'}})
        mock_oc_info.side_effect = [
            {'config': {'config': {'Labels': {'release': '202610160945.p2.g62965e8.assembly.stream.el10'}}}},
            {'digest': 'sha256:arm64'},
        ]

        gacli._get_rhcos_container()

        self.assertEqual(mock_oc_info.call_args_list[1].args[0], 'quay.io/art-images:5.1-10.2-202610160945-node-image')
        self.assertEqual(gacli.rhcos_by_tag['rhel-coreos']['aarch64'], 'quay.io/art-images@sha256:arm64')

    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_primary_container_name', return_value='rhel-coreos')
    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_container_configs')
    @patch('doozerlib.cli.release_gen_assembly.util.oc_image_info_for_arch')
    @patch('doozerlib.cli.release_gen_assembly.get_art_prod_image_repo_for_version', return_value='quay.io/art-dev')
    def test_uses_existing_manifest_list_tag_label(self, mock_art_repo, mock_oc_info, mock_get_configs, mock_primary):
        """When coreos.build.manifest-list-tag label is present, use it directly (4.x/5.0 path)."""
        gacli = self._make_gacli()

        # x86_64 already found, need to resolve aarch64
        tag_config = MagicMock()
        tag_config.name = 'rhel-coreos'
        mock_get_configs.return_value = [tag_config]

        gacli.rhcos_by_tag = defaultdict(
            dict,
            {
                'rhel-coreos': {'x86_64': 'quay.io/art-dev@sha256:' + 'a' * 64},
            },
        )

        # oc_image_info_for_arch returns different results for different calls:
        # First call: get amd64 image info (with the existing label)
        # Second call: resolve the manifest list for aarch64
        mock_oc_info.side_effect = [
            {
                'config': {
                    'config': {
                        'Labels': {
                            'coreos.build.manifest-list-tag': '5.1-9.8-202610160945-node-image',
                        }
                    }
                }
            },
            {'digest': 'sha256:' + 'b' * 64},
        ]

        gacli._get_rhcos_container()

        # Should use the label value directly
        second_call = mock_oc_info.call_args_list[1]
        self.assertEqual(second_call.args[0], 'quay.io/art-dev:5.1-9.8-202610160945-node-image')

        # aarch64 should be resolved
        self.assertEqual(
            gacli.rhcos_by_tag['rhel-coreos']['aarch64'],
            'quay.io/art-dev@sha256:' + 'b' * 64,
        )

    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_primary_container_name', return_value='rhel-coreos')
    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_container_configs')
    @patch('doozerlib.cli.release_gen_assembly.util.oc_image_info_for_arch')
    @patch('doozerlib.cli.release_gen_assembly.get_art_prod_image_repo_for_version', return_value='quay.io/art-dev')
    def test_falls_back_to_release_label(self, mock_art_repo, mock_oc_info, mock_get_configs, mock_primary):
        """When coreos.build.manifest-list-tag is absent, derive tag from release label (5.1+ path).
        The timestamped tag only exists in the art-images repo (from rhcos_index_tag config),
        not in ocp-v4.0-art-dev."""
        art_images_repo = 'quay.io/redhat-user-workloads/ocp-art-tenant/art-images'
        gacli = self._make_gacli()

        tag_config = MagicMock()
        tag_config.name = 'rhel-coreos'
        tag_config.rhcos_index_tag = f'{art_images_repo}:5.1-9.8-node-image'
        mock_get_configs.return_value = [tag_config]

        gacli.rhcos_by_tag = defaultdict(
            dict,
            {
                'rhel-coreos': {'x86_64': 'quay.io/art-dev@sha256:' + 'a' * 64},
            },
        )

        # First call: get amd64 image info - NO manifest-list-tag, but has release label
        # Second call: resolve the derived tag for aarch64
        mock_oc_info.side_effect = [
            {
                'config': {
                    'config': {
                        'Labels': {
                            'release': '202610160945.p2.g62965e8.assembly.stream.el9',
                            # No 'coreos.build.manifest-list-tag'
                        }
                    }
                }
            },
            {'digest': 'sha256:' + 'c' * 64},
        ]

        gacli._get_rhcos_container()

        # Should derive the tag from the release label timestamp and use art-images repo
        second_call = mock_oc_info.call_args_list[1]
        self.assertEqual(second_call.args[0], f'{art_images_repo}:5.1-9.8-202610160945-node-image')

        # aarch64 should be resolved using art-images repo
        self.assertEqual(
            gacli.rhcos_by_tag['rhel-coreos']['aarch64'],
            f'{art_images_repo}@sha256:' + 'c' * 64,
        )

        # Should log the fallback
        gacli.logger.info.assert_any_call(
            'Derived manifest-list tag %s from release label %s (coreos.build.manifest-list-tag label absent)',
            '5.1-9.8-202610160945-node-image',
            '202610160945.p2.g62965e8.assembly.stream.el9',
        )

    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_primary_container_name', return_value='rhel-coreos')
    @patch('doozerlib.cli.release_gen_assembly.rhcos.get_container_configs')
    @patch('doozerlib.cli.release_gen_assembly.util.oc_image_info_for_arch')
    @patch('doozerlib.cli.release_gen_assembly.get_art_prod_image_repo_for_version', return_value='quay.io/art-dev')
    def test_errors_when_both_labels_missing(self, mock_art_repo, mock_oc_info, mock_get_configs, mock_primary):
        """When both coreos.build.manifest-list-tag and release labels are missing, exit with error."""
        gacli = self._make_gacli()

        tag_config = MagicMock()
        tag_config.name = 'rhel-coreos'
        mock_get_configs.return_value = [tag_config]

        gacli.rhcos_by_tag = defaultdict(
            dict,
            {
                'rhel-coreos': {'x86_64': 'quay.io/art-dev@sha256:' + 'a' * 64},
            },
        )
        gacli._exit_with_error = MagicMock()

        # Image has neither label
        mock_oc_info.return_value = {'config': {'config': {'Labels': {}}}}

        gacli._get_rhcos_container()

        gacli._exit_with_error.assert_called_once()
        error_msg = gacli._exit_with_error.call_args.args[0]
        self.assertIn('missing both', error_msg)
        self.assertIn('coreos.build.manifest-list-tag', error_msg)
        self.assertIn('release', error_msg)
