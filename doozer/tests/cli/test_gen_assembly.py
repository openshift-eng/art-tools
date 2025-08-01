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

    @patch('doozerlib.cli.release_gen_assembly.GenAssemblyCli._exit_with_error', MagicMock(return_value=None))
    def test_group_nightly_mismatch(self):
        """
        Specified nightlies must match the group `major.minor` parameter.
        The command should raise an error otherwise
        """

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.12'
        gacli = flexmock(
            GenAssemblyCli(
                runtime=runtime,
                nightlies=['4.13.0-0.nightly-2022-12-01-153811'],
            )
        )
        gacli.should_receive('_exit_with_error').once()
        gacli._get_release_pullspecs()

    def test_nightly_release_pullspecs(self):
        """
        Check that for nightlies matching group param, two maps are populated:
        One that associates the group arch with the complete nightlies pullspecs,
        and one that associates the group arch with the nightlies names
        """

        runtime = MagicMock()
        runtime.get_minor_version.return_value = '4.13'

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
            },
        )
        self.assertEqual(release_jira, "ART-0")

    def test_get_advisories_release_jira_candidate_reuse(self):
        runtime = MagicMock()
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
                {'kind': 'prerelease'},
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
