from datetime import datetime
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.model import Model
from doozerlib.cli.release_gen_assembly import GenAssemblyCli
from flexmock import flexmock
from semver import VersionInfo


class TestGenAssemblyCli(IsolatedAsyncioTestCase):
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
            ],
            'env': 'stage',
        }
        self.assertEqual(expected, gacli._get_shipment_info())
        gacli.logger.warning.assert_called_once_with("No matching previous assembly found")

    @patch('doozerlib.cli.release_gen_assembly.get_assembly_release_date')
    @patch('doozerlib.cli.release_gen_assembly.get_in_flight')
    def test_set_release_date_and_in_flight_custom(self, mock_get_in_flight, mock_get_assembly_release_date):
        """
        Test that for custom assemblies, the method returns early without doing anything
        """
        runtime = MagicMock()
        runtime.group = 'openshift-4.13'

        gacli = GenAssemblyCli(
            runtime=runtime, custom=True, gen_assembly_name='custom-assembly', release_date=None, in_flight=None
        )

        # Method should return early and not call external functions
        gacli._set_release_date_and_in_flight()

        mock_get_assembly_release_date.assert_not_called()
        mock_get_in_flight.assert_not_called()

        # Values should remain unchanged
        self.assertIsNone(gacli.release_date)
        self.assertIsNone(gacli.in_flight)

    @patch('doozerlib.cli.release_gen_assembly.get_assembly_release_date')
    @patch('doozerlib.cli.release_gen_assembly.get_in_flight')
    def test_set_release_date_and_in_flight_no_release_date(self, mock_get_in_flight, mock_get_assembly_release_date):
        """
        Test that when no release date is provided, it fetches from schedule
        """
        runtime = MagicMock()
        runtime.group = 'openshift-4.13'

        fetched_date = datetime(2023, 12, 15)
        determined_in_flight = '4.12.45'

        mock_get_assembly_release_date.return_value = fetched_date
        mock_get_in_flight.return_value = determined_in_flight

        gacli = GenAssemblyCli(
            runtime=runtime, custom=False, gen_assembly_name='4.13.5', release_date=None, in_flight=None
        )

        gacli._set_release_date_and_in_flight()

        # Should fetch release date from schedule
        mock_get_assembly_release_date.assert_called_once_with('4.13.5', gacli.assembly_type, 'openshift-4.13')

        # Should determine in-flight release
        mock_get_in_flight.assert_called_once_with('openshift-4.13', fetched_date)

        # Should set the fetched values
        self.assertEqual(gacli.release_date, fetched_date)
        self.assertEqual(gacli.in_flight, determined_in_flight)

    @patch('doozerlib.cli.release_gen_assembly.get_assembly_release_date')
    @patch('doozerlib.cli.release_gen_assembly.get_in_flight')
    def test_set_release_date_and_in_flight_with_release_date(self, mock_get_in_flight, mock_get_assembly_release_date):
        """
        Test that when release date is provided, it uses it without fetching from schedule
        """
        runtime = MagicMock()
        runtime.group = 'openshift-4.13'

        provided_date = datetime(2023, 11, 20)
        determined_in_flight = '4.12.30'

        mock_get_in_flight.return_value = determined_in_flight

        gacli = GenAssemblyCli(
            runtime=runtime, custom=False, gen_assembly_name='4.13.3', release_date=provided_date, in_flight=None
        )

        gacli._set_release_date_and_in_flight()

        # Should NOT fetch release date from schedule
        mock_get_assembly_release_date.assert_not_called()

        # Should determine in-flight release using provided date
        mock_get_in_flight.assert_called_once_with('openshift-4.13', provided_date)

        # Should keep the provided date
        self.assertEqual(gacli.release_date, provided_date)
        self.assertEqual(gacli.in_flight, determined_in_flight)

    @patch('doozerlib.cli.release_gen_assembly.get_assembly_release_date')
    @patch('doozerlib.cli.release_gen_assembly.get_in_flight')
    def test_set_release_date_and_in_flight_matching_in_flight(
        self, mock_get_in_flight, mock_get_assembly_release_date
    ):
        """
        Test that when provided in-flight matches determined in-flight, it uses it without warning
        """
        runtime = MagicMock()
        runtime.group = 'openshift-4.13'

        release_date = datetime(2023, 12, 1)
        provided_in_flight = '4.12.35'
        determined_in_flight = '4.12.35'  # Same as provided

        mock_get_assembly_release_date.return_value = release_date
        mock_get_in_flight.return_value = determined_in_flight

        gacli = GenAssemblyCli(
            runtime=runtime, custom=False, gen_assembly_name='4.13.4', release_date=None, in_flight=provided_in_flight
        )

        gacli._set_release_date_and_in_flight()

        # Should keep the provided in-flight release
        self.assertEqual(gacli.in_flight, provided_in_flight)

    @patch('doozerlib.cli.release_gen_assembly.get_assembly_release_date')
    @patch('doozerlib.cli.release_gen_assembly.get_in_flight')
    def test_set_release_date_and_in_flight_mismatched_in_flight(
        self, mock_get_in_flight, mock_get_assembly_release_date
    ):
        """
        Test that when provided in-flight doesn't match determined in-flight, it logs warning but keeps provided
        """
        runtime = MagicMock()
        runtime.group = 'openshift-4.13'

        release_date = datetime(2023, 12, 1)
        provided_in_flight = '4.12.30'
        determined_in_flight = '4.12.35'  # Different from provided

        mock_get_assembly_release_date.return_value = release_date
        mock_get_in_flight.return_value = determined_in_flight

        gacli = GenAssemblyCli(
            runtime=runtime, custom=False, gen_assembly_name='4.13.4', release_date=None, in_flight=provided_in_flight
        )

        gacli._set_release_date_and_in_flight()

        # Should keep the provided in-flight release (not the determined one)
        self.assertEqual(gacli.in_flight, provided_in_flight)

    @patch('doozerlib.cli.release_gen_assembly.get_assembly_release_date')
    @patch('doozerlib.cli.release_gen_assembly.get_in_flight')
    def test_set_release_date_and_in_flight_no_in_flight_provided(
        self, mock_get_in_flight, mock_get_assembly_release_date
    ):
        """
        Test that when no in-flight is provided, it uses the determined one
        """
        runtime = MagicMock()
        runtime.group = 'openshift-4.13'

        release_date = datetime(2023, 12, 1)
        determined_in_flight = '4.12.40'

        mock_get_assembly_release_date.return_value = release_date
        mock_get_in_flight.return_value = determined_in_flight

        gacli = GenAssemblyCli(
            runtime=runtime, custom=False, gen_assembly_name='4.13.6', release_date=None, in_flight=None
        )

        gacli._set_release_date_and_in_flight()

        # Should use the determined in-flight release
        self.assertEqual(gacli.in_flight, determined_in_flight)

    @patch('doozerlib.cli.release_gen_assembly.get_assembly_release_date')
    @patch('doozerlib.cli.release_gen_assembly.get_in_flight')
    def test_set_release_date_and_in_flight_all_provided(self, mock_get_in_flight, mock_get_assembly_release_date):
        """
        Test the complete flow when both release date and in-flight are provided
        """
        runtime = MagicMock()
        runtime.group = 'openshift-4.14'

        provided_date = datetime(2024, 1, 15)
        provided_in_flight = '4.13.10'
        determined_in_flight = '4.13.12'  # Different from provided

        mock_get_in_flight.return_value = determined_in_flight

        gacli = GenAssemblyCli(
            runtime=runtime,
            custom=False,
            gen_assembly_name='4.14.1',
            release_date=provided_date,
            in_flight=provided_in_flight,
        )

        gacli._set_release_date_and_in_flight()

        # Should not fetch release date from schedule
        mock_get_assembly_release_date.assert_not_called()

        # Should determine in-flight using provided date
        mock_get_in_flight.assert_called_once_with('openshift-4.14', provided_date)

        # Should keep both provided values
        self.assertEqual(gacli.release_date, provided_date)
        self.assertEqual(gacli.in_flight, provided_in_flight)

    def test_generate_assembly_definition_basic_structure(self):
        """Test basic structure of assembly definition"""
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 14)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.14.1', gen_microshift=False)
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.STANDARD
        gacli.final_previous_list = []
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {'x86_64': 'test-release'}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}
        gacli.release_date = datetime(2023, 11, 15)

        result = gacli._generate_assembly_definition()

        expected = {
            'releases': {
                '4.14.1': {
                    'assembly': {
                        'type': 'standard',
                        'basis': {
                            'brew_event': 12345,
                            'reference_releases': {'x86_64': 'test-release'},
                        },
                        'group': {
                            'advisories': {'image': -1, 'rpm': -1, 'extras': -1, 'metadata': -1},
                            'release_jira': 'ART-0',
                            'release_date': '2023-Nov-15',
                        },
                        'rhcos': {'machine-os': {'images': {'x86_64': 'test-rhcos'}}},
                        'members': {'rpms': [], 'images': []},
                    }
                }
            }
        }
        self.assertEqual(result, expected)

    def test_generate_assembly_definition_custom_assembly(self):
        """Test custom assembly doesn't include advisories or release_date"""
        runtime = MagicMock(build_system='brew')

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='custom-test', custom=True)
        gacli.assembly_type = AssemblyTypes.CUSTOM
        gacli.final_previous_list = []
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.primary_rhcos_tag = 'machine-os'
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}

        result = gacli._generate_assembly_definition()

        expected = {
            'releases': {
                'custom-test': {
                    'assembly': {
                        'type': 'custom',
                        'basis': {
                            'brew_event': 12345,
                            'reference_releases': {},
                        },
                        'group': {'arches!': ['x86_64']},
                        'rhcos': {'machine-os': {'images': {'x86_64': 'test-rhcos'}}},
                        'members': {'rpms': [], 'images': []},
                    }
                }
            }
        }
        self.assertEqual(result, expected)

    def test_generate_assembly_definition_konflux_build_system(self):
        """Test Konflux build system uses 'time' instead of 'brew_event'"""
        runtime = MagicMock(build_system='konflux')
        runtime.get_major_minor_fields.return_value = (4, 14)

        test_time = datetime(2023, 12, 15, 10, 30, 0)
        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.14.1', gen_microshift=False)
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.STANDARD
        gacli.final_previous_list = []
        gacli.assembly_basis_time = test_time
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}
        gacli.release_date = datetime(2023, 12, 15)

        result = gacli._generate_assembly_definition()

        expected = {
            'releases': {
                '4.14.1': {
                    'assembly': {
                        'type': 'standard',
                        'basis': {
                            'time': test_time,
                            'reference_releases': {},
                        },
                        'group': {
                            'advisories': {'rpm': -1},
                            'release_jira': 'ART-0',
                            'release_date': '2023-Dec-15',
                            'shipment': {
                                'advisories': [
                                    {'kind': 'image'},
                                    {'kind': 'extras'},
                                    {'kind': 'metadata'},
                                ],
                            },
                        },
                        'rhcos': {'machine-os': {'images': {'x86_64': 'test-rhcos'}}},
                        'members': {'rpms': [], 'images': []},
                    }
                }
            }
        }
        self.assertEqual(result, expected)

    def test_generate_assembly_definition_release_date_formatting(self):
        """Test release_date is formatted correctly"""
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 14)

        test_date = datetime(2023, 12, 15)
        gacli = GenAssemblyCli(
            runtime=runtime, gen_assembly_name='4.14.1', release_date=test_date, gen_microshift=False
        )
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.STANDARD
        gacli.final_previous_list = []
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}

        result = gacli._generate_assembly_definition()

        expected = {
            'releases': {
                '4.14.1': {
                    'assembly': {
                        'type': 'standard',
                        'basis': {
                            'brew_event': 12345,
                            'reference_releases': {},
                        },
                        'group': {
                            'advisories': {'image': -1, 'rpm': -1, 'extras': -1, 'metadata': -1},
                            'release_jira': 'ART-0',
                            'release_date': '2023-Dec-15',
                        },
                        'rhcos': {'machine-os': {'images': {'x86_64': 'test-rhcos'}}},
                        'members': {'rpms': [], 'images': []},
                    }
                }
            }
        }
        self.assertEqual(result, expected)

    def test_generate_assembly_definition_previous_list(self):
        """Test previous list is included as upgrades"""
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 14)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.14.1', gen_microshift=False)
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.STANDARD
        gacli.final_previous_list = [VersionInfo.parse('4.13.1'), VersionInfo.parse('4.13.0')]
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}
        gacli.release_date = datetime(2023, 10, 20)

        result = gacli._generate_assembly_definition()

        expected = {
            'releases': {
                '4.14.1': {
                    'assembly': {
                        'type': 'standard',
                        'basis': {
                            'brew_event': 12345,
                            'reference_releases': {},
                        },
                        'group': {
                            'advisories': {'image': -1, 'rpm': -1, 'extras': -1, 'metadata': -1},
                            'release_jira': 'ART-0',
                            'release_date': '2023-Oct-20',
                            'upgrades': '4.13.1,4.13.0',
                        },
                        'rhcos': {'machine-os': {'images': {'x86_64': 'test-rhcos'}}},
                        'members': {'rpms': [], 'images': []},
                    }
                }
            }
        }
        self.assertEqual(result, expected)

    def test_generate_assembly_definition_prerelease_mode(self):
        """Test prerelease mode sets operator_index_mode"""
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 14)

        gacli = GenAssemblyCli(
            runtime=runtime, gen_assembly_name='ec.0', pre_ga_mode='prerelease', gen_microshift=False
        )
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.PREVIEW
        gacli.final_previous_list = []
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}
        gacli.release_date = datetime(2023, 9, 10)

        result = gacli._generate_assembly_definition()

        expected = {
            'releases': {
                'ec.0': {
                    'assembly': {
                        'type': 'preview',
                        'basis': {
                            'brew_event': 12345,
                            'reference_releases': {},
                        },
                        'group': {
                            'advisories': {'image': -1, 'rpm': -1, 'extras': -1, 'metadata': -1, 'prerelease': -1},
                            'release_jira': 'ART-0',
                            'release_date': '2023-Sep-10',
                            'operator_index_mode': 'pre-release',
                        },
                        'rhcos': {'machine-os': {'images': {'x86_64': 'test-rhcos'}}},
                        'members': {'rpms': [], 'images': []},
                    }
                }
            }
        }
        self.assertEqual(result, expected)

    def test_generate_assembly_definition_microshift_enabled(self):
        """Test microshift advisory is added when gen_microshift=True"""
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 14)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.14.1', gen_microshift=True)
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.STANDARD
        gacli.final_previous_list = []
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}
        gacli.release_date = datetime(2023, 11, 15)

        result = gacli._generate_assembly_definition()

        expected = {
            'releases': {
                '4.14.1': {
                    'assembly': {
                        'type': 'standard',
                        'basis': {
                            'brew_event': 12345,
                            'reference_releases': {},
                        },
                        'group': {
                            'advisories': {'image': -1, 'rpm': -1, 'extras': -1, 'metadata': -1, 'microshift': -1},
                            'release_jira': 'ART-0',
                            'release_date': '2023-Nov-15',
                        },
                        'rhcos': {'machine-os': {'images': {'x86_64': 'test-rhcos'}}},
                        'members': {'rpms': [], 'images': []},
                    }
                }
            }
        }
        self.assertEqual(result, expected)

    def test_generate_assembly_definition_microshift_disabled(self):
        """Test microshift advisory is NOT added when gen_microshift=False"""
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 14)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.14.1', gen_microshift=False)
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.STANDARD
        gacli.final_previous_list = []
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}
        gacli.release_date = datetime(2023, 11, 15)

        result = gacli._generate_assembly_definition()

        advisories = result['releases']['4.14.1']['assembly']['group']['advisories']
        self.assertNotIn('microshift', advisories)
        self.assertEqual(advisories, {'image': -1, 'rpm': -1, 'extras': -1, 'metadata': -1})

    def test_generate_assembly_definition_microshift_pre_414(self):
        """Test microshift advisory is NOT added for versions < 4.14"""
        runtime = MagicMock(build_system='brew')
        runtime.get_major_minor_fields.return_value = (4, 13)

        gacli = GenAssemblyCli(runtime=runtime, gen_assembly_name='4.13.1', gen_microshift=True)
        gacli.releases_config = MagicMock()
        gacli.releases_config.releases = {}
        gacli.assembly_type = AssemblyTypes.STANDARD
        gacli.final_previous_list = []
        gacli.basis_event = 12345
        gacli.reference_releases_by_arch = {}
        gacli.rhcos_by_tag = {'machine-os': {'x86_64': 'test-rhcos'}}
        gacli.force_is = set()
        gacli.component_image_builds = {}
        gacli.component_rpm_builds = {}
        gacli.release_date = datetime(2023, 11, 15)

        result = gacli._generate_assembly_definition()

        advisories = result['releases']['4.13.1']['assembly']['group']['advisories']
        self.assertNotIn('microshift', advisories)
        self.assertEqual(advisories, {'image': -1, 'rpm': -1, 'extras': -1, 'metadata': -1})
