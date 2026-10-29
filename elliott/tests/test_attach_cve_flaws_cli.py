import unittest
from typing import Dict, Iterable, List, cast
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

from artcommonlib.assembly import AssemblyTypes
from click.testing import CliRunner
from elliottlib import constants
from elliottlib.bzutil import Bug, BugzillaBug
from elliottlib.cli.attach_cve_flaws_cli import AttachCveFlaws
from elliottlib.cli.common import cli
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.exceptions import ElliottFatalError
from elliottlib.shipment_model import ReleaseNotes
from parameterized import parameterized


class TestAttachCVEFlawsCLI(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Provide a minimal mock runtime for AttachCveFlaws
        self.mock_runtime = Mock()
        self.mock_runtime.get_errata_config.return_value = {}
        self.mock_runtime.get_major_minor_patch.return_value = (4, 4, 42)
        self.mock_runtime.get_bug_tracker.return_value = Mock()
        self.mock_runtime.group_config.advisories = {}
        self.mock_runtime.build_system = 'brew'
        self.mock_runtime.assembly = None
        self.mock_runtime.get_releases_config.return_value = {}
        self.mock_runtime.get_bug_tracker.side_effect = lambda t: Mock(
            type=t, get_tracker_bugs=Mock(return_value=[]), advisory_bug_ids=Mock(return_value=[]), attach_bugs=Mock()
        )
        self.mock_runtime.build_retrying_koji_client.return_value = Mock()

    def test_get_updated_advisory_rhsa(self):
        boilerplate = {
            'security_reviewer': 'some reviewer',
            'synopsis': 'some synopsis',
            'description': 'some description with {CVES}',
            'topic': "some topic {IMPACT}",
            'solution': 'some solution',
        }
        advisory = Mock(
            errata_type="RHBA",
            cve_names="something",
            security_impact="Low",
            update=Mock(),
            topic='some topic',
        )

        flaw_bugs = [
            Mock(alias=['CVE-2022-123'], severity='urgent', summary='CVE-2022-123 foo'),
            Mock(alias=['CVE-2022-456'], severity='high', summary='CVE-2022-456 bar'),
        ]

        pipeline = AttachCveFlaws(
            self.mock_runtime,
            advisory_id=0,
            into_default_advisories=False,
            default_advisory_type="",
            output="json",
            noop=False,
        )
        pipeline.minor = '4'
        pipeline.patch = '42'
        pipeline.get_updated_advisory_rhsa(
            boilerplate,
            advisory,
            flaw_bugs,
        )

        advisory.update.assert_any_call(
            errata_type='RHSA',
            security_reviewer=boilerplate['security_reviewer'],
            synopsis=boilerplate['synopsis'],
            topic=boilerplate['topic'].format(IMPACT="Low"),
            solution=boilerplate['solution'],
            security_impact='Low',
        )

        impact = 'Critical'
        advisory.update.assert_any_call(
            topic=boilerplate['topic'].format(IMPACT=impact),
        )
        advisory.update.assert_any_call(
            cve_names='CVE-2022-123 CVE-2022-456',
        )
        advisory.update.assert_any_call(
            security_impact=impact,
        )
        advisory.update.assert_any_call(
            description='some description with * foo (CVE-2022-123)\n* bar (CVE-2022-456)',
        )

    @patch("elliottlib.cli.attach_cve_flaws_cli.get_vulnerability_draft_flaws")
    def test_fail_on_vulnerability_draft_flaws(self, get_draft_flaws):
        tracker = Mock(id="OCPBUGS-1")
        draft_flaw = Mock(id=123)
        get_draft_flaws.return_value = {tracker.id: [draft_flaw]}
        pipeline = AttachCveFlaws(
            self.mock_runtime,
            advisory_id=0,
            into_default_advisories=False,
            default_advisory_type="",
            output="json",
            noop=False,
        )

        with self.assertRaisesRegex(
            ElliottFatalError,
            r'Cannot attach flaw bug\(s\) with Component "vulnerability-draft".*OCPBUGS-1: \[123\]',
        ):
            pipeline._fail_on_vulnerability_draft_flaws([tracker])

    @patch("elliottlib.errata_async.AsyncErrataUtils.associate_builds_with_cves", autospec=True)
    async def test_associate_builds_with_cves_bz(self, fake_urls_associate_builds_with_cves: AsyncMock):
        errata_api = AsyncMock(spec=AsyncErrataAPI)
        advisory = Mock(
            errata_id=12345,
            errata_builds={
                "Fake-Product-Version1": {
                    "a-1.0.0-1.el8": {},
                    "b-1.0.0-1.el8": {},
                    "c-1.0.0-1.el8": {},
                    "d-1.0.0-1.el8": {},
                },
                "Fake-Product-Version2": {
                    "a-1.0.0-1.el7": {},
                    "e-1.0.0-1.el7": {},
                    "f-1.0.0-1.el7": {},
                },
            },
        )
        errata_api.get_builds_flattened.return_value = [
            "a-1.0.0-1.el8",
            "b-1.0.0-1.el8",
            "c-1.0.0-1.el8",
            "d-1.0.0-1.el8",
            "a-1.0.0-1.el7",
            "e-1.0.0-1.el7",
            "f-1.0.0-1.el7",
        ]
        tracker_flaws = {
            1: [101, 103],
            2: [101, 103],
            3: [102, 103],
            4: [101, 103],
            5: [102],
        }
        attached_tracker_bugs = [
            BugzillaBug(Mock(id=1, keywords=constants.TRACKER_BUG_KEYWORDS, whiteboard="component: a")),
            BugzillaBug(Mock(id=2, keywords=constants.TRACKER_BUG_KEYWORDS, whiteboard="component: b")),
            BugzillaBug(Mock(id=3, keywords=constants.TRACKER_BUG_KEYWORDS, whiteboard="component: c")),
            BugzillaBug(Mock(id=4, keywords=constants.TRACKER_BUG_KEYWORDS, whiteboard="component: d")),
            BugzillaBug(Mock(id=5, keywords=constants.TRACKER_BUG_KEYWORDS, whiteboard="component: e")),
        ]
        flaw_id_bugs = {
            101: BugzillaBug(Mock(id=101, keywords=["Security"], alias=["CVE-2099-1"])),
            102: BugzillaBug(Mock(id=102, keywords=["Security"], alias=["CVE-2099-2"])),
            103: BugzillaBug(Mock(id=103, keywords=["Security"], alias=["CVE-2099-3"])),
        }
        flaw_bugs = list(flaw_id_bugs.values())
        pipeline = AttachCveFlaws(
            self.mock_runtime,
            advisory_id=0,
            into_default_advisories=False,
            default_advisory_type="",
            output="json",
            noop=False,
        )
        pipeline.errata_api = errata_api
        actual = await pipeline.associate_builds_with_cves(
            advisory,
            cast(Iterable[Bug], flaw_bugs),
            cast(List[Bug], attached_tracker_bugs),
            cast(Dict[int, Iterable], tracker_flaws),
        )
        expected_builds = [
            'a-1.0.0-1.el8',
            'b-1.0.0-1.el8',
            'c-1.0.0-1.el8',
            'd-1.0.0-1.el8',
            'a-1.0.0-1.el7',
            'e-1.0.0-1.el7',
            'f-1.0.0-1.el7',
        ]
        expected_cve_component_mapping = {
            'CVE-2099-1': {'a', 'd', 'b'},
            'CVE-2099-3': {'a', 'd', 'b', 'c'},
            'CVE-2099-2': {'c', 'e'},
        }
        fake_urls_associate_builds_with_cves.assert_awaited_once_with(
            errata_api, 12345, expected_builds, expected_cve_component_mapping, dry_run=False
        )
        self.assertEqual(actual, None)

    def test_get_cve_component_mapping_simple_case(self):
        # Create flaw bugs
        flaw_bugs = [
            BugzillaBug(Mock(id=101, alias=["CVE-2022-1"])),
            BugzillaBug(Mock(id=102, alias=["CVE-2022-2"])),
        ]

        attached_tracker_bugs = [
            BugzillaBug(Mock(id=1, whiteboard="component: component-a")),
            BugzillaBug(Mock(id=2, whiteboard="component: component-b")),
        ]

        tracker_flaws = {
            1: [101, 102],  # Tracker 1 → CVE-2022-1, CVE-2022-2
            2: [102],  # Tracker 2 → CVE-2022-2
        }

        result = AttachCveFlaws.get_cve_component_mapping(
            self.mock_runtime,
            cast(Iterable[Bug], flaw_bugs),
            cast(List[Bug], attached_tracker_bugs),
            cast(Dict[int, Iterable], tracker_flaws),
        )

        expected = {
            "CVE-2022-1": {"component-a"},
            "CVE-2022-2": {"component-a", "component-b"},
        }

        self.assertEqual(result, expected)

    def test_get_cve_component_mapping_rhcos(self):
        with patch("artcommonlib.arch_util.RHCOS_BREW_COMPONENTS", {"rhcos-x86_64", "rhcos-aarch64"}):
            flaw_bugs = [
                BugzillaBug(Mock(id=101, alias=['CVE-2022-1'])),
            ]
            attached_tracker_bugs = [
                BugzillaBug(Mock(id=1, whiteboard='component: rhcos')),
            ]
            tracker_flaws = {
                1: [101],
            }
            attached_components = {'rhcos-x86_64', 'rhcos-aarch64', 'some-other-component'}

            result = AttachCveFlaws.get_cve_component_mapping(
                self.mock_runtime,
                cast(Iterable[Bug], flaw_bugs),
                cast(List[Bug], attached_tracker_bugs),
                cast(Dict[int, Iterable], tracker_flaws),
                attached_components,
            )
            expected = {'CVE-2022-1': {'rhcos-x86_64', 'rhcos-aarch64'}}
            self.assertEqual(result, expected)

    def test_get_cve_component_mapping_rhcos_new_pscomponent_versions(self):
        """Test that the new 'openshift/ose-rhel-coreos-{N}' pscomponent values
        (for any RHEL version) are handled the same as the legacy 'rhcos' value."""
        for pscomponent in (
            "openshift/ose-rhel-coreos-8",
            "openshift/ose-rhel-coreos-9",
            "openshift/ose-rhel-coreos-10",
        ):
            with self.subTest(pscomponent=pscomponent):
                with patch("artcommonlib.arch_util.RHCOS_BREW_COMPONENTS", {"rhcos-x86_64", "rhcos-aarch64"}):
                    flaw_bugs = [
                        BugzillaBug(Mock(id=101, alias=['CVE-2022-1'])),
                    ]
                    attached_tracker_bugs = [
                        BugzillaBug(Mock(id=1, whiteboard=f'component: {pscomponent}')),
                    ]
                    tracker_flaws = {
                        1: [101],
                    }
                    attached_components = {'rhcos-x86_64', 'rhcos-aarch64', 'some-other-component'}

                    result = AttachCveFlaws.get_cve_component_mapping(
                        self.mock_runtime,
                        cast(Iterable[Bug], flaw_bugs),
                        cast(List[Bug], attached_tracker_bugs),
                        cast(Dict[int, Iterable], tracker_flaws),
                        attached_components,
                    )
                    expected = {'CVE-2022-1': {'rhcos-x86_64', 'rhcos-aarch64'}}
                    self.assertEqual(result, expected)

    def test_get_cve_component_non_attached_flaw(self):
        flaw_bugs = [
            BugzillaBug(Mock(id=101, alias=['CVE-2022-1'])),
        ]
        attached_tracker_bugs = [
            BugzillaBug(Mock(id=1, whiteboard='component: component-a')),
        ]
        tracker_flaws = {
            1: [101, 102],  # 102 is not in flaw_bugs, so it gets ignored
        }
        result = AttachCveFlaws.get_cve_component_mapping(
            self.mock_runtime,
            cast(Iterable[Bug], flaw_bugs),
            cast(List[Bug], attached_tracker_bugs),
            cast(Dict[int, Iterable], tracker_flaws),
        )
        expected = {'CVE-2022-1': {'component-a'}}
        self.assertEqual(result, expected)

    def test_get_cve_component_invalid_alias(self):
        flaw_bugs_no_alias = [
            BugzillaBug(Mock(id=101, alias=[])),
        ]
        flaw_bugs_multiple_aliases = [
            BugzillaBug(Mock(id=101, alias=['CVE-2022-1', 'CVE-2022-2'])),
        ]
        attached_tracker_bugs = [
            BugzillaBug(Mock(id=1, whiteboard='component: component-a')),
        ]
        tracker_flaws = {
            1: [101],
        }
        with self.assertRaisesRegex(ValueError, "Bug 101 should have exactly 1 CVE alias."):
            AttachCveFlaws.get_cve_component_mapping(
                self.mock_runtime,
                cast(Iterable[Bug], flaw_bugs_no_alias),
                cast(List[Bug], attached_tracker_bugs),
                cast(Dict[int, Iterable], tracker_flaws),
            )
        with self.assertRaisesRegex(ValueError, "Bug 101 should have exactly 1 CVE alias."):
            AttachCveFlaws.get_cve_component_mapping(
                self.mock_runtime,
                cast(Iterable[Bug], flaw_bugs_multiple_aliases),
                cast(List[Bug], attached_tracker_bugs),
                cast(Dict[int, Iterable], tracker_flaws),
            )

    def test_get_cve_component_mapping_missing_whiteboard(self):
        flaw_bugs = [
            BugzillaBug(Mock(id=101, alias=['CVE-2022-1'])),
        ]
        attached_tracker_bugs = [
            BugzillaBug(Mock(id=1, whiteboard='component: ')),
        ]
        tracker_flaws = {
            1: [101],
        }
        with self.assertRaisesRegex(ValueError, "Bug 1 doesn't have a valid whiteboard component."):
            AttachCveFlaws.get_cve_component_mapping(
                self.mock_runtime,
                cast(Iterable[Bug], flaw_bugs),
                cast(List[Bug], attached_tracker_bugs),
                cast(Dict[int, Iterable], tracker_flaws),
            )

    def test_contains_placeholders_with_placeholders(self):
        """
        Test that _contains_placeholders returns True when text contains template placeholders.
        """
        # Test with IMAGE_ADVISORY placeholder
        text_with_image_advisory = "This advisory is related to {IMAGE_ADVISORY}"
        self.assertTrue(AttachCveFlaws._contains_placeholders(text_with_image_advisory))

        # Test with SHA digest placeholders
        text_with_sha = "The image digest is {x864_DIGEST}"
        self.assertTrue(AttachCveFlaws._contains_placeholders(text_with_sha))

        # Test with multiple placeholders
        text_with_multiple = """
        Related advisories: {IMAGE_ADVISORY}
        SHA digests: {x864_DIGEST}, {s390x_DIGEST}, {ppc64le_DIGEST}, {aarch64_DIGEST}
        """
        self.assertTrue(AttachCveFlaws._contains_placeholders(text_with_multiple))

    def test_contains_placeholders_without_placeholders(self):
        """
        Test that _contains_placeholders returns False when text has real values (no placeholders).
        """
        # Test with real advisory ID
        text_with_real_advisory = "This advisory is related to RHSA-2026:2129"
        self.assertFalse(AttachCveFlaws._contains_placeholders(text_with_real_advisory))

        # Test with real SHA digest
        text_with_real_sha = (
            "The image digest is sha256:7ca8870aa5e505f969aa26161594a3f99b65baf7d29bab8adaca0cade51b0bb6"
        )
        self.assertFalse(AttachCveFlaws._contains_placeholders(text_with_real_sha))

        # Test with multiple real values
        text_with_real_values = """
        Related advisories: RHSA-2026:2129
        SHA digest: sha256:7ca8870aa5e505f969aa26161594a3f99b65baf7d29bab8adaca0cade51b0bb6
        """
        self.assertFalse(AttachCveFlaws._contains_placeholders(text_with_real_values))

        # Test with empty string
        self.assertFalse(AttachCveFlaws._contains_placeholders(""))

    def test_update_release_notes_preserves_description_with_real_values(self):
        """
        Test that update_release_notes preserves description when it contains real values (no placeholders).
        """
        boilerplate = {
            "synopsis": "New synopsis {MAJOR}.{MINOR}",
            "topic": "New topic with {IMPACT}",
            "description": "New description with {IMAGE_ADVISORY} and {CVES}",
            "solution": "New solution with {x864_DIGEST}",
        }

        # Release notes with real values (no placeholders)
        original_description = "See the image advisory RHSA-2026:2129 for container images"
        original_solution = (
            "The image digest is sha256:7ca8870aa5e505f969aa26161594a3f99b65baf7d29bab8adaca0cade51b0bb6"
        )

        release_notes = ReleaseNotes(
            type="RHBA",
            synopsis="Old synopsis",
            topic="Old topic",
            description=original_description,
            solution=original_solution,
        )

        flaw_bugs = [
            Mock(alias=['CVE-2022-123'], severity='urgent', summary='CVE-2022-123 foo', id=123),
        ]

        with patch('elliottlib.cli.attach_cve_flaws_cli.get_advisory_boilerplate', return_value=boilerplate):
            with patch('elliottlib.cli.attach_cve_flaws_cli.set_bugzilla_bug_ids'):
                pipeline = AttachCveFlaws(
                    self.mock_runtime,
                    advisory_id=0,
                    into_default_advisories=False,
                    default_advisory_type="image",
                    output="json",
                    noop=False,
                )

                pipeline.update_release_notes(
                    release_notes,
                    flaw_bugs,
                    [],
                    {},
                    None,
                )

                # Description and solution should be preserved (no placeholders)
                self.assertEqual(release_notes.description, original_description)
                self.assertEqual(release_notes.solution, original_solution)

                # Type should be converted to RHSA
                self.assertEqual(release_notes.type, "RHSA")

    def test_update_release_notes_regenerates_description_with_placeholders(self):
        """
        Test that update_release_notes regenerates description when it contains placeholders.
        """
        boilerplate = {
            'synopsis': 'New synopsis {MAJOR}.{MINOR}',
            'topic': 'New topic with {IMPACT}',
            'description': 'New description with {CVES}',
            'solution': 'New solution',
        }

        # Release notes with placeholders
        release_notes = ReleaseNotes(
            type="RHBA",
            synopsis="Old synopsis",
            topic="Old topic",
            description="See the image advisory {IMAGE_ADVISORY} for container images",
            solution="The image digest is {x864_DIGEST}",
        )

        flaw_bugs = [
            Mock(alias=['CVE-2022-123'], severity='urgent', summary='CVE-2022-123 foo', id=123),
        ]

        with patch('elliottlib.cli.attach_cve_flaws_cli.get_advisory_boilerplate', return_value=boilerplate):
            with patch('elliottlib.cli.attach_cve_flaws_cli.set_bugzilla_bug_ids'):
                pipeline = AttachCveFlaws(
                    self.mock_runtime,
                    advisory_id=0,
                    into_default_advisories=False,
                    default_advisory_type="image",
                    output="json",
                    noop=False,
                )

                pipeline.update_release_notes(
                    release_notes,
                    flaw_bugs,
                    [],
                    {},
                    None,
                )

                # Description and solution should be regenerated (had placeholders)
                self.assertNotEqual(
                    release_notes.description, "See the image advisory {IMAGE_ADVISORY} for container images"
                )
                self.assertIn("CVE-2022-123", release_notes.description)

                # Type should be converted to RHSA
                self.assertEqual(release_notes.type, "RHSA")

    @parameterized.expand(
        [
            ('rhea_ga', 'RHEA', AssemblyTypes.STANDARD, '4.99.0', True, 'RHEA'),
            ('rhea_zstream', 'RHEA', AssemblyTypes.STANDARD, '4.99.1', True, 'RHEA'),
            ('rhea_no_assembly', 'RHEA', AssemblyTypes.STREAM, None, True, 'RHEA'),
            ('rhsa_ga', 'RHSA', AssemblyTypes.STANDARD, '4.99.0', True, 'RHEA'),
            ('rhsa_zstream', 'RHSA', AssemblyTypes.STANDARD, '4.99.1', True, 'RHBA'),
            ('rhsa_candidate', 'RHSA', AssemblyTypes.CANDIDATE, '4.99.0', True, 'RHBA'),
            ('rhsa_preview', 'RHSA', AssemblyTypes.PREVIEW, '4.99.0', True, 'RHBA'),
            ('rhsa_stream', 'RHSA', AssemblyTypes.STREAM, 'stream', True, 'RHBA'),
            ('rhsa_no_assembly', 'RHSA', AssemblyTypes.STREAM, None, True, 'RHBA'),
            ('rhsa_standard_no_assembly', 'RHSA', AssemblyTypes.STANDARD, None, True, 'RHBA'),
            ('rhsa_prerelease', 'RHSA', AssemblyTypes.STANDARD, '4.99.0-ec.1', True, 'RHBA'),
            ('rhba_ga', 'RHBA', AssemblyTypes.STANDARD, '4.99.0', True, 'RHBA'),
            ('rhba_zstream', 'RHBA', AssemblyTypes.STANDARD, '4.99.1', True, 'RHBA'),
            ('disabled_rhea', 'RHEA', AssemblyTypes.STANDARD, '4.99.0', False, 'RHEA'),
            ('disabled_rhsa_ga', 'RHSA', AssemblyTypes.STANDARD, '4.99.0', False, 'RHSA'),
            ('disabled_rhsa_zstream', 'RHSA', AssemblyTypes.STANDARD, '4.99.1', False, 'RHSA'),
        ]
    )
    def test_update_release_notes_without_flaws(
        self, name, initial_type, assembly_type, assembly, reconcile, expected_type
    ):
        self.mock_runtime.assembly_type = assembly_type
        self.mock_runtime.assembly = assembly
        release_notes = ReleaseNotes(
            type=initial_type,
            synopsis='Original synopsis',
            topic='Original topic',
            description='{IMAGE_ADVISORY} description',
            solution='{x864_DIGEST} solution',
            cves=[{'key': 'CVE-2099-0001', 'component': 'example-component'}],
        )
        original_notes = release_notes.model_copy(deep=True)
        boilerplate = {
            'synopsis': 'Reconciled synopsis {MAJOR}.{MINOR}.{PATCH}',
            'topic': 'Reconciled topic',
            'description': 'Reconciled description',
            'solution': 'Reconciled solution',
        }
        pipeline = AttachCveFlaws(
            self.mock_runtime,
            advisory_id=0,
            into_default_advisories=False,
            default_advisory_type='image',
            output='json',
            noop=False,
            reconcile=reconcile,
        )
        with (
            patch(
                'elliottlib.cli.attach_cve_flaws_cli.get_advisory_boilerplate', return_value=boilerplate
            ) as get_boilerplate,
            patch('elliottlib.cli.attach_cve_flaws_cli.set_bugzilla_bug_ids') as set_bug_ids,
        ):
            pipeline.update_release_notes(release_notes, [], [], {})

        self.assertEqual(release_notes.type, expected_type)
        if reconcile and initial_type == 'RHSA':
            get_boilerplate.assert_called_once_with(
                runtime=self.mock_runtime, et_data={}, art_advisory_key='image', errata_type=expected_type
            )
            set_bug_ids.assert_called_once_with(release_notes, [])
            self.assertIsNone(release_notes.cves)
            self.assertEqual(release_notes.synopsis, 'Reconciled synopsis 4.4.42')
            self.assertEqual(release_notes.topic, boilerplate['topic'])
            self.assertEqual(release_notes.description, boilerplate['description'])
            self.assertEqual(release_notes.solution, boilerplate['solution'])
        else:
            self.assertEqual(release_notes, original_notes)
            get_boilerplate.assert_not_called()
            set_bug_ids.assert_not_called()

    @parameterized.expand([(True,), (False,)])
    def test_update_release_notes_promotes_rhea_with_flaws(self, reconcile):
        self.mock_runtime.assembly_type = AssemblyTypes.STANDARD
        self.mock_runtime.assembly = '4.99.0'
        release_notes = ReleaseNotes(type='RHEA', synopsis='Synopsis', topic='Topic', description='', solution='')
        flaw_bugs = [Mock(id=123, alias=['CVE-2099-0001'], severity='high', summary='CVE-2099-0001 example flaw')]
        boilerplate = {
            'synopsis': '{IMPACT} security update',
            'topic': '{IMPACT} topic',
            'description': '{CVES}',
            'solution': 'Example solution',
        }
        pipeline = AttachCveFlaws(
            self.mock_runtime,
            advisory_id=0,
            into_default_advisories=False,
            default_advisory_type='image',
            output='json',
            noop=False,
            reconcile=reconcile,
        )
        with (
            patch(
                'elliottlib.cli.attach_cve_flaws_cli.get_advisory_boilerplate', return_value=boilerplate
            ) as get_boilerplate,
            patch.object(
                AttachCveFlaws, 'get_cve_component_mapping', return_value={'CVE-2099-0001': ['example-component']}
            ),
            patch('elliottlib.cli.attach_cve_flaws_cli.set_bugzilla_bug_ids') as set_bug_ids,
        ):
            pipeline.update_release_notes(release_notes, flaw_bugs, [], {})

        self.assertEqual(release_notes.type, 'RHSA')
        self.assertEqual(
            [(cve.key, cve.component) for cve in release_notes.cves], [('CVE-2099-0001', 'example-component')]
        )
        self.assertEqual(release_notes.synopsis, 'Important security update')
        self.assertIn('CVE-2099-0001', release_notes.description)
        get_boilerplate.assert_called_once_with(
            runtime=self.mock_runtime, et_data={}, art_advisory_key='image', errata_type='RHSA'
        )
        set_bug_ids.assert_called_once_with(release_notes, [123])

    def test_update_release_notes_reconcile_preserves_real_values(self):
        """
        Test that reconciliation preserves description/solution when they contain real values.
        """
        from elliottlib.shipment_model import ReleaseNotes

        boilerplate = {
            'synopsis': 'RHBA synopsis {MAJOR}.{MINOR}',
            'topic': 'RHBA topic',
            'description': 'RHBA description',
            'solution': 'RHBA solution',
        }

        # Release notes with real values (no placeholders)
        original_description = "See the image advisory RHSA-2026:2129 for container images"
        original_solution = (
            "The image digest is sha256:7ca8870aa5e505f969aa26161594a3f99b65baf7d29bab8adaca0cade51b0bb6"
        )

        release_notes = ReleaseNotes(
            type="RHSA",
            synopsis="RHSA synopsis",
            topic="RHSA topic",
            description=original_description,
            solution=original_solution,
        )

        with patch('elliottlib.cli.attach_cve_flaws_cli.get_advisory_boilerplate', return_value=boilerplate):
            with patch('elliottlib.cli.attach_cve_flaws_cli.set_bugzilla_bug_ids'):
                pipeline = AttachCveFlaws(
                    self.mock_runtime,
                    advisory_id=0,
                    into_default_advisories=False,
                    default_advisory_type="image",
                    output="json",
                    noop=False,
                    reconcile=True,
                )

                pipeline.update_release_notes(
                    release_notes,
                    [],  # No flaw bugs - triggers reconciliation
                    [],
                    {},
                    None,
                )

                # Description and solution should be preserved during reconciliation
                self.assertEqual(release_notes.description, original_description)
                self.assertEqual(release_notes.solution, original_solution)

                # Type should be converted to RHBA
                self.assertEqual(release_notes.type, "RHBA")


class TestAttachCVEFlawsHelp(unittest.TestCase):
    def test_reconcile_help_explains_advisory_types(self):
        """Registered CLI help describes GA reconciliation without initializing services."""
        with patch('elliottlib.cli.common.Runtime') as runtime:
            result = CliRunner().invoke(cli, ['attach-cve-flaws', '--help'])

        self.assertEqual(result.exit_code, 0, result.output)
        help_text = ' '.join(result.output.split())
        self.assertIn('--reconcile', help_text)
        self.assertIn('RHEA for standard .0 GA assemblies', help_text)
        self.assertIn('RHBA for other assemblies', help_text)
        self.assertIn('when no eligible flaws remain', help_text)
        self.assertIn('removes flaw bugs and CVE associations if applicable', help_text)
        runtime.return_value.initialize.assert_not_called()


if __name__ == '__main__':
    unittest.main()
