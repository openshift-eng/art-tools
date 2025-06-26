import unittest
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

from elliottlib import constants
from elliottlib.bzutil import BugzillaBug
from elliottlib.cli import attach_cve_flaws_cli
from elliottlib.errata_async import AsyncErrataAPI


class TestAttachCVEFlawsCLI(unittest.IsolatedAsyncioTestCase):
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
        versions = {'minor': '4', 'patch': '42'}

        flaw_bugs = [
            Mock(alias=['CVE-2022-123'], severity='urgent', summary='CVE-2022-123 foo'),
            Mock(alias=['CVE-2022-456'], severity='high', summary='CVE-2022-456 bar'),
        ]

        attach_cve_flaws_cli.get_updated_advisory_rhsa(
            boilerplate,
            advisory,
            flaw_bugs,
            versions,
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
        actual = await attach_cve_flaws_cli.associate_builds_with_cves(
            errata_api, advisory, flaw_bugs, attached_tracker_bugs, tracker_flaws, dry_run=False
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

        result = attach_cve_flaws_cli.get_cve_component_mapping(flaw_bugs, attached_tracker_bugs, tracker_flaws)

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

            result = attach_cve_flaws_cli.get_cve_component_mapping(
                flaw_bugs, attached_tracker_bugs, tracker_flaws, attached_components
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
        result = attach_cve_flaws_cli.get_cve_component_mapping(flaw_bugs, attached_tracker_bugs, tracker_flaws)
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
            attach_cve_flaws_cli.get_cve_component_mapping(flaw_bugs_no_alias, attached_tracker_bugs, tracker_flaws)
        with self.assertRaisesRegex(ValueError, "Bug 101 should have exactly 1 CVE alias."):
            attach_cve_flaws_cli.get_cve_component_mapping(
                flaw_bugs_multiple_aliases, attached_tracker_bugs, tracker_flaws
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
            attach_cve_flaws_cli.get_cve_component_mapping(flaw_bugs, attached_tracker_bugs, tracker_flaws)


if __name__ == '__main__':
    unittest.main()
