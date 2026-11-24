from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.release_util import get_patch_from_release
from artcommonlib.rpm_utils import parse_nvr
from elliottlib import constants
from elliottlib.cli.find_bugs_golang_cli import FindBugsGolangCli
from elliottlib.exceptions import ElliottFatalError
from semver.version import Version


def _make_cli(**overrides):
    defaults = dict(
        runtime=MagicMock(),
        pullspec="registry.example.com/ocp/release:4.17",
        cve_ids=(),
        components=(),
        tracker_ids=(),
        analyze=True,
        fixed_in_nvrs=(),
        update_tracker=False,
        force_update_tracker=False,
        art_jira=None,
        exclude_bug_statuses=[],
        jql_filter=None,
        dry_run=True,
    )
    defaults.update(overrides)
    return FindBugsGolangCli(**defaults)


class TestChangelogParsingFlag(IsolatedAsyncioTestCase):
    def test_parse_changelog_defaults_to_false(self):
        cli = _make_cli()
        self.assertFalse(cli.parse_changelog)

    def _make_runnable_cli(self, **overrides):
        """Build a CLI whose run() reaches the changelog-parsing decision point and
        then exits early (no bugs found), so tests can assert on _get_cves_from_nvr_changelogs
        without needing to mock the rest of the (heavy) bug-analysis pipeline.
        """
        runtime = MagicMock()
        runtime.get_major_minor.return_value = (4, 17)
        cli = _make_cli(
            runtime=runtime,
            fixed_in_nvrs=("golang-1.22.12-11.el9",),
            tracker_ids=("OCPBUGS-1",),
            **overrides,
        )
        cli.jira_tracker.get_bugs.return_value = []
        return cli

    @patch("elliottlib.cli.find_bugs_golang_cli.golang_report_for_version", return_value=[])
    @patch.object(FindBugsGolangCli, "_get_cves_from_nvr_changelogs")
    async def test_changelog_not_parsed_when_flag_disabled(self, mock_get_cves, _mock_report):
        cli = self._make_runnable_cli(cve_ids=(), parse_changelog=False)
        await cli.run()
        mock_get_cves.assert_not_called()

    @patch("elliottlib.cli.find_bugs_golang_cli.golang_report_for_version", return_value=[])
    @patch.object(FindBugsGolangCli, "_get_cves_from_nvr_changelogs")
    async def test_changelog_parsed_when_flag_enabled(self, mock_get_cves, _mock_report):
        mock_get_cves.return_value = {"CVE-2024-1394"}
        cli = self._make_runnable_cli(cve_ids=(), parse_changelog=True)
        await cli.run()
        mock_get_cves.assert_called_once()
        self.assertEqual(cli.cve_ids, ("CVE-2024-1394",))

    @patch("elliottlib.cli.find_bugs_golang_cli.golang_report_for_version", return_value=[])
    @patch.object(FindBugsGolangCli, "_get_cves_from_nvr_changelogs")
    async def test_changelog_not_parsed_when_cve_ids_explicitly_given(self, mock_get_cves, _mock_report):
        cli = self._make_runnable_cli(cve_ids=("CVE-2024-1394",), parse_changelog=True)
        await cli.run()
        mock_get_cves.assert_not_called()


def _make_bug(bug_id="OCPBUGS-99999", component="openshift-golang-builder-container"):
    bug = MagicMock()
    bug.id = bug_id
    bug.whiteboard_component = component
    bug.corresponding_flaw_bug_ids = [123456]
    return bug


class TestNightlyVerification(IsolatedAsyncioTestCase):
    @patch('elliottlib.cli.find_bugs_golang_cli.golang_report_for_version', return_value=[])
    @patch('elliottlib.cli.find_bugs_golang_cli.get_cve_from_go_db', return_value='GO-2026-1234')
    @patch('elliottlib.cli.find_bugs_golang_cli.get_fixed_in_version_go_db', return_value={Version.parse('1.26.1')})
    @patch('elliottlib.cli.find_bugs_golang_cli.get_cve_from_prodsec_db', return_value=None)
    @patch('elliottlib.cli.find_bugs_golang_cli.get_container_configs', return_value=[])
    @patch('elliottlib.cli.find_bugs_golang_cli.get_nvrs_from_release', side_effect=RuntimeError('unauthorized'))
    async def test_unreadable_nightly_prevents_all_tracker_updates(self, read_release, *_):
        cli = _make_cli(update_tracker=True, dry_run=False)
        cli._runtime.get_major_minor.return_value = (4, 17)
        cli.jira_tracker.target_release.return_value = ('4.17.z',)
        rpm_bug = _make_bug('OCPBUGS-1', component='some-rpm')
        builder_bug = _make_bug('OCPBUGS-2')
        rpm_bug.cve_id = 'CVE-2026-2345'
        builder_bug.cve_id = 'CVE-2026-1234'
        cli.jira_tracker._search.return_value = [rpm_bug, builder_bug]
        cli.is_fixed_rpm = AsyncMock(return_value=(True, 'RPM rebuilt', [], []))
        cli.move_bug_and_comment = MagicMock()

        with self.assertRaisesRegex(ElliottFatalError, 'Could not read release payload.*unauthorized'):
            await cli.run()

        cli.is_fixed_rpm.assert_awaited_once()
        read_release.assert_awaited_once_with(cli.pullspec, set(), registry_config=cli._runtime.registry_config)
        cli.move_bug_and_comment.assert_not_called()


@patch("elliottlib.cli.find_bugs_golang_cli.errata")
class TestIsFixed(TestCase):
    def test_all_builds_fixed_single_version(self, mock_errata):
        cli = _make_cli()
        bug = _make_bug(component="some-rpm")
        tracker_fixed_in = {Version.parse("1.22.12-11")}
        go_nvr_map = {
            "golang-1.22.12-11.el9": [("some-rpm", "1.0", "1.el9")],
        }
        fixed, comment, _, _ = cli._is_fixed(bug, tracker_fixed_in, go_nvr_map)
        self.assertTrue(fixed)

    def test_build_older_than_fix(self, mock_errata):
        cli = _make_cli()
        bug = _make_bug(component="some-rpm")
        tracker_fixed_in = {Version.parse("1.22.12-13")}
        go_nvr_map = {
            "golang-1.22.12-11.el9": [("some-rpm", "1.0", "1.el9")],
        }
        fixed, comment, _, _ = cli._is_fixed(bug, tracker_fixed_in, go_nvr_map)
        self.assertFalse(fixed)

    def test_multi_nvr_all_versions_in_tracker_fixed_in(self, mock_errata):
        """When el8 and el9 builds have different release numbers, both versions
        must appear in tracker_fixed_in for all builds to be considered fixed.
        This is the scenario that was broken before the fix: only the last NVR's
        version was kept, so the el9 build (release -11) was considered unfixed
        even though its changelog included the CVE fix.
        """
        cli = _make_cli()
        bug = _make_bug(component="some-rpm")
        tracker_fixed_in = {Version.parse("1.22.12-11"), Version.parse("1.22.12-13")}
        go_nvr_map = {
            "golang-1.22.12-11.el9": [("some-rpm", "1.0", "1.el9")] * 173,
            "golang-1.22.12-13.el8": [("some-rpm", "1.0", "1.el8")] * 15,
        }
        fixed, comment, _, _ = cli._is_fixed(bug, tracker_fixed_in, go_nvr_map)
        self.assertTrue(fixed)

    def test_multi_nvr_only_last_version_in_tracker_marks_unfixed(self, mock_errata):
        """With only the last NVR's version (the old buggy behavior),
        builds using the earlier release number are incorrectly unfixed.
        """
        cli = _make_cli()
        bug = _make_bug(component="some-rpm")
        tracker_fixed_in = {Version.parse("1.22.12-13")}
        go_nvr_map = {
            "golang-1.22.12-11.el9": [("some-rpm", "1.0", "1.el9")] * 173,
            "golang-1.22.12-13.el8": [("some-rpm", "1.0", "1.el8")] * 15,
        }
        fixed, comment, _, _ = cli._is_fixed(bug, tracker_fixed_in, go_nvr_map)
        self.assertFalse(fixed)

    def test_different_major_minor_not_compared(self, mock_errata):
        cli = _make_cli()
        bug = _make_bug(component="some-rpm")
        tracker_fixed_in = {Version.parse("1.24.12")}
        go_nvr_map = {
            "golang-1.23.10-11.el9": [("some-rpm", "1.0", "1.el9")],
        }
        fixed, comment, _, _ = cli._is_fixed(bug, tracker_fixed_in, go_nvr_map)
        self.assertFalse(fixed)

    @patch("elliottlib.cli.find_bugs_golang_cli.get_golang_container_nvrs")
    def test_builder_container_10pct_threshold(self, mock_get_golang, mock_errata):
        mock_get_golang.return_value = {"golang-1.22.12-11.el9": []}
        cli = _make_cli()
        bug = _make_bug(component=constants.GOLANG_BUILDER_CVE_COMPONENT)
        tracker_fixed_in = {Version.parse("1.22.12-11"), Version.parse("1.22.12-13")}
        go_nvr_map = {
            "golang-1.22.12-11.el9": [("img", "1.0", "1.el9")] * 173,
            "golang-1.22.12-13.el8": [("img", "1.0", "1.el8")] * 15,
            "golang-1.23.10-11.el9": [("img", "1.0", "1.el9")],
        }
        fixed, comment, _, _ = cli._is_fixed(bug, tracker_fixed_in, go_nvr_map)
        self.assertTrue(fixed, "1.23 has only 1 build (<10%), should be considered fixed via threshold")


class TestNvrVersionCollection(TestCase):
    def test_collects_all_nvr_versions(self):
        """Verify that version extraction from fixed_in_nvrs produces a version
        entry for every NVR, not just the last one.
        """
        fixed_in_nvrs = ["golang-1.22.12-11.el9", "golang-1.22.12-13.el8"]
        versions = set()
        for nvr in fixed_in_nvrs:
            parsed_nvr = parse_nvr(nvr)
            patch = get_patch_from_release(parsed_nvr['release'])
            version = Version.parse(f"{parsed_nvr['version']}-{patch}")
            versions.add(version)

        self.assertEqual(versions, {Version.parse("1.22.12-11"), Version.parse("1.22.12-13")})

    def test_single_nvr(self):
        fixed_in_nvrs = ["golang-1.22.12-13.el8"]
        versions = set()
        for nvr in fixed_in_nvrs:
            parsed_nvr = parse_nvr(nvr)
            patch = get_patch_from_release(parsed_nvr['release'])
            version = Version.parse(f"{parsed_nvr['version']}-{patch}")
            versions.add(version)

        self.assertEqual(versions, {Version.parse("1.22.12-13")})
