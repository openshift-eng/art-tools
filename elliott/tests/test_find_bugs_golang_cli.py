from unittest import TestCase
from unittest.mock import MagicMock, patch

from artcommonlib.release_util import get_patch_from_release
from artcommonlib.rpm_utils import parse_nvr
from elliottlib import constants
from elliottlib.cli.find_bugs_golang_cli import FindBugsGolangCli
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


def _make_bug(bug_id="OCPBUGS-99999", component="openshift-golang-builder-container"):
    bug = MagicMock()
    bug.id = bug_id
    bug.whiteboard_component = component
    bug.corresponding_flaw_bug_ids = [123456]
    return bug


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
