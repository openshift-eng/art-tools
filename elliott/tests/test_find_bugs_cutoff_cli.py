import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import click
from elliottlib.cli.find_bugs_cutoff_cli import (
    filter_by_component,
    get_component_names_from_nvrs,
    get_timestamp_from_nvr,
    parse_cutoff_time,
    validate_options,
)
from flexmock import flexmock


class TestGetTimestampFromNvr(unittest.TestCase):
    def test_valid_nvr(self):
        nvr = "ose-installer-container-v4.18.0-202504151328.p0.g6666.assembly.stream"
        ts = get_timestamp_from_nvr(nvr)
        expected = datetime(2025, 4, 15, 13, 28, tzinfo=timezone.utc).timestamp()
        self.assertEqual(ts, expected)

    def test_valid_nvr_different_timestamp(self):
        nvr = "logging-fluentd-container-v4.1.14-201908291507"
        ts = get_timestamp_from_nvr(nvr)
        expected = datetime(2019, 8, 29, 15, 7, tzinfo=timezone.utc).timestamp()
        self.assertEqual(ts, expected)

    def test_nvr_without_timestamp_raises(self):
        nvr = "some-package-1.0-1.el8"
        with self.assertRaises(click.BadParameter):
            get_timestamp_from_nvr(nvr)


class TestParseCutoffTime(unittest.TestCase):
    def test_iso_with_z_suffix(self):
        ts = parse_cutoff_time("2025-04-15T13:28:00Z")
        expected = datetime(2025, 4, 15, 13, 28, tzinfo=timezone.utc).timestamp()
        self.assertEqual(ts, expected)

    def test_iso_with_offset(self):
        ts = parse_cutoff_time("2025-04-15T13:28:00+00:00")
        expected = datetime(2025, 4, 15, 13, 28, tzinfo=timezone.utc).timestamp()
        self.assertEqual(ts, expected)

    def test_naive_datetime_treated_as_utc(self):
        ts = parse_cutoff_time("2025-04-15T13:28:00")
        expected = datetime(2025, 4, 15, 13, 28, tzinfo=timezone.utc).timestamp()
        self.assertEqual(ts, expected)


class TestGetComponentNamesFromNvrs(unittest.TestCase):
    def test_single_nvr(self):
        names = get_component_names_from_nvrs(["ose-installer-container-v4.18.0-202504151328.p0.g6666.assembly.stream"])
        self.assertEqual(names, {"ose-installer-container"})

    def test_multiple_nvrs(self):
        names = get_component_names_from_nvrs(
            [
                "ose-installer-container-v4.18.0-202504011000.p0.g1234.assembly.stream",
                "ose-cli-container-v4.18.0-202504151328.p0.g5678.assembly.stream",
            ]
        )
        self.assertEqual(names, {"ose-installer-container", "ose-cli-container"})

    def test_same_package_different_versions(self):
        names = get_component_names_from_nvrs(
            [
                "ose-installer-container-v4.18.0-202504011000.p0.g1234.assembly.stream",
                "ose-installer-container-v4.18.0-202504151328.p0.g5678.assembly.stream",
            ]
        )
        self.assertEqual(names, {"ose-installer-container"})

    def test_none_values_skipped(self):
        names = get_component_names_from_nvrs(
            [None, "ose-installer-container-v4.18.0-202504151328.p0.g1234.assembly.stream"]
        )
        self.assertEqual(names, {"ose-installer-container"})

    def test_empty_list(self):
        names = get_component_names_from_nvrs([])
        self.assertEqual(names, set())


class TestFilterByComponent(unittest.TestCase):
    def _make_bug(self, bug_id, is_tracker, whiteboard_component=None):
        bug = flexmock(
            id=bug_id,
            is_tracker_bug=lambda: is_tracker,
            whiteboard_component=whiteboard_component,
        )
        return bug

    def test_non_tracker_bugs_pass_through(self):
        runtime = MagicMock()
        bugs = [
            self._make_bug("OCPBUGS-1", False),
            self._make_bug("OCPBUGS-2", False),
        ]
        result = filter_by_component(bugs, {"ose-installer-container"}, runtime)
        self.assertEqual(len(result), 2)

    def test_tracker_bugs_filtered_by_component(self):
        runtime = MagicMock()
        bugs = [
            self._make_bug("OCPBUGS-1", True, "ose-installer-container"),
            self._make_bug("OCPBUGS-2", True, "ose-cli-container"),
            self._make_bug("OCPBUGS-3", False),
        ]
        with patch(
            "elliottlib.cli.find_bugs_cutoff_cli.normalize_component_by_ocp_delivery_repo",
            side_effect=lambda runtime, name: name,
        ):
            result = filter_by_component(bugs, {"ose-installer-container"}, runtime)
        result_ids = [b.id for b in result]
        self.assertIn("OCPBUGS-1", result_ids)
        self.assertNotIn("OCPBUGS-2", result_ids)
        self.assertIn("OCPBUGS-3", result_ids)

    def test_tracker_with_no_whiteboard_excluded(self):
        runtime = MagicMock()
        bugs = [
            self._make_bug("OCPBUGS-1", True, None),
        ]
        with patch(
            "elliottlib.cli.find_bugs_cutoff_cli.normalize_component_by_ocp_delivery_repo",
            side_effect=lambda runtime, name: name,
        ):
            result = filter_by_component(bugs, {"ose-installer-container"}, runtime)
        self.assertEqual(len(result), 0)

    def test_normalization_applied(self):
        runtime = MagicMock()
        bugs = [
            self._make_bug("OCPBUGS-1", True, "openshift5/installer-rhel9"),
        ]
        with patch(
            "elliottlib.cli.find_bugs_cutoff_cli.normalize_component_by_ocp_delivery_repo",
            side_effect=lambda runtime, name: "ose-installer-container" if name.startswith("openshift5/") else name,
        ):
            result = filter_by_component(bugs, {"ose-installer-container"}, runtime)
        self.assertEqual(len(result), 1)


class TestValidateOptions(unittest.TestCase):
    def test_single_nvr_mode(self):
        mode = validate_options(
            nvr="foo-1.0-1", cutoff_time=None, from_nvr=None, to_nvr=None, from_time=None, to_time=None
        )
        self.assertEqual(mode, "single")

    def test_single_cutoff_time_mode(self):
        mode = validate_options(
            nvr=None, cutoff_time="2025-01-01T00:00:00Z", from_nvr=None, to_nvr=None, from_time=None, to_time=None
        )
        self.assertEqual(mode, "single")

    def test_range_nvr_mode(self):
        mode = validate_options(
            nvr=None, cutoff_time=None, from_nvr="foo-1.0-1", to_nvr="foo-1.0-2", from_time=None, to_time=None
        )
        self.assertEqual(mode, "range")

    def test_range_time_mode(self):
        mode = validate_options(
            nvr=None,
            cutoff_time=None,
            from_nvr=None,
            to_nvr=None,
            from_time="2025-01-01T00:00:00Z",
            to_time="2025-02-01T00:00:00Z",
        )
        self.assertEqual(mode, "range")

    def test_range_mixed_nvr_and_time(self):
        mode = validate_options(
            nvr=None,
            cutoff_time=None,
            from_nvr="foo-1.0-1",
            to_nvr=None,
            from_time=None,
            to_time="2025-02-01T00:00:00Z",
        )
        self.assertEqual(mode, "range")

    def test_no_options_raises(self):
        with self.assertRaises(click.BadParameter):
            validate_options(nvr=None, cutoff_time=None, from_nvr=None, to_nvr=None, from_time=None, to_time=None)

    def test_both_nvr_and_cutoff_time_raises(self):
        with self.assertRaises(click.BadParameter):
            validate_options(
                nvr="foo-1.0-1",
                cutoff_time="2025-01-01T00:00:00Z",
                from_nvr=None,
                to_nvr=None,
                from_time=None,
                to_time=None,
            )

    def test_single_and_range_mixed_raises(self):
        with self.assertRaises(click.BadParameter):
            validate_options(
                nvr="foo-1.0-1", cutoff_time=None, from_nvr="bar-1.0-1", to_nvr=None, from_time=None, to_time=None
            )

    def test_both_from_nvr_and_from_time_raises(self):
        with self.assertRaises(click.BadParameter):
            validate_options(
                nvr=None,
                cutoff_time=None,
                from_nvr="foo-1.0-1",
                to_nvr="bar-1.0-2",
                from_time="2025-01-01T00:00:00Z",
                to_time=None,
            )

    def test_both_to_nvr_and_to_time_raises(self):
        with self.assertRaises(click.BadParameter):
            validate_options(
                nvr=None,
                cutoff_time=None,
                from_nvr="foo-1.0-1",
                to_nvr="bar-1.0-2",
                from_time=None,
                to_time="2025-02-01T00:00:00Z",
            )

    def test_range_missing_from_raises(self):
        with self.assertRaises(click.BadParameter):
            validate_options(
                nvr=None, cutoff_time=None, from_nvr=None, to_nvr="bar-1.0-2", from_time=None, to_time=None
            )

    def test_range_missing_to_raises(self):
        with self.assertRaises(click.BadParameter):
            validate_options(
                nvr=None, cutoff_time=None, from_nvr="foo-1.0-1", to_nvr=None, from_time=None, to_time=None
            )


if __name__ == '__main__':
    unittest.main()
