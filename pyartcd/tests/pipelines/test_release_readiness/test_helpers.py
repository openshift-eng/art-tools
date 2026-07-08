from datetime import timezone
from unittest import IsolatedAsyncioTestCase

from pyartcd.pipelines.release_readiness.helpers import parse_nightly_timestamp, worst_status
from pyartcd.pipelines.release_readiness.models import Status


class TestHelpers(IsolatedAsyncioTestCase):
    def test_worst_status_all_green(self):
        self.assertEqual(worst_status([Status.GREEN, Status.GREEN]), Status.GREEN)

    def test_worst_status_with_yellow(self):
        self.assertEqual(worst_status([Status.GREEN, Status.YELLOW]), Status.YELLOW)

    def test_worst_status_with_red(self):
        self.assertEqual(worst_status([Status.GREEN, Status.YELLOW, Status.RED]), Status.RED)

    def test_worst_status_empty(self):
        self.assertEqual(worst_status([]), Status.GREEN)

    def test_parse_nightly_timestamp_standard(self):
        ts = parse_nightly_timestamp("4.21.0-0.nightly-2026-07-07-031500")
        self.assertIsNotNone(ts)
        self.assertEqual(ts.year, 2026)
        self.assertEqual(ts.month, 7)
        self.assertEqual(ts.day, 7)
        self.assertEqual(ts.hour, 3)
        self.assertEqual(ts.minute, 15)
        self.assertEqual(ts.second, 0)
        self.assertEqual(ts.tzinfo, timezone.utc)

    def test_parse_nightly_timestamp_konflux(self):
        ts = parse_nightly_timestamp("4.22.0-0.konflux-nightly-2026-03-13-124748")
        self.assertIsNotNone(ts)
        self.assertEqual(ts.year, 2026)
        self.assertEqual(ts.month, 3)
        self.assertEqual(ts.hour, 12)

    def test_parse_nightly_timestamp_invalid(self):
        self.assertIsNone(parse_nightly_timestamp("invalid-name"))
        self.assertIsNone(parse_nightly_timestamp(""))
