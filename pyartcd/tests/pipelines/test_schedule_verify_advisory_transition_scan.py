import unittest
from unittest.mock import MagicMock, call, patch

from pyartcd.pipelines.scheduled.schedule_verify_advisory_transition_scan import scan_and_trigger

MODULE = "pyartcd.pipelines.scheduled.schedule_verify_advisory_transition_scan"


def make_mr(title="Shipment for 4.22.9", labels=None, state="opened", iid=42):
    return MagicMock(iid=iid, title=title, state=state, labels=["verify-release-success"] if labels is None else labels)


class TestScheduleVerifyAdvisoryTransitionScan(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(dry_run=False)
        self.gitlab = self.enterContext(patch(f"{MODULE}.GitLabClient.from_url")).return_value
        self.trigger = self.enterContext(patch(f"{MODULE}.jenkins.start_verify_advisory_transition"))

    async def test_queries_all_states_and_triggers_each_release(self):
        self.gitlab.list_merge_requests.return_value = [
            make_mr("Shipment for 4.22.9", state="closed"),
            make_mr("Shipment for 4.22.10 (ship date: 2026-10-23)", state="merged"),
            make_mr("Draft: Shipment for 5.0.0-rc.6"),
            make_mr("Shipment for 5.0.0-ec.2", state="merged"),
        ]
        await scan_and_trigger(self.runtime)
        self.gitlab.list_merge_requests.assert_called_once_with(
            "hybrid-platforms/art/ocp-shipment-data",
            state="all",
            labels=["verify-release-success"],
            per_page=100,
        )
        self.assertEqual(
            self.trigger.call_args_list,
            [
                call(version="4.22", assembly="4.22.9", block_until_building=False),
                call(version="4.22", assembly="4.22.10", block_until_building=False),
            ],
        )

    async def test_skips_eligible_rc_and_ec_shipments(self):
        for assembly in ("rc.6", "ec.2"):
            with self.subTest(assembly=assembly):
                self.gitlab.list_merge_requests.return_value = [make_mr(f"Shipment for 5.0.0-{assembly}")]
                await scan_and_trigger(self.runtime)
                self.trigger.assert_not_called()

    async def test_skips_completed_unverified_and_non_ocp_releases(self):
        self.gitlab.list_merge_requests.return_value = [
            make_mr(labels=["verify-release-success", "advisory-transition-success"]),
            make_mr(labels=[]),
            make_mr("Shipment for logging 6.5.2"),
        ]
        await scan_and_trigger(self.runtime)
        self.trigger.assert_not_called()

    async def test_dry_run_never_queues(self):
        self.runtime.dry_run = True
        self.gitlab.list_merge_requests.return_value = [make_mr()]
        await scan_and_trigger(self.runtime)
        self.trigger.assert_not_called()

    async def test_deduplicates_release_within_scan(self):
        self.gitlab.list_merge_requests.return_value = [make_mr(), make_mr(iid=43)]
        await scan_and_trigger(self.runtime)
        self.trigger.assert_called_once()

    async def test_trigger_error_does_not_stop_other_releases(self):
        self.gitlab.list_merge_requests.return_value = [make_mr(), make_mr("Shipment for 4.21.7")]
        self.trigger.side_effect = [RuntimeError("Jenkins unavailable"), None]
        await scan_and_trigger(self.runtime)
        self.assertEqual(self.trigger.call_count, 2)

    async def test_no_candidates(self):
        self.gitlab.list_merge_requests.return_value = []
        await scan_and_trigger(self.runtime)
        self.trigger.assert_not_called()
