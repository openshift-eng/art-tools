"""Unit tests for evidence-plugin contracts and bug-sweep integration."""

import json
import unittest
from unittest.mock import AsyncMock, Mock, patch

import elliottlib.cli.find_bugs_sweep_cli as sweep_cli
from elliottlib.bug_sweep_plugins.contracts import (
    PluginContext,
    PluginDecision,
    PluginDisposition,
    PluginEvaluation,
    PluginResult,
)
from elliottlib.bug_sweep_plugins.registry import available_plugin_names, create_plugins
from elliottlib.bug_sweep_plugins.rhcos_rpm import RHCOSRpmEvidencePlugin
from elliottlib.bug_sweep_plugins.runner import evaluate_plugins
from elliottlib.bzutil import JIRABugTracker
from elliottlib.cli.find_bugs_cli import FindBugsCli
from elliottlib.cli.find_bugs_sweep_cli import FindBugsSweep
from elliottlib.exceptions import ElliottFatalError
from elliottlib.rhcos_rpm_evidence.models import AnalysisReport, TrackerDisposition, TrackerFinding


class _Plugin:
    """Small test plugin that returns a configured result."""

    def __init__(self, name: str, result: PluginResult):
        self._name = name
        self._result = result

    @property
    def name(self) -> str:
        """Return this test plugin's stable name."""
        return self._name

    def evaluate(self, context: PluginContext) -> PluginResult:
        """Return the configured result."""
        return self._result


def _plugin_result(
    name: str = "test",
    decisions: tuple[PluginDecision, ...] = (),
    complete: bool = True,
) -> PluginResult:
    """Create a plugin result fixture."""
    return PluginResult(name, decisions, complete)


class TestPluginRunner(unittest.TestCase):
    """Check validation and reduction of plugin decisions."""

    def test_runner_claims_all_decisions_and_includes_only_include_disposition(self):
        result = _plugin_result(
            decisions=(
                PluginDecision("OCPBUGS-1", PluginDisposition.INCLUDE, "eligible"),
                PluginDecision("OCPBUGS-2", PluginDisposition.SUPPRESS, "no evidence"),
                PluginDecision("OCPBUGS-3", PluginDisposition.REVIEW, "needs human"),
            )
        )
        plugin = _Plugin("test", result)

        evaluation = evaluate_plugins((plugin,), Mock())

        self.assertEqual(evaluation.claimed_issue_keys, {"OCPBUGS-1", "OCPBUGS-2", "OCPBUGS-3"})
        self.assertEqual(evaluation.included_issue_keys, {"OCPBUGS-1"})
        self.assertEqual(evaluation.results, (result,))

    def test_runner_rejects_incomplete_search_duplicate_keys_and_multiple_owners(self):
        incomplete = _Plugin("partial", _plugin_result(name="partial", complete=False))
        with self.assertRaises(ElliottFatalError):
            evaluate_plugins((incomplete,), Mock())

        repeated = PluginDecision("OCPBUGS-1", PluginDisposition.REVIEW, "review")
        duplicate_key = _Plugin("duplicate", _plugin_result(name="duplicate", decisions=(repeated, repeated)))
        with self.assertRaisesRegex(ValueError, "duplicate issue decisions"):
            evaluate_plugins((duplicate_key,), Mock())

        shared_issue = PluginDecision("OCPBUGS-1", PluginDisposition.SUPPRESS, "claimed")
        first = _Plugin("first", _plugin_result(name="first", decisions=(shared_issue,)))
        second = _Plugin("second", _plugin_result(name="second", decisions=(shared_issue,)))
        with self.assertRaisesRegex(ValueError, "claimed by multiple"):
            evaluate_plugins((first, second), Mock())

    def test_runner_rejects_malformed_plugin_outputs(self):
        wrong_name = _Plugin("expected", _plugin_result(name="other"))
        with self.assertRaisesRegex(ValueError, "returned a result named"):
            evaluate_plugins((wrong_name,), Mock())

        empty_key = _Plugin(
            "empty", _plugin_result(name="empty", decisions=(PluginDecision("", PluginDisposition.REVIEW, "x"),))
        )
        with self.assertRaisesRegex(ValueError, "empty issue key"):
            evaluate_plugins((empty_key,), Mock())

        bad_disposition = _Plugin(
            "bad", _plugin_result(name="bad", decisions=(PluginDecision("OCPBUGS-1", "include", "x"),))
        )
        with self.assertRaisesRegex(ValueError, "invalid decision disposition"):
            evaluate_plugins((bad_disposition,), Mock())

        duplicate_plugin = _Plugin("same", _plugin_result(name="same"))
        with self.assertRaisesRegex(ValueError, "selected more than once"):
            evaluate_plugins((duplicate_plugin, duplicate_plugin), Mock())

    def test_registry_lists_and_validates_explicit_plugins(self):
        self.assertEqual(available_plugin_names(), ("rhcos-rpm",))
        (plugin,) = create_plugins(("rhcos-rpm",))
        self.assertIsInstance(plugin, RHCOSRpmEvidencePlugin)
        with self.assertRaisesRegex(ValueError, "Unknown evidence plugin"):
            create_plugins(("missing",))
        with self.assertRaisesRegex(ValueError, "selected only once"):
            create_plugins(("rhcos-rpm", "rhcos-rpm"))


class TestRHCOSPlugin(unittest.TestCase):
    """Check the RHCOS plugin adapter and disposition mapping."""

    def test_every_domain_disposition_maps_to_plugin_contract(self):
        include = {
            TrackerDisposition.READY_TO_VERIFY,
            TrackerDisposition.WOULD_VERIFY,
            TrackerDisposition.VERIFIED_FROM_EVIDENCE,
            TrackerDisposition.ALREADY_VERIFIED,
        }
        review = {
            TrackerDisposition.NEEDS_REVIEW,
            TrackerDisposition.EVIDENCE_INCOMPLETE,
            TrackerDisposition.STATUS_NOT_ELIGIBLE,
            TrackerDisposition.STATUS_UPDATE_FAILED,
        }
        suppress = {
            TrackerDisposition.EXCLUDED,
            TrackerDisposition.NO_EVIDENCE,
            TrackerDisposition.OBSERVED_IN_BASIS,
        }

        for disposition in include:
            self.assertEqual(RHCOSRpmEvidencePlugin._disposition_for(disposition), PluginDisposition.INCLUDE)
        for disposition in review:
            self.assertEqual(RHCOSRpmEvidencePlugin._disposition_for(disposition), PluginDisposition.REVIEW)
        for disposition in suppress:
            self.assertEqual(RHCOSRpmEvidencePlugin._disposition_for(disposition), PluginDisposition.SUPPRESS)

    def test_plugin_rejects_non_jira_trackers(self):
        context = PluginContext(Mock(), Mock(), frozenset())
        with self.assertRaisesRegex(TypeError, "requires the Jira bug tracker"):
            RHCOSRpmEvidencePlugin().evaluate(context)

    def test_plugin_translates_analysis_findings_to_decisions(self):
        report = AnalysisReport(
            "4.22.22",
            (),
            True,
            (),
            (
                TrackerFinding(
                    "OCPBUGS-1",
                    "CVE-1",
                    "rhcos",
                    "NEW",
                    TrackerDisposition.READY_TO_VERIFY,
                    "RHCOS evidence qualifies this tracker",
                ),
                TrackerFinding(
                    "OCPBUGS-2", "CVE-2", "rhcos", "NEW", TrackerDisposition.NO_EVIDENCE, "No changed RPM evidence"
                ),
            ),
            True,
            warnings=("RHCOS_BUILD_EVIDENCE_COLLECTION_FAILED: RHCOS RPM evidence collection failed",),
        )
        runtime = Mock(assembly="4.22.22")
        tracker = object.__new__(JIRABugTracker)
        context = PluginContext(runtime, tracker, frozenset({"OCPBUGS-3"}))

        with (
            patch("elliottlib.bug_sweep_plugins.rhcos_rpm.RHCOSInventoryProvider") as inventory_source,
            patch("elliottlib.bug_sweep_plugins.rhcos_rpm.KojiErrataRpmBuildEvidenceSource") as build_source,
            patch("elliottlib.bug_sweep_plugins.rhcos_rpm.JiraRHCOSTrackerRepository") as tracker_repository,
            patch("elliottlib.bug_sweep_plugins.rhcos_rpm.RHCOSRpmEvidenceService") as service_class,
        ):
            service_class.return_value.run.return_value = report
            result = RHCOSRpmEvidencePlugin().evaluate(context)

        service_class.assert_called_once_with(
            assembly="4.22.22",
            inventory_source=inventory_source.return_value,
            build_evidence_source=build_source.return_value,
            tracker_repository=tracker_repository.return_value,
            excluded_issue_keys=frozenset({"OCPBUGS-3"}),
        )
        self.assertEqual(result.plugin_name, "rhcos-rpm")
        self.assertTrue(result.domain_search_complete)
        self.assertEqual(
            [decision.disposition for decision in result.decisions],
            [PluginDisposition.INCLUDE, PluginDisposition.SUPPRESS],
        )
        self.assertEqual(
            result.warnings,
            ("RHCOS_BUILD_EVIDENCE_COLLECTION_FAILED: RHCOS RPM evidence collection failed",),
        )
        self.assertEqual(result.decisions[0].reason, "RHCOS evidence qualifies this tracker")


class TestBugSweepPluginOverlay(unittest.IsolatedAsyncioTestCase):
    """Check that plugin ownership is applied before normal sweep filters."""

    async def test_included_issue_bypasses_status_cutoff_and_claimed_issue_does_not_fall_back(self):
        review_bug = Mock(id="OCPBUGS-REVIEW")
        ordinary_bug = Mock(id="OCPBUGS-ORDINARY")
        included_bug = Mock(id="OCPBUGS-INCLUDE")
        runtime = Mock(
            debug=False,
            assembly="4.22.22",
            group="openshift-4.22",
        )
        runtime.get_releases_config.return_value = {}
        bug_tracker = Mock(type="jira")
        bug_tracker.get_bugs.return_value = [included_bug]
        sweep = FindBugsSweep(art_managed_trackers_only=False)
        sweep.search = Mock(return_value=[review_bug, ordinary_bug])
        evaluation = PluginEvaluation(
            claimed_issue_keys=frozenset({"OCPBUGS-REVIEW", "OCPBUGS-INCLUDE"}),
            included_issue_keys=frozenset({"OCPBUGS-INCLUDE"}),
            results=(_plugin_result("rhcos-rpm"),),
        )
        plugin_results = []

        with (
            patch.object(sweep_cli, "assembly_targeted_fixes_only", return_value=False),
            patch.object(sweep_cli, "get_assembly_bug_ids", return_value=(set(), set())),
            patch.object(sweep_cli, "_run_evidence_plugins", return_value=evaluation),
            patch.object(sweep_cli, "get_sweep_cutoff_timestamp", new_callable=AsyncMock, return_value=123),
        ):
            bug_tracker.filter_bugs_by_cutoff_event.return_value = []
            bugs = await sweep_cli.get_bugs_sweep(
                runtime,
                sweep,
                bug_tracker,
                filter_attached_bugs=False,
                evidence_plugins=("rhcos-rpm",),
                plugin_results=plugin_results,
            )

        self.assertEqual(bugs, [included_bug])
        sweep.search.assert_called_once_with(bug_tracker_obj=bug_tracker, verbose=False)
        bug_tracker.get_bugs.assert_called_once_with(["OCPBUGS-INCLUDE"], verbose=False)
        self.assertEqual(plugin_results, list(evaluation.results))


class TestFindBugsJson(unittest.IsolatedAsyncioTestCase):
    """Keep plugin JSON focused on actionable decisions."""

    async def test_json_output_includes_only_include_and_review_decisions(self):
        result = PluginResult(
            "rhcos-rpm",
            (
                PluginDecision("OCPBUGS-1", PluginDisposition.INCLUDE, "ready"),
                PluginDecision("OCPBUGS-2", PluginDisposition.SUPPRESS, "no evidence"),
                PluginDecision("OCPBUGS-3", PluginDisposition.REVIEW, "inspect"),
            ),
            True,
        )
        runtime = Mock()
        cli = FindBugsCli(runtime, False, "json", False, False, False, evidence_plugins=("rhcos-rpm",))

        async def find_bugs():
            cli.plugin_results = (result,)
            return {"rpm": set()}

        with (
            patch.object(cli, "find_bugs", side_effect=find_bugs),
            patch("elliottlib.cli.find_bugs_cli.click.echo") as echo,
        ):
            await cli.run()

        output = json.loads(echo.call_args.args[0])
        self.assertEqual(set(output), {"bugs_by_kind", "evidence_plugins"})
        self.assertNotIn("report", output)
        self.assertEqual(
            output["evidence_plugins"]["rhcos-rpm"],
            [
                {"issue_key": "OCPBUGS-1", "disposition": "include", "reason": "ready"},
                {"issue_key": "OCPBUGS-3", "disposition": "review", "reason": "inspect"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
