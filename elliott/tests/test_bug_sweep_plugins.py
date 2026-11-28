"""Unit tests for evidence-plugin contracts and bug-sweep integration."""

import unittest
from unittest.mock import Mock, patch

from elliottlib.bug_sweep_plugins.contracts import (
    PluginContext,
    PluginDecision,
    PluginDisposition,
    PluginResult,
)
from elliottlib.bug_sweep_plugins.registry import available_plugin_names, create_plugins
from elliottlib.bug_sweep_plugins.runner import evaluate_plugins
from elliottlib.bzutil import Bug, JIRABugTracker
from elliottlib.exceptions import ElliottFatalError
from elliottlib.rhcos_rpm_evidence.models import TrackerDisposition


class _TestBug(Bug):
    """Minimal bug object for plugin contract tests."""

    def __init__(self, issue_key: str):
        self._issue_key = issue_key

    @property
    def id(self) -> str:
        """Return the test issue key."""
        return self._issue_key


def _plugin_decision(issue_key: str, disposition: PluginDisposition, reason: str) -> PluginDecision:
    """Create a plugin decision with a bug object."""
    return PluginDecision(_TestBug(issue_key), disposition, reason)


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

    def test_runner_returns_validated_plugin_results(self):
        result = _plugin_result(
            decisions=(
                _plugin_decision("OCPBUGS-1", PluginDisposition.INCLUDE, "eligible"),
                _plugin_decision("OCPBUGS-2", PluginDisposition.SUPPRESS, "no evidence"),
                _plugin_decision("OCPBUGS-3", PluginDisposition.REVIEW, "needs human"),
            )
        )
        plugin = _Plugin("test", result)

        results = evaluate_plugins((plugin,), Mock())

        self.assertEqual(results, (result,))

    def test_runner_rejects_incomplete_search_duplicate_keys_and_multiple_owners(self):
        incomplete = _Plugin("partial", _plugin_result(name="partial", complete=False))
        with self.assertRaises(ElliottFatalError):
            evaluate_plugins((incomplete,), Mock())

        repeated = _plugin_decision("OCPBUGS-1", PluginDisposition.REVIEW, "review")
        duplicate_key = _Plugin("duplicate", _plugin_result(name="duplicate", decisions=(repeated, repeated)))
        with self.assertRaisesRegex(ValueError, "duplicate issue decisions"):
            evaluate_plugins((duplicate_key,), Mock())

        shared_issue = _plugin_decision("OCPBUGS-1", PluginDisposition.SUPPRESS, "claimed")
        first = _Plugin("first", _plugin_result(name="first", decisions=(shared_issue,)))
        second = _Plugin("second", _plugin_result(name="second", decisions=(shared_issue,)))
        with self.assertRaisesRegex(ValueError, "claimed by multiple"):
            evaluate_plugins((first, second), Mock())

    def test_runner_rejects_malformed_plugin_outputs(self):
        wrong_name = _Plugin("expected", _plugin_result(name="other"))
        with self.assertRaisesRegex(ValueError, "returned a result named"):
            evaluate_plugins((wrong_name,), Mock())

        empty_key = _Plugin(
            "empty", _plugin_result(name="empty", decisions=(_plugin_decision("", PluginDisposition.REVIEW, "x"),))
        )
        with self.assertRaisesRegex(ValueError, "empty issue key"):
            evaluate_plugins((empty_key,), Mock())

        bad_disposition = _Plugin(
            "bad", _plugin_result(name="bad", decisions=(_plugin_decision("OCPBUGS-1", "include", "x"),))
        )
        with self.assertRaisesRegex(ValueError, "invalid decision disposition"):
            evaluate_plugins((bad_disposition,), Mock())

        duplicate_plugin = _Plugin("same", _plugin_result(name="same"))
        with self.assertRaisesRegex(ValueError, "selected more than once"):
            evaluate_plugins((duplicate_plugin, duplicate_plugin), Mock())

    def test_registry_lists_and_validates_explicit_plugins(self):
        self.assertEqual(available_plugin_names(), ("jql_sweep", "rhcos-rpm"))
        self.assertEqual(create_plugins(()), ())
        with self.assertRaisesRegex(ValueError, "Unknown bug-sweep plugin"):
            create_plugins(({"name": "missing"},))
        with self.assertRaisesRegex(ValueError, "selected only once"):
            create_plugins(({"name": "missing"}, {"name": "missing"}))

    def test_rhcos_plugin_returns_discovered_issue_objects(self):
        issue = _TestBug("OCPBUGS-1")
        tracker = Mock(spec=JIRABugTracker)
        context = PluginContext(Mock(assembly="4.22.1"), tracker, frozenset())
        finding = Mock(issue_key=issue.id, disposition=TrackerDisposition.READY_TO_VERIFY, reason="evidence")
        report = Mock(
            findings=(finding,),
            tracker_search_complete=True,
            warnings=("BREW_CHANGELOG_LOOKUP_FAILED: Changelog lookup failed for build private-package-1-1",),
        )
        with (
            patch("elliottlib.bug_sweep_plugins.rhcos_rpm.JiraRHCOSTrackerRepository") as repository,
            patch("elliottlib.bug_sweep_plugins.rhcos_rpm.RHCOSRpmEvidenceService") as service,
        ):
            repository.return_value.issues_by_key = {issue.id: issue}
            service.return_value.run.return_value = report
            results = evaluate_plugins(create_plugins(({"name": "rhcos-rpm"},)), context)

        self.assertIs(results[0].decisions[0].issue, issue)
        self.assertEqual(results[0].decisions[0].disposition, PluginDisposition.INCLUDE)
        self.assertEqual(results[0].warnings, ("BREW_CHANGELOG_LOOKUP_FAILED",))


if __name__ == "__main__":
    unittest.main()
