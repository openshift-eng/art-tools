"""Unit tests for evidence-plugin contracts and bug-sweep integration."""

import unittest
from unittest.mock import Mock

from elliottlib.bug_sweep_plugins.contracts import (
    PluginContext,
    PluginDecision,
    PluginDisposition,
    PluginResult,
)
from elliottlib.bug_sweep_plugins.registry import available_plugin_names, create_plugins
from elliottlib.bug_sweep_plugins.runner import evaluate_plugins
from elliottlib.exceptions import ElliottFatalError


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
                PluginDecision("OCPBUGS-1", PluginDisposition.INCLUDE, "eligible"),
                PluginDecision("OCPBUGS-2", PluginDisposition.SUPPRESS, "no evidence"),
                PluginDecision("OCPBUGS-3", PluginDisposition.REVIEW, "needs human"),
            )
        )
        plugin = _Plugin("test", result)

        results = evaluate_plugins((plugin,), Mock())

        self.assertEqual(results, (result,))

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
        self.assertEqual(available_plugin_names(), ())
        self.assertEqual(create_plugins(()), ())
        with self.assertRaisesRegex(ValueError, "Unknown bug-sweep plugin"):
            create_plugins(({"name": "missing"},))
        with self.assertRaisesRegex(ValueError, "selected only once"):
            create_plugins(({"name": "missing"}, {"name": "missing"}))


if __name__ == "__main__":
    unittest.main()
