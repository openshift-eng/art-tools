import json
import unittest

from pipeline_ui.components import PREFIX, REPORT, component_details


def run_with(stages, *, terminal=False, resources=None, marker=None, **fields):
    entry = {
        "version": 1,
        "type": "bundle",
        "name": "operator",
        "displayName": "operator-bundle",
        "stages": stages,
        "resources": resources or [],
        **fields,
    }
    return {
        "metadata": {
            "annotations": {
                PREFIX + "1": json.dumps(entry),
                REPORT: json.dumps(marker or {"version": 1, "partial": False}),
            }
        },
        "status": {"completionTime": "now"} if terminal else {},
    }


def stage(status, category="build", required=True):
    return {"status": status, "category": category, "required": required}


class ComponentTests(unittest.TestCase):
    def test_old_runs_have_no_report(self):
        result = component_details({})
        self.assertEqual(result["components"], [])
        self.assertFalse(result["componentReporting"]["available"])

    def test_required_release_failure_preserves_successful_build(self):
        result = component_details(
            run_with({"build": stage("Succeeded"), "release": stage("Failed", "release")}, fbcExcluded=True)
        )
        component = result["components"][0]
        self.assertEqual(component["status"], "Failed")
        self.assertEqual(component["summaries"], {"build": "Succeeded", "check": "—", "release": "Failed"})
        self.assertTrue(component["fbcExcluded"])

    def test_optional_release_failure_keeps_overall_success(self):
        result = component_details(
            run_with({"build": stage("Succeeded"), "release": stage("Failed", "release", False)})
        )
        component = result["components"][0]
        self.assertEqual(component["status"], "Succeeded")
        self.assertTrue(component["hasFailures"])

    def test_optional_test_failure_keeps_overall_success(self):
        component = component_details(run_with({"build": stage("Succeeded"), "ITS": stage("Failed", "check", False)}))[
            "components"
        ][0]
        self.assertEqual(component["status"], "Succeeded")
        self.assertEqual(component["summaries"]["check"], "Failed")

    def test_active_stage_stays_running(self):
        result = component_details(run_with({"build": stage("Running")}))
        self.assertEqual(result["components"][0]["status"], "Running")
        self.assertFalse(result["componentReporting"]["partial"])

    def test_unfinished_stage_is_unknown_after_art_completion(self):
        result = component_details(
            run_with({"build": stage("Succeeded"), "release": stage("Running", "release")}, terminal=True)
        )
        self.assertEqual(result["components"][0]["status"], "Unknown")
        self.assertTrue(result["componentReporting"]["partial"])

    def test_terminal_condition_without_completion_time_is_terminal(self):
        run = run_with({"build": stage("Pending")})
        run["status"] = {"conditions": [{"type": "Succeeded", "status": "False", "reason": "Cancelled"}]}
        self.assertEqual(component_details(run)["components"][0]["status"], "Unknown")

    def test_earlier_failed_attempt_is_still_filterable(self):
        resources = [
            {"name": "attempt-1", "status": "Failed", "url": "https://konflux.example/run"},
            {"name": "attempt-2", "status": "Succeeded", "url": "https://konflux.example/run-2"},
        ]
        component = component_details(run_with({"build": stage("Succeeded")}, resources=resources))["components"][0]
        self.assertEqual(component["status"], "Succeeded")
        self.assertTrue(component["hasFailures"])
        self.assertEqual(len(component["resources"]), 2)

    def test_reused_and_skipped_builds(self):
        for outcome in ("Reused", "Skipped"):
            component = component_details(
                run_with({"build": stage(outcome), "preparation": stage("Succeeded", "preparation")})
            )["components"][0]
            self.assertEqual(component["status"], outcome)

    def test_dry_run(self):
        component = component_details(run_with({"build": stage("DryRun")}))["components"][0]
        self.assertEqual(component["status"], "DryRun")

    def test_dry_run_stays_distinct_from_successful_rebase(self):
        component = component_details(
            run_with({"rebase": stage("Succeeded", "preparation"), "build": stage("DryRun")}, dryRun=True)
        )["components"][0]
        self.assertEqual(component["status"], "DryRun")

    def test_terminal_unknown_is_incomplete(self):
        result = component_details(run_with({"build": stage("Unknown")}, terminal=True))
        self.assertTrue(result["componentReporting"]["partial"])
        self.assertFalse(result["componentReporting"]["complete"])

    def test_malformed_marker_is_incomplete(self):
        run = run_with({"build": stage("Succeeded")})
        run["metadata"]["annotations"][REPORT] = "not JSON"
        self.assertTrue(component_details(run)["componentReporting"]["partial"])

    def test_bad_annotation_does_not_hide_valid_components(self):
        run = run_with({"build": stage("Succeeded")})
        run["metadata"]["annotations"][PREFIX + "bad"] = "invalid JSON"
        result = component_details(run)
        self.assertEqual(len(result["components"]), 1)
        self.assertTrue(result["componentReporting"]["partial"])

    def test_unsupported_schema_is_incomplete(self):
        run = run_with({"build": stage("Succeeded")})
        run["metadata"]["annotations"][PREFIX + "1"] = json.dumps({"version": 2})
        result = component_details(run)
        self.assertEqual(result["components"], [])
        self.assertTrue(result["componentReporting"]["partial"])

    def test_invalid_resource_urls_not_returned_as_links(self):
        component = component_details(
            run_with(
                {"build": stage("Succeeded")},
                resources=[
                    {"name": "bad", "url": "javascript:alert(1)"},
                    {"name": "good", "url": "https://konflux.example/run"},
                ],
            )
        )["components"][0]
        self.assertIsNone(component["resources"][0]["url"])
        self.assertEqual(component["resources"][1]["url"], "https://konflux.example/run")

    def test_capacity_marker_visible(self):
        result = component_details(
            run_with(
                {"build": stage("Succeeded")},
                marker={"version": 1, "partial": True, "reason": "Annotation capacity exceeded"},
            )
        )
        self.assertTrue(result["componentReporting"]["partial"])
        self.assertEqual(result["componentReporting"]["reason"], "Annotation capacity exceeded")
