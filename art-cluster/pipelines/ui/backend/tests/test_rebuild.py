import unittest

from pipeline_ui.rebuild import InvalidRun, build_run, parameter_form

PIPELINE = {
    "metadata": {"namespace": "art-logging-tenant", "name": "release-from-fbc", "resourceVersion": "42"},
    "spec": {
        "params": [
            {"name": "assembly", "type": "string", "default": "stream"},
            {"name": "force", "type": "string", "default": "false"},
            {"name": "new-required", "type": "array"},
        ],
        "workspaces": [],
    },
}
OLD_RUN = {
    "metadata": {"namespace": "art-logging-tenant", "name": "release-from-fbc-abc", "uid": "old-uid"},
    "spec": {
        "pipelineRef": {"name": "release-from-fbc"},
        "params": [{"name": "assembly", "value": "4.20"}, {"name": "removed", "value": "old"}],
        "taskRunTemplate": {"serviceAccountName": "pipeline"},
        "timeouts": {"pipeline": "1h"},
    },
}


class RebuildTests(unittest.TestCase):
    def test_form_merges_run_values_and_current_defaults(self):
        form = parameter_form(PIPELINE, OLD_RUN)
        values = {item["name"]: (item["value"], item["source"]) for item in form["parameters"]}
        self.assertEqual(values["assembly"], ("4.20", "run"))
        self.assertEqual(values["force"], ("false", "default"))
        self.assertEqual(values["new-required"], (None, "required"))
        self.assertEqual(form["removedParameters"], ["removed"])

    def test_rebuild_uses_current_pipeline_and_preserves_execution_settings(self):
        run = build_run(
            PIPELINE,
            {"assembly": "4.21", "force": "true", "new-required": ["x"]},
            source_run=OLD_RUN,
        )
        self.assertEqual(run["spec"]["pipelineRef"], {"name": "release-from-fbc"})
        self.assertEqual(run["spec"]["taskRunTemplate"], {"serviceAccountName": "pipeline"})
        self.assertEqual(run["spec"]["timeouts"], {"pipeline": "6h"})
        self.assertNotIn("status", run)
        self.assertNotIn("removed", {item["name"] for item in run["spec"]["params"]})

    def test_required_and_type_errors(self):
        with self.assertRaisesRegex(InvalidRun, "new-required is required"):
            build_run(PIPELINE, {"assembly": "4.21"})
        with self.assertRaisesRegex(InvalidRun, "array of strings"):
            build_run(PIPELINE, {"new-required": "x"})

    def test_unknown_parameter_is_rejected(self):
        with self.assertRaisesRegex(InvalidRun, "no longer defined"):
            build_run(PIPELINE, {"new-required": [], "removed": "x"}, source_run=OLD_RUN)


if __name__ == "__main__":
    unittest.main()
