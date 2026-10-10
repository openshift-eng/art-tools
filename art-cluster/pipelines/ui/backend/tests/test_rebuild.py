import unittest
from copy import deepcopy

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
        "timeouts": {"pipeline": "3h", "tasks": "2h", "finally": "15m"},
    },
}
PROMOTE_PIPELINE = {
    "metadata": {"namespace": "art-openshift-tenant", "name": "promote-assembly", "resourceVersion": "42"},
    "spec": {
        "params": [
            {"name": "version", "type": "string"},
            {"name": "assembly", "type": "string", "default": "stream"},
        ],
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

    def test_rebuild_preserves_execution_settings_and_uses_cluster_timeout_defaults(self):
        run = build_run(
            PIPELINE,
            {"assembly": "4.21", "force": "true", "new-required": ["x"]},
            source_run=OLD_RUN,
        )
        self.assertEqual(run["spec"]["pipelineRef"], {"name": "release-from-fbc"})
        self.assertEqual(run["metadata"]["generateName"], "release-from-fbc-")
        self.assertEqual(run["spec"]["taskRunTemplate"], {"serviceAccountName": "pipeline"})
        self.assertNotIn("timeouts", run["spec"])
        self.assertEqual(
            run["metadata"]["annotations"],
            {
                "art.openshift.io/rebuilt-from": "old-uid",
                "art.openshift.io/rebuilt-from-name": "release-from-fbc-abc",
            },
        )
        self.assertEqual(OLD_RUN["spec"]["timeouts"], {"pipeline": "3h", "tasks": "2h", "finally": "15m"})
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


class PromoteRunNamingTests(unittest.TestCase):
    def test_start_includes_assembly_and_preserves_pipeline_parameters(self):
        values = {"version": "4.22", "assembly": "4.22.18"}
        run = build_run(PROMOTE_PIPELINE, values)
        self.assertEqual(run["metadata"]["generateName"], "promote-assembly-4.22.18-")
        self.assertEqual(run["spec"]["pipelineRef"], {"name": "promote-assembly"})
        self.assertEqual({param["name"]: param["value"] for param in run["spec"]["params"]}, values)
        self.assertNotIn("name", run["metadata"])

    def test_start_uses_default_assembly(self):
        run = build_run(PROMOTE_PIPELINE, {"version": "4.22"})
        self.assertEqual(run["metadata"]["generateName"], "promote-assembly-stream-")
        self.assertIn({"name": "assembly", "value": "stream"}, run["spec"]["params"])

    def test_rebuild_uses_edited_assembly_and_preserves_source_annotations(self):
        source = {
            "metadata": {"namespace": "art-openshift-tenant", "name": "promote-assembly-ql58s", "uid": "old-uid"},
            "spec": {
                "pipelineRef": {"name": "promote-assembly"},
                "params": [{"name": "version", "value": "4.22"}, {"name": "assembly", "value": "4.22.17"}],
            },
        }
        original = deepcopy(source)
        run = build_run(PROMOTE_PIPELINE, {"version": "4.22", "assembly": "4.22.18"}, source_run=source)
        self.assertEqual(run["metadata"]["generateName"], "promote-assembly-4.22.18-")
        self.assertEqual(
            run["metadata"]["annotations"],
            {
                "art.openshift.io/rebuilt-from": "old-uid",
                "art.openshift.io/rebuilt-from-name": "promote-assembly-ql58s",
            },
        )
        self.assertEqual(source, original)

    def test_normalizes_only_the_name_fragment(self):
        cases = (
            ("4.22.18+ART_Test", "4.22.18-art-test"),
            (" 4.22.18 ", "4.22.18"),
            ("...--4..-22-.--18---...", "4.22.18"),
            ("_RC_/Test", "rc-test"),
        )
        for assembly, fragment in cases:
            with self.subTest(assembly=assembly):
                run = build_run(PROMOTE_PIPELINE, {"version": "4.22", "assembly": assembly})
                self.assertEqual(run["metadata"]["generateName"], f"promote-assembly-{fragment}-")
                self.assertIn({"name": "assembly", "value": assembly}, run["spec"]["params"])

    def test_empty_name_fragment_uses_pipeline_prefix(self):
        for assembly in ("", "...", "--", " /_ "):
            with self.subTest(assembly=assembly):
                run = build_run(PROMOTE_PIPELINE, {"version": "4.22", "assembly": assembly})
                self.assertEqual(run["metadata"]["generateName"], "promote-assembly-")
                self.assertIn({"name": "assembly", "value": assembly}, run["spec"]["params"])

    def test_missing_assembly_definition_uses_pipeline_prefix(self):
        pipeline = deepcopy(PROMOTE_PIPELINE)
        pipeline["spec"]["params"] = [{"name": "version", "type": "string"}]
        run = build_run(pipeline, {"version": "4.22"})
        self.assertEqual(run["metadata"]["generateName"], "promote-assembly-")

    def test_long_assembly_is_truncated_without_a_trailing_separator(self):
        for assembly, fragment in (
            ("a" * 100, "a" * 31),
            ("a" * 30 + ".beta", "a" * 30),
            ("a" * 30 + "-beta", "a" * 30),
        ):
            with self.subTest(assembly=assembly):
                run = build_run(PROMOTE_PIPELINE, {"version": "4.22", "assembly": assembly})
                prefix = run["metadata"]["generateName"]
                self.assertEqual(prefix, f"promote-assembly-{fragment}-")
                self.assertLessEqual(len(prefix), 49)
                self.assertIn({"name": "assembly", "value": assembly}, run["spec"]["params"])

    def test_other_pipeline_names_are_unchanged(self):
        run = build_run(PIPELINE, {"assembly": "4.22.18", "new-required": []})
        self.assertEqual(run["metadata"]["generateName"], "release-from-fbc-")


if __name__ == "__main__":
    unittest.main()
