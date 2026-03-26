import unittest
from unittest.mock import MagicMock, patch

from elliottlib.shipment_model import (
    ComponentSource,
    GitSource,
    ReleaseNotes,
    Snapshot,
    SnapshotComponent,
    SnapshotSpec,
)
from pyartcd.pipelines.release_from_fbc import ReleaseFromFbcPipeline


def _make_snapshot(app="oadp-1-4"):
    return Snapshot(
        spec=SnapshotSpec(
            application=app,
            components=[
                SnapshotComponent(
                    name="oadp-1-4-oadp-velero",
                    containerImage="quay.io/test/image@sha256:abc",
                    source=ComponentSource(git=GitSource(url="https://github.com/test/repo", revision="abc123")),
                )
            ],
        ),
        nvrs=["oadp-velero-container-1.4.5-1"],
    )


class TestReleaseFromFbcPipeline(unittest.IsolatedAsyncioTestCase):
    def _make_pipeline(self, jira_bugs=None):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="oadp-1.4",
            assembly="1.4.5",
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            create_mr=False,
            jira_bugs=jira_bugs,
        )
        pipeline.product = "oadp"
        return pipeline

    def test_jira_bugs_stored(self):
        """jira_bugs parameter is stored on the pipeline."""
        pipeline = self._make_pipeline(jira_bugs=["OADP-1111", "OADP-2222"])
        self.assertEqual(pipeline.jira_bugs, ["OADP-1111", "OADP-2222"])

    def test_jira_bugs_default_none(self):
        """jira_bugs defaults to None when not provided."""
        pipeline = self._make_pipeline()
        self.assertIsNone(pipeline.jira_bugs)

    def test_create_shipment_config_image_with_release_notes(self):
        """create_shipment_config should include release_notes in data for image kind."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        release_notes = ReleaseNotes(type="RHBA")
        config = pipeline.create_shipment_config("image", snapshot, release_notes=release_notes)

        self.assertIsNotNone(config.shipment.data)
        self.assertEqual(config.shipment.data.releaseNotes.type, "RHBA")

    def test_create_shipment_config_image_without_release_notes(self):
        """create_shipment_config should produce no data for non-OpenShift image without release_notes."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        config = pipeline.create_shipment_config("image", snapshot)

        self.assertIsNone(config.shipment.data)

    def test_create_shipment_config_fbc_ignores_release_notes(self):
        """FBC shipments should never have data.releaseNotes, even if release_notes is provided."""
        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        release_notes = ReleaseNotes(type="RHSA")
        config = pipeline.create_shipment_config("fbc", snapshot, release_notes=release_notes)

        self.assertIsNone(config.shipment.data)

    def test_create_shipment_config_rhsa_release_notes(self):
        """RHSA release notes with CVE associations should be preserved."""
        from elliottlib.shipment_model import CveAssociation, Issue, Issues

        pipeline = self._make_pipeline()
        snapshot = _make_snapshot()

        release_notes = ReleaseNotes(
            type="RHSA",
            cves=[CveAssociation(key="CVE-2025-12345", component="oadp-1-4-oadp-velero")],
            issues=Issues(
                fixed=[
                    Issue(id="98765", source="bugzilla.redhat.com"),
                    Issue(id="OADP-7223", source="redhat.atlassian.net"),
                ]
            ),
        )
        config = pipeline.create_shipment_config("image", snapshot, release_notes=release_notes)

        rn = config.shipment.data.releaseNotes
        self.assertEqual(rn.type, "RHSA")
        self.assertEqual(len(rn.cves), 1)
        self.assertEqual(rn.cves[0].key, "CVE-2025-12345")
        self.assertEqual(len(rn.issues.fixed), 2)

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_assert")
    async def test_generate_release_notes_calls_elliott(self, mock_cmd_assert):
        """generate_release_notes should call elliott and parse YAML output."""
        pipeline = self._make_pipeline(jira_bugs=["OADP-1111", "OADP-2222"])

        yaml_output = "type: RHBA\nissues:\n  fixed:\n  - id: OADP-1111\n    source: issues.redhat.com\n  - id: OADP-2222\n    source: issues.redhat.com\n"
        mock_cmd_assert.return_value = (yaml_output, "")

        result = await pipeline.generate_release_notes()

        self.assertIsNotNone(result)
        self.assertEqual(result.type, "RHBA")
        self.assertEqual(len(result.issues.fixed), 2)

        # Verify the command includes process-release-from-fbc-bugs
        call_args = mock_cmd_assert.call_args[0][0]
        self.assertIn("process-release-from-fbc-bugs", call_args)
        self.assertIn("--jira-bugs=OADP-1111,OADP-2222", call_args)

    async def test_generate_release_notes_returns_none_without_bugs(self):
        """generate_release_notes should return None if no jira_bugs."""
        pipeline = self._make_pipeline(jira_bugs=None)
        result = await pipeline.generate_release_notes()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
