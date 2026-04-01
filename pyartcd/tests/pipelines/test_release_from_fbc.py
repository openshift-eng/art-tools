import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import click
from elliottlib.shipment_model import (
    ComponentSource,
    GitSource,
    ReleaseNotes,
    Snapshot,
    SnapshotComponent,
    SnapshotSpec,
)
from pyartcd.pipelines.release_from_fbc import ReleaseFromFbcPipeline, _normalize_release_date


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


class TestReleaseFromFbcPipeline(unittest.TestCase):
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
    def test_generate_release_notes_calls_elliott(self, mock_cmd_assert):
        """generate_release_notes should call elliott and parse YAML output."""
        pipeline = self._make_pipeline(jira_bugs=["OADP-1111", "OADP-2222"])

        yaml_output = "type: RHBA\nissues:\n  fixed:\n  - id: OADP-1111\n    source: issues.redhat.com\n  - id: OADP-2222\n    source: issues.redhat.com\n"
        mock_cmd_assert.return_value = (yaml_output, "")

        result = pipeline.generate_release_notes()

        self.assertIsNotNone(result)
        self.assertEqual(result.type, "RHBA")
        self.assertEqual(len(result.issues.fixed), 2)

        # Verify the command includes process-release-from-fbc-bugs
        call_args = mock_cmd_assert.call_args[0][0]
        self.assertIn("process-release-from-fbc-bugs", call_args)
        self.assertIn("--jira-bugs=OADP-1111,OADP-2222", call_args)

    def test_generate_release_notes_returns_none_without_bugs(self):
        """generate_release_notes should return None if no jira_bugs."""
        pipeline = self._make_pipeline(jira_bugs=None)
        result = pipeline.generate_release_notes()
        self.assertIsNone(result)


class TestNormalizeReleaseDate(unittest.TestCase):
    def test_abbreviated_month_passthrough(self):
        self.assertEqual(_normalize_release_date("2026-Mar-31"), "2026-Mar-31")

    def test_numeric_month_converted(self):
        self.assertEqual(_normalize_release_date("2026-03-31"), "2026-Mar-31")

    def test_numeric_month_january(self):
        self.assertEqual(_normalize_release_date("2026-01-15"), "2026-Jan-15")

    def test_abbreviated_month_december(self):
        self.assertEqual(_normalize_release_date("2026-Dec-25"), "2026-Dec-25")

    def test_whitespace_trimmed(self):
        self.assertEqual(_normalize_release_date("  2026-Mar-31  "), "2026-Mar-31")

    def test_invalid_format_raises(self):
        with self.assertRaises(click.ClickException) as ctx:
            _normalize_release_date("March 31, 2026")
        self.assertIn("Invalid date format", str(ctx.exception))

    def test_empty_string_raises(self):
        with self.assertRaises(click.ClickException):
            _normalize_release_date("")

    def test_garbage_raises(self):
        with self.assertRaises(click.ClickException):
            _normalize_release_date("not-a-date")


class TestTargetReleaseDate(unittest.TestCase):
    def _make_pipeline(self, target_release_date=None):
        runtime = MagicMock()
        runtime.dry_run = True
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {"gitlab_url": "https://gitlab.example.com"}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="oadp-1.4",
            assembly="1.4.8",
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            create_mr=True,
            target_release_date=target_release_date,
        )
        pipeline.product = "oadp"
        return pipeline

    def test_target_release_date_stored(self):
        pipeline = self._make_pipeline(target_release_date="2026-Mar-31")
        self.assertEqual(pipeline.target_release_date, "2026-Mar-31")

    def test_target_release_date_default_none(self):
        pipeline = self._make_pipeline()
        self.assertIsNone(pipeline.target_release_date)


class TestLoadMrApproversFromGroupConfig(unittest.TestCase):
    """Tests for _load_mr_approvers_from_group_config."""

    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="logging-6.5",
            assembly="6.5.1",
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            create_mr=False,
        )
        return pipeline

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_valid_dict_returned(self, mock_cmd):
        mock_cmd.return_value = (0, "QE:\n- asdas1\n- rjohnson\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.get_event_loop().run_until_complete(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {"QE": ["asdas1", "rjohnson"]})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_non_dict_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "- asdas1\n- rjohnson\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.get_event_loop().run_until_complete(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_scalar_string_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "just-a-string\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.get_event_loop().run_until_complete(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_none_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "None", "")
        pipeline = self._make_pipeline()
        result = asyncio.get_event_loop().run_until_complete(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_null_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "null", "")
        pipeline = self._make_pipeline()
        result = asyncio.get_event_loop().run_until_complete(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_empty_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "  \n", "")
        pipeline = self._make_pipeline()
        result = asyncio.get_event_loop().run_until_complete(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_exception_returns_empty(self, mock_cmd):
        mock_cmd.side_effect = RuntimeError("doozer failed")
        pipeline = self._make_pipeline()
        result = asyncio.get_event_loop().run_until_complete(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})


class TestCreateShipmentMrApprovalRules(unittest.TestCase):
    """Tests for approval-rule handling inside create_shipment_mr."""

    def _make_pipeline(self, dry_run=False):
        runtime = MagicMock()
        runtime.dry_run = dry_run
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {"gitlab_url": "https://gitlab.example.com"}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="logging-6.5",
            assembly="6.5.1",
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            create_mr=True,
        )
        pipeline.product = "logging"
        pipeline.shipment_data_repo = AsyncMock()
        pipeline.shipment_data_repo_push_url = "https://gitlab.example.com/user/ocp-shipment-data.git"
        pipeline.shipment_data_repo_pull_url = "https://gitlab.example.com/org/ocp-shipment-data.git"
        return pipeline

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_dry_run_skips_set_mr_approval_rules(self, mock_cmd):
        """In dry-run, approval rules are logged but set_mr_approval_rules is not called."""
        mock_cmd.return_value = (0, "QE:\n- asdas1\n", "")

        pipeline = self._make_pipeline(dry_run=True)
        pipeline.update_shipment_data = AsyncMock(return_value=True)
        pipeline._get_gitlab_project = MagicMock()

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        mr_url = asyncio.get_event_loop().run_until_complete(pipeline.create_shipment_mr({}, env="prod"))

        mock_gitlab.set_mr_approval_rules.assert_not_called()
        self.assertIn("placeholder", mr_url)

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_non_dry_run_calls_set_mr_approval_rules(self, mock_cmd):
        """When not dry-run, set_mr_approval_rules is called with the approvers config."""
        mock_cmd.return_value = (0, "QE:\n- asdas1\n- rjohnson\n", "")

        pipeline = self._make_pipeline(dry_run=False)
        pipeline.update_shipment_data = AsyncMock(return_value=True)

        mock_source_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/repo/-/merge_requests/1"
        mock_source_project.mergerequests.create.return_value = mock_mr
        pipeline._get_gitlab_project = MagicMock(return_value=mock_source_project)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        mr_url = asyncio.get_event_loop().run_until_complete(pipeline.create_shipment_mr({}, env="prod"))

        mock_gitlab.set_mr_approval_rules.assert_awaited_once_with(
            "https://gitlab.example.com/org/repo/-/merge_requests/1",
            {"QE": ["asdas1", "rjohnson"]},
        )
        self.assertEqual(mr_url, mock_mr.web_url)

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_no_approvers_skips_call(self, mock_cmd):
        """When mr_approvers is empty, set_mr_approval_rules is not called."""
        mock_cmd.return_value = (0, "None", "")

        pipeline = self._make_pipeline(dry_run=False)
        pipeline.update_shipment_data = AsyncMock(return_value=True)

        mock_source_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/repo/-/merge_requests/2"
        mock_source_project.mergerequests.create.return_value = mock_mr
        pipeline._get_gitlab_project = MagicMock(return_value=mock_source_project)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.get_event_loop().run_until_complete(pipeline.create_shipment_mr({}, env="prod"))

        mock_gitlab.set_mr_approval_rules.assert_not_called()

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_approval_rules_exception_logged_not_raised(self, mock_cmd):
        """If set_mr_approval_rules raises, the exception is caught and logged."""
        mock_cmd.return_value = (0, "QE:\n- asdas1\n", "")

        pipeline = self._make_pipeline(dry_run=False)
        pipeline.update_shipment_data = AsyncMock(return_value=True)

        mock_source_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/repo/-/merge_requests/3"
        mock_source_project.mergerequests.create.return_value = mock_mr
        pipeline._get_gitlab_project = MagicMock(return_value=mock_source_project)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock(side_effect=RuntimeError("API error"))
        pipeline.__dict__["_gitlab"] = mock_gitlab

        mr_url = asyncio.get_event_loop().run_until_complete(pipeline.create_shipment_mr({}, env="prod"))

        self.assertEqual(mr_url, mock_mr.web_url)


class TestSetMrApprovalRules(unittest.TestCase):
    """Tests for GitLabClient.set_mr_approval_rules."""

    def _make_client(self, dry_run=False):
        with patch("artcommonlib.gitlab.gitlab.Gitlab") as mock_gl_class:
            mock_instance = MagicMock()
            mock_gl_class.return_value = mock_instance
            from artcommonlib.gitlab import GitLabClient

            client = GitLabClient("https://gitlab.example.com", "fake-token", dry_run=dry_run)
        return client

    def test_empty_config_returns_early(self):
        client = self._make_client()
        client.get_mr_from_url = MagicMock()
        asyncio.get_event_loop().run_until_complete(
            client.set_mr_approval_rules("https://gitlab.example.com/a/b/-/merge_requests/1", {})
        )
        client.get_mr_from_url.assert_not_called()

    def test_empty_url_returns_early(self):
        client = self._make_client()
        client.get_mr_from_url = MagicMock()
        asyncio.get_event_loop().run_until_complete(client.set_mr_approval_rules("", {"QE": ["user1"]}))
        client.get_mr_from_url.assert_not_called()

    def test_dry_run_does_not_mutate(self):
        client = self._make_client(dry_run=True)
        client.get_mr_from_url = MagicMock()

        asyncio.get_event_loop().run_until_complete(
            client.set_mr_approval_rules(
                "https://gitlab.example.com/a/b/-/merge_requests/1",
                {"QE": ["user1"]},
            )
        )

        client.get_mr_from_url.assert_not_called()

    def test_deletes_non_art_and_creates_new(self):
        client = self._make_client(dry_run=False)

        mock_mr = MagicMock()
        art_rule = MagicMock()
        art_rule.name = "ART"
        art_rule.id = 100
        ert_rule = MagicMock()
        ert_rule.name = "ERT"
        ert_rule.id = 200
        docs_rule = MagicMock()
        docs_rule.name = "Docs"
        docs_rule.id = 300
        mock_mr.approval_rules.list.return_value = [art_rule, ert_rule, docs_rule]
        client.get_mr_from_url = MagicMock(return_value=mock_mr)

        mock_user = MagicMock()
        mock_user.id = 15399
        client._client.users.list = MagicMock(return_value=[mock_user])

        asyncio.get_event_loop().run_until_complete(
            client.set_mr_approval_rules(
                "https://gitlab.example.com/a/b/-/merge_requests/1",
                {"QE": ["asdas1"]},
            )
        )

        art_rule.delete.assert_not_called()
        ert_rule.delete.assert_called_once()
        docs_rule.delete.assert_called_once()
        mock_mr.approval_rules.create.assert_called_once_with(
            {"name": "QE", "approvals_required": 1, "user_ids": [15399]}
        )

    def test_skips_rule_when_no_users_resolved(self):
        client = self._make_client(dry_run=False)

        mock_mr = MagicMock()
        mock_mr.approval_rules.list.return_value = []
        client.get_mr_from_url = MagicMock(return_value=mock_mr)
        client._client.users.list = MagicMock(return_value=[])

        asyncio.get_event_loop().run_until_complete(
            client.set_mr_approval_rules(
                "https://gitlab.example.com/a/b/-/merge_requests/1",
                {"QE": ["nonexistent_user"]},
            )
        )

        mock_mr.approval_rules.create.assert_not_called()

    def test_multiple_groups_created(self):
        client = self._make_client(dry_run=False)

        mock_mr = MagicMock()
        mock_mr.approval_rules.list.return_value = []
        client.get_mr_from_url = MagicMock(return_value=mock_mr)

        user_map = {
            "user1": MagicMock(id=1),
            "user2": MagicMock(id=2),
            "user3": MagicMock(id=3),
        }
        client._client.users.list = MagicMock(side_effect=lambda username: [user_map[username]])

        asyncio.get_event_loop().run_until_complete(
            client.set_mr_approval_rules(
                "https://gitlab.example.com/a/b/-/merge_requests/1",
                {"QE": ["user1", "user2"], "Dev": ["user3"]},
            )
        )

        self.assertEqual(mock_mr.approval_rules.create.call_count, 2)


if __name__ == "__main__":
    unittest.main()
