import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import click
from elliottlib.shipment_model import (
    ComponentSource,
    GitSource,
    Issue,
    Issues,
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
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {"QE": ["asdas1", "rjohnson"]})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_non_dict_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "- asdas1\n- rjohnson\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_scalar_string_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "just-a-string\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_none_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "None", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_null_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "null", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_empty_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "  \n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_exception_returns_empty(self, mock_cmd):
        mock_cmd.side_effect = RuntimeError("doozer failed")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
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

        mr_url = asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

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

        mr_url = asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

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

        asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

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

        mr_url = asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

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
        asyncio.run(client.set_mr_approval_rules("https://gitlab.example.com/a/b/-/merge_requests/1", {}))
        client.get_mr_from_url.assert_not_called()

    def test_empty_url_returns_early(self):
        client = self._make_client()
        client.get_mr_from_url = MagicMock()
        asyncio.run(client.set_mr_approval_rules("", {"QE": ["user1"]}))
        client.get_mr_from_url.assert_not_called()

    def test_dry_run_does_not_mutate(self):
        client = self._make_client(dry_run=True)
        client.get_mr_from_url = MagicMock()

        asyncio.run(
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

        asyncio.run(
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

        asyncio.run(
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

        asyncio.run(
            client.set_mr_approval_rules(
                "https://gitlab.example.com/a/b/-/merge_requests/1",
                {"QE": ["user1", "user2"], "Dev": ["user3"]},
            )
        )

        self.assertEqual(mock_mr.approval_rules.create.call_count, 2)


class TestReleaseFromFbcPipelineInit(unittest.TestCase):
    def _make_pipeline(self, **kwargs):
        defaults = dict(
            runtime=MagicMock(),
            group="oadp-1.5",
            assembly="1.5.3",
            fbc_pullspecs=["quay.io/example/fbc:v1"],
        )
        defaults.update(kwargs)
        defaults["runtime"].config = {}
        defaults["runtime"].dry_run = False
        defaults["runtime"].working_dir = MagicMock()
        defaults["runtime"].working_dir.absolute.return_value = MagicMock()
        return ReleaseFromFbcPipeline(**defaults)

    def test_default_extra_image_nvrs_is_empty(self):
        pipeline = self._make_pipeline()
        self.assertEqual(pipeline.extra_image_nvrs, [])

    def test_extra_image_nvrs_stored(self):
        nvrs = ["foo-container-1.0-1.el9", "bar-container-2.0-1.el9"]
        pipeline = self._make_pipeline(extra_image_nvrs=nvrs)
        self.assertEqual(pipeline.extra_image_nvrs, nvrs)

    def test_none_extra_image_nvrs_becomes_empty_list(self):
        pipeline = self._make_pipeline(extra_image_nvrs=None)
        self.assertEqual(pipeline.extra_image_nvrs, [])


class TestCategorizeNvrs(unittest.TestCase):
    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.config = {}
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        return ReleaseFromFbcPipeline(
            runtime=runtime,
            group="oadp-1.5",
            assembly="1.5.3",
            fbc_pullspecs=["quay.io/example/fbc:v1"],
        )

    def test_categorize_image_and_fbc(self):
        pipeline = self._make_pipeline()
        nvrs = ["oadp-operator-container-1.5.3-1.el9", "oadp-operator-fbc-1.5.3-1.el9"]
        result = pipeline.categorize_nvrs(nvrs)
        self.assertIn("oadp-operator-container-1.5.3-1.el9", result["image"])
        self.assertIn("oadp-operator-fbc-1.5.3-1.el9", result["fbc"])


class TestExtraImageNvrsValidation(unittest.TestCase):
    def _make_pipeline(self, extra_image_nvrs, fbc_pullspecs=None):
        runtime = MagicMock()
        runtime.config = {}
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="oadp-1.5",
            assembly="1.5.3",
            fbc_pullspecs=fbc_pullspecs or [],
            extra_image_nvrs=extra_image_nvrs,
        )
        pipeline.product = "oadp"
        return pipeline

    def test_fbc_nvr_in_extra_image_nvrs_raises(self):
        """run() should raise RuntimeError when extra_image_nvrs contains an FBC build."""
        pipeline = self._make_pipeline(extra_image_nvrs=["oadp-operator-fbc-1.5.3-1.el9.p2"])
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="oadp")

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(pipeline.run())
        self.assertIn("FBC builds", str(ctx.exception))

    def test_non_fbc_nvrs_do_not_raise_validation_error(self):
        """run() should not raise FBC validation error for normal image NVRs."""
        pipeline = self._make_pipeline(extra_image_nvrs=["oadp-velero-container-1.5.3-1.el9"])
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="oadp")
        pipeline.create_snapshot = AsyncMock(return_value=MagicMock())
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.write_shipment_files_locally = AsyncMock()

        try:
            asyncio.run(pipeline.run())
        except RuntimeError as e:
            self.assertNotIn("FBC builds", str(e))


class TestEmbargoedNvrDefensiveCheck(unittest.TestCase):
    """run() must refuse to release if any embargoed (private-fix) NVR is found, whether it came
    from an FBC pullspec or from a manual --extra-image-nvrs override."""

    def _make_pipeline(self, extra_image_nvrs=None, fbc_pullspecs=None):
        runtime = MagicMock()
        runtime.config = {}
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="oadp-1.5",
            assembly="1.5.3",
            fbc_pullspecs=fbc_pullspecs or [],
            extra_image_nvrs=extra_image_nvrs,
        )
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="oadp")
        return pipeline

    def test_embargoed_fbc_nvr_alone_does_not_raise(self):
        """The FBC (catalog) image's own NVR is excluded from the embargo check: it's just a
        catalog wrapper and, by construction, never carries a p-flag at all (see build_fbc.py,
        which tags it with a bare timestamp, e.g.
        'openshift-migration-operator-fbc-1.8.16-20260715174111.ocp4.19'). Since is_nvr_embargoed()
        defaults to treating an NVR with no p-flag as embargoed, checking the FBC's own NVR would
        make this defensive check fail unconditionally on every run - confirm it's excluded."""
        pipeline = self._make_pipeline(fbc_pullspecs=["quay.io/example/fbc:v1"])
        pipeline.validate_fbc_related_images = AsyncMock(return_value=[])
        pipeline.extract_fbc_nvr = MagicMock(return_value="oadp-operator-fbc-1.5.3-20260715174111.ocp4.19")
        pipeline.create_snapshot = AsyncMock(return_value=MagicMock())
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.write_shipment_files_locally = AsyncMock()

        asyncio.run(pipeline.run())

    def test_embargoed_related_nvr_raises(self):
        """An embargoed NVR among the FBC's related images (the actual bundle/operand images
        referenced inside the catalog) must fail the release."""
        embargoed_nvr = "oadp-velero-container-1.5.3-202607151200.p3.g172d0b2.assembly.stream.el9"
        pipeline = self._make_pipeline(fbc_pullspecs=["quay.io/example/fbc:v1"])
        pipeline.validate_fbc_related_images = AsyncMock(return_value=[embargoed_nvr])
        pipeline.extract_fbc_nvr = MagicMock(return_value="oadp-operator-fbc-1.5.3-20260715174111.ocp4.19")

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(pipeline.run())
        self.assertIn("embargoed", str(ctx.exception))
        self.assertIn(embargoed_nvr, str(ctx.exception))

    def test_ambiguous_nvr_raises(self):
        """An NVR whose embargo status is ambiguous (matches more than one visibility suffix)
        must fail the release rather than have is_nvr_embargoed() guess which one applies."""
        ambiguous_nvr = "oadp-velero-container-1.5.3.202607151200.p2.g172d0b2.assembly.stream.el9.p3"
        pipeline = self._make_pipeline(fbc_pullspecs=["quay.io/example/fbc:v1"])
        pipeline.validate_fbc_related_images = AsyncMock(return_value=[ambiguous_nvr])
        pipeline.extract_fbc_nvr = MagicMock(return_value="oadp-operator-fbc-1.5.3-20260715174111.ocp4.19")

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(pipeline.run())
        self.assertIn("embargo status", str(ctx.exception))

    def test_embargoed_extra_image_nvr_raises(self):
        """An embargoed NVR passed via --extra-image-nvrs must fail the release."""
        embargoed_nvr = "oadp-velero-container-1.5.3-202607151200.p3.g172d0b2.assembly.stream.el9"
        pipeline = self._make_pipeline(extra_image_nvrs=[embargoed_nvr])

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(pipeline.run())
        self.assertIn("embargoed", str(ctx.exception))
        self.assertIn(embargoed_nvr, str(ctx.exception))

    def test_non_embargoed_nvrs_do_not_raise_embargo_error(self):
        """Public (non-embargoed) NVRs should not trigger the embargo defensive check. The
        mocked workflow should complete normally, so require the run to fully succeed rather
        than suppressing any RuntimeError (which would mask unrelated regressions)."""
        pipeline = self._make_pipeline(
            extra_image_nvrs=["oadp-velero-container-1.5.3-202607151200.p2.g172d0b2.assembly.stream.el9"]
        )
        pipeline.create_snapshot = AsyncMock(return_value=MagicMock())
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.write_shipment_files_locally = AsyncMock()

        asyncio.run(pipeline.run())


class TestSetShipmentMrReady(unittest.TestCase):
    """Tests for ReleaseFromFbcPipeline.set_shipment_mr_ready()."""

    def _make_pipeline(self, dry_run=False):
        runtime = MagicMock()
        runtime.dry_run = dry_run
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="oadp-1.4",
            assembly="1.4.5",
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            create_mr=True,
        )
        pipeline.product = "oadp"
        pipeline.shipment_mr_url = "https://gitlab.cee.redhat.com/test/repo/-/merge_requests/42"
        return pipeline

    @patch("asyncio.sleep", new_callable=AsyncMock)
    def test_set_shipment_mr_ready_happy_path(self, mock_sleep):
        """MR is marked ready, 30s sleep, pipeline triggered. Assert all three calls."""
        pipeline = self._make_pipeline(dry_run=False)

        mock_mr = MagicMock()
        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock(return_value=mock_mr)
        mock_gitlab.trigger_ci_pipeline = AsyncMock(return_value="https://gitlab.cee.redhat.com/pipeline/123")
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_awaited_once_with(pipeline.shipment_mr_url)
        mock_sleep.assert_awaited_once_with(30)
        mock_gitlab.trigger_ci_pipeline.assert_awaited_once_with(mock_mr)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    def test_set_shipment_mr_ready_dry_run(self, mock_sleep):
        """dry_run=True. set_mr_ready is called but sleep and trigger_ci_pipeline are NOT called."""
        pipeline = self._make_pipeline(dry_run=True)

        mock_mr = MagicMock()
        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock(return_value=mock_mr)
        mock_gitlab.trigger_ci_pipeline = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_awaited_once_with(pipeline.shipment_mr_url)
        mock_sleep.assert_not_awaited()
        mock_gitlab.trigger_ci_pipeline.assert_not_awaited()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    def test_set_shipment_mr_ready_pipeline_trigger_fails(self, mock_sleep):
        """trigger_ci_pipeline raises Exception. Assert it doesn't propagate (method catches it)."""
        pipeline = self._make_pipeline(dry_run=False)

        mock_mr = MagicMock()
        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock(return_value=mock_mr)
        mock_gitlab.trigger_ci_pipeline = AsyncMock(side_effect=Exception("GitLab API error"))
        pipeline.__dict__["_gitlab"] = mock_gitlab

        # Should not raise
        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_awaited_once_with(pipeline.shipment_mr_url)
        mock_sleep.assert_awaited_once_with(30)
        mock_gitlab.trigger_ci_pipeline.assert_awaited_once_with(mock_mr)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    def test_set_shipment_mr_ready_mr_is_none(self, mock_sleep):
        """set_mr_ready returns None. Assert sleep and trigger_ci_pipeline are NOT called."""
        pipeline = self._make_pipeline(dry_run=False)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_ready = AsyncMock(return_value=None)
        mock_gitlab.trigger_ci_pipeline = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.set_shipment_mr_ready())

        mock_gitlab.set_mr_ready.assert_awaited_once_with(pipeline.shipment_mr_url)
        mock_sleep.assert_not_awaited()
        mock_gitlab.trigger_ci_pipeline.assert_not_awaited()


class TestCliValidation(unittest.TestCase):
    """Test CLI argument validation via CliRunner against the real Click command."""

    def _invoke(self, extra_args):
        from click.testing import CliRunner
        from pyartcd.pipelines.release_from_fbc import release_from_fbc
        from pyartcd.runtime import Runtime

        asyncio.set_event_loop(asyncio.new_event_loop())

        mock_runtime = MagicMock(spec=Runtime)
        mock_runtime.dry_run = False
        mock_runtime.working_dir = MagicMock()
        mock_runtime.working_dir.absolute.return_value = MagicMock()
        mock_runtime.config = {}

        runner = CliRunner()
        base_args = ["--group", "oadp-1.5", "--assembly", "1.5.3"]
        return runner.invoke(release_from_fbc, base_args + extra_args, obj=mock_runtime, standalone_mode=False)

    def test_both_empty_raises_error(self):
        """Both --fbc-pullspecs and --extra-image-nvrs empty should fail."""
        result = self._invoke([])
        self.assertIsInstance(result.exception, click.ClickException)
        self.assertIn("At least one of", str(result.exception))

    @patch("pyartcd.pipelines.release_from_fbc.ReleaseFromFbcPipeline")
    def test_fbc_only_does_not_raise(self, mock_pipeline_cls):
        """Providing only --fbc-pullspecs should pass CLI validation."""
        mock_pipeline_cls.return_value.run = AsyncMock()
        result = self._invoke(["--fbc-pullspecs", "quay.io/example/fbc:v1"])
        self.assertNotIsInstance(result.exception, click.ClickException)

    @patch("pyartcd.pipelines.release_from_fbc.ReleaseFromFbcPipeline")
    def test_extra_nvrs_only_does_not_raise(self, mock_pipeline_cls):
        """Providing only --extra-image-nvrs should pass CLI validation."""
        mock_pipeline_cls.return_value.run = AsyncMock()
        result = self._invoke(["--extra-image-nvrs", "foo-container-1.0-1.el9"])
        self.assertNotIsInstance(result.exception, click.ClickException)


class TestGetFileFromBranch(unittest.TestCase):
    """Tests for get_file_from_branch helper method."""

    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="logging-6.5",
            assembly="6.5.0",
            fbc_pullspecs=["quay.io/test/fbc:latest"],
        )
        pipeline.product = "logging"
        return pipeline

    @patch("pyartcd.pipelines.release_from_fbc.get_github_client_for_org")
    def test_successful_fetch(self, mock_get_client):
        """get_file_from_branch should fetch file content from GitHub."""
        pipeline = self._make_pipeline()

        mock_github = MagicMock()
        mock_get_client.return_value = mock_github

        mock_repo = MagicMock()
        mock_github.get_repo.return_value = mock_repo

        mock_file = MagicMock()
        mock_file.decoded_content = b"file content here"
        mock_repo.get_contents.return_value = mock_file

        result = pipeline.get_file_from_branch("openshift-4.20", "releases.yml")

        self.assertEqual(result, b"file content here")
        mock_get_client.assert_called_once_with("openshift-eng")
        mock_github.get_repo.assert_called_once_with("openshift-eng/ocp-build-data")
        mock_repo.get_contents.assert_called_once_with("releases.yml", ref="openshift-4.20")

    @patch("pyartcd.pipelines.release_from_fbc.get_github_client_for_org")
    def test_github_exception_raises_value_error(self, mock_get_client):
        """get_file_from_branch should raise ValueError on GithubException."""
        from github import GithubException

        pipeline = self._make_pipeline()

        mock_github = MagicMock()
        mock_get_client.return_value = mock_github

        mock_repo = MagicMock()
        mock_github.get_repo.return_value = mock_repo

        mock_repo.get_contents.side_effect = GithubException(404, "Not Found", {})

        with self.assertRaises(ValueError) as ctx:
            pipeline.get_file_from_branch("openshift-4.20", "releases.yml")

        self.assertIn("Failed to fetch", str(ctx.exception))
        self.assertIn("releases.yml", str(ctx.exception))


class TestLoadReleaseNotesTemplate(unittest.TestCase):
    """Tests for _load_release_notes_template method."""

    def _make_pipeline(self, group="openshift-4.22", assembly="4.22.0"):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group=group,
            assembly=assembly,
            fbc_pullspecs=["quay.io/test/fbc:latest"],
        )
        pipeline.product = "ocp"
        return pipeline

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    @patch.object(ReleaseFromFbcPipeline, "get_file_from_branch")
    def test_template_found_with_placeholder_substitution(self, mock_get_file, mock_get_boilerplate):
        """Template is found via get_advisory_boilerplate using kind as key, placeholders substituted."""
        mock_get_boilerplate.return_value = {
            "synopsis": "OpenShift Container Platform 4.{MINOR}.{PATCH} extras update",
            "topic": "Red Hat OpenShift Container Platform release 4.{MINOR}.{PATCH} is now available.",
            "description": "This advisory contains updates for OCP 4.{MINOR}.{PATCH}.",
            "solution": "For OCP 4.{MINOR} see the docs.",
        }
        mock_get_file.return_value = b"""
vars:
  MAJOR: 4
  MINOR: 22
"""

        pipeline = self._make_pipeline()
        result = pipeline._load_release_notes_template(kind="extras")

        self.assertIsNotNone(result)
        self.assertEqual(result["synopsis"], "OpenShift Container Platform 4.22.0 extras update")
        self.assertEqual(result["topic"], "Red Hat OpenShift Container Platform release 4.22.0 is now available.")
        self.assertEqual(result["description"], "This advisory contains updates for OCP 4.22.0.")
        self.assertEqual(result["solution"], "For OCP 4.22 see the docs.")

        mock_get_boilerplate.assert_called_once_with(
            runtime=pipeline,
            et_data={},
            art_advisory_key="extras",
            errata_type="RHBA",
        )
        mock_get_file.assert_called_once_with("openshift-4.22", "group.yml")

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    def test_template_not_found_returns_none(self, mock_get_boilerplate):
        """When get_advisory_boilerplate raises ValueError, return None."""
        mock_get_boilerplate.side_effect = ValueError("Boilerplate ocp not found")

        pipeline = self._make_pipeline()
        result = pipeline._load_release_notes_template(kind="extras")

        self.assertIsNone(result)

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    def test_github_api_failure_returns_none(self, mock_get_boilerplate):
        """When get_advisory_boilerplate raises an unexpected error, return None."""
        mock_get_boilerplate.side_effect = Exception("GitHub API failure")

        pipeline = self._make_pipeline()
        result = pipeline._load_release_notes_template(kind="extras")

        self.assertIsNone(result)

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    @patch.object(ReleaseFromFbcPipeline, "get_file_from_branch")
    def test_missing_major_minor_in_group_yml_returns_none(self, mock_get_file, mock_get_boilerplate):
        """When group.yml has no MAJOR/MINOR in vars section, return None."""
        mock_get_boilerplate.return_value = {
            "synopsis": "Synopsis",
            "topic": "Topic",
            "description": "Description",
            "solution": "Solution",
        }
        mock_get_file.return_value = b"""
product: logging
"""

        pipeline = self._make_pipeline()
        result = pipeline._load_release_notes_template(kind="extras")

        self.assertIsNone(result)

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    @patch.object(ReleaseFromFbcPipeline, "get_file_from_branch")
    def test_assembly_version_parsing_two_components(self, mock_get_file, mock_get_boilerplate):
        """Assembly with only major.minor (no patch) defaults patch to '0'."""
        mock_get_boilerplate.return_value = {
            "synopsis": "Version {MAJOR}.{MINOR}.{PATCH}",
            "topic": "Topic",
            "description": "Description",
            "solution": "Solution",
        }
        mock_get_file.return_value = b"""
vars:
  MAJOR: 4
  MINOR: 22
"""

        pipeline = self._make_pipeline(assembly="4.22")
        result = pipeline._load_release_notes_template(kind="extras")

        self.assertIsNotNone(result)
        self.assertEqual(result["synopsis"], "Version 4.22.0")

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    @patch.object(ReleaseFromFbcPipeline, "get_file_from_branch")
    def test_unresolved_placeholders_left_as_is(self, mock_get_file, mock_get_boilerplate):
        """SafeFormatter leaves unrecognized placeholders (like {IMAGE_ADVISORY}) intact."""
        mock_get_boilerplate.return_value = {
            "synopsis": "OCP 4.{MINOR}.{PATCH} extras update",
            "topic": "Topic",
            "description": "See https://access.redhat.com/errata/{IMAGE_ADVISORY}",
            "solution": "Solution",
        }
        mock_get_file.return_value = b"""
vars:
  MAJOR: 4
  MINOR: 22
"""

        pipeline = self._make_pipeline()
        result = pipeline._load_release_notes_template(kind="extras")

        self.assertIsNotNone(result)
        self.assertEqual(result["synopsis"], "OCP 4.22.0 extras update")
        self.assertEqual(result["description"], "See https://access.redhat.com/errata/{IMAGE_ADVISORY}")

    # -- Layered product mode tests (kind=None, uses self.product as key) --

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    @patch.object(ReleaseFromFbcPipeline, "get_file_from_branch")
    def test_layered_product_template_with_product_vars(self, mock_get_file, mock_get_boilerplate):
        """Layered product path uses self.product as key and PRODUCT_* placeholders."""
        mock_get_boilerplate.return_value = {
            "synopsis": "Logging for Red Hat OpenShift - {PRODUCT_MAJOR}.{PRODUCT_MINOR}.{PRODUCT_PATCH}",
            "topic": "Logging for Red Hat OpenShift - {PRODUCT_MAJOR}.{PRODUCT_MINOR}.{PRODUCT_PATCH}",
            "description": "Red Hat OpenShift Logging {PRODUCT_MAJOR}.{PRODUCT_MINOR}.{PRODUCT_PATCH} update",
            "solution": "For OCP {OCP_RELEASE_NOTES_VERSION} see ocp-{OCP_RELEASE_NOTES_VERSION_DASHED}-release-notes",
        }
        mock_get_file.return_value = b"""
OCP_RELEASE_NOTES_VERSION: "4.18"
"""

        pipeline = self._make_pipeline(group="logging-6.2", assembly="6.2.1")
        pipeline.product = "openshift-logging"
        result = pipeline._load_release_notes_template()

        self.assertIsNotNone(result)
        self.assertEqual(result["synopsis"], "Logging for Red Hat OpenShift - 6.2.1")
        self.assertEqual(result["topic"], "Logging for Red Hat OpenShift - 6.2.1")
        self.assertEqual(result["description"], "Red Hat OpenShift Logging 6.2.1 update")
        self.assertEqual(result["solution"], "For OCP 4.18 see ocp-4-18-release-notes")

        mock_get_boilerplate.assert_called_once_with(
            runtime=pipeline,
            et_data={},
            art_advisory_key="openshift-logging",
            errata_type="RHBA",
        )
        mock_get_file.assert_called_once_with("logging-6.2", "group.yml")

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    @patch.object(ReleaseFromFbcPipeline, "get_file_from_branch")
    def test_layered_product_missing_ocp_release_notes_version_returns_none(self, mock_get_file, mock_get_boilerplate):
        """Layered product without OCP_RELEASE_NOTES_VERSION in group.yml returns None."""
        mock_get_boilerplate.return_value = {
            "synopsis": "Synopsis",
            "topic": "Topic",
            "description": "Description",
            "solution": "Solution",
        }
        mock_get_file.return_value = b"""
product: openshift-logging
"""

        pipeline = self._make_pipeline(group="logging-6.3", assembly="6.3.5")
        pipeline.product = "openshift-logging"
        result = pipeline._load_release_notes_template()

        self.assertIsNone(result)

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    def test_layered_product_no_template_key_returns_none(self, mock_get_boilerplate):
        """Layered product whose product name has no template key returns None."""
        mock_get_boilerplate.side_effect = ValueError("Boilerplate mta not found")

        pipeline = self._make_pipeline(group="mta-7.2", assembly="7.2.0")
        pipeline.product = "mta"
        result = pipeline._load_release_notes_template()

        self.assertIsNone(result)

    @patch("pyartcd.pipelines.release_from_fbc.get_advisory_boilerplate")
    @patch.object(ReleaseFromFbcPipeline, "get_file_from_branch")
    def test_layered_product_assembly_two_components(self, mock_get_file, mock_get_boilerplate):
        """Layered product with major.minor assembly (no patch) uses empty string for PRODUCT_PATCH."""
        mock_get_boilerplate.return_value = {
            "synopsis": "Version {PRODUCT_MAJOR}.{PRODUCT_MINOR}.{PRODUCT_PATCH}",
            "topic": "Topic",
            "description": "Description",
            "solution": "Solution for {OCP_RELEASE_NOTES_VERSION}",
        }
        mock_get_file.return_value = b"""
OCP_RELEASE_NOTES_VERSION: "4.21"
"""

        pipeline = self._make_pipeline(group="logging-6.5", assembly="6.5")
        pipeline.product = "openshift-logging"
        result = pipeline._load_release_notes_template()

        self.assertIsNotNone(result)
        self.assertEqual(result["synopsis"], "Version 6.5.")
        self.assertEqual(result["solution"], "Solution for 4.21")


class TestRunWithTemplate(unittest.TestCase):
    def _make_pipeline(self, group="logging-6.5", assembly="6.5.0", jira_bugs=None):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}
        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group=group,
            assembly=assembly,
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            jira_bugs=jira_bugs,
        )
        pipeline.product = "openshift-logging"
        return pipeline

    @patch.object(ReleaseFromFbcPipeline, "_load_release_notes_template")
    def test_template_applied_when_no_jira_bugs(self, mock_load_template):
        pipeline = self._make_pipeline()
        mock_load_template.return_value = {
            "synopsis": "Logging 6.5.0",
            "topic": "Logging 6.5.0 topic",
            "description": "Logging 6.5.0 description",
            "solution": "Logging 6.5.0 solution",
        }

        release_notes = None
        template = pipeline._load_release_notes_template()
        if template:
            if release_notes is None:
                release_notes = ReleaseNotes(type="RHBA")
            release_notes.synopsis = template["synopsis"]
            release_notes.topic = template["topic"]
            release_notes.description = template["description"]
            release_notes.solution = template["solution"]

        self.assertIsNotNone(release_notes)
        self.assertEqual(release_notes.type, "RHBA")
        self.assertEqual(release_notes.synopsis, "Logging 6.5.0")
        self.assertEqual(release_notes.topic, "Logging 6.5.0 topic")

    @patch.object(ReleaseFromFbcPipeline, "_load_release_notes_template")
    def test_template_preserves_jira_fields(self, mock_load_template):
        pipeline = self._make_pipeline(jira_bugs=["LOG-1234"])
        mock_load_template.return_value = {
            "synopsis": "Logging 6.5.0",
            "topic": "Logging 6.5.0 topic",
            "description": "Logging 6.5.0 description",
            "solution": "Logging 6.5.0 solution",
        }

        release_notes = ReleaseNotes(
            type="RHSA",
            issues=Issues(fixed=[Issue(id="LOG-1234", source="redhat.atlassian.net")]),
        )

        template = pipeline._load_release_notes_template()
        if template:
            release_notes.synopsis = template["synopsis"]
            release_notes.topic = template["topic"]
            release_notes.description = template["description"]
            release_notes.solution = template["solution"]

        self.assertEqual(release_notes.type, "RHSA")
        self.assertEqual(release_notes.synopsis, "Logging 6.5.0")
        self.assertEqual(len(release_notes.issues.fixed), 1)
        self.assertEqual(release_notes.issues.fixed[0].id, "LOG-1234")

    @patch.object(ReleaseFromFbcPipeline, "_load_release_notes_template")
    def test_no_template_no_bugs_returns_none(self, mock_load_template):
        pipeline = self._make_pipeline()
        mock_load_template.return_value = None

        release_notes = None
        template = pipeline._load_release_notes_template()
        if template:
            if release_notes is None:
                release_notes = ReleaseNotes(type="RHBA")

        self.assertIsNone(release_notes)


class TestOcpOptionalMode(unittest.TestCase):
    """Tests for OCP optional-operator mode (--ocp-optional)."""

    def _make_pipeline(
        self, ocp_optional=False, exclude_nvr_components=None, group="openshift-4.22", assembly="4.22.0"
    ):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group=group,
            assembly=assembly,
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            create_mr=False,
            ocp_optional=ocp_optional,
            exclude_nvr_components=exclude_nvr_components,
        )
        pipeline.product = "ocp"
        return pipeline

    def test_ocp_optional_flag_stored(self):
        pipeline = self._make_pipeline(ocp_optional=True)
        self.assertTrue(pipeline.ocp_optional)

    def test_ocp_optional_default_false(self):
        pipeline = self._make_pipeline()
        self.assertFalse(pipeline.ocp_optional)

    def test_exclude_nvr_components_stored_as_set(self):
        pipeline = self._make_pipeline(
            ocp_optional=True,
            exclude_nvr_components=["kube-rbac-proxy-container", "other-container"],
        )
        self.assertEqual(pipeline.excluded_components, {"kube-rbac-proxy-container", "other-container"})

    def test_exclude_nvr_components_default_empty_set(self):
        pipeline = self._make_pipeline(ocp_optional=True)
        self.assertEqual(pipeline.excluded_components, set())

    # -- categorize_nvrs tests --

    def test_categorize_ocp_optional_all_included(self):
        """In OCP optional mode, all non-FBC images (including bundles and payload deps) go to extras."""
        pipeline = self._make_pipeline(ocp_optional=True)
        nvrs = [
            "ose-metallb-operator-container-v4.22.0-202605281227.el9",
            "ose-metallb-container-v4.22.0-202605281227.el9",
            "ose-frr-container-v4.22.0-202605281227.el9",
            "kube-rbac-proxy-container-v4.22.0-202605281227.el9",
            "ose-metallb-operator-bundle-container-v4.22.0.202605281227.el9-1",
            "ose-metallb-operator-fbc-4.22.0-20260528170921",
        ]
        result = pipeline.categorize_nvrs(nvrs)

        self.assertEqual(
            result["extras"],
            [
                "ose-metallb-operator-container-v4.22.0-202605281227.el9",
                "ose-metallb-container-v4.22.0-202605281227.el9",
                "ose-frr-container-v4.22.0-202605281227.el9",
                "kube-rbac-proxy-container-v4.22.0-202605281227.el9",
                "ose-metallb-operator-bundle-container-v4.22.0.202605281227.el9-1",
            ],
        )
        self.assertNotIn("metadata", result)
        self.assertEqual(
            result["fbc"],
            [
                "ose-metallb-operator-fbc-4.22.0-20260528170921",
            ],
        )
        self.assertEqual(result["external"], [])

    def test_categorize_ocp_optional_with_exclusion(self):
        """Explicitly excluded components go to external."""
        pipeline = self._make_pipeline(
            ocp_optional=True,
            exclude_nvr_components=["kube-rbac-proxy-container"],
        )
        nvrs = [
            "ose-metallb-operator-container-v4.22.0-202605281227.el9",
            "kube-rbac-proxy-container-v4.22.0-202605281227.el9",
            "ose-metallb-operator-fbc-4.22.0-20260528170921",
        ]
        result = pipeline.categorize_nvrs(nvrs)

        self.assertEqual(
            result["extras"],
            [
                "ose-metallb-operator-container-v4.22.0-202605281227.el9",
            ],
        )
        self.assertEqual(
            result["external"],
            [
                "kube-rbac-proxy-container-v4.22.0-202605281227.el9",
            ],
        )
        self.assertEqual(
            result["fbc"],
            [
                "ose-metallb-operator-fbc-4.22.0-20260528170921",
            ],
        )

    def test_categorize_default_mode_unchanged(self):
        """Default (non-OCP-optional) mode still uses image/fbc/external."""
        pipeline = self._make_pipeline(ocp_optional=False)
        nvrs = [
            "ose-metallb-operator-container-v4.22.0-202605281227.el9",
            "ose-metallb-operator-fbc-4.22.0-20260528170921",
        ]
        result = pipeline.categorize_nvrs(nvrs)

        self.assertIn("image", result)
        self.assertIn("fbc", result)
        self.assertNotIn("extras", result)
        self.assertNotIn("metadata", result)

    def test_categorize_bundles_go_to_extras(self):
        """Bundle NVRs go to extras alongside all other non-FBC images."""
        pipeline = self._make_pipeline(ocp_optional=True)
        nvrs = [
            "ose-metallb-operator-bundle-container-v4.22.0.202605281227.el9-1",
            "some-other-bundle-thing-container-v1.0.0-1.el9",
        ]
        result = pipeline.categorize_nvrs(nvrs)
        self.assertNotIn("metadata", result)
        self.assertEqual(len(result["extras"]), 2)

    # -- run() integration tests --

    def test_extra_image_nvrs_merged_into_extras_key(self):
        """In OCP optional mode, extra_image_nvrs should merge into 'extras', not 'image'."""
        pipeline = self._make_pipeline(ocp_optional=True)
        pipeline.extra_image_nvrs = ["extra-operator-container-v4.22.0-1.el9.p2"]
        pipeline.fbc_pullspecs = []
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="ocp")
        pipeline._load_release_notes_template = MagicMock(return_value=None)
        pipeline.create_snapshot = AsyncMock(return_value=_make_snapshot(app="openshift-4-22"))
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.write_shipment_files_locally = AsyncMock()

        asyncio.run(pipeline.run())

        snapshot_call_args = pipeline.create_snapshot.call_args[0][0]
        self.assertIn("extra-operator-container-v4.22.0-1.el9.p2", snapshot_call_args)

        config_call_args = pipeline.create_shipment_config.call_args
        self.assertEqual(config_call_args[0][0], "extras")

    # -- create_shipment_config tests --

    def test_create_shipment_config_extras_with_release_notes(self):
        """extras kind should include release notes when provided."""
        pipeline = self._make_pipeline(ocp_optional=True)
        snapshot = _make_snapshot(app="openshift-4-22")
        release_notes = ReleaseNotes(type="RHBA", synopsis="test")
        config = pipeline.create_shipment_config("extras", snapshot, release_notes=release_notes)

        self.assertIsNotNone(config.shipment.data)
        self.assertEqual(config.shipment.data.releaseNotes.type, "RHBA")
        self.assertFalse(config.shipment.metadata.fbc)

    def test_create_shipment_config_extras_openshift_minimal_notes(self):
        """extras kind for openshift-* group gets minimal release notes automatically."""
        pipeline = self._make_pipeline(ocp_optional=True)
        snapshot = _make_snapshot(app="openshift-4-22")
        config = pipeline.create_shipment_config("extras", snapshot)

        self.assertIsNotNone(config.shipment.data)
        self.assertEqual(config.shipment.data.releaseNotes.type, "RHBA")
        self.assertIn("extras", config.shipment.data.releaseNotes.synopsis)

    def test_create_shipment_config_extras_no_notes_for_non_openshift(self):
        """extras kind for non-openshift group should have no data when no release notes provided."""
        pipeline = self._make_pipeline(ocp_optional=True, group="oadp-1.4", assembly="1.4.5")
        pipeline.product = "oadp"
        snapshot = _make_snapshot(app="oadp-1-4")
        config = pipeline.create_shipment_config("extras", snapshot)

        self.assertIsNone(config.shipment.data)
        self.assertFalse(config.shipment.metadata.fbc)

    def test_create_shipment_config_fbc_still_works(self):
        """fbc kind in OCP optional mode should still set fbc=True and no data."""
        pipeline = self._make_pipeline(ocp_optional=True)
        snapshot = _make_snapshot(app="fbc-openshift-4-22")
        config = pipeline.create_shipment_config("fbc", snapshot)

        self.assertTrue(config.shipment.metadata.fbc)
        self.assertIsNone(config.shipment.data)

    # -- _get_main_ocp_shipment_url tests --

    def test_get_main_ocp_shipment_url_found(self):
        """Should return URL when releases.yml has shipment.url for the assembly."""
        pipeline = self._make_pipeline(ocp_optional=True, assembly="rc.5")
        releases_yml = {
            "releases": {
                "rc.5": {
                    "assembly": {
                        "group": {
                            "shipment": {
                                "url": "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/549"
                            }
                        }
                    }
                }
            }
        }
        import yaml as stdlib_yaml

        pipeline.get_file_from_branch = MagicMock(return_value=stdlib_yaml.dump(releases_yml).encode())
        result = pipeline._get_main_ocp_shipment_url()
        self.assertEqual(
            result,
            "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/549",
        )

    def test_get_main_ocp_shipment_url_found_override_suffix(self):
        """Should return URL when releases.yml uses 'shipment!' override marker."""
        pipeline = self._make_pipeline(ocp_optional=True, assembly="4.22.0")
        releases_yml = {
            "releases": {
                "4.22.0": {
                    "assembly": {
                        "group": {
                            "shipment!": {
                                "url": "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/562"
                            }
                        }
                    }
                }
            }
        }
        import yaml as stdlib_yaml

        pipeline.get_file_from_branch = MagicMock(return_value=stdlib_yaml.dump(releases_yml).encode())
        result = pipeline._get_main_ocp_shipment_url()
        self.assertEqual(
            result,
            "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/562",
        )

    def test_get_main_ocp_shipment_url_missing(self):
        """Should return None when assembly exists but has no shipment URL."""
        pipeline = self._make_pipeline(ocp_optional=True, assembly="rc.5")
        releases_yml = {"releases": {"rc.5": {"assembly": {"group": {"release_jira": "ART-12345"}}}}}
        import yaml as stdlib_yaml

        pipeline.get_file_from_branch = MagicMock(return_value=stdlib_yaml.dump(releases_yml).encode())
        result = pipeline._get_main_ocp_shipment_url()
        self.assertIsNone(result)

    def test_get_main_ocp_shipment_url_error(self):
        """Should return None and log warning on fetch failure."""
        pipeline = self._make_pipeline(ocp_optional=True)
        pipeline.get_file_from_branch = MagicMock(side_effect=ValueError("GitHub auth failed"))
        result = pipeline._get_main_ocp_shipment_url()
        self.assertIsNone(result)

    # -- MR dependency wiring in run() --

    def test_mr_dependency_set_in_ocp_optional_run(self):
        """run() should call _set_shipment_mr_dependency when ocp_optional and MR created."""
        pipeline = self._make_pipeline(ocp_optional=True)
        pipeline.create_mr = True
        pipeline.fbc_pullspecs = []
        pipeline.extra_image_nvrs = ["extra-op-container-v4.22.0-1.el9.p2"]
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline.setup_shipment_repo = AsyncMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="ocp")
        pipeline._load_release_notes_template = MagicMock(return_value=None)
        pipeline.create_snapshot = AsyncMock(return_value=_make_snapshot(app="openshift-4-22"))
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.create_shipment_mr = AsyncMock(return_value="https://gitlab.example.com/g/p/-/merge_requests/100")
        pipeline.shipment_mr_url = "https://gitlab.example.com/g/p/-/merge_requests/100"
        pipeline._get_main_ocp_shipment_url = MagicMock(
            return_value="https://gitlab.example.com/g/p/-/merge_requests/50"
        )
        pipeline._set_shipment_mr_dependency = AsyncMock()
        pipeline.set_shipment_mr_ready = AsyncMock()

        asyncio.run(pipeline.run())

        pipeline._get_main_ocp_shipment_url.assert_called_once()
        pipeline._set_shipment_mr_dependency.assert_awaited_once_with(
            "https://gitlab.example.com/g/p/-/merge_requests/50"
        )

    def test_mr_dependency_not_set_for_default_mode(self):
        """run() should NOT call _set_shipment_mr_dependency in default (non-ocp-optional) mode."""
        pipeline = self._make_pipeline(ocp_optional=False, group="oadp-1.5", assembly="1.5.0")
        pipeline.product = "oadp"
        pipeline.create_mr = True
        pipeline.fbc_pullspecs = []
        pipeline.extra_image_nvrs = ["oadp-container-v1.5.0-1.el9.p2"]
        pipeline.check_env_vars = MagicMock()
        pipeline.setup_working_dir = MagicMock()
        pipeline.setup_shipment_repo = AsyncMock()
        pipeline._load_product_from_group_config = AsyncMock(return_value="oadp")
        pipeline._load_release_notes_template = MagicMock(return_value=None)
        pipeline.create_snapshot = AsyncMock(return_value=_make_snapshot(app="oadp-1-5"))
        pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        pipeline.create_shipment_mr = AsyncMock(return_value="https://gitlab.example.com/g/p/-/merge_requests/200")
        pipeline.shipment_mr_url = "https://gitlab.example.com/g/p/-/merge_requests/200"
        pipeline._get_main_ocp_shipment_url = MagicMock()
        pipeline._set_shipment_mr_dependency = AsyncMock()
        pipeline.set_shipment_mr_ready = AsyncMock()

        asyncio.run(pipeline.run())

        pipeline._get_main_ocp_shipment_url.assert_not_called()
        pipeline._set_shipment_mr_dependency.assert_not_awaited()


class TestResolveSingleImageKey(unittest.TestCase):
    """Tests for _resolve_single_image_key."""

    def _make_pipeline(self, fbc_pullspecs=None, extra_image_nvrs=None):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="logging-6.5",
            assembly="6.5.1",
            fbc_pullspecs=fbc_pullspecs or [],
            extra_image_nvrs=extra_image_nvrs,
            create_mr=False,
        )
        return pipeline

    def test_single_fbc_operator_returns_key(self):
        pipeline = self._make_pipeline(fbc_pullspecs=["quay.io/test/fbc:latest"])
        pipeline._fbc_operator_keys = ["cluster-logging-operator"]
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertEqual(result, "cluster-logging-operator")

    def test_multiple_fbc_operators_returns_none(self):
        pipeline = self._make_pipeline(fbc_pullspecs=["quay.io/a:v1", "quay.io/b:v2"])
        pipeline._fbc_operator_keys = ["operator-a", "operator-b"]
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertIsNone(result)

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_single_extra_nvr_resolves_via_doozer(self, mock_cmd):
        """Single extra NVR resolves component to doozer key via images:print."""
        mock_cmd.return_value = (0, "oadp-velero-container: oadp-velero\n", "")
        pipeline = self._make_pipeline(extra_image_nvrs=["oadp-velero-container-1.4.5-1.el9"])
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertEqual(result, "oadp-velero")

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_single_extra_nvr_no_container_suffix(self, mock_cmd):
        """Extra NVR without -container suffix is resolved via doozer lookup."""
        mock_cmd.return_value = (0, "my-custom-build: my-custom-image\n", "")
        pipeline = self._make_pipeline(extra_image_nvrs=["my-custom-build-1.0.0-1.el9"])
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertEqual(result, "my-custom-image")

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_single_extra_nvr_unresolvable_returns_none(self, mock_cmd):
        """When doozer cannot resolve the component, returns None for group fallback."""
        mock_cmd.return_value = (0, "other-component: other-image\n", "")
        pipeline = self._make_pipeline(extra_image_nvrs=["unknown-component-1.0.0-1.el9"])
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertIsNone(result)

    def test_multiple_extra_nvrs_returns_none(self):
        pipeline = self._make_pipeline(extra_image_nvrs=["foo-container-1.0-1.el9", "bar-container-2.0-1.el9"])
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertIsNone(result)

    def test_both_fbc_and_extras_returns_none(self):
        pipeline = self._make_pipeline(
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            extra_image_nvrs=["foo-container-1.0-1.el9"],
        )
        pipeline._fbc_operator_keys = ["my-operator"]
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertIsNone(result)

    def test_unknown_fbc_keys_filtered_out(self):
        pipeline = self._make_pipeline(fbc_pullspecs=["quay.io/test/fbc:latest"])
        pipeline._fbc_operator_keys = ["UNKNOWN-12345678", "cluster-logging-operator"]
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertEqual(result, "cluster-logging-operator")

    def test_only_unknown_fbc_keys_returns_none(self):
        pipeline = self._make_pipeline(fbc_pullspecs=["quay.io/test/fbc:latest"])
        pipeline._fbc_operator_keys = ["UNKNOWN-12345678"]
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertIsNone(result)

    def test_no_fbc_no_extras_returns_none(self):
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._resolve_single_image_key())
        self.assertIsNone(result)


class TestLoadMrApproversFromImageConfig(unittest.TestCase):
    """Tests for _load_mr_approvers_from_image_config."""

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
        mock_cmd.return_value = (0, "images:\n  my-operator:\n    QE:\n    - user1\n    - user2\nrpms: {}\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_image_config("my-operator"))
        self.assertEqual(result, {"QE": ["user1", "user2"]})
        mock_cmd.assert_called_once()
        cmd_args = mock_cmd.call_args[0][0]
        self.assertIn("-i", cmd_args)
        self.assertIn("my-operator", cmd_args)
        self.assertIn("mr_approvers", cmd_args)
        self.assertIn("--yaml", cmd_args)

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_non_dict_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "images:\n  my-operator:\n  - user1\n  - user2\nrpms: {}\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_image_config("my-operator"))
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_none_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "images:\n  my-operator: null\nrpms: {}\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_image_config("my-operator"))
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_missing_key_in_envelope_returns_empty(self, mock_cmd):
        """When the requested doozer key is absent from the images envelope, returns empty."""
        mock_cmd.return_value = (0, "images:\n  other-operator:\n    QE:\n    - user1\nrpms: {}\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_image_config("my-operator"))
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_empty_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "  \n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_image_config("my-operator"))
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_empty_dict_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "images:\n  my-operator: {}\nrpms: {}\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_image_config("my-operator"))
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_exception_returns_empty(self, mock_cmd):
        mock_cmd.side_effect = RuntimeError("doozer failed")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_image_config("my-operator"))
        self.assertEqual(result, {})


class TestCreateShipmentMrImageConfigApprovers(unittest.TestCase):
    """Tests for image-config-first approver resolution in create_shipment_mr."""

    def _make_pipeline(self, dry_run=False, fbc_pullspecs=None, extra_image_nvrs=None):
        runtime = MagicMock()
        runtime.dry_run = dry_run
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {"gitlab_url": "https://gitlab.example.com"}

        pipeline = ReleaseFromFbcPipeline(
            runtime=runtime,
            group="logging-6.5",
            assembly="6.5.1",
            fbc_pullspecs=fbc_pullspecs or ["quay.io/test/fbc:latest"],
            extra_image_nvrs=extra_image_nvrs,
            create_mr=True,
        )
        pipeline.product = "logging"
        pipeline.shipment_data_repo = AsyncMock()
        pipeline.shipment_data_repo_push_url = "https://gitlab.example.com/user/ocp-shipment-data.git"
        pipeline.shipment_data_repo_pull_url = "https://gitlab.example.com/org/ocp-shipment-data.git"
        return pipeline

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_image_config_approvers_override_group(self, mock_cmd):
        """When image config has mr_approvers, group.yml is not consulted."""
        # First call: _load_mr_approvers_from_image_config (returns valid config)
        # Second call would be group config but should not happen
        mock_cmd.return_value = (
            0,
            "images:\n  cluster-logging-operator:\n    Release:\n    - release_user\nrpms: {}\n",
            "",
        )

        pipeline = self._make_pipeline(dry_run=False)
        pipeline._fbc_operator_keys = ["cluster-logging-operator"]
        pipeline.update_shipment_data = AsyncMock(return_value=True)

        mock_source_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/repo/-/merge_requests/10"
        mock_source_project.mergerequests.create.return_value = mock_mr
        pipeline._get_gitlab_project = MagicMock(return_value=mock_source_project)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

        mock_gitlab.set_mr_approval_rules.assert_awaited_once_with(
            "https://gitlab.example.com/org/repo/-/merge_requests/10",
            {"Release": ["release_user"]},
        )

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_falls_back_to_group_when_image_config_empty(self, mock_cmd):
        """When image config has no mr_approvers, falls back to group.yml."""
        call_count = [0]

        async def mock_gather(cmd):
            call_count[0] += 1
            if "config:print" in cmd:
                return (0, "images:\n  cluster-logging-operator: null\nrpms: {}\n", "")
            elif "config:read-group" in cmd:
                return (0, "QE:\n- group_user\n", "")
            return (0, "", "")

        mock_cmd.side_effect = mock_gather

        pipeline = self._make_pipeline(dry_run=False)
        pipeline._fbc_operator_keys = ["cluster-logging-operator"]
        pipeline.update_shipment_data = AsyncMock(return_value=True)

        mock_source_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/repo/-/merge_requests/11"
        mock_source_project.mergerequests.create.return_value = mock_mr
        pipeline._get_gitlab_project = MagicMock(return_value=mock_source_project)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

        mock_gitlab.set_mr_approval_rules.assert_awaited_once_with(
            "https://gitlab.example.com/org/repo/-/merge_requests/11",
            {"QE": ["group_user"]},
        )

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_ambiguous_falls_back_to_group(self, mock_cmd):
        """When both fbc_pullspecs and extra_image_nvrs are present, falls back to group."""
        mock_cmd.return_value = (0, "QE:\n- group_user\n", "")

        pipeline = self._make_pipeline(
            dry_run=False,
            fbc_pullspecs=["quay.io/test/fbc:latest"],
            extra_image_nvrs=["foo-container-1.0-1.el9"],
        )
        pipeline._fbc_operator_keys = ["my-operator"]
        pipeline.update_shipment_data = AsyncMock(return_value=True)

        mock_source_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/repo/-/merge_requests/12"
        mock_source_project.mergerequests.create.return_value = mock_mr
        pipeline._get_gitlab_project = MagicMock(return_value=mock_source_project)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

        # Should use group config since it's ambiguous
        mock_gitlab.set_mr_approval_rules.assert_awaited_once_with(
            "https://gitlab.example.com/org/repo/-/merge_requests/12",
            {"QE": ["group_user"]},
        )

    @patch("pyartcd.pipelines.release_from_fbc.exectools.cmd_gather_async")
    def test_extras_only_resolves_image_config_approvers(self, mock_cmd):
        """When only extra NVRs are provided, resolves doozer key and uses image config approvers."""

        async def mock_gather(cmd):
            if "images:print" in cmd:
                return (0, "oadp-velero-container: oadp-velero\n", "")
            elif "config:print" in cmd:
                return (0, "images:\n  oadp-velero:\n    QE:\n    - image_user\nrpms: {}\n", "")
            elif "config:read-group" in cmd:
                return (0, "QE:\n- group_user\n", "")
            return (0, "", "")

        mock_cmd.side_effect = mock_gather

        pipeline = self._make_pipeline(
            dry_run=False,
            fbc_pullspecs=[],
            extra_image_nvrs=["oadp-velero-container-1.4.5-1.el9"],
        )
        pipeline.update_shipment_data = AsyncMock(return_value=True)

        mock_source_project = MagicMock()
        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/repo/-/merge_requests/13"
        mock_source_project.mergerequests.create.return_value = mock_mr
        pipeline._get_gitlab_project = MagicMock(return_value=mock_source_project)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        pipeline.__dict__["_gitlab"] = mock_gitlab

        asyncio.run(pipeline.create_shipment_mr({}, env="prod"))

        mock_gitlab.set_mr_approval_rules.assert_awaited_once_with(
            "https://gitlab.example.com/org/repo/-/merge_requests/13",
            {"QE": ["image_user"]},
        )


if __name__ == "__main__":
    unittest.main()
