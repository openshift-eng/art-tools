import asyncio
import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from elliottlib.cli.verify_release_cli import (
    VERIFY_RELEASE_FAIL_LABEL,
    VERIFY_RELEASE_SUCCESS_LABEL,
    LabelCheckResult,
    MarkResult,
    _handle_label_check,
    _handle_mark,
    verify_release_cli,
)


class TestLabelCheckResult(TestCase):
    def test_verified(self):
        r = LabelCheckResult(state="verified", mr_url="https://gitlab/mr/1")
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_not_verified(self):
        r = LabelCheckResult(state="not_verified", mr_url="https://gitlab/mr/1")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_to_dict(self):
        r = LabelCheckResult(state="verified", mr_url="https://gitlab/mr/1")
        d = r.to_dict()
        self.assertTrue(d["passed"])
        self.assertEqual(d["state"], "verified")
        self.assertEqual(d["mr_url"], "https://gitlab/mr/1")

    def test_render_text_verified(self):
        r = LabelCheckResult(state="verified", mr_url="https://gitlab/mr/1")
        text = r.render_text()
        self.assertIn("PASS", text)
        self.assertIn("https://gitlab/mr/1", text)

    def test_render_text_not_verified(self):
        r = LabelCheckResult(state="not_verified", mr_url="https://gitlab/mr/1")
        text = r.render_text()
        self.assertIn("NOT VERIFIED", text)


class TestMarkResult(TestCase):
    def test_success(self):
        r = MarkResult(mr_url="https://gitlab/mr/1", label_added=True)
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_failure(self):
        r = MarkResult(mr_url="https://gitlab/mr/1", label_added=False, error="connection refused")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_to_dict(self):
        r = MarkResult(mr_url="https://gitlab/mr/1", label_added=True)
        d = r.to_dict()
        self.assertTrue(d["passed"])
        self.assertTrue(d["label_added"])
        self.assertEqual(d["label"], VERIFY_RELEASE_SUCCESS_LABEL)

    def test_to_dict_with_error(self):
        r = MarkResult(mr_url="https://gitlab/mr/1", error="timeout")
        d = r.to_dict()
        self.assertFalse(d["passed"])
        self.assertEqual(d["error"], "timeout")

    def test_render_text_success(self):
        r = MarkResult(mr_url="https://gitlab/mr/1", label_added=True)
        text = r.render_text()
        self.assertIn("PASS", text)
        self.assertIn("https://gitlab/mr/1", text)

    def test_render_text_failure(self):
        r = MarkResult(mr_url="https://gitlab/mr/1", error="forbidden")
        text = r.render_text()
        self.assertIn("FAIL", text)
        self.assertIn("forbidden", text)


class TestHandleLabelCheck(TestCase):
    @patch("elliottlib.cli.verify_release_cli.handle_verify_result")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_verified(self, mock_gitlab_cls, mock_handle):
        mock_mr = MagicMock()
        mock_mr.labels = ["stage-release-success", VERIFY_RELEASE_SUCCESS_LABEL]
        mock_client = MagicMock()
        mock_client.get_mr_from_url.return_value = mock_mr
        mock_gitlab_cls.from_url.return_value = mock_client

        _handle_label_check("https://gitlab/project/-/merge_requests/1", "text")

        result = mock_handle.call_args[0][0]
        self.assertIsInstance(result, LabelCheckResult)
        self.assertEqual(result.state, "verified")
        self.assertTrue(result.passed)

    @patch("elliottlib.cli.verify_release_cli.handle_verify_result")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_not_verified(self, mock_gitlab_cls, mock_handle):
        mock_mr = MagicMock()
        mock_mr.labels = ["stage-release-success"]
        mock_client = MagicMock()
        mock_client.get_mr_from_url.return_value = mock_mr
        mock_gitlab_cls.from_url.return_value = mock_client

        _handle_label_check("https://gitlab/project/-/merge_requests/1", "text")

        result = mock_handle.call_args[0][0]
        self.assertEqual(result.state, "not_verified")
        self.assertFalse(result.passed)

    @patch("elliottlib.cli.verify_release_cli.handle_verify_result")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_none_labels(self, mock_gitlab_cls, mock_handle):
        mock_mr = MagicMock()
        mock_mr.labels = None
        mock_client = MagicMock()
        mock_client.get_mr_from_url.return_value = mock_mr
        mock_gitlab_cls.from_url.return_value = mock_client

        _handle_label_check("https://gitlab/project/-/merge_requests/1", "text")

        result = mock_handle.call_args[0][0]
        self.assertEqual(result.state, "not_verified")


class TestHandleMark(TestCase):
    @patch("elliottlib.cli.verify_release_cli.handle_verify_result")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_success(self, mock_gitlab_cls, mock_handle):
        mock_client = MagicMock()
        mock_gitlab_cls.from_url.return_value = mock_client

        runtime = MagicMock()
        runtime.dry_run = False

        _handle_mark(runtime, "https://gitlab/group/project/-/merge_requests/42", "text")

        mock_client.add_mr_labels.assert_called_once_with(
            "https://gitlab/group/project/-/merge_requests/42",
            [VERIFY_RELEASE_SUCCESS_LABEL],
            remove_labels=[VERIFY_RELEASE_FAIL_LABEL],
        )
        result = mock_handle.call_args[0][0]
        self.assertIsInstance(result, MarkResult)
        self.assertTrue(result.label_added)
        self.assertIsNone(result.error)

    @patch("elliottlib.cli.verify_release_cli.handle_verify_result")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_dry_run(self, mock_gitlab_cls, mock_handle):
        runtime = MagicMock()
        runtime.dry_run = True

        _handle_mark(runtime, "https://gitlab/group/project/-/merge_requests/42", "text")

        mock_gitlab_cls.from_url.assert_not_called()
        result = mock_handle.call_args[0][0]
        self.assertTrue(result.label_added)

    @patch("elliottlib.cli.verify_release_cli.handle_verify_result")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_gitlab_error(self, mock_gitlab_cls, mock_handle):
        mock_client = MagicMock()
        mock_client.add_mr_labels.side_effect = Exception("403 Forbidden")
        mock_gitlab_cls.from_url.return_value = mock_client

        runtime = MagicMock()
        runtime.dry_run = False

        _handle_mark(runtime, "https://gitlab/group/project/-/merge_requests/42", "text")

        result = mock_handle.call_args[0][0]
        self.assertFalse(result.label_added)
        self.assertIn("403 Forbidden", result.error)


class TestVerifyReleaseCLI(TestCase):
    def setUp(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.addCleanup(loop.close)
        self.addCleanup(asyncio.set_event_loop, None)

    @patch("elliottlib.cli.verify_release_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_dry_run_does_not_update_gitlab(self, mock_gitlab_cls, mock_shipment_url):
        runtime = MagicMock(spec=["initialize", "dry_run"])
        runtime.initialize.side_effect = lambda **kwargs: self.assertTrue(runtime.dry_run)
        mock_shipment_url.return_value = "https://gitlab/group/project/-/merge_requests/42"

        result = CliRunner().invoke(verify_release_cli, ["--mark", "--dry-run"], obj=runtime)

        self.assertEqual(result.exit_code, 0, result.output)
        runtime.initialize.assert_called_once_with(no_group=False)
        mock_gitlab_cls.from_url.assert_not_called()

    @patch("elliottlib.cli.verify_release_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_without_dry_run_updates_gitlab(self, mock_gitlab_cls, mock_shipment_url):
        runtime = MagicMock(spec=["initialize", "dry_run"])
        runtime.initialize.side_effect = lambda **kwargs: self.assertFalse(runtime.dry_run)
        mock_shipment_url.return_value = "https://gitlab/group/project/-/merge_requests/42"

        result = CliRunner().invoke(verify_release_cli, ["--mark"], obj=runtime)

        self.assertEqual(result.exit_code, 0, result.output)
        mock_gitlab_cls.from_url.return_value.add_mr_labels.assert_called_once_with(
            "https://gitlab/group/project/-/merge_requests/42",
            [VERIFY_RELEASE_SUCCESS_LABEL],
            remove_labels=[VERIFY_RELEASE_FAIL_LABEL],
        )

    @patch("elliottlib.cli.verify_release_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_failed_replaces_success(self, gitlab_cls, shipment_url):
        runtime = MagicMock()
        shipment_url.return_value = "https://gitlab.example.com/group/project/-/merge_requests/42"
        result = CliRunner().invoke(verify_release_cli, ["--mark-failed", "-o", "json"], obj=runtime)
        self.assertEqual(result.exit_code, 0, result.output)
        gitlab_cls.from_url.return_value.add_mr_labels.assert_called_once_with(
            shipment_url.return_value, [VERIFY_RELEASE_FAIL_LABEL], remove_labels=[VERIFY_RELEASE_SUCCESS_LABEL]
        )
        data = json.loads(result.output)
        self.assertTrue(data["passed"])
        self.assertEqual(data["label"], VERIFY_RELEASE_FAIL_LABEL)

    @patch("elliottlib.cli.verify_release_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_failed_dry_run_does_not_update(self, gitlab_cls, shipment_url):
        runtime = MagicMock()
        result = CliRunner().invoke(verify_release_cli, ["--mark-failed", "--dry-run"], obj=runtime)
        self.assertEqual(result.exit_code, 0, result.output)
        gitlab_cls.from_url.assert_not_called()

    def test_mark_options_are_mutually_exclusive(self):
        runtime = MagicMock()
        result = CliRunner().invoke(verify_release_cli, ["--mark", "--mark-failed"], obj=runtime)
        self.assertEqual(result.exit_code, 2, result.output)
        self.assertIn("mutually exclusive", result.output)
        runtime.initialize.assert_not_called()

    @patch("elliottlib.cli.verify_release_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_label_check_reports_failed_without_writing(self, gitlab_cls, shipment_url):
        runtime = MagicMock()
        shipment_url.return_value = "https://gitlab.example.com/group/project/-/merge_requests/42"
        client = gitlab_cls.from_url.return_value
        client.get_mr_from_url.return_value.labels = [VERIFY_RELEASE_FAIL_LABEL, "unrelated"]
        result = CliRunner().invoke(verify_release_cli, ["-o", "json"], obj=runtime)
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertEqual(json.loads(result.output)["state"], "failed")
        client.add_mr_labels.assert_not_called()

    @patch("elliottlib.cli.verify_release_cli.get_assembly_shipment_url")
    @patch("elliottlib.cli.verify_release_cli.GitLabClient")
    def test_mark_failed_reports_api_error(self, gitlab_cls, shipment_url):
        runtime = MagicMock()
        shipment_url.return_value = "https://gitlab.example.com/group/project/-/merge_requests/42"
        gitlab_cls.from_url.return_value.add_mr_labels.side_effect = RuntimeError("403 Forbidden")
        result = CliRunner().invoke(verify_release_cli, ["--mark-failed", "-o", "json"], obj=runtime)
        self.assertEqual(result.exit_code, 1, result.output)
        data = json.loads(result.output)
        self.assertFalse(data["label_added"])
        self.assertEqual(data["error"], "403 Forbidden")
