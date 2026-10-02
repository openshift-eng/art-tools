from unittest import TestCase
from unittest.mock import MagicMock, patch

from click import ClickException
from elliottlib.cli.verify_stage_testing_cli import (
    LabelCheckResult,
    StageTestingResult,
    TriggerResult,
    _add_mr_label,
    _check_mr_label,
    _parse_gitlab_mr_url,
    get_job_status,
    trigger_prow_job,
)


class TestStageTestingResult(TestCase):
    def test_success(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="success")
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)
        self.assertTrue(r.terminal)
        self.assertFalse(r.pending)

    def test_failure(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="failure")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)
        self.assertTrue(r.terminal)

    def test_pending(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="pending")
        self.assertFalse(r.passed)
        self.assertFalse(r.failed)
        self.assertFalse(r.terminal)
        self.assertTrue(r.pending)

    def test_aborted(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="aborted")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)
        self.assertTrue(r.terminal)

    def test_error(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="error")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)
        self.assertTrue(r.terminal)

    def test_to_dict(self):
        r = StageTestingResult(
            job_id="abc",
            job_name="test",
            state="success",
            url="https://prow.ci/view/123",
        )
        d = r.to_dict()
        self.assertEqual(d["job_id"], "abc")
        self.assertTrue(d["passed"])
        self.assertTrue(d["terminal"])
        self.assertEqual(d["url"], "https://prow.ci/view/123")
        self.assertFalse(d["label_added"])

    def test_to_dict_with_label(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="success", label_added=True)
        d = r.to_dict()
        self.assertTrue(d["label_added"])

    def test_render_text(self):
        r = StageTestingResult(
            job_id="abc",
            job_name="test",
            state="success",
            url="https://prow.ci/view/123",
        )
        text = r.render_text()
        self.assertIn("SUCCESS", text)
        self.assertIn("abc", text)
        self.assertIn("https://prow.ci/view/123", text)

    def test_render_text_with_label(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="success", label_added=True)
        text = r.render_text()
        self.assertIn("stage-testing-success", text)

    def test_label_error(self):
        r = StageTestingResult(job_id="abc", job_name="test", state="success", label_error="connection refused")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)
        self.assertTrue(r.terminal)
        d = r.to_dict()
        self.assertEqual(d["label_error"], "connection refused")
        self.assertFalse(d["passed"])
        text = r.render_text()
        self.assertIn("Label update failed", text)


class TestLabelCheckResult(TestCase):
    def test_found(self):
        r = LabelCheckResult(label_found=True, mr_url="https://gitlab/mr/1")
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)
        d = r.to_dict()
        self.assertTrue(d["passed"])
        self.assertTrue(d["label_found"])

    def test_not_found(self):
        r = LabelCheckResult(label_found=False, mr_url="https://gitlab/mr/1")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_render_text(self):
        r = LabelCheckResult(label_found=True, mr_url="https://gitlab/mr/1")
        text = r.render_text()
        self.assertIn("PASS", text)
        self.assertIn("stage-testing-success", text)


class TestParseGitlabMrUrl(TestCase):
    def test_valid_url(self):
        url = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/456"
        base, project, iid = _parse_gitlab_mr_url(url)
        self.assertEqual(base, "https://gitlab.cee.redhat.com")
        self.assertEqual(project, "hybrid-platforms/art/ocp-shipment-data")
        self.assertEqual(iid, "456")

    def test_invalid_url(self):
        with self.assertRaises(ClickException):
            _parse_gitlab_mr_url("https://example.com/not-a-mr")


class TestGetJobStatus(TestCase):
    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_get_status_success(self, mock_session):
        prow_yaml = """
status:
  state: success
  url: https://prow.ci.openshift.org/view/gs/test-platform-results/logs/job/123
  startTime: "2026-09-30T10:00:00Z"
  completionTime: "2026-09-30T12:00:00Z"
spec:
  job: periodic-ci-openshift-openshift-tests-private-release-4.22-stage-testing-e2e-aws-ipi
"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = prow_yaml
        mock_session.return_value.get.return_value = mock_response

        result = get_job_status("job-123")

        self.assertEqual(result.job_id, "job-123")
        self.assertEqual(result.state, "success")
        self.assertTrue(result.passed)
        self.assertFalse(result.failed)

    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_get_status_pending(self, mock_session):
        prow_yaml = """
status:
  state: pending
  url: https://prow.ci.openshift.org/view/gs/test-platform-results/logs/job/123
  startTime: "2026-09-30T10:00:00Z"
spec:
  job: periodic-ci-openshift-openshift-tests-private-release-4.22-stage-testing-e2e-aws-ipi
"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = prow_yaml
        mock_session.return_value.get.return_value = mock_response

        result = get_job_status("job-123")
        self.assertTrue(result.pending)
        self.assertFalse(result.failed)
        self.assertIsNone(result.completion_time)

    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_get_status_http_error(self, mock_session):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.reason = "Not Found"
        mock_session.return_value.get.return_value = mock_response

        with self.assertRaises(ClickException):
            get_job_status("nonexistent")

    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_get_status_empty_response(self, mock_session):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_session.return_value.get.return_value = mock_response

        with self.assertRaises(ClickException):
            get_job_status("job-123")


class TestCheckMrLabel(TestCase):
    @patch("elliottlib.cli.verify_stage_testing_cli._get_gitlab_headers")
    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_label_found(self, mock_session, mock_headers):
        mock_headers.return_value = {"PRIVATE-TOKEN": "token"}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"labels": ["stage-testing-success", "other"]}
        mock_session.return_value.get.return_value = mock_response

        result = _check_mr_label("https://gitlab.cee.redhat.com/group/project/-/merge_requests/1")
        self.assertTrue(result)

    @patch("elliottlib.cli.verify_stage_testing_cli._get_gitlab_headers")
    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_label_not_found(self, mock_session, mock_headers):
        mock_headers.return_value = {"PRIVATE-TOKEN": "token"}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"labels": ["stage-release-success"]}
        mock_session.return_value.get.return_value = mock_response

        result = _check_mr_label("https://gitlab.cee.redhat.com/group/project/-/merge_requests/1")
        self.assertFalse(result)


class TestAddMrLabel(TestCase):
    @patch("elliottlib.cli.verify_stage_testing_cli._get_gitlab_headers")
    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_add_label_success(self, mock_session, mock_headers):
        mock_headers.return_value = {"PRIVATE-TOKEN": "token"}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.return_value.put.return_value = mock_response

        _add_mr_label("https://gitlab.cee.redhat.com/group/project/-/merge_requests/1")

        call_kwargs = mock_session.return_value.put.call_args
        self.assertEqual(call_kwargs.kwargs["json"], {"add_labels": "stage-testing-success"})

    @patch("elliottlib.cli.verify_stage_testing_cli._get_gitlab_headers")
    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    def test_add_label_http_error(self, mock_session, mock_headers):
        mock_headers.return_value = {"PRIVATE-TOKEN": "token"}
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.reason = "Forbidden"
        mock_session.return_value.put.return_value = mock_response

        with self.assertRaises(ClickException):
            _add_mr_label("https://gitlab.cee.redhat.com/group/project/-/merge_requests/1")


class TestTriggerResult(TestCase):
    def test_triggered(self):
        result = TriggerResult(job_id="job-123", job_name="test-job")
        self.assertTrue(result.passed)
        d = result.to_dict()
        self.assertEqual(d["job_id"], "job-123")
        self.assertEqual(d["job_name"], "test-job")

    def test_empty(self):
        result = TriggerResult()
        self.assertFalse(result.passed)

    def test_render_text(self):
        result = TriggerResult(job_id="job-123", job_name="test-job")
        text = result.render_text()
        self.assertIn("TRIGGERED", text)
        self.assertIn("job-123", text)


class TestTriggerProwJob(TestCase):
    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    @patch.dict("os.environ", {"GANGWAY_TOKEN": "test-token"})
    def test_trigger_success(self, mock_session):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "job-456"}
        mock_session.return_value.post.return_value = mock_response

        job_id, job_name = trigger_prow_job("4.22", "4.22.9")

        self.assertEqual(job_id, "job-456")
        self.assertIn("4.22-stage-testing", job_name)
        call_kwargs = mock_session.return_value.post.call_args
        data = call_kwargs.kwargs["json"]
        self.assertEqual(
            data["pod_spec_options"]["envs"]["RELEASE_IMAGE_LATEST"],
            "quay.io/openshift-release-dev/ocp-release:4.22.9-x86_64",
        )

    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    @patch.dict("os.environ", {"GANGWAY_TOKEN": "test-token"})
    def test_trigger_http_error(self, mock_session):
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.reason = "Forbidden"
        mock_session.return_value.post.return_value = mock_response

        with self.assertRaises(ClickException):
            trigger_prow_job("4.22", "4.22.9")

    @patch("elliottlib.cli.verify_stage_testing_cli._get_session")
    @patch.dict("os.environ", {}, clear=False)
    def test_trigger_no_token(self, mock_session):
        import os

        os.environ.pop("GANGWAY_TOKEN", None)

        with self.assertRaises(ClickException):
            trigger_prow_job("4.22", "4.22.9")
