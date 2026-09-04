import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from aiohttp import ClientSession
from elliottlib.cli.verify_image_grades_cli import (
    ImageGradeResult,
    VerifyImageGradesResult,
    _parse_timestamp,
    extract_digest,
    fetch_shipment_components,
    get_current_grade,
    query_freshness_grades,
    render_result,
    resolve_shipment_mr_url,
    verify_image_grades,
)


class TestExtractDigest(unittest.TestCase):
    def test_sha256_digest(self):
        ps = "registry.redhat.io/openshift4/ose-cli@sha256:abc123def456"
        self.assertEqual(extract_digest(ps), "abc123def456")

    def test_no_digest(self):
        self.assertEqual(extract_digest("registry.redhat.io/openshift4/ose-cli:v4.18"), "")

    def test_empty(self):
        self.assertEqual(extract_digest(""), "")


class TestGetCurrentGrade(unittest.TestCase):
    def test_empty_grades(self):
        self.assertEqual(get_current_grade([]), "Unknown")

    def test_single_valid_grade(self):
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        self.assertEqual(get_current_grade([{"start_date": past, "grade": "A"}]), "A")

    def test_future_grade_ignored(self):
        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        self.assertEqual(get_current_grade([{"start_date": future, "grade": "A"}]), "Unknown")

    def test_most_recent_grade_wins(self):
        old = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        recent = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        grades = [
            {"start_date": old, "grade": "A"},
            {"start_date": recent, "grade": "C"},
        ]
        self.assertEqual(get_current_grade(grades), "C")

    def test_missing_start_date_skipped(self):
        valid = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        grades = [
            {"grade": "F"},
            {"start_date": valid, "grade": "B"},
        ]
        self.assertEqual(get_current_grade(grades), "B")

    def test_missing_grade_field(self):
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        self.assertEqual(get_current_grade([{"start_date": past}]), "Unknown")

    def test_z_suffix_timestamp(self):
        self.assertEqual(get_current_grade([{"start_date": "2024-01-01T00:00:00Z", "grade": "A"}]), "A")

    def test_naive_timestamp(self):
        self.assertEqual(get_current_grade([{"start_date": "2024-01-01T00:00:00", "grade": "B"}]), "B")


class TestParseTimestamp(unittest.TestCase):
    def test_z_suffix(self):
        dt = _parse_timestamp("2024-01-01T00:00:00Z")
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_naive_gets_utc(self):
        dt = _parse_timestamp("2024-06-15T12:30:00")
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_offset_preserved(self):
        dt = _parse_timestamp("2024-01-01T00:00:00+05:00")
        self.assertEqual(dt.utcoffset().total_seconds(), 5 * 3600)


class TestImageGradeResult(unittest.TestCase):
    def test_healthy_grades(self):
        for grade in ("A", "B"):
            r = ImageGradeResult(name="test", pullspec="ps", digest="d", grade=grade)
            self.assertTrue(r.healthy, f"Grade {grade} should be healthy")

    def test_unhealthy_grades(self):
        for grade in ("C", "D", "E", "F", "Unknown"):
            r = ImageGradeResult(name="test", pullspec="ps", digest="d", grade=grade)
            self.assertFalse(r.healthy, f"Grade {grade} should be unhealthy")


class TestVerifyImageGradesResult(unittest.TestCase):
    def _make_result(self, grades):
        return VerifyImageGradesResult(
            shipment_mr_url="https://gitlab.example.com/mr/1",
            shipment_version="4.18.51",
            results=[
                ImageGradeResult(name=f"img-{i}", pullspec=f"ps-{i}", digest=f"d-{i}", grade=g)
                for i, g in enumerate(grades)
            ],
        )

    def test_all_healthy(self):
        r = self._make_result(["A", "B", "A"])
        self.assertTrue(r.passed)
        self.assertEqual(len(r.unhealthy_images), 0)
        self.assertEqual(r.total_scanned, 3)

    def test_some_unhealthy(self):
        r = self._make_result(["A", "C", "Unknown"])
        self.assertFalse(r.passed)
        self.assertEqual(len(r.unhealthy_images), 2)
        self.assertEqual(r.unknown_count, 1)

    def test_empty(self):
        r = self._make_result([])
        self.assertFalse(r.passed)
        self.assertEqual(r.total_scanned, 0)


class TestResolveShipmentMrUrl(unittest.TestCase):
    def test_resolves_url(self):
        mock_runtime = MagicMock()
        mock_runtime.assembly = "4.18.51"
        mock_runtime.get_releases_config.return_value = MagicMock()

        with patch("elliottlib.cli.verify_image_grades_cli.assembly_config_struct") as mock_acs:
            mock_acs.return_value = {"shipment": {"url": "https://gitlab.example.com/mr/1"}}
            url = resolve_shipment_mr_url(mock_runtime)

        self.assertEqual(url, "https://gitlab.example.com/mr/1")
        mock_acs.assert_called_once_with(
            mock_runtime.get_releases_config.return_value,
            "4.18.51",
            "group",
            {},
        )

    def test_raises_when_no_url(self):
        mock_runtime = MagicMock()
        mock_runtime.assembly = "4.18.51"
        mock_runtime.get_releases_config.return_value = MagicMock()

        with patch("elliottlib.cli.verify_image_grades_cli.assembly_config_struct") as mock_acs:
            mock_acs.return_value = {}
            with self.assertRaises(RuntimeError) as ctx:
                resolve_shipment_mr_url(mock_runtime)
            self.assertIn("4.18.51", str(ctx.exception))

    def test_raises_when_shipment_without_url(self):
        mock_runtime = MagicMock()
        mock_runtime.assembly = "4.18.51"
        mock_runtime.get_releases_config.return_value = MagicMock()

        with patch("elliottlib.cli.verify_image_grades_cli.assembly_config_struct") as mock_acs:
            mock_acs.return_value = {"shipment": {}}
            with self.assertRaises(RuntimeError):
                resolve_shipment_mr_url(mock_runtime)


class TestQueryFreshnessGrades(unittest.IsolatedAsyncioTestCase):
    async def test_successful_query(self):
        grades = [{"start_date": "2026-01-01T00:00:00+00:00", "grade": "A"}]
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"data": [{"freshness_grades": grades}]})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock(spec=ClientSession)
        mock_session.get = MagicMock(return_value=mock_resp)

        result, available = await query_freshness_grades(mock_session, "a" * 64)
        self.assertEqual(result, grades)
        self.assertTrue(available)

    async def test_empty_data(self):
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"data": []})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock(spec=ClientSession)
        mock_session.get = MagicMock(return_value=mock_resp)

        result, available = await query_freshness_grades(mock_session, "a" * 64)
        self.assertEqual(result, [])
        self.assertTrue(available)

    async def test_api_error(self):
        mock_resp = AsyncMock()
        mock_resp.status = 500
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock(spec=ClientSession)
        mock_session.get = MagicMock(return_value=mock_resp)

        result, available = await query_freshness_grades(mock_session, "a" * 64)
        self.assertEqual(result, [])
        self.assertFalse(available)

    async def test_malformed_digest_rejected(self):
        mock_session = AsyncMock(spec=ClientSession)
        result, available = await query_freshness_grades(mock_session, "not-a-valid-digest!")
        self.assertEqual(result, [])
        self.assertFalse(available)
        mock_session.get.assert_not_called()


class TestFetchShipmentComponents(unittest.TestCase):
    @patch("elliottlib.cli.verify_image_grades_cli.requests")
    @patch("elliottlib.cli.verify_image_grades_cli.GitLabClient")
    def test_parses_components_from_advisory(self, mock_gl_cls, mock_requests):
        shipment_yaml = """
shipment:
  environments:
    stage:
      advisory:
        internal_url: "https://errata.devel.redhat.com/advisory/12345"
"""
        mock_file = MagicMock()
        mock_file.decode.return_value = shipment_yaml.encode("utf-8")

        mock_project = MagicMock()
        mock_project.files.get.return_value = mock_file

        mock_diff = MagicMock()
        mock_diff.diffs = [{"new_path": "shipments/4.18/4.18.51.yaml"}]

        mock_mr = MagicMock()
        mock_mr.title = "Shipment for 4.18.51"
        mock_mr.source_branch = "shipment-4.18.51"
        mock_mr.source_project_id = 123
        mock_mr.diffs.list.return_value = [MagicMock(id=1)]
        mock_mr.diffs.get.return_value = mock_diff

        mock_gl = MagicMock()
        mock_gl.get_mr_from_url.return_value = mock_mr
        mock_gl.get_project.return_value = mock_project
        mock_gl_cls.from_url.return_value = mock_gl

        advisory_content = {
            "spec": {
                "content": {
                    "images": [
                        {
                            "component": "ose-cli",
                            "containerImage": "registry.redhat.io/openshift4/ose-cli@sha256:abc",
                            "architecture": "amd64",
                        },
                        {
                            "component": "ose-installer",
                            "containerImage": "registry.redhat.io/openshift4/ose-installer@sha256:def",
                            "architecture": "amd64",
                        },
                    ]
                }
            }
        }
        mock_resp = MagicMock()
        mock_resp.text = yaml.dump(advisory_content)
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        components, version = fetch_shipment_components("https://gitlab.example.com/mr/1")

        self.assertEqual(version, "4.18.51")
        self.assertEqual(len(components), 2)
        self.assertEqual(components[0][0], "ose-cli")
        self.assertIn("sha256:abc", components[0][1])
        mock_requests.get.assert_called_once_with(
            "https://errata.devel.redhat.com/advisory/12345", timeout=30, allow_redirects=False
        )

    @patch("elliottlib.cli.verify_image_grades_cli.requests")
    @patch("elliottlib.cli.verify_image_grades_cli.GitLabClient")
    def test_skips_fbc_files(self, mock_gl_cls, mock_requests):
        mock_diff = MagicMock()
        mock_diff.diffs = [{"new_path": "shipments/4.18/4.18.51.fbc.yaml"}]

        mock_mr = MagicMock()
        mock_mr.title = "Shipment for 4.18.51"
        mock_mr.source_branch = "shipment-4.18.51"
        mock_mr.source_project_id = 123
        mock_mr.diffs.list.return_value = [MagicMock(id=1)]
        mock_mr.diffs.get.return_value = mock_diff

        mock_gl = MagicMock()
        mock_gl.get_mr_from_url.return_value = mock_mr
        mock_gl.get_project.return_value = MagicMock()
        mock_gl_cls.from_url.return_value = mock_gl

        components, _ = fetch_shipment_components("https://gitlab.example.com/mr/1")

        self.assertEqual(len(components), 0)
        mock_requests.get.assert_not_called()

    @patch("elliottlib.cli.verify_image_grades_cli.requests")
    @patch("elliottlib.cli.verify_image_grades_cli.GitLabClient")
    def test_skips_files_without_advisory_url(self, mock_gl_cls, mock_requests):
        shipment_yaml = """
shipment:
  snapshot:
    spec:
      components:
        - name: something
          containerImage: "quay.io/something@sha256:abc"
"""
        mock_file = MagicMock()
        mock_file.decode.return_value = shipment_yaml.encode("utf-8")

        mock_project = MagicMock()
        mock_project.files.get.return_value = mock_file

        mock_diff = MagicMock()
        mock_diff.diffs = [{"new_path": "shipments/4.18/4.18.51.yaml"}]

        mock_mr = MagicMock()
        mock_mr.title = "Shipment for 4.18.51"
        mock_mr.source_branch = "shipment-4.18.51"
        mock_mr.source_project_id = 123
        mock_mr.diffs.list.return_value = [MagicMock(id=1)]
        mock_mr.diffs.get.return_value = mock_diff

        mock_gl = MagicMock()
        mock_gl.get_mr_from_url.return_value = mock_mr
        mock_gl.get_project.return_value = mock_project
        mock_gl_cls.from_url.return_value = mock_gl

        components, _ = fetch_shipment_components("https://gitlab.example.com/mr/1")

        self.assertEqual(len(components), 0)
        mock_requests.get.assert_not_called()

    @patch("elliottlib.cli.verify_image_grades_cli.requests")
    @patch("elliottlib.cli.verify_image_grades_cli.GitLabClient")
    def test_raises_when_all_files_fail(self, mock_gl_cls, mock_requests):
        mock_file = MagicMock()
        mock_file.decode.side_effect = RuntimeError("decode error")

        mock_project = MagicMock()
        mock_project.files.get.return_value = mock_file

        mock_diff = MagicMock()
        mock_diff.diffs = [{"new_path": "shipments/4.18/4.18.51.yaml"}]

        mock_mr = MagicMock()
        mock_mr.title = "Shipment for 4.18.51"
        mock_mr.source_branch = "shipment-4.18.51"
        mock_mr.source_project_id = 123
        mock_mr.diffs.list.return_value = [MagicMock(id=1)]
        mock_mr.diffs.get.return_value = mock_diff

        mock_gl = MagicMock()
        mock_gl.get_mr_from_url.return_value = mock_mr
        mock_gl.get_project.return_value = mock_project
        mock_gl_cls.from_url.return_value = mock_gl

        with self.assertRaises(RuntimeError) as ctx:
            fetch_shipment_components("https://gitlab.example.com/mr/1")
        self.assertIn("Failed to read any shipment file", str(ctx.exception))

    @patch("elliottlib.cli.verify_image_grades_cli.requests")
    @patch("elliottlib.cli.verify_image_grades_cli.GitLabClient")
    def test_accepts_gitlab_advisory_url(self, mock_gl_cls, mock_requests):
        shipment_yaml = """
shipment:
  environments:
    stage:
      advisory:
        internal_url: "https://gitlab.cee.redhat.com/rhtap-release/advisories/-/raw/main/data/advisories/ocp/2026/12345/advisory.yaml"
"""
        mock_file = MagicMock()
        mock_file.decode.return_value = shipment_yaml.encode("utf-8")

        mock_project = MagicMock()
        mock_project.files.get.return_value = mock_file

        mock_diff = MagicMock()
        mock_diff.diffs = [{"new_path": "shipments/4.18/4.18.51.yaml"}]

        mock_mr = MagicMock()
        mock_mr.title = "Shipment for 4.18.51"
        mock_mr.source_branch = "shipment-4.18.51"
        mock_mr.source_project_id = 123
        mock_mr.diffs.list.return_value = [MagicMock(id=1)]
        mock_mr.diffs.get.return_value = mock_diff

        mock_gl = MagicMock()
        mock_gl.get_mr_from_url.return_value = mock_mr
        mock_gl.get_project.return_value = mock_project
        mock_gl_cls.from_url.return_value = mock_gl

        advisory_content = {
            "spec": {
                "content": {
                    "images": [
                        {"component": "ose-cli", "containerImage": "registry.redhat.io/openshift4/ose-cli@sha256:abc"},
                    ]
                }
            }
        }
        mock_resp = MagicMock()
        mock_resp.text = yaml.dump(advisory_content)
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        components, _ = fetch_shipment_components("https://gitlab.example.com/mr/1")
        self.assertEqual(len(components), 1)
        self.assertEqual(components[0][0], "ose-cli")

    @patch("elliottlib.cli.verify_image_grades_cli.requests")
    @patch("elliottlib.cli.verify_image_grades_cli.GitLabClient")
    def test_rejects_disallowed_advisory_host(self, mock_gl_cls, mock_requests):
        shipment_yaml = """
shipment:
  environments:
    stage:
      advisory:
        internal_url: "https://evil.example.com/advisory/12345"
"""
        mock_file = MagicMock()
        mock_file.decode.return_value = shipment_yaml.encode("utf-8")

        mock_project = MagicMock()
        mock_project.files.get.return_value = mock_file

        mock_diff = MagicMock()
        mock_diff.diffs = [{"new_path": "shipments/4.18/4.18.51.yaml"}]

        mock_mr = MagicMock()
        mock_mr.title = "Shipment for 4.18.51"
        mock_mr.source_branch = "shipment-4.18.51"
        mock_mr.source_project_id = 123
        mock_mr.diffs.list.return_value = [MagicMock(id=1)]
        mock_mr.diffs.get.return_value = mock_diff

        mock_gl = MagicMock()
        mock_gl.get_mr_from_url.return_value = mock_mr
        mock_gl.get_project.return_value = mock_project
        mock_gl_cls.from_url.return_value = mock_gl

        with self.assertRaises(RuntimeError) as ctx:
            fetch_shipment_components("https://gitlab.example.com/mr/1")
        self.assertIn("Failed to read any shipment file", str(ctx.exception))
        mock_requests.get.assert_not_called()


class TestVerifyImageGrades(unittest.IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_image_grades_cli.fetch_shipment_components")
    @patch("elliottlib.cli.verify_image_grades_cli.query_freshness_grades")
    async def test_all_healthy(self, mock_query, mock_fetch):
        mock_fetch.return_value = (
            [
                ("ose-cli", "registry.redhat.io/openshift4/ose-cli@sha256:aaa"),
                ("ose-installer", "registry.redhat.io/openshift4/ose-installer@sha256:bbb"),
            ],
            "4.18.51",
        )
        past = "2026-01-01T00:00:00+00:00"
        mock_query.return_value = ([{"start_date": past, "grade": "A"}], True)

        result = await verify_image_grades("https://gitlab.example.com/mr/1")

        self.assertTrue(result.passed)
        self.assertEqual(result.total_scanned, 2)
        self.assertEqual(len(result.unhealthy_images), 0)
        self.assertEqual(len(result.unavailable_images), 0)

    @patch("elliottlib.cli.verify_image_grades_cli.fetch_shipment_components")
    @patch("elliottlib.cli.verify_image_grades_cli.query_freshness_grades")
    async def test_unhealthy_detected(self, mock_query, mock_fetch):
        mock_fetch.return_value = (
            [("ose-cli", "registry.redhat.io/openshift4/ose-cli@sha256:aaa")],
            "4.18.51",
        )
        past = "2026-01-01T00:00:00+00:00"
        mock_query.return_value = ([{"start_date": past, "grade": "D"}], True)

        result = await verify_image_grades("https://gitlab.example.com/mr/1")

        self.assertFalse(result.passed)
        self.assertEqual(len(result.unhealthy_images), 1)
        self.assertEqual(result.unhealthy_images[0].grade, "D")
        self.assertTrue(result.unhealthy_images[0].available)

    @patch("elliottlib.cli.verify_image_grades_cli.fetch_shipment_components")
    @patch("elliottlib.cli.verify_image_grades_cli.query_freshness_grades")
    async def test_unavailable_tracked(self, mock_query, mock_fetch):
        mock_fetch.return_value = (
            [("ose-cli", "registry.redhat.io/openshift4/ose-cli@sha256:aaa")],
            "4.18.51",
        )
        mock_query.return_value = ([], False)

        result = await verify_image_grades("https://gitlab.example.com/mr/1")

        self.assertFalse(result.passed)
        self.assertEqual(len(result.unavailable_images), 1)
        self.assertFalse(result.results[0].available)

    @patch("elliottlib.cli.verify_image_grades_cli.fetch_shipment_components")
    @patch("elliottlib.cli.verify_image_grades_cli.query_freshness_grades")
    async def test_no_digest_in_pullspec(self, mock_query, mock_fetch):
        mock_fetch.return_value = (
            [("ose-cli", "registry.redhat.io/openshift4/ose-cli:v4.18")],
            "4.18.51",
        )

        result = await verify_image_grades("https://gitlab.example.com/mr/1")

        self.assertEqual(result.results[0].grade, "Unknown")
        self.assertFalse(result.results[0].healthy)
        self.assertFalse(result.results[0].available)
        mock_query.assert_not_called()


class TestRenderResult(unittest.TestCase):
    def _make_result(self, passed=True):
        grades = ["A", "B"] if passed else ["A", "D", "Unknown"]
        return VerifyImageGradesResult(
            shipment_mr_url="https://gitlab.example.com/mr/1",
            shipment_version="4.18.51",
            results=[
                ImageGradeResult(name=f"img-{i}", pullspec=f"ps-{i}", digest=f"d-{i}", grade=g)
                for i, g in enumerate(grades)
            ],
        )

    def test_text_pass(self):
        text = render_result(self._make_result(passed=True), "text")
        self.assertIn("PASS", text)
        self.assertIn("4.18.51", text)
        self.assertNotIn("UNHEALTHY", text)

    def test_text_fail(self):
        text = render_result(self._make_result(passed=False), "text")
        self.assertIn("FAIL", text)
        self.assertIn("UNHEALTHY IMAGES", text)
        self.assertIn("grade D", text)

    def test_json_output(self):
        out = render_result(self._make_result(passed=True), "json")
        data = json.loads(out)
        self.assertTrue(data["passed"])
        self.assertEqual(data["total_scanned"], 2)
        self.assertEqual(len(data["unhealthy_images"]), 0)
        self.assertEqual(len(data["results"]), 2)

    def test_json_unhealthy(self):
        out = render_result(self._make_result(passed=False), "json")
        data = json.loads(out)
        self.assertFalse(data["passed"])
        self.assertEqual(data["unhealthy_count"], 2)
        self.assertEqual(data["unknown_count"], 1)

    def test_text_unavailable_separate_from_unhealthy(self):
        result = VerifyImageGradesResult(
            shipment_mr_url="https://gitlab.example.com/mr/1",
            shipment_version="4.18.51",
            results=[
                ImageGradeResult(name="ok", pullspec="ps-ok", digest="d-ok", grade="A"),
                ImageGradeResult(name="bad", pullspec="ps-bad", digest="d-bad", grade="D"),
                ImageGradeResult(name="down", pullspec="ps-down", digest="d-down", grade="Unknown", available=False),
            ],
        )
        text = render_result(result, "text")
        self.assertIn("UNAVAILABLE (grade lookup failed):", text)
        self.assertIn("down", text)
        self.assertIn("UNHEALTHY IMAGES:", text)
        self.assertIn("bad", text)

    def test_json_unavailable(self):
        result = VerifyImageGradesResult(
            shipment_mr_url="https://gitlab.example.com/mr/1",
            shipment_version="4.18.51",
            results=[
                ImageGradeResult(name="down", pullspec="ps-down", digest="", grade="Unknown", available=False),
            ],
        )
        out = render_result(result, "json")
        data = json.loads(out)
        self.assertEqual(data["unavailable_count"], 1)
        self.assertEqual(len(data["unavailable_images"]), 1)
        self.assertFalse(data["results"][0]["available"])


if __name__ == "__main__":
    unittest.main()
