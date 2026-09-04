import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.verify_qe_qualifier_cli import (
    QualifierCheckResult,
    VerifyQeQualifierResult,
    check_qe_qualifier,
    render_result,
    verify_qe_qualifier,
)


class TestQualifierCheckResult(TestCase):
    def test_passed_badge_earned(self):
        r = QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=True)
        self.assertTrue(r.passed)

    def test_failed_badge_not_earned(self):
        r = QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=False)
        self.assertFalse(r.passed)

    def test_failed_badge_none(self):
        r = QualifierCheckResult(release_tag="4.22.9", arch="amd64")
        self.assertFalse(r.passed)

    def test_failed_with_error(self):
        r = QualifierCheckResult(release_tag="4.22.9", arch="amd64", error="not found")
        self.assertFalse(r.passed)


class TestVerifyQeQualifierResult(TestCase):
    def test_passed_all_earned(self):
        r = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=True)],
            nightly_results=[
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="amd64", badge_earned=True)
            ],
        )
        self.assertTrue(r.passed)

    def test_failed_stable_not_earned(self):
        r = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=False)],
            nightly_results=[
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="amd64", badge_earned=True)
            ],
        )
        self.assertFalse(r.passed)

    def test_failed_nightly_not_earned(self):
        r = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=True)],
            nightly_results=[
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="amd64", badge_earned=False)
            ],
        )
        self.assertFalse(r.passed)

    def test_failed_empty(self):
        r = VerifyQeQualifierResult(assembly="4.22.9")
        self.assertFalse(r.passed)

    def test_passed_multi_arch(self):
        r = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[
                QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=True),
                QualifierCheckResult(release_tag="4.22.9", arch="arm64", badge_earned=True),
            ],
            nightly_results=[
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="amd64", badge_earned=True),
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="arm64", badge_earned=True),
            ],
        )
        self.assertTrue(r.passed)

    def test_failed_one_arch_missing(self):
        r = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[
                QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=True),
                QualifierCheckResult(release_tag="4.22.9", arch="arm64", badge_earned=False),
            ],
            nightly_results=[
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="amd64", badge_earned=True),
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="arm64", badge_earned=True),
            ],
        )
        self.assertFalse(r.passed)


def _mock_aiohttp_session(response_status, response_json=None, raise_for_status_error=None):
    mock_response = AsyncMock()
    mock_response.status = response_status
    mock_response.json = AsyncMock(return_value=response_json)
    mock_response.raise_for_status = MagicMock(side_effect=raise_for_status_error)
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_response)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


def _mock_error_session(error):
    mock_session = AsyncMock()
    mock_session.get = MagicMock(side_effect=error)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


class TestCheckQeQualifier(IsolatedAsyncioTestCase):
    async def test_badge_earned(self):
        response_data = {
            "qualifiers": {
                "qe": {
                    "aggregateState": "Success",
                    "badgeName": "QE",
                    "badgeEarned": True,
                    "badgePropagated": True,
                    "approval": True,
                }
            }
        }
        mock_session = _mock_aiohttp_session(200, response_data)
        result = await check_qe_qualifier("4.22.9", "amd64", mock_session)
        self.assertTrue(result.passed)
        self.assertTrue(result.badge_earned)
        self.assertIsNone(result.error)

    async def test_badge_not_earned(self):
        response_data = {
            "qualifiers": {
                "qe": {
                    "aggregateState": "Pending",
                    "badgeName": "QE",
                    "badgeEarned": False,
                    "badgePropagated": False,
                    "approval": False,
                }
            }
        }
        mock_session = _mock_aiohttp_session(200, response_data)
        result = await check_qe_qualifier("4.22.9", "amd64", mock_session)
        self.assertFalse(result.passed)
        self.assertFalse(result.badge_earned)

    async def test_no_qe_qualifier(self):
        response_data = {"qualifiers": {}}
        mock_session = _mock_aiohttp_session(200, response_data)
        result = await check_qe_qualifier("4.22.9", "amd64", mock_session)
        self.assertFalse(result.passed)
        self.assertFalse(result.badge_earned)

    async def test_empty_qualifiers(self):
        response_data = {}
        mock_session = _mock_aiohttp_session(200, response_data)
        result = await check_qe_qualifier("4.22.9", "amd64", mock_session)
        self.assertFalse(result.passed)

    async def test_404_not_found(self):
        mock_session = _mock_aiohttp_session(404)
        result = await check_qe_qualifier("4.22.99", "amd64", mock_session)
        self.assertFalse(result.passed)
        self.assertIn("not found", result.error)

    async def test_network_error(self):
        mock_session = _mock_error_session(Exception("connection refused"))
        result = await check_qe_qualifier("4.22.9", "amd64", mock_session)
        self.assertFalse(result.passed)
        self.assertIn("Failed to query", result.error)

    async def test_http_500_error(self):
        mock_session = _mock_aiohttp_session(500, raise_for_status_error=Exception("500 Server Error"))
        result = await check_qe_qualifier("4.22.9", "amd64", mock_session)
        self.assertFalse(result.passed)
        self.assertIn("Failed to query", result.error)

    async def test_correct_url_construction(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        await check_qe_qualifier("4.22.9", "amd64", mock_session)
        mock_session.get.assert_called_once_with(
            "https://amd64.ocp.releases.ci.openshift.org/api/v1/releasetag/4.22.9/qualifiers"
        )

    async def test_arm64_url(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        await check_qe_qualifier("4.22.9", "arm64", mock_session)
        mock_session.get.assert_called_once_with(
            "https://arm64.ocp.releases.ci.openshift.org/api/v1/releasetag/4.22.9/qualifiers"
        )


class TestVerifyQeQualifier(IsolatedAsyncioTestCase):
    async def test_single_arch_all_pass(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        with patch("elliottlib.cli.verify_qe_qualifier_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await verify_qe_qualifier(
                assembly="4.22.9",
                arches=["x86_64"],
                nightly_tags={"x86_64": "4.22.0-0.nightly-2026-08-05-104816"},
            )
        self.assertTrue(result.passed)
        self.assertEqual(len(result.stable_results), 1)
        self.assertEqual(len(result.nightly_results), 1)

    async def test_single_arch_stable_fail(self):
        async def mock_check(tag, arch, session):
            result = QualifierCheckResult(release_tag=tag, arch=arch)
            result.badge_earned = "nightly" in tag
            return result

        with patch("elliottlib.cli.verify_qe_qualifier_cli.check_qe_qualifier", side_effect=mock_check):
            result = await verify_qe_qualifier(
                assembly="4.22.9",
                arches=["x86_64"],
                nightly_tags={"x86_64": "4.22.0-0.nightly-2026-08-05-104816"},
            )
        self.assertFalse(result.passed)
        self.assertFalse(result.stable_results[0].passed)
        self.assertTrue(result.nightly_results[0].passed)

    async def test_multi_arch(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        with patch("elliottlib.cli.verify_qe_qualifier_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await verify_qe_qualifier(
                assembly="4.22.9",
                arches=["x86_64", "aarch64"],
                nightly_tags={
                    "x86_64": "4.22.0-0.nightly-2026-08-05-104816",
                    "aarch64": "4.22.0-0.nightly-arm64-2026-08-05-104816",
                },
            )
        self.assertTrue(result.passed)
        self.assertEqual(len(result.stable_results), 2)
        self.assertEqual(len(result.nightly_results), 2)

    async def test_no_nightly_for_arch(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        with patch("elliottlib.cli.verify_qe_qualifier_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await verify_qe_qualifier(
                assembly="4.22.9",
                arches=["x86_64"],
                nightly_tags={},
            )
        self.assertTrue(result.passed)
        self.assertEqual(len(result.stable_results), 1)
        self.assertEqual(len(result.nightly_results), 0)

    async def test_check_stable_only(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        with patch("elliottlib.cli.verify_qe_qualifier_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await verify_qe_qualifier(
                assembly="4.22.9",
                arches=["x86_64"],
                nightly_tags={"x86_64": "4.22.0-0.nightly-2026-08-05-104816"},
                check_stable=True,
                check_nightly=False,
            )
        self.assertTrue(result.passed)
        self.assertEqual(len(result.stable_results), 1)
        self.assertEqual(len(result.nightly_results), 0)

    async def test_stable_only_no_nightly_tags(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        with patch("elliottlib.cli.verify_qe_qualifier_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await verify_qe_qualifier(
                assembly="4.22.9",
                arches=["x86_64"],
                nightly_tags={},
                check_stable=True,
                check_nightly=False,
            )
        self.assertTrue(result.passed)
        self.assertEqual(len(result.stable_results), 1)
        self.assertEqual(len(result.nightly_results), 0)

    async def test_check_nightly_only(self):
        response_data = {"qualifiers": {"qe": {"badgeEarned": True}}}
        mock_session = _mock_aiohttp_session(200, response_data)
        with patch("elliottlib.cli.verify_qe_qualifier_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await verify_qe_qualifier(
                assembly="4.22.9",
                arches=["x86_64"],
                nightly_tags={"x86_64": "4.22.0-0.nightly-2026-08-05-104816"},
                check_stable=False,
                check_nightly=True,
            )
        self.assertTrue(result.passed)
        self.assertEqual(len(result.stable_results), 0)
        self.assertEqual(len(result.nightly_results), 1)


class TestRenderResult(TestCase):
    def test_text_all_pass(self):
        result = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=True)],
            nightly_results=[
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="amd64", badge_earned=True)
            ],
        )
        text = render_result(result, "text")
        self.assertIn("Assembly: 4.22.9", text)
        self.assertIn("Overall: PASS", text)
        self.assertIn("Stable:", text)
        self.assertIn("Nightly:", text)

    def test_text_fail(self):
        result = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=False)],
        )
        text = render_result(result, "text")
        self.assertIn("Overall: FAIL", text)

    def test_text_error(self):
        result = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", error="release not found")],
        )
        text = render_result(result, "text")
        self.assertIn("ERROR", text)
        self.assertIn("release not found", text)

    def test_json_output(self):
        result = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", badge_earned=True)],
            nightly_results=[
                QualifierCheckResult(release_tag="4.22.0-0.nightly-2026-08-05-104816", arch="amd64", badge_earned=True)
            ],
        )
        text = render_result(result, "json")
        data = json.loads(text)
        self.assertEqual(data["assembly"], "4.22.9")
        self.assertTrue(data["passed"])
        self.assertEqual(len(data["stable"]), 1)
        self.assertEqual(len(data["nightly"]), 1)
        self.assertTrue(data["stable"][0]["badge_earned"])

    def test_json_with_error(self):
        result = VerifyQeQualifierResult(
            assembly="4.22.9",
            stable_results=[QualifierCheckResult(release_tag="4.22.9", arch="amd64", error="not found")],
        )
        text = render_result(result, "json")
        data = json.loads(text)
        self.assertFalse(data["passed"])
        self.assertEqual(data["stable"][0]["error"], "not found")
