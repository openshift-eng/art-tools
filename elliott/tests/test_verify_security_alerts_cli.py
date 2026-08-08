import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

from elliottlib.cli.verify_security_alerts_cli import (
    AdvisoryAlertResult,
    VerifySecurityAlertsResult,
    check_advisory_security_alerts,
    get_errata_type,
    render_result,
    verify_security_alerts,
)


class TestAdvisoryAlertResult(TestCase):
    def test_ok(self):
        r = AdvisoryAlertResult(advisory_id=1, impetus="rpm", errata_type="rhsa", blocking=False)
        self.assertTrue(r.ok)
        self.assertFalse(r.failed)

    def test_blocking(self):
        r = AdvisoryAlertResult(advisory_id=1, impetus="rpm", errata_type="rhsa", blocking=True)
        self.assertFalse(r.ok)
        self.assertTrue(r.failed)

    def test_error(self):
        r = AdvisoryAlertResult(advisory_id=1, impetus="rpm", error="boom")
        self.assertFalse(r.ok)
        self.assertTrue(r.failed)

    def test_skipped_is_ok(self):
        r = AdvisoryAlertResult(advisory_id=1, impetus="rpm", errata_type="rhba", skipped=True)
        self.assertTrue(r.ok)
        self.assertFalse(r.failed)


class TestVerifySecurityAlertsResult(TestCase):
    def test_all_ok(self):
        r = VerifySecurityAlertsResult(
            advisories=[
                AdvisoryAlertResult(advisory_id=1, impetus="rpm", errata_type="rhsa"),
                AdvisoryAlertResult(advisory_id=2, impetus="rhcos", errata_type="rhba", skipped=True),
            ]
        )
        self.assertTrue(r.ok)
        self.assertFalse(r.failed)

    def test_one_blocking(self):
        r = VerifySecurityAlertsResult(
            advisories=[
                AdvisoryAlertResult(advisory_id=1, impetus="rpm", errata_type="rhsa", blocking=True),
                AdvisoryAlertResult(advisory_id=2, impetus="rhcos", errata_type="rhba", skipped=True),
            ]
        )
        self.assertFalse(r.ok)
        self.assertTrue(r.failed)

    def test_global_errors(self):
        r = VerifySecurityAlertsResult(errors=["oops"])
        self.assertFalse(r.ok)
        self.assertTrue(r.failed)


class TestGetErrataType(TestCase):
    def test_rhsa(self):
        self.assertEqual(get_errata_type({"errata": {"rhsa": {}}}), "rhsa")

    def test_rhba(self):
        self.assertEqual(get_errata_type({"errata": {"rhba": {}}}), "rhba")

    def test_empty(self):
        self.assertEqual(get_errata_type({"errata": {}}), "")

    def test_missing(self):
        self.assertEqual(get_errata_type({}), "")


class TestCheckAdvisorySecurityAlerts(IsolatedAsyncioTestCase):
    async def test_rhba_skipped(self):
        api = AsyncMock()
        api.get_advisory.return_value = {"errata": {"rhba": {}}}
        result = await check_advisory_security_alerts(api, 12345, "rpm")
        self.assertTrue(result.skipped)
        self.assertTrue(result.ok)
        api.refresh_security_alerts.assert_not_called()

    async def test_rhsa_no_blocking(self):
        api = AsyncMock()
        api.get_advisory.return_value = {"errata": {"rhsa": {}}}
        api.refresh_security_alerts.return_value = {"alerts": {"blocking": False, "alerts": []}}
        result = await check_advisory_security_alerts(api, 12345, "rpm")
        self.assertFalse(result.blocking)
        self.assertTrue(result.ok)

    async def test_unknown_type(self):
        api = AsyncMock()
        api.get_advisory.return_value = {}
        result = await check_advisory_security_alerts(api, 12345, "rpm")
        self.assertTrue(result.failed)
        self.assertIn("unable to determine", result.error)
        api.refresh_security_alerts.assert_not_called()

    async def test_rhsa_blocking(self):
        api = AsyncMock()
        api.get_advisory.return_value = {"errata": {"rhsa": {}}}
        api.refresh_security_alerts.return_value = {"alerts": {"blocking": True, "alerts": [{"id": 1}]}}
        result = await check_advisory_security_alerts(api, 12345, "rpm")
        self.assertTrue(result.blocking)
        self.assertTrue(result.failed)

    async def test_rhsa_empty_alerts(self):
        api = AsyncMock()
        api.get_advisory.return_value = {"errata": {"rhsa": {}}}
        api.refresh_security_alerts.return_value = {"alerts": {}}
        result = await check_advisory_security_alerts(api, 12345, "rpm")
        self.assertFalse(result.blocking)
        self.assertTrue(result.ok)

    async def test_api_error(self):
        api = AsyncMock()
        api.get_advisory.side_effect = RuntimeError("connection failed")
        result = await check_advisory_security_alerts(api, 12345, "rpm")
        self.assertTrue(result.failed)
        self.assertIn("connection failed", result.error)

    async def test_refresh_error(self):
        api = AsyncMock()
        api.get_advisory.return_value = {"errata": {"rhsa": {}}}
        api.refresh_security_alerts.side_effect = RuntimeError("refresh failed")
        result = await check_advisory_security_alerts(api, 12345, "rpm")
        self.assertTrue(result.failed)
        self.assertIn("refresh failed", result.error)


class TestVerifySecurityAlerts(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_security_alerts_cli.AsyncErrataAPI")
    async def test_all_ok(self, mock_api_cls):
        api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        api.get_advisory.return_value = {"errata": {"rhba": {}}}

        result = await verify_security_alerts({"rpm": 111, "rhcos": 222})
        self.assertTrue(result.ok)
        self.assertEqual(len(result.advisories), 2)

    @patch("elliottlib.cli.verify_security_alerts_cli.AsyncErrataAPI")
    async def test_rhsa_blocking(self, mock_api_cls):
        api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        api.get_advisory.return_value = {"errata": {"rhsa": {}}}
        api.refresh_security_alerts.return_value = {"alerts": {"blocking": True, "alerts": [{"id": 1}]}}

        result = await verify_security_alerts({"rpm": 111})
        self.assertFalse(result.ok)
        self.assertTrue(result.failed)

    @patch("elliottlib.cli.verify_security_alerts_cli.AsyncErrataAPI")
    async def test_mixed_types(self, mock_api_cls):
        api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        def get_advisory_side_effect(advisory_id):
            if advisory_id == 111:
                return {"errata": {"rhsa": {}}}
            return {"errata": {"rhba": {}}}

        api.get_advisory.side_effect = get_advisory_side_effect
        api.refresh_security_alerts.return_value = {"alerts": {"blocking": False, "alerts": []}}

        result = await verify_security_alerts({"rpm": 111, "rhcos": 222})
        self.assertTrue(result.ok)
        rhsa_result = next(a for a in result.advisories if a.impetus == "rpm")
        rhba_result = next(a for a in result.advisories if a.impetus == "rhcos")
        self.assertFalse(rhsa_result.skipped)
        self.assertTrue(rhba_result.skipped)


class TestRenderResult(TestCase):
    def test_text_ok(self):
        r = VerifySecurityAlertsResult(
            advisories=[
                AdvisoryAlertResult(advisory_id=12345, impetus="rpm", errata_type="rhsa"),
            ]
        )
        text = render_result(r, "text")
        self.assertIn("OK", text)
        self.assertIn("12345", text)

    def test_text_blocking(self):
        r = VerifySecurityAlertsResult(
            advisories=[
                AdvisoryAlertResult(advisory_id=12345, impetus="rpm", errata_type="rhsa", blocking=True),
            ]
        )
        text = render_result(r, "text")
        self.assertIn("BLOCKING", text)
        self.assertIn("FAIL", text)

    def test_text_skipped(self):
        r = VerifySecurityAlertsResult(
            advisories=[
                AdvisoryAlertResult(advisory_id=12345, impetus="rpm", errata_type="rhba", skipped=True),
            ]
        )
        text = render_result(r, "text")
        self.assertIn("SKIPPED", text)
        self.assertIn("RHBA", text)

    def test_json_output(self):
        r = VerifySecurityAlertsResult(
            advisories=[
                AdvisoryAlertResult(advisory_id=12345, impetus="rpm", errata_type="rhsa", blocking=True),
            ]
        )
        data = json.loads(render_result(r, "json"))
        self.assertFalse(data["ok"])
        self.assertTrue(data["failed"])
        self.assertTrue(data["advisories"][0]["blocking"])

    def test_text_error(self):
        r = VerifySecurityAlertsResult(
            advisories=[
                AdvisoryAlertResult(advisory_id=12345, impetus="rpm", error="boom"),
            ]
        )
        text = render_result(r, "text")
        self.assertIn("ERROR", text)
        self.assertIn("boom", text)
