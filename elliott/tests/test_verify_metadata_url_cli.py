import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.verify_metadata_url_cli import (
    VerifyMetadataUrlResult,
    check_url_accessible,
    extract_metadata_url,
    get_release_pullspec,
    render_result,
    verify_metadata_url,
)


class TestVerifyMetadataUrlResult(TestCase):
    def test_passed(self):
        r = VerifyMetadataUrlResult(
            release="4.18",
            pullspec="quay.io/openshift-release-dev/ocp-release:4.18.51-x86_64",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=True,
        )
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_not_accessible(self):
        r = VerifyMetadataUrlResult(
            release="4.18",
            pullspec="quay.io/test:4.18",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=False,
        )
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_error(self):
        r = VerifyMetadataUrlResult(release="4.18", error="connection failed")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_error_overrides_accessible(self):
        r = VerifyMetadataUrlResult(
            release="4.18",
            accessible=True,
            error="some warning",
        )
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)


class TestGetReleasePullspec(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_metadata_url_cli.aiohttp.ClientSession")
    async def test_success(self, mock_session_cls):
        mock_resp = AsyncMock()
        mock_resp.ok = True
        mock_resp.json = AsyncMock(
            return_value={"pullSpec": "quay.io/openshift-release-dev/ocp-release:4.18.51-x86_64"}
        )
        mock_resp.release = MagicMock()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_resp)
        mock_session_cls.return_value = mock_session

        result = await get_release_pullspec("4.18")
        self.assertEqual(result, "quay.io/openshift-release-dev/ocp-release:4.18.51-x86_64")

    @patch("elliottlib.cli.verify_metadata_url_cli.aiohttp.ClientSession")
    async def test_api_error(self, mock_session_cls):
        mock_resp = AsyncMock()
        mock_resp.ok = False
        mock_resp.status = 404
        mock_resp.release = MagicMock()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_resp)
        mock_session_cls.return_value = mock_session

        with self.assertRaises(RuntimeError):
            await get_release_pullspec("4.99")

    @patch("elliottlib.cli.verify_metadata_url_cli.aiohttp.ClientSession")
    async def test_no_pullspec_in_response(self, mock_session_cls):
        mock_resp = AsyncMock()
        mock_resp.ok = True
        mock_resp.json = AsyncMock(return_value={"name": "4.18.51"})
        mock_resp.release = MagicMock()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_resp)
        mock_session_cls.return_value = mock_session

        with self.assertRaises(RuntimeError):
            await get_release_pullspec("4.18")

    @patch("elliottlib.cli.verify_metadata_url_cli.aiohttp.ClientSession")
    async def test_major_version_5(self, mock_session_cls):
        mock_resp = AsyncMock()
        mock_resp.ok = True
        mock_resp.json = AsyncMock(return_value={"pullSpec": "quay.io/openshift-release-dev/ocp-release:5.0.3-x86_64"})
        mock_resp.release = MagicMock()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_resp)
        mock_session_cls.return_value = mock_session

        result = await get_release_pullspec("5.0")
        self.assertEqual(result, "quay.io/openshift-release-dev/ocp-release:5.0.3-x86_64")
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        self.assertIn("5-stable", str(call_args))


class TestExtractMetadataUrl(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_metadata_url_cli.exectools.cmd_gather_async")
    async def test_success(self, mock_cmd):
        release_info = {"metadata": {"metadata": {"url": "https://access.redhat.com/errata/RHBA-2025:1234"}}}
        mock_cmd.return_value = (0, json.dumps(release_info), "")

        result = await extract_metadata_url("quay.io/test:4.18")
        self.assertEqual(result, "https://access.redhat.com/errata/RHBA-2025:1234")

    @patch("elliottlib.cli.verify_metadata_url_cli.exectools.cmd_gather_async")
    async def test_oc_command_fails(self, mock_cmd):
        mock_cmd.return_value = (1, "", "error: unauthorized")

        with self.assertRaises(RuntimeError) as ctx:
            await extract_metadata_url("quay.io/test:4.18")
        self.assertIn("oc adm release info failed", str(ctx.exception))

    @patch("elliottlib.cli.verify_metadata_url_cli.exectools.cmd_gather_async")
    async def test_missing_metadata_key(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps({"metadata": {}}), "")

        with self.assertRaises(RuntimeError) as ctx:
            await extract_metadata_url("quay.io/test:4.18")
        self.assertIn("not found", str(ctx.exception))

    @patch("elliottlib.cli.verify_metadata_url_cli.exectools.cmd_gather_async")
    async def test_empty_url(self, mock_cmd):
        release_info = {"metadata": {"metadata": {"url": ""}}}
        mock_cmd.return_value = (0, json.dumps(release_info), "")

        with self.assertRaises(RuntimeError) as ctx:
            await extract_metadata_url("quay.io/test:4.18")
        self.assertIn("empty", str(ctx.exception))


class TestCheckUrlAccessible(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_metadata_url_cli.aiohttp.ClientSession")
    async def test_accessible(self, mock_session_cls):
        mock_resp = AsyncMock()
        mock_resp.ok = True
        mock_resp.status = 200
        mock_resp.release = MagicMock()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_resp)
        mock_session_cls.return_value = mock_session

        result = await check_url_accessible("https://access.redhat.com/errata/RHBA-2025:1234")
        self.assertTrue(result)

    @patch("elliottlib.cli.verify_metadata_url_cli.aiohttp.ClientSession")
    async def test_not_accessible(self, mock_session_cls):
        mock_resp = AsyncMock()
        mock_resp.ok = False
        mock_resp.status = 404
        mock_resp.release = MagicMock()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_resp)
        mock_session_cls.return_value = mock_session

        result = await check_url_accessible("https://access.redhat.com/errata/RHBA-2025:9999")
        self.assertFalse(result)


class TestVerifyMetadataUrl(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible")
    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    @patch("elliottlib.cli.verify_metadata_url_cli.get_release_pullspec")
    async def test_all_pass(self, mock_pullspec, mock_extract, mock_check):
        mock_pullspec.return_value = "quay.io/test:4.18.51"
        mock_extract.return_value = "https://access.redhat.com/errata/RHBA-2025:1234"
        mock_check.return_value = True

        result = await verify_metadata_url("4.18")
        self.assertTrue(result.passed)
        self.assertEqual(result.pullspec, "quay.io/test:4.18.51")
        self.assertEqual(result.metadata_url, "https://access.redhat.com/errata/RHBA-2025:1234")

    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible")
    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    @patch("elliottlib.cli.verify_metadata_url_cli.get_release_pullspec")
    async def test_url_not_accessible(self, mock_pullspec, mock_extract, mock_check):
        mock_pullspec.return_value = "quay.io/test:4.18.51"
        mock_extract.return_value = "https://access.redhat.com/errata/RHBA-2025:1234"
        mock_check.return_value = False

        result = await verify_metadata_url("4.18")
        self.assertFalse(result.passed)
        self.assertTrue(result.failed)
        self.assertFalse(result.accessible)

    @patch("elliottlib.cli.verify_metadata_url_cli.get_release_pullspec")
    async def test_pullspec_error(self, mock_pullspec):
        mock_pullspec.side_effect = RuntimeError("API unreachable")

        result = await verify_metadata_url("4.18")
        self.assertFalse(result.passed)
        self.assertIn("API unreachable", result.error)

    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    @patch("elliottlib.cli.verify_metadata_url_cli.get_release_pullspec")
    async def test_oc_error(self, mock_pullspec, mock_extract):
        mock_pullspec.return_value = "quay.io/test:4.18.51"
        mock_extract.side_effect = RuntimeError("oc adm release info failed")

        result = await verify_metadata_url("4.18")
        self.assertFalse(result.passed)
        self.assertIn("oc adm release info failed", result.error)

    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible")
    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    @patch("elliottlib.cli.verify_metadata_url_cli.get_release_pullspec")
    async def test_http_check_error(self, mock_pullspec, mock_extract, mock_check):
        mock_pullspec.return_value = "quay.io/test:4.18.51"
        mock_extract.return_value = "https://access.redhat.com/errata/RHBA-2025:1234"
        mock_check.side_effect = Exception("connection timeout")

        result = await verify_metadata_url("4.18")
        self.assertFalse(result.passed)
        self.assertIn("connection timeout", result.error)


class TestRenderResult(TestCase):
    def test_text_pass(self):
        r = VerifyMetadataUrlResult(
            release="4.18",
            pullspec="quay.io/test:4.18.51",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=True,
        )
        text = render_result(r, "text")
        self.assertIn("PASS", text)
        self.assertIn("4.18", text)
        self.assertIn("access.redhat.com", text)

    def test_text_fail(self):
        r = VerifyMetadataUrlResult(
            release="4.18",
            pullspec="quay.io/test:4.18.51",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=False,
        )
        text = render_result(r, "text")
        self.assertIn("FAIL", text)
        self.assertIn("no", text)

    def test_text_error(self):
        r = VerifyMetadataUrlResult(release="4.18", error="API unreachable")
        text = render_result(r, "text")
        self.assertIn("FAIL", text)
        self.assertIn("API unreachable", text)

    def test_json_pass(self):
        r = VerifyMetadataUrlResult(
            release="4.18",
            pullspec="quay.io/test:4.18.51",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=True,
        )
        data = json.loads(render_result(r, "json"))
        self.assertTrue(data["passed"])
        self.assertFalse(data["failed"])
        self.assertEqual(data["release"], "4.18")
        self.assertEqual(data["metadata_url"], "https://access.redhat.com/errata/RHBA-2025:1234")

    def test_json_fail(self):
        r = VerifyMetadataUrlResult(release="4.18", error="boom")
        data = json.loads(render_result(r, "json"))
        self.assertFalse(data["passed"])
        self.assertTrue(data["failed"])
        self.assertEqual(data["error"], "boom")
