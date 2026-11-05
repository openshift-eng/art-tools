import asyncio
import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from click.testing import CliRunner
from elliottlib import Runtime
from elliottlib.cli.verify_metadata_url_cli import (
    VerifyMetadataUrlResult,
    check_url_accessible,
    extract_metadata_url,
    validate_metadata_url,
    verify_metadata_url,
    verify_metadata_url_cli,
)
from elliottlib.verify_common import render_verify_result

INVALID_RELEASE_VERSIONS = (
    "4.18",
    "4",
    "5.0.0-ec.5",
    "5.0.0-rc.1",
    "4.18.50-hotfix",
    "4.18.50#suffix",
    "4.18.50&prefix=5.0",
    "4.18.50?prefix=5.0",
    "4.18.50%23suffix",
    "4.18.50+suffix",
    "4.18.50/suffix",
    " 4.18.50",
    "4.18.50 ",
    "4.18.50\n",
    "4.18.50\t",
    "４.１８.５０",
    "v4.18.50",
    "4.18.",
    "4.18.50.1",
)


class TestVerifyMetadataUrlResult(TestCase):
    def test_passed(self):
        r = VerifyMetadataUrlResult(
            release="4.18.51",
            pullspec="quay.io/openshift-release-dev/ocp-release:4.18.51-x86_64",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=True,
        )
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_not_accessible(self):
        r = VerifyMetadataUrlResult(
            release="4.18.51",
            pullspec="quay.io/test:4.18",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=False,
        )
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_error(self):
        r = VerifyMetadataUrlResult(release="4.18.51", error="connection failed")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_error_overrides_accessible(self):
        r = VerifyMetadataUrlResult(
            release="4.18.51",
            accessible=True,
            error="some warning",
        )
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)


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


class TestValidateMetadataUrl(TestCase):
    def test_valid_url(self):
        validate_metadata_url("https://access.redhat.com/errata/RHBA-2025:1234")

    def test_valid_url_with_port_443(self):
        validate_metadata_url("https://access.redhat.com:443/errata/RHBA-2025:1234")

    def test_http_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            validate_metadata_url("http://access.redhat.com/errata/RHBA-2025:1234")
        self.assertIn("https", str(ctx.exception))

    def test_wrong_host_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            validate_metadata_url("https://evil.example.com/errata/RHBA-2025:1234")
        self.assertIn("access.redhat.com", str(ctx.exception))

    def test_subdomain_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            validate_metadata_url("https://access.redhat.com.evil.example/errata/RHBA-2025:1234")
        self.assertIn("access.redhat.com", str(ctx.exception))

    def test_non_standard_port_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            validate_metadata_url("https://access.redhat.com:8443/errata/RHBA-2025:1234")
        self.assertIn("port", str(ctx.exception))

    def test_userinfo_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            validate_metadata_url("https://user:pass@access.redhat.com/errata/RHBA-2025:1234")
        self.assertIn("credentials", str(ctx.exception))


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

    @patch("elliottlib.cli.verify_metadata_url_cli.aiohttp.ClientSession")
    async def test_only_http_200_is_accessible(self, mock_session_cls):
        url = "https://access.redhat.com/errata/RHBA-2025:1234"
        for status in (201, 204, 301, 302, 303, 304, 307, 308):
            with self.subTest(status=status):
                response = MagicMock(status=status, ok=True)
                session = AsyncMock()
                session.get.return_value = response
                mock_session_cls.return_value = session

                result = await check_url_accessible(url)

                self.assertFalse(result)
                session.get.assert_awaited_once_with(url, timeout=ANY, allow_redirects=False)
                response.release.assert_called_once_with()
                session.close.assert_awaited_once_with()


class TestVerifyMetadataUrl(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible")
    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    async def test_exact_release_payload(self, mock_extract, mock_check):
        mock_extract.return_value = "https://access.redhat.com/errata/RHBA-2025:1234"
        mock_check.return_value = True
        for release in ("4.18.0", "4.18.5", "4.18.50", "5.0.3"):
            with self.subTest(release=release):
                mock_extract.reset_mock()
                mock_check.reset_mock()
                pullspec = f"quay.io/openshift-release-dev/ocp-release:{release}-x86_64"

                result = await verify_metadata_url(release)

                self.assertTrue(result.passed)
                self.assertEqual(result.release, release)
                self.assertEqual(result.pullspec, pullspec)
                self.assertEqual(result.metadata_url, mock_extract.return_value)
                mock_extract.assert_awaited_once_with(pullspec, registry_config=None)
                mock_check.assert_awaited_once_with(mock_extract.return_value)

    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible")
    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    async def test_url_not_accessible(self, mock_extract, mock_check):
        mock_extract.return_value = "https://access.redhat.com/errata/RHBA-2025:1234"
        mock_check.return_value = False

        result = await verify_metadata_url("4.18.51")
        self.assertFalse(result.passed)
        self.assertTrue(result.failed)
        self.assertFalse(result.accessible)

    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible")
    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    async def test_oc_error(self, mock_extract, mock_check):
        mock_extract.side_effect = RuntimeError("oc adm release info failed")

        result = await verify_metadata_url("4.18.51")
        self.assertFalse(result.passed)
        self.assertIn("oc adm release info failed", result.error)
        mock_check.assert_not_awaited()

    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible")
    @patch("elliottlib.cli.verify_metadata_url_cli.extract_metadata_url")
    async def test_http_check_error(self, mock_extract, mock_check):
        mock_extract.return_value = "https://access.redhat.com/errata/RHBA-2025:1234"
        mock_check.side_effect = Exception("connection timeout")

        result = await verify_metadata_url("4.18.51")
        self.assertFalse(result.passed)
        self.assertIn("connection timeout", result.error)


class TestMetadataRegistryAuthentication(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_metadata_url_cli.check_url_accessible", return_value=True)
    @patch("elliottlib.cli.verify_metadata_url_cli.exectools.cmd_gather_async")
    async def test_registry_credentials_reach_oc(self, mock_cmd, mock_check):
        url = "https://access.redhat.com/errata/RHBA-2025:1234"
        mock_cmd.return_value = (0, json.dumps({"metadata": {"metadata": {"url": url}}}), "")
        for expected_auth in ("/jenkins/quay-auth.json", None):
            with self.subTest(registry_config=expected_auth):
                mock_cmd.reset_mock()
                mock_check.reset_mock()
                runtime = Runtime(assembly="4.21.41", registry_config=expected_auth)
                result = await verify_metadata_url(runtime.assembly, registry_config=runtime.registry_config)

                self.assertTrue(result.passed)
                expected_cmd = [
                    "oc",
                    "adm",
                    "release",
                    "info",
                    "quay.io/openshift-release-dev/ocp-release:4.21.41-x86_64",
                    "-o",
                    "json",
                ]
                if expected_auth:
                    expected_cmd.append(f"--registry-config={expected_auth}")
                mock_cmd.assert_awaited_once_with(expected_cmd, check=False)
                mock_check.assert_awaited_once_with(url)


class TestVerifyMetadataUrlCLI(TestCase):
    def setUp(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.addCleanup(loop.close)
        self.addCleanup(asyncio.set_event_loop, None)
        self.runner = CliRunner()
        initialize_patch = patch.object(
            Runtime, "initialize", side_effect=AssertionError("Runtime must not initialize")
        )
        self.mock_initialize = initialize_patch.start()
        self.addCleanup(initialize_patch.stop)
        verify_patch = patch("elliottlib.cli.verify_metadata_url_cli.verify_metadata_url", new_callable=AsyncMock)
        self.mock_verify = verify_patch.start()
        self.addCleanup(verify_patch.stop)

    def test_release_without_group_or_runtime_initialization(self):
        for release in ("4.18.50", "5.0.3"):
            with self.subTest(release=release):
                self.mock_verify.reset_mock()
                self.mock_verify.return_value = VerifyMetadataUrlResult(release=release, accessible=True)

                result = self.runner.invoke(verify_metadata_url_cli, [], obj=Runtime(assembly=release))

                self.assertEqual(result.exit_code, 0, result.output)
                self.assertIn("Overall: PASS", result.output)
                self.mock_verify.assert_awaited_once_with(release=release, registry_config=None)
                self.mock_initialize.assert_not_called()

    def test_explicit_registry_config_is_forwarded(self):
        self.mock_verify.return_value = VerifyMetadataUrlResult(release="4.21.41", accessible=True)
        runtime = Runtime(assembly="4.21.41", registry_config="/explicit/auth.json")

        result = self.runner.invoke(verify_metadata_url_cli, [], obj=runtime)

        self.assertEqual(result.exit_code, 0, result.output)
        self.mock_verify.assert_awaited_once_with(release="4.21.41", registry_config="/explicit/auth.json")
        self.mock_initialize.assert_not_called()

    def test_missing_or_default_assembly_is_rejected(self):
        for assembly in (None, "", "stream"):
            with self.subTest(assembly=assembly):
                result = self.runner.invoke(verify_metadata_url_cli, [], obj=Runtime(assembly=assembly))

                self.assertEqual(result.exit_code, 2, result.output)
                self.assertIn("Invalid value for --assembly", result.output)
                self.mock_verify.assert_not_awaited()
                self.mock_initialize.assert_not_called()

    def test_invalid_release_is_rejected(self):
        self.mock_verify.return_value = VerifyMetadataUrlResult(accessible=True)
        for release in INVALID_RELEASE_VERSIONS:
            for output in ("text", "json"):
                with self.subTest(release=release, output=output):
                    result = self.runner.invoke(
                        verify_metadata_url_cli, ["--output", output], obj=Runtime(assembly=release)
                    )

                    self.assertEqual(result.exit_code, 2, result.output)
                    self.assertIn("Invalid value for --assembly", result.output)
                    self.assertIn("X.Y.Z", result.output)
                    self.assertNotIn("PASS", result.output)
                    self.mock_verify.assert_not_awaited()

    def test_json_success(self):
        release = "4.18.50"
        self.mock_verify.return_value = VerifyMetadataUrlResult(release=release, accessible=True)

        result = self.runner.invoke(verify_metadata_url_cli, ["--output", "json"], obj=Runtime(assembly=release))

        self.assertEqual(result.exit_code, 0, result.output)
        data = json.loads(result.output)
        self.assertTrue(data["passed"])
        self.assertEqual(data["release"], release)
        self.mock_initialize.assert_not_called()

    def test_json_failure_returns_nonzero(self):
        release = "4.18.50"
        self.mock_verify.return_value = VerifyMetadataUrlResult(release=release, accessible=False)

        result = self.runner.invoke(verify_metadata_url_cli, ["--output", "json"], obj=Runtime(assembly=release))

        self.assertEqual(result.exit_code, 1, result.output)
        data = json.loads(result.output)
        self.assertFalse(data["passed"])
        self.assertTrue(data["failed"])
        self.mock_initialize.assert_not_called()


class TestRenderResult(TestCase):
    def test_text_pass(self):
        r = VerifyMetadataUrlResult(
            release="4.18.51",
            pullspec="quay.io/test:4.18.51",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=True,
        )
        text = render_verify_result(r, "text")
        self.assertIn("PASS", text)
        self.assertIn("4.18.51", text)
        self.assertIn("access.redhat.com", text)

    def test_text_fail(self):
        r = VerifyMetadataUrlResult(
            release="4.18.51",
            pullspec="quay.io/test:4.18.51",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=False,
        )
        text = render_verify_result(r, "text")
        self.assertIn("FAIL", text)
        self.assertIn("no", text)

    def test_text_error(self):
        r = VerifyMetadataUrlResult(release="4.18.51", error="API unreachable")
        text = render_verify_result(r, "text")
        self.assertIn("FAIL", text)
        self.assertIn("API unreachable", text)

    def test_json_pass(self):
        r = VerifyMetadataUrlResult(
            release="4.18.51",
            pullspec="quay.io/test:4.18.51",
            metadata_url="https://access.redhat.com/errata/RHBA-2025:1234",
            accessible=True,
        )
        data = json.loads(render_verify_result(r, "json"))
        self.assertTrue(data["passed"])
        self.assertFalse(data["failed"])
        self.assertEqual(data["release"], "4.18.51")
        self.assertEqual(data["metadata_url"], "https://access.redhat.com/errata/RHBA-2025:1234")

    def test_json_fail(self):
        r = VerifyMetadataUrlResult(release="4.18.51", error="boom")
        data = json.loads(render_verify_result(r, "json"))
        self.assertFalse(data["passed"])
        self.assertTrue(data["failed"])
        self.assertEqual(data["error"], "boom")
