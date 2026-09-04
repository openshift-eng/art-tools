import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from elliottlib.cli.verify_signatures_cli import (
    SignatureCheckResult,
    VerifySignaturesResult,
    check_signature_on_mirror,
    get_release_image_digest,
    render_result,
    verify_release_signatures,
)


class TestSignatureCheckResult(TestCase):
    def test_passed_dev_only_true(self):
        r = SignatureCheckResult(arch="x86_64", pullspec="p", digest="d", dev_mirror=True)
        self.assertTrue(r.passed)

    def test_passed_dev_only_false(self):
        r = SignatureCheckResult(arch="x86_64", pullspec="p", digest="d", dev_mirror=False)
        self.assertFalse(r.passed)

    def test_passed_both_true(self):
        r = SignatureCheckResult(arch="x86_64", pullspec="p", digest="d", dev_mirror=True, prod_mirror=True)
        self.assertTrue(r.passed)

    def test_passed_mixed(self):
        r = SignatureCheckResult(arch="x86_64", pullspec="p", digest="d", dev_mirror=True, prod_mirror=False)
        self.assertFalse(r.passed)

    def test_passed_no_checks(self):
        r = SignatureCheckResult(arch="x86_64", pullspec="p", digest="d")
        self.assertFalse(r.passed)


class TestVerifySignaturesResult(TestCase):
    def test_passed_all_good(self):
        r = VerifySignaturesResult(
            release_name="4.18.36",
            arch_results=[
                SignatureCheckResult(arch="x86_64", pullspec="p1", digest="d1", dev_mirror=True),
                SignatureCheckResult(arch="aarch64", pullspec="p2", digest="d2", dev_mirror=True),
            ],
        )
        self.assertTrue(r.passed)

    def test_passed_one_fail(self):
        r = VerifySignaturesResult(
            release_name="4.18.36",
            arch_results=[
                SignatureCheckResult(arch="x86_64", pullspec="p1", digest="d1", dev_mirror=True),
                SignatureCheckResult(arch="aarch64", pullspec="p2", digest="d2", dev_mirror=False),
            ],
        )
        self.assertFalse(r.passed)

    def test_passed_with_errors(self):
        r = VerifySignaturesResult(
            release_name="4.18.36",
            arch_results=[
                SignatureCheckResult(arch="x86_64", pullspec="p1", digest="d1", dev_mirror=True),
            ],
            errors=["something went wrong"],
        )
        self.assertFalse(r.passed)

    def test_passed_empty(self):
        r = VerifySignaturesResult(release_name="4.18.36")
        self.assertFalse(r.passed)


class TestCheckSignatureOnMirror(IsolatedAsyncioTestCase):
    async def test_found_signature(self):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("elliottlib.cli.verify_signatures_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await check_signature_on_mirror("abc123", "openshift-release-dev/ocp-release")
        self.assertTrue(result)

    async def test_not_found(self):
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("elliottlib.cli.verify_signatures_cli.aiohttp.ClientSession", return_value=mock_session):
            result = await check_signature_on_mirror("abc123", "openshift-release-dev/ocp-release")
        self.assertFalse(result)

    async def test_server_error_raises(self):
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.request_info = MagicMock()
        mock_response.history = ()
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("elliottlib.cli.verify_signatures_cli.aiohttp.ClientSession", return_value=mock_session):
            with self.assertRaises(aiohttp.ClientResponseError):
                await check_signature_on_mirror("abc123", "openshift-release-dev/ocp-release")


class TestGetReleaseImageDigest(IsolatedAsyncioTestCase):
    async def test_success(self):
        mock_output = json.dumps({"digest": "sha256:abc123"})
        with patch(
            "elliottlib.cli.verify_signatures_cli.exectools.cmd_gather_async", new_callable=AsyncMock
        ) as mock_cmd:
            mock_cmd.return_value = (0, mock_output, "")
            result = await get_release_image_digest("quay.io/openshift-release-dev/ocp-release:4.18.36-x86_64")
        self.assertEqual(result, "sha256:abc123")

    async def test_list_response(self):
        mock_output = json.dumps([{"digest": "sha256:def456"}])
        with patch(
            "elliottlib.cli.verify_signatures_cli.exectools.cmd_gather_async", new_callable=AsyncMock
        ) as mock_cmd:
            mock_cmd.return_value = (0, mock_output, "")
            result = await get_release_image_digest("pullspec")
        self.assertEqual(result, "sha256:def456")

    async def test_command_failure(self):
        with patch(
            "elliottlib.cli.verify_signatures_cli.exectools.cmd_gather_async", new_callable=AsyncMock
        ) as mock_cmd:
            mock_cmd.return_value = (1, "", "error msg")
            with self.assertRaises(RuntimeError):
                await get_release_image_digest("pullspec")

    async def test_missing_digest(self):
        mock_output = json.dumps({"config": {}})
        with patch(
            "elliottlib.cli.verify_signatures_cli.exectools.cmd_gather_async", new_callable=AsyncMock
        ) as mock_cmd:
            mock_cmd.return_value = (0, mock_output, "")
            with self.assertRaises(RuntimeError):
                await get_release_image_digest("pullspec")


class TestVerifyReleaseSignatures(IsolatedAsyncioTestCase):
    async def test_single_arch_pass(self):
        with (
            patch(
                "elliottlib.cli.verify_signatures_cli.get_release_image_digest", new_callable=AsyncMock
            ) as mock_digest,
            patch(
                "elliottlib.cli.verify_signatures_cli.check_signature_on_mirror", new_callable=AsyncMock
            ) as mock_check,
        ):
            mock_digest.return_value = "sha256:abc123"
            mock_check.return_value = True
            result = await verify_release_signatures("4.18.36", ["x86_64"], check_dev=True, check_prod=False)
        self.assertTrue(result.passed)
        self.assertEqual(len(result.arch_results), 1)
        self.assertTrue(result.arch_results[0].dev_mirror)

    async def test_single_arch_fail(self):
        with (
            patch(
                "elliottlib.cli.verify_signatures_cli.get_release_image_digest", new_callable=AsyncMock
            ) as mock_digest,
            patch(
                "elliottlib.cli.verify_signatures_cli.check_signature_on_mirror", new_callable=AsyncMock
            ) as mock_check,
        ):
            mock_digest.return_value = "sha256:abc123"
            mock_check.return_value = False
            result = await verify_release_signatures("4.18.36", ["x86_64"], check_dev=True, check_prod=False)
        self.assertFalse(result.passed)

    async def test_digest_error(self):
        with patch(
            "elliottlib.cli.verify_signatures_cli.get_release_image_digest", new_callable=AsyncMock
        ) as mock_digest:
            mock_digest.side_effect = RuntimeError("oc failed")
            result = await verify_release_signatures("4.18.36", ["x86_64"], check_dev=True, check_prod=False)
        self.assertFalse(result.passed)
        self.assertEqual(len(result.errors), 1)
        self.assertIn("failed to get digest", result.errors[0])

    async def test_multi_arch(self):
        with (
            patch(
                "elliottlib.cli.verify_signatures_cli.get_release_image_digest", new_callable=AsyncMock
            ) as mock_digest,
            patch(
                "elliottlib.cli.verify_signatures_cli.check_signature_on_mirror", new_callable=AsyncMock
            ) as mock_check,
        ):
            mock_digest.return_value = "sha256:abc123"
            mock_check.return_value = True
            result = await verify_release_signatures("4.18.36", ["x86_64", "aarch64"], check_dev=True, check_prod=False)
        self.assertTrue(result.passed)
        self.assertEqual(len(result.arch_results), 2)

    async def test_both_mirrors(self):
        with (
            patch(
                "elliottlib.cli.verify_signatures_cli.get_release_image_digest", new_callable=AsyncMock
            ) as mock_digest,
            patch(
                "elliottlib.cli.verify_signatures_cli.check_signature_on_mirror", new_callable=AsyncMock
            ) as mock_check,
        ):
            mock_digest.return_value = "sha256:abc123"
            mock_check.return_value = True
            result = await verify_release_signatures("4.18.36", ["x86_64"], check_dev=True, check_prod=True)
        self.assertTrue(result.passed)
        self.assertTrue(result.arch_results[0].dev_mirror)
        self.assertTrue(result.arch_results[0].prod_mirror)
        self.assertEqual(mock_check.call_count, 2)


class TestRenderResult(TestCase):
    def test_text_output(self):
        result = VerifySignaturesResult(
            release_name="4.18.36",
            arch_results=[
                SignatureCheckResult(arch="x86_64", pullspec="p1", digest="sha256:abc", dev_mirror=True),
            ],
        )
        text = render_result(result, "text")
        self.assertIn("Release: 4.18.36", text)
        self.assertIn("PASS", text)
        self.assertIn("x86_64", text)

    def test_json_output(self):
        result = VerifySignaturesResult(
            release_name="4.18.36",
            arch_results=[
                SignatureCheckResult(arch="x86_64", pullspec="p1", digest="sha256:abc", dev_mirror=True),
            ],
        )
        text = render_result(result, "json")
        data = json.loads(text)
        self.assertEqual(data["release"], "4.18.36")
        self.assertTrue(data["passed"])
        self.assertEqual(len(data["arch_results"]), 1)

    def test_text_with_errors(self):
        result = VerifySignaturesResult(
            release_name="4.18.36",
            errors=["something broke"],
        )
        text = render_result(result, "text")
        self.assertIn("FAIL", text)
        self.assertIn("something broke", text)
