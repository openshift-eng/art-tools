import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.verify_kernel_tag_cli import (
    AdvisoryKernelResult,
    KernelBuildInfo,
    VerifyKernelTagResult,
    check_advisory_kernel_tag,
    check_kernel_tags,
    find_rhcos_nvrs,
    get_kernel_nvrs_from_metadata,
    get_kernel_packages_and_tag,
    get_kernel_rpms_from_rhcos,
    get_rpm_deliveries_config,
    nvr_to_brewroot_metadata_url,
    render_result,
    verify_kernel_tag,
)


class TestKernelBuildInfo(IsolatedAsyncioTestCase):
    def test_no_stop_ship(self):
        kb = KernelBuildInfo(nvr="kernel-5.14.0-284.14.1.el9_2")
        self.assertFalse(kb.has_stop_ship)

    def test_has_stop_ship(self):
        kb = KernelBuildInfo(nvr="kernel-5.14.0-284.14.1.el9_2", has_stop_ship=True)
        self.assertTrue(kb.has_stop_ship)


class TestAdvisoryKernelResult(IsolatedAsyncioTestCase):
    def test_passed_no_kernels(self):
        r = AdvisoryKernelResult(advisory_id=1, impetus="image")
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_passed_with_ok_kernels(self):
        r = AdvisoryKernelResult(
            advisory_id=1,
            impetus="image",
            kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9")],
        )
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_failed_stop_ship(self):
        r = AdvisoryKernelResult(
            advisory_id=1,
            impetus="image",
            kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9", has_stop_ship=True)],
        )
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_failed_error(self):
        r = AdvisoryKernelResult(advisory_id=1, impetus="image", error="some error")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_mixed_kernels(self):
        r = AdvisoryKernelResult(
            advisory_id=1,
            impetus="image",
            kernel_builds=[
                KernelBuildInfo(nvr="kernel-5.14.0-1.el9"),
                KernelBuildInfo(nvr="kernel-rt-5.14.0-1.el9", has_stop_ship=True),
            ],
        )
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)


class TestVerifyKernelTagResult(IsolatedAsyncioTestCase):
    def test_passed(self):
        r = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(
                    advisory_id=1,
                    impetus="image",
                    kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9")],
                )
            ],
            stop_ship_tag="early-kernel-stop-ship",
        )
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_failed(self):
        r = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(
                    advisory_id=1,
                    impetus="image",
                    kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9", has_stop_ship=True)],
                )
            ],
        )
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_empty(self):
        r = VerifyKernelTagResult()
        self.assertFalse(r.passed)
        self.assertFalse(r.failed)

    def test_passed_with_skipped(self):
        r = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(advisory_id=1, impetus="image", skipped=True),
                AdvisoryKernelResult(
                    advisory_id=2,
                    impetus="rhcos",
                    kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9")],
                ),
            ],
        )
        self.assertTrue(r.passed)


class TestGetRpmDeliveriesConfig(IsolatedAsyncioTestCase):
    def test_with_config(self):
        runtime = MagicMock()
        mock_deliveries = MagicMock()
        mock_deliveries.primitive.return_value = [
            {
                "packages": ["kernel"],
                "stop_ship_tag": "early-kernel-stop-ship",
                "integration_tag": "early-kernel-candidate",
            }
        ]
        runtime.group_config.get.return_value = mock_deliveries
        result = get_rpm_deliveries_config(runtime)
        self.assertEqual(len(result), 1)

    def test_without_config(self):
        runtime = MagicMock()
        runtime.group_config.get.return_value = None
        result = get_rpm_deliveries_config(runtime)
        self.assertEqual(result, [])


class TestGetKernelPackagesAndTag(IsolatedAsyncioTestCase):
    def test_standard_config(self):
        config = [
            {
                "packages": ["kernel", "kernel-rt"],
                "stop_ship_tag": "early-kernel-stop-ship",
                "integration_tag": "early-kernel-candidate",
            }
        ]
        packages, tag = get_kernel_packages_and_tag(config)
        self.assertEqual(packages, {"kernel", "kernel-rt"})
        self.assertEqual(tag, "early-kernel-stop-ship")

    def test_no_stop_ship_tag(self):
        config = [{"packages": ["foo"], "integration_tag": "bar"}]
        packages, tag = get_kernel_packages_and_tag(config)
        self.assertEqual(packages, set())
        self.assertEqual(tag, "")

    def test_multiple_entries_same_tag(self):
        config = [
            {"packages": ["kernel"], "stop_ship_tag": "early-kernel-stop-ship", "integration_tag": "ic1"},
            {"packages": ["kernel-rt"], "stop_ship_tag": "early-kernel-stop-ship", "integration_tag": "ic2"},
        ]
        packages, tag = get_kernel_packages_and_tag(config)
        self.assertEqual(packages, {"kernel", "kernel-rt"})
        self.assertEqual(tag, "early-kernel-stop-ship")

    def test_multiple_entries_different_tags_raises(self):
        config = [
            {"packages": ["kernel"], "stop_ship_tag": "tag1", "integration_tag": "ic1"},
            {"packages": ["kernel-rt"], "stop_ship_tag": "tag2", "integration_tag": "ic2"},
        ]
        with self.assertRaises(ValueError):
            get_kernel_packages_and_tag(config)


class TestFindRhcosNvrs(IsolatedAsyncioTestCase):
    def test_finds_rhcos(self):
        builds = {"ose-installer-4.18.0-1.el9", "rhcos-x86_64-418.92.1-1", "rhcos-aarch64-418.92.1-1"}
        result = find_rhcos_nvrs(builds)
        self.assertEqual(result, ["rhcos-aarch64-418.92.1-1", "rhcos-x86_64-418.92.1-1"])

    def test_no_rhcos(self):
        builds = {"ose-installer-4.18.0-1.el9"}
        result = find_rhcos_nvrs(builds)
        self.assertEqual(result, [])


class TestNvrToBrewrootMetadataUrl(IsolatedAsyncioTestCase):
    def test_standard_nvr(self):
        url = nvr_to_brewroot_metadata_url("rhcos-x86_64-418.92.202407091253-1")
        self.assertIn("/packages/rhcos-x86_64/418.92.202407091253/1/metadata.json", url)

    def test_rhcos_421(self):
        url = nvr_to_brewroot_metadata_url("rhcos-x86_64-4.21.9.6.202608051529-0")
        self.assertIn("/packages/rhcos-x86_64/4.21.9.6.202608051529/0/metadata.json", url)


class TestGetKernelNvrsFromMetadata(IsolatedAsyncioTestCase):
    def test_finds_kernel(self):
        metadata = {
            "output": [
                {
                    "components": [
                        {"name": "kernel", "version": "5.14.0", "release": "570.132.1.el9_6"},
                        {"name": "bash", "version": "5.2.26", "release": "1.el9"},
                        {"name": "kernel-core", "version": "5.14.0", "release": "570.132.1.el9_6"},
                    ]
                }
            ]
        }
        result = get_kernel_nvrs_from_metadata(metadata, {"kernel", "kernel-rt"})
        self.assertEqual(result, ["kernel-5.14.0-570.132.1.el9_6"])

    def test_no_kernel(self):
        metadata = {"output": [{"components": [{"name": "bash", "version": "5.2.26", "release": "1.el9"}]}]}
        result = get_kernel_nvrs_from_metadata(metadata, {"kernel"})
        self.assertEqual(result, [])

    def test_empty_output(self):
        result = get_kernel_nvrs_from_metadata({"output": []}, {"kernel"})
        self.assertEqual(result, [])

    def test_null_components(self):
        metadata = {"output": [{"components": None}]}
        result = get_kernel_nvrs_from_metadata(metadata, {"kernel"})
        self.assertEqual(result, [])

    def test_dedup_across_outputs(self):
        metadata = {
            "output": [
                {"components": [{"name": "kernel", "version": "5.14.0", "release": "1.el9"}]},
                {"components": [{"name": "kernel", "version": "5.14.0", "release": "1.el9"}]},
            ]
        }
        result = get_kernel_nvrs_from_metadata(metadata, {"kernel"})
        self.assertEqual(len(result), 1)


class TestGetKernelRpmsFromRhcos(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_kernel_tag_cli.requests.get")
    def test_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "output": [
                {
                    "components": [
                        {"name": "kernel", "version": "5.14.0", "release": "1.el9"},
                    ]
                }
            ]
        }
        mock_get.return_value = mock_resp
        result = get_kernel_rpms_from_rhcos("rhcos-x86_64-418.92.1-1", {"kernel"})
        self.assertEqual(result, ["kernel-5.14.0-1.el9"])
        mock_resp.raise_for_status.assert_called_once()

    @patch("elliottlib.cli.verify_kernel_tag_cli.requests.get")
    def test_http_error(self, mock_get):
        from requests.exceptions import HTTPError

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = HTTPError("404 Not Found")
        mock_get.return_value = mock_resp
        with self.assertRaises(HTTPError):
            get_kernel_rpms_from_rhcos("rhcos-x86_64-fake-1", {"kernel"})


class TestCheckKernelTags(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_kernel_tag_cli.brew.get_builds_tags")
    def test_no_stop_ship(self, mock_get_tags):
        mock_get_tags.return_value = [
            [{"name": "rhaos-4.18-rhel-9-candidate"}, {"name": "early-kernel-candidate"}],
        ]
        result = check_kernel_tags(MagicMock(), ["kernel-5.14.0-1.el9"], "early-kernel-stop-ship")
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].has_stop_ship)

    @patch("elliottlib.cli.verify_kernel_tag_cli.brew.get_builds_tags")
    def test_has_stop_ship(self, mock_get_tags):
        mock_get_tags.return_value = [
            [{"name": "early-kernel-stop-ship"}, {"name": "early-kernel-candidate"}],
        ]
        result = check_kernel_tags(MagicMock(), ["kernel-5.14.0-1.el9"], "early-kernel-stop-ship")
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].has_stop_ship)

    @patch("elliottlib.cli.verify_kernel_tag_cli.brew.get_builds_tags")
    def test_empty_nvrs(self, mock_get_tags):
        result = check_kernel_tags(MagicMock(), [], "early-kernel-stop-ship")
        self.assertEqual(result, [])
        mock_get_tags.assert_not_called()


class TestCheckAdvisoryKernelTag(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_kernel_tag_cli.check_kernel_tags")
    @patch("elliottlib.cli.verify_kernel_tag_cli.get_kernel_rpms_from_rhcos")
    async def test_no_rhcos_builds(self, mock_get_rpms, mock_check_tags):
        api = AsyncMock()
        api.get_builds_flattened.return_value = {"ose-installer-4.18.0-1.el9"}
        result = await check_advisory_kernel_tag(api, 12345, "image", MagicMock(), {"kernel"}, "early-kernel-stop-ship")
        self.assertTrue(result.skipped)
        mock_get_rpms.assert_not_called()

    @patch("elliottlib.cli.verify_kernel_tag_cli.check_kernel_tags")
    @patch("elliottlib.cli.verify_kernel_tag_cli.get_kernel_rpms_from_rhcos")
    async def test_kernel_ok(self, mock_get_rpms, mock_check_tags):
        api = AsyncMock()
        api.get_builds_flattened.return_value = {"rhcos-x86_64-418.92.1-1"}
        mock_get_rpms.return_value = ["kernel-5.14.0-1.el9"]
        mock_check_tags.return_value = [KernelBuildInfo(nvr="kernel-5.14.0-1.el9")]

        result = await check_advisory_kernel_tag(api, 12345, "image", MagicMock(), {"kernel"}, "early-kernel-stop-ship")
        self.assertTrue(result.passed)
        self.assertFalse(result.skipped)

    @patch("elliottlib.cli.verify_kernel_tag_cli.check_kernel_tags")
    @patch("elliottlib.cli.verify_kernel_tag_cli.get_kernel_rpms_from_rhcos")
    async def test_kernel_stop_ship(self, mock_get_rpms, mock_check_tags):
        api = AsyncMock()
        api.get_builds_flattened.return_value = {"rhcos-x86_64-418.92.1-1"}
        mock_get_rpms.return_value = ["kernel-5.14.0-1.el9"]
        mock_check_tags.return_value = [KernelBuildInfo(nvr="kernel-5.14.0-1.el9", has_stop_ship=True)]

        result = await check_advisory_kernel_tag(api, 12345, "image", MagicMock(), {"kernel"}, "early-kernel-stop-ship")
        self.assertTrue(result.failed)

    @patch("elliottlib.cli.verify_kernel_tag_cli.check_kernel_tags")
    @patch("elliottlib.cli.verify_kernel_tag_cli.get_kernel_rpms_from_rhcos")
    async def test_no_kernel_rpms_found(self, mock_get_rpms, mock_check_tags):
        api = AsyncMock()
        api.get_builds_flattened.return_value = {"rhcos-x86_64-418.92.1-1"}
        mock_get_rpms.return_value = []
        mock_check_tags.return_value = []

        result = await check_advisory_kernel_tag(api, 12345, "image", MagicMock(), {"kernel"}, "early-kernel-stop-ship")
        self.assertTrue(result.failed)
        self.assertIn("no kernel RPMs found", result.error)

    @patch("elliottlib.cli.verify_kernel_tag_cli.check_kernel_tags")
    @patch("elliottlib.cli.verify_kernel_tag_cli.get_kernel_rpms_from_rhcos")
    async def test_api_error(self, mock_get_rpms, mock_check_tags):
        api = AsyncMock()
        api.get_builds_flattened.side_effect = Exception("connection failed")

        result = await check_advisory_kernel_tag(api, 12345, "image", MagicMock(), {"kernel"}, "early-kernel-stop-ship")
        self.assertEqual(result.error, "connection failed")
        self.assertTrue(result.failed)

    @patch("elliottlib.cli.verify_kernel_tag_cli.check_kernel_tags")
    @patch("elliottlib.cli.verify_kernel_tag_cli.get_kernel_rpms_from_rhcos")
    async def test_multiple_rhcos_dedup(self, mock_get_rpms, mock_check_tags):
        api = AsyncMock()
        api.get_builds_flattened.return_value = {"rhcos-x86_64-418.92.1-1", "rhcos-aarch64-418.92.1-1"}
        mock_get_rpms.side_effect = [
            ["kernel-5.14.0-1.el9"],
            ["kernel-5.14.0-1.el9"],
        ]
        mock_check_tags.return_value = [KernelBuildInfo(nvr="kernel-5.14.0-1.el9")]

        await check_advisory_kernel_tag(api, 12345, "image", MagicMock(), {"kernel"}, "early-kernel-stop-ship")
        mock_check_tags.assert_called_once()
        args = mock_check_tags.call_args[0]
        self.assertEqual(args[1], ["kernel-5.14.0-1.el9"])


class TestVerifyKernelTag(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_kernel_tag_cli.AsyncErrataAPI")
    @patch("elliottlib.cli.verify_kernel_tag_cli.check_advisory_kernel_tag")
    async def test_checks_all_advisories(self, mock_check, mock_api_cls):
        mock_api = AsyncMock()
        mock_api_cls.return_value.__aenter__ = AsyncMock(return_value=mock_api)
        mock_api_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_check.side_effect = [
            AdvisoryKernelResult(
                advisory_id=100,
                impetus="image",
                kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9")],
            ),
            AdvisoryKernelResult(advisory_id=200, impetus="rhcos", skipped=True),
        ]

        koji_api = MagicMock()
        result = await verify_kernel_tag({"image": 100, "rhcos": 200}, koji_api, {"kernel"}, "early-kernel-stop-ship")
        self.assertEqual(len(result.advisories), 2)
        self.assertTrue(result.passed)
        self.assertEqual(mock_check.call_count, 2)


class TestRenderResult(IsolatedAsyncioTestCase):
    def test_text_passed(self):
        result = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(
                    advisory_id=100,
                    impetus="image",
                    kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9")],
                )
            ],
            stop_ship_tag="early-kernel-stop-ship",
        )
        text = render_result(result, "text")
        self.assertIn("OK", text)
        self.assertIn("PASS", text)
        self.assertIn("kernel-5.14.0-1.el9", text)

    def test_text_failed(self):
        result = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(
                    advisory_id=100,
                    impetus="image",
                    kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9", has_stop_ship=True)],
                )
            ],
            stop_ship_tag="early-kernel-stop-ship",
        )
        text = render_result(result, "text")
        self.assertIn("STOP-SHIP", text)
        self.assertIn("FAIL", text)

    def test_text_skipped(self):
        result = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(advisory_id=100, impetus="rpm", skipped=True),
            ],
            stop_ship_tag="early-kernel-stop-ship",
        )
        text = render_result(result, "text")
        self.assertIn("SKIPPED", text)

    def test_text_error(self):
        result = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(advisory_id=100, impetus="image", error="connection failed"),
            ],
            stop_ship_tag="early-kernel-stop-ship",
        )
        text = render_result(result, "text")
        self.assertIn("ERROR", text)
        self.assertIn("connection failed", text)

    def test_json_output(self):
        result = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(
                    advisory_id=100,
                    impetus="image",
                    rhcos_builds=["rhcos-x86_64-418.92.1-1"],
                    kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9")],
                )
            ],
            stop_ship_tag="early-kernel-stop-ship",
        )
        output = render_result(result, "json")
        data = json.loads(output)
        self.assertTrue(data["passed"])
        self.assertFalse(data["failed"])
        self.assertEqual(data["stop_ship_tag"], "early-kernel-stop-ship")
        self.assertEqual(len(data["advisories"]), 1)
        self.assertEqual(data["advisories"][0]["advisory_id"], 100)
        self.assertEqual(len(data["advisories"][0]["kernel_builds"]), 1)

    def test_json_stop_ship(self):
        result = VerifyKernelTagResult(
            advisories=[
                AdvisoryKernelResult(
                    advisory_id=100,
                    impetus="image",
                    kernel_builds=[KernelBuildInfo(nvr="kernel-5.14.0-1.el9", has_stop_ship=True)],
                )
            ],
            stop_ship_tag="early-kernel-stop-ship",
        )
        output = render_result(result, "json")
        data = json.loads(output)
        self.assertFalse(data["passed"])
        self.assertTrue(data["failed"])
        self.assertTrue(data["advisories"][0]["kernel_builds"][0]["has_stop_ship"])
