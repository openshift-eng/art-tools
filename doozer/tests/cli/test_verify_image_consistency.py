import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from doozerlib.cli.verify_image_consistency import (
    ImageCheckResult,
    ImageIdentifiers,
    VerifyImageConsistencyResult,
    _is_skipped_image,
    fetch_image_identifiers,
    fetch_payload_images,
    identifiers_match,
    render_result,
    verify_image_consistency,
)


class TestIsSkippedImage(IsolatedAsyncioTestCase):
    def test_machine_os_content(self):
        self.assertTrue(_is_skipped_image("machine-os-content"))

    def test_rhel_coreos(self):
        self.assertTrue(_is_skipped_image("rhel-coreos"))

    def test_rhel_coreos_9(self):
        self.assertTrue(_is_skipped_image("rhel-coreos-9"))

    def test_rhel_coreos_extensions(self):
        self.assertTrue(_is_skipped_image("rhel-coreos-extensions"))

    def test_rhel_coreos_9_extensions(self):
        self.assertTrue(_is_skipped_image("rhel-coreos-9-extensions"))

    def test_regular_image(self):
        self.assertFalse(_is_skipped_image("ose-cli"))

    def test_partial_match_not_skipped(self):
        self.assertFalse(_is_skipped_image("rhel-coreos-extra"))


class TestIdentifiersMatch(IsolatedAsyncioTestCase):
    def test_match_by_list_digest(self):
        a = ImageIdentifiers(pullspec="a", list_digest="sha256:abc")
        b = ImageIdentifiers(pullspec="b", list_digest="sha256:abc")
        self.assertTrue(identifiers_match(a, b))

    def test_match_by_digest(self):
        a = ImageIdentifiers(pullspec="a", digest="sha256:def")
        b = ImageIdentifiers(pullspec="b", digest="sha256:def")
        self.assertTrue(identifiers_match(a, b))

    def test_match_by_vcs_ref(self):
        a = ImageIdentifiers(pullspec="a", vcs_ref="abc123")
        b = ImageIdentifiers(pullspec="b", vcs_ref="abc123")
        self.assertTrue(identifiers_match(a, b))

    def test_no_match(self):
        a = ImageIdentifiers(pullspec="a", digest="sha256:aaa", vcs_ref="111")
        b = ImageIdentifiers(pullspec="b", digest="sha256:bbb", vcs_ref="222")
        self.assertFalse(identifiers_match(a, b))

    def test_empty_fields_no_match(self):
        a = ImageIdentifiers(pullspec="a")
        b = ImageIdentifiers(pullspec="b")
        self.assertFalse(identifiers_match(a, b))

    def test_list_digest_takes_priority(self):
        a = ImageIdentifiers(pullspec="a", list_digest="sha256:same", digest="sha256:diff_a")
        b = ImageIdentifiers(pullspec="b", list_digest="sha256:same", digest="sha256:diff_b")
        self.assertTrue(identifiers_match(a, b))


class TestFetchPayloadImages(IsolatedAsyncioTestCase):
    @patch("doozerlib.cli.verify_image_consistency.exectools.cmd_gather_async")
    async def test_returns_images_and_version(self, mock_cmd):
        payload_data = {
            "metadata": {"version": "4.20.1"},
            "references": {
                "spec": {
                    "tags": [
                        {"name": "ose-cli", "from": {"name": "quay.io/ocp/cli@sha256:abc"}},
                        {"name": "machine-os-content", "from": {"name": "quay.io/ocp/rhcos@sha256:def"}},
                    ]
                }
            },
        }
        mock_cmd.return_value = (0, json.dumps(payload_data), "")

        images, version = await fetch_payload_images("quay.io/ocp:4.20.1")
        self.assertEqual(version, "4.20.1")
        self.assertEqual(len(images), 2)
        self.assertEqual(images[0], ("ose-cli", "quay.io/ocp/cli@sha256:abc"))

    @patch("doozerlib.cli.verify_image_consistency.exectools.cmd_gather_async")
    async def test_raises_on_failure(self, mock_cmd):
        mock_cmd.return_value = (1, "", "error occurred")
        with self.assertRaises(RuntimeError):
            await fetch_payload_images("quay.io/ocp:4.20.1")


class TestFetchImageIdentifiers(IsolatedAsyncioTestCase):
    @patch("doozerlib.cli.verify_image_consistency.oc_image_info__cached_async")
    async def test_parses_metadata(self, mock_oc):
        oc_output = {
            "digest": "sha256:img_digest",
            "listDigest": "sha256:list_digest",
            "config": {
                "config": {
                    "Labels": {
                        "vcs-ref": "abc123",
                        "name": "ose-cli",
                    }
                }
            },
        }
        mock_oc.return_value = json.dumps(oc_output)

        result = await fetch_image_identifiers("quay.io/ocp/cli@sha256:abc")
        self.assertEqual(result.digest, "sha256:img_digest")
        self.assertEqual(result.list_digest, "sha256:list_digest")
        self.assertEqual(result.vcs_ref, "abc123")
        self.assertEqual(result.name, "ose-cli")

    @patch("doozerlib.cli.verify_image_consistency.oc_image_info__cached_async")
    async def test_returns_empty_on_error(self, mock_oc):
        mock_oc.side_effect = Exception("oc failed")

        result = await fetch_image_identifiers("quay.io/ocp/cli@sha256:abc")
        self.assertEqual(result.digest, "")
        self.assertEqual(result.list_digest, "")
        self.assertEqual(result.vcs_ref, "")


class TestVerifyImageConsistency(IsolatedAsyncioTestCase):
    @patch("doozerlib.cli.verify_image_consistency.check_catalog")
    @patch("doozerlib.cli.verify_image_consistency.fetch_image_identifiers")
    @patch("doozerlib.cli.verify_image_consistency.fetch_shipment_components")
    @patch("doozerlib.cli.verify_image_consistency.fetch_payload_images")
    async def test_all_images_match(self, mock_payload, mock_shipment, mock_identifiers, mock_catalog):
        mock_payload.return_value = (
            [
                ("ose-cli", "quay.io/ocp/cli@sha256:aaa"),
                ("machine-os-content", "quay.io/ocp/rhcos@sha256:bbb"),
            ],
            "4.20.1",
        )
        mock_shipment.return_value = (
            [("cli", "registry.redhat.io/ocp/cli@sha256:ccc")],
            "4.20.1",
        )

        async def mock_id(pullspec):
            return ImageIdentifiers(pullspec=pullspec, digest="sha256:shared_digest")

        mock_identifiers.side_effect = mock_id
        mock_catalog.return_value = False

        result = await verify_image_consistency("quay.io/ocp:4.20.1", "https://gitlab.example.com/mr/1")

        self.assertTrue(result.passed)
        self.assertEqual(len(result.skipped_images), 1)
        self.assertIn("machine-os-content", result.skipped_images)
        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].found_in, "shipment")

    @patch("doozerlib.cli.verify_image_consistency.check_catalog")
    @patch("doozerlib.cli.verify_image_consistency.fetch_image_identifiers")
    @patch("doozerlib.cli.verify_image_consistency.fetch_shipment_components")
    @patch("doozerlib.cli.verify_image_consistency.fetch_payload_images")
    async def test_image_not_found(self, mock_payload, mock_shipment, mock_identifiers, mock_catalog):
        mock_payload.return_value = (
            [("ose-cli", "quay.io/ocp/cli@sha256:aaa")],
            "4.20.1",
        )
        mock_shipment.return_value = (
            [("other", "registry.redhat.io/ocp/other@sha256:bbb")],
            "4.20.1",
        )

        async def mock_id(pullspec):
            if "cli" in pullspec:
                return ImageIdentifiers(pullspec=pullspec, digest="sha256:cli_digest")
            return ImageIdentifiers(pullspec=pullspec, digest="sha256:other_digest")

        mock_identifiers.side_effect = mock_id
        mock_catalog.return_value = False

        result = await verify_image_consistency("quay.io/ocp:4.20.1", "https://gitlab.example.com/mr/1")

        self.assertFalse(result.passed)
        self.assertEqual(len(result.failed_images), 1)
        self.assertEqual(result.failed_images[0].name, "ose-cli")

    @patch("doozerlib.cli.verify_image_consistency.check_catalog")
    @patch("doozerlib.cli.verify_image_consistency.fetch_image_identifiers")
    @patch("doozerlib.cli.verify_image_consistency.fetch_shipment_components")
    @patch("doozerlib.cli.verify_image_consistency.fetch_payload_images")
    async def test_catalog_fallback(self, mock_payload, mock_shipment, mock_identifiers, mock_catalog):
        mock_payload.return_value = (
            [("ose-cli", "quay.io/ocp/cli@sha256:aaa")],
            "4.20.1",
        )
        mock_shipment.return_value = ([], "4.20.1")

        async def mock_id(pullspec):
            return ImageIdentifiers(pullspec=pullspec, digest="sha256:cli_digest")

        mock_identifiers.side_effect = mock_id
        mock_catalog.return_value = True

        result = await verify_image_consistency("quay.io/ocp:4.20.1", "https://gitlab.example.com/mr/1")

        self.assertTrue(result.passed)
        self.assertEqual(result.results[0].found_in, "catalog")


class TestRenderResult(IsolatedAsyncioTestCase):
    def _make_result(self, passed=True):
        results = [ImageCheckResult(name="ose-cli", pullspec="quay.io/ocp/cli@sha256:abc", found_in="shipment")]
        if not passed:
            results.append(ImageCheckResult(name="ose-api", pullspec="quay.io/ocp/api@sha256:def"))
        return VerifyImageConsistencyResult(
            payload_url="quay.io/ocp:4.20.1",
            shipment_mr_url="https://gitlab.example.com/mr/1",
            payload_version="4.20.1",
            shipment_version="4.20.1",
            payload_image_count=3,
            shipment_component_count=2,
            skipped_images=["machine-os-content"],
            results=results,
        )

    def test_text_output_pass(self):
        result = self._make_result(passed=True)
        output = render_result(result, "text")
        self.assertIn("PASS", output)
        self.assertIn("1/1 passed", output)
        self.assertNotIn("NOT FOUND", output)

    def test_text_output_fail(self):
        result = self._make_result(passed=False)
        output = render_result(result, "text")
        self.assertIn("FAIL", output)
        self.assertIn("NOT FOUND", output)
        self.assertIn("ose-api", output)

    def test_json_output(self):
        result = self._make_result(passed=True)
        output = render_result(result, "json")
        data = json.loads(output)
        self.assertTrue(data["passed"])
        self.assertEqual(data["payload_version"], "4.20.1")
        self.assertEqual(len(data["results"]), 1)
        self.assertEqual(data["results"][0]["name"], "ose-cli")

    def test_json_output_fail(self):
        result = self._make_result(passed=False)
        output = render_result(result, "json")
        data = json.loads(output)
        self.assertFalse(data["passed"])
        self.assertEqual(len(data["failed_images"]), 1)
        self.assertEqual(data["failed_images"][0]["name"], "ose-api")
