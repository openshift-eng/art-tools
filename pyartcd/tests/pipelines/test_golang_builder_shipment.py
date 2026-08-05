import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock, patch

from doozerlib.backend.golang_builder_shipment import (
    GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP,
    GolangBuilderShipmentHandler,
    derive_golang_group,
)
from pyartcd.pipelines.golang_builder_shipment import resolve_konflux_image_nvrs, resolve_lifecycle_env


class TestResolveReleasePlan(unittest.TestCase):
    def test_prod_returns_prod_plan(self):
        plan = GolangBuilderShipmentHandler.resolve_release_plan("prod")
        self.assertEqual(plan, "ocp-art-golang-builder-prod-rhel9")

    def test_ec_returns_ec_plan(self):
        plan = GolangBuilderShipmentHandler.resolve_release_plan("ec")
        self.assertEqual(plan, "ocp-art-golang-builder-ec-rhel9")

    def test_unknown_env_raises(self):
        with self.assertRaises(ValueError):
            GolangBuilderShipmentHandler.resolve_release_plan("staging")

    def test_map_keys_are_complete(self):
        self.assertIn("prod", GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP)
        self.assertIn("ec", GOLANG_BUILDER_SHIPMENT_RELEASE_PLAN_MAP)


class _FakeResponse:
    def __init__(self, status, body=""):
        self.status = status
        self._body = body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _FakeSession:
    def __init__(self, response):
        self._response = response

    def get(self, *args, **kwargs):
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestResolveLifecycleEnv(IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_release_phase_returns_prod(self, mock_session_cls):
        resp = _FakeResponse(200, "software_lifecycle:\n  phase: release\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.18")
        self.assertEqual(result, "prod")

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_pre_release_phase_returns_ec(self, mock_session_cls):
        resp = _FakeResponse(200, "software_lifecycle:\n  phase: pre-release\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.23")
        self.assertEqual(result, "ec")

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_missing_lifecycle_defaults_to_prod(self, mock_session_cls):
        resp = _FakeResponse(200, "vars:\n  GOLANG_VERSION: '1.22'\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.16")
        self.assertEqual(result, "prod")

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_http_error_raises(self, mock_session_cls):
        resp = _FakeResponse(404)
        mock_session_cls.return_value = _FakeSession(resp)
        with self.assertRaises(RuntimeError):
            await resolve_lifecycle_env("99.99")

    @patch("pyartcd.pipelines.golang_builder_shipment.aiohttp.ClientSession")
    async def test_unknown_phase_defaults_to_prod(self, mock_session_cls):
        resp = _FakeResponse(200, "software_lifecycle:\n  phase: maintenance\n")
        mock_session_cls.return_value = _FakeSession(resp)
        result = await resolve_lifecycle_env("4.14")
        self.assertEqual(result, "prod")


class TestDeriveGolangGroup(unittest.TestCase):
    def test_from_rpm_nvr(self):
        self.assertEqual(derive_golang_group(["golang-1.25.9-1.el9"]), "rhel-9-golang-1.25")

    def test_from_rpm_nvr_el8(self):
        self.assertEqual(derive_golang_group(["golang-1.22.3-2.el8"]), "rhel-8-golang-1.22")

    def test_from_konflux_image_nvr(self):
        nvr = "openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9"
        self.assertEqual(derive_golang_group([nvr]), "rhel-9-golang-1.25")

    def test_unknown_nvr_raises(self):
        with self.assertRaises(ValueError):
            derive_golang_group(["not-a-golang-nvr-1.0-1.noarch"])


class TestResolveKonfluxImageNvrs(IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.golang_builder_shipment.KonfluxDb")
    async def test_resolves_rpm_nvr_to_image_nvr(self, mock_db_cls):
        record = Mock()
        record.nvr = "openshift-golang-builder-container-v1.25.9-202605121249.p2.gdf787b0.el9"

        async def _search(*args, **kwargs):
            yield record

        mock_db_cls.return_value.search_builds_by_fields = _search

        result = await resolve_konflux_image_nvrs(["golang-1.25.9-1.el9"])
        self.assertEqual(result, [record.nvr])

    @patch("pyartcd.pipelines.golang_builder_shipment.KonfluxDb")
    async def test_missing_build_raises(self, mock_db_cls):
        async def _search(*args, **kwargs):
            if False:
                yield None

        mock_db_cls.return_value.search_builds_by_fields = _search

        with self.assertRaises(RuntimeError):
            await resolve_konflux_image_nvrs(["golang-1.25.9-1.el9"])


if __name__ == "__main__":
    unittest.main()
