import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.constants import (
    KONFLUX_RELEASE_EC_POLICY_CONFIGURATION,
    KONFLUX_RELEASE_FBC_EC_POLICY_CONFIGURATION,
    KONFLUX_RELEASE_PREGA_EC_POLICY_CONFIGURATION,
)
from pyartcd.pipelines.scheduled import schedule_build_conforma_verify as scheduler


def _runtime():
    runtime = MagicMock()
    runtime.working_dir = MagicMock()
    runtime.logger = MagicMock()
    return runtime


class TestRunFor(unittest.IsolatedAsyncioTestCase):
    async def _run(self, group, group_config, serial=False):
        runtime = _runtime()
        with (
            patch.object(scheduler.util, "is_build_permitted", new=AsyncMock(return_value=True)) as permitted,
            patch.object(scheduler.util, "load_group_config", new=AsyncMock(return_value=group_config)) as load_config,
            patch.object(scheduler.jenkins, "start_build_conforma_verify") as start_build,
        ):
            await scheduler.run_for(group, runtime, serial=serial)
        return permitted, load_config, start_build

    async def test_ocp_pre_release_uses_prega_policies(self):
        _, load_config, start_build = await self._run(
            "openshift-4.23",
            {"product": "openshift-logging", "software_lifecycle": {"phase": "pre-release"}},
        )

        load_config.assert_awaited_once_with(group="openshift-4.23", assembly="stream")
        self.assertEqual(start_build.call_args.kwargs["group"], "openshift-4.23")
        self.assertEqual(start_build.call_args.kwargs["ec_policy"], KONFLUX_RELEASE_PREGA_EC_POLICY_CONFIGURATION)
        self.assertEqual(start_build.call_args.kwargs["fbc_ec_policy"], KONFLUX_RELEASE_FBC_EC_POLICY_CONFIGURATION)

    async def test_ocp_non_pre_release_uses_ga_policies(self):
        _, _, start_build = await self._run(
            "openshift-4.22",
            {"product": "ocp", "software_lifecycle": {"phase": "release"}},
        )

        self.assertEqual(start_build.call_args.kwargs["ec_policy"], KONFLUX_RELEASE_EC_POLICY_CONFIGURATION)
        self.assertEqual(start_build.call_args.kwargs["fbc_ec_policy"], KONFLUX_RELEASE_FBC_EC_POLICY_CONFIGURATION)

    async def test_oadp_uses_product_policies_and_full_group(self):
        with patch.object(scheduler, "datetime") as datetime_mock:
            datetime_mock.now.return_value = datetime(2026, 8, 31, tzinfo=timezone.utc)
            _, _, start_build = await self._run("oadp-1.5", {"product": "oadp"}, serial=True)

        self.assertEqual(
            start_build.call_args.kwargs,
            {
                "group": "oadp-1.5",
                "assembly": "stream",
                "ec_policy": "rhtap-releng-tenant/registry-art-oadp-stage",
                "fbc_ec_policy": "rhtap-releng-tenant/fbc-art-oadp-stage",
                "effective_time": "2026-09-21T00:00:00Z",
                "include_corresponding_bundles": True,
                "include_corresponding_fbcs": True,
                "report_to_slack": True,
                "block_until_building": True,
                "block_until_complete": True,
            },
        )

    async def test_mta_uses_generic_fbc_stage_policy(self):
        _, _, start_build = await self._run("mta-8.2", {"product": "mta"})

        self.assertEqual(start_build.call_args.kwargs["ec_policy"], "rhtap-releng-tenant/registry-art-mta-stage")
        self.assertEqual(start_build.call_args.kwargs["fbc_ec_policy"], "rhtap-releng-tenant/fbc-stage")

    async def test_logging_uses_stage_fbc_policy(self):
        _, _, start_build = await self._run("logging-6.2", {"product": "openshift-logging"})

        self.assertEqual(start_build.call_args.kwargs["ec_policy"], "rhtap-releng-tenant/registry-art-logging-stage")
        self.assertEqual(start_build.call_args.kwargs["fbc_ec_policy"], "rhtap-releng-tenant/fbc-stage")

    async def test_oc_mirror_uses_registry_standard_and_no_fbc_policy(self):
        _, _, start_build = await self._run("oc-mirror-2.0", {"product": "oc-mirror"})

        self.assertEqual(start_build.call_args.kwargs["ec_policy"], "rhtap-releng-tenant/registry-standard")
        self.assertIsNone(start_build.call_args.kwargs["fbc_ec_policy"])
        self.assertFalse(start_build.call_args.kwargs["include_corresponding_fbcs"])

    async def test_unknown_lp_product_raises(self):
        runtime = _runtime()
        with (
            patch.object(scheduler.util, "is_build_permitted", new=AsyncMock(return_value=True)),
            patch.object(scheduler.util, "load_group_config", new=AsyncMock(return_value={"product": "unknown"})),
            patch.object(scheduler.jenkins, "start_build_conforma_verify") as start_build,
        ):
            with self.assertRaisesRegex(ValueError, "unknown-1.0.*unknown"):
                await scheduler.run_for("unknown-1.0", runtime)
        start_build.assert_not_called()

    async def test_freeze_skips_without_loading_config_or_starting_jenkins(self):
        runtime = _runtime()
        with (
            patch.object(scheduler.util, "is_build_permitted", new=AsyncMock(return_value=False)) as permitted,
            patch.object(scheduler.util, "load_group_config", new=AsyncMock()) as load_config,
            patch.object(scheduler.jenkins, "start_build_conforma_verify") as start_build,
        ):
            await scheduler.run_for("oadp-1.5", runtime)

        permitted.assert_awaited_once()
        load_config.assert_not_awaited()
        start_build.assert_not_called()
