import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib.constants import ART_BUILD_HISTORY_URL
from pyartcd.pipelines.layered_products_image_health import (
    LayeredProductHealthReport,
    LayeredProductsImageHealthPipeline,
    _parse_groups,
)


class TestParseGroups(unittest.TestCase):
    def test_parse_groups_trims_and_deduplicates(self):
        self.assertEqual(
            _parse_groups("oadp-1.5, logging-6.7,oadp-1.5"),
            ["oadp-1.5", "logging-6.7"],
        )

    def test_parse_groups_rejects_empty_input(self):
        with self.assertRaises(ValueError):
            _parse_groups(" ,  ")


class TestCliRegistration(unittest.TestCase):
    def test_layered_products_image_health_command_is_registered(self):
        from pyartcd.__main__ import cli

        with patch("pyartcd.cli.Runtime.from_config_file"):
            result = CliRunner().invoke(cli, ["layered-products-image-health", "--help"])
        command = cli.commands["layered-products-image-health"]
        options = {parameter.name: parameter for parameter in command.params}

        self.assertEqual(result.exit_code, 0)
        self.assertIn("layered-products-image-health", result.output)
        self.assertIn("--groups", result.output)
        self.assertIn("--assembly", result.output)
        self.assertEqual(
            set(options),
            {"groups", "assembly", "data_path", "data_gitref", "image_list"},
        )
        self.assertTrue(options["groups"].required)


class TestCollectGroup(unittest.IsolatedAsyncioTestCase):
    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.working_dir = Path("/tmp/layered-products-image-health-test")
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        return LayeredProductsImageHealthPipeline(
            runtime=runtime,
            groups="oadp-1.5",
            data_path="https://github.com/openshift-eng/ocp-build-data",
            data_gitref="",
            assembly="stream",
            image_list="",
        )

    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.get_rebase_failures",
        new_callable=AsyncMock,
        return_value={},
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.get_counter_failures",
        new_callable=AsyncMock,
        return_value={},
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.load_group_config",
        new_callable=AsyncMock,
        return_value={"product": "oadp"},
    )
    async def test_collect_group_resolves_product_variant_and_filters_counters(
        self,
        mock_load_group_config,
        mock_get_counter_failures,
        mock_get_rebase_failures,
    ):
        pipeline = self._make_pipeline()
        pipeline._get_build_concerns = AsyncMock(return_value=[])

        report = await pipeline._collect_group("oadp-1.5")

        self.assertEqual(report.product, "oadp")
        self.assertIs(report.variant, BuildVariant.OADP)
        mock_load_group_config.assert_awaited_once()
        self.assertEqual(mock_get_counter_failures.await_count, 3)
        for call in mock_get_counter_failures.await_args_list:
            self.assertEqual(call.kwargs["group"], "oadp-1.5")
            self.assertEqual(call.kwargs["build_variant"], "oadp")
        mock_get_rebase_failures.assert_awaited_once_with(
            group="oadp-1.5",
            branches=["rebase-failure"],
            build_systems=["konflux"],
            build_variant="oadp",
            logger=pipeline.runtime.logger,
        )

    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.load_group_config",
        new_callable=AsyncMock,
        return_value={},
    )
    async def test_collect_group_requires_product(self, _mock_load_group_config):
        pipeline = self._make_pipeline()

        with self.assertRaisesRegex(ValueError, "No product found"):
            await pipeline._collect_group("oadp-1.5")

    @patch(
        "pyartcd.pipelines.layered_products_image_health.exectools",
        create=True,
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.get_group_images",
        new_callable=AsyncMock,
        return_value=["oadp-operator"],
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.get_rebase_failures",
        new_callable=AsyncMock,
        return_value={},
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.get_counter_failures",
        new_callable=AsyncMock,
        return_value={"oadp-operator": {"failure_count": 2}},
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.load_group_config",
        new_callable=AsyncMock,
        return_value={"product": "oadp"},
    )
    async def test_collect_group_scopes_doozer_to_redis_images(
        self,
        _mock_load_group_config,
        _mock_get_counter_failures,
        _mock_get_rebase_failures,
        mock_get_group_images,
        mock_exectools,
    ):
        mock_cmd_gather_async = AsyncMock(
            return_value=(
                0,
                json.dumps([{"image_name": "oadp-operator", "code": "LATEST_ATTEMPT_FAILED"}]),
                "",
            )
        )
        mock_exectools.cmd_gather_async = mock_cmd_gather_async
        pipeline = self._make_pipeline()

        report = await pipeline._collect_group("oadp-1.5")

        mock_get_group_images.assert_awaited_once_with(
            group="oadp-1.5",
            assembly="stream",
            build_system="konflux",
            working_dir=pipeline._doozer_working / "oadp-1.5",
            doozer_data_path=pipeline.data_path,
            doozer_data_gitref="",
            variant="oadp",
        )
        command = mock_cmd_gather_async.await_args.args[0]
        self.assertIn("--group=oadp-1.5", command)
        self.assertIn("--assembly=stream", command)
        self.assertIn("--build-system=konflux", command)
        self.assertIn("--variant=oadp", command)
        self.assertIn("--images=oadp-operator", command)
        self.assertEqual(command[-1], "images:health")
        self.assertEqual(report.build_concerns[0]["image_name"], "oadp-operator")

    @patch(
        "pyartcd.pipelines.layered_products_image_health.exectools",
        create=True,
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.get_rebase_failures",
        new_callable=AsyncMock,
        return_value={},
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.get_counter_failures",
        new_callable=AsyncMock,
        return_value={},
    )
    @patch(
        "pyartcd.pipelines.layered_products_image_health.util.load_group_config",
        new_callable=AsyncMock,
        return_value={"product": "oadp"},
    )
    async def test_collect_group_skips_doozer_when_no_build_failures(
        self,
        _mock_load_group_config,
        _mock_get_counter_failures,
        _mock_get_rebase_failures,
        mock_exectools,
    ):
        mock_exectools.cmd_gather_async = AsyncMock()
        pipeline = self._make_pipeline()

        report = await pipeline._collect_group("oadp-1.5")

        mock_exectools.cmd_gather_async.assert_not_awaited()
        self.assertEqual(report.build_concerns, [])


class TestLayeredProductHealthReport(unittest.IsolatedAsyncioTestCase):
    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.working_dir = Path("/tmp/layered-products-image-health-test")
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        return LayeredProductsImageHealthPipeline(
            runtime=runtime,
            groups="oadp-1.5,logging-6.7",
            data_path="https://github.com/openshift-eng/ocp-build-data",
            data_gitref="",
            assembly="stream",
            image_list="",
        )

    @staticmethod
    def _make_report(group, product, variant, **overrides):
        values = {
            "group": group,
            "product": product,
            "variant": variant,
            "build_concerns": [],
            "build_failures": {},
            "its_failures": {},
            "release_failures": {},
            "rebase_failures": {},
        }
        values.update(overrides)
        return LayeredProductHealthReport(**values)

    def test_build_report_separates_groups_and_failure_categories(self):
        pipeline = self._make_pipeline()
        reports = [
            self._make_report("oadp-1.5", "oadp", BuildVariant.OADP),
            self._make_report(
                "logging-6.7",
                "openshift-logging",
                BuildVariant.LOGGING,
                build_concerns=[
                    {
                        "image_name": "logging-collector",
                        "code": "LATEST_ATTEMPT_FAILED",
                        "latest_success_idx": 2,
                        "latest_failed_nvr": "logging-collector-1",
                        "latest_failed_build_record_id": "record-1",
                        "latest_failed_build_time": "2026-09-22T09:00:00+00:00",
                    }
                ],
                build_failures={"logging-collector": {"failure_count": 2}},
                its_failures={"logging-operator": {"failure_count": 3}},
                release_failures={"logging-rhel9": {"failure_count": 1}},
                rebase_failures={"logging-kibana": {"failure_count": 4}},
            ),
        ]

        summary = pipeline._build_variant_message("openshift-logging", [reports[1]])
        details = pipeline._build_group_details(reports[1])

        self.assertIn("logging-6.7", summary)
        self.assertNotIn("(openshift-logging)", summary)
        self.assertNotIn("*Build Failures (", summary)
        self.assertIn("openshift-logging", summary)
        self.assertIn("Build Failures", details)
        self.assertIn("ITS Verification Failures", details)
        self.assertIn("Release to Authz Failures", details)
        self.assertIn("Rebase Failures", details)
        self.assertIn(ART_BUILD_HISTORY_URL, details)

    def test_all_healthy_report_has_no_empty_failure_sections(self):
        pipeline = self._make_pipeline()
        report = self._make_report("oadp-1.5", "oadp", BuildVariant.OADP)

        details = pipeline._build_variant_message("oadp", [report])

        self.assertIn("oadp-1.5", details)
        self.assertIn(":white_check_mark: healthy", details)
        self.assertNotIn("*Build Failures (", details)
        self.assertNotIn("Rebase Failures", details)

    async def test_notify_slack_posts_variant_summaries_and_threads_failures(self):
        pipeline = self._make_pipeline()
        reports = [
            self._make_report("oadp-1.5", "oadp", BuildVariant.OADP),
            self._make_report(
                "logging-6.7",
                "openshift-logging",
                BuildVariant.LOGGING,
                build_concerns=[
                    {
                        "image_name": "logging-collector",
                        "code": "LATEST_ATTEMPT_FAILED",
                        "latest_success_idx": 2,
                        "latest_failed_nvr": "logging-collector-1",
                        "latest_failed_build_record_id": "record-1",
                        "latest_failed_build_time": "2026-09-22T09:00:00+00:00",
                    }
                ],
            ),
            self._make_report("logging-6.0", "openshift-logging", BuildVariant.LOGGING),
        ]
        pipeline.slack_client.say = AsyncMock(side_effect=[{"ts": "oadp"}, {"ts": "logging"}, {}])

        await pipeline._notify_slack(reports)

        pipeline.slack_client.bind_channel.assert_called_once_with("#art-release-layered-operators")
        self.assertEqual(pipeline.slack_client.say.await_count, 3)
        self.assertIn("oadp-1.5", pipeline.slack_client.say.await_args_list[0].args[0])
        logging_summary = pipeline.slack_client.say.await_args_list[1].args[0]
        self.assertIn("logging-6.7", logging_summary)
        self.assertIn("logging-6.0", logging_summary)
        self.assertNotIn("*Build Failures (", logging_summary)
        logging_details = pipeline.slack_client.say.await_args_list[2]
        self.assertIn("Build Failures", logging_details.args[0])
        self.assertEqual(logging_details.kwargs["thread_ts"], "logging")
        self.assertNotIn("thread_ts", pipeline.slack_client.say.await_args_list[0].kwargs)
        self.assertNotIn("thread_ts", pipeline.slack_client.say.await_args_list[1].kwargs)

    async def test_run_reports_partial_group_failure_then_raises(self):
        pipeline = self._make_pipeline()
        successful_report = self._make_report("oadp-1.5", "oadp", BuildVariant.OADP)
        failed_report = self._make_report(
            "logging-6.7",
            "unknown",
            None,
            error="Unable to read group configuration",
        )
        pipeline._collect_group_safely = AsyncMock(side_effect=[successful_report, failed_report])
        pipeline._notify_slack = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "logging-6.7"):
            await pipeline.run()

        pipeline._notify_slack.assert_awaited_once_with([failed_report, successful_report])
