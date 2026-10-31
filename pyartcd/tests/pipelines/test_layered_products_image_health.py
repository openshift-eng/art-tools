import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib.cli.images_health import ConcernCode
from doozerlib.constants import ART_BUILD_HISTORY_URL
from pyartcd.fbc_health import FbcHealthReport
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
    def setUp(self):
        from pyartcd.fbc_health import FbcHealthReport

        collector_patch = patch(
            'pyartcd.pipelines.layered_products_image_health.collect_fbc_health',
            new_callable=AsyncMock,
            return_value=FbcHealthReport(),
        )
        self.collect_fbc_health = collector_patch.start()
        self.addCleanup(collector_patch.stop)

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
        self.assertIn(":white_check_mark: no tracked failures", details)
        self.assertNotIn("*Build Failures (", details)
        self.assertNotIn("Rebase Failures", details)

    def _incomplete_report(self, **overrides):
        values = dict(
            error='Image collection incomplete',
            build_failures={'build-image': {'failure_count': 2, 'jenkins_url': 'https://execution.invalid/raw'}},
            its_failures={'its-image': {'failure_count': 3}},
            release_failures={'release-image': {'failure_count': 4}},
            rebase_failures={'rebase-image': {'failure_count': 5}},
            fbc_health=FbcHealthReport(
                {
                    'fbc': dict(
                        variant='oadp',
                        assembly='stream',
                        target='4.18',
                        kind='operator',
                        entity='fbc-operator',
                        version='1',
                        failure_count=6,
                        jenkins_url='https://execution.invalid/fbc',
                    )
                }
            ),
        )
        values.update(overrides)
        return self._make_report('oadp-1.5', 'oadp', BuildVariant.OADP, **values)

    def _assert_collected_failures(self, summary, details):
        for category in (
            '1 Redis-tracked build failure(s) (unvalidated)',
            '1 ITS failure(s)',
            '1 release failure(s)',
            '1 rebase failure(s)',
            '1 tracked FBC failure(s)',
        ):
            self.assertIn(category, summary)
        for name, count in (
            ('build-image', 2),
            ('its-image', 3),
            ('release-image', 4),
            ('rebase-image', 5),
            ('fbc-operator', 6),
        ):
            self.assertIn(f'`{name}`', details)
            self.assertRegex(details, rf'`{name}`[^\n]*Failed {count} time')
        for message in (summary, details):
            self.assertIn('incomplete', message)
            self.assertIn('unvalidated', message)
            self.assertNotIn('Healthy', message)
            self.assertNotIn('execution.invalid', message)

    def test_incomplete_report_retains_all_collected_failure_categories(self):
        pipeline = self._make_pipeline()
        report = self._incomplete_report()
        self._assert_collected_failures(
            pipeline._build_variant_message('oadp', [report]), pipeline._build_group_message(report)
        )

    async def test_incomplete_empty_and_fbc_only_reports_never_claim_health(self):
        pipeline = self._make_pipeline()
        for fbc_health in (FbcHealthReport(), FbcHealthReport(error=True), self._incomplete_report().fbc_health):
            with self.subTest(fbc_health=fbc_health):
                report = self._incomplete_report(
                    build_failures={}, its_failures={}, release_failures={}, rebase_failures={}, fbc_health=fbc_health
                )
                messages = [
                    pipeline._build_variant_message('oadp', [report]),
                    pipeline._build_group_message(report),
                    pipeline._build_group_details(report),
                ]
                for message in messages:
                    self.assertIn('incomplete', message)
                    self.assertNotIn('Healthy', message)
                    self.assertNotIn('no tracked failures', message.lower())
                if fbc_health.summary():
                    self.assertIn(fbc_health.summary(), messages[0])
                    self.assertIn(fbc_health.details(), messages[1])
                    self.assertIn(fbc_health.details(), messages[2])
                pipeline.slack_client.say = AsyncMock(return_value={'ts': 'summary'})
                await pipeline._notify_slack([report])
                self.assertEqual(pipeline.slack_client.say.await_count, 2)
                self.assertIn('incomplete', pipeline.slack_client.say.await_args.args[0])

    def test_unvalidated_build_fallback_deduplicates_concerns_and_preserves_complete_semantics(self):
        pipeline = self._make_pipeline()
        report = self._incomplete_report(
            build_concerns=[
                {'image_name': 'known-failed', 'code': ConcernCode.LATEST_ATTEMPT_FAILED.value},
                {'image_name': 'known-succeeded', 'code': ConcernCode.LATEST_BUILD_SUCCEEDED.value},
            ],
            build_failures={
                'known-failed': {'failure_count': 7},
                'known-succeeded': {'failure_count': 8},
                'build-image': {'failure_count': 2},
            },
        )
        summary = pipeline._build_variant_message('oadp', [report])
        details = pipeline._build_group_message(report)
        self.assertIn('1 build failure(s)', summary)
        self.assertIn('1 Redis-tracked build failure(s) (unvalidated)', summary)
        self.assertEqual(details.count('`known-failed`'), 1)
        self.assertEqual(details.count('`build-image`'), 1)
        self.assertNotIn('known-succeeded', details)
        report.error = None
        self.assertNotIn('unvalidated', pipeline._build_variant_message('oadp', [report]))
        self.assertNotIn('`build-image`', pipeline._build_group_message(report))
        report.error = 'Image collection incomplete'
        pipeline.image_list = ['known-failed']
        self.assertNotIn('unvalidated', pipeline._build_variant_message('oadp', [report]))
        self.assertNotIn('`build-image`', pipeline._build_group_message(report))

    async def test_run_publishes_retained_counters_before_raising_real_query_failure(self):
        pipeline = self._make_pipeline()
        pipeline.groups = ['oadp-1.5']
        retained = self._incomplete_report()
        counters = {
            'build-failure': retained.build_failures,
            'ec-failure': retained.its_failures,
            'release-failure': retained.release_failures,
        }
        published = []

        async def publish(message, **kwargs):
            published.append(message)
            return {'ts': 'summary'}

        pipeline.slack_client.say = AsyncMock(side_effect=publish)
        pipeline._get_build_concerns = AsyncMock(side_effect=RuntimeError('BigQuery unavailable'))
        with (
            patch(
                'pyartcd.pipelines.layered_products_image_health.collect_fbc_health',
                new=AsyncMock(return_value=retained.fbc_health),
            ),
            patch(
                'pyartcd.pipelines.layered_products_image_health.util.load_group_config',
                new=AsyncMock(return_value={'product': 'oadp'}),
            ),
            patch(
                'pyartcd.pipelines.layered_products_image_health.util.get_counter_failures',
                new=AsyncMock(side_effect=lambda kind, **kwargs: counters[kind]),
            ),
            patch(
                'pyartcd.pipelines.layered_products_image_health.util.get_rebase_failures',
                new=AsyncMock(return_value=retained.rebase_failures),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, 'Layered product health collection failed for: oadp-1.5'):
                try:
                    await pipeline.run()
                except RuntimeError:
                    self.assertEqual(len(published), 2)
                    raise
        self._assert_collected_failures(*published)
        self.assertIn('`oadp`', published[0])
        self.assertEqual(pipeline.slack_client.say.await_args_list[1].kwargs['thread_ts'], 'summary')
        pipeline._get_build_concerns.assert_awaited_once_with(
            'oadp-1.5', BuildVariant.OADP, {'build-image', 'its-image', 'release-image'}
        )

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
