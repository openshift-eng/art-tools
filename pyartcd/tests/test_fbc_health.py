import fnmatch
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.variants import BuildVariant
from pyartcd.fbc_health import (
    FbcHealthReport,
    collect_fbc_health,
    counter_prefix,
    invocation_scope,
    update_counters,
)
from pyartcd.pipelines.build_fbc import BuildFbcPipeline
from pyartcd.pipelines.images_health import ImagesHealthPipeline
from pyartcd.pipelines.layered_products_image_health import (
    LayeredProductHealthReport,
    LayeredProductsImageHealthPipeline,
)
from pyartcd.slack import SlackClient


class TestFbcCounters(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.values = {}
        self.logger = MagicMock()
        self.prefix = counter_prefix('openshift-4.18', 'stream', '4.18', '4.18')
        self.scope = invocation_scope(only='operator')

        async def redis_call(operation, key):
            self.assertEqual(operation, 'incr')
            self.values[key] = str(int(self.values.get(key, 0)) + 1)

        async def redis_set(key, value):
            self.values[key] = value

        async def redis_delete(pattern):
            for key in list(self.values):
                if fnmatch.fnmatchcase(key, pattern):
                    del self.values[key]

        patches = {
            'call': AsyncMock(side_effect=redis_call),
            'set_value': AsyncMock(side_effect=redis_set),
            'delete_keys_by_pattern': AsyncMock(side_effect=redis_delete),
            'get_keys': AsyncMock(side_effect=lambda pattern: sorted(fnmatch.filter(self.values, pattern))),
            'get_value': AsyncMock(side_effect=lambda key: self.values.get(key)),
        }
        self.redis = patches
        for name, mock in patches.items():
            redis_patch = patch(f'pyartcd.fbc_health.redis.{name}', mock)
            redis_patch.start()
            self.addCleanup(redis_patch.stop)

    async def update(self, records=None, failed=False, prefix=None, scope=None, dry_run=False):
        await update_counters(
            prefix or self.prefix,
            scope or self.scope,
            records,
            failed,
            dry_run,
            self.logger,
            url='https://example.com/build/1',
        )

    async def test_repeated_failures_count_once_and_normalize_rebase_names(self):
        records = {
            'rebase_failure': [{'name': 'operator-fbc'}, {'name': 'operator-fbc'}],
            'build_failure': [{'name': 'operator'}],
            'fbc_failure': [{'name': 'operator'}],
        }
        await self.update(records, failed=True)
        await self.update(records, failed=True)
        self.assertEqual(self.values[f'{self.prefix}:operator:operator:failure'], '2')
        self.assertEqual(self.redis['call'].await_count, 2)
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertEqual(len(report.failures), 1)
        self.assertIn('Failed 2 time(s)', report.details())

    async def test_partial_success_resets_only_successful_builds(self):
        await self.update({'build_failure': [{'name': name} for name in ('good', 'bad', 'unselected', 'rebase-only')]})
        await self.update(
            {
                'build_success': [{'name': 'good'}],
                'build_failure': [{'name': 'bad'}, {'name': 'bad'}],
            },
            failed=True,
        )
        self.assertNotIn(f'{self.prefix}:operator:good:failure', self.values)
        self.assertEqual(self.values[f'{self.prefix}:operator:bad:failure'], '2')
        for name in ('unselected', 'rebase-only'):
            self.assertEqual(self.values[f'{self.prefix}:operator:{name}:failure'], '1')

    async def test_full_target_isolation_and_group_collection(self):
        prefixes = [
            self.prefix,
            counter_prefix('openshift-4.18', 'release', '4.18', '4.18'),
            counter_prefix('openshift-4.18', 'stream', '4.19', '4.18'),
            counter_prefix('openshift-4.18', 'stream', '4.18', '4.19'),
            counter_prefix('other-group', 'stream', '4.18', '4.18'),
        ]
        for prefix in prefixes:
            await self.update({'build_failure': [{'name': 'operator'}]}, prefix=prefix)
        await self.update({'build_success': [{'name': 'operator'}]})
        for prefix in prefixes[1:]:
            self.assertIn(f'{prefix}:operator:operator:failure', self.values)
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertEqual(len(report.failures), 3)
        self.assertIn('assembly `release`', report.details())
        self.assertIn('build version `4.19`', report.details())
        self.assertIn('OCP target `4.19`', report.details())

    async def test_unattributed_failure_and_scope_specific_recovery(self):
        await self.update(failed=True)
        fallback = f'{self.prefix}:invocation:{self.scope}:failure'
        self.assertEqual(self.values[fallback], '1')
        await self.update({'build_success': [{'name': 'different'}]}, scope=invocation_scope(only='different'))
        self.assertIn(fallback, self.values)
        await self.update({'build_success': [{'name': 'operator'}]})
        self.assertNotIn(fallback, self.values)

    async def test_missing_records_do_not_establish_recovery(self):
        await self.update(failed=True)
        await self.update(records={})
        self.assertEqual(self.values[f'{self.prefix}:invocation:{self.scope}:failure'], '2')
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertIn('invocation', report.details())

    async def test_known_failure_does_not_clear_previous_invocation_failure(self):
        await self.update(failed=True)
        await self.update({'build_failure': [{'name': 'operator'}]}, failed=True)
        self.assertIn(f'{self.prefix}:invocation:{self.scope}:failure', self.values)

    async def test_skips_do_not_increment_or_clear_invocation_fallback(self):
        await self.update(failed=True)
        fallback = f'{self.prefix}:invocation:{self.scope}:failure'
        await self.update({'skipped': [{'name': 'operator'}]})
        await self.update({'skipped': [{'name': 'operator'}], 'build_success': [{'name': 'other'}]})
        self.assertEqual(self.values[fallback], '1')
        self.assertEqual(self.redis['call'].await_count, 1)

    def test_scope_canonicalization_and_key_hardening(self):
        self.assertEqual(
            invocation_scope(only=' a,b,a ', exclude=' c, d', operator_nvrs='b-1,a-1', skip_tasks=('b', 'a')),
            invocation_scope(only='b,a', exclude='d,c', operator_nvrs='a-1,b-1', skip_tasks=('a', 'b', 'a')),
        )
        self.assertNotEqual(invocation_scope(only='a'), invocation_scope(exclude='a'))
        self.assertNotEqual(invocation_scope(data_gitref='a'), invocation_scope(data_gitref='b'))
        prefix = counter_prefix('group:*?[x]\\', 'assembly:[]', '1:*', '4.18')
        self.assertFalse(any(character in prefix for character in '*?[]\\'))

    async def test_encoded_group_and_operator_do_not_match_unrelated_keys(self):
        prefix = counter_prefix('group*', 'release:x', '1.0', '4.18')
        await self.update({'build_failure': [{'name': 'operator:*'}]}, prefix=prefix)
        await self.update({'build_failure': [{'name': 'other'}]}, prefix=counter_prefix('group-other', 's', '1', '4'))
        report = await collect_fbc_health('group*', self.logger)
        self.assertEqual(len(report.failures), 1)
        self.assertIn('operator `operator:*`', report.details())
        await self.update({'build_success': [{'name': 'operator:*'}]}, prefix=prefix)
        self.assertEqual(len(self.values), 2)

    async def test_write_and_reset_outages_are_best_effort(self):
        self.redis['call'].side_effect = RuntimeError('offline')
        self.redis['delete_keys_by_pattern'].side_effect = RuntimeError('offline')
        await self.update({'build_success': [{'name': 'good'}], 'build_failure': [{'name': 'bad'}]}, failed=True)
        self.assertEqual(self.logger.exception.call_count, 2)

    async def test_metadata_write_failure_keeps_failure_count_visible(self):
        self.redis['set_value'].side_effect = RuntimeError('metadata unavailable')
        await self.update({'build_failure': [{'name': 'operator'}]}, failed=True)
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertEqual(len(report.failures), 1)
        self.assertIn('Failed 1 time(s)', report.details())

    async def test_dry_run_does_not_write_or_reset(self):
        await self.update({'build_success': [{'name': 'good'}], 'build_failure': [{'name': 'bad'}]}, dry_run=True)
        await self.update(failed=True, dry_run=True)
        self.redis['call'].assert_not_awaited()
        self.redis['set_value'].assert_not_awaited()
        self.redis['delete_keys_by_pattern'].assert_not_awaited()

    async def test_read_error_is_not_healthy(self):
        self.redis['get_keys'].side_effect = RuntimeError('offline')
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertTrue(report.error)
        self.assertIn('unavailable', report.summary())
        self.assertIn('recovery cannot be determined', report.details())

    async def test_partial_read_error_retains_entries_and_marks_incomplete(self):
        await self.update({'build_failure': [{'name': 'a'}, {'name': 'b'}]})
        original_get = self.redis['get_value'].side_effect

        async def failing_get(key):
            if ':operator:b:' in key:
                raise RuntimeError('offline')
            return original_get(key)

        self.redis['get_value'].side_effect = failing_get
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertTrue(report.error)
        self.assertEqual(len(report.failures), 1)

    async def test_invalid_count_is_unavailable_not_healthy(self):
        self.values[f'{self.prefix}:operator:operator:failure'] = 'invalid'
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertTrue(report.error)

    async def test_metadata_read_error_does_not_leave_incomplete_entry(self):
        await self.update({'build_failure': [{'name': 'operator'}]})
        original_get = self.redis['get_value'].side_effect

        async def failing_get(key):
            if key.endswith(':jenkins_url'):
                raise RuntimeError('offline')
            return original_get(key)

        self.redis['get_value'].side_effect = failing_get
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertTrue(report.error)
        self.assertEqual(report.failures, {})
        self.assertIn('unavailable', report.details())

    async def test_disappeared_count_is_not_a_failure(self):
        self.redis['get_keys'].return_value = [f'{self.prefix}:operator:operator:failure']
        self.redis['get_keys'].side_effect = None
        report = await collect_fbc_health('openshift-4.18', self.logger)
        self.assertFalse(report.error)
        self.assertEqual(report.failures, {})


class TestFbcPipelineTracking(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.runtime = MagicMock(dry_run=False, doozer_working=self.directory.name)
        self.runtime.new_slack_client.return_value.say = AsyncMock()
        self.runtime.new_slack_client.side_effect = AssertionError('FBC builds must not initialize Slack')
        self.pipeline = BuildFbcPipeline(
            runtime=self.runtime,
            version='4.18',
            assembly='stream',
            data_path='',
            data_gitref='',
            only='',
            exclude='',
            operator_nvrs='',
            fbc_repo='',
            kubeconfig='/unused',
            plr_template='',
            skip_checks=False,
        )
        self.increment = AsyncMock()
        self.reset = AsyncMock()
        for name, mock in (('increment_fail_counter', self.increment), ('reset_fail_counter', self.reset)):
            counter_patch = patch(f'pyartcd.fbc_health.util.{name}', mock)
            counter_patch.start()
            self.addCleanup(counter_patch.stop)
        for name in ('resolve_konflux_namespace_by_product', 'resolve_konflux_kubeconfig_by_product'):
            config_patch = patch(f'pyartcd.pipelines.build_fbc.{name}', return_value='unused')
            config_patch.start()
            self.addCleanup(config_patch.stop)
        reporter_patch = patch('pyartcd.pipelines.build_fbc.get_reporter')
        self.reporter = reporter_patch.start()
        self.reporter.return_value.note = AsyncMock()
        self.addCleanup(reporter_patch.stop)
        group_patch = patch('pyartcd.pipelines.build_fbc.load_group_config', new_callable=AsyncMock, return_value={})
        group_patch.start()
        self.addCleanup(group_patch.stop)
        self.pipeline._check_production_index_exists = AsyncMock(return_value=True)

    async def test_missing_log_preserves_original_exception_and_tracks_fallback(self):
        original = RuntimeError('build failed')
        self.pipeline._run_doozer = AsyncMock(side_effect=original)
        with self.assertRaises(RuntimeError) as caught:
            await self.pipeline.run()
        self.assertIs(caught.exception, original)
        self.assertIn(':invocation:', self.increment.await_args.args[0])
        self.runtime.new_slack_client.return_value.say.assert_not_awaited()

    async def test_success_without_records_is_visible_without_failing_build(self):
        self.pipeline._run_doozer = AsyncMock()
        await self.pipeline.run()
        self.assertIn(':invocation:', self.increment.await_args.args[0])
        self.reset.assert_not_awaited()

    async def test_stale_success_log_cannot_reset_failed_build(self):
        record_path = Path(self.directory.name, 'record.log')
        record_path.write_text('build_fbc_konflux|name=stale|status=0|\n')

        async def fail(*args, **kwargs):
            self.assertFalse(record_path.exists())
            raise RuntimeError('build failed')

        self.pipeline._run_doozer = AsyncMock(side_effect=fail)
        with self.assertRaises(RuntimeError):
            await self.pipeline.run()
        self.reset.assert_not_awaited()
        self.assertIn(':invocation:', self.increment.await_args.args[0])

    async def test_partial_records_update_successes_and_failures(self):
        async def fail(*args, **kwargs):
            Path(self.directory.name, 'record.log').write_text(
                'rebase_fbc_konflux|name=bad-fbc|status=1|\n'
                'build_fbc_konflux|name=good|status=0|fbc_nvr=good-1|bundle_nvrs=good-bundle-1|\n'
                'build_fbc_konflux|name=bad|status=1|\n'
                'failed_fbc_konflux|name=bad|status=1|\n'
                'failed_fbc_konflux|name=before-rebase|status=1|\n'
            )
            raise RuntimeError('partial failure')

        self.pipeline._run_doozer = AsyncMock(side_effect=fail)
        with self.assertRaises(RuntimeError):
            await self.pipeline.run()
        self.assertEqual(self.increment.await_count, 2)
        branches = {call.args[0].rsplit(':', 1)[-1] for call in self.increment.await_args_list}
        self.assertEqual(branches, {'bad', 'before-rebase'})
        self.reset.assert_awaited_once()
        self.assertTrue(self.reset.await_args.args[0].endswith(':operator:good'))

    async def test_redis_failure_never_masks_build_exception(self):
        original = RuntimeError('original failure')
        self.pipeline._run = AsyncMock(side_effect=original)
        self.increment.side_effect = RuntimeError('Redis offline')
        with self.assertRaises(RuntimeError) as caught:
            await self.pipeline.run()
        self.assertIs(caught.exception, original)

    async def test_redis_failure_does_not_fail_successful_build(self):
        async def succeed():
            self.pipeline._fbc_records = {'build_success': [{'name': 'operator'}]}
            return True

        self.pipeline._run = AsyncMock(side_effect=succeed)
        self.reset.side_effect = RuntimeError('Redis offline')
        await self.pipeline.run()
        self.increment.assert_not_awaited()

    async def test_success_resets_build_and_matching_fallback(self):
        async def succeed(*args, **kwargs):
            Path(self.directory.name, 'record.log').write_text(
                'build_fbc_konflux|name=operator|status=0|fbc_nvr=fbc-1|bundle_nvrs=bundle-1|\n'
            )

        self.pipeline._run_doozer = AsyncMock(side_effect=succeed)
        await self.pipeline.run()
        self.increment.assert_not_awaited()
        self.assertEqual(self.reset.await_count, 2)
        branches = [call.args[0] for call in self.reset.await_args_list]
        self.assertTrue(any(branch.endswith(':operator:operator') for branch in branches))
        self.assertTrue(any(':invocation:' in branch for branch in branches))

    async def test_malformed_records_preserve_original_failure(self):
        async def fail(*args, **kwargs):
            Path(self.directory.name, 'record.log').write_text('build_fbc_konflux|status=invalid|\n')
            raise RuntimeError('original failure')

        self.pipeline._run_doozer = AsyncMock(side_effect=fail)
        with self.assertRaisesRegex(RuntimeError, 'original failure'):
            await self.pipeline.run()
        self.assertIn(':invocation:', self.increment.await_args.args[0])
        self.reset.assert_not_awaited()

    async def test_rebase_success_alone_does_not_reset(self):
        async def fail(*args, **kwargs):
            Path(self.directory.name, 'record.log').write_text('rebase_fbc_konflux|name=operator-fbc|status=0|\n')
            raise RuntimeError('before build')

        self.pipeline._run_doozer = AsyncMock(side_effect=fail)
        with self.assertRaisesRegex(RuntimeError, 'before build'):
            await self.pipeline.run()
        self.assertIn(':invocation:', self.increment.await_args.args[0])
        self.reset.assert_not_awaited()

    async def test_reused_build_is_not_a_failure_or_recovery(self):
        async def reuse(*args, **kwargs):
            Path(self.directory.name, 'record.log').write_text('skip_fbc_konflux|name=operator|status=0|\n')

        self.pipeline._run_doozer = AsyncMock(side_effect=reuse)
        await self.pipeline.run()
        self.increment.assert_not_awaited()
        self.reset.assert_not_awaited()

    async def test_production_index_skip_leaves_counters_unchanged(self):
        self.pipeline._check_production_index_exists.return_value = False
        await self.pipeline.run()
        self.increment.assert_not_awaited()
        self.reset.assert_not_awaited()

    async def test_tracking_uses_build_version_and_explicit_target(self):
        self.pipeline.group = 'product-1.0'
        self.pipeline.version = '1.0'
        self.pipeline.major_minor = '4.19'
        self.pipeline._run = AsyncMock(side_effect=RuntimeError('before records'))
        with self.assertRaises(RuntimeError):
            await self.pipeline.run()
        self.assertTrue(
            self.increment.await_args.args[0].startswith(
                'count:fbc-failure:konflux:product-1.0:stream:1.0:4.19:invocation:'
            )
        )

    async def test_tracking_uses_effective_group_target(self):
        self.pipeline._run_doozer = AsyncMock(side_effect=RuntimeError('no outcomes'))
        with patch(
            'pyartcd.pipelines.build_fbc.load_group_config',
            new=AsyncMock(return_value={'vars': {'MAJOR': 4, 'MINOR': 19}}),
        ):
            with self.assertRaises(RuntimeError):
                await self.pipeline.run()
        self.assertTrue(
            self.increment.await_args.args[0].startswith(
                'count:fbc-failure:konflux:openshift-4.18:stream:4.18:4.19:invocation:'
            )
        )

    async def test_dry_run_failure_does_not_write_or_notify(self):
        self.runtime.dry_run = True
        self.pipeline._run = AsyncMock(side_effect=RuntimeError('dry-run failure'))
        with self.assertRaises(RuntimeError):
            await self.pipeline.run()
        self.increment.assert_not_awaited()
        self.reset.assert_not_awaited()
        self.runtime.new_slack_client.return_value.say.assert_not_awaited()


class TestFbcReporting(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(dry_run=False)
        self.runtime.working_dir = Path('/unused')
        self.runtime.new_slack_client.return_value.say = AsyncMock(return_value={'ts': 'thread'})
        self.report = FbcHealthReport(
            failures={
                'key': {
                    'assembly': 'release',
                    'version': '1.0',
                    'target': '4.18',
                    'kind': 'operator',
                    'entity': 'operator',
                    'failure_count': 1,
                    'url': 'https://example.com/build/1',
                },
            }
        )

    def ocp_pipeline(self):
        return ImagesHealthPipeline(
            runtime=self.runtime,
            versions='4.18',
            send_to_release_channel=True,
            public_channel='4.18',
            data_path='',
            data_gitref='',
            image_list='',
            assembly='stream',
            sync_jira=False,
        )

    async def test_ocp_fbc_only_failures_in_both_notification_paths(self):
        pipeline = self.ocp_pipeline()
        pipeline.fbc_health = {'4.18': self.report}
        await pipeline.notify_release_channel('4.18')
        await pipeline.notify_public_channel()
        messages = [call.args[0] for call in pipeline.slack_client.say.await_args_list]
        self.assertEqual(len(messages), 4)
        self.assertIn('1 FBC failure(s)', messages[0])
        for message in (messages[1], messages[3]):
            self.assertIn('OCP target `4.18`', message)
            self.assertIn('assembly `release`', message)
        self.assertFalse(any('All images are healthy' in message for message in messages))

    async def test_ocp_read_failure_is_not_reported_healthy(self):
        pipeline = self.ocp_pipeline()
        pipeline.fbc_health = {'4.18': FbcHealthReport(error=True)}
        await pipeline.notify_release_channel('4.18')
        await pipeline.notify_public_channel()
        messages = [call.args[0] for call in pipeline.slack_client.say.await_args_list]
        self.assertEqual(len(messages), 4)
        self.assertIn('unavailable', messages[0])
        self.assertFalse(any('All images are healthy' in message for message in messages))

    async def test_ocp_run_collects_fbc_without_jira_creation(self):
        pipeline = self.ocp_pipeline()
        pipeline.scanned_versions = ['4.18']
        for method in ('get_report', 'get_rebase_failures', 'get_ec_failures', 'get_release_failures'):
            setattr(pipeline, method, AsyncMock())
        with patch(
            'pyartcd.pipelines.images_health.collect_fbc_health', new=AsyncMock(return_value=self.report)
        ) as collect:
            await pipeline.run()
        collect.assert_awaited_once_with('openshift-4.18', self.runtime.logger)
        self.runtime.new_jira_client.assert_not_called()

    def layered_pipeline(self):
        return LayeredProductsImageHealthPipeline(self.runtime, 'product-1.0', '', '', 'stream', '')

    def layered_report(self, report):
        return LayeredProductHealthReport(
            group='product-1.0',
            product='oadp',
            variant=BuildVariant.OADP,
            build_concerns=[],
            build_failures={},
            its_failures={},
            release_failures={},
            rebase_failures={},
            fbc_health=report,
        )

    async def test_layered_fbc_only_summary_and_detail(self):
        pipeline = self.layered_pipeline()
        await pipeline._notify_slack([self.layered_report(self.report)])
        messages = [call.args[0] for call in pipeline.slack_client.say.await_args_list]
        self.assertEqual(len(messages), 2)
        self.assertIn('1 FBC failure(s)', messages[0])
        self.assertIn('assembly `release`', messages[1])
        self.assertIn('OCP target `4.18`', messages[1])
        self.assertNotIn('healthy', messages[0])

    async def test_layered_read_failure_is_not_reported_healthy(self):
        pipeline = self.layered_pipeline()
        await pipeline._notify_slack([self.layered_report(FbcHealthReport(error=True))])
        messages = [call.args[0] for call in pipeline.slack_client.say.await_args_list]
        self.assertEqual(len(messages), 2)
        self.assertIn('unavailable', messages[0])
        self.assertIn('recovery cannot be determined', messages[1])
        self.assertNotIn('healthy', messages[0])

    async def test_layered_collector_includes_all_fbc_targets(self):
        pipeline = self.layered_pipeline()
        pipeline._get_build_concerns = AsyncMock(return_value=[])
        with (
            patch(
                'pyartcd.pipelines.layered_products_image_health.util.load_group_config',
                new=AsyncMock(return_value={'product': 'oadp'}),
            ),
            patch(
                'pyartcd.pipelines.layered_products_image_health.util.get_counter_failures',
                new=AsyncMock(return_value={}),
            ),
            patch(
                'pyartcd.pipelines.layered_products_image_health.util.get_rebase_failures',
                new=AsyncMock(return_value={}),
            ),
            patch(
                'pyartcd.pipelines.layered_products_image_health.collect_fbc_health',
                new=AsyncMock(return_value=self.report),
            ) as collect,
        ):
            report = await pipeline._collect_group('product-1.0')
        collect.assert_awaited_once_with('product-1.0', self.runtime.logger)
        self.assertIs(report.fbc_health, self.report)

    async def test_dry_run_slack_transport_never_posts(self):
        client = SlackClient(token=None, job_name=None, build_id=None, build_url=None, dry_run=True)
        client._client.chat_postMessage = AsyncMock()
        self.runtime.new_slack_client.return_value = client
        pipeline = self.ocp_pipeline()
        pipeline.fbc_health = {'4.18': self.report}
        await pipeline.notify_release_channel('4.18')
        await pipeline.notify_public_channel()
        await self.layered_pipeline()._notify_slack([self.layered_report(self.report)])
        client._client.chat_postMessage.assert_not_awaited()
