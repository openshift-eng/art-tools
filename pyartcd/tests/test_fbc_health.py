import fnmatch
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.fbc_build import build_fbc_catalogs
from pyartcd.fbc_health import (
    FbcEvidence,
    FbcHealthReport,
    account_run,
    collect_fbc_health,
    counter_prefix,
    read_evidence,
)
from pyartcd.pipelines.build_fbc import BuildFbcPipeline
from pyartcd.pipelines.images_health import ImagesHealthPipeline
from pyartcd.pipelines.layered_products_image_health import LayeredProductsImageHealthPipeline
from pyartcd.slack import SlackClient

from pyartcd import fbc_health


class TestFbcState(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.values = {}
        self.expiry = {}
        self.clock = 0
        self.logger = MagicMock()
        self.prefix = counter_prefix('product', 'oadp', 'stream', '4.18')
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name, 'record.log')
        self.conn = MagicMock()
        self.transaction = MagicMock()
        self.conn.pipeline.return_value.__aenter__ = AsyncMock(return_value=self.transaction)
        self.conn.pipeline.return_value.__aexit__ = AsyncMock()

        async def execute():
            for call in self.transaction.method_calls:
                operation, arguments, options = call
                if operation == 'hincrby':
                    key, field, amount = arguments
                    state = self.values.setdefault(key, {})
                    state[field] = str(int(state.get(field, 0)) + amount)
                elif operation == 'hset':
                    self.values.setdefault(arguments[0], {}).update(options['mapping'])
                elif operation == 'expire':
                    self.expiry[arguments[0]] = self.clock + arguments[1]
            self.transaction.reset_mock()

        self.transaction.execute = AsyncMock(side_effect=execute)

        original_store = fbc_health._store_state

        async def store(key, metadata, increment):
            await original_store.__wrapped__(self.conn, key, metadata, increment)

        async def delete(key):
            self.values.pop(key, None)
            self.expiry.pop(key, None)

        async def read(operation, key, *fields):
            self.assertEqual(operation, 'hmget')
            return [self.values.get(key, {}).get(field) for field in fields]

        self.store = AsyncMock(side_effect=store)
        self.delete = AsyncMock(side_effect=delete)
        self.read = AsyncMock(side_effect=read)
        self.keys = AsyncMock(side_effect=lambda pattern: fnmatch.filter(self.values, pattern))
        for target, mock in (
            ('redis.delete_key', self.delete),
            ('redis.call', self.read),
            ('redis.get_keys', self.keys),
        ):
            patcher = patch(f'pyartcd.fbc_health.{target}', mock)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch('pyartcd.fbc_health._store_state', self.store)
        patcher.start()
        self.addCleanup(patcher.stop)

    def evidence(self, **outcomes):
        return FbcEvidence(
            {name: {'outcome': outcome, 'target': '4.18'} for name, outcome in outcomes.items()}, complete=True
        )

    async def update(self, evidence, version='1.0', artifacts='op-1-1', failed=False, prefix=None, dry_run=False):
        await fbc_health.update_counters(
            prefix or self.prefix,
            version,
            artifacts,
            evidence,
            failed,
            dry_run,
            self.logger,
            url='https://execution.invalid/private?token=placeholder',
        )

    async def test_versions_nvrs_and_selections_do_not_strand_state(self):
        for version in range(50):
            await self.update(self.evidence(operator='failed'), str(version), f'op-{version}-1', failed=True)
        self.assertEqual(self.values[f'{self.prefix}:operator:operator']['failure_count'], '50')
        self.assertEqual(len(self.values), 1)
        await self.update(self.evidence(other='succeeded'), '51', 'other-51-1')
        self.assertEqual(len(self.values), 1)
        await self.update(self.evidence(operator='succeeded'), '52', 'op-52-1')
        self.assertEqual(self.values, {})

    async def test_partial_success_and_scope_isolation(self):
        scopes = [self.prefix] + [
            counter_prefix(*scope)
            for scope in (
                ('other', 'oadp', 'stream', '4.18'),
                ('product', 'rhmtc', 'stream', '4.18'),
                ('product', 'oadp', 'release', '4.18'),
                ('product', 'oadp', 'stream', '4.19'),
            )
        ]
        for scope in scopes:
            await self.update(self.evidence(good='failed', bad='failed'), prefix=scope, failed=True)
        await self.update(self.evidence(good='succeeded', bad='failed'), failed=True)
        self.assertNotIn(f'{self.prefix}:operator:good', self.values)
        self.assertEqual(self.values[f'{self.prefix}:operator:bad']['failure_count'], '2')
        for scope in scopes[1:]:
            self.assertIn(f'{scope}:operator:good', self.values)
        report = await collect_fbc_health('product', self.logger, variant='oadp')
        self.assertEqual(len(report.failures), 5)

    async def test_atomic_expiry_and_no_refresh_on_reads_or_unrelated_builds(self):
        await self.update(self.evidence(operator='failed'), failed=True)
        self.conn.pipeline.assert_called_with(transaction=True)
        key = f'{self.prefix}:operator:operator'
        self.assertEqual(self.expiry[key], fbc_health.RETENTION_SECONDS)
        self.clock = 100
        await collect_fbc_health('product', self.logger)
        await self.update(self.evidence(other='succeeded'))
        self.assertEqual(self.expiry[key], fbc_health.RETENTION_SECONDS)
        await self.update(self.evidence(operator='failed'), failed=True)
        self.assertEqual(self.expiry[key], fbc_health.RETENTION_SECONDS + 100)
        self.clock += fbc_health.RETENTION_SECONDS
        for expired in list(self.expiry):
            if self.expiry[expired] <= self.clock:
                self.values.pop(expired)
        self.assertEqual((await collect_fbc_health('product', self.logger)).summary(), '')

    async def test_invocation_supersession_skips_and_unavailable_evidence(self):
        missing = FbcEvidence(unavailable=True)
        await self.update(missing)
        self.assertNotIn(f'{self.prefix}:invocation:latest', self.values)
        self.assertNotIn('failure_count', self.values[f'{self.prefix}:tracking:latest'])
        await self.update(missing, failed=True)
        await self.update(missing, artifacts='different-2-1', failed=True)
        self.assertEqual(self.values[f'{self.prefix}:invocation:latest']['failure_count'], '2')
        await self.update(self.evidence(operator='reused', other='skipped'))
        self.assertIn(f'{self.prefix}:invocation:latest', self.values)
        await self.update(self.evidence(operator='failed'), failed=True)
        self.assertNotIn(f'{self.prefix}:invocation:latest', self.values)
        await self.update(self.evidence(operator='reused'))
        self.assertIn(f'{self.prefix}:operator:operator', self.values)

    async def test_partial_malformed_records_preserve_valid_outcomes(self):
        await self.update(self.evidence(good='failed', bad='failed'))
        self.path.write_text(
            'fbc_outcome|name=good|outcome=succeeded|target=4.18|\n'
            'fbc_outcome|name=bad|outcome=failed|\n'
            'fbc_outcome|name=broken|outcome=invalid|\n'
            'fbc_invocation|status=complete|operators=3|target=4.18|\n'
        )
        evidence = read_evidence(self.path)
        self.assertTrue(evidence.unavailable)
        await self.update(evidence, failed=True)
        self.assertNotIn(f'{self.prefix}:operator:good', self.values)
        self.assertEqual(self.values[f'{self.prefix}:operator:bad']['failure_count'], '2')
        self.assertIn(f'{self.prefix}:tracking:latest', self.values)
        self.assertNotIn(f'{self.prefix}:operator:broken', self.values)

    async def test_missing_log_and_malformed_completion_are_unavailable(self):
        self.assertTrue(read_evidence(self.path).unavailable)
        self.path.write_text('fbc_outcome|name=op|outcome=failed|\nfbc_invocation|operators=invalid|status=complete|\n')
        evidence = read_evidence(self.path)
        self.assertTrue(evidence.unavailable)
        self.assertIn('op', evidence.outcomes)
        await self.update(evidence, failed=True)
        self.assertNotIn(f'{self.prefix}:invocation:latest', self.values)

    async def test_stale_unreadable_evidence_and_resolved_configuration(self):
        unknown = counter_prefix('product', 'unknown', 'stream', '4.18')
        await self.update(FbcEvidence(unavailable=True), prefix=unknown, failed=True)
        self.path.write_text(
            'fbc_outcome|name=operator|outcome=failed|operator_nvr=operator-2-1|\n'
            'fbc_invocation|status=complete|operators=1|target=4.18|\n'
        )
        await account_run(self.path, 'product', 'oadp', 'stream', '4.18', '2', '', True, False, self.logger)
        self.assertNotIn(f'{unknown}:invocation:latest', self.values)
        self.assertEqual(self.values[f'{self.prefix}:operator:operator']['operator_nvr'], 'operator-2-1')
        self.path.write_text('fbc_outcome|name=operator|outcome=succeeded|target=4.18|\n')
        await account_run(
            self.path, 'product', 'oadp', 'stream', '4.18', '3', '', False, False, self.logger, fresh=False
        )
        self.assertIn(f'{self.prefix}:operator:operator', self.values)
        self.assertIn(f'{self.prefix}:tracking:latest', self.values)

    async def test_conflicting_outcomes_cannot_establish_recovery(self):
        self.path.write_text(
            'fbc_outcome|name=operator|outcome=succeeded|target=4.18|\n'
            'fbc_outcome|name=operator|outcome=failed|\n'
            'fbc_invocation|status=complete|operators=1|target=4.18|\n'
        )
        evidence = read_evidence(self.path)
        self.assertTrue(evidence.unavailable)
        self.assertEqual(evidence.outcomes['operator']['outcome'], 'failed')

    async def test_legacy_direct_target_comes_from_execution_not_product_version(self):
        self.path.write_text(
            'fbc_outcome|name=operator|outcome=failed|\nfbc_invocation|status=complete|operators=1|target=4.18|\n'
        )
        await account_run(self.path, 'product', 'oadp', 'stream', 'default', '1.2.3', '', True, False, self.logger)
        self.assertIn(f'{self.prefix}:operator:operator', self.values)

    async def test_success_requires_actual_target_evidence(self):
        await self.update(self.evidence(operator='failed'), failed=True)
        self.path.write_text('fbc_outcome|name=operator|outcome=succeeded|target=4.19|\n')
        await account_run(self.path, 'product', 'oadp', 'stream', '4.18', '2', '', False, False, self.logger)
        self.assertIn(f'{self.prefix}:operator:operator', self.values)
        self.assertIn(f'{self.prefix}:tracking:latest', self.values)

    async def test_dry_run_atomic_and_reset_failures(self):
        await self.update(self.evidence(operator='failed'), dry_run=True)
        self.store.assert_not_awaited()
        self.transaction.execute.side_effect = RuntimeError('unavailable')
        await self.update(self.evidence(operator='failed'), failed=True)
        self.assertEqual(self.values, {})
        self.delete.side_effect = RuntimeError('unavailable')
        await self.update(self.evidence(operator='succeeded'))
        self.assertTrue(self.logger.warning.called)

    async def test_partial_read_failure_urls_and_metadata_excluded(self):
        await self.update(self.evidence(first='failed', second='failed'), failed=True)
        first_key = f'{self.prefix}:operator:first'
        self.values[f'{self.prefix}:operator:second']['failure_count'] = 'invalid'
        report = await collect_fbc_health('product', self.logger)
        self.assertTrue(report.error)
        self.assertIn(first_key, report.failures)
        self.assertIn('unavailable', report.summary())
        self.assertNotIn('execution.invalid', repr(report))
        self.assertNotIn('token=', report.details())
        self.assertNotIn('operator_nvrs', repr(report))
        self.keys.side_effect = RuntimeError('unavailable')
        self.assertTrue((await collect_fbc_health('product', self.logger)).error)

    def test_key_hardening(self):
        prefix = counter_prefix('group:*', 'variant?', 'release[]', '4.18')
        self.assertFalse(any(character in prefix for character in '*?[]'))


class TestFbcProducers(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.runtime = MagicMock(dry_run=False, doozer_working=self.directory.name)
        self.runtime.working_dir = Path(self.directory.name)
        self.runtime.new_slack_client.side_effect = AssertionError('No immediate Slack')

    def pipeline(self):
        return BuildFbcPipeline(self.runtime, '4.18', 'stream', '', '', '', '', 'operator-1-1', '', '', '', False)

    async def test_original_entrypoint_accounts_once_preserves_error_and_no_slack(self):
        pipeline = self.pipeline()
        original = RuntimeError('original build failure')
        pipeline._run = AsyncMock(side_effect=original)
        Path(self.directory.name, 'record.log').write_text('stale evidence')
        with patch('pyartcd.pipelines.build_fbc.account_run', new_callable=AsyncMock) as account:
            with self.assertRaises(RuntimeError) as caught:
                await pipeline.run()
            self.assertIs(caught.exception, original)
            account.assert_awaited_once()
            self.assertTrue(account.await_args.args[-3])
            self.assertFalse(Path(self.directory.name, 'record.log').exists())
        self.runtime.new_slack_client.assert_not_called()

    async def test_production_skip_does_not_change_state(self):
        pipeline = self.pipeline()
        pipeline._run = AsyncMock(return_value=None)
        with patch('pyartcd.pipelines.build_fbc.account_run', new_callable=AsyncMock) as account:
            await pipeline.run()
        account.assert_not_awaited()

    async def test_tracking_failure_does_not_fail_original_success(self):
        pipeline = self.pipeline()
        pipeline._run = AsyncMock(return_value=True)
        with patch('pyartcd.pipelines.build_fbc.account_run', new=AsyncMock(side_effect=RuntimeError('tracking'))):
            await pipeline.run()

    async def test_direct_catalog_entrypoint_accounts_once_per_target(self):
        async def run_locked(coro, **kwargs):
            return await coro

        async def command(cmd, **kwargs):
            target = next(arg.split('=', 1)[1] for arg in cmd if arg.startswith('--major-minor='))
            return (
                0,
                json.dumps({'nvrs': [f'op-fbc-1-1.ocp{target}'], 'pullspecs': ['quay.io/example/fbc@sha256:a']}),
                '',
            )

        with (
            patch('pyartcd.fbc_build.account_run', new_callable=AsyncMock) as account,
            patch('pyartcd.fbc_build.locks.run_with_lock', new=AsyncMock(side_effect=run_locked)),
            patch('pyartcd.fbc_build.jenkins.get_build_path_or_random', return_value='test'),
            patch('pyartcd.fbc_build.resolve_konflux_namespace_by_product', return_value='test'),
            patch('pyartcd.fbc_build.resolve_konflux_kubeconfig_by_product', return_value='test'),
            patch('pyartcd.fbc_build.exectools.cmd_gather_async', new=AsyncMock(side_effect=command)) as cmd,
        ):
            arguments = dict(
                runtime=self.runtime,
                group='product',
                assembly='release',
                version='2',
                product='oadp',
                operator_nvrs_by_target={'4.18': ['op-2-1'], '4.19': ['op-2-1']},
            )
            await build_fbc_catalogs(**arguments)
            self.assertEqual(account.await_count, 2)
            self.assertEqual({call.args[4] for call in account.await_args_list}, {'4.18', '4.19'})
            self.assertTrue(all(not call.args[-3] for call in account.await_args_list))
            account.reset_mock()
            cmd.side_effect = RuntimeError('before records')
            with self.assertRaises(RuntimeError):
                await build_fbc_catalogs(**arguments)
            self.assertEqual(account.await_count, 2)
            self.assertTrue(all(call.args[-3] for call in account.await_args_list))
            account.reset_mock()
            self.runtime.dry_run = True
            await build_fbc_catalogs(**arguments)
            account.assert_not_awaited()


class TestFbcConsumers(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(dry_run=False)
        self.runtime.working_dir = Path('/unused')
        self.runtime.new_slack_client.return_value.say = AsyncMock(return_value={'ts': 'thread'})
        self.report = FbcHealthReport(
            {
                'key': dict(
                    variant='oadp',
                    assembly='release',
                    target='4.18',
                    kind='operator',
                    entity='operator',
                    version='2',
                    failure_count=3,
                )
            }
        )

    def ocp(self):
        return ImagesHealthPipeline(self.runtime, '4.18', True, 'test', '', '', '', 'stream', sync_jira=False)

    async def test_ocp_collection_failure_retains_fbc_and_propagates_after_notifications(self):
        pipeline = self.ocp()

        async def fail(version):
            pipeline.scanned_versions.append(version)
            raise RuntimeError('image query unavailable')

        pipeline.get_report = AsyncMock(side_effect=fail)
        for method in ('get_rebase_failures', 'get_ec_failures', 'get_release_failures'):
            setattr(pipeline, method, AsyncMock())
        with patch('pyartcd.pipelines.images_health.collect_fbc_health', new=AsyncMock(return_value=self.report)):
            with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                await pipeline.run()
        messages = [call.args[0] for call in pipeline.slack_client.say.await_args_list]
        self.assertTrue(any('incomplete' in message for message in messages))
        self.assertEqual(sum('Failed 3 time(s)' in message for message in messages), 2)
        self.runtime.new_jira_client.assert_not_called()

    async def test_frozen_group_is_not_collected_or_reported(self):
        pipeline = self.ocp()
        with (
            patch('pyartcd.pipelines.images_health.util.is_build_permitted', new=AsyncMock(return_value=False)),
            patch('pyartcd.pipelines.images_health.collect_fbc_health', new_callable=AsyncMock) as collect,
        ):
            await pipeline.run()
        collect.assert_not_awaited()
        self.assertEqual(pipeline.scanned_versions, [])

    async def test_layered_config_or_image_exception_preserves_fbc(self):
        for stage in ('configuration', 'images'):
            with self.subTest(stage=stage):
                pipeline = LayeredProductsImageHealthPipeline(self.runtime, 'product', '', '', 'stream', '')
                pipeline._get_build_concerns = AsyncMock(side_effect=RuntimeError('query'))
                with (
                    patch(
                        'pyartcd.pipelines.layered_products_image_health.collect_fbc_health',
                        new=AsyncMock(return_value=self.report),
                    ),
                    patch(
                        'pyartcd.pipelines.layered_products_image_health.util.load_group_config',
                        new=AsyncMock(
                            side_effect=RuntimeError('config') if stage == 'configuration' else None,
                            return_value={'product': 'oadp'},
                        ),
                    ),
                    patch(
                        'pyartcd.pipelines.layered_products_image_health.util.get_counter_failures',
                        new=AsyncMock(return_value={}),
                    ),
                    patch(
                        'pyartcd.pipelines.layered_products_image_health.util.get_rebase_failures',
                        new=AsyncMock(return_value={}),
                    ),
                ):
                    with self.assertRaises(RuntimeError):
                        await pipeline.run()
                messages = [call.args[0] for call in pipeline.slack_client.say.await_args_list]
                self.assertTrue(any('3 time(s)' in message for message in messages))
                self.assertTrue(any('incomplete' in message for message in messages))

    async def test_fbc_only_and_read_error_reach_summary_and_threads(self):
        pipeline = self.ocp()
        for report in (self.report, FbcHealthReport(error=True)):
            pipeline.fbc_health = {'4.18': report}
            pipeline.slack_client.say.reset_mock()
            await pipeline.notify_release_channel('4.18')
            await pipeline.notify_public_channel()
            messages = [call.args[0] for call in pipeline.slack_client.say.await_args_list]
            self.assertEqual(len(messages), 4)
            self.assertTrue(all('healthy' not in message for message in messages))
            self.assertIn(report.details(), messages[1])
            self.assertIn(report.details(), messages[3])

    async def test_absence_means_no_tracked_failures_not_verified_health(self):
        pipeline = self.ocp()
        await pipeline.notify_release_channel('4.18')
        self.assertIn('No tracked failures', pipeline.slack_client.say.await_args.args[0])

    async def test_dry_run_slack_transport_does_not_post(self):
        client = SlackClient(token=None, job_name=None, build_id=None, build_url=None, dry_run=True)
        client._client.chat_postMessage = AsyncMock()
        self.runtime.new_slack_client.return_value = client
        pipeline = self.ocp()
        pipeline.public_channel = '#test'
        pipeline.fbc_health = {'4.18': self.report}
        await pipeline.notify_release_channel('4.18')
        await pipeline.notify_public_channel()
        client._client.chat_postMessage.assert_not_awaited()
