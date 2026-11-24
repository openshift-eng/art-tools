import asyncio
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib.cli.scan_sources_konflux import (
    ConfigScanSources,
    config_scan_source_changes_konflux,
)
from doozerlib.image import ImageMetadata
from doozerlib.metadata import RebuildHint, RebuildHintCode
from doozerlib.runtime import Runtime

CUTOFF = datetime(2026, 11, 16, tzinfo=timezone.utc)


def build_record(start_time, outcome=KonfluxBuildOutcome.SUCCESS):
    record = MagicMock(spec=KonfluxBuildRecord)
    record.nvr = 'test-component-1.0-1.el9'
    record.start_time = start_time
    record.commitish = 'abc123'
    record.outcome = outcome
    return record


class TestBuildCutoffOption(TestCase):
    def test_default_and_explicit_cutoffs(self):
        for args, expected in [
            ([], CUTOFF),
            (['--rebuild-before', '2026-11-16'], CUTOFF.replace(tzinfo=None)),
            (['--rebuild-before', '2026-11-15T19:00:00-05:00'], CUTOFF),
            (['--rebuild-before', '2026-11-16T00:00:00.000000Z'], CUTOFF),
        ]:
            with self.subTest(args=args):
                with config_scan_source_changes_konflux.make_context('scan', args) as ctx:
                    self.assertEqual(ctx.params['rebuild_before'], expected)

    def test_invalid_cutoff_fails_before_initialization(self):
        runtime = MagicMock(spec=Runtime)
        for value in ['yesterday', '2026-02-30', '2026-11-16T12:00:00']:
            with self.subTest(value=value):
                result = CliRunner().invoke(
                    config_scan_source_changes_konflux, ['--rebuild-before', value], obj=runtime
                )
                self.assertEqual(result.exit_code, 2)
                self.assertIn('--rebuild-before', result.output)
        runtime.initialize.assert_not_called()

    def test_cli_passes_default_cutoff_and_skip_rpms_to_scanner(self):
        runtime = MagicMock(spec=Runtime)
        runtime.variant = BuildVariant.OCP
        with asyncio.Runner(), patch('doozerlib.cli.scan_sources_konflux.ConfigScanSources') as scanner_class:
            scanner_class.return_value.run = AsyncMock()
            result = CliRunner().invoke(config_scan_source_changes_konflux, ['--skip-rpms'], obj=runtime)
        self.assertEqual(result.exit_code, 0, result.output)
        runtime.initialize.assert_called_once_with(mode='images', clone_distgits=False)
        self.assertEqual(scanner_class.call_args.kwargs['rebuild_before'], CUTOFF)
        self.assertTrue(scanner_class.call_args.kwargs['skip_rpms'])
        scanner_class.return_value.run.assert_awaited_once()


class TestBuildCutoff(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.konflux_db = MagicMock()
        self.runtime.group = 'openshift-4.22'
        self.runtime.assembly = 'stream'
        self.runtime.group_config = Model({'scan_freshness': {'threshold_hours': 6}})
        self.runtime.load_disabled = False
        self.runtime.image_metas.return_value = []
        self.runtime.rpm_metas.return_value = []
        self.scanner = ConfigScanSources(self.runtime, '', MagicMock(), as_yaml=True, rebuild_before=CUTOFF)
        self.scanner.scan_for_config_changes = AsyncMock()
        self.scanner.scan_for_upstream_changes = AsyncMock()
        self.image = MagicMock(spec=ImageMetadata)
        self.image.distgit_key = 'test-image'
        self.image.qualified_key = 'containers/test-image'
        self.image.config = Model({})
        self.image.enabled = True
        self.image.branch_el_target.return_value = 9
        self.image.get_descendants.return_value = []
        self.image.get_latest_build = AsyncMock(return_value=None)
        self.scanner.latest_image_build_records_map = {
            self.image.distgit_key: build_record(CUTOFF - timedelta(seconds=1))
        }

    def test_cutoff_boundaries_and_timezones(self):
        for start_time, expected in [
            (CUTOFF - timedelta(microseconds=1), True),
            (CUTOFF, False),
            (CUTOFF + timedelta(seconds=1), False),
            (datetime(2026, 11, 15, 19, tzinfo=timezone(timedelta(hours=-5))), False),
            (datetime(2026, 11, 15, 18, 59, tzinfo=timezone(timedelta(hours=-5))), True),
            (datetime(2026, 11, 15), True),
            (datetime(2026, 11, 16), False),
            (None, True),
        ]:
            with self.subTest(start_time=start_time):
                hint = self.scanner.get_build_cutoff_hint(build_record(start_time))
                self.assertEqual(hint is not None, expected)
                if hint:
                    self.assertEqual(hint.code, RebuildHintCode.BUILD_BEFORE_CUTOFF)
                    self.assertTrue(hint.rebuild)
                    self.assertIn('test-component-1.0-1.el9', hint.reason)
                    self.assertIn(CUTOFF.isoformat(), hint.reason)

    def test_no_cutoff_preserves_existing_behavior(self):
        self.scanner.rebuild_before = None
        self.assertIsNone(self.scanner.get_build_cutoff_hint(build_record(None)))

    def test_date_only_constructor_value_is_utc(self):
        scanner = ConfigScanSources(self.runtime, '', MagicMock(), False, rebuild_before=datetime(2026, 11, 16))
        self.assertEqual(scanner.rebuild_before, CUTOFF)

    async def test_old_images_rebuild_in_ocp_and_lp_after_source_and_config_checks(self):
        for group in ['openshift-4.22', 'oadp-1.5']:
            with self.subTest(group=group):
                self.runtime.group = group
                self.scanner.changing_image_names.clear()
                with patch.object(self.scanner, 'scan_for_config_changes', new_callable=AsyncMock) as config_scan:
                    await self.scanner.scan_image(self.image)
                self.assertIn('test-image', self.scanner.changing_image_names)
                self.assertEqual(self.scanner.issues, [])
                config_scan.assert_awaited_once_with(self.image)
                self.scanner.scan_for_upstream_changes.assert_awaited_with(self.image)
                self.assertEqual(
                    self.scanner.assessment_code['containers/test-image+True'], RebuildHintCode.BUILD_BEFORE_CUTOFF
                )

    async def test_new_upstream_fix_is_not_delayed_by_old_commit_failure(self):
        now = CUTOFF + timedelta(hours=12)
        failed_attempt = build_record(now - timedelta(hours=1), KonfluxBuildOutcome.BUILD_ERROR)
        failed_attempt.commitish = 'old-commit'

        async def latest_build(**kwargs):
            if kwargs.get('extra_patterns', {}).get('commitish') == 'new-fix':
                return None
            return failed_attempt

        self.image.get_latest_build = AsyncMock(side_effect=latest_build)
        self.scanner.scan_for_upstream_changes = ConfigScanSources.scan_for_upstream_changes.__get__(self.scanner)
        with (
            patch.object(self.scanner, 'find_upstream_commit_hash', return_value='new-fix'),
            patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime,
        ):
            mock_datetime.now.return_value = now
            await self.scanner.scan_image(self.image)

        self.assertEqual(self.scanner.issues, [])
        self.assertIn('test-image', self.scanner.changing_image_names)
        self.assertEqual(
            self.scanner.assessment_code['containers/test-image+True'], RebuildHintCode.NEW_UPSTREAM_COMMIT
        )

    async def test_config_fix_is_not_delayed_by_old_failure(self):
        self.image.get_latest_build.return_value = build_record(
            CUTOFF + timedelta(hours=11), KonfluxBuildOutcome.BUILD_ERROR
        )
        self.scanner.scan_for_config_changes.side_effect = lambda meta: self.scanner.add_image_meta_change(
            meta, RebuildHint(RebuildHintCode.CONFIG_CHANGE, 'Metadata configuration change')
        )

        await self.scanner.scan_image(self.image)

        self.assertEqual(self.scanner.issues, [])
        self.assertIn('test-image', self.scanner.changing_image_names)
        self.assertEqual(self.scanner.assessment_code['containers/test-image+True'], RebuildHintCode.CONFIG_CHANGE)
        self.image.get_latest_build.assert_not_awaited()

    async def test_embargoed_rebuild_reports_stale_build_and_plans_public_parents(self):
        private = build_record(CUTOFF + timedelta(days=5))
        private.nvr = 'test-component-1.0-2.p3.el9'
        private.embargoed = True
        public = build_record(CUTOFF - timedelta(days=3))
        public.nvr = 'test-component-1.0-1.p2.el9'
        public.embargoed = False
        self.image.is_payload = True
        self.image.for_release = True
        parent = MagicMock(spec=ImageMetadata)
        parent.distgit_key = 'public-parent'
        parent.enabled = True
        parent.config = Model({})
        parent.branch_el_target.return_value = 9
        parent.get_latest_konflux_build.return_value = None
        self.image.get_parent_members.return_value = {'public-parent': None}
        self.runtime.late_resolve_image.return_value = parent
        self.runtime.image_map = {'test-image': self.image}
        self.runtime.image_tree = {'test-image': {}}
        self.scanner.all_image_metas = {self.image}
        self.scanner.skip_rpms = True
        self.image.get_latest_build.return_value = private
        self.image.get_latest_konflux_build.return_value = public

        with (
            patch.object(self.scanner, 'get_current_task_bundle_shas', AsyncMock(return_value={})),
            patch.object(self.scanner, 'scan_image', AsyncMock()),
            patch('doozerlib.cli.scan_sources_konflux.click.echo') as echo,
        ):
            await self.scanner.run()

        self.image.get_latest_konflux_build.assert_awaited_once_with(default=None, el_target=9, embargoed=False)
        self.assertEqual(self.scanner.changing_image_names, set())
        self.assertEqual(self.scanner.issues, [])
        report = yaml.safe_load(echo.call_args.args[0])
        self.assertEqual(report['images'], [])
        self.assertEqual(report['public_rebuild']['images'], ['public-parent', 'test-image'])
        parent.get_latest_konflux_build.assert_awaited_once_with(el_target=9, embargoed=False)
        finding = report['public_build_cutoff'][0]
        self.assertEqual(finding['name'], 'test-image')
        self.assertEqual(finding['latest_nvr'], private.nvr)
        self.assertEqual(finding['public_nvr'], public.nvr)
        self.assertIn(public.start_time.isoformat(), finding['reason'])
        self.assertIn(CUTOFF.isoformat(), finding['reason'])

    async def test_public_rebuild_excludes_konflux_disabled_and_wip_images_and_parents(self):
        private = build_record(CUTOFF + timedelta(days=5))
        private.embargoed = True
        self.image.is_payload = True
        self.image.for_release = True
        self.image.get_latest_konflux_build.return_value = None

        ineligible = {
            f'{mode}-image': MagicMock(
                spec=ImageMetadata,
                distgit_key=f'{mode}-image',
                enabled=True,
                is_payload=True,
                for_release=True,
                config=Model({'konflux': {'mode': mode}}),
            )
            for mode in ('disabled', 'wip')
        }
        for image in ineligible.values():
            image.get_latest_konflux_build.return_value = None

        self.image.get_parent_members.return_value = ineligible
        self.runtime.image_map = {'test-image': self.image, **ineligible}
        self.runtime.image_tree = {}
        self.scanner.all_image_metas = set(self.runtime.image_map.values())
        self.scanner.skip_rpms = True
        self.scanner.latest_image_build_records_map = {name: private for name in self.runtime.image_map}

        with (
            patch.object(self.scanner, 'get_current_task_bundle_shas', AsyncMock(return_value={})),
            patch('doozerlib.cli.scan_sources_konflux.click.echo') as echo,
        ):
            await self.scanner.run()

        report = yaml.safe_load(echo.call_args.args[0])
        self.assertEqual([finding['name'] for finding in report['public_build_cutoff']], ['test-image'])
        self.assertEqual(report['public_rebuild']['images'], ['test-image'])
        for image in ineligible.values():
            image.get_latest_konflux_build.assert_not_awaited()

    async def test_embargoed_rebuild_with_fresh_or_missing_public_build(self):
        private = build_record(CUTOFF + timedelta(days=5))
        private.embargoed = True
        self.scanner.latest_image_build_records_map['test-image'] = private
        public = build_record(CUTOFF)
        public.embargoed = False
        self.image.get_latest_konflux_build.return_value = public
        await self.scanner.check_public_build_cutoff(self.image)
        self.assertEqual(self.scanner.public_build_cutoff, [])

        for missing_or_private in (None, private):
            with self.subTest(public=missing_or_private):
                self.image.get_latest_konflux_build.return_value = missing_or_private
                await self.scanner.check_public_build_cutoff(self.image)
                finding = self.scanner.public_build_cutoff.pop()
                self.assertIsNone(finding['public_nvr'])
                self.assertIn('No eligible public build', finding['reason'])

    async def test_successful_rebuild_satisfies_cutoff(self):
        self.scanner.latest_image_build_records_map['test-image'] = build_record(CUTOFF)
        self.assertFalse(await self.scanner.scan_build_cutoff_changes(self.image))
        self.assertEqual(self.scanner.changing_image_names, set())
        self.image.get_latest_build.assert_not_awaited()

    async def test_stale_pin_reports_issue_without_repeated_cutoff_rebuilds(self):
        pinned_build = self.scanner.latest_image_build_records_map['test-image']
        self.image.config = Model({'is': {'nvr': pinned_build.nvr}})
        child = MagicMock(spec=ImageMetadata)
        child.distgit_key = 'child'
        child.qualified_key = 'containers/child'
        self.image.get_descendants.return_value = [child]
        self.scanner.all_image_metas = {self.image}

        # Even a successful replacement cannot change the NVR selected by the pin.
        self.image.get_latest_build.return_value = build_record(CUTOFF + timedelta(hours=1))
        for _ in range(2):
            self.scanner.issues.clear()
            self.assertFalse(await self.scanner.scan_build_cutoff_changes(self.image))
            self.assertEqual(self.scanner.changing_image_names, set())
            self.assertEqual(self.scanner.assessment_code, {})
            self.assertEqual(len(self.scanner.issues), 1)
            self.assertEqual(self.scanner.issues[0]['name'], 'test-image')
            self.assertIn(pinned_build.nvr, self.scanner.issues[0]['issue'])
            self.assertIn('update or remove the is pin', self.scanner.issues[0]['issue'])

            with patch('doozerlib.cli.scan_sources_konflux.click.echo') as echo:
                self.scanner.generate_report()
            report = yaml.safe_load(echo.call_args.args[0])
            self.assertEqual(report['images'], [])
            self.assertEqual(report['issues'], self.scanner.issues)

        self.image.get_latest_build.assert_not_awaited()
        self.image.get_descendants.assert_not_called()

    async def test_fresh_pin_does_not_report_cutoff_issue(self):
        pinned_build = build_record(CUTOFF)
        self.image.config = Model({'is': {'nvr': pinned_build.nvr}})
        self.scanner.latest_image_build_records_map['test-image'] = pinned_build

        self.assertFalse(await self.scanner.scan_build_cutoff_changes(self.image))

        self.assertEqual(self.scanner.issues, [])
        self.assertEqual(self.scanner.changing_image_names, set())
        self.image.get_latest_build.assert_not_awaited()

    async def test_missing_start_time_is_rebuilt(self):
        self.scanner.latest_image_build_records_map['test-image'] = build_record(None)
        await self.scanner.scan_image(self.image)
        self.assertIn('test-image', self.scanner.changing_image_names)
        self.assertIn('has no start time', self.scanner.assessment_reason['containers/test-image+True'])

    async def test_descendants_and_yaml_report_include_rebuild_reason(self):
        child = MagicMock(spec=ImageMetadata)
        child.distgit_key = 'child'
        child.qualified_key = 'containers/child'
        child.config = Model({})
        child.enabled = True
        self.image.get_descendants.return_value = [child]
        self.scanner.all_image_metas = {self.image, child}
        self.runtime.image_map = {'test-image': self.image, 'child': child}

        await self.scanner.scan_image(self.image)
        with patch('doozerlib.cli.scan_sources_konflux.click.echo') as echo:
            self.scanner.generate_report()
        report = yaml.safe_load(echo.call_args.args[0])
        results = {image['name']: image for image in report['images']}
        self.assertEqual(results['test-image']['code'], 'BUILD_BEFORE_CUTOFF')
        self.assertTrue(results['test-image']['changed'])
        self.assertEqual(results['child']['code'], 'ANCESTOR_CHANGING')

    async def test_recent_failure_delays_cutoff_rebuild(self):
        now = CUTOFF + timedelta(hours=12)
        for outcome in KonfluxBuildOutcome:
            if not outcome.is_failure():
                continue
            with self.subTest(outcome=outcome):
                attempts = [
                    self.scanner.latest_image_build_records_map['test-image'],
                    build_record(now - timedelta(hours=7), KonfluxBuildOutcome.FAILURE),
                    build_record(now - timedelta(hours=1), outcome),
                ]

                async def latest_build(**kwargs):
                    requested_outcome = kwargs['outcome']
                    matches = [r for r in attempts if requested_outcome is None or r.outcome == requested_outcome]
                    return max(matches, key=lambda r: r.start_time, default=None)

                self.image.get_latest_build = AsyncMock(side_effect=latest_build)
                with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
                    mock_datetime.now.return_value = now
                    await self.scanner.scan_image(self.image)
                self.assertEqual(self.scanner.issues, [])
                self.assertEqual(self.scanner.changing_image_names, set())
                self.assertEqual(
                    self.scanner.assessment_code['containers/test-image+False'], RebuildHintCode.DELAYING_NEXT_ATTEMPT
                )
                self.assertIsNone(self.image.get_latest_build.call_args.kwargs['outcome'])

    async def test_expired_failure_allows_cutoff_rebuild(self):
        now = CUTOFF + timedelta(hours=12)
        self.image.get_latest_build.return_value = build_record(
            now - timedelta(hours=7), KonfluxBuildOutcome.BUILD_ERROR
        )
        with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
            mock_datetime.now.return_value = now
            await self.scanner.scan_image(self.image)
        self.assertIn('test-image', self.scanner.changing_image_names)

    async def test_successful_attempt_does_not_delay_cutoff_rebuild(self):
        self.image.get_latest_build.return_value = self.scanner.latest_image_build_records_map['test-image']
        await self.scanner.scan_image(self.image)
        self.assertEqual(self.scanner.issues, [])
        self.assertIn('test-image', self.scanner.changing_image_names)

    async def test_upstream_commit_build_error_preserves_backoff(self):
        now = CUTOFF + timedelta(hours=12)
        failed_attempt = build_record(now - timedelta(hours=1), KonfluxBuildOutcome.BUILD_ERROR)

        async def latest_build(**kwargs):
            return failed_attempt if kwargs['outcome'] is None else None

        self.image.get_latest_build = AsyncMock(side_effect=latest_build)
        with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
            mock_datetime.now.return_value = now
            await self.scanner.handle_missing_upstream_commit_build(self.image, 'abc123')
        self.assertEqual(self.scanner.changing_image_names, set())
        self.assertEqual(self.image.get_latest_build.call_args.kwargs['extra_patterns'], {'commitish': 'abc123'})

    async def test_no_latest_build_keeps_existing_reason(self):
        self.scanner.latest_image_build_records_map.clear()
        await self.scanner.scan_image(self.image)
        self.assertEqual(self.scanner.assessment_code['containers/test-image+True'], RebuildHintCode.NO_LATEST_BUILD)

    async def test_rpm_cutoff_is_checked_per_el_target(self):
        rpm = MagicMock()
        rpm.name = rpm.distgit_key = 'test-rpm'
        rpm.qualified_key = 'rpms/test-rpm'
        rpm.config.targets = ['el8', 'el9']
        rpm.determine_rhel_targets.return_value = [8, 9]
        fresh = build_record(CUTOFF)
        stale = build_record(CUTOFF - timedelta(days=1))
        self.scanner.all_rpm_metas = {rpm}
        self.scanner.latest_rpm_build_records_map = {'test-rpm': {'el8': fresh, 'el9': stale}}

        async def latest_build(**kwargs):
            return fresh

        rpm.get_latest_build = AsyncMock(side_effect=latest_build)
        koji = self.runtime.pooled_koji_client_session.return_value.__enter__.return_value
        koji.getLatestRPMS.return_value = ([], [])
        with (
            patch('doozerlib.cli.scan_sources_konflux.Dir'),
            patch('doozerlib.cli.scan_sources_konflux.cmd_gather_async', AsyncMock(return_value=(0, 'abc123', ''))),
        ):
            await self.scanner.check_changing_rpms()
        self.assertEqual(self.scanner.changing_rpm_names, {'test-rpm'})
        self.assertEqual(self.scanner.assessment_code['rpms/test-rpm+True'], RebuildHintCode.BUILD_BEFORE_CUTOFF)
        self.assertIn('el9:', self.scanner.assessment_reason['rpms/test-rpm+True'])
        failure_queries = [call.kwargs for call in rpm.get_latest_build.await_args_list if 'outcome' in call.kwargs]
        self.assertEqual({query['el_target'] for query in failure_queries}, {'el8', 'el9'})
        self.assertTrue(all(query['engine'] == Engine.BREW.value for query in failure_queries))

    async def test_rpm_recent_failure_preserves_backoff(self):
        rpm = MagicMock()
        rpm.name = rpm.distgit_key = 'test-rpm'
        rpm.config.targets = ['el9']
        rpm.determine_rhel_targets.return_value = [9]
        now = CUTOFF + timedelta(hours=12)
        failed_attempt = build_record(now - timedelta(hours=1), KonfluxBuildOutcome.BUILD_ERROR)

        async def latest_build(**kwargs):
            return failed_attempt if kwargs['outcome'] is None else None

        rpm.get_latest_build = AsyncMock(side_effect=latest_build)
        self.scanner.all_rpm_metas = {rpm}
        self.scanner.latest_rpm_build_records_map = {'test-rpm': {'el9': build_record(CUTOFF - timedelta(days=1))}}
        with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
            mock_datetime.now.return_value = now
            await self.scanner.check_changing_rpms()
        self.assertEqual(self.scanner.changing_rpm_names, set())
