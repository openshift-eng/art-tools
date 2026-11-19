#!/usr/bin/env python3

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from artcommonlib.constants import KONFLUX_IMAGE_TEST_REPO
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from pyartcd.counter_models import BuildFailCounterContext
from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline, ocp4
from pyartcd.runtime import Runtime
from pyartcd.util import update_build_fail_counters

from pyartcd import util


class TestGlobalAutomationFreeze(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def make_pipeline(assembly='test', **kwargs):
        return KonfluxOcpPipeline(
            runtime=MagicMock(),
            version='4.22',
            assembly=assembly,
            image_build_strategy='only',
            rpm_build_strategy='none',
            build_priority='auto',
            **kwargs,
        )

    @patch('pyartcd.pipelines.ocp4_konflux.util.is_build_permitted', new_callable=AsyncMock, return_value=False)
    @patch('pyartcd.pipelines.ocp4_konflux.RegistryConfig')
    @patch.dict(os.environ, {'XDG_RUNTIME_DIR': '/tmp/keep-runtime-dir'}, clear=True)
    async def test_direct_run_skips_before_credentials_and_builds(self, registry_config, build_permitted):
        # freeze_automation forbids the run (e.g. a global 'yes' hard stop, or 'scheduled' on a
        # non-manual run): the build is skipped before any credentials or builds are touched.
        for assembly in ('stream', 'test', '4.22.1'):
            with self.subTest(assembly=assembly):
                pipeline = self.make_pipeline(assembly)
                pipeline._run_pipeline = AsyncMock()

                await pipeline.run()

                pipeline._run_pipeline.assert_not_awaited()
                registry_config.assert_not_called()
                self.assertEqual(os.environ['XDG_RUNTIME_DIR'], '/tmp/keep-runtime-dir')

    @patch('pyartcd.pipelines.ocp4_konflux.util.is_build_permitted', new_callable=AsyncMock, return_value=True)
    @patch.dict(os.environ, {}, clear=True)
    async def test_permitted_run_reaches_existing_credential_validation(self, build_permitted):
        pipeline = self.make_pipeline()
        with self.assertRaisesRegex(ValueError, 'QUAY_AUTH_FILE'):
            await pipeline.run()

    @patch('pyartcd.pipelines.ocp4_konflux.util.is_build_permitted', side_effect=TimeoutError)
    @patch('pyartcd.pipelines.ocp4_konflux.RegistryConfig')
    async def test_global_freeze_read_failure_stops_build(self, registry_config, build_permitted):
        # Fail closed: an error evaluating freeze_automation must stop the build, not permit it.
        pipeline = self.make_pipeline()
        pipeline._run_pipeline = AsyncMock()
        with self.assertRaises(TimeoutError):
            await pipeline.run()
        pipeline._run_pipeline.assert_not_awaited()
        registry_config.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.util.is_build_permitted', side_effect=TimeoutError)
    @patch('pyartcd.pipelines.ocp4_konflux.RegistryConfig')
    @patch.dict(os.environ, {'QCI_USER': 'test-user', 'QCI_PASSWORD': 'test-password'}, clear=True)
    async def test_explicit_override_runs_pipeline_without_evaluating_freeze(self, registry_config, build_permitted):
        with tempfile.TemporaryDirectory() as directory:
            auth_file = Path(directory) / 'auth.json'
            auth_file.write_text('{}')
            registry_config.return_value.__enter__.return_value = str(auth_file)
            pipeline = self.make_pipeline('stream', ignore_automation_freeze=True)
            pipeline._run_pipeline = AsyncMock()

            with (
                patch.dict(os.environ, {'QUAY_AUTH_FILE': str(auth_file), 'RHCOS_QUAY_AUTH_FILE': str(auth_file)}),
                self.assertLogs('pyartcd.pipelines.ocp4_konflux', level='WARNING') as logs,
            ):
                await pipeline.run()

        build_permitted.assert_not_awaited()
        pipeline._run_pipeline.assert_awaited_once()
        self.assertIn('--ignore-automation-freeze was explicitly set', '\n'.join(logs.output))

    @patch('pyartcd.pipelines.ocp4_konflux.uses_konflux_imagestream_override', return_value=True)
    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.update_description')
    @patch('pyartcd.pipelines.ocp4_konflux.jenkins.start_build_plashets')
    async def test_freeze_override_is_forwarded_to_plashet_job(self, start_plashets, _description, _override):
        for override in (False, True):
            with self.subTest(override=override):
                start_plashets.reset_mock()
                pipeline = self.make_pipeline('stream', ignore_automation_freeze=override)
                pipeline.runtime.dry_run = True
                for method in (
                    'initialize',
                    'rebase_and_build_rpms',
                    'rebase_and_build_images',
                    'sync_images',
                    'sweep_bugs',
                    'sweep_golang_bugs',
                    'mirror_streams_to_ci',
                    'mirror_test_builds_to_quay',
                    'clean_up',
                ):
                    setattr(pipeline, method, AsyncMock())
                pipeline.trigger_bundle_build = MagicMock()

                await pipeline._run_pipeline()

                start_plashets.assert_called_once()
                self.assertIs(start_plashets.call_args.kwargs['ignore_automation_freeze'], override)
                self.assertTrue(start_plashets.call_args.kwargs['block_until_complete'])
                pipeline.rebase_and_build_rpms.assert_awaited_once()
                pipeline.rebase_and_build_images.assert_awaited_once()

    @patch('pyartcd.util.is_manual_build')
    @patch('pyartcd.util.get_global_freeze_automation', new_callable=AsyncMock, return_value='scheduled')
    @patch.dict(os.environ, {}, clear=True)
    async def test_global_scheduled_is_manual_aware(self, global_freeze, manual_build):
        # A global 'scheduled' blocks a non-manual run entirely (skips before credential checks)...
        manual_build.return_value = False
        pipeline = self.make_pipeline('stream')
        pipeline._run_pipeline = AsyncMock()
        await pipeline.run()
        pipeline._run_pipeline.assert_not_awaited()

        # ...but permits a manual run, which then proceeds to the existing credential validation.
        manual_build.return_value = True
        pipeline = self.make_pipeline('stream')
        with self.assertRaisesRegex(ValueError, 'QUAY_AUTH_FILE'):
            await pipeline.run()


class TestAutomationFreezeOption(unittest.TestCase):
    def test_cli_override_preserves_build_lock(self):
        async def run_locked(coro, **kwargs):
            await coro

        args = [
            '--version',
            '4.21',
            '--assembly',
            'stream',
            '--image-build-strategy',
            'none',
            '--image-list',
            '',
            '--rpm-build-strategy',
            'only',
            '--rpm-list',
            'openshift',
            '--build-priority',
            'auto',
        ]
        for override in (False, True):
            with self.subTest(override=override):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    with (
                        patch('pyartcd.pipelines.ocp4_konflux.KonfluxOcpPipeline') as pipeline_class,
                        patch(
                            'pyartcd.pipelines.ocp4_konflux.jenkins.get_build_path_or_random', return_value='test-run'
                        ),
                        patch(
                            'pyartcd.pipelines.ocp4_konflux.locks.run_with_lock',
                            new_callable=AsyncMock,
                            side_effect=run_locked,
                        ) as run_with_lock,
                    ):
                        pipeline_class.return_value.run = AsyncMock()
                        result = CliRunner().invoke(
                            ocp4,
                            args + (['--ignore-automation-freeze'] if override else []),
                            obj=MagicMock(spec=Runtime),
                        )
                        self.assertEqual(result.exit_code, 0, result.output or str(result.exception))
                        self.assertIs(pipeline_class.call_args.kwargs['ignore_automation_freeze'], override)
                        run_with_lock.assert_awaited_once()
                        self.assertEqual(run_with_lock.call_args.kwargs['lock_name'], 'lock:build-konflux:4.21:stream')
                        pipeline_class.return_value.run.assert_awaited_once()
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)


class TestUpdateBuildFailCounters(unittest.IsolatedAsyncioTestCase):
    """Tests for the shared Konflux build counter utility used by OCP."""

    async def _update_build_fail_counters(
        self,
        assembly,
        group,
        built_images,
        failed_images,
        record_log,
        reset_counter,
        increment_counter,
    ):
        failed_entries = {
            entry["name"]: entry for entry in record_log.get("image_build_konflux", []) if int(entry["status"])
        }
        await update_build_fail_counters(
            context=BuildFailCounterContext(
                group=group,
                assembly=assembly,
                build_variant=BuildVariant.OCP,
                jenkins_url=os.getenv("BUILD_URL"),
                built_images=built_images,
                failed_images=failed_images,
                failed_entries=failed_entries,
                reset_counter=reset_counter,
                increment_counter=increment_counter,
            )
        )

    @patch.dict(os.environ, {"BUILD_URL": "https://jenkins.example.com/job/1"})
    async def test_infra_failure_still_resets_built_image_counters(self):
        """Infra failures do not prevent successful images from being reset."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()
        built_images = ["driver-toolkit", "base-images"]
        failed_images = ["enterprise-cluster-capacity"]
        record_log = {
            "image_build_konflux": [
                {
                    "name": "enterprise-cluster-capacity",
                    "status": "1",
                    "task_id": "n/a",
                    "task_url": "n/a",
                    "message": "infrastructure failure",
                    "outcome": "",
                    "nvrs": "",
                    "build_pipeline_url": "",
                }
            ]
        }

        await self._update_build_fail_counters(
            "stream",
            "openshift-4.21",
            built_images,
            failed_images,
            record_log,
            mock_reset,
            mock_increment,
        )

        self.assertEqual(mock_reset.call_count, len(built_images) * 3)
        reset_keys = {call.args[0] for call in mock_reset.call_args_list}
        expected_keys = {
            f"count:{counter_type}:konflux:openshift-4.21:{image}"
            for image in built_images
            for counter_type in ("build-failure", "ec-failure", "release-failure")
        }
        self.assertEqual(reset_keys, expected_keys)
        mock_increment.assert_not_called()

    async def test_non_stream_assembly_skips_all_counters(self):
        """Non-stream assemblies do not update counters."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()

        await self._update_build_fail_counters(
            "4.21.3",
            "openshift-4.21",
            ["some-image"],
            [],
            {},
            mock_reset,
            mock_increment,
        )

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()

    async def test_no_built_no_failed_images_noop(self):
        """Empty built and failed lists produce no counter operations."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()

        await self._update_build_fail_counters(
            "stream",
            "openshift-4.21",
            [],
            [],
            {},
            mock_reset,
            mock_increment,
        )

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()

    @patch.dict(os.environ, {"BUILD_URL": "https://jenkins.example.com/job/1"})
    async def test_build_failure_counter_stores_ocp_variant(self):
        """OCP build failures include the OCP build variant metadata."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()
        record_log = {
            "image_build_konflux": [
                {
                    "name": "ironic",
                    "status": "1",
                    "task_id": "plr-1",
                    "task_url": "https://konflux.example.com/plr-1",
                    "outcome": "build_error",
                    "nvrs": "ironic-1.0-1",
                    "build_pipeline_url": "https://konflux.example.com/plr-1",
                }
            ]
        }

        await self._update_build_fail_counters(
            "stream",
            "openshift-4.21",
            [],
            ["ironic"],
            record_log,
            mock_reset,
            mock_increment,
        )

        self.assertEqual(mock_increment.call_args.kwargs["build_variant"], "ocp")


class TestDryRunSideEffects(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

    def make_pipeline(self, dry_run, assembly='stream'):
        runtime = MagicMock(dry_run=dry_run, doozer_working=self.temp_dir.name)
        pipeline = KonfluxOcpPipeline(
            runtime=runtime,
            assembly=assembly,
            version='5.1',
            data_path='https://github.com/example/ocp-build-data',
            image_build_strategy='only',
            image_list='example',
            rpm_build_strategy='none',
            build_priority='auto',
        )
        pipeline.group_images = ['example']
        pipeline.build_plan.images_included = ['example']
        pipeline._registry_auth_file = '/tmp/registry-auth.json'
        return pipeline

    async def test_rebase_dry_run_does_not_reset_or_increment_failure_counters(self):
        for dry_run in (True, False):
            for failed in (False, True):
                with self.subTest(dry_run=dry_run, failed=failed):
                    pipeline = self.make_pipeline(dry_run)
                    Path(self.temp_dir.name, 'state.yaml').write_text(
                        'images:konflux:rebase:\n'
                        f'  failed-images: {"[example]" if failed else "[]"}\n'
                        '  images:\n'
                        f'    example:\n      status: {"failed" if failed else "success"}\n'
                    )
                    with (
                        patch(
                            'pyartcd.pipelines.ocp4_konflux.exectools.cmd_assert_async',
                            new_callable=AsyncMock,
                            side_effect=ChildProcessError('rebase failed') if failed else None,
                        ) as command,
                        patch('pyartcd.pipelines.ocp4_konflux.reset_fail_counter', new_callable=AsyncMock) as reset,
                        patch(
                            'pyartcd.pipelines.ocp4_konflux.increment_fail_counter', new_callable=AsyncMock
                        ) as increment,
                    ):
                        await pipeline.rebase_images('v5.1.0', 'test')

                    self.assertEqual('--push' in command.await_args.args[0], not dry_run)
                    self.assertEqual(reset.await_count, int(not dry_run and not failed))
                    self.assertEqual(increment.await_count, int(not dry_run and failed))
                    if failed:
                        self.assertEqual(pipeline.rebase_failures, ['example'])

    async def test_ci_mirroring_requires_live_stream_build(self):
        for dry_run in (True, False):
            for assembly in ('stream', 'test', 'ec.1'):
                with self.subTest(dry_run=dry_run, assembly=assembly):
                    pipeline = self.make_pipeline(dry_run, assembly)
                    pipeline.parse_record_log = MagicMock(
                        return_value={'image_build_konflux': [{'name': 'ose-openshift-apiserver', 'status': '0'}]}
                    )
                    with patch(
                        'pyartcd.pipelines.ocp4_konflux.exectools.cmd_assert_async', new_callable=AsyncMock
                    ) as cmd:
                        await pipeline.mirror_streams_to_ci()

                    should_mirror = not dry_run and assembly == 'stream'
                    self.assertEqual(cmd.await_count, int(should_mirror))
                    if should_mirror:
                        self.assertEqual(
                            cmd.await_args.args[0][-4:],
                            ['images:streams', 'mirror', '--registry-auth', '/tmp/registry-auth.json'],
                        )
                    else:
                        pipeline.parse_record_log.assert_not_called()

    async def test_simulated_operator_success_does_not_launch_bundle_job(self):
        for dry_run in (True, False):
            with self.subTest(dry_run=dry_run):
                pipeline = self.make_pipeline(dry_run, assembly='test')
                pipeline.parse_record_log = MagicMock(
                    return_value={
                        'image_build_konflux': [
                            {'has_olm_bundle': '1', 'status': '0', 'nvrs': 'example-operator-v5.1.0-test'}
                        ]
                    }
                )
                with patch('pyartcd.jenkins.start_build') as submit:
                    pipeline.trigger_bundle_build()

                self.assertEqual(submit.call_count, int(not dry_run))
                if not dry_run:
                    self.assertEqual(submit.call_args.kwargs['params']['OPERATOR_NVRS'], 'example-operator-v5.1.0-test')

    async def test_branch_protection_notification_requires_live_stream_build(self):
        Path(self.temp_dir.name, 'record.log').touch()
        for dry_run in (True, False):
            for assembly in ('stream', 'test'):
                with self.subTest(dry_run=dry_run, assembly=assembly):
                    pipeline = self.make_pipeline(dry_run, assembly)
                    pipeline.rebase_images = AsyncMock()
                    pipeline.build_images = AsyncMock()
                    with patch(
                        'pyartcd.pipelines.ocp4_konflux.util.notify_branch_protection_missing', new_callable=AsyncMock
                    ) as notify:
                        await pipeline.rebase_and_build_images()

                    self.assertEqual(notify.await_count, int(not dry_run and assembly == 'stream'))
                    self.assertEqual(pipeline.runtime.new_mail_client.call_count, notify.await_count)
                    pipeline.rebase_images.assert_awaited_once()
                    pipeline.build_images.assert_awaited_once()


class TestMirrorTestBuildsToQuay(unittest.IsolatedAsyncioTestCase):
    """Tests for the test-build auto-sync hook (mirror_test_builds_to_quay)."""

    def make_pipeline(self, assembly='test', dry_run=False):
        runtime = MagicMock(dry_run=dry_run)
        with patch('pyartcd.pipelines.ocp4_konflux.util.default_release_suffix', return_value='202408190000'):
            pipeline = KonfluxOcpPipeline(
                runtime=runtime,
                assembly=assembly,
                version='4.18',
                data_path='https://github.com/example/ocp-build-data',
                image_build_strategy='all',
                rpm_build_strategy='none',
                build_priority='auto',
            )
        return pipeline

    async def test_noop_when_not_test_assembly(self):
        pipeline = self.make_pipeline(assembly='stream')
        pipeline.building_images = MagicMock(return_value=True)
        pipeline.parse_record_log = MagicMock()
        with patch('pyartcd.pipelines.ocp4_konflux.sync_image_to_quay', new_callable=AsyncMock) as mock_sync:
            await pipeline.mirror_test_builds_to_quay()
        mock_sync.assert_not_awaited()
        pipeline.parse_record_log.assert_not_called()

    async def test_syncs_good_builds_and_skips_rhcos_failed_embargoed(self):
        pipeline = self.make_pipeline(assembly='test')
        pipeline.building_images = MagicMock(return_value=True)
        pipeline.parse_record_log = MagicMock(
            return_value={
                'image_build_konflux': [
                    {
                        'name': 'ose-good',
                        'status': '0',
                        'image_pullspec': 'quay.io/x/art-images@sha256:aaa',
                        'image_tag': 'good-tag',
                        'nvrs': 'ose-good-4.18.0-1.p0',
                    },
                    {
                        'name': 'ose-embargoed',
                        'status': '0',
                        'image_pullspec': 'quay.io/x/art-images@sha256:bbb',
                        'image_tag': 'embargoed-tag',
                        'nvrs': 'ose-embargoed-4.18.0-1.p1',
                    },
                    {
                        'name': 'rhcos-node-image',
                        'status': '0',
                        'image_pullspec': 'quay.io/x/art-images@sha256:ccc',
                        'image_tag': 'rhcos-tag',
                        'nvrs': 'rhcos-node-image-4.18.0-1.p0',
                    },
                    {
                        'name': 'ose-failed',
                        'status': '1',
                        'image_pullspec': 'quay.io/x/art-images@sha256:ddd',
                        'image_tag': 'failed-tag',
                        'nvrs': 'ose-failed-4.18.0-1.p0',
                    },
                ]
            }
        )

        def fake_embargo(nvr):
            return '.p1' in nvr

        with (
            patch('pyartcd.pipelines.ocp4_konflux.sync_image_to_quay', new_callable=AsyncMock) as mock_sync,
            patch('pyartcd.pipelines.ocp4_konflux.is_nvr_embargoed', side_effect=fake_embargo),
        ):
            await pipeline.mirror_test_builds_to_quay()

        # Only the single good, non-RHCOS, non-embargoed, successful build should sync, and only
        # the build-specific image_tag is applied (no floating "<name>-<X.Y>" tag).
        mock_sync.assert_awaited_once_with(
            'quay.io/x/art-images@sha256:aaa',
            KONFLUX_IMAGE_TEST_REPO,
            ['good-tag'],
            dry_run=False,
            logger=ANY,
        )

    async def test_one_sync_failure_still_attempts_others_and_raises(self):
        pipeline = self.make_pipeline(assembly='test')
        pipeline.building_images = MagicMock(return_value=True)
        pipeline.parse_record_log = MagicMock(
            return_value={
                'image_build_konflux': [
                    {
                        'name': 'ose-one',
                        'status': '0',
                        'image_pullspec': 'quay.io/x/art-images@sha256:aaa',
                        'image_tag': 'one-tag',
                        'nvrs': 'ose-one-4.18.0-1.p0',
                    },
                    {
                        'name': 'ose-two',
                        'status': '0',
                        'image_pullspec': 'quay.io/x/art-images@sha256:bbb',
                        'image_tag': 'two-tag',
                        'nvrs': 'ose-two-4.18.0-1.p0',
                    },
                ]
            }
        )

        async def fake_sync(image_pullspec, *args, **kwargs):
            if image_pullspec == 'quay.io/x/art-images@sha256:aaa':
                raise RuntimeError('mirror failed')

        with (
            patch(
                'pyartcd.pipelines.ocp4_konflux.sync_image_to_quay',
                new_callable=AsyncMock,
                side_effect=fake_sync,
            ) as mock_sync,
            patch('pyartcd.pipelines.ocp4_konflux.is_nvr_embargoed', return_value=False),
        ):
            with self.assertRaises(RuntimeError):
                await pipeline.mirror_test_builds_to_quay()

        # Both eligible builds were attempted even though the first one failed.
        self.assertEqual(mock_sync.await_count, 2)
        synced_pullspecs = {call.args[0] for call in mock_sync.await_args_list}
        self.assertEqual(
            synced_pullspecs,
            {'quay.io/x/art-images@sha256:aaa', 'quay.io/x/art-images@sha256:bbb'},
        )

    async def test_no_images_building_is_noop(self):
        pipeline = self.make_pipeline(assembly='test')
        pipeline.building_images = MagicMock(return_value=False)
        pipeline.parse_record_log = MagicMock()
        with patch('pyartcd.pipelines.ocp4_konflux.sync_image_to_quay', new_callable=AsyncMock) as mock_sync:
            await pipeline.mirror_test_builds_to_quay()
        mock_sync.assert_not_awaited()
        pipeline.parse_record_log.assert_not_called()


class TestKonfluxOcpPipelineOwnerFailureEmails(unittest.IsolatedAsyncioTestCase):
    """Tests for _handle_image_build_failures owner emails (ART-21640)."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.runtime = MagicMock()
        self.runtime.dry_run = False
        self.runtime.doozer_working = str(Path(self.tmpdir) / 'doozer_working')
        Path(self.runtime.doozer_working).mkdir(parents=True)
        self.mail_client = MagicMock()
        self.runtime.new_mail_client.return_value = self.mail_client

    def _make_pipeline(self, assembly='stream', **kwargs):
        defaults = {
            'runtime': self.runtime,
            'assembly': assembly,
            'version': '4.18',
            'data_path': 'test-path',
            'image_build_strategy': 'all',
            'image_list': '',
            'rpm_build_strategy': 'none',
            'rpm_list': '',
            'data_gitref': '',
            'kubeconfig': None,
            'skip_rebase': False,
            'skip_bundle_build': False,
            'arches': (),
            'plr_template': '',
            'lock_identifier': 'test',
            'skip_plashets': False,
            'build_priority': 'auto',
            'use_mass_rebuild_locks': False,
            'network_mode': None,
            'mail_list_failure': 'aos-art-automation+failed-ocp4-konflux-build@redhat.com',
        }
        defaults.update(kwargs)
        return KonfluxOcpPipeline(**defaults)

    def _build_failure_entry(self, name, outcome='failure', **extra):
        entry = {
            'name': name,
            'status': '-1',
            'nvrs': f'{name}-1.0-1',
            'outcome': outcome,
            'owners': 'owner@redhat.com',
            'message': 'build failed',
            'task_id': 'plr-1',
            'task_url': f'http://build/plr/{name}',
            'build_pipeline_url': f'http://build/plr/{name}',
            'ec_pipeline_url': '',
            'release_pipeline': '',
        }
        entry.update(extra)
        return entry

    def test_categorize_separates_failure_types(self):
        record_log = {
            'image_build_konflux': [
                self._build_failure_entry('ironic', outcome='failure'),
                self._build_failure_entry('ceo', outcome='its_error'),
                self._build_failure_entry('base', outcome='release_error'),
                self._build_failure_entry(
                    'child',
                    message='parent images failed to build: base',
                    task_id='n/a',
                ),
            ]
        }
        failed_entries = {entry['name']: entry for entry in record_log['image_build_konflux'] if int(entry['status'])}
        counter_failed_images = util.get_failed_images_for_counter_updates(
            ['ironic', 'ceo', 'base', 'child'], failed_entries
        )
        categories = util.categorize_failed_images(counter_failed_images, failed_entries)

        self.assertEqual(categories.build, ['ironic'])
        self.assertEqual(categories.its, ['ceo'])
        self.assertEqual(categories.release, ['base'])

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    @patch('pyartcd.pipelines.ocp4_konflux.util.get_counter_failures', new_callable=AsyncMock)
    async def test_handle_emails_build_failures_on_stream(self, mock_counters, mock_mail):
        mock_counters.return_value = {'ironic': {'failure_count': 5}}
        pipeline = self._make_pipeline()
        record_log = {
            'image_build_konflux': [
                self._build_failure_entry('ironic', outcome='failure'),
                {
                    'name': 'ok-image',
                    'status': '0',
                    'nvrs': 'ok-1.0-1',
                    'outcome': 'success',
                    'message': '',
                    'task_id': 'plr-ok',
                },
            ]
        }
        await pipeline._handle_image_build_failures(record_log)

        mock_mail.assert_called_once()
        failed_builds = mock_mail.call_args.kwargs['failed_builds']
        self.assertEqual(list(failed_builds.keys()), ['ironic'])
        self.assertEqual(mock_mail.call_args.kwargs['default_owner'], pipeline.mail_list_failure)
        self.assertEqual(mock_mail.call_args.kwargs['failure_counts'], {'ironic': 5})

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    @patch('pyartcd.pipelines.ocp4_konflux.util.get_counter_failures', new_callable=AsyncMock)
    async def test_handle_skips_first_time_failures(self, mock_counters, mock_mail):
        # Counter was just incremented to 1 for this run only; not yet a repeat failure.
        mock_counters.return_value = {'ironic': {'failure_count': 1}}
        pipeline = self._make_pipeline()
        record_log = {'image_build_konflux': [self._build_failure_entry('ironic', outcome='failure')]}
        await pipeline._handle_image_build_failures(record_log)
        mock_mail.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    @patch('pyartcd.pipelines.ocp4_konflux.util.get_counter_failures', new_callable=AsyncMock)
    async def test_handle_skips_below_repeat_threshold(self, mock_counters, mock_mail):
        # One short of the 5-consecutive-failure threshold.
        mock_counters.return_value = {'ironic': {'failure_count': 4}}
        pipeline = self._make_pipeline()
        record_log = {'image_build_konflux': [self._build_failure_entry('ironic', outcome='failure')]}
        await pipeline._handle_image_build_failures(record_log)
        mock_mail.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    async def test_handle_skips_non_stream(self, mock_mail):
        pipeline = self._make_pipeline(assembly='4.18.1')
        record_log = {'image_build_konflux': [self._build_failure_entry('ironic')]}
        await pipeline._handle_image_build_failures(record_log)
        mock_mail.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    async def test_handle_skips_ec_and_release_failures(self, mock_mail):
        pipeline = self._make_pipeline()
        record_log = {
            'image_build_konflux': [
                self._build_failure_entry('ceo', outcome='its_error'),
                self._build_failure_entry('base', outcome='release_error'),
            ]
        }
        await pipeline._handle_image_build_failures(record_log)
        mock_mail.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    async def test_handle_skips_high_failure_ratio(self, mock_mail):
        pipeline = self._make_pipeline()
        # 4 failures out of 12 (> 25% and total > 10) → do not spam
        entries = [self._build_failure_entry(f'img-{i}') for i in range(4)]
        entries += [
            {
                'name': f'ok-{i}',
                'status': '0',
                'nvrs': f'ok-{i}-1.0-1',
                'outcome': 'success',
                'message': '',
                'task_id': f'plr-ok-{i}',
            }
            for i in range(8)
        ]
        record_log = {'image_build_konflux': entries}
        await pipeline._handle_image_build_failures(record_log)
        mock_mail.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    @patch('pyartcd.pipelines.ocp4_konflux.util.get_counter_failures', new_callable=AsyncMock)
    async def test_handle_emails_when_ratio_below_threshold(self, mock_counters, mock_mail):
        mock_counters.return_value = {'img-0': {'failure_count': 5}, 'img-1': {'failure_count': 6}}
        pipeline = self._make_pipeline()
        # 2 failures out of 12 (< 25%) → spam owners
        entries = [self._build_failure_entry(f'img-{i}') for i in range(2)]
        entries += [
            {
                'name': f'ok-{i}',
                'status': '0',
                'nvrs': f'ok-{i}-1.0-1',
                'outcome': 'success',
                'message': '',
                'task_id': f'plr-ok-{i}',
            }
            for i in range(10)
        ]
        record_log = {'image_build_konflux': entries}
        await pipeline._handle_image_build_failures(record_log)
        mock_mail.assert_called_once()
        self.assertEqual(sorted(mock_mail.call_args.kwargs['failed_builds'].keys()), ['img-0', 'img-1'])

    @patch('pyartcd.pipelines.ocp4_konflux.util.mail_build_failure_owners_konflux')
    @patch('pyartcd.pipelines.ocp4_konflux.util.get_counter_failures', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.update_build_fail_counters', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.KonfluxOcpPipeline.parse_record_log')
    @patch('pyartcd.pipelines.ocp4_konflux.jenkins')
    async def test_sync_images_calls_handle_failures(
        self, mock_jenkins, mock_parse, mock_counters_update, mock_counters, mock_mail
    ):
        mock_counters.return_value = {'ironic': {'failure_count': 5}}
        pipeline = self._make_pipeline()
        record_log = {
            'image_build_konflux': [
                self._build_failure_entry('ironic'),
                {
                    'name': 'ok-image',
                    'status': '0',
                    'nvrs': 'ok-1.0-1',
                    'outcome': 'success',
                    'message': '',
                    'task_id': 'plr-ok',
                },
            ]
        }
        mock_parse.return_value = record_log

        with patch.object(pipeline, 'building_images', return_value=True):
            with patch('pyartcd.pipelines.ocp4_konflux.jenkins.start_build_sync'):
                await pipeline.sync_images()

        mock_counters_update.assert_awaited_once()
        mock_mail.assert_called_once()


if __name__ == "__main__":
    unittest.main()
