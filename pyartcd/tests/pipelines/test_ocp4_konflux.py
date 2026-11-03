#!/usr/bin/env python3

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.variants import BuildVariant
from pyartcd.counter_models import BuildFailCounterContext
from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline
from pyartcd.util import update_build_fail_counters


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


ACTIVE_GOLANG_VERSIONS = {
    '4.19': {'1.23', '1.24'},
    '4.20': {'1.24', '1.25'},
    '4.21': {'1.24', '1.25'},
    '4.22': {'1.25', '1.26'},
    '4.23': {'1.26'},
    '5.0': {'1.26'},
}


class TestSweepGolangBugs(unittest.IsolatedAsyncioTestCase):
    def make_pipeline(self, version='4.22', assembly='stream', dry_run=False):
        runtime = MagicMock(dry_run=dry_run, doozer_working='/tmp/doozer-working')
        pipeline = KonfluxOcpPipeline(
            runtime=runtime,
            assembly=assembly,
            version=version,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            image_build_strategy='all',
            rpm_build_strategy='none',
            build_priority='auto',
        )
        pipeline._registry_auth_file = '/tmp/registry-auth.json'
        pipeline.slack_client = MagicMock()
        pipeline.slack_client.say = AsyncMock()
        return pipeline

    async def sweep(self, pipeline, active=ACTIVE_GOLANG_VERSIONS, side_effect=None):
        discovery = (
            MagicMock(return_value=active) if not isinstance(active, Exception) else MagicMock(side_effect=active)
        )
        with (
            patch('pyartcd.pipelines.ocp4_konflux.get_active_ocp_golang_major_minors', discovery),
            patch('pyartcd.pipelines.ocp4_konflux.exectools.cmd_assert_async', new_callable=AsyncMock) as cmd,
        ):
            cmd.side_effect = side_effect
            await pipeline.sweep_golang_bugs()
        groups = [call.args[0][3] for call in cmd.await_args_list]
        return cmd, groups

    async def test_two_golangs_sweep_every_version_sharing_either(self):
        pipeline = self.make_pipeline(version='4.22')
        cmd, groups = await self.sweep(pipeline)
        self.assertEqual(
            groups,
            [
                '--group=openshift-4.20',
                '--group=openshift-4.21',
                '--group=openshift-4.22',
                '--group=openshift-4.23',
                '--group=openshift-5.0',
            ],
        )
        self.assertEqual(
            cmd.await_args_list[0].args[0],
            [
                'elliott',
                '--assembly',
                'stream',
                '--group=openshift-4.20',
                '--registry-config=/tmp/registry-auth.json',
                'find-bugs:golang',
                '--analyze',
                '--update-tracker',
            ],
        )
        pipeline.slack_client.say.assert_not_awaited()

    async def test_non_stream_assembly_is_skipped(self):
        pipeline = self.make_pipeline(assembly='test')
        cmd, _ = await self.sweep(pipeline)
        cmd.assert_not_awaited()

    async def test_discovery_failure_sweeps_own_version_only(self):
        pipeline = self.make_pipeline(version='4.22')
        _, groups = await self.sweep(pipeline, active=RuntimeError('GitHub API down'))
        self.assertEqual(groups, ['--group=openshift-4.22'])

    async def test_version_without_go_vars_sweeps_own_version_only(self):
        pipeline = self.make_pipeline(version='4.18')
        _, groups = await self.sweep(pipeline)
        self.assertEqual(groups, ['--group=openshift-4.18'])

    async def test_sibling_failures_do_not_stop_the_sweep_and_are_only_logged(self):
        pipeline = self.make_pipeline(version='4.23')
        with patch.object(pipeline.runtime, 'logger') as mock_logger:
            cmd, groups = await self.sweep(
                pipeline, side_effect=[ChildProcessError('boom'), None, ChildProcessError('boom')]
            )
            mock_logger.warning.assert_any_call('Golang bug sweep failed for sibling OCP version(s): %s', '4.22, 5.0')
        self.assertEqual(groups, ['--group=openshift-4.22', '--group=openshift-4.23', '--group=openshift-5.0'])
        pipeline.slack_client.bind_channel.assert_not_called()
        pipeline.slack_client.say.assert_not_awaited()

    async def test_own_version_failure_sends_slack_even_when_siblings_succeed(self):
        pipeline = self.make_pipeline(version='4.23')
        cmd, groups = await self.sweep(pipeline, side_effect=[None, ChildProcessError('boom'), None])
        self.assertEqual(groups, ['--group=openshift-4.22', '--group=openshift-4.23', '--group=openshift-5.0'])
        pipeline.slack_client.bind_channel.assert_called_once_with('openshift-4.23')
        pipeline.slack_client.say.assert_awaited_once_with('Golang bug sweep failed for 4.23. Please investigate')

    async def test_slack_failure_does_not_raise(self):
        pipeline = self.make_pipeline(version='5.0')
        pipeline.slack_client.say.side_effect = RuntimeError('slack down')
        await self.sweep(pipeline, side_effect=ChildProcessError('boom'))
        pipeline.slack_client.say.assert_awaited_once()

    async def test_dry_run_passes_flag_and_skips_slack(self):
        pipeline = self.make_pipeline(version='5.0', dry_run=True)
        cmd, _ = await self.sweep(pipeline, side_effect=ChildProcessError('boom'))
        self.assertEqual(cmd.await_count, 3)
        for call in cmd.await_args_list:
            self.assertEqual(call.args[0][-1], '--dry-run')
        pipeline.slack_client.say.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
