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


class TestMirrorImages(unittest.IsolatedAsyncioTestCase):
    """
    Tests for KonfluxOcpPipeline.mirror_images().

    Focus: group-component and NVR tags are included alongside image_tag
    and latest_tag when syncing to art-images-share (ART-23164).
    """

    def _make_pipeline(self, assembly='stream', version='4.21'):
        runtime = MagicMock()
        runtime.doozer_working = '/tmp/doozer-working'
        runtime.dry_run = False
        runtime.new_slack_client.return_value = MagicMock()
        with patch('pyartcd.pipelines.ocp4_konflux.util.default_release_suffix', return_value='202408190000'):
            pipeline = KonfluxOcpPipeline(
                runtime=runtime,
                assembly=assembly,
                version=version,
                image_build_strategy='all',
                rpm_build_strategy='none',
                build_priority='auto',
                data_path='https://github.com/openshift-eng/ocp-build-data',
            )
        return pipeline

    def _build_entry(
        self,
        name='test-image',
        delivery_repo_name=None,
        status='0',
        nvrs='test-image-v4.21.0-202408190000.p0.gabcdef.assembly.stream.el9',
        image_tag='sha256-abc123',
        image_pullspec='quay.io/src/image@sha256:abc123',
    ):
        entry = {
            'name': name,
            'status': status,
            'task_id': '12345',
            'task_url': 'https://example.com',
            'message': '',
            'outcome': '',
            'nvrs': nvrs,
            'build_pipeline_url': '',
            'image_tag': image_tag,
            'image_pullspec': image_pullspec,
            'has_olm_bundle': '0',
        }
        if delivery_repo_name is not None:
            entry['delivery_repo_name'] = delivery_repo_name
        return entry

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_group_component_tag_uses_delivery_repo_name(self, mock_embargoed, mock_sync):
        """When delivery_repo_name is present, the group-component tag uses it."""
        pipeline = self._make_pipeline(version='4.17')
        build = self._build_entry(name='ansible-operator', delivery_repo_name='ose-ansible-operator')
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_called_once()
        tags = mock_sync.call_args[0][2]
        self.assertIn('openshift-4.17-ose-ansible-operator', tags)
        # The tag must NOT use the plain name when delivery_repo_name is set
        self.assertNotIn('openshift-4.17-ansible-operator', tags)

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_group_component_tag_falls_back_to_name(self, mock_embargoed, mock_sync):
        """When delivery_repo_name is absent, the group-component tag falls back to build name."""
        pipeline = self._make_pipeline(version='4.17')
        # No delivery_repo_name key in build entry
        build = self._build_entry(name='ose-ansible-operator')
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_called_once()
        tags = mock_sync.call_args[0][2]
        self.assertIn('openshift-4.17-ose-ansible-operator', tags)

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_single_nvr_included_as_tag(self, mock_embargoed, mock_sync):
        """A single NVR value is included as an additional tag."""
        pipeline = self._make_pipeline()
        nvr = 'test-image-v4.21.0-202408190000.p0.gabcdef.assembly.stream.el9'
        build = self._build_entry(nvrs=nvr)
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_called_once()
        tags = mock_sync.call_args[0][2]
        self.assertIn(nvr, tags)
        self.assertIn(build['image_tag'], tags)
        self.assertIn(f'{build["name"]}-4.21', tags)

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_multiple_comma_separated_nvrs(self, mock_embargoed, mock_sync):
        """Comma-separated NVRs are each added as separate tags."""
        pipeline = self._make_pipeline()
        nvr1 = 'test-image-v4.21.0-202408190000.p0.gabcdef.assembly.stream.el9'
        nvr2 = 'test-image-v4.21.0-202408190000.p0.gabcdef.assembly.stream.el8'
        build = self._build_entry(nvrs=f'{nvr1},{nvr2}')
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_called_once()
        tags = mock_sync.call_args[0][2]
        self.assertIn(nvr1, tags)
        self.assertIn(nvr2, tags)
        # image_tag, latest_tag, group_component_tag, nvr1, nvr2
        self.assertEqual(len(tags), 5)

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_empty_nvrs_no_extra_tags(self, mock_embargoed, mock_sync):
        """Empty nvrs string does not add empty-string tags."""
        pipeline = self._make_pipeline()
        build = self._build_entry(nvrs='')
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_called_once()
        tags = mock_sync.call_args[0][2]
        # image_tag, latest_tag, group_component_tag (no NVR tags)
        self.assertEqual(len(tags), 3)

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_nvrs_with_whitespace_trimmed(self, mock_embargoed, mock_sync):
        """Whitespace around comma-separated NVRs is stripped."""
        pipeline = self._make_pipeline()
        nvr1 = 'test-image-v4.21.0-202408190000.el9'
        nvr2 = 'test-image-v4.21.0-202408190000.el8'
        build = self._build_entry(nvrs=f' {nvr1} , {nvr2} ')
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_called_once()
        tags = mock_sync.call_args[0][2]
        self.assertIn(nvr1, tags)
        self.assertIn(nvr2, tags)
        for tag in tags:
            self.assertEqual(tag, tag.strip())
            self.assertTrue(len(tag) > 0)

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_group_component_tag_format(self, mock_embargoed, mock_sync):
        """The group-component tag follows openshift-<version>-<delivery_name> format."""
        pipeline = self._make_pipeline(version='4.17')
        build = self._build_entry(name='ansible-operator', delivery_repo_name='ose-ansible-operator')
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_called_once()
        tags = mock_sync.call_args[0][2]
        self.assertIn('openshift-4.17-ose-ansible-operator', tags)

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=False)
    async def test_non_stream_assembly_skips_sync(self, mock_embargoed, mock_sync):
        """Non-stream assemblies skip syncing entirely."""
        pipeline = self._make_pipeline(assembly='4.21.3')
        build = self._build_entry()
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_not_called()

    @patch('pyartcd.pipelines.ocp4_konflux.sync_to_quay', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.is_release_embargoed', return_value=True)
    async def test_embargoed_build_skips_sync(self, mock_embargoed, mock_sync):
        """Embargoed builds are not synced."""
        pipeline = self._make_pipeline()
        build = self._build_entry()
        pipeline.parse_record_log = MagicMock(return_value={'image_build_konflux': [build]})
        pipeline.building_images = MagicMock(return_value=True)

        await pipeline.mirror_images()

        mock_sync.assert_not_called()


if __name__ == '__main__':
    unittest.main()
