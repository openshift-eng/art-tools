#!/usr/bin/env python3

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline


class TestUpdateBuildFailCounters(unittest.IsolatedAsyncioTestCase):
    """
    Tests for KonfluxOcpPipeline.update_build_fail_counters().

    Focus: the early-return path (all failures are infra/parent-dep) must still
    reset counters for successfully built images before returning.
    """

    def _make_pipeline(self, assembly='stream', version='4.21'):
        runtime = MagicMock()
        runtime.doozer_working = '/tmp/doozer-working'
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

    def _infra_failure_record_log(self, failed_image: str):
        """Record log where the sole failure has task_id=n/a (infra failure)."""
        return {
            'image_build_konflux': [
                {
                    'name': failed_image,
                    'status': '1',
                    'task_id': 'n/a',
                    'task_url': 'n/a',
                    'message': 'infrastructure failure',
                    'outcome': '',
                    'nvrs': '',
                    'build_pipeline_url': '',
                }
            ]
        }

    @patch.dict(os.environ, {'BUILD_URL': 'https://jenkins.example.com/job/1'})
    @patch('pyartcd.pipelines.ocp4_konflux.reset_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.increment_fail_counter', new_callable=AsyncMock)
    async def test_infra_failure_still_resets_built_image_counters(self, mock_increment, mock_reset):
        """
        When all failures are infra (task_id=n/a), the early return must NOT skip
        resetting counters for successfully built images.

        Regression test for ART-22800.
        """
        pipeline = self._make_pipeline()

        built_images = ['driver-toolkit', 'base-images']
        failed_images = ['enterprise-cluster-capacity']
        record_log = self._infra_failure_record_log('enterprise-cluster-capacity')

        await pipeline.update_build_fail_counters(built_images, failed_images, record_log)

        # reset_fail_counter must be called for each built image × 3 counter types
        self.assertEqual(mock_reset.call_count, len(built_images) * 3)
        reset_keys = {call.args[0] for call in mock_reset.call_args_list}
        expected_keys = {
            f'count:{ct}:konflux:openshift-4.21:{img}'
            for img in built_images
            for ct in ('build-failure', 'ec-failure', 'release-failure')
        }
        self.assertEqual(reset_keys, expected_keys)

        # increment_fail_counter must NOT be called (infra failure, no real build attempted)
        mock_increment.assert_not_called()

    @patch.dict(os.environ, {'BUILD_URL': 'https://jenkins.example.com/job/1'})
    @patch('pyartcd.pipelines.ocp4_konflux.reset_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.increment_fail_counter', new_callable=AsyncMock)
    async def test_non_stream_assembly_skips_all_counters(self, mock_increment, mock_reset):
        """Non-stream assemblies must return immediately without touching any counters."""
        pipeline = self._make_pipeline(assembly='4.21.3')

        await pipeline.update_build_fail_counters(['some-image'], [], {})

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()

    @patch.dict(os.environ, {'BUILD_URL': 'https://jenkins.example.com/job/1'})
    @patch('pyartcd.pipelines.ocp4_konflux.reset_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.increment_fail_counter', new_callable=AsyncMock)
    async def test_no_built_no_failed_images_noop(self, mock_increment, mock_reset):
        """Empty built and failed lists produce no counter operations."""
        pipeline = self._make_pipeline()

        await pipeline.update_build_fail_counters([], [], {})

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()


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
