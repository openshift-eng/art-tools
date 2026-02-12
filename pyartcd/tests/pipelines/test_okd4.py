#!/usr/bin/env python3

"""
Unit tests for the okd pipeline.
"""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.ocp4_konflux import BuildStrategy
from pyartcd.pipelines.okd import BuildPlan, KonfluxOkdPipeline


class TestKonfluxOkdPipeline(IsolatedAsyncioTestCase):
    def setUp(self):
        """
        Set up common test fixtures.
        """

        self.mock_runtime = MagicMock()
        self.mock_runtime.working_dir = MagicMock()
        self.mock_runtime.logger = MagicMock()
        self.mock_runtime.dry_run = False
        self.mock_runtime.doozer_working = '/tmp/doozer_working'

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()

        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

    async def test_mirror_coreos_imagestreams_success(self):
        """
        Test successful CoreOS imagestream mirroring.
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.20',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch('pyartcd.pipelines.okd.jenkins') as mock_jenkins,
        ):
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            # Should be called twice - once for stream-coreos, once for stream-coreos-extensions
            self.assertEqual(mock_tag.call_count, 2)

            # Check first call (stream-coreos)
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['source_pullspec'], 'origin/scos-4.20:stream-coreos')
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-4.20-art:stream-coreos')

            # Check second call (stream-coreos-extensions)
            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], 'origin/scos-4.20:stream-coreos-extensions')
            self.assertEqual(second_call[1]['target_tag'], 'origin/scos-4.20-art:stream-coreos-extensions')

            # Should update Jenkins description twice (once per successful tag)
            self.assertEqual(mock_jenkins.update_description.call_count, 2)

    async def test_mirror_coreos_imagestreams_skipped_for_non_stream_assembly(self):
        """
        Test that CoreOS mirroring is skipped for non-stream assemblies.
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='4.22.1',  # Not 'stream'
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.22',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag:
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            mock_tag.assert_not_called()

    async def test_mirror_coreos_imagestreams_dry_run(self):
        """
        Test that CoreOS mirroring is skipped in dry-run mode.
        """

        # given
        self.mock_runtime.dry_run = True
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.20',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag:
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            mock_tag.assert_not_called()

    async def test_mirror_coreos_imagestreams_handles_failure_gracefully(self):
        """
        Test that CoreOS mirroring failures don't crash the pipeline.
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.20',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch('pyartcd.pipelines.okd.jenkins') as mock_jenkins,
        ):
            # Simulate a failure for both tags
            mock_tag.side_effect = Exception('oc tag failed')

            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            # Should be called twice - once for each tag
            self.assertEqual(mock_tag.call_count, 2)

            # Verify errors were logged to Jenkins (twice, once for each tag)
            error_calls = [
                call
                for call in mock_jenkins.update_description.call_args_list
                if 'Failed to mirror CoreOS imagestream tag' in str(call)
            ]
            self.assertEqual(len(error_calls), 2)

    async def test_mirror_coreos_imagestreams_custom_namespace(self):
        """
        Test CoreOS mirroring with a custom imagestream namespace.
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.20',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='custom-namespace',
        )

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            # Should be called twice with custom namespace
            self.assertEqual(mock_tag.call_count, 2)

            # Check first call uses custom namespace
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['source_pullspec'], 'custom-namespace/scos-4.20:stream-coreos')
            self.assertEqual(first_call[1]['target_tag'], 'custom-namespace/scos-4.20-art:stream-coreos')

            # Check second call uses custom namespace
            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], 'custom-namespace/scos-4.20:stream-coreos-extensions')
            self.assertEqual(second_call[1]['target_tag'], 'custom-namespace/scos-4.20-art:stream-coreos-extensions')

    async def test_mirror_coreos_imagestreams_skipped_for_4_21(self):
        """
        Test that CoreOS mirroring is skipped for version 4.21 (handled by openshift/release).
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.21',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag:
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            mock_tag.assert_not_called()

    async def test_mirror_coreos_imagestreams_skipped_for_4_22(self):
        """
        Test that CoreOS mirroring is skipped for version 4.22 (handled by openshift/release).
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.22',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag:
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            mock_tag.assert_not_called()

    async def test_mirror_coreos_imagestreams_4_23_uses_4_22_source(self):
        """
        Test that CoreOS mirroring for version 4.23 uses 4.22 as the source.
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.23',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            # Should be called twice
            self.assertEqual(mock_tag.call_count, 2)

            # Check that 4.22 is used as source, 4.23 as target
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['source_pullspec'], 'origin/scos-4.22:stream-coreos')
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-4.23-art:stream-coreos')

            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], 'origin/scos-4.22:stream-coreos-extensions')
            self.assertEqual(second_call[1]['target_tag'], 'origin/scos-4.23-art:stream-coreos-extensions')

    async def test_mirror_coreos_imagestreams_5_0_uses_4_22_source(self):
        """
        Test that CoreOS mirroring for version 5.0 uses 4.22 as the source.
        """

        # given
        pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='5.0',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            # Should be called twice
            self.assertEqual(mock_tag.call_count, 2)

            # Check that 4.22 is used as source, 5.0 as target
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['source_pullspec'], 'origin/scos-4.22:stream-coreos')
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-5.0-art:stream-coreos')

            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], 'origin/scos-4.22:stream-coreos-extensions')
            self.assertEqual(second_call[1]['target_tag'], 'origin/scos-5.0-art:stream-coreos-extensions')


class TestGetPayloadTagName(IsolatedAsyncioTestCase):
    def setUp(self):
        """
        Set up common test fixtures.
        """

        self.mock_runtime = MagicMock()
        self.mock_runtime.working_dir = MagicMock()
        self.mock_runtime.logger = MagicMock()
        self.mock_runtime.dry_run = False
        self.mock_runtime.doozer_working = '/tmp/doozer_working'

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()

        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

        self.pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.22',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

    def test_get_payload_tag_name_with_okd_payload_name(self):
        """
        Test that okd.payload_name takes highest precedence.
        """

        # given
        image_metadata = {
            'okd': {'payload_name': 'custom-okd-name'},
            'payload_name': 'standard-name',
            'name': 'openshift/ose-foo',
        }

        # when
        result = self.pipeline._get_payload_tag_name('foo', image_metadata)

        # then
        self.assertEqual(result, 'custom-okd-name')

    def test_get_payload_tag_name_with_payload_name(self):
        """
        Test that payload_name is used when okd.payload_name is not present.
        """

        # given
        image_metadata = {'payload_name': 'standard-name', 'name': 'openshift/ose-foo'}

        # when
        result = self.pipeline._get_payload_tag_name('foo', image_metadata)

        # then
        self.assertEqual(result, 'standard-name')

    def test_get_payload_tag_name_strips_ose_prefix(self):
        """
        Test that ose- prefix is stripped from image name.
        """

        # given
        image_metadata = {'name': 'openshift/ose-cli'}

        # when
        result = self.pipeline._get_payload_tag_name('cli', image_metadata)

        # then
        self.assertEqual(result, 'cli')

    def test_get_payload_tag_name_keeps_non_ose_name(self):
        """
        Test that non-ose names are kept as-is.
        """

        # given
        image_metadata = {'name': 'openshift/installer'}

        # when
        result = self.pipeline._get_payload_tag_name('installer', image_metadata)

        # then
        self.assertEqual(result, 'installer')

    def test_get_payload_tag_name_uses_distgit_key_as_fallback(self):
        """
        Test that distgit key is used when name is not present.
        """

        # given
        image_metadata = {}

        # when
        result = self.pipeline._get_payload_tag_name('my-image', image_metadata)

        # then
        self.assertEqual(result, 'my-image')


class TestBuildingImages(IsolatedAsyncioTestCase):
    def setUp(self):
        """
        Set up common test fixtures.
        """

        self.mock_runtime = MagicMock()
        self.mock_runtime.working_dir = MagicMock()
        self.mock_runtime.logger = MagicMock()
        self.mock_runtime.dry_run = False
        self.mock_runtime.doozer_working = '/tmp/doozer_working'

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()

        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

        self.pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.22',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

    def test_building_images_with_strategy_none(self):
        """
        Test that building_images returns False for NONE strategy.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.NONE

        # when
        result = self.pipeline.building_images()

        # then
        self.assertFalse(result)

    def test_building_images_with_strategy_all(self):
        """
        Test that building_images returns True for ALL strategy.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ALL

        # when
        result = self.pipeline.building_images()

        # then
        self.assertTrue(result)

    def test_building_images_with_strategy_only_and_images(self):
        """
        Test that building_images returns True for ONLY strategy with images.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ONLY
        self.pipeline.build_plan.images_included = ['image1', 'image2']

        # when
        result = self.pipeline.building_images()

        # then
        self.assertTrue(result)

    def test_building_images_with_strategy_only_and_no_images(self):
        """
        Test that building_images returns False for ONLY strategy without images.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ONLY
        self.pipeline.build_plan.images_included = []

        # when
        result = self.pipeline.building_images()

        # then
        self.assertFalse(result)

    def test_building_images_with_strategy_except(self):
        """
        Test that building_images returns True for EXCEPT strategy.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.EXCEPT
        self.pipeline.build_plan.images_excluded = ['image1']

        # when
        result = self.pipeline.building_images()

        # then
        self.assertTrue(result)


class TestIncludeExcludeParam(IsolatedAsyncioTestCase):
    def setUp(self):
        """
        Set up common test fixtures.
        """

        self.mock_runtime = MagicMock()
        self.mock_runtime.working_dir = MagicMock()
        self.mock_runtime.logger = MagicMock()
        self.mock_runtime.dry_run = False
        self.mock_runtime.doozer_working = '/tmp/doozer_working'

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()

        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

        self.pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            data_gitref='',
            version='4.22',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

    def test_include_exclude_param_with_strategy_all(self):
        """
        Test that ALL strategy returns empty list.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ALL

        # when
        result = self.pipeline.include_exclude_param()

        # then
        self.assertEqual(result, [])

    def test_include_exclude_param_with_strategy_only(self):
        """
        Test that ONLY strategy returns --images parameter.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ONLY
        self.pipeline.build_plan.images_included = ['image1', 'image2', 'image3']

        # when
        result = self.pipeline.include_exclude_param()

        # then
        self.assertEqual(result, ['--images=image1,image2,image3'])

    def test_include_exclude_param_with_strategy_except(self):
        """
        Test that EXCEPT strategy returns --images= and --exclude parameters.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.EXCEPT
        self.pipeline.build_plan.images_excluded = ['bad1', 'bad2']

        # when
        result = self.pipeline.include_exclude_param()

        # then
        self.assertEqual(result, ['--images=', '--exclude=bad1,bad2'])

    def test_include_exclude_param_with_strategy_none_raises_error(self):
        """
        Test that NONE strategy raises ValueError.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.NONE

        # when/then
        with self.assertRaises(ValueError) as context:
            self.pipeline.include_exclude_param()

        self.assertIn('Invalid build strategy', str(context.exception))


class TestBuildPlan(IsolatedAsyncioTestCase):
    def test_build_plan_initialization_with_defaults(self):
        """
        Test BuildPlan initialization with default strategy.
        """

        # when
        plan = BuildPlan()

        # then
        self.assertEqual(plan.image_build_strategy, BuildStrategy.ALL)
        self.assertEqual(plan.images_included, [])
        self.assertEqual(plan.images_excluded, [])
        self.assertEqual(plan.active_image_count, 0)

    def test_build_plan_initialization_with_custom_strategy(self):
        """
        Test BuildPlan initialization with custom strategy.
        """

        # when
        plan = BuildPlan(image_build_strategy=BuildStrategy.ONLY)

        # then
        self.assertEqual(plan.image_build_strategy, BuildStrategy.ONLY)

    def test_build_plan_str_representation(self):
        """
        Test BuildPlan string representation.
        """

        # given
        plan = BuildPlan(image_build_strategy=BuildStrategy.EXCEPT)
        plan.images_excluded = ['image1', 'image2']
        plan.active_image_count = 10

        # when
        result = str(plan)

        # then
        self.assertIn('"image_build_strategy"', result)
        self.assertIn('"images_excluded"', result)
        self.assertIn('image1', result)
        self.assertIn('image2', result)
        self.assertIn('"active_image_count": 10', result)
