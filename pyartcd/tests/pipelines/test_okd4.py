#!/usr/bin/env python3

"""
Unit tests for the okd pipeline.
"""

import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
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
            version='4.23',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )
        # Simulate successful image builds
        pipeline.built_images = [
            {'name': 'test-image', 'nvr': 'test-1.0', 'image_pullspec': 'quay.io/test:latest', 'image_tag': 'latest'}
        ]

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch('pyartcd.pipelines.okd.jenkins') as mock_jenkins,
        ):
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            # Should be called twice - once for stream-coreos, once for stream-coreos-extensions
            self.assertEqual(mock_tag.call_count, 2)

            # Check first call (stream-coreos) — source is scos-5.0 (master branch)
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['source_pullspec'], 'origin/scos-5.0:stream-coreos')
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-4.23-art:stream-coreos')

            # Check second call (stream-coreos-extensions)
            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], 'origin/scos-5.0:stream-coreos-extensions')
            self.assertEqual(second_call[1]['target_tag'], 'origin/scos-4.23-art:stream-coreos-extensions')

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

    async def test_mirror_coreos_imagestreams_skipped_when_no_images_built(self):
        """
        Test that CoreOS mirroring is skipped when no images were successfully built.
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
        # No images were built (empty list)
        pipeline.built_images = []

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
            version='4.23',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )
        # Simulate successful image builds
        pipeline.built_images = [
            {'name': 'test-image', 'nvr': 'test-1.0', 'image_pullspec': 'quay.io/test:latest', 'image_tag': 'latest'}
        ]

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
            version='4.23',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )
        # Simulate successful image builds
        pipeline.built_images = [
            {'name': 'test-image', 'nvr': 'test-1.0', 'image_pullspec': 'quay.io/test:latest', 'image_tag': 'latest'}
        ]

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
            version='4.23',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='custom-namespace',
        )
        # Simulate successful image builds
        pipeline.built_images = [
            {'name': 'test-image', 'nvr': 'test-1.0', 'image_pullspec': 'quay.io/test:latest', 'image_tag': 'latest'}
        ]

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
            self.assertEqual(first_call[1]['source_pullspec'], 'custom-namespace/scos-5.0:stream-coreos')
            self.assertEqual(first_call[1]['target_tag'], 'custom-namespace/scos-4.23-art:stream-coreos')

            # Check second call uses custom namespace
            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], 'custom-namespace/scos-5.0:stream-coreos-extensions')
            self.assertEqual(second_call[1]['target_tag'], 'custom-namespace/scos-4.23-art:stream-coreos-extensions')

    async def test_mirror_coreos_imagestreams_uses_same_version_source(self):
        """
        Test that CoreOS mirroring uses the version's own scos stream as source for every
        version except the 4.23 special case (which has no dedicated scos-4.23 stream).
        """

        for version in ['4.21', '4.22', '5.0', '5.1']:
            with self.subTest(version=version):
                pipeline = KonfluxOkdPipeline(
                    runtime=self.mock_runtime,
                    image_build_strategy='all',
                    image_list=None,
                    assembly='stream',
                    data_path='https://github.com/openshift-eng/ocp-build-data',
                    data_gitref='',
                    version=version,
                    ignore_locks=False,
                    plr_template='',
                    lock_identifier='test-lock',
                    build_priority='10',
                    imagestream_namespace='origin',
                )

                pipeline.built_images = [
                    {
                        'name': 'test-image',
                        'nvr': 'test-1.0',
                        'image_pullspec': 'quay.io/test:latest',
                        'image_tag': 'latest',
                    }
                ]
                with (
                    patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
                    patch('pyartcd.pipelines.okd.jenkins'),
                ):
                    await pipeline.mirror_coreos_imagestreams()

                    self.assertEqual(mock_tag.call_count, 2)

                    first_call = mock_tag.call_args_list[0]
                    self.assertEqual(first_call[1]['source_pullspec'], f'origin/scos-{version}:stream-coreos')
                    self.assertEqual(first_call[1]['target_tag'], f'origin/scos-{version}-art:stream-coreos')

                    second_call = mock_tag.call_args_list[1]
                    self.assertEqual(
                        second_call[1]['source_pullspec'], f'origin/scos-{version}:stream-coreos-extensions'
                    )
                    self.assertEqual(
                        second_call[1]['target_tag'], f'origin/scos-{version}-art:stream-coreos-extensions'
                    )

    async def test_mirror_coreos_imagestreams_4_23_uses_5_0_source(self):
        """
        Test that CoreOS mirroring for version 4.23 uses 5.0 as the source.
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
        # Simulate successful image builds
        pipeline.built_images = [
            {'name': 'test-image', 'nvr': 'test-1.0', 'image_pullspec': 'quay.io/test:latest', 'image_tag': 'latest'}
        ]

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            # when
            await pipeline.mirror_coreos_imagestreams()

            # then
            # Should be called twice
            self.assertEqual(mock_tag.call_count, 2)

            # Check that 5.0 is used as source, 4.23 as target
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['source_pullspec'], 'origin/scos-5.0:stream-coreos')
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-4.23-art:stream-coreos')

            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], 'origin/scos-5.0:stream-coreos-extensions')
            self.assertEqual(second_call[1]['target_tag'], 'origin/scos-4.23-art:stream-coreos-extensions')


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
        Test that building_images returns True for ALL strategy with payload images.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ALL
        self.pipeline.group_images = ['image1', 'image2']  # Payload images after filtering

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
        self.pipeline.group_images = ['image1', 'image2', 'image3']  # Payload images after filtering
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
        Test that building_images returns True for EXCEPT strategy with some images to build.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.EXCEPT
        self.pipeline.group_images = ['image1', 'image2', 'image3']  # Payload images after filtering
        self.pipeline.build_plan.images_excluded = ['image1']

        # when
        result = self.pipeline.building_images()

        # then
        self.assertTrue(result)

    def test_building_images_with_strategy_all_no_payload_images(self):
        """
        Test that building_images returns False when no payload images exist (all filtered out).
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ALL
        self.pipeline.group_images = []  # All images filtered out as non-payload

        # when
        result = self.pipeline.building_images()

        # then
        self.assertFalse(result)

    def test_building_images_with_strategy_except_all_excluded(self):
        """
        Test that building_images returns False when all payload images are excluded.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.EXCEPT
        self.pipeline.group_images = ['image1', 'image2']
        self.pipeline.build_plan.images_excluded = ['image1', 'image2']  # All images excluded

        # when
        result = self.pipeline.building_images()

        # then
        self.assertFalse(result)

    def test_building_images_with_strategy_only_no_payload_match(self):
        """
        Test that building_images returns False when requested images are not payload images.
        This simulates the case where IMAGE_LIST contains only non-payload images.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ONLY
        self.pipeline.group_images = []  # Non-payload images were filtered out
        self.pipeline.build_plan.images_included = []  # No matches after filtering

        # when
        result = self.pipeline.building_images()

        # then
        self.assertFalse(result)


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
        Test that ALL strategy returns --images parameter with all payload images.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ALL
        self.pipeline.group_images = ['image1', 'image2', 'image3']  # Payload images after filtering

        # when
        result = self.pipeline.include_exclude_param()

        # then
        self.assertEqual(result, ['--images=image1,image2,image3'])

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
        Test that EXCEPT strategy returns --images parameter with remaining images after exclusion.
        """

        # given
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.EXCEPT
        self.pipeline.group_images = ['good1', 'good2', 'bad1', 'bad2']  # All payload images
        self.pipeline.build_plan.images_excluded = ['bad1', 'bad2']

        # when
        result = self.pipeline.include_exclude_param()

        # then
        # Should only include images not in the exclusion list
        self.assertEqual(result, ['--images=good1,good2'])

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


class TestDetectEmbargoedBuilds(IsolatedAsyncioTestCase):
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
        # Set up group_images so building_images() returns True by default
        self.pipeline.group_images = ['cli', 'installer', 'operator']

    async def test_detect_embargoed_builds_missing_rebase_results(self):
        """
        Test that pipeline crashes when rebase results are missing (fail-safe).
        """

        # given
        with patch.object(self.pipeline, 'load_state_yaml', return_value={}):
            # when/then
            with self.assertRaises(RuntimeError) as context:
                await self.pipeline.detect_embargoed_builds()

            self.assertIn('EMBARGO SAFETY', str(context.exception))
            self.assertIn('no rebase results', str(context.exception))

    async def test_detect_embargoed_builds_public_images_only(self):
        """
        Test that public images (private_fix=False) are allowed through.
        """

        # given
        mock_state = {
            'images:okd:rebase': {
                'images': {
                    'cli': {'status': 'success', 'private_fix': False},
                    'installer': {'status': 'success', 'private_fix': False},
                }
            }
        }

        with patch.object(self.pipeline, 'load_state_yaml', return_value=mock_state):
            # when
            await self.pipeline.detect_embargoed_builds()

            # then
            self.assertEqual(self.pipeline.embargoed_builds, [])

    async def test_detect_embargoed_builds_detects_private_fixes(self):
        """
        Test that embargoed images (private_fix=True) are detected and excluded.
        """

        # given
        mock_state = {
            'images:okd:rebase': {
                'images': {
                    'cli': {'status': 'success', 'private_fix': False},
                    'installer': {'status': 'success', 'private_fix': True},
                }
            }
        }

        with (
            patch.object(self.pipeline, 'load_state_yaml', return_value=mock_state),
            patch('pyartcd.pipelines.okd.jenkins') as mock_jenkins,
        ):
            # when
            await self.pipeline.detect_embargoed_builds()

            # then
            self.assertEqual(len(self.pipeline.embargoed_builds), 1)
            self.assertEqual(self.pipeline.embargoed_builds[0]['name'], 'installer')

            # Jenkins should be updated with warning
            mock_jenkins.update_description.assert_called_once()
            call_args = mock_jenkins.update_description.call_args[0][0]
            self.assertIn('EMBARGOED', call_args)
            self.assertIn('installer', call_args)

    async def test_detect_embargoed_builds_missing_private_fix_field(self):
        """
        Test that pipeline crashes when private_fix field is missing (fail-safe).
        """

        # given
        mock_state = {
            'images:okd:rebase': {
                'images': {
                    'cli': {'status': 'success'},  # Missing private_fix field
                }
            }
        }

        with patch.object(self.pipeline, 'load_state_yaml', return_value=mock_state):
            # when/then - expect KeyError when private_fix is missing
            with self.assertRaises(KeyError):
                await self.pipeline.detect_embargoed_builds()

    async def test_detect_embargoed_builds_missing_status_field(self):
        """
        Test that pipeline crashes when status field is missing (fail-safe).
        """

        # given
        mock_state = {
            'images:okd:rebase': {
                'images': {
                    'cli': {'private_fix': False},  # Missing status field
                }
            }
        }

        with patch.object(self.pipeline, 'load_state_yaml', return_value=mock_state):
            # when/then - expect KeyError when status is missing
            with self.assertRaises(KeyError):
                await self.pipeline.detect_embargoed_builds()

    async def test_detect_embargoed_builds_multiple_embargoed_images(self):
        """
        Test detection with multiple embargoed images.
        """

        # given
        mock_state = {
            'images:okd:rebase': {
                'images': {
                    'image1': {'status': 'success', 'private_fix': True},
                    'image2': {'status': 'success', 'private_fix': True},
                    'image3': {'status': 'success', 'private_fix': False},
                }
            }
        }

        with (
            patch.object(self.pipeline, 'load_state_yaml', return_value=mock_state),
            patch('pyartcd.pipelines.okd.jenkins') as mock_jenkins,
        ):
            # when
            await self.pipeline.detect_embargoed_builds()

            # then
            self.assertEqual(len(self.pipeline.embargoed_builds), 2)
            embargoed_names = {img['name'] for img in self.pipeline.embargoed_builds}
            self.assertEqual(embargoed_names, {'image1', 'image2'})

            mock_jenkins.update_description.assert_called_once()

    async def test_detect_embargoed_builds_skips_failed_images(self):
        """
        Test that failed/skipped images are ignored during embargo detection.
        """

        # given
        mock_state = {
            'images:okd:rebase': {
                'images': {
                    'cli': {'status': 'success', 'private_fix': False},
                    'installer': {'status': 'failure', 'private_fix': True},
                    'operator': {'status': 'skipped', 'private_fix': False},
                }
            }
        }

        with patch.object(self.pipeline, 'load_state_yaml', return_value=mock_state):
            # when
            await self.pipeline.detect_embargoed_builds()

            # then
            # Only successful images are checked; failed/skipped are ignored
            self.assertEqual(len(self.pipeline.embargoed_builds), 0)

    async def test_detect_embargoed_builds_no_images_to_build(self):
        """
        Test that embargo detection is skipped when no images are being built (all filtered out).
        """

        # given
        self.pipeline.group_images = []  # No payload images
        self.pipeline.build_plan.images_included = []
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ONLY

        # when
        await self.pipeline.detect_embargoed_builds()

        # then - should complete without error
        self.assertEqual(len(self.pipeline.embargoed_builds), 0)


class TestUpdateImagestreamsDataPath(IsolatedAsyncioTestCase):
    """Tests that update_imagestreams resolves the ocp-build-data clone directory
    dynamically from self.data_path, so forks with different repo names work."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.mock_runtime = MagicMock()
        self.mock_runtime.dry_run = False
        self.mock_runtime.doozer_working = str(Path(self.tmpdir) / 'doozer_working')
        Path(self.mock_runtime.doozer_working).mkdir(parents=True)

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()
        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

    def _make_pipeline(self, data_path):
        return KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='all',
            image_list=None,
            assembly='stream',
            data_path=data_path,
            data_gitref='',
            version='4.22',
            ignore_locks=False,
            plr_template='',
            lock_identifier='test-lock',
            build_priority='10',
            imagestream_namespace='origin',
        )

    async def test_update_imagestreams_canonical_data_path(self):
        """update_imagestreams reads metadata from 'ocp-build-data' for the canonical URL."""

        # given
        data_path = 'https://github.com/openshift-eng/ocp-build-data'
        pipeline = self._make_pipeline(data_path)
        pipeline.built_images = [
            {'name': 'cli', 'nvr': 'cli-1.0', 'image_pullspec': 'quay.io/okd/cli@sha256:abc', 'image_tag': 'latest'},
        ]

        # Create the expected metadata directory and file
        images_dir = Path(self.mock_runtime.doozer_working) / 'ocp-build-data' / 'images'
        images_dir.mkdir(parents=True)
        yaml_file = images_dir / 'cli.yml'
        yaml_file.write_text(yaml.dump({'name': 'openshift/ose-cli', 'for_payload': True}))

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock),
            patch.object(
                pipeline, '_get_arch_pullspec', new_callable=AsyncMock, return_value='quay.io/okd/cli@sha256:arm64'
            ),
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            # when / then — should not raise FileNotFoundError
            await pipeline.update_imagestreams()

    async def test_update_imagestreams_fork_data_path(self):
        """update_imagestreams reads metadata from fork-named directory, not hardcoded 'ocp-build-data'."""

        # given — a fork whose repo basename differs from the canonical name
        data_path = 'https://github.com/redhat-chai-bot/openshift-eng_ocp-build-data'
        pipeline = self._make_pipeline(data_path)
        pipeline.built_images = [
            {'name': 'cli', 'nvr': 'cli-1.0', 'image_pullspec': 'quay.io/okd/cli@sha256:abc', 'image_tag': 'latest'},
        ]

        # Create the expected metadata directory using the fork's repo name
        images_dir = Path(self.mock_runtime.doozer_working) / 'openshift-eng_ocp-build-data' / 'images'
        images_dir.mkdir(parents=True)
        yaml_file = images_dir / 'cli.yml'
        yaml_file.write_text(yaml.dump({'name': 'openshift/ose-cli', 'for_payload': True}))

        arm64_pullspec = 'quay.io/okd/cli@sha256:arm64digest'
        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch.object(pipeline, '_get_arch_pullspec', new_callable=AsyncMock, return_value=arm64_pullspec),
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            # when — should resolve the fork directory, not hardcoded 'ocp-build-data'
            await pipeline.update_imagestreams()

            # then — tagged into both multi-arch and arm64 imagestreams
            self.assertEqual(mock_tag.call_count, 2)

    async def test_update_imagestreams_data_path_with_git_suffix(self):
        """update_imagestreams strips .git suffix when deriving the data directory name."""

        # given
        data_path = 'https://github.com/openshift-eng/ocp-build-data.git'
        pipeline = self._make_pipeline(data_path)
        pipeline.built_images = [
            {'name': 'cli', 'nvr': 'cli-1.0', 'image_pullspec': 'quay.io/okd/cli@sha256:abc', 'image_tag': 'latest'},
        ]

        # .git suffix should be stripped → directory is 'ocp-build-data'
        images_dir = Path(self.mock_runtime.doozer_working) / 'ocp-build-data' / 'images'
        images_dir.mkdir(parents=True)
        yaml_file = images_dir / 'cli.yml'
        yaml_file.write_text(yaml.dump({'name': 'openshift/ose-cli', 'for_payload': True}))

        arm64_pullspec = 'quay.io/okd/cli@sha256:arm64digest'
        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch.object(pipeline, '_get_arch_pullspec', new_callable=AsyncMock, return_value=arm64_pullspec),
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            # when / then — should not raise
            await pipeline.update_imagestreams()
            # tagged into both multi-arch and arm64 imagestreams
            self.assertEqual(mock_tag.call_count, 2)

    async def test_update_imagestreams_tags_arm64_imagestream(self):
        """update_imagestreams tags aarch64-specific pullspec into origin-arm64/scos-{version}-art-arm64."""

        data_path = 'https://github.com/openshift-eng/ocp-build-data'
        pipeline = self._make_pipeline(data_path)
        pipeline.built_images = [
            {
                'name': 'cli',
                'nvr': 'cli-1.0',
                'image_pullspec': 'quay.io/okd/cli@sha256:multiarchabcdef',
                'image_tag': 'latest',
            },
        ]

        images_dir = Path(self.mock_runtime.doozer_working) / 'ocp-build-data' / 'images'
        images_dir.mkdir(parents=True)
        (images_dir / 'cli.yml').write_text(yaml.dump({'name': 'openshift/ose-cli', 'for_payload': True}))

        arm64_pullspec = 'quay.io/okd/cli@sha256:arm64digest'
        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch.object(pipeline, '_get_arch_pullspec', new_callable=AsyncMock, return_value=arm64_pullspec),
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            await pipeline.update_imagestreams()

            # First call: multi-arch manifest into scos-4.22-art
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['source_pullspec'], 'quay.io/okd/cli@sha256:multiarchabcdef')
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-4.22-art:cli')

            # Second call: arm64-specific pullspec into origin-arm64/scos-4.22-art-arm64
            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['source_pullspec'], arm64_pullspec)
            self.assertEqual(second_call[1]['target_tag'], 'origin-arm64/scos-4.22-art-arm64:cli')

    async def test_update_imagestreams_arm64_failure_does_not_block_other_images(self):
        """Failure to resolve arm64 pullspec logs a warning but does not fail the pipeline."""

        data_path = 'https://github.com/openshift-eng/ocp-build-data'
        pipeline = self._make_pipeline(data_path)
        pipeline.built_images = [
            {
                'name': 'cli',
                'nvr': 'cli-1.0',
                'image_pullspec': 'quay.io/okd/cli@sha256:abc',
                'image_tag': 'latest',
            },
        ]

        images_dir = Path(self.mock_runtime.doozer_working) / 'ocp-build-data' / 'images'
        images_dir.mkdir(parents=True)
        (images_dir / 'cli.yml').write_text(yaml.dump({'name': 'openshift/ose-cli', 'for_payload': True}))

        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            # arm64 resolution fails
            patch.object(pipeline, '_get_arch_pullspec', new_callable=AsyncMock, return_value=None),
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            await pipeline.update_imagestreams()

            # multi-arch tag still succeeds; arm64 tag is skipped
            self.assertEqual(mock_tag.call_count, 1)
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-4.22-art:cli')


class TestGetArchPullspec(IsolatedAsyncioTestCase):
    """Tests for _get_arch_pullspec: registry_config fix and generic arch support."""

    def setUp(self):
        self.mock_runtime = MagicMock()
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

    @patch('pyartcd.pipelines.okd.oc_image_info_for_arch_async', new_callable=AsyncMock)
    async def test_get_arch_pullspec_passes_registry_config(self, mock_oc_info):
        """_get_arch_pullspec passes registry_config pointing to a temp file with empty JSON."""
        mock_oc_info.return_value = {'digest': 'sha256:arm64digest'}

        result = await self.pipeline._get_arch_pullspec('quay.io/okd/cli@sha256:manifest', 'arm64')

        self.assertEqual(result, 'quay.io/okd/cli@sha256:arm64digest')
        mock_oc_info.assert_called_once()
        call_kwargs = mock_oc_info.call_args[1]
        self.assertIn('registry_config', call_kwargs)
        # The temp file may be cleaned up, but registry_config was passed (non-None)
        self.assertIsNotNone(call_kwargs['registry_config'])

    @patch('pyartcd.pipelines.okd.oc_image_info_for_arch_async', new_callable=AsyncMock)
    async def test_get_arch_pullspec_passes_go_arch(self, mock_oc_info):
        """_get_arch_pullspec forwards the go_arch parameter correctly."""
        mock_oc_info.return_value = {'digest': 'sha256:s390xdigest'}

        result = await self.pipeline._get_arch_pullspec('quay.io/okd/cli@sha256:manifest', 's390x')

        self.assertEqual(result, 'quay.io/okd/cli@sha256:s390xdigest')
        call_kwargs = mock_oc_info.call_args
        self.assertEqual(call_kwargs[0][0], 'quay.io/okd/cli@sha256:manifest')
        self.assertEqual(call_kwargs[1]['go_arch'], 's390x')

    @patch('pyartcd.pipelines.okd.oc_image_info_for_arch_async', new_callable=AsyncMock)
    async def test_get_arch_pullspec_returns_none_on_no_digest(self, mock_oc_info):
        """_get_arch_pullspec returns None when no digest is found."""
        mock_oc_info.return_value = {}

        result = await self.pipeline._get_arch_pullspec('quay.io/okd/cli@sha256:manifest', 'arm64')

        self.assertIsNone(result)

    @patch('pyartcd.pipelines.okd.oc_image_info_for_arch_async', new_callable=AsyncMock)
    async def test_get_arch_pullspec_returns_none_on_exception(self, mock_oc_info):
        """_get_arch_pullspec returns None and logs warning on failure."""
        mock_oc_info.side_effect = Exception('oc failed')

        result = await self.pipeline._get_arch_pullspec('quay.io/okd/cli@sha256:manifest', 'arm64')

        self.assertIsNone(result)


class TestArchSuffixedNamespace(IsolatedAsyncioTestCase):
    """Tests for arch-suffixed namespace derivation in update_imagestreams."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.mock_runtime = MagicMock()
        self.mock_runtime.dry_run = False
        self.mock_runtime.doozer_working = str(Path(self.tmpdir) / 'doozer_working')
        Path(self.mock_runtime.doozer_working).mkdir(parents=True)

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()
        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

    async def test_arm64_imagestream_uses_arch_suffixed_namespace(self):
        """Verify that arm64 imagestream uses origin-arm64 namespace (not origin)."""

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
        pipeline.built_images = [
            {
                'name': 'cli',
                'nvr': 'cli-1.0',
                'image_pullspec': 'quay.io/okd/cli@sha256:multiarch',
                'image_tag': 'latest',
            },
        ]

        images_dir = Path(self.mock_runtime.doozer_working) / 'ocp-build-data' / 'images'
        images_dir.mkdir(parents=True)
        (images_dir / 'cli.yml').write_text(yaml.dump({'name': 'openshift/ose-cli', 'for_payload': True}))

        arm64_pullspec = 'quay.io/okd/cli@sha256:arm64digest'
        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock) as mock_tag,
            patch.object(pipeline, '_get_arch_pullspec', new_callable=AsyncMock, return_value=arm64_pullspec),
            patch('pyartcd.pipelines.okd.jenkins'),
        ):
            await pipeline.update_imagestreams()

            # multi-arch into origin/scos-5.0-art
            first_call = mock_tag.call_args_list[0]
            self.assertEqual(first_call[1]['target_tag'], 'origin/scos-5.0-art:cli')

            # arm64 into origin-arm64/scos-5.0-art-arm64 (arch-suffixed namespace!)
            second_call = mock_tag.call_args_list[1]
            self.assertEqual(second_call[1]['target_tag'], 'origin-arm64/scos-5.0-art-arm64:cli')

    async def test_log_message_only_reports_updated_imagestreams(self):
        """Verify the log message only mentions imagestreams that were actually updated."""

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
        pipeline.built_images = [
            {
                'name': 'cli',
                'nvr': 'cli-1.0',
                'image_pullspec': 'quay.io/okd/cli@sha256:multiarch',
                'image_tag': 'latest',
            },
        ]

        images_dir = Path(self.mock_runtime.doozer_working) / 'ocp-build-data' / 'images'
        images_dir.mkdir(parents=True)
        (images_dir / 'cli.yml').write_text(yaml.dump({'name': 'openshift/ose-cli', 'for_payload': True}))

        # arm64 resolution fails — only primary imagestream should be mentioned
        with (
            patch.object(pipeline, '_tag_image_to_stream', new_callable=AsyncMock),
            patch.object(pipeline, '_get_arch_pullspec', new_callable=AsyncMock, return_value=None),
            patch('pyartcd.pipelines.okd.jenkins') as mock_jenkins,
        ):
            await pipeline.update_imagestreams()

            # Jenkins description should only mention the primary imagestream
            desc_calls = mock_jenkins.update_description.call_args_list
            success_calls = [c for c in desc_calls if 'Updated' in str(c)]
            self.assertEqual(len(success_calls), 1)
            success_msg = success_calls[0][0][0]
            self.assertIn('origin/scos-4.22-art', success_msg)
            # Should NOT mention the arm64 imagestream since it wasn't updated
            self.assertNotIn('arm64', success_msg)


class TestRebaseFailCounters(IsolatedAsyncioTestCase):
    """Tests for update_rebase_fail_counters reading per-image status from state.yaml."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.mock_runtime = MagicMock()
        self.mock_runtime.dry_run = False
        self.mock_runtime.doozer_working = str(Path(self.tmpdir) / 'doozer_working')
        Path(self.mock_runtime.doozer_working).mkdir(parents=True)

        mock_slack_client = MagicMock()
        mock_slack_client.say = AsyncMock()
        mock_slack_client.bind_channel = MagicMock()
        self.mock_runtime.new_slack_client = MagicMock(return_value=mock_slack_client)

        self.pipeline = KonfluxOkdPipeline(
            runtime=self.mock_runtime,
            image_build_strategy='only',
            image_list='img-a',
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

    def _write_state(self, images_state):
        state = {'images:okd:rebase': {'images': images_state}}
        state_path = Path(self.mock_runtime.doozer_working) / 'state.yaml'
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

    @patch('pyartcd.pipelines.okd.increment_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.okd.reset_fail_counter', new_callable=AsyncMock)
    async def test_only_strategy_resets_successful_images_from_state(self, mock_reset, mock_incr):
        """ONLY strategy reads per-image status from state.yaml and resets only status=='success'."""
        self._write_state(
            {
                'img-a': {'status': 'success', 'private_fix': False},
                'extra-parent': {'status': 'success', 'private_fix': False},
                'parent-img': {'status': 'failure', 'private_fix': False},
            }
        )
        self.pipeline.build_plan.image_build_strategy = BuildStrategy.ONLY

        await self.pipeline.update_rebase_fail_counters(['parent-img'])

        reset_names = {c.args[0].split(':')[-1] for c in mock_reset.call_args_list}
        incr_names = {c.args[0].split(':')[-1] for c in mock_incr.call_args_list}
        self.assertEqual(reset_names, {'img-a', 'extra-parent'})
        self.assertEqual(incr_names, {'parent-img'})
