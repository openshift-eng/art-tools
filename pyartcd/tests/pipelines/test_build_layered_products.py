import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import yaml
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.variants import BuildVariant
from doozerlib.constants import KONFLUX_DEFAULT_IMAGE_REPO
from pyartcd.build_strategy import BuildStrategy
from pyartcd.pipelines.build_layered_products import BuildLayeredProductsPipeline
from pyartcd.runtime import Runtime


class TestBuildLayeredProductsPipeline(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = Mock(spec=Runtime)
        self.tmpdir = tempfile.mkdtemp()
        self.runtime.working_dir = Path(self.tmpdir)
        self.runtime.doozer_working = os.path.join(self.tmpdir, 'doozer_working')
        os.makedirs(self.runtime.doozer_working, exist_ok=True)
        self.runtime.dry_run = False
        self.runtime.config = {}
        self.runtime.logger = MagicMock()
        self.rebase_reset_patcher = patch(
            "pyartcd.pipelines.build_layered_products.reset_fail_counter", new_callable=AsyncMock
        )
        self.rebase_increment_patcher = patch(
            "pyartcd.pipelines.build_layered_products.increment_fail_counter", new_callable=AsyncMock
        )
        self.mock_rebase_reset = self.rebase_reset_patcher.start()
        self.mock_rebase_increment = self.rebase_increment_patcher.start()
        self.addCleanup(self.rebase_reset_patcher.stop)
        self.addCleanup(self.rebase_increment_patcher.stop)

        with patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins'):
            self.pipeline = BuildLayeredProductsPipeline(
                runtime=self.runtime,
                group='oadp-1.4',
                version='1.4.9',
                assembly='stream',
                image_list='oadp-velero-restic-restore-helper',
                data_path='https://github.com/openshift-eng/ocp-build-data',
                skip_bundle_build=True,
            )

    def test_test_assembly_sets_test_banner(self):
        """When assembly is 'test', the Jenkins title should include [TEST]."""
        with (
            patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins'),
            patch('pyartcd.pipelines.build_layered_products.jenkins.update_title') as mock_update_title,
        ):
            BuildLayeredProductsPipeline(
                runtime=self.runtime,
                group='oadp-1.4',
                version='1.4.9',
                assembly='test',
                image_list='oadp-velero-restic-restore-helper',
                data_path='https://github.com/openshift-eng/ocp-build-data',
                skip_bundle_build=True,
            )
        mock_update_title.assert_called_once_with(" [TEST]")

    def test_non_test_assembly_does_not_set_test_banner(self):
        """When assembly is not 'test', the Jenkins [TEST] banner should not be set."""
        with (
            patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins'),
            patch('pyartcd.pipelines.build_layered_products.jenkins.update_title') as mock_update_title,
        ):
            BuildLayeredProductsPipeline(
                runtime=self.runtime,
                group='oadp-1.4',
                version='1.4.9',
                assembly='stream',
                image_list='oadp-velero-restic-restore-helper',
                data_path='https://github.com/openshift-eng/ocp-build-data',
                skip_bundle_build=True,
            )
        mock_update_title.assert_not_called()

    def test_doozer_base_command_uses_explicit_build_variant(self):
        command = self.pipeline._doozer_base_command(BuildVariant.OADP)

        self.assertIn("--variant=oadp", command)

    @patch("pyartcd.pipelines.build_layered_products.increment_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_layered_products.reset_fail_counter", new_callable=AsyncMock)
    async def test_update_build_fail_counters_tags_layered_failures_with_variant(self, mock_reset, mock_increment):
        """Layered-product build counters store the product build variant."""
        record_log_path = Path(self.runtime.doozer_working, "record.log")
        record_log_path.write_text(
            "\n".join(
                [
                    "image_build_konflux|name=oadp-operator|status=0|nvrs=oadp-operator-1.0-1",
                    "image_build_konflux|name=oadp-velero|status=-1|nvrs=oadp-velero-1.0-1|build_pipeline_url=http://plr/1",
                ]
            )
            + "\n"
        )

        await self.pipeline.update_build_fail_counters(BuildVariant.OADP)

        self.assertEqual(
            {call.args[0] for call in mock_reset.await_args_list},
            {
                "count:build-failure:konflux:oadp-1.4:oadp-operator",
                "count:ec-failure:konflux:oadp-1.4:oadp-operator",
                "count:release-failure:konflux:oadp-1.4:oadp-operator",
            },
        )
        mock_increment.assert_awaited_once_with(
            "count:build-failure:konflux:oadp-1.4:oadp-velero",
            build_variant="oadp",
            jenkins_url=os.getenv("BUILD_URL"),
            nvr="oadp-velero-1.0-1",
            pipeline_url="http://plr/1",
        )

    @patch("pyartcd.pipelines.build_layered_products.increment_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_layered_products.reset_fail_counter", new_callable=AsyncMock)
    async def test_update_build_fail_counters_categorizes_failure_types(self, mock_reset, mock_increment):
        """Layered-product counters use separate build, ITS, and release categories."""
        record_log_path = Path(self.runtime.doozer_working, "record.log")
        record_log_path.write_text(
            "\n".join(
                [
                    "image_build_konflux|name=successful|status=0|nvrs=successful-1.0-1",
                    "image_build_konflux|name=build-failure|status=-1|outcome=build_error|build_pipeline_url=http://build/1",
                    "image_build_konflux|name=its-failure|status=-1|outcome=its_error|ec_pipeline_url=http://its/1",
                    "image_build_konflux|name=release-failure|status=-1|outcome=release_error|release_pipeline=http://release/1",
                ]
            )
            + "\n"
        )

        await self.pipeline.update_build_fail_counters(BuildVariant.OADP)

        self.assertEqual(len(mock_reset.await_args_list), 3)
        increment_calls = {call.args[0]: call for call in mock_increment.await_args_list}
        self.assertEqual(
            set(increment_calls),
            {
                "count:build-failure:konflux:oadp-1.4:build-failure",
                "count:ec-failure:konflux:oadp-1.4:its-failure",
                "count:release-failure:konflux:oadp-1.4:release-failure",
            },
        )
        self.assertEqual(
            increment_calls["count:ec-failure:konflux:oadp-1.4:its-failure"].kwargs["pipeline_url"], "http://its/1"
        )
        self.assertEqual(
            increment_calls["count:release-failure:konflux:oadp-1.4:release-failure"].kwargs["pipeline_url"],
            "http://release/1",
        )

    @patch("pyartcd.pipelines.build_layered_products.increment_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_layered_products.reset_fail_counter", new_callable=AsyncMock)
    async def test_update_build_fail_counters_preserves_mixed_failure_behavior(self, mock_reset, mock_increment):
        """Layered-product counters preserve the existing mixed-failure behavior."""
        record_log_path = Path(self.runtime.doozer_working, "record.log")
        record_log_path.write_text(
            "\n".join(
                [
                    "image_build_konflux|name=real-failure|status=-1|task_id=plr-1|nvrs=real-1",
                    "image_build_konflux|name=infrastructure-failure|status=-1|task_id=n/a|nvrs=",
                    "image_build_konflux|name=parent-failure|status=-1|task_id=n/a|message=parent images failed to build",
                ]
            )
            + "\n"
        )

        await self.pipeline.update_build_fail_counters(BuildVariant.OADP)

        self.assertEqual(
            [call.args[0] for call in mock_increment.await_args_list],
            [
                "count:build-failure:konflux:oadp-1.4:real-failure",
                "count:build-failure:konflux:oadp-1.4:infrastructure-failure",
            ],
        )

    @patch("pyartcd.util.logger")
    @patch("pyartcd.pipelines.build_layered_products.increment_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_layered_products.reset_fail_counter", new_callable=AsyncMock)
    async def test_update_build_fail_counters_skips_unattempted_failures(self, mock_reset, mock_increment, mock_logger):
        """Layered-product counters skip failures when no image build was attempted."""
        record_log_path = Path(self.runtime.doozer_working, "record.log")
        record_log_path.write_text(
            "\n".join(
                [
                    "image_build_konflux|name=infrastructure-failure|status=-1|task_id=n/a",
                    "image_build_konflux|name=parent-failure|status=-1|task_id=n/a|message=parent images failed to build",
                ]
            )
            + "\n"
        )

        await self.pipeline.update_build_fail_counters(BuildVariant.OADP)

        mock_reset.assert_not_awaited()
        mock_increment.assert_not_awaited()
        mock_logger.warning.assert_called_once()

    @patch("pyartcd.pipelines.build_layered_products.increment_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_layered_products.reset_fail_counter", new_callable=AsyncMock)
    async def test_update_build_fail_counters_propagates_redis_errors(self, mock_reset, mock_increment):
        """Redis counter errors propagate instead of being silently discarded."""
        record_log_path = Path(self.runtime.doozer_working, "record.log")
        record_log_path.write_text("image_build_konflux|name=oadp-operator|status=0\n")
        mock_reset.side_effect = RuntimeError("Redis unavailable")

        with self.assertRaisesRegex(RuntimeError, "Redis unavailable"):
            await self.pipeline.update_build_fail_counters(BuildVariant.OADP)

    async def test_rebase_success_returns_no_excluded_images(self):
        """When rebase succeeds, no images are excluded."""
        with patch('pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async', new_callable=AsyncMock):
            result = await self.pipeline._rebase('img-a,img-b', BuildVariant.OADP)
        self.assertEqual(result, [])
        self.assertEqual(
            [call.args[0] for call in self.mock_rebase_reset.await_args_list],
            [
                "count:rebase-failure:konflux:oadp-1.4:img-a",
                "count:rebase-failure:konflux:oadp-1.4:img-b",
            ],
        )
        self.mock_rebase_increment.assert_not_awaited()

    async def test_rebase_updates_shared_failure_counters(self):
        """Layered-product rebase failures use the shared OCP counter behavior."""
        state = {
            "images:konflux:rebase": {
                "images": {
                    "healthy": {"status": "success"},
                    "failed": {"status": "failure"},
                    "skipped": {"status": "skipped"},
                },
                "failed-images": ["failed"],
                "skipped-due-to-parent-rebase-failure": ["skipped"],
            }
        }
        state_path = Path(self.runtime.doozer_working, "state.yaml")
        with state_path.open("w") as f:
            yaml.safe_dump(state, f)

        with patch(
            "pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async",
            new_callable=AsyncMock,
            side_effect=ChildProcessError("exit code 1"),
        ):
            result = await self.pipeline._rebase("healthy,failed,skipped", BuildVariant.OADP)

        self.assertEqual(result, ["failed", "skipped"])
        self.mock_rebase_reset.assert_awaited_once_with("count:rebase-failure:konflux:oadp-1.4:healthy")
        self.mock_rebase_increment.assert_awaited_once_with(
            "count:rebase-failure:konflux:oadp-1.4:failed",
            build_variant="oadp",
            jenkins_url=os.getenv("BUILD_URL"),
        )

    async def test_rebase_failure_returns_excluded_images(self):
        """When rebase fails and state.yaml records failed images, those images are returned as excluded."""
        state = {'images:konflux:rebase': {'failed-images': ['oadp-operator']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            result = await self.pipeline._rebase('oadp-velero-restic-restore-helper,oadp-operator', BuildVariant.OADP)

        self.assertEqual(result, ['oadp-operator'])

    async def test_rebase_failure_returns_skipped_due_to_parent_images(self):
        """Images listed as skipped due to parent rebase failure are included in the excluded list."""
        state = {
            'images:konflux:rebase': {
                'failed-images': ['parent-image'],
                'skipped-due-to-parent-rebase-failure': ['child-image'],
            }
        }
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            result = await self.pipeline._rebase('parent-image,child-image,other-image', BuildVariant.OADP)

        self.assertEqual(result, ['parent-image', 'child-image'])

    async def test_rebase_failure_all_images_excluded(self):
        """When all requested images fail rebase, all are returned as excluded."""
        state = {'images:konflux:rebase': {'failed-images': ['img-a', 'img-b']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            result = await self.pipeline._rebase('img-a,img-b', BuildVariant.OADP)

        self.assertEqual(result, ['img-a', 'img-b'])

    async def test_rebase_failure_no_state_file_reraises(self):
        """When rebase fails but state.yaml does not exist, the error is re-raised."""
        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            with self.assertRaises(ChildProcessError):
                await self.pipeline._rebase('img-a', BuildVariant.OADP)

    async def test_rebase_failure_no_failed_images_in_state_reraises(self):
        """When state.yaml exists but has no failed-images, the error is re-raised."""
        state = {'some_other_key': {}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            with self.assertRaises(ChildProcessError):
                await self.pipeline._rebase('img-a', BuildVariant.OADP)

    async def test_rebase_and_build_raises_when_no_images_remain(self):
        """When all images fail rebase, ValueError is raised."""
        state = {'images:konflux:rebase': {'failed-images': ['oadp-velero-restic-restore-helper']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            with self.assertRaises(ValueError) as ctx:
                await self.pipeline._rebase_and_build('oadp', KONFLUX_DEFAULT_IMAGE_REPO)

        self.assertIn('No buildable images remaining after rebase', str(ctx.exception))
        self.pipeline._logger.error.assert_any_call('No buildable images remaining after rebase')

    async def test_rebase_and_build_proceeds_with_remaining_images(self):
        """When some images fail rebase, build proceeds with the surviving images."""
        state = {'images:konflux:rebase': {'failed-images': ['oadp-operator']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        self.pipeline.image_list = 'oadp-velero-restic-restore-helper,oadp-operator'

        call_count = 0

        async def mock_cmd_assert(cmd, env=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ChildProcessError('exit code 1')

        with (
            patch(
                'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
                side_effect=mock_cmd_assert,
            ),
            patch(
                'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
                return_value='/path/to/kubeconfig',
            ),
            patch.object(self.pipeline, '_update_build_description'),
        ):
            await self.pipeline._rebase_and_build('oadp', KONFLUX_DEFAULT_IMAGE_REPO)

        self.assertEqual(call_count, 2)
        self.pipeline._logger.info.assert_any_call('Successfully built oadp-velero-restic-restore-helper')

    async def test_skip_rebase_passes_full_list_to_build(self):
        """When skip_rebase is set, the full image list goes directly to build."""
        self.pipeline.skip_rebase = True

        with (
            patch(
                'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
                new_callable=AsyncMock,
            ) as mock_cmd,
            patch(
                'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
                return_value='/path/to/kubeconfig',
            ),
            patch.object(self.pipeline, '_update_build_description'),
        ):
            await self.pipeline._rebase_and_build('oadp', KONFLUX_DEFAULT_IMAGE_REPO)

        mock_cmd.assert_called_once()
        cmd = mock_cmd.call_args[0][0]
        self.assertIn(f'--images={self.pipeline.image_list}', cmd)
        self.assertIn('--variant=oadp', cmd)
        self.assertIn('beta:images:konflux:build', cmd)

    @patch('pyartcd.pipelines.build_layered_products.jenkins.update_description')
    def test_update_build_description_no_record_log(self, mock_update_desc):
        """When record.log does not exist, no description update is made."""
        self.pipeline._update_build_description()
        mock_update_desc.assert_not_called()

    @patch('pyartcd.pipelines.build_layered_products.jenkins.update_description')
    def test_update_build_description_all_succeeded(self, mock_update_desc):
        """When all images succeed, description shows count with no failures."""
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        record_log_path.write_text('image_build_konflux|name=img-a|status=0\nimage_build_konflux|name=img-b|status=0\n')
        self.pipeline._update_build_description()
        mock_update_desc.assert_called_once_with('2 image(s) succeeded<br/>')

    @patch('pyartcd.pipelines.build_layered_products.jenkins.update_description')
    def test_update_build_description_some_failed(self, mock_update_desc):
        """When some images fail, description shows succeeded/failed counts and lists failed names."""
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        record_log_path.write_text(
            'image_build_konflux|name=img-a|status=0\n'
            'image_build_konflux|name=img-b|status=1\n'
            'image_build_konflux|name=img-c|status=1\n'
        )
        self.pipeline._update_build_description()
        calls = [c[0][0] for c in mock_update_desc.call_args_list]
        self.assertEqual(calls[0], '1 image(s) succeeded, 2 image(s) failed<br/>')
        self.assertEqual(calls[1], 'Failed images: img-b, img-c<br/>')

    @patch('pyartcd.pipelines.build_layered_products.jenkins.update_description')
    def test_update_build_description_many_failed(self, mock_update_desc):
        """When more than 10 images fail, description omits individual names."""
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        lines = []
        for i in range(12):
            lines.append(f'image_build_konflux|name=img-{i}|status=1\n')
        lines.append('image_build_konflux|name=img-ok|status=0\n')
        record_log_path.write_text(''.join(lines))
        self.pipeline._update_build_description()
        calls = [c[0][0] for c in mock_update_desc.call_args_list]
        self.assertEqual(calls[0], '1 image(s) succeeded, 12 image(s) failed<br/>')
        self.assertEqual(calls[1], 'Check record.log for the full list of failed images<br/>')

    @patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.build_layered_products.load_group_config')
    @patch('pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch(
        'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
        return_value='/path/to/kubeconfig',
    )
    @patch('pyartcd.pipelines.build_layered_products.resolve_konflux_namespace_by_product', return_value='test-ns')
    async def test_image_repo_from_group_config(
        self, mock_resolve_ns, mock_resolve_kube, mock_cmd, mock_load_config, mock_jenkins
    ):
        """When group config has konflux.image_repo, it should be used instead of the default."""
        mock_load_config.return_value = {
            'product': 'openshift_agent_installer',
            'version': '4.20',
            'konflux': {
                'image_repo': 'quay.io/redhat-user-workloads/ocp-agent-based-installer-tenant/ove-ui-iso',
            },
        }

        pipeline = BuildLayeredProductsPipeline(
            runtime=self.runtime,
            group='openshift_agent_installer-4.20',
            version='4.20',
            assembly='stream',
            image_list='art-agent-installer-iso',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            skip_bundle_build=True,
            skip_rebase=True,
        )

        with patch(
            "pyartcd.pipelines.build_layered_products.get_build_variant_for_product",
            return_value=BuildVariant.OCP,
        ):
            await pipeline.run()

        build_cmd = mock_cmd.call_args[0][0]
        image_repo_args = [arg for arg in build_cmd if arg.startswith('--image-repo=')]
        self.assertEqual(len(image_repo_args), 1)
        self.assertEqual(
            image_repo_args[0],
            '--image-repo=quay.io/redhat-user-workloads/ocp-agent-based-installer-tenant/ove-ui-iso',
        )

    @patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.build_layered_products.load_group_config')
    @patch('pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch(
        'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
        return_value='/path/to/kubeconfig',
    )
    @patch('pyartcd.pipelines.build_layered_products.resolve_konflux_namespace_by_product', return_value='test-ns')
    async def test_image_repo_falls_back_to_default(
        self, mock_resolve_ns, mock_resolve_kube, mock_cmd, mock_load_config, mock_jenkins
    ):
        """When group config does not have konflux.image_repo, fall back to KONFLUX_DEFAULT_IMAGE_REPO."""
        mock_load_config.return_value = {
            'product': 'ocp',
            'version': '4.18',
        }

        pipeline = BuildLayeredProductsPipeline(
            runtime=self.runtime,
            group='openshift-4.18',
            version='4.18',
            assembly='stream',
            image_list='some-image',
            data_path='https://github.com/openshift-eng/ocp-build-data',
            skip_bundle_build=True,
            skip_rebase=True,
        )

        await pipeline.run()

        build_cmd = mock_cmd.call_args[0][0]
        image_repo_args = [arg for arg in build_cmd if arg.startswith('--image-repo=')]
        self.assertEqual(len(image_repo_args), 1)
        self.assertEqual(image_repo_args[0], f'--image-repo={KONFLUX_DEFAULT_IMAGE_REPO}')

    def test_all_strategy_with_image_list_raises(self):
        """When strategy is all and image_list is non-empty, ValueError is raised."""
        with patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins'):
            with self.assertRaises(ValueError) as ctx:
                BuildLayeredProductsPipeline(
                    runtime=self.runtime,
                    group='oadp-1.4',
                    version='1.4.9',
                    assembly='stream',
                    image_list='oadp-operator',
                    image_build_strategy='all',
                    data_path='https://github.com/openshift-eng/ocp-build-data',
                    skip_bundle_build=True,
                )
        self.assertIn("image_list must be empty", str(ctx.exception))

    async def test_rebase_all_strategy_no_images_flag(self):
        """When strategy is all, rebase command does not include --images=."""
        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
        ) as mock_cmd:
            await self.pipeline._rebase(None, BuildVariant.OADP)

        cmd = mock_cmd.call_args[0][0]
        self.assertFalse(any(arg.startswith('--images=') for arg in cmd))

    async def test_rebase_all_strategy_returns_excluded_on_failure(self):
        """When all-strategy rebase fails, excluded images are returned."""
        state = {'images:konflux:rebase': {'failed-images': ['img-a']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            result = await self.pipeline._rebase(None, BuildVariant.OADP)

        self.assertEqual(result, ['img-a'])

    async def test_build_all_strategy_no_exclusions(self):
        """When strategy is all with no exclusions, build command has no --images= flag."""
        with (
            patch(
                'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
                new_callable=AsyncMock,
            ) as mock_cmd,
            patch(
                'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
                return_value='/path/to/kubeconfig',
            ),
        ):
            await self.pipeline._build(
                BuildStrategy.ALL, None, [], 'oadp', KONFLUX_DEFAULT_IMAGE_REPO, BuildVariant.OADP
            )

        cmd = mock_cmd.call_args[0][0]
        self.assertFalse(any(arg.startswith('--images=') for arg in cmd))
        self.assertFalse(any(arg.startswith('--exclude=') for arg in cmd))

    def _write_image_build_record_log(self, entries):
        """Write an image_build_konflux record.log with the given (name, nvr) entries, all successful with a bundle."""
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        lines = []
        for name, nvr in entries:
            lines.append(f'image_build_konflux|name={name}|has_olm_bundle=1|status=0|nvrs={nvr}\n')
        record_log_path.write_text(''.join(lines))

    @patch('pyartcd.pipelines.build_layered_products.jenkins.start_olm_bundle_konflux')
    @patch('pyartcd.pipelines.build_layered_products.jenkins.get_propagatable_params', return_value={})
    def test_trigger_bundle_build_filters_embargoed_operators(self, mock_get_params, mock_start_bundle):
        """Embargoed operator NVRs are dropped from the trigger; non-embargoed ones proceed."""
        self.pipeline.skip_bundle_build = False
        self._write_image_build_record_log(
            [
                ('oadp-operator', 'oadp-operator-container-v1.4.9-202607151200.p2.g172d0b2.assembly.stream.el9'),
                ('oadp-operator', 'oadp-operator-container-v1.4.9-202607151201.p3.g8a1f3c4.assembly.stream.el9'),
            ]
        )

        self.pipeline.trigger_bundle_build()

        mock_start_bundle.assert_called_once()
        self.assertEqual(
            mock_start_bundle.call_args.kwargs['operator_nvrs'],
            ['oadp-operator-container-v1.4.9-202607151200.p2.g172d0b2.assembly.stream.el9'],
        )
        self.assertEqual(
            self.pipeline.embargoed_operators_skipped,
            ['oadp-operator-container-v1.4.9-202607151201.p3.g8a1f3c4.assembly.stream.el9'],
        )

    @patch('pyartcd.pipelines.build_layered_products.jenkins.start_olm_bundle_konflux')
    @patch('pyartcd.pipelines.build_layered_products.jenkins.get_propagatable_params', return_value={})
    def test_trigger_bundle_build_ambiguous_nvr_skips_and_warns(self, mock_get_params, mock_start_bundle):
        """An NVR whose embargo status is ambiguous (matches more than one visibility suffix)
        is treated like an embargoed NVR: skip its bundle trigger and record it as skipped,
        rather than letting the ValueError bubble up and get silently swallowed by the
        broader except-Exception handler around this method."""
        self.pipeline.skip_bundle_build = False
        ambiguous_nvr = 'oadp-operator-container-v1.4.9.202607151200.p2.g172d0b2.assembly.stream.el9.p3'
        self._write_image_build_record_log(
            [
                ('oadp-operator', ambiguous_nvr),
                ('oadp-operator', 'oadp-operator-container-v1.4.9-202607151201.p2.g8a1f3c4.assembly.stream.el9'),
            ]
        )

        self.pipeline.trigger_bundle_build()

        mock_start_bundle.assert_called_once()
        self.assertEqual(
            mock_start_bundle.call_args.kwargs['operator_nvrs'],
            ['oadp-operator-container-v1.4.9-202607151201.p2.g8a1f3c4.assembly.stream.el9'],
        )
        self.assertEqual(self.pipeline.embargoed_operators_skipped, [ambiguous_nvr])

    @patch('pyartcd.pipelines.build_layered_products.jenkins.start_olm_bundle_konflux')
    @patch('pyartcd.pipelines.build_layered_products.jenkins.get_propagatable_params', return_value={})
    def test_trigger_bundle_build_all_embargoed_skips_trigger(self, mock_get_params, mock_start_bundle):
        """When every operator NVR is embargoed, the bundle build trigger is never called."""
        self.pipeline.skip_bundle_build = False
        self._write_image_build_record_log(
            [
                ('oadp-operator', 'oadp-operator-container-v1.4.9-202607151200.p3.g172d0b2.assembly.stream.el9'),
            ]
        )

        self.pipeline.trigger_bundle_build()

        mock_start_bundle.assert_not_called()
        self.assertEqual(
            self.pipeline.embargoed_operators_skipped,
            ['oadp-operator-container-v1.4.9-202607151200.p3.g172d0b2.assembly.stream.el9'],
        )

    @patch('pyartcd.pipelines.build_layered_products.jenkins.start_olm_bundle_konflux')
    @patch('pyartcd.pipelines.build_layered_products.jenkins.get_propagatable_params', return_value={})
    def test_trigger_bundle_build_no_embargoed_calls_normally(self, mock_get_params, mock_start_bundle):
        """When no operator NVRs are embargoed, the trigger proceeds normally with an empty skip list."""
        self.pipeline.skip_bundle_build = False
        self._write_image_build_record_log(
            [
                ('oadp-operator', 'oadp-operator-container-v1.4.9-202607151200.p2.g172d0b2.assembly.stream.el9'),
            ]
        )

        self.pipeline.trigger_bundle_build()

        mock_start_bundle.assert_called_once()
        self.assertEqual(
            mock_start_bundle.call_args.kwargs['operator_nvrs'],
            ['oadp-operator-container-v1.4.9-202607151200.p2.g172d0b2.assembly.stream.el9'],
        )
        self.assertEqual(self.pipeline.embargoed_operators_skipped, [])

    @patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.build_layered_products.load_group_config')
    async def test_run_exits_unstable_when_embargoed_operators_skipped(self, mock_load_config, mock_jenkins):
        """run() exits with code 2 (UNSTABLE) when any embargoed operator NVRs were skipped."""
        mock_load_config.return_value = {'product': 'oadp', 'version': '1.4.9'}
        self.pipeline.skip_rebase = True
        self.pipeline.skip_bundle_build = True

        with (
            patch.object(self.pipeline, '_rebase_and_build', new_callable=AsyncMock),
            patch.object(self.pipeline, 'trigger_bundle_build') as mock_trigger,
        ):

            def fake_trigger():
                self.pipeline.embargoed_operators_skipped.append(
                    'oadp-operator-container-v1.4.9-202607151200.p3.g172d0b2.assembly.stream.el9'
                )

            mock_trigger.side_effect = fake_trigger

            with self.assertRaises(SystemExit) as ctx:
                await self.pipeline.run()

        self.assertEqual(ctx.exception.code, 2)

    @patch('pyartcd.pipelines.build_layered_products.jenkins.init_jenkins')
    @patch('pyartcd.pipelines.build_layered_products.load_group_config')
    async def test_run_succeeds_when_no_operators_skipped(self, mock_load_config, mock_jenkins):
        """run() completes normally (no SystemExit) when no operator NVRs were skipped."""
        mock_load_config.return_value = {'product': 'oadp', 'version': '1.4.9'}
        self.pipeline.skip_rebase = True
        self.pipeline.skip_bundle_build = True

        with (
            patch.object(self.pipeline, '_rebase_and_build', new_callable=AsyncMock),
            patch.object(self.pipeline, 'trigger_bundle_build'),
        ):
            await self.pipeline.run()  # Should not raise

    async def test_build_all_strategy_with_exclusions(self):
        """When strategy is all with rebase exclusions, build uses --images= --exclude=."""
        with (
            patch(
                'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
                new_callable=AsyncMock,
            ) as mock_cmd,
            patch(
                'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
                return_value='/path/to/kubeconfig',
            ),
        ):
            await self.pipeline._build(
                BuildStrategy.ALL,
                None,
                ['img-a', 'img-b'],
                'oadp',
                KONFLUX_DEFAULT_IMAGE_REPO,
                BuildVariant.OADP,
            )

        cmd = mock_cmd.call_args[0][0]
        self.assertIn('--images=', cmd)
        self.assertIn('--exclude=img-a,img-b', cmd)

    async def test_build_passes_independent_verification_options(self):
        self.pipeline.skip_ec_verify = True
        self.pipeline.skip_custom_its = True
        with (
            patch(
                'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
                new_callable=AsyncMock,
            ) as mock_cmd,
            patch(
                'pyartcd.pipelines.build_layered_products.resolve_konflux_kubeconfig_by_product',
                return_value='/path/to/kubeconfig',
            ),
        ):
            await self.pipeline._build(BuildStrategy.ONLY, 'img-a', [], 'oadp', KONFLUX_DEFAULT_IMAGE_REPO)

        cmd = mock_cmd.call_args.args[0]
        self.assertIn('--skip-ec-verify', cmd)
        self.assertIn('--skip-custom-its', cmd)

    @patch('pyartcd.pipelines.build_layered_products.increment_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_layered_products.reset_fail_counter', new_callable=AsyncMock)
    async def test_layered_build_failure_counters(self, mock_reset, mock_increment):
        record_log = {
            'image_build_konflux': [
                {'name': 'ok', 'status': '0'},
                {
                    'name': 'build-fail',
                    'status': '1',
                    'task_id': 'build-plr',
                    'outcome': 'build_error',
                    'build_pipeline_url': 'https://example/build',
                },
                {
                    'name': 'its-fail',
                    'status': '1',
                    'task_id': 'build-plr',
                    'outcome': str(KonfluxBuildOutcome.ITS_ERROR),
                    'ec_pipeline_url': 'https://example/its',
                },
                {
                    'name': 'release-fail',
                    'status': '1',
                    'task_id': 'build-plr',
                    'outcome': str(KonfluxBuildOutcome.RELEASE_ERROR),
                    'release_pipeline': 'https://example/release',
                },
                {'name': 'infra', 'status': '1', 'task_id': 'n/a', 'outcome': 'failure'},
                {
                    'name': 'child',
                    'status': '1',
                    'task_id': 'n/a',
                    'message': "Couldn't build child because parent images failed to build",
                },
            ]
        }
        with patch.object(self.pipeline, 'parse_record_log', return_value=record_log):
            await self.pipeline._update_build_fail_counters()

        reset_keys = {call.args[0] for call in mock_reset.await_args_list}
        self.assertEqual(
            reset_keys,
            {
                'count:build-failure:konflux:oadp-1.4:ok',
                'count:ec-failure:konflux:oadp-1.4:ok',
                'count:release-failure:konflux:oadp-1.4:ok',
            },
        )
        increment_keys = {call.args[0] for call in mock_increment.await_args_list}
        self.assertEqual(
            increment_keys,
            {
                'count:build-failure:konflux:oadp-1.4:build-fail',
                'count:ec-failure:konflux:oadp-1.4:its-fail',
                'count:release-failure:konflux:oadp-1.4:release-fail',
            },
        )

    @patch('pyartcd.pipelines.build_layered_products.increment_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_layered_products.reset_fail_counter', new_callable=AsyncMock)
    async def test_failure_counters_skip_non_stream_assemblies(self, mock_reset, mock_increment):
        self.pipeline.assembly = 'test'
        await self.pipeline._update_build_fail_counters()
        mock_reset.assert_not_awaited()
        mock_increment.assert_not_awaited()

    @patch('pyartcd.pipelines.build_layered_products.increment_fail_counter', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.build_layered_products.reset_fail_counter', new_callable=AsyncMock)
    async def test_failure_counters_skip_dry_run(self, mock_reset, mock_increment):
        self.runtime.dry_run = True
        with patch.object(self.pipeline, 'parse_record_log') as mock_parse_record_log:
            await self.pipeline._update_build_fail_counters()

        mock_parse_record_log.assert_not_called()
        mock_reset.assert_not_awaited()
        mock_increment.assert_not_awaited()
