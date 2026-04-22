import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import yaml
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

    async def test_rebase_success_returns_original_image_list(self):
        """When rebase succeeds, the full image list is returned unchanged."""
        with patch('pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async', new_callable=AsyncMock):
            result = await self.pipeline._rebase('img-a,img-b')
        self.assertEqual(result, 'img-a,img-b')

    async def test_rebase_failure_excludes_failed_images(self):
        """When rebase fails and state.yaml records failed images, those images are excluded."""
        state = {'images:konflux:rebase': {'failed-images': ['oadp-operator']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            result = await self.pipeline._rebase('oadp-velero-restic-restore-helper,oadp-operator')

        self.assertEqual(result, 'oadp-velero-restic-restore-helper')

    async def test_rebase_failure_excludes_skipped_due_to_parent_images(self):
        """Images listed only as skipped due to parent rebase failure are excluded from the build list."""
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
            result = await self.pipeline._rebase('parent-image,child-image,other-image')

        self.assertEqual(result, 'other-image')

    async def test_rebase_failure_all_images_failed(self):
        """When all requested images fail rebase, an empty string is returned."""
        state = {'images:konflux:rebase': {'failed-images': ['img-a', 'img-b']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            result = await self.pipeline._rebase('img-a,img-b')

        self.assertEqual(result, '')

    async def test_rebase_failure_no_state_file_reraises(self):
        """When rebase fails but state.yaml does not exist, the error is re-raised."""
        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            with self.assertRaises(ChildProcessError):
                await self.pipeline._rebase('img-a')

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
                await self.pipeline._rebase('img-a')

    async def test_rebase_and_build_skips_build_when_no_images_remain(self):
        """When all images fail rebase, the build step is skipped entirely."""
        state = {'images:konflux:rebase': {'failed-images': ['oadp-velero-restic-restore-helper']}}
        state_path = Path(self.runtime.doozer_working, 'state.yaml')
        with state_path.open('w') as f:
            yaml.safe_dump(state, f)

        with patch(
            'pyartcd.pipelines.build_layered_products.exectools.cmd_assert_async',
            new_callable=AsyncMock,
            side_effect=ChildProcessError('exit code 1'),
        ):
            await self.pipeline._rebase_and_build('oadp')

        self.pipeline._logger.warning.assert_any_call('No buildable images remaining after rebase; skipping build')

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
        ):
            await self.pipeline._rebase_and_build('oadp')

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
        ):
            await self.pipeline._rebase_and_build('oadp')

        mock_cmd.assert_called_once()
        cmd = mock_cmd.call_args[0][0]
        self.assertIn(f'--images={self.pipeline.image_list}', cmd)
        self.assertIn('beta:images:konflux:build', cmd)
