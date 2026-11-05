import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.constants import RHCOS_IMAGE_REPO
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.model import Model
from doozerlib.backend.konflux_post_build import (
    POST_BUILD_CALLBACKS,
    RhcosNodeImagePostBuild,
    build_post_build_callbacks,
    register_post_build,
)


def _pullspec(digit):
    return f'{RHCOS_IMAGE_REPO}@sha256:{digit * 64}'


class TestRhcosNodeImagePostBuild(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = 'openshift-4.23'
        self.runtime.assembly = 'stream'
        self.runtime.group_config = Model(
            {
                'rhcos': {'payload_tags': [{'rhel_version': '9.8'}, {'rhel_version': '10.2'}]},
                'vars': {'RHCOS_EL_MAJOR': '9', 'RHCOS_EL_MINOR': '8'},
            }
        )
        self.runtime.konflux_db.get_latest_build = AsyncMock()
        self.node = SimpleNamespace(distgit_key='rhcos-node-image')
        self.extensions = SimpleNamespace(distgit_key='rhcos-node-extensions')

    @patch('doozerlib.backend.konflux_post_build.jenkins.update_description')
    @patch('doozerlib.backend.konflux_post_build.jenkins.start_build')
    async def test_rebuilt_pair_runs_once_with_extensions_owning_trigger(self, start_build, _update_description):
        start_build.return_value = ('SUCCESS', 'https://jenkins.example/post-build/1')
        callback = RhcosNodeImagePostBuild(self.runtime, {self.node.distgit_key, self.extensions.distgit_key})

        self.assertTrue(callback.allows_dependents_before_completion(self.node))
        # When both members are built together, the node only publishes its pullspec and
        # defers: it is not gated on the pair post-build job, so it returns immediately.
        self.assertEqual(await callback.run(self.node, _pullspec('a')), '')
        self.runtime.konflux_db.get_latest_build.assert_not_awaited()

        # The extensions image owns the single pair trigger and returns the job URL.
        self.assertEqual(
            await callback.run(self.extensions, _pullspec('b')),
            'https://jenkins.example/post-build/1',
        )
        start_build.assert_called_once()
        job, params = start_build.call_args.args
        self.assertEqual(job.value, 'aos-cd-builds/build%2Frhcos-node-image-post-build')
        self.assertEqual(params['RELEASE'], '4.23-9.8')
        self.assertEqual(params['NODE_IMAGE'], _pullspec('a'))
        self.assertEqual(params['EXTENSIONS_IMAGE'], _pullspec('b'))

    @patch('doozerlib.backend.konflux_post_build.jenkins.start_build')
    async def test_failed_pair_job_fails_the_owning_extensions(self, start_build):
        start_build.return_value = ('FAILURE', 'https://jenkins.example/post-build/2')
        callback = RhcosNodeImagePostBuild(self.runtime, {self.node.distgit_key, self.extensions.distgit_key})

        # The node defers and is not gated on the pair job outcome.
        self.assertEqual(await callback.run(self.node, _pullspec('a')), '')
        # The extensions owns the trigger, so it surfaces the pair job failure.
        with self.assertRaisesRegex(RuntimeError, 'result=FAILURE'):
            await callback.run(self.extensions, _pullspec('b'))

        self.assertEqual(start_build.call_count, 1)

    async def test_failed_partner_build_fails_the_owner(self):
        callback = RhcosNodeImagePostBuild(self.runtime, {self.node.distgit_key, self.extensions.distgit_key})
        # The node image failed to build and never published a pullspec.
        callback.build_failed(self.node)

        # The extensions owns the pair trigger and cannot run without its rebuilt partner.
        with self.assertRaisesRegex(RuntimeError, 'rhcos-node-image did not build successfully'):
            await callback.run(self.extensions, _pullspec('b'))

    @patch('doozerlib.backend.konflux_post_build.jenkins.update_description')
    @patch('doozerlib.backend.konflux_post_build.jenkins.start_build')
    async def test_single_image_uses_latest_successful_partner(self, start_build, _update_description):
        start_build.return_value = ('SUCCESS', 'https://jenkins.example/post-build/3')
        self.runtime.konflux_db.get_latest_build.return_value = SimpleNamespace(image_pullspec=_pullspec('b'))
        callback = RhcosNodeImagePostBuild(self.runtime, {self.node.distgit_key})

        await callback.run(self.node, _pullspec('a'))

        self.runtime.konflux_db.get_latest_build.assert_awaited_once_with(
            name=self.extensions.distgit_key,
            group='openshift-4.23',
            outcome=KonfluxBuildOutcome.SUCCESS,
            assembly='stream',
            exclude_large_columns=True,
        )
        self.assertEqual(start_build.call_args.args[1]['EXTENSIONS_IMAGE'], _pullspec('b'))

    @patch('doozerlib.backend.konflux_post_build.jenkins.update_description')
    @patch('doozerlib.backend.konflux_post_build.jenkins.start_build')
    async def test_rhel10_pair_uses_its_release_stream(self, start_build, _update_description):
        start_build.return_value = ('SUCCESS', 'https://jenkins.example/post-build/4')
        node = SimpleNamespace(distgit_key='rhcos-node-image-rhel10')
        extensions = SimpleNamespace(distgit_key='rhcos-node-extensions-rhel10')
        callback = RhcosNodeImagePostBuild(self.runtime, {node.distgit_key, extensions.distgit_key})

        await asyncio.gather(
            callback.run(node, _pullspec('c')),
            callback.run(extensions, _pullspec('d')),
        )

        self.assertEqual(start_build.call_args.args[1]['RELEASE'], '4.23-10.2')

    @patch('doozerlib.backend.konflux_post_build.jenkins.start_build')
    async def test_non_stream_assembly_skips_post_build(self, start_build):
        self.runtime.assembly = 'test'
        callback = RhcosNodeImagePostBuild(self.runtime, {self.node.distgit_key})

        self.assertFalse(callback.allows_dependents_before_completion(self.node))
        self.assertEqual(await callback.run(self.node, _pullspec('a')), '')
        start_build.assert_not_called()


class TestPostBuildRegistry(unittest.IsolatedAsyncioTestCase):
    """Coverage for the callback registry, the generic factory, and ``create``."""

    @staticmethod
    def _meta(distgit_key, post_build=None):
        return SimpleNamespace(
            distgit_key=distgit_key,
            config=Model({'konflux': {'post_build': post_build}}),
        )

    def _runtime(self):
        runtime = MagicMock()
        runtime.assembly = 'stream'
        return runtime

    def test_rhcos_callback_is_registered(self):
        self.assertIs(POST_BUILD_CALLBACKS['rhcos_node_image'], RhcosNodeImagePostBuild)

    def test_register_post_build_registers_and_returns_class(self):
        @register_post_build('temp_test_callback')
        class _Temp:
            pass

        try:
            self.assertIs(POST_BUILD_CALLBACKS['temp_test_callback'], _Temp)
        finally:
            POST_BUILD_CALLBACKS.pop('temp_test_callback', None)

    def test_unknown_callback_name_raises(self):
        metas = [self._meta('some-image', 'does_not_exist')]
        with self.assertRaisesRegex(ValueError, r"Unknown konflux.post_build callback 'does_not_exist' for some-image"):
            build_post_build_callbacks(self._runtime(), metas, skip=False)

    def test_no_post_build_config_returns_empty(self):
        metas = [self._meta('a'), self._meta('b')]
        self.assertEqual(build_post_build_callbacks(self._runtime(), metas, skip=False), {})

    def test_unsupported_image_for_rhcos_callback_raises(self):
        metas = [self._meta('random-image', 'rhcos_node_image')]
        with self.assertRaisesRegex(ValueError, 'rhcos_node_image post-build callback does not support random-image'):
            build_post_build_callbacks(self._runtime(), metas, skip=False)

    def test_both_pair_members_must_configure(self):
        # Both pair members selected, but only the node configures the callback.
        metas = [
            self._meta('rhcos-node-image', 'rhcos_node_image'),
            self._meta('rhcos-node-extensions'),
        ]
        with self.assertRaisesRegex(
            ValueError, 'Both rhcos-node-image and rhcos-node-extensions must configure konflux.post_build'
        ):
            build_post_build_callbacks(self._runtime(), metas, skip=False)

    async def test_single_pair_member_selected_is_allowed(self):
        # Only the node is selected; the extensions is not, so no pairing error.
        metas = [self._meta('rhcos-node-image', 'rhcos_node_image')]
        callbacks = build_post_build_callbacks(self._runtime(), metas, skip=False)
        self.assertEqual(callbacks['rhcos_node_image'].selected_images, {'rhcos-node-image'})

    async def test_build_post_build_callbacks_constructs_rhcos(self):
        metas = [
            self._meta('rhcos-node-image', 'rhcos_node_image'),
            self._meta('rhcos-node-extensions', 'rhcos_node_image'),
            self._meta('plain-image'),
        ]
        callbacks = build_post_build_callbacks(self._runtime(), metas, skip=False)

        self.assertEqual(set(callbacks), {'rhcos_node_image'})
        callback = callbacks['rhcos_node_image']
        self.assertIsInstance(callback, RhcosNodeImagePostBuild)
        self.assertEqual(callback.selected_images, {'rhcos-node-image', 'rhcos-node-extensions'})
        self.assertFalse(callback.skip)

    async def test_create_passes_skip_through(self):
        node = self._meta('rhcos-node-image', 'rhcos_node_image')
        extensions = self._meta('rhcos-node-extensions', 'rhcos_node_image')
        callback = RhcosNodeImagePostBuild.create(self._runtime(), [node, extensions], [node, extensions], skip=True)
        self.assertEqual(callback.selected_images, {'rhcos-node-image', 'rhcos-node-extensions'})
        self.assertTrue(callback.skip)
