import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from artcommonlib.gitdata import DataObj
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib import Runtime
from doozerlib.backend.base_image_handler import BaseImageReleaseResult
from doozerlib.cli.images import images_show_ancestors, release_to_base_repo
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata


def _invoke_release_to_base_repo_inner(runtime, nvr: str, enabled_override: bool = False):
    """Call the bare async handler (avoid Click/pass_runtime context in unit tests)."""
    fn = release_to_base_repo.callback
    while hasattr(fn, '__wrapped__'):
        fn = fn.__wrapped__
    return asyncio.run(fn(runtime, nvr, enabled_override))


class TestImagesCli(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.patchers = [
            patch('doozerlib.metadata.ImageDistGitRepo'),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self):
        for p in self.patchers:
            p.stop()

    def test_show_ancestors(self):
        runtime = Mock(spec=Runtime)
        runtime.assembly = 'test'
        runtime.group_config = MagicMock()
        runtime.assembly_basis_event = None
        runtime.late_resolve_image.return_value = None
        runtime.image_map = MagicMock()
        runtime.variant = BuildVariant.OCP  # Add variant attribute
        image_map = {}

        # Image definitions
        image_defs = {
            'grandparent': {'name': 'grandparent-image', 'parent': None, 'builders': []},
            'parent': {'name': 'parent-image', 'parent': 'grandparent', 'builders': []},
            'builder-image': {
                'name': 'builder-image',
                'parent': 'grandparent',
                'builders': [],
                'provides_stream': 'my-stream',
            },
            'child': {'name': 'child-image', 'parent': 'parent', 'builders': ['builder-image', 'my-stream']},
        }

        # Create ImageMetadata objects
        for key, value in image_defs.items():
            data = {'name': value['name']}
            if value.get('provides_stream'):
                data['provides'] = {'stream': value['provides_stream']}

            from_data = {}
            if value.get('builders'):
                from_data['builder'] = []
                for b in value['builders']:
                    if b == 'builder-image':
                        from_data['builder'].append({'member': 'builder-image'})
                    else:
                        from_data['builder'].append({'stream': b})

            # Set parent, which can be from.member or from.image
            if value['parent']:
                # In our test, all parents are members
                from_data['member'] = value['parent']

            if from_data:
                data['from'] = from_data

            data_obj = DataObj(key, f'path/to/{key}.yml', data)
            meta = ImageMetadata(runtime, data_obj, clone_source=False)
            meta.distgit_key = key
            image_map[key] = meta

        # Set up parent relationships and runtime mocks
        for key, meta in image_map.items():
            parent_key = image_defs[key]['parent']
            if parent_key:
                meta.parent = image_map[parent_key]

        runtime.image_map.get.side_effect = image_map.get
        runtime.image_metas.return_value = image_map.values()
        runtime.initialize.return_value = None

        # Test with a child image that has a direct parent and builder images
        result = self.runner.invoke(images_show_ancestors, ['--image-names', 'child'], obj=runtime)
        self.assertEqual(result.exit_code, 0)
        # Expecting parent, grandparent (from parent), and builder-image (and its parent, grandparent)
        self.assertEqual(result.output.strip(), 'builder-image,grandparent,parent')

        # Test with multiple images
        result = self.runner.invoke(images_show_ancestors, ['--image-names', 'child,parent'], obj=runtime)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output.strip(), 'builder-image,grandparent,parent')

        # Test with an image with no parent
        result = self.runner.invoke(images_show_ancestors, ['--image-names', 'grandparent'], obj=runtime)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.output.strip(), '')

        # Test with a non-existent image
        result = self.runner.invoke(images_show_ancestors, ['--image-names', 'non-existent'], obj=runtime)
        self.assertIsInstance(result.exception, DoozerFatalError)
        self.assertIn("Image 'non-existent' not found in group.", str(result.exception))


class TestReleaseToBaseRepo(unittest.TestCase):
    def _runtime_with_base_image(self):
        runtime = MagicMock()
        runtime.group = "openshift-4.22"
        runtime.product = "ocp"
        runtime.variant = BuildVariant.OCP
        runtime.assembly = "stream"
        image_model = Model(
            {
                "name": "ose-base",
                "base_only": True,
                "base_image_release": {"enabled": True},
                "distgit": {"component": "ose-base-container"},
            }
        )
        data = Model({"key": "openshift-enterprise-base-rhel9", "data": image_model, "filename": "base.yaml"})
        md = ImageMetadata(runtime, data)
        md.distgit_key = "openshift-enterprise-base-rhel9"
        runtime.image_map = {md.distgit_key: md}
        return runtime

    def test_inserts_followup_row_after_snapshot_release(self):
        runtime = self._runtime_with_base_image()
        nvr = "ose-base-container-v4.22-1.el9"
        source = KonfluxBuildRecord(
            name="openshift-enterprise-base-rhel9",
            group=runtime.group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            engine=Engine.KONFLUX,
            record_id="prior-id",
            build_id="shared-bid",
            image_pullspec="quay.io/base@sha256:abc",
            rebase_repo_url="https://git.example/r.git",
            rebase_commitish="deadbeef",
        )

        kb = MagicMock()
        kb.get_build_record_by_nvr = AsyncMock(return_value=source)
        kb.bind = MagicMock()

        captured = []

        async def capture_builds(builds):
            captured.extend(builds)

        kb.add_builds = AsyncMock(side_effect=capture_builds)

        runtime.konflux_db = kb
        runtime.initialize = MagicMock()

        release_out = BaseImageReleaseResult(
            release_name="r1",
            snapshot_name="s1",
            nvr=nvr,
            release_pipeline="https://pipeline",
            released_pullspec="registry.example/img:tag",
        )

        with patch("doozerlib.cli.images.BaseImageHandler") as bh_cls:

            async def snap_release(inp, enabled_override=False):
                self.assertFalse(inp.is_golang_builder)
                self.assertEqual(inp.distgit_key, source.name)
                self.assertEqual(inp.container_image, source.image_pullspec)
                self.assertEqual(inp.rebase_repo_url, source.rebase_repo_url)
                self.assertEqual(inp.rebase_commitish, source.rebase_commitish)
                return release_out

            bh_cls.return_value.snapshot_release = snap_release

            _invoke_release_to_base_repo_inner(runtime, nvr)

        kb.get_build_record_by_nvr.assert_awaited_once()
        self.assertEqual(len(captured), 1)
        followup = captured[0]
        self.assertEqual(followup.build_id, "shared-bid")
        self.assertEqual(followup.release_pipeline, release_out.release_pipeline)
        self.assertEqual(followup.released_pullspec, release_out.released_pullspec)
        self.assertNotEqual(followup.record_id, "prior-id")

    def test_golang_builder_snapshot_input_openshift_slash_builder_name(self):
        """Regression: openshift/golang-builder in metadata marks golang (ART-18934)."""
        runtime = MagicMock()
        runtime.group = "rhel-9-golang-1.23"
        runtime.product = "ocp"
        runtime.variant = BuildVariant.OCP
        runtime.assembly = "stream"
        image_model = Model(
            {
                "name": "openshift/golang-builder",
                "base_image_release": {"enabled": True},
                "distgit": {"component": "openshift-golang-builder-container"},
            }
        )
        data = Model({"key": "openshift-golang-builder", "data": image_model, "filename": "golang.yaml"})
        md = ImageMetadata(runtime, data)
        md.distgit_key = "openshift-golang-builder"
        runtime.image_map = {"openshift-golang-builder": md}

        nvr = "openshift-golang-builder-container-v1.23.10-1.el9"
        source = KonfluxBuildRecord(
            name="openshift-golang-builder",
            group=runtime.group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            engine=Engine.KONFLUX,
            image_pullspec="quay.io/g@sha256:x",
            rebase_repo_url="",
            rebase_commitish="",
        )
        kb = MagicMock()
        kb.get_build_record_by_nvr = AsyncMock(return_value=source)
        kb.bind = MagicMock()
        kb.add_builds = AsyncMock()
        runtime.konflux_db = kb
        runtime.initialize = MagicMock()

        release_out = BaseImageReleaseResult(
            release_name="r1",
            snapshot_name="s1",
            nvr=nvr,
            release_pipeline="https://p",
            released_pullspec="registry.io/i:t",
        )

        with patch("doozerlib.cli.images.BaseImageHandler") as bh_cls:

            async def snap_release(inp, enabled_override=False):
                self.assertTrue(inp.is_golang_builder)
                return release_out

            bh_cls.return_value.snapshot_release = snap_release

            _invoke_release_to_base_repo_inner(runtime, nvr)

        kb.add_builds.assert_awaited_once()

    def test_enabled_override_flag_passed_to_snapshot_release(self):
        runtime = self._runtime_with_base_image()
        nvr = "ose-base-container-v4.22-1.el9"
        source = KonfluxBuildRecord(
            name="openshift-enterprise-base-rhel9",
            group=runtime.group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            engine=Engine.KONFLUX,
            image_pullspec="quay.io/base@sha256:abc",
            rebase_repo_url="https://git.example/r.git",
            rebase_commitish="deadbeef",
        )

        kb = MagicMock()
        kb.get_build_record_by_nvr = AsyncMock(return_value=source)
        kb.bind = MagicMock()
        kb.add_builds = AsyncMock()
        runtime.konflux_db = kb
        runtime.initialize = MagicMock()

        release_out = BaseImageReleaseResult(
            release_name="r1",
            snapshot_name="s1",
            nvr=nvr,
            release_pipeline="https://pipeline",
            released_pullspec="registry.example/img:tag",
        )

        with patch("doozerlib.cli.images.BaseImageHandler") as bh_cls:
            received_enabled_override = []

            async def snap_release(inp, enabled_override=False):
                received_enabled_override.append(enabled_override)
                return release_out

            bh_cls.return_value.snapshot_release = snap_release

            _invoke_release_to_base_repo_inner(runtime, nvr, enabled_override=True)

        self.assertEqual(received_enabled_override, [True])

    def _konflux_db_for_source(self, runtime, source):
        kb = MagicMock()
        kb.get_build_record_by_nvr = AsyncMock(return_value=source)
        kb.bind = MagicMock()
        kb.add_builds = AsyncMock()
        runtime.konflux_db = kb
        runtime.initialize = MagicMock()
        return kb

    def test_raises_when_base_image_release_disabled_without_override(self):
        runtime = self._runtime_with_base_image()
        base_md = runtime.image_map["openshift-enterprise-base-rhel9"]
        base_md.config.base_image_release.enabled = False

        nvr = "ose-base-container-v4.22-1.el9"
        source = KonfluxBuildRecord(
            name="openshift-enterprise-base-rhel9",
            group=runtime.group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            engine=Engine.KONFLUX,
            image_pullspec="quay.io/base@sha256:abc",
            rebase_repo_url="https://git.example/r.git",
            rebase_commitish="deadbeef",
        )
        self._konflux_db_for_source(runtime, source)

        with patch("doozerlib.cli.images.BaseImageHandler") as bh_cls:
            with self.assertRaises(DoozerFatalError) as ctx:
                _invoke_release_to_base_repo_inner(runtime, nvr)
            self.assertIn("base_image_release.enabled is false", str(ctx.exception))
            self.assertIn("--enabled-override", str(ctx.exception))
            bh_cls.assert_not_called()

    def test_raises_on_test_assembly_even_with_enabled_override(self):
        runtime = self._runtime_with_base_image()
        runtime.assembly = "test"
        nvr = "ose-base-container-v4.22-1.el9"
        source = KonfluxBuildRecord(
            name="openshift-enterprise-base-rhel9",
            group=runtime.group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            engine=Engine.KONFLUX,
            image_pullspec="quay.io/base@sha256:abc",
            rebase_repo_url="https://git.example/r.git",
            rebase_commitish="deadbeef",
        )
        self._konflux_db_for_source(runtime, source)

        with patch("doozerlib.cli.images.BaseImageHandler") as bh_cls:
            with self.assertRaises(DoozerFatalError) as ctx:
                _invoke_release_to_base_repo_inner(runtime, nvr, enabled_override=True)
            self.assertIn("test assembly is excluded", str(ctx.exception))
            bh_cls.assert_not_called()

    def test_raises_when_snapshot_release_returns_none(self):
        runtime = self._runtime_with_base_image()
        nvr = "ose-base-container-v4.22-1.el9"
        source = KonfluxBuildRecord(
            name="openshift-enterprise-base-rhel9",
            group=runtime.group,
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            engine=Engine.KONFLUX,
            image_pullspec="quay.io/base@sha256:abc",
            rebase_repo_url="https://git.example/r.git",
            rebase_commitish="deadbeef",
        )
        self._konflux_db_for_source(runtime, source)

        with patch("doozerlib.cli.images.BaseImageHandler") as bh_cls:
            bh_cls.return_value.snapshot_release = AsyncMock(return_value=None)

            with self.assertRaises(DoozerFatalError) as ctx:
                _invoke_release_to_base_repo_inner(runtime, nvr)

            self.assertIn("snapshot-release did not complete", str(ctx.exception))
            self.assertIn(nvr, str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
