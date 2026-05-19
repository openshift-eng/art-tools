import unittest
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME
from artcommonlib.gitdata import DataObj
from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib import Runtime
from doozerlib.cli import images as images_cli
from doozerlib.cli.images import images_show_ancestors
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata


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


class TestSnapshotInputFromKonfluxSuccessBuild(IsolatedAsyncioTestCase):
    def _base_runtime_and_md(self):
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
        return runtime, md

    async def test_builds_inputs_from_success_rows(self):
        runtime, md = self._base_runtime_and_md()

        record = MagicMock()
        record.nvr = "ose-base-container-v4.22-1.el9"
        record.name = "openshift-enterprise-base-rhel9"
        record.image_pullspec = "quay.io/x@sha256:abc"
        record.rebase_repo_url = "https://git.example/r.git"
        record.rebase_commitish = "deadbeef"

        runtime.konflux_db = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=record)
        runtime.image_map = {record.name: md}

        inp = await images_cli._snapshot_input_from_konflux_success_build(runtime, record.nvr)

        self.assertIsNotNone(inp)
        assert inp is not None
        self.assertEqual(inp.nvr, record.nvr)
        self.assertEqual(inp.distgit_key, record.name)
        self.assertEqual(inp.container_image, record.image_pullspec)
        self.assertEqual(inp.rebase_repo_url, record.rebase_repo_url)
        self.assertFalse(inp.is_golang_builder)

    async def test_skips_unknown_nvr(self):
        runtime, _ = self._base_runtime_and_md()

        runtime.konflux_db = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=None)

        inp = await images_cli._snapshot_input_from_konflux_success_build(runtime, "nosuch-container-v1-1.el9")
        self.assertIsNone(inp)

    async def test_golang_builder_flag_from_metadata(self):
        runtime = MagicMock()
        runtime.group = "openshift-4.22"
        runtime.product = "ocp"
        runtime.variant = BuildVariant.OCP
        runtime.assembly = "stream"

        image_model = Model(
            {
                "name": GOLANG_BUILDER_IMAGE_NAME,
                "base_image_release": {"enabled": True},
                "distgit": {"component": "ose-golang-builder-container"},
            }
        )
        data = Model({"key": "openshift-golang-builder", "data": image_model, "filename": "golang.yaml"})
        md = ImageMetadata(runtime, data)
        md.distgit_key = "openshift-golang-builder"

        record = MagicMock()
        record.nvr = "openshift-golang-builder-container-v1-1.el9"
        record.name = "openshift-golang-builder"
        record.image_pullspec = "quay.io/golang@sha256:x"
        record.rebase_repo_url = ""
        record.rebase_commitish = ""

        runtime.konflux_db = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=record)
        runtime.image_map = {record.name: md}

        inp = await images_cli._snapshot_input_from_konflux_success_build(runtime, record.nvr)
        self.assertIsNotNone(inp)
        assert inp is not None
        self.assertTrue(inp.is_golang_builder)

    async def test_golang_builder_flag_slash_name_in_metadata(self):
        """config.name openshift/golang-builder must match ImageMetadata.is_golang_builder (ART-18934)."""
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

        record = MagicMock()
        record.nvr = "openshift-golang-builder-container-v1.23.10-1.el9"
        record.name = "openshift-golang-builder"
        record.image_pullspec = "quay.io/golang@sha256:x"
        record.rebase_repo_url = ""
        record.rebase_commitish = ""

        runtime.konflux_db = MagicMock()
        runtime.konflux_db.get_latest_build = AsyncMock(return_value=record)
        runtime.image_map = {record.name: md}

        inp = await images_cli._snapshot_input_from_konflux_success_build(runtime, record.nvr)
        self.assertIsNotNone(inp)
        assert inp is not None
        self.assertTrue(inp.is_golang_builder)


if __name__ == '__main__':
    unittest.main()
