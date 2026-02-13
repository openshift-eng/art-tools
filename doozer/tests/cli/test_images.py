import unittest
from unittest.mock import MagicMock, Mock, patch

from artcommonlib.gitdata import DataObj
from click.testing import CliRunner
from doozerlib import Runtime
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


if __name__ == '__main__':
    unittest.main()
