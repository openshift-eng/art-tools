import json
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.opm import (
    OpmRegistryAuth,
    generate_basic_template,
    generate_dockerfile,
    iter_json_objects,
    render,
    render_catalog_from_template,
    render_packages,
    verify_opm,
)


class TestOpm(unittest.IsolatedAsyncioTestCase):
    @patch('doozerlib.opm.gather_opm', new_callable=AsyncMock)
    async def test_verify_opm(self, mock_gather_opm):
        mock_gather_opm.return_value = (0, 'OpmVersion:"v1.47.0"', '')
        await verify_opm()
        mock_gather_opm.assert_called_once_with(['version'])

        mock_gather_opm.return_value = (0, 'OpmVersion:"v1.46.0"', '')
        with self.assertRaises(IOError, msg="opm version 1.46.0 is too old. Please upgrade to at least 1.47.0."):
            await verify_opm()

        mock_gather_opm.side_effect = FileNotFoundError
        with self.assertRaises(FileNotFoundError, msg="opm binary not found. Please install opm."):
            await verify_opm()

    @patch('doozerlib.opm.gather_opm', new_callable=AsyncMock)
    async def test_render(self, mock_gather_opm):
        mock_gather_opm.return_value = (0, '---\nkey: value\n', '')
        auth = MagicMock()
        blobs = await render('test-catalog', auth=auth)
        self.assertEqual(list(blobs), [{'key': 'value'}])
        mock_gather_opm.assert_called_once_with(
            ['render', '--migrate-level', 'none', '-o', 'yaml', '--', 'test-catalog'], auth=auth, check=False
        )

    @patch("builtins.open")
    @patch('doozerlib.opm.gather_opm', new_callable=AsyncMock)
    async def test_generate_basic_template(self, mock_gather_opm, mock_open):
        catalog_file = Path('/path/to/catalog.yaml')
        template_file = Path('/path/to/template.yaml')
        await generate_basic_template(catalog_file, template_file)
        mock_gather_opm.assert_called_once_with(
            [
                'alpha',
                'convert-template',
                'basic',
                '-o',
                'yaml',
                '--',
                str(catalog_file),
            ],
            stdout=mock_open.return_value.__enter__.return_value,
        )

    @patch("builtins.open")
    @patch('doozerlib.opm.gather_opm', new_callable=AsyncMock)
    async def test_render_catalog_from_template(self, mock_gather_opm, mock_open):
        template_file = Path('/path/to/template.yaml')
        catalog_file = Path('/path/to/catalog.yaml')
        auth = MagicMock(spec=OpmRegistryAuth)
        await render_catalog_from_template(template_file, catalog_file, auth=auth)
        mock_gather_opm.assert_called_once_with(
            [
                'alpha',
                'render-template',
                'basic',
                '--migrate-level',
                'none',
                '-o',
                'yaml',
                '--',
                str(template_file),
            ],
            stdout=mock_open.return_value.__enter__.return_value,
            auth=auth,
        )

        with self.assertRaises(ValueError, msg="Invalid migrate level: invalid"):
            await render_catalog_from_template(template_file, catalog_file, migrate_level='invalid')

    @patch('doozerlib.opm.gather_opm', new_callable=AsyncMock)
    async def test_generate_dockerfile(self, mock_gather_opm):
        dest_dir = Path('/path/to/dest')
        dc_dir_name = 'dc-dir'
        base_image = 'base-image'
        builder_image = 'builder-image'
        await generate_dockerfile(dest_dir, dc_dir_name, base_image, builder_image)
        mock_gather_opm.assert_called_once_with(
            ['generate', 'dockerfile', '--builder-image', builder_image, '--base-image', base_image, '--', dc_dir_name],
            cwd=dest_dir,
        )


class TestStreamedCatalog(unittest.IsolatedAsyncioTestCase):
    async def test_preserves_full_selected_package_blobs_and_ignores_unrelated_packages(self):
        selected = [
            {'schema': 'olm.package', 'name': 'quay', 'defaultChannel': 'stable'},
            {
                'schema': 'olm.channel',
                'package': 'quay',
                'name': 'stable',
                'entries': [{'name': 'quay.v1'}, {'name': 'quay.v2', 'replaces': 'quay.v1'}],
            },
            {
                'schema': 'olm.bundle',
                'package': 'quay',
                'name': 'quay.v1',
                'properties': [{'type': 'olm.csv.metadata', 'value': {'description': 'full metadata'}}],
                'relatedImages': [{'name': 'operand', 'image': 'image@sha256:abc'}],
            },
            {'schema': 'olm.bundle', 'package': 'quay', 'name': 'quay.v2'},
            {'schema': 'olm.deprecations', 'package': 'quay', 'entries': [{'reference': {'name': 'quay.v1'}}]},
        ]
        blobs = [selected[0], {'schema': 'olm.package', 'name': 'other'}, *selected[1:]]

        async def render_to_file(args, *, stdout, **kwargs):
            self.assertEqual(args, ['render', '--migrate-level', 'none', '-o', 'json', '--', 'index@sha256:abc'])
            self.assertEqual(kwargs['env']['GOGC'], '20')
            for blob in blobs:
                stdout.write(json.dumps(blob) + '\n')
            return 0, '', ''

        with (
            patch.dict('os.environ', {}, clear=True),
            patch('doozerlib.opm.gather_opm', new=AsyncMock(side_effect=render_to_file)) as command,
        ):
            result = await render_packages('index@sha256:abc', packages={'quay'})
        self.assertEqual(result, selected)
        self.assertTrue(command.await_args.kwargs['stdout'].closed)
        self.assertFalse(Path(command.await_args.kwargs['stdout'].name).exists())

    async def test_allows_new_package_absent_from_nonempty_production_catalog(self):
        async def render_to_file(args, *, stdout, **kwargs):
            stdout.write('{"schema":"olm.package","name":"other"}')
            return 0, '', ''

        with patch('doozerlib.opm.gather_opm', new=AsyncMock(side_effect=render_to_file)):
            self.assertEqual(await render_packages('index', packages={'new-package'}), [])

    async def test_rejects_empty_malformed_and_failed_renders(self):
        for rc, output, error in [(0, '', ''), (0, '{broken', ''), (1, '', 'registry unavailable')]:

            async def render_to_file(args, *, stdout, **kwargs):
                stdout.write(output)
                return rc, '', error

            with (
                self.subTest(output=output, rc=rc),
                patch('doozerlib.opm.gather_opm', new=AsyncMock(side_effect=render_to_file)),
                self.assertRaises((ValueError, IOError)),
            ):
                await render_packages.__wrapped__('index', packages={'quay'})

    def test_json_stream_handles_chunk_boundaries_without_unbounded_reads(self):
        class SmallChunks(StringIO):
            def read(self, size=-1):
                if size <= 0:
                    raise AssertionError('Catalog stream must use bounded reads')
                return super().read(min(size, 7))

        expected = [{'name': 'escaped " and unicode \u00e9', 'values': [1, 2]}, {'schema': 'olm.bundle'}]
        stream = SmallChunks(' \n'.join(json.dumps(blob) for blob in expected))
        self.assertEqual(list(iter_json_objects(stream)), expected)
        for text in ['[]', 'null', '{"ok":1} trailing', '{"incomplete":']:
            with self.subTest(text=text), self.assertRaises(ValueError):
                list(iter_json_objects(SmallChunks(text)))
