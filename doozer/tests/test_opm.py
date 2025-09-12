import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.opm import (
    OpmRegistryAuth,
    generate_basic_template,
    generate_dockerfile,
    render,
    render_catalog_from_template,
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
            ['render', '--migrate-level', 'none', '-o', 'yaml', '--', 'test-catalog'], auth=auth
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
