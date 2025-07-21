import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.cli.release_gen_payload import GenPayloadCli
from doozerlib.constants import KONFLUX_IMAGES_SHARE


class TestReleaseGenPayload(unittest.TestCase):
    def test_mirror_payload_content_konflux_non_embargoed(self):
        # Create a mock runtime object
        runtime = MagicMock()
        runtime.build_system = 'konflux'

        # Create a mock GenPayloadCli instance
        cli = GenPayloadCli(runtime=runtime)
        cli.logger = MagicMock()
        cli.output_path = MagicMock()
        cli.output_path.joinpath.return_value = MagicMock()
        cli.apply = True

        # Create mock payload entries
        image_meta = MagicMock()
        image_meta.image_name_short = 'test-image'
        build_record_inspector = MagicMock()
        build_record_inspector.is_under_embargo.return_value = False
        image_inspector = MagicMock()
        image_inspector.get_pullspec.return_value = 'registry.example.com/test/test-image@sha256:123'
        image_inspector.get_digest.return_value = 'sha256:456'
        payload_entry = MagicMock()
        payload_entry.image_meta = image_meta
        payload_entry.build_record_inspector = build_record_inspector
        payload_entry.image_inspector = image_inspector
        payload_entry.dest_pullspec = 'quay.io/org/repo@sha256:456'
        payload_entry.dest_manifest_list_pullspec = None

        payload_entries = {'test-image': payload_entry}

        # Mock the async file write
        mock_file = AsyncMock()
        mock_context_manager = AsyncMock()
        mock_context_manager.__aenter__.return_value = mock_file

        mock_aio_open = MagicMock()
        mock_aio_open.return_value = mock_context_manager

        # Run the method
        async def run_test():
            with (
                patch('doozerlib.cli.release_gen_payload.aiofiles.open', mock_aio_open),
                patch('doozerlib.cli.release_gen_payload.exectools.cmd_assert_async', new_callable=AsyncMock),
            ):
                await cli.mirror_payload_content('x86_64', payload_entries)

        asyncio.run(run_test())

        # Check the content written to the file
        write_calls = mock_file.write.call_args_list
        content = "".join(c[0][0] for c in write_calls)

        self.assertIn('registry.example.com/test/test-image@sha256:123=quay.io/org/repo@sha256:456', content)
        self.assertIn(
            f'registry.example.com/test/test-image@sha256:123={KONFLUX_IMAGES_SHARE}/test-image@sha256:456',
            content,
        )


if __name__ == '__main__':
    unittest.main()
