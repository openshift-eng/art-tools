import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.lockfile import (
    ArtifactInfo,
    ArtifactLockfileGenerator,
)


class TestArtifactInfo(unittest.TestCase):
    def test_to_dict(self):
        artifact_info = ArtifactInfo(url="https://example.com/file.pem", checksum="sha256:abc123", filename="file.pem")
        expected = {"download_url": "https://example.com/file.pem", "checksum": "sha256:abc123", "filename": "file.pem"}
        self.assertEqual(artifact_info.to_dict(), expected)

    def test_creation(self):
        artifact_info = ArtifactInfo(
            url="https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem",
            checksum="sha256:def456",
            filename="Current-IT-Root-CAs.pem",
        )
        self.assertEqual(artifact_info.url, "https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem")
        self.assertEqual(artifact_info.checksum, "sha256:def456")
        self.assertEqual(artifact_info.filename, "Current-IT-Root-CAs.pem")


class TestArtifactLockfileGenerator(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.logger = MagicMock()
        self.runtime = MagicMock()
        self.generator = ArtifactLockfileGenerator(logger=self.logger, runtime=self.runtime)

    def test_extract_filename_from_url(self):
        """Test filename extraction from URLs"""
        # Test normal URL with filename
        url = "https://example.com/path/to/file.pem"
        result = self.generator._extract_filename_from_url(url)
        self.assertEqual(result, "file.pem")

        # Test URL ending with slash
        url_with_slash = "https://example.com/path/to/"
        result = self.generator._extract_filename_from_url(url_with_slash)
        self.assertEqual(result, "artifact")

        # Test URL with query parameters
        url_with_params = "https://example.com/cert.pem?version=latest"
        result = self.generator._extract_filename_from_url(url_with_params)
        self.assertEqual(result, "cert.pem?version=latest")

    def test_should_generate_artifact_lockfile(self):
        """Test lockfile generation decision logic"""
        mock_image_meta = MagicMock()
        mock_dest_dir = MagicMock()

        # Test enabled case
        mock_image_meta.is_artifact_lockfile_enabled.return_value = True
        result = self.generator.should_generate_artifact_lockfile(mock_image_meta, mock_dest_dir)
        self.assertTrue(result)

        # Test disabled case
        mock_image_meta.is_artifact_lockfile_enabled.return_value = False
        result = self.generator.should_generate_artifact_lockfile(mock_image_meta, mock_dest_dir)
        self.assertFalse(result)

    @patch.object(ArtifactLockfileGenerator, '_write_yaml')
    @patch('aiohttp.ClientSession')
    async def test_generate_artifact_lockfile_success(self, mock_session_class, mock_write_yaml):
        """Test successful artifact lockfile generation with URL list"""
        # Setup mocks
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.get_required_artifacts.return_value = [
            {'url': "https://example.com/cert1.pem", 'filename': None},
            {'url': "https://example.com/cert2.pem", 'filename': None},
        ]

        mock_dest_dir = Path("/tmp/test")

        # Mock session and download behavior
        mock_session = MagicMock()
        mock_session_class.return_value.__aenter__.return_value = mock_session

        mock_artifact_info1 = MagicMock()
        mock_artifact_info1.to_dict.return_value = {
            "download_url": "https://example.com/cert1.pem",
            "checksum": "sha256:abc1",
            "filename": "cert1.pem",
        }
        mock_artifact_info2 = MagicMock()
        mock_artifact_info2.to_dict.return_value = {
            "download_url": "https://example.com/cert2.pem",
            "checksum": "sha256:abc2",
            "filename": "cert2.pem",
        }

        # Mock the download and compute checksum method
        self.generator._download_and_compute_checksum = AsyncMock()
        self.generator._download_and_compute_checksum.side_effect = [mock_artifact_info1, mock_artifact_info2]

        # Mock should_generate_artifact_lockfile to return True
        self.generator.should_generate_artifact_lockfile = MagicMock(return_value=True)

        # Execute
        await self.generator.generate_artifact_lockfile(mock_image_meta, mock_dest_dir)

        # Verify calls
        self.generator._download_and_compute_checksum.assert_any_call(
            mock_session, {'url': 'https://example.com/cert1.pem', 'filename': None}
        )
        self.generator._download_and_compute_checksum.assert_any_call(
            mock_session, {'url': 'https://example.com/cert2.pem', 'filename': None}
        )

        # Verify lockfile data structure
        expected_lockfile_data = {
            "metadata": {"version": "1.0"},
            "artifacts": [
                {"download_url": "https://example.com/cert1.pem", "checksum": "sha256:abc1", "filename": "cert1.pem"},
                {"download_url": "https://example.com/cert2.pem", "checksum": "sha256:abc2", "filename": "cert2.pem"},
            ],
        }
        mock_write_yaml.assert_called_once_with(expected_lockfile_data, mock_dest_dir / "artifacts.lock.yaml")

    @patch.object(ArtifactLockfileGenerator, '_write_yaml')
    @patch('aiohttp.ClientSession')
    async def test_generate_artifact_lockfile_custom_filename(self, mock_session_class, mock_write_yaml):
        """Test artifact lockfile generation with custom destination filenames"""
        # Setup mocks
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.get_required_artifacts.return_value = [
            {'url': "https://example.com/v1.2.3/long-path/file.jar", 'filename': "custom.jar"},
            {'url': "https://example.com/cert.pem", 'filename': None},  # Auto-extract
        ]

        mock_dest_dir = Path("/tmp/test")

        # Mock session and download behavior
        mock_session = MagicMock()
        mock_session_class.return_value.__aenter__.return_value = mock_session

        mock_artifact_info1 = MagicMock()
        mock_artifact_info1.to_dict.return_value = {
            "download_url": "https://example.com/v1.2.3/long-path/file.jar",
            "checksum": "sha256:abc1",
            "filename": "custom.jar",
        }
        mock_artifact_info2 = MagicMock()
        mock_artifact_info2.to_dict.return_value = {
            "download_url": "https://example.com/cert.pem",
            "checksum": "sha256:abc2",
            "filename": "cert.pem",
        }

        # Mock the download and compute checksum method
        self.generator._download_and_compute_checksum = AsyncMock()
        self.generator._download_and_compute_checksum.side_effect = [mock_artifact_info1, mock_artifact_info2]

        # Mock should_generate_artifact_lockfile to return True
        self.generator.should_generate_artifact_lockfile = MagicMock(return_value=True)

        # Execute
        await self.generator.generate_artifact_lockfile(mock_image_meta, mock_dest_dir)

        # Verify calls
        self.generator._download_and_compute_checksum.assert_any_call(
            mock_session, {'url': 'https://example.com/v1.2.3/long-path/file.jar', 'filename': 'custom.jar'}
        )
        self.generator._download_and_compute_checksum.assert_any_call(
            mock_session, {'url': 'https://example.com/cert.pem', 'filename': None}
        )

        # Verify lockfile data structure
        expected_lockfile_data = {
            "metadata": {"version": "1.0"},
            "artifacts": [
                {
                    "download_url": "https://example.com/v1.2.3/long-path/file.jar",
                    "checksum": "sha256:abc1",
                    "filename": "custom.jar",
                },
                {"download_url": "https://example.com/cert.pem", "checksum": "sha256:abc2", "filename": "cert.pem"},
            ],
        }
        mock_write_yaml.assert_called_once_with(expected_lockfile_data, mock_dest_dir / "artifacts.lock.yaml")

    async def test_generate_artifact_lockfile_disabled(self):
        """Test skipping when artifact lockfile is disabled"""
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_dest_dir = Path("/tmp/test")

        # Mock should_generate_artifact_lockfile to return False
        self.generator.should_generate_artifact_lockfile = MagicMock(return_value=False)

        # Execute
        await self.generator.generate_artifact_lockfile(mock_image_meta, mock_dest_dir)

        # Verify early return
        self.logger.debug.assert_called_with("Skipping artifact lockfile generation for test-image")

    async def test_generate_artifact_lockfile_no_artifacts(self):
        """Test behavior when no artifacts are defined"""
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.get_required_artifacts.return_value = []
        mock_dest_dir = Path("/tmp/test")

        # Mock should_generate_artifact_lockfile to return True
        self.generator.should_generate_artifact_lockfile = MagicMock(return_value=True)

        # Execute
        await self.generator.generate_artifact_lockfile(mock_image_meta, mock_dest_dir)

        # Verify warning and early return
        self.logger.warning.assert_called_with("No artifacts defined for test-image")
