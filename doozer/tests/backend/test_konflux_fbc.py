import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_fbc import KonfluxFbcImporter
from doozerlib.image import ImageMetadata


class TestKonfluxFbcImporter(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.base_dir = Path("/tmp/konflux_fbc")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.ocp_version = (4, 9)
        self.keep_templates = False
        self.upcycle = False
        self.push = False
        self.commit_message = "Test commit message"
        self.fbc_repo = "https://example.com/fbc-repo.git"
        self.logger = MagicMock()

        self.importer = KonfluxFbcImporter(
            base_dir=self.base_dir,
            group=self.group,
            assembly=self.assembly,
            ocp_version=self.ocp_version,
            keep_templates=self.keep_templates,
            upcycle=self.upcycle,
            push=self.push,
            commit_message=self.commit_message,
            fbc_repo=self.fbc_repo,
            logger=self.logger
        )

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._update_dir")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_import_from_index_image(self, mock_opm, mock_build_repo, mock_update_dir):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        index_image = "test-index-image"

        build_repo = mock_build_repo.return_value
        mock_opm.validate = AsyncMock()

        await self.importer.import_from_index_image(metadata, index_image)

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_opm.validate.assert_called_once_with(self.base_dir.joinpath(metadata.distgit_key, "catalog"))
        build_repo.commit.assert_called_once_with(self.commit_message, allow_empty=True)
        build_repo.push.assert_not_called()

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._update_dir")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_import_from_index_image_with_push(self, mock_opm, mock_build_repo, mock_update_dir):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        index_image = "test-index-image"

        build_repo = mock_build_repo.return_value

        mock_opm.validate = AsyncMock()

        self.importer.push = True

        await self.importer.import_from_index_image(metadata, index_image)
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_opm.validate.assert_called_once_with(self.base_dir.joinpath(metadata.distgit_key, "catalog"))
        build_repo.commit.assert_called_once_with(self.commit_message, allow_empty=True)
        build_repo.push.assert_called_once()

    @patch("shutil.rmtree")
    @patch("pathlib.Path.open")
    @patch("pathlib.Path.mkdir")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_package_name", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_catalog_blobs_from_index_image", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_update_dir(self, mock_opm, mock_get_catalog_blobs, mock_get_package_name, mock_mkdir: MagicMock, mock_open, mock_rmtree):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir
        index_image = "test-index-image"
        logger = MagicMock()

        mock_get_package_name.return_value = "test-package"
        mock_get_catalog_blobs.return_value = [{"schema": "olm.package", "name": "test-package"}]
        mock_opm.generate_basic_template = AsyncMock()
        mock_opm.render_catalog_from_template = AsyncMock()
        mock_opm.generate_dockerfile = AsyncMock()

        mock_org_catalog_file = mock_open.return_value.__enter__.return_value = StringIO()

        await self.importer._update_dir(metadata, build_repo, index_image, logger)

        mock_get_package_name.assert_called_once_with(metadata)
        mock_get_catalog_blobs.assert_called_once_with(index_image, "test-package")
        self.assertEqual(mock_org_catalog_file.getvalue(), '---\nname: test-package\nschema: olm.package\n')
        mock_mkdir.assert_has_calls([
            call(parents=True, exist_ok=True),
            call(parents=True, exist_ok=True),
            call(parents=True, exist_ok=True),
        ])
        mock_opm.generate_basic_template.assert_called_once()
        mock_opm.render_catalog_from_template.assert_called_once()
        mock_opm.generate_dockerfile.assert_called_once()
        mock_rmtree.assert_has_calls([
            call(self.base_dir.joinpath("catalog-migrate")),
            call(self.base_dir.joinpath("catalog-templates")),
        ])

    @patch("doozerlib.backend.konflux_fbc.opm.render")
    async def test_render_index_image(self, mock_render):
        actual = await self.importer._render_index_image("test-index-image-pullspec")
        self.assertEqual(actual, mock_render.return_value)
        mock_render.assert_called_once_with("test-index-image-pullspec")

    def test_filter_catalog_blobs(self):
        catalog_blobs = [
            {"schema": "olm.package", "name": "test-package"},
            {"schema": "olm.channel", "name": "test-channel", "package": "test-package"},
            {"schema": "olm.package", "name": "test-package2"},
            {"schema": "olm.channel", "name": "test-channel2", "package": "test-package2"},
            {"schema": "olm.package", "name": "test-package3"},
            {"schema": "olm.channel", "name": "test-channel3", "package": "test-package3"},
            {"schema": "olm.package", "name": "test-package4"},
            {"schema": "olm.channel", "name": "test-channel4", "package": "test-package4"},
        ]
        actual = self.importer._filter_catalog_blobs(catalog_blobs, {"test-package2", "test-package3"})
        self.assertEqual(actual.keys(), {"test-package2", "test-package3"})
        self.assertListEqual(actual["test-package2"], [
            {"schema": "olm.package", "name": "test-package2"},
            {"schema": "olm.channel", "name": "test-channel2", "package": "test-package2"},
        ])
        self.assertListEqual(actual["test-package3"], [
            {"schema": "olm.package", "name": "test-package3"},
            {"schema": "olm.channel", "name": "test-channel3", "package": "test-package3"},
        ])

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._render_index_image")
    async def test_get_catalog_blobs_from_index_image(self, mock_render_index_image):
        index_image = "test-index-image"
        mock_render_index_image.return_value = [
            {"schema": "olm.package", "name": "test-package"},
            {"schema": "olm.channel", "name": "test-channel", "package": "test-package"},
            {"schema": "olm.package", "name": "test-package2"},
            {"schema": "olm.channel", "name": "test-channel2", "package": "test-package2"},
            {"schema": "olm.package", "name": "test-package3"},
            {"schema": "olm.channel", "name": "test-channel3", "package": "test-package3"},
            {"schema": "olm.package", "name": "test-package4"},
        ]
        actual = await self.importer._get_catalog_blobs_from_index_image(index_image, "test-package")
        self.assertEqual(actual, [
            {"schema": "olm.package", "name": "test-package"},
            {"schema": "olm.channel", "name": "test-channel", "package": "test-package"},
        ])
        mock_render_index_image.assert_called_once_with(index_image)

    @patch("pathlib.Path.open")
    @patch("pathlib.Path.glob")
    async def test_get_package_name(self, mock_glob, mock_open):
        metadata = MagicMock(spec=ImageMetadata)
        runtime = metadata.runtime = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.config = {
            "update-csv": {
                "manifests-dir": "test-manifests-dir"
            }
        }
        source_resolver = runtime.source_resolver
        source_resolver.get_source_dir.return_value = Path("/tmp/source-dir")
        mock_glob.return_value = iter([Path("/tmp/source-dir/test-manifests-dir/test-package-name.package.yaml")])
        mock_open.return_value.__enter__.return_value = StringIO("packageName: test-package-name")
        actual = await self.importer._get_package_name(metadata)
        self.assertEqual(actual, "test-package-name")
        source_resolver.resolve_source.assert_called_once_with(metadata)
