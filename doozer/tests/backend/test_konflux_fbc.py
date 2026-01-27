import logging
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, call, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBundleBuildRecord
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.backend.konflux_fbc import (
    KonfluxFbcBuilder,
    KonfluxFbcFragmentMerger,
    KonfluxFbcImporter,
    KonfluxFbcRebaser,
    _categorize_catalog_blobs,
    _generate_fbc_branch_name,
)
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo, PodInfo
from doozerlib.image import ImageMetadata
from doozerlib.opm import OpmRegistryAuth, yaml


class TestKonfluxFbcImporter(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.base_dir = Path("/tmp/konflux_fbc")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.ocp_version = (4, 9)
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
            upcycle=self.upcycle,
            push=self.push,
            commit_message=self.commit_message,
            fbc_repo=self.fbc_repo,
            auth=OpmRegistryAuth(path="/path/to/auth.json"),
            logger=self.logger,
        )

    @patch(
        "doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_package_name",
        new_callable=AsyncMock,
        return_value="test-package",
    )
    @patch(
        "doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_catalog_blobs_from_index_image",
        new_callable=AsyncMock,
        return_value=[{"schema": "olm.package", "name": "test-package"}],
    )
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._update_dir")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_import_from_index_image(
        self, mock_opm, mock_build_repo, mock_update_dir, mock_get_catalog_blobs, mock_get_package_name
    ):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        index_image = "test-index-image"

        build_repo = mock_build_repo.return_value
        mock_opm.validate = AsyncMock()

        await self.importer.import_from_index_image(metadata, index_image)

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-ocp-4.9-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_opm.validate.assert_called_once_with(self.base_dir.joinpath(metadata.distgit_key, "catalog"))
        build_repo.commit.assert_called_once_with(self.commit_message, allow_empty=True)
        build_repo.push.assert_not_called()
        mock_update_dir.assert_awaited_once_with(
            build_repo,
            "test-package",
            [{"schema": "olm.package", "name": "test-package"}],
            ANY,
        )
        mock_get_catalog_blobs.assert_awaited_once_with(
            index_image,
            "test-package",
            migrate_level="none",
        )
        mock_get_package_name.assert_awaited_once_with(metadata)

    @patch(
        "doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_package_name",
        new_callable=AsyncMock,
        return_value="test-package",
    )
    @patch(
        "doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_catalog_blobs_from_index_image",
        new_callable=AsyncMock,
        return_value=[{"schema": "olm.package", "name": "test-package"}],
    )
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._update_dir")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_import_from_index_image_with_push(
        self, mock_opm, mock_build_repo, mock_update_dir, mock_get_catalog_blobs, mock_get_package_name
    ):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        index_image = "test-index-image"

        build_repo = mock_build_repo.return_value

        mock_opm.validate = AsyncMock()

        self.importer.push = True

        await self.importer.import_from_index_image(metadata, index_image)
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_opm.validate.assert_called_once_with(self.base_dir.joinpath(metadata.distgit_key, "catalog"))
        build_repo.commit.assert_called_once_with(self.commit_message, allow_empty=True)
        build_repo.push.assert_called_once()
        mock_update_dir.assert_awaited_once_with(
            build_repo,
            "test-package",
            [{"schema": "olm.package", "name": "test-package"}],
            ANY,
        )
        mock_get_catalog_blobs.assert_awaited_once_with(
            index_image,
            "test-package",
            migrate_level="none",
        )
        mock_get_package_name.assert_awaited_once_with(metadata)

    @patch("shutil.rmtree")
    @patch("pathlib.Path.open")
    @patch("pathlib.Path.mkdir")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_package_name", new_callable=AsyncMock)
    @patch(
        "doozerlib.backend.konflux_fbc.KonfluxFbcImporter._get_catalog_blobs_from_index_image", new_callable=AsyncMock
    )
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_update_dir(
        self, mock_opm, mock_get_catalog_blobs, mock_get_package_name, mock_mkdir: MagicMock, mock_open, mock_rmtree
    ):
        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir
        logger = MagicMock()

        mock_opm.generate_basic_template = AsyncMock()
        mock_opm.render_catalog_from_template = AsyncMock()
        mock_opm.generate_dockerfile = AsyncMock()

        package_name = "test-package"
        catalog_blobs = [{"schema": "olm.package", "name": "test-package"}]

        mock_org_catalog_file = mock_open.return_value.__enter__.return_value = StringIO()

        await self.importer._update_dir(build_repo, package_name, catalog_blobs, logger)

        self.assertEqual(mock_org_catalog_file.getvalue(), '---\nname: test-package\nschema: olm.package\n')
        mock_mkdir.assert_has_calls(
            [
                call(parents=True, exist_ok=True),
            ]
        )
        mock_opm.generate_dockerfile.assert_called_once()

    @patch("doozerlib.backend.konflux_fbc.opm.render")
    async def test_render_index_image(self, mock_render):
        actual = await self.importer._render_index_image("test-index-image-pullspec")
        self.assertEqual(actual, mock_render.return_value)
        mock_render.assert_called_once_with(
            "test-index-image-pullspec", migrate_level="none", auth=OpmRegistryAuth(path='/path/to/auth.json')
        )

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
        self.assertListEqual(
            actual["test-package2"],
            [
                {"schema": "olm.package", "name": "test-package2"},
                {"schema": "olm.channel", "name": "test-channel2", "package": "test-package2"},
            ],
        )
        self.assertListEqual(
            actual["test-package3"],
            [
                {"schema": "olm.package", "name": "test-package3"},
                {"schema": "olm.channel", "name": "test-channel3", "package": "test-package3"},
            ],
        )

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
        self.assertEqual(
            actual,
            [
                {"schema": "olm.package", "name": "test-package"},
                {"schema": "olm.channel", "name": "test-channel", "package": "test-package"},
            ],
        )
        mock_render_index_image.assert_called_once_with(index_image, migrate_level="none")

    @patch("pathlib.Path.open")
    @patch("pathlib.Path.glob")
    async def test_get_package_name(self, mock_glob, mock_open):
        metadata = MagicMock(spec=ImageMetadata)
        runtime = metadata.runtime = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.config = {
            "update-csv": {
                "manifests-dir": "test-manifests-dir",
            },
        }
        source_resolver = runtime.source_resolver
        source_resolver.get_source_dir.return_value = Path("/tmp/source-dir")
        mock_glob.return_value = iter([Path("/tmp/source-dir/test-manifests-dir/test-package-name.package.yaml")])
        mock_open.return_value.__enter__.return_value = StringIO("packageName: test-package-name")
        actual = await self.importer._get_package_name(metadata)
        self.assertEqual(actual, "test-package-name")
        source_resolver.resolve_source.assert_called_once_with(metadata)


class TestKonfluxFbcRebaser(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.base_dir = Path("/tmp/konflux_fbc_rebaser")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.version = "1.0.0"
        self.release = "1"
        self.commit_message = "Test rebase commit message"
        self.push = False
        self.fbc_repo = "https://example.com/fbc-repo.git"
        self.upcycle = False
        self.logger = MagicMock()

        self.rebaser = KonfluxFbcRebaser(
            base_dir=self.base_dir,
            group=self.group,
            assembly=self.assembly,
            version=self.version,
            release=self.release,
            commit_message=self.commit_message,
            push=self.push,
            fbc_repo=self.fbc_repo,
            upcycle=self.upcycle,
        )

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._rebase_dir")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_rebase(self, mock_opm, mock_build_repo, mock_rebase_dir):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="foo-bundle-1.0.0-1",
            operator_nvr="foo-operator-1.0.0-1",
            operand_nvrs=["foo-operand-1.0.0-1"],
        )
        version = "1.0.0"
        release = "1"

        build_repo = mock_build_repo.return_value
        build_repo.local_dir = self.base_dir.joinpath(metadata.distgit_key)
        mock_opm.validate = AsyncMock()
        mock_rebase_dir.return_value = "test-distgit-key-fbc-1.0.0-1"

        actual = await self.rebaser.rebase(metadata, bundle_build, version, release)
        self.assertEqual(actual, "test-distgit-key-fbc-1.0.0-1")

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-ocp-4.9-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_rebase_dir.assert_called_once_with(metadata, build_repo, bundle_build, version, release, ANY)
        mock_opm.validate.assert_called_once_with(self.base_dir.joinpath(metadata.distgit_key, "catalog"))
        build_repo.commit.assert_called_once_with(ANY, allow_empty=True)
        build_repo.push.assert_not_called()

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._rebase_dir")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_rebase_with_push(self, mock_opm, mock_build_repo, mock_rebase_dir):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="foo-bundle-1.0.0-1",
            operator_nvr="foo-operator-1.0.0-1",
            operand_nvrs=["foo-operand-1.0.0-1"],
        )
        version = "1.0.0"
        release = "1"

        build_repo = mock_build_repo.return_value
        build_repo.local_dir = self.base_dir.joinpath(metadata.distgit_key)
        mock_opm.validate = AsyncMock()
        mock_rebase_dir.return_value = "test-distgit-key-fbc-1.0.0-1"
        self.rebaser.push = True

        actual = await self.rebaser.rebase(metadata, bundle_build, version, release)
        self.assertEqual(actual, "test-distgit-key-fbc-1.0.0-1")

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-ocp-4.9-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_rebase_dir.assert_called_once_with(metadata, build_repo, bundle_build, version, release, ANY)
        mock_opm.validate.assert_called_once_with(self.base_dir.joinpath(metadata.distgit_key, "catalog"))
        build_repo.commit.assert_called_once_with(ANY, allow_empty=True)
        build_repo.push.assert_called_once()

    @patch("doozerlib.opm.generate_dockerfile")
    @patch("pathlib.Path.unlink")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._load_csv_from_bundle")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._get_referenced_images")
    @patch("doozerlib.backend.konflux_fbc.DockerfileParser")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.is_file", return_value=True)
    @patch("pathlib.Path.open")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._fetch_olm_bundle_image_info", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._fetch_olm_bundle_blob", new_callable=AsyncMock)
    async def test_rebase_dir(
        self,
        mock_fetch_olm_bundle_blob,
        mock_fetch_olm_bundle_image_info,
        mock_open,
        mock_is_file,
        mock_mkdir,
        MockDockerfileParser,
        mock_get_referenced_images,
        mock_load_csv_from_bundle: AsyncMock,
        mock_path_unlink: Mock,
        mock_generate_dockerfile: AsyncMock,
    ):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        metadata.runtime = MagicMock()
        metadata.get_olm_bundle_delivery_repo_name = MagicMock(return_value="openshift4/foo-bundle")
        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir
        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="foo-bundle-1.0.0-1",
            image_pullspec="dev.example.com/foo-bundle@1",
            image_tag="deadbeef",
            source_repo="https://example.com/foo-operator.git",
            commitish="beefdead",
            operator_nvr="foo-operator-1.0.0-1",
            operand_nvrs=[],
        )
        version = "1.0.0"
        release = "1"
        logger = MagicMock()

        mock_fetch_olm_bundle_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "name": "test-image-name",
                        "operators.operatorframework.io.bundle.channels.v1": "test-channel",
                        "operators.operatorframework.io.bundle.channel.default.v1": "test-default-channel",
                        "operators.operatorframework.io.bundle.package.v1": "test-package",
                        "operators.operatorframework.io.bundle.manifests.v1": "manifests/",
                    },
                },
            },
        }
        mock_fetch_olm_bundle_blob.return_value = (
            "test-bundle-name.1.2.3",
            "test-package",
            {
                "schema": "olm.bundle",
                "name": "test-bundle-name.1.2.3",
                "package": "test-package",
                "properties": [
                    {
                        "type": "olm.csv.metadata",
                        "value": {
                            "annotations": {
                                "olm.skipRange": ">=4.8.0 <4.17.0",
                            },
                        },
                    }
                ],
                "relatedImages": [
                    {"name": "", "image": "example.com/test-bundle@1"},
                    {"name": "test-operator", "image": "example.com/test-operator@2"},
                ],
            },
        )
        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {}
        mock_dfp.labels = {}

        org_catalog_blobs = [
            {"schema": "olm.package", "name": "test-package", "defaultChannel": "test-default-channel"},
            {
                "schema": "olm.channel",
                "name": "test-channel",
                "package": "test-package",
                "entries": [
                    {"name": "test-bundle-name.1.0.0", "skipRange": ">=4.8.0 <4.17.0"},
                    {"name": "test-bundle-name.1.0.1", "skipRange": ">=4.8.0 <4.17.0"},
                    {
                        "name": "test-bundle-name.1.1.0",
                        "skipRange": ">=4.8.0 <4.17.0",
                        "skips": ["test-bundle-name.0.0.9", "test-bundle-name.1.0.0", "test-bundle-name.1.0.1"],
                    },
                ],
            },
            {
                "schema": "olm.bundle",
                "name": "test-bundle-name.1.0.0",
                "package": "test-package",
                "properties": [],
                "relatedImages": [
                    {"name": "", "image": "example.com/openshift/test-bundle@1"},
                    {"name": "test-operator", "image": "example.com/test-operator@2"},
                ],
            },
        ]
        org_catalog_file = StringIO()
        yaml.dump_all(org_catalog_blobs, org_catalog_file)
        org_catalog_file.seek(0)
        result_catalog_file = StringIO()
        images_mirror_set_file = StringIO()
        mock_open.return_value.__enter__.side_effect = [org_catalog_file, result_catalog_file, images_mirror_set_file]
        mock_get_referenced_images.return_value = [
            MagicMock(image_pullspec="example.com/art-images@2"),
        ]

        mock_load_csv_from_bundle.return_value = {
            "metadata": {
                "name": "test-bundle-name",
                "annotations": {
                    "olm.skipRange": ">=4.8.0 <4.17.0",
                },
            }
        }

        actual = await self.rebaser._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)
        self.assertEqual(actual, "test-distgit-key-fbc-1.0.0-1")

        mock_fetch_olm_bundle_image_info.assert_called_once_with(bundle_build)
        mock_fetch_olm_bundle_blob.assert_called_once_with(bundle_build, migrate_level="none")
        # Replace deprecated assertDictContainsSubset with explicit checks
        expected_envs = {
            "__doozer_group": "test-group",
            "__doozer_key": "test-distgit-key",
            "__doozer_version": "1.0.0",
            "__doozer_release": "1",
            "__doozer_bundle_nvrs": "foo-bundle-1.0.0-1",
        }
        for k, v in expected_envs.items():
            self.assertIn(k, mock_dfp.envs)
            self.assertEqual(mock_dfp.envs[k], v)

        expected_labels = {
            "io.openshift.build.source-location": "https://example.com/foo-operator.git",
            "io.openshift.build.commit.id": "beefdead",
        }
        for k, v in expected_labels.items():
            self.assertIn(k, mock_dfp.labels)
            self.assertEqual(mock_dfp.labels[k], v)

        result_catalog_file.seek(0)
        result_catalog_blobs = list(yaml.load_all(result_catalog_file))
        result_catalog_blobs = _categorize_catalog_blobs(result_catalog_blobs)
        self.assertEqual(result_catalog_blobs.keys(), {"test-package"})
        self.assertEqual(
            result_catalog_blobs["test-package"]["olm.channel"]["test-channel"]["entries"],
            [
                {"name": "test-bundle-name.1.0.0", "skipRange": ">=4.8.0 <4.17.0"},
                {"name": "test-bundle-name.1.0.1", "skipRange": ">=4.8.0 <4.17.0"},
                {"name": "test-bundle-name.1.1.0", "skipRange": ">=4.8.0 <4.17.0"},
                {
                    "name": "test-bundle-name.1.2.3",
                    "replaces": "test-bundle-name.1.0.0",
                    "skipRange": ">=4.8.0 <4.17.0",
                    "skips": [
                        "test-bundle-name.0.0.9",
                        "test-bundle-name.1.0.0",
                        "test-bundle-name.1.0.1",
                        "test-bundle-name.1.1.0",
                    ],
                },
            ],
        )
        self.assertEqual(
            result_catalog_blobs["test-package"]["olm.bundle"].keys(),
            {"test-bundle-name.1.0.0", "test-bundle-name.1.2.3"},
        )

        images_mirror_set_file.seek(0)
        images_mirror_set = yaml.load(images_mirror_set_file)
        self.assertEqual(len(images_mirror_set["spec"]["imageDigestMirrors"]), 2)

        mock_load_csv_from_bundle.assert_awaited_once_with(bundle_build, "manifests/")
        mock_path_unlink.assert_called_once_with()
        mock_generate_dockerfile.assert_awaited_once_with(
            build_repo.local_dir,
            'catalog',
            base_image='registry.redhat.io/openshift4/ose-operator-registry:v1.1',
            builder_image='registry.redhat.io/openshift4/ose-operator-registry:v1.1',
        )

    def test_generate_image_digest_mirror_set(self):
        olm_bundle_blobs = [
            {
                "relatedImages": [
                    {
                        "name": "pf-status-relay-rhel9-operator",
                        "image": "registry.redhat.io/openshift4/pf-status-relay-rhel9-operator@sha256:9930cd2f6519e619da1811f04e4d3a73c29f519142064a1562e0759e251bf319",
                    },
                    {
                        "name": "pf-status-relay-rhel9",
                        "image": "registry.redhat.io/openshift4/pf-status-relay-rhel9@sha256:bd8dddf22ed4d977127d8c1e89b868ecb4a34645ac09d376cedbcaa03176a846",
                    },
                    {
                        "name": "ose-kube-rbac-proxy-rhel9",
                        "image": "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9@sha256:683e74056df40f38004b2145b8a037dd43b56376061915f37ccb50b5ed19b404",
                    },
                    {
                        "name": "",
                        "image": "registry.redhat.io/openshift4/pf-status-relay-operator-bundle@sha256:d62745a5780f4224d49fad22be910e426cf3bfc3150b0e4cdc00fa69a6983a9b",
                    },
                ],
            },
        ]
        ref_pullspecs = [
            "build.example.com/art-images@sha256:9930cd2f6519e619da1811f04e4d3a73c29f519142064a1562e0759e251bf319",
            "build.example.com/art-images@sha256:bd8dddf22ed4d977127d8c1e89b868ecb4a34645ac09d376cedbcaa03176a846",
            "build.example.com/art-images@sha256:683e74056df40f38004b2145b8a037dd43b56376061915f37ccb50b5ed19b404",
            "build.example.com/art-images@sha256:d62745a5780f4224d49fad22be910e426cf3bfc3150b0e4cdc00fa69a6983a9b",
        ]
        result = self.rebaser._generate_image_digest_mirror_set(olm_bundle_blobs, ref_pullspecs)
        self.assertEqual(len(result["spec"]["imageDigestMirrors"]), 4)

    @patch("doozerlib.util.oc_image_info_for_arch_async__caching", new_callable=AsyncMock)
    async def test_fetch_olm_bundle_image_info(self, mock_oc_image_info):
        bundle_build = MagicMock(spec=KonfluxBundleBuildRecord)
        bundle_build.image_pullspec = "test-image-pullspec"
        mock_oc_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "name": "test-image-name",
                        "operators.operatorframework.io.bundle.channels.v1": "test-channel",
                        "operators.operatorframework.io.bundle.channel.default.v1": "test-default-channel",
                        "operators.operatorframework.io.bundle.package.v1": "test-package",
                    },
                },
            },
        }
        actual = await self.rebaser._fetch_olm_bundle_image_info(bundle_build)
        labels = actual["config"]["config"]["Labels"]
        self.assertEqual(labels.get("name"), "test-image-name")
        self.assertEqual(labels.get("operators.operatorframework.io.bundle.channels.v1"), "test-channel")
        self.assertEqual(labels.get("operators.operatorframework.io.bundle.channel.default.v1"), "test-default-channel")
        self.assertEqual(labels.get("operators.operatorframework.io.bundle.package.v1"), "test-package")

    @patch("doozerlib.opm.render", new_callable=AsyncMock)
    async def test_fetch_olm_bundle_blob(self, mock_render):
        bundle_build = MagicMock(spec=KonfluxBundleBuildRecord)
        bundle_build.image_pullspec = "test-image-pullspec"
        mock_render.return_value = [
            {
                "schema": "olm.bundle",
                "name": "test-bundle-name",
                "package": "test-package",
                "properties": [{"type": "olm.csv.metadata", "value": {"annotations": {}}}],
            }
        ]
        actual = await self.rebaser._fetch_olm_bundle_blob(bundle_build, migrate_level="none")
        self.assertEqual(
            actual,
            (
                "test-bundle-name",
                "test-package",
                {
                    "schema": "olm.bundle",
                    "name": "test-bundle-name",
                    "package": "test-package",
                    "properties": [{"type": "olm.csv.metadata", "value": {"annotations": {}}}],
                },
            ),
        )
        mock_render.assert_called_once_with("test-image-pullspec", migrate_level="none", auth=ANY)

    def test_categorize_catalog_blobs(self):
        catalog_blobs = [
            {"schema": "olm.package", "name": "test-package"},
            {"schema": "olm.channel", "name": "test-channel", "package": "test-package"},
            {"schema": "olm.bundle", "name": "test-bundle", "package": "test-package"},
            {"schema": "olm.package", "name": "test-package2"},
            {"schema": "olm.channel", "name": "test-channel2", "package": "test-package2"},
            {"schema": "olm.bundle", "name": "test-bundle2", "package": "test-package2"},
        ]
        actual = _categorize_catalog_blobs(catalog_blobs)
        self.assertEqual(actual.keys(), {"test-package", "test-package2"})
        self.assertEqual(actual["test-package"].keys(), {"olm.package", "olm.channel", "olm.bundle"})
        self.assertEqual(
            actual["test-package"]["olm.package"]["test-package"], {"schema": "olm.package", "name": "test-package"}
        )
        self.assertEqual(
            actual["test-package"]["olm.channel"]["test-channel"],
            {"schema": "olm.channel", "name": "test-channel", "package": "test-package"},
        )
        self.assertEqual(
            actual["test-package"]["olm.bundle"]["test-bundle"],
            {"schema": "olm.bundle", "name": "test-bundle", "package": "test-package"},
        )
        self.assertEqual(actual["test-package2"].keys(), {"olm.package", "olm.channel", "olm.bundle"})
        self.assertEqual(
            actual["test-package2"]["olm.package"]["test-package2"], {"schema": "olm.package", "name": "test-package2"}
        )
        self.assertEqual(
            actual["test-package2"]["olm.channel"]["test-channel2"],
            {"schema": "olm.channel", "name": "test-channel2", "package": "test-package2"},
        )
        self.assertEqual(
            actual["test-package2"]["olm.bundle"]["test-bundle2"],
            {"schema": "olm.bundle", "name": "test-bundle2", "package": "test-package2"},
        )


class TestKonfluxFbcBuilder(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.base_dir = Path("/tmp/konflux_fbc_builder")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.product = "test-product"
        self.db = MagicMock()
        self.fbc_repo = "https://example.com/fbc-repo.git"
        self.konflux_namespace = "test-namespace"
        self.konflux_kubeconfig = "/path/to/kube/config"
        self.konflux_context = None
        self.image_repo = "quay.io/test-repo"
        self.skip_checks = False
        self.pipelinerun_template_url = "https://example.com/template.yaml"
        self.dry_run = False
        self.record_logger = MagicMock()
        self.logger = logging.getLogger("test-logger")

        with patch("doozerlib.backend.konflux_fbc.KonfluxClient", spec=KonfluxClient) as MockKonfluxClient:
            self.kube_client = MockKonfluxClient.from_kubeconfig.return_value = AsyncMock(spec=KonfluxClient)
            self.builder = KonfluxFbcBuilder(
                base_dir=self.base_dir,
                group=self.group,
                assembly=self.assembly,
                product=self.product,
                db=self.db,
                fbc_repo=self.fbc_repo,
                konflux_namespace=self.konflux_namespace,
                konflux_kubeconfig=self.konflux_kubeconfig,
                konflux_context=self.konflux_context,
                image_repo=self.image_repo,
                skip_checks=self.skip_checks,
                pipelinerun_template_url=self.pipelinerun_template_url,
                dry_run=self.dry_run,
                record_logger=self.record_logger,
                logger=self.logger,
            )

    async def test_start_build(self):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "foo"
        metadata.runtime = MagicMock()
        metadata.runtime.assembly = "test"
        build_repo = MagicMock(
            spec=BuildRepo, https_url="https://example.com/foo.git", branch="test-branch", commit_hash="deadbeef"
        )
        kube_client = self.kube_client
        kube_client.resource_url.return_value = "https://example.com/pipelinerun/test-pipeline-run-name"
        pplr = kube_client.start_pipeline_run_for_image_build.return_value = MagicMock(
            **{"metadata.name": "test-pipeline-run-name"},
        )

        result = await self.builder._start_build(
            metadata,
            build_repo,
            output_image="test-image-pullspec",
            arches=["x86_64", "s390x"],
            logger=self.logger,
        )
        kube_client.ensure_application.assert_awaited_once_with(name="fbc-test-group", display_name="fbc-test-group")
        kube_client.ensure_component.assert_awaited_once_with(
            name="fbc-test-group-foo",
            application="fbc-test-group",
            component_name="fbc-test-group-foo",
            image_repo="test-image-pullspec",
            source_url=build_repo.https_url,
            revision=build_repo.branch,
        )
        kube_client.start_pipeline_run_for_image_build.assert_awaited_once_with(
            generate_name='fbc-test-group-foo-',
            namespace='test-namespace',
            application_name='fbc-test-group',
            component_name='fbc-test-group-foo',
            git_url='https://example.com/foo.git',
            commit_sha="deadbeef",
            target_branch='test-branch',
            output_image='test-image-pullspec',
            vm_override={},
            building_arches=['x86_64', 's390x'],
            additional_tags=[],
            skip_checks=False,
            hermetic=True,
            dockerfile='catalog.Dockerfile',
            pipelinerun_template_url='https://example.com/template.yaml',
            build_priority='2',
        )
        self.assertEqual(result, (pplr, "https://example.com/pipelinerun/test-pipeline-run-name"))

    async def test_start_build_2(self):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "foo"
        metadata.runtime = MagicMock()
        metadata.runtime.assembly = "stream"
        metadata.runtime.group = "openshift-4.20"
        metadata.config = MagicMock()
        metadata.config.delivery.delivery_repo_names = ["openshift4/sample-operator-1", "openshift4/sample-operator-2"]
        build_repo = MagicMock(
            spec=BuildRepo, https_url="https://example.com/foo.git", branch="test-branch", commit_hash="deadbeef"
        )
        kube_client = self.kube_client
        kube_client.resource_url.return_value = "https://example.com/pipelinerun/test-pipeline-run-name"
        pplr = kube_client.start_pipeline_run_for_image_build.return_value = MagicMock(
            **{"metadata.name": "test-pipeline-run-name"},
        )

        result = await self.builder._start_build(
            metadata,
            build_repo,
            output_image="test-image-pullspec",
            arches=["x86_64", "s390x"],
            logger=self.logger,
        )
        kube_client.ensure_application.assert_awaited_once_with(name="fbc-test-group", display_name="fbc-test-group")
        kube_client.ensure_component.assert_awaited_once_with(
            name="fbc-test-group-foo",
            application="fbc-test-group",
            component_name="fbc-test-group-foo",
            image_repo="test-image-pullspec",
            source_url=build_repo.https_url,
            revision=build_repo.branch,
        )
        kube_client.start_pipeline_run_for_image_build.assert_awaited_once_with(
            generate_name='fbc-test-group-foo-',
            namespace='test-namespace',
            application_name='fbc-test-group',
            component_name='fbc-test-group-foo',
            git_url='https://example.com/foo.git',
            commit_sha="deadbeef",
            target_branch='test-branch',
            output_image='test-image-pullspec',
            vm_override={},
            building_arches=['x86_64', 's390x'],
            additional_tags=["ocp__4.20__sample-operator-1", "ocp__4.20__sample-operator-2"],
            skip_checks=False,
            hermetic=True,
            dockerfile='catalog.Dockerfile',
            pipelinerun_template_url='https://example.com/template.yaml',
            build_priority='2',
        )
        self.assertEqual(result, (pplr, "https://example.com/pipelinerun/test-pipeline-run-name"))

    @patch("pathlib.Path.exists", return_value=False)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcBuilder._update_konflux_db")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcBuilder._start_build")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.DockerfileParser")
    async def test_build(
        self, MockDockerfileParser, MockBuildRepo, mock_start_build, mock_update_konflux_db: AsyncMock, mock_exists
    ):
        mock_konflux_client = self.kube_client
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        all_arches = metadata.get_arches.return_value = list(KonfluxClient.SUPPORTED_ARCHES.keys())
        build_repo = MockBuildRepo.return_value
        build_repo.local_dir = self.base_dir.joinpath(metadata.distgit_key)
        mock_konflux_client.start_pipeline_run_for_image_build = AsyncMock()
        mock_konflux_client.resource_url = MagicMock(return_value="https://example.com/pipeline")
        mock_pipelinerun_info = MagicMock()
        mock_pipelinerun_info.name = "test-pipelinerun-name"
        mock_pipelinerun_info.to_dict.return_value = {"metadata": {"name": "test-pipelinerun-name"}}
        mock_start_build.return_value = (
            mock_pipelinerun_info,
            "https://example.com/pipeline",
        )
        mock_pipelinerun = {
            "metadata": {"name": "test-pipelinerun-name"},
            "status": {"conditions": [{"type": "Succeeded", "status": "True"}]},
        }
        mock_konflux_client.wait_for_pipelinerun.return_value = PipelineRunInfo(mock_pipelinerun, {})

        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {
            "__doozer_version": "1.0.0",
            "__doozer_release": "1",
            "__doozer_bundle_nvrs": "foo-bundle-1.0.0-1",
        }
        mock_dfp.labels = {
            'com.redhat.art.name': 'test-distgit-key-fbc',
            'com.redhat.art.nvr': 'test-distgit-key-fbc-1.0.0-1',
        }

        await self.builder.build(metadata)
        MockBuildRepo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-ocp-4.9-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(strict=True)
        MockDockerfileParser.assert_called_once_with(
            str(self.base_dir.joinpath(metadata.distgit_key, "catalog.Dockerfile"))
        )
        mock_update_konflux_db.assert_has_awaits(
            [
                call(
                    metadata,
                    build_repo,
                    mock_start_build.return_value[0],
                    KonfluxBuildOutcome.PENDING,
                    all_arches,
                    logger=ANY,
                ),
                call(
                    metadata,
                    build_repo,
                    mock_konflux_client.wait_for_pipelinerun.return_value,
                    KonfluxBuildOutcome.SUCCESS,
                    all_arches,
                    logger=ANY,
                ),
            ]
        )
        mock_konflux_client.wait_for_pipelinerun.assert_called_once_with(
            "test-pipelinerun-name", namespace="test-namespace"
        )
        self.builder._record_logger.add_record.assert_called_once_with(
            "build_fbc_konflux",
            status=0,
            name='test-distgit-key-fbc',
            message='Success',
            task_id='test-pipelinerun-name',
            task_url='https://example.com/pipeline',
            fbc_nvr='test-distgit-key-fbc-1.0.0-1',
            bundle_nvrs='foo-bundle-1.0.0-1',
        )
        MockBuildRepo.from_local_dir.assert_not_called()

    @patch("pathlib.Path.exists", return_value=True)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcBuilder._update_konflux_db")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcBuilder._start_build")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.DockerfileParser")
    async def test_build_with_existing_repo(
        self, MockDockerfileParser, MockBuildRepo, mock_start_build, mock_update_konflux_db: AsyncMock, mock_exists
    ):
        mock_konflux_client = self.kube_client
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "9"
        all_arches = metadata.get_arches.return_value = list(KonfluxClient.SUPPORTED_ARCHES.keys())
        build_repo = MockBuildRepo.from_local_dir.return_value
        build_repo.local_dir = self.base_dir.joinpath(metadata.distgit_key)
        mock_konflux_client.start_pipeline_run_for_image_build = AsyncMock()
        mock_konflux_client.resource_url = MagicMock(return_value="https://example.com/pipeline")
        mock_pipelinerun_info = MagicMock()
        mock_pipelinerun_info.name = "test-pipelinerun-name"
        mock_pipelinerun_info.to_dict.return_value = {"metadata": {"name": "test-pipelinerun-name"}}
        mock_start_build.return_value = (
            mock_pipelinerun_info,
            "https://example.com/pipeline",
        )
        mock_pipelinerun = {
            "metadata": {"name": "test-pipelinerun-name"},
            "status": {"conditions": [{"type": "Succeeded", "status": "True"}]},
        }
        mock_konflux_client.wait_for_pipelinerun.return_value = PipelineRunInfo(mock_pipelinerun, {})

        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {
            "__doozer_version": "1.0.0",
            "__doozer_release": "1",
            "__doozer_bundle_nvrs": "foo-bundle-1.0.0-1",
        }
        mock_dfp.labels = {
            'com.redhat.art.name': 'test-distgit-key-fbc',
            'com.redhat.art.nvr': 'test-distgit-key-fbc-1.0.0-1',
        }

        await self.builder.build(metadata)
        MockBuildRepo.assert_not_called()
        MockDockerfileParser.assert_called_once_with(
            str(self.base_dir.joinpath(metadata.distgit_key, "catalog.Dockerfile"))
        )
        mock_update_konflux_db.assert_has_awaits(
            [
                call(
                    metadata,
                    build_repo,
                    mock_start_build.return_value[0],
                    KonfluxBuildOutcome.PENDING,
                    all_arches,
                    logger=ANY,
                ),
                call(
                    metadata,
                    build_repo,
                    mock_konflux_client.wait_for_pipelinerun.return_value,
                    KonfluxBuildOutcome.SUCCESS,
                    all_arches,
                    logger=ANY,
                ),
            ]
        )
        mock_konflux_client.wait_for_pipelinerun.assert_called_once_with(
            "test-pipelinerun-name", namespace="test-namespace"
        )
        self.builder._record_logger.add_record.assert_called_once_with(
            "build_fbc_konflux",
            status=0,
            name='test-distgit-key-fbc',
            message='Success',
            task_id='test-pipelinerun-name',
            task_url='https://example.com/pipeline',
            fbc_nvr='test-distgit-key-fbc-1.0.0-1',
            bundle_nvrs='foo-bundle-1.0.0-1',
        )
        MockBuildRepo.from_local_dir.assert_awaited_once_with(self.base_dir.joinpath(metadata.distgit_key), ANY)

    def test_extract_git_commits_from_nvrs(self):
        """Test extraction of git commits from bundle NVRs"""
        # Test single NVR with git commit
        bundle_nvrs = "cluster-nfd-operator-metadata-container-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1"
        result = self.builder._extract_git_commits_from_nvrs(bundle_nvrs)
        self.assertEqual(result, ["gd5498aa"])

        # Test multiple NVRs with different git commits
        bundle_nvrs = "operator1-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1,operator2-v4.12.0.202509242028.p2.gf1234b.assembly.stream.el8-1"
        result = self.builder._extract_git_commits_from_nvrs(bundle_nvrs)
        self.assertEqual(result, ["gd5498aa", "gf1234b"])

        # Test multiple NVRs with duplicate git commits (should deduplicate)
        bundle_nvrs = "operator1-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1,operator2-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1"
        result = self.builder._extract_git_commits_from_nvrs(bundle_nvrs)
        self.assertEqual(result, ["gd5498aa"])

        # Test NVR without git commit
        bundle_nvrs = "operator-without-commit-v4.12.0.202509242028.p2.assembly.stream.el8-1"
        result = self.builder._extract_git_commits_from_nvrs(bundle_nvrs)
        self.assertEqual(result, [])

        # Test empty string
        result = self.builder._extract_git_commits_from_nvrs("")
        self.assertEqual(result, [])

        # Test None
        result = self.builder._extract_git_commits_from_nvrs(None)
        self.assertEqual(result, [])

        # Test mixed case - some with commits, some without
        bundle_nvrs = "operator1-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1,operator2-without-commit-v4.12.0.202509242028.p2.assembly.stream.el8-1,operator3-v4.12.0.202509242028.p2.gf1234b.assembly.stream.el8-1"
        result = self.builder._extract_git_commits_from_nvrs(bundle_nvrs)
        self.assertEqual(result, ["gd5498aa", "gf1234b"])

        # Test with spaces around commas
        bundle_nvrs = "operator1-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1, operator2-v4.12.0.202509242028.p2.gf1234b.assembly.stream.el8-1"
        result = self.builder._extract_git_commits_from_nvrs(bundle_nvrs)
        self.assertEqual(result, ["gd5498aa", "gf1234b"])

    async def test_start_build_with_git_commits_and_version_override(self):
        """Test _start_build with git commits from bundle NVRs and major_minor_override for non-openshift groups"""
        # Set up a non-openshift group with major_minor_override and git commits
        self.builder.major_minor_override = (4, 12)
        self.builder.source_git_commits = ["gd5498aa", "gf1234b"]

        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "foo"
        metadata.runtime = MagicMock()
        metadata.runtime.assembly = "stream"
        metadata.runtime.group = "oadp-1.5"  # Non-openshift group
        metadata.config = MagicMock()
        metadata.config.delivery.delivery_repo_names = ["openshift4/oadp-operator"]

        build_repo = MagicMock(
            spec=BuildRepo, https_url="https://example.com/foo.git", branch="test-branch", commit_hash="deadbeef"
        )
        kube_client = self.kube_client
        kube_client.resource_url.return_value = "https://example.com/pipelinerun/test-pipeline-run-name"
        pplr = kube_client.start_pipeline_run_for_image_build.return_value = MagicMock(
            **{"metadata.name": "test-pipeline-run-name"},
        )

        result = await self.builder._start_build(
            metadata,
            build_repo,
            output_image="test-image-pullspec",
            arches=["x86_64", "s390x"],
            logger=self.logger,
        )

        # Verify the additional tags include version override and git commits
        expected_additional_tags = [
            "test-group__v4.12__oadp-operator",
            "test-group__v4.12__oadp-operator__gd5498aa",
            "test-group__v4.12__oadp-operator__gf1234b",
        ]

        kube_client.start_pipeline_run_for_image_build.assert_awaited_once_with(
            generate_name='fbc-test-group-foo-',
            namespace='test-namespace',
            application_name='fbc-test-group',
            component_name='fbc-test-group-foo',
            git_url='https://example.com/foo.git',
            commit_sha="deadbeef",
            target_branch='test-branch',
            output_image='test-image-pullspec',
            vm_override={},
            building_arches=['x86_64', 's390x'],
            additional_tags=expected_additional_tags,
            skip_checks=False,
            hermetic=True,
            dockerfile='catalog.Dockerfile',
            pipelinerun_template_url='https://example.com/template.yaml',
            build_priority='2',
        )
        self.assertEqual(result, (pplr, "https://example.com/pipelinerun/test-pipeline-run-name"))

    async def test_start_build_openshift_group(self):
        """Test _start_build with openshift group format"""
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "foo"
        metadata.runtime = MagicMock()
        metadata.runtime.assembly = "stream"
        metadata.runtime.group = "openshift-4.15"  # Openshift group
        metadata.config = MagicMock()
        metadata.config.delivery.delivery_repo_names = [
            "openshift4/cluster-nfd-operator",
            "openshift4/cluster-nfd-operator-bundle",
        ]

        build_repo = MagicMock(
            spec=BuildRepo, https_url="https://example.com/foo.git", branch="test-branch", commit_hash="deadbeef"
        )
        kube_client = self.kube_client
        kube_client.resource_url.return_value = "https://example.com/pipelinerun/test-pipeline-run-name"
        pplr = kube_client.start_pipeline_run_for_image_build.return_value = MagicMock(
            **{"metadata.name": "test-pipeline-run-name"},
        )

        result = await self.builder._start_build(
            metadata,
            build_repo,
            output_image="test-image-pullspec",
            arches=["x86_64", "s390x"],
            logger=self.logger,
        )

        # Verify the additional tags for openshift group (no version override, no git commits)
        expected_additional_tags = ["ocp__4.15__cluster-nfd-operator", "ocp__4.15__cluster-nfd-operator-bundle"]

        kube_client.start_pipeline_run_for_image_build.assert_awaited_once_with(
            generate_name='fbc-test-group-foo-',
            namespace='test-namespace',
            application_name='fbc-test-group',
            component_name='fbc-test-group-foo',
            git_url='https://example.com/foo.git',
            commit_sha="deadbeef",
            target_branch='test-branch',
            output_image='test-image-pullspec',
            vm_override={},
            building_arches=['x86_64', 's390x'],
            additional_tags=expected_additional_tags,
            skip_checks=False,
            hermetic=True,
            dockerfile='catalog.Dockerfile',
            pipelinerun_template_url='https://example.com/template.yaml',
            build_priority='2',
        )
        self.assertEqual(result, (pplr, "https://example.com/pipelinerun/test-pipeline-run-name"))


class TestKonfluxFbcFragmentMerger(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.working_dir = Path("/tmp/konflux_fbc_fragment_merger")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.db = MagicMock()
        self.fbc_repo = "https://example.com/fbc-repo.git"
        self.konflux_namespace = "test-namespace"
        self.konflux_kubeconfig = "/path/to/kube/config"
        self.konflux_context = None
        self.image_repo = "quay.io/test-repo"
        self.skip_checks = False
        self.pipelinerun_template_url = "https://example.com/template.yaml"
        self.dry_run = False
        self.record_logger = MagicMock()
        self.logger = MagicMock()

        with patch("doozerlib.backend.konflux_fbc.KonfluxClient", spec=KonfluxClient) as MockKonfluxClient:
            self.kube_client = MockKonfluxClient.from_kubeconfig.return_value = AsyncMock(spec=KonfluxClient)
            self.merger = KonfluxFbcFragmentMerger(
                working_dir=self.working_dir,
                group=self.group,
                group_config=MagicMock(),
                fbc_git_repo=self.fbc_repo,
                konflux_namespace=self.konflux_namespace,
                konflux_kubeconfig=self.konflux_kubeconfig,
                konflux_context=self.konflux_context,
                skip_checks=self.skip_checks,
                plr_template=self.pipelinerun_template_url,
                dry_run=self.dry_run,
                logger=self.logger,
            )

    @patch("doozerlib.util.oc_image_info_for_arch_async__caching", new_callable=AsyncMock)
    @patch("httpx.AsyncClient", autospec=True)
    async def test_merge_idms(self, mock_async_client: Mock, mock_oc_image_info_for_arch_async__caching: AsyncMock):
        http_client = mock_async_client.return_value.__aenter__.return_value = AsyncMock()
        http_client.get.side_effect = [
            MagicMock(
                text=f"""
kind: ImageDigestMirrorSet
apiVersion: config.openshift.io/v1
metadata:
  name: art-images-fbc-staging-index
  namespace: openshift-marketplace
spec:
  imageDigestMirrors:
    - source: example.com/source-a{idx}
      mirrors:
        - example.com/mirror-a{idx}
    - source: example.com/source-b{idx}
      mirrors:
        - example.com/mirror-b{idx}

"""
            )
            for idx in range(3)
        ]
        mock_oc_image_info_for_arch_async__caching.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "vcs-ref": "deadbeef",
                    },
                },
            },
        }

        expected = {
            'apiVersion': 'config.openshift.io/v1',
            'kind': 'ImageDigestMirrorSet',
            'metadata': {'name': 'art-stage-mirror-set', 'namespace': 'openshift-marketplace'},
            'spec': {
                'imageDigestMirrors': [
                    {'source': 'example.com/source-a0', 'mirrors': ['example.com/mirror-a0']},
                    {'source': 'example.com/source-a1', 'mirrors': ['example.com/mirror-a1']},
                    {'source': 'example.com/source-a2', 'mirrors': ['example.com/mirror-a2']},
                    {'source': 'example.com/source-b0', 'mirrors': ['example.com/mirror-b0']},
                    {'source': 'example.com/source-b1', 'mirrors': ['example.com/mirror-b1']},
                    {'source': 'example.com/source-b2', 'mirrors': ['example.com/mirror-b2']},
                ]
            },
        }

        result = await self.merger._merge_idms(["example.com/idm1", "example.com/idm2", "example.com/idm3"])
        self.assertEqual(result, expected)


class TestGenerateFbcBranchName(unittest.TestCase):
    """Test the _generate_fbc_branch_name function."""

    def test_openshift_group_branch_naming(self):
        """Test branch naming for OpenShift groups (traditional naming)."""
        branch_name = _generate_fbc_branch_name(
            group="openshift-4.17",
            assembly="stream",
            distgit_key="cluster-nfd-operator",
            ocp_version=(4, 17),  # Should be ignored for OpenShift groups
        )
        expected = "art-openshift-4.17-assembly-stream-fbc-cluster-nfd-operator"
        self.assertEqual(branch_name, expected)

    def test_non_openshift_group_branch_naming(self):
        """Test branch naming for non-OpenShift groups (includes OCP version)."""
        branch_name = _generate_fbc_branch_name(
            group="oadp-1.5", assembly="stream", distgit_key="oadp-operator", ocp_version=(4, 17)
        )
        expected = "art-oadp-1.5-ocp-4.17-assembly-stream-fbc-oadp-operator"
        self.assertEqual(branch_name, expected)

    def test_non_openshift_group_different_assembly(self):
        """Test branch naming for non-OpenShift groups with different assembly."""
        branch_name = _generate_fbc_branch_name(
            group="mta-1.2", assembly="4.17.2", distgit_key="mta-operator", ocp_version=(4, 17)
        )
        expected = "art-mta-1.2-ocp-4.17-assembly-4.17.2-fbc-mta-operator"
        self.assertEqual(branch_name, expected)

    def test_non_openshift_group_missing_ocp_version(self):
        """Test that missing ocp_version raises ValueError for non-OpenShift groups."""
        with self.assertRaises(ValueError) as cm:
            _generate_fbc_branch_name(
                group="oadp-1.5", assembly="stream", distgit_key="oadp-operator", ocp_version=None
            )
        self.assertIn("ocp_version is required for non-OpenShift group 'oadp-1.5'", str(cm.exception))
