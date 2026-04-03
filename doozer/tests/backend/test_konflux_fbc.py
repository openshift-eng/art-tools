import logging
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, call, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBundleBuildRecord
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.backend.konflux_fbc import (
    AssemblyBundleCsvInfo,
    KonfluxFbcBuilder,
    KonfluxFbcFragmentMerger,
    KonfluxFbcImporter,
    KonfluxFbcRebaser,
    _fetch_csv_from_git,
    _generate_fbc_branch_name,
)
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo
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
            strict=True,
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
            strict=True,
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
            "test-index-image-pullspec",
            migrate_level="none",
            auth=OpmRegistryAuth(path='/path/to/auth.json'),
            strict=True,
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

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcImporter._render_index_image", new_callable=AsyncMock)
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
        mock_render_index_image.assert_called_once_with(index_image, migrate_level="none", strict=True)

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
        # Non-OpenShift group: release gets OCP version suffix
        expected_release = "1.ocp4.9"
        mock_rebase_dir.return_value = f"test-distgit-key-fbc-1.0.0-{expected_release}"

        actual = await self.rebaser.rebase(metadata, bundle_build, version, release)
        self.assertEqual(actual, f"test-distgit-key-fbc-1.0.0-{expected_release}")

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-ocp-4.9-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_rebase_dir.assert_called_once_with(metadata, build_repo, bundle_build, version, expected_release, ANY)
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
        # Non-OpenShift group: release gets OCP version suffix
        expected_release = "1.ocp4.9"
        mock_rebase_dir.return_value = f"test-distgit-key-fbc-1.0.0-{expected_release}"
        self.rebaser.push = True

        actual = await self.rebaser.rebase(metadata, bundle_build, version, release)
        self.assertEqual(actual, f"test-distgit-key-fbc-1.0.0-{expected_release}")

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-ocp-4.9-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=False)
        mock_rebase_dir.assert_called_once_with(metadata, build_repo, bundle_build, version, expected_release, ANY)
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
        result_catalog_blobs = self.rebaser._catagorize_catalog_blobs(result_catalog_blobs)
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

    @patch("doozerlib.util.oc_image_info_for_arch_async", new_callable=AsyncMock)
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
        actual = self.rebaser._catagorize_catalog_blobs(catalog_blobs)
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

    def test_extract_version_from_bundle_name(self):
        """Test version extraction from bundle names with 'v' prefix"""
        from semver import VersionInfo

        # Test standard format with v prefix
        version = self.rebaser._extract_version_from_bundle_name("oadp-operator.v1.3.9")
        self.assertEqual(version, VersionInfo(1, 3, 9))

        # Test another version
        version = self.rebaser._extract_version_from_bundle_name("oadp-operator.v1.4.4")
        self.assertEqual(version, VersionInfo(1, 4, 4))

        # Test without v prefix (backward compatibility for tests)
        version = self.rebaser._extract_version_from_bundle_name("test-bundle-name.v2.1.0")
        self.assertEqual(version, VersionInfo(2, 1, 0))

        # Test invalid format - should raise ValueError
        with self.assertRaises(ValueError) as cm:
            self.rebaser._extract_version_from_bundle_name("invalid-name")
        self.assertIn("Cannot parse semantic version", str(cm.exception))

        # Test malformed version - should raise ValueError
        with self.assertRaises(ValueError) as cm:
            self.rebaser._extract_version_from_bundle_name("operator.vX.Y.Z")
        self.assertIn("Cannot parse semantic version", str(cm.exception))

    def test_rebuild_replaces_chain(self):
        """Test rebuilding replaces chain based on sorted entry order"""
        # Test with three entries in sorted order
        entries = [
            {"name": "operator.v1.0.0", "replaces": "old-operator"},
            {"name": "operator.v1.1.0"},
            {"name": "operator.v1.2.0", "replaces": "operator.v1.0.0"},
        ]

        self.rebaser._rebuild_replaces_chain(entries)

        # First entry should have no replaces
        self.assertNotIn('replaces', entries[0])

        # Second entry should replace first
        self.assertEqual(entries[1]['replaces'], 'operator.v1.0.0')

        # Third entry should replace second
        self.assertEqual(entries[2]['replaces'], 'operator.v1.1.0')

    def test_rebuild_replaces_chain_single_entry(self):
        """Test rebuilding replaces chain with single entry"""
        entries = [{"name": "operator.v1.0.0", "replaces": "old-operator"}]

        self.rebaser._rebuild_replaces_chain(entries)

        # Single entry should have no replaces
        self.assertNotIn('replaces', entries[0])

    def test_rebuild_replaces_chain_empty(self):
        """Test rebuilding replaces chain with empty list"""
        entries = []

        # Should not raise an error
        self.rebaser._rebuild_replaces_chain(entries)

        self.assertEqual(len(entries), 0)

    def test_resolve_default_channel_multiple_versioned(self):
        """Highest semver channel wins among same-prefix versioned channels"""
        channels = ["stable-6.2", "stable-6.3", "stable-6.4"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.3"),
            "stable-6.4",
        )

    def test_resolve_default_channel_single_versioned(self):
        """Single versioned channel returns itself"""
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(["release-1.8"], "release-1.8"),
            "release-1.8",
        )

    def test_resolve_default_channel_non_versioned(self):
        """Non-versioned channel names are returned unchanged"""
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(["stable", "candidate"], "stable"),
            "stable",
        )

    def test_resolve_default_channel_with_v_prefix(self):
        """Channels using v-prefixed versions (stable-v8.0) are resolved correctly"""
        channels = ["stable-v8.0", "stable-v8.1"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-v8.0"),
            "stable-v8.1",
        )

    def test_resolve_default_channel_ignores_different_prefix(self):
        """Only channels sharing the same prefix are compared"""
        channels = ["stable-6.3", "stable-6.4", "candidate-4.18"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.3"),
            "stable-6.4",
        )

    def test_resolve_default_channel_default_already_highest(self):
        """When the bundle label's channel is already the highest, it is returned"""
        channels = ["stable-6.2", "stable-6.3", "stable-6.4"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.4"),
            "stable-6.4",
        )

    def test_resolve_default_channel_major_only(self):
        """Channels with only a major version component are handled"""
        channels = ["release-5", "release-6"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "release-5"),
            "release-6",
        )

    def test_resolve_default_channel_empty_list(self):
        """Empty channel list returns original default unchanged"""
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel([], "stable-6.3"),
            "stable-6.3",
        )

    def test_resolve_default_channel_all_invalid_versions(self):
        """When all versions are invalid, returns original default"""
        channels = ["stable-invalid", "stable-bad", "stable-xyz"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.3"),
            "stable-6.3",
        )

    def test_resolve_default_channel_default_not_in_list(self):
        """Default channel not in list still finds highest version"""
        channels = ["stable-6.4", "stable-6.5"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.3"),
            "stable-6.5",
        )

    def test_resolve_default_channel_mixed_valid_invalid(self):
        """Mixed valid and invalid versions skips invalid ones"""
        channels = ["stable-6.2", "stable-invalid", "stable-6.4", "stable-bad"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.3"),
            "stable-6.4",
        )

    def test_resolve_default_channel_four_part_version(self):
        """Versions with 4+ parts use first 3 for comparison"""
        channels = ["stable-1.2.3.4", "stable-1.2.4"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-1.2.3"),
            "stable-1.2.4",
        )

    def test_resolve_default_channel_duplicates(self):
        """Duplicate channel names don't break comparison"""
        channels = ["stable-6.3", "stable-6.3", "stable-6.2"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.2"),
            "stable-6.3",
        )

    def test_resolve_default_channel_no_matching_prefix(self):
        """No channels with matching prefix returns original default"""
        channels = ["candidate-4.18", "preview-4.19"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.3"),
            "stable-6.3",
        )

    def test_resolve_default_channel_different_component_counts(self):
        """Version normalization handles different component counts"""
        channels = ["stable-6", "stable-6.3.1"]
        self.assertEqual(
            KonfluxFbcRebaser._resolve_default_channel(channels, "stable-6.3"),
            "stable-6.3.1",
        )

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
    async def test_rebase_with_insert_missing_entry(
        self,
        mock_fetch_olm_bundle_blob,
        mock_fetch_olm_bundle_image_info,
        mock_open,
        mock_is_file,
        mock_mkdir,
        MockDockerfileParser,
        mock_get_referenced_images,
        mock_load_csv_from_bundle,
        mock_path_unlink,
        mock_generate_dockerfile,
    ):
        """Test that insert_missing_entry flag causes version-ordered sorting"""
        # Create rebaser with insert_missing_entry=True
        rebaser = KonfluxFbcRebaser(
            base_dir=self.base_dir,
            group=self.group,
            assembly=self.assembly,
            version="1.3.9",
            release="1",
            commit_message="test",
            push=False,
            fbc_repo="https://example.com/fbc.git",
            upcycle=False,
            insert_missing_entry=True,
        )

        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-operator"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "14"
        metadata.runtime.konflux_db = MagicMock()
        metadata.config = MagicMock()
        metadata.config.delivery = MagicMock()
        metadata.config.delivery.delivery_repo_names = ["test-repo"]
        metadata.get_olm_bundle_delivery_repo_name.return_value = "test-repo"

        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir

        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="test-operator-bundle-1.3.9-1",
            operator_nvr="test-operator-1.3.9-1",
            operand_nvrs=[],
            image_pullspec="example.com/test-bundle@sha256:abc",
            image_tag="abc123",
            source_repo="https://example.com/test-operator.git",
            commitish="abc123def",
        )

        version = "1.3.9"
        release = "1"
        logger = MagicMock()

        mock_fetch_olm_bundle_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "name": "test-operator",
                        "operators.operatorframework.io.bundle.channels.v1": "stable",
                        "operators.operatorframework.io.bundle.channel.default.v1": "stable",
                        "operators.operatorframework.io.bundle.package.v1": "test-package",
                        "operators.operatorframework.io.bundle.manifests.v1": "manifests/",
                    },
                },
            },
        }

        mock_fetch_olm_bundle_blob.return_value = (
            "test-operator.v1.3.9",
            "test-package",
            {
                "schema": "olm.bundle",
                "name": "test-operator.v1.3.9",
                "package": "test-package",
                "properties": [],
                "relatedImages": [{"name": "", "image": "example.com/test-bundle@sha256:abc"}],
            },
        )

        # Set up catalog with entries in wrong order: [v1.0.0, v1.4.4]
        # After adding v1.3.9, they should be sorted to [v1.0.0, v1.3.9, v1.4.4]
        org_catalog_blobs = [
            {"schema": "olm.package", "name": "test-package", "defaultChannel": "stable"},
            {
                "schema": "olm.channel",
                "name": "stable",
                "package": "test-package",
                "entries": [
                    {"name": "test-operator.v1.0.0"},
                    {"name": "test-operator.v1.4.4"},
                ],
            },
            {
                "schema": "olm.bundle",
                "name": "test-operator.v1.0.0",
                "package": "test-package",
                "properties": [],
                "relatedImages": [],
            },
        ]

        org_catalog_file = StringIO()
        yaml.dump_all(org_catalog_blobs, org_catalog_file)
        org_catalog_file.seek(0)
        result_catalog_file = StringIO()
        images_mirror_set_file = StringIO()
        mock_open.return_value.__enter__.side_effect = [org_catalog_file, result_catalog_file, images_mirror_set_file]

        mock_get_referenced_images.return_value = []
        mock_load_csv_from_bundle.return_value = {
            "metadata": {"name": "test-operator", "annotations": {}},
            "spec": {"icon": []},
        }

        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {}
        mock_dfp.labels = {}

        mock_generate_dockerfile.return_value = None

        # Run the rebase
        await rebaser._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)

        # Verify the entries are sorted by version
        result_catalog_file.seek(0)
        result_catalog_blobs = list(yaml.load_all(result_catalog_file))
        result_catalog_blobs = rebaser._catagorize_catalog_blobs(result_catalog_blobs)

        entries = result_catalog_blobs["test-package"]["olm.channel"]["stable"]["entries"]
        entry_names = [e["name"] for e in entries]

        # Verify correct sorted order: v1.0.0 < v1.3.9 < v1.4.4
        self.assertEqual(entry_names, ["test-operator.v1.0.0", "test-operator.v1.3.9", "test-operator.v1.4.4"])

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
    async def test_rebase_with_insert_missing_entry_replaces_chain(
        self,
        mock_fetch_olm_bundle_blob,
        mock_fetch_olm_bundle_image_info,
        mock_open,
        mock_is_file,
        mock_mkdir,
        MockDockerfileParser,
        mock_get_referenced_images,
        mock_load_csv_from_bundle,
        mock_path_unlink,
        mock_generate_dockerfile,
    ):
        """Test that insert_missing_entry rebuilds replaces chain for non-OpenShift groups"""
        # Create rebaser with non-OpenShift group and insert_missing_entry=True
        rebaser = KonfluxFbcRebaser(
            base_dir=self.base_dir,
            group="oadp-1.3",  # Non-OpenShift group that uses replaces
            assembly=self.assembly,
            version="1.3.8",
            release="1",
            commit_message="test",
            push=False,
            fbc_repo="https://example.com/fbc.git",
            upcycle=False,
            insert_missing_entry=True,
        )

        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "oadp-operator"
        # Mock the runtime and group_config structure
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "1"
        metadata.runtime.group_config.vars.MINOR = "3"
        metadata.runtime.konflux_db = MagicMock()
        metadata.config = MagicMock()
        metadata.config.delivery = MagicMock()
        metadata.config.delivery.delivery_repo_names = ["oadp-repo"]
        metadata.get_olm_bundle_delivery_repo_name.return_value = "oadp-repo"

        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir

        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="oadp-operator-bundle-1.3.8-1",
            operator_nvr="oadp-operator-1.3.8-1",
            operand_nvrs=[],
            image_pullspec="example.com/oadp-bundle@sha256:abc",
            image_tag="abc123",
            source_repo="https://example.com/oadp-operator.git",
            commitish="abc123def",
        )

        version = "1.3.8"
        release = "1"
        logger = MagicMock()

        mock_fetch_olm_bundle_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "name": "oadp-operator",
                        "operators.operatorframework.io.bundle.channels.v1": "stable",
                        "operators.operatorframework.io.bundle.channel.default.v1": "stable",
                        "operators.operatorframework.io.bundle.package.v1": "oadp-package",
                        "operators.operatorframework.io.bundle.manifests.v1": "manifests/",
                    },
                },
            },
        }

        mock_fetch_olm_bundle_blob.return_value = (
            "oadp-operator.v1.3.8",
            "oadp-package",
            {
                "schema": "olm.bundle",
                "name": "oadp-operator.v1.3.8",
                "package": "oadp-package",
                "properties": [],
                "relatedImages": [{"name": "", "image": "example.com/oadp-bundle@sha256:abc"}],
            },
        )

        # Set up catalog with entries: [v1.3.7, v1.3.9 (replaces v1.3.7)]
        # After adding v1.3.8 and sorting, should become:
        # [v1.3.7 (no replaces), v1.3.8 (replaces v1.3.7), v1.3.9 (replaces v1.3.8)]
        org_catalog_blobs = [
            {"schema": "olm.package", "name": "oadp-package", "defaultChannel": "stable"},
            {
                "schema": "olm.channel",
                "name": "stable",
                "package": "oadp-package",
                "entries": [
                    {"name": "oadp-operator.v1.3.7"},
                    {"name": "oadp-operator.v1.3.9", "replaces": "oadp-operator.v1.3.7"},
                ],
            },
            {
                "schema": "olm.bundle",
                "name": "oadp-operator.v1.3.7",
                "package": "oadp-package",
                "properties": [],
                "relatedImages": [],
            },
        ]

        org_catalog_file = StringIO()
        yaml.dump_all(org_catalog_blobs, org_catalog_file)
        org_catalog_file.seek(0)
        result_catalog_file = StringIO()
        images_mirror_set_file = StringIO()
        mock_open.return_value.__enter__.side_effect = [org_catalog_file, result_catalog_file, images_mirror_set_file]

        mock_get_referenced_images.return_value = []
        mock_load_csv_from_bundle.return_value = {
            "metadata": {"name": "oadp-operator", "annotations": {}},
            "spec": {"icon": []},
        }

        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {}
        mock_dfp.labels = {}

        mock_generate_dockerfile.return_value = None

        # Run the rebase
        await rebaser._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)

        # Verify the entries are sorted by version with correct replaces chain
        result_catalog_file.seek(0)
        result_catalog_blobs = list(yaml.load_all(result_catalog_file))
        result_catalog_blobs = rebaser._catagorize_catalog_blobs(result_catalog_blobs)

        entries = result_catalog_blobs["oadp-package"]["olm.channel"]["stable"]["entries"]

        # Verify correct sorted order: v1.3.7 < v1.3.8 < v1.3.9
        self.assertEqual(len(entries), 3)

        # v1.3.7 should be first with no replaces
        self.assertEqual(entries[0]['name'], 'oadp-operator.v1.3.7')
        self.assertNotIn('replaces', entries[0])

        # v1.3.8 should be second and replace v1.3.7
        self.assertEqual(entries[1]['name'], 'oadp-operator.v1.3.8')
        self.assertEqual(entries[1]['replaces'], 'oadp-operator.v1.3.7')

        # v1.3.9 should be third and replace v1.3.8
        self.assertEqual(entries[2]['name'], 'oadp-operator.v1.3.9')
        self.assertEqual(entries[2]['replaces'], 'oadp-operator.v1.3.8')

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
    async def test_rebase_dir_resolves_highest_default_channel(
        self,
        mock_fetch_olm_bundle_blob,
        mock_fetch_olm_bundle_image_info,
        mock_open,
        mock_is_file,
        mock_mkdir,
        MockDockerfileParser,
        mock_get_referenced_images,
        mock_load_csv_from_bundle,
        mock_path_unlink,
        mock_generate_dockerfile,
    ):
        """For non-openshift groups, defaultChannel resolves to the highest versioned channel"""
        rebaser = KonfluxFbcRebaser(
            base_dir=self.base_dir,
            group="logging-6.3",
            assembly=self.assembly,
            version="6.3.4",
            release="1",
            commit_message="test",
            push=False,
            fbc_repo="https://example.com/fbc.git",
            upcycle=False,
        )

        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "cluster-logging-operator"
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "6"
        metadata.runtime.group_config.vars.MINOR = "3"
        metadata.runtime.konflux_db = MagicMock()
        metadata.config = MagicMock()
        metadata.config.delivery = MagicMock()
        metadata.config.delivery.delivery_repo_names = ["logging-repo"]
        metadata.get_olm_bundle_delivery_repo_name.return_value = "logging-repo"

        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir
        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="cluster-logging-operator-bundle-6.3.4-1",
            operator_nvr="cluster-logging-operator-6.3.4-1",
            operand_nvrs=[],
            image_pullspec="example.com/logging-bundle@sha256:abc",
            image_tag="abc123",
            source_repo="https://example.com/cluster-logging-operator.git",
            commitish="abc123def",
        )
        logger = MagicMock()

        mock_fetch_olm_bundle_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "name": "cluster-logging-operator",
                        "operators.operatorframework.io.bundle.channels.v1": "stable-6.3",
                        "operators.operatorframework.io.bundle.channel.default.v1": "stable-6.3",
                        "operators.operatorframework.io.bundle.package.v1": "cluster-logging",
                        "operators.operatorframework.io.bundle.manifests.v1": "manifests/",
                    },
                },
            },
        }

        mock_fetch_olm_bundle_blob.return_value = (
            "cluster-logging.v6.3.4",
            "cluster-logging",
            {
                "schema": "olm.bundle",
                "name": "cluster-logging.v6.3.4",
                "package": "cluster-logging",
                "properties": [],
                "relatedImages": [{"name": "", "image": "example.com/logging-bundle@sha256:abc"}],
            },
        )

        org_catalog_blobs = [
            {"schema": "olm.package", "name": "cluster-logging", "defaultChannel": "stable-6.3"},
            {
                "schema": "olm.channel",
                "name": "stable-6.2",
                "package": "cluster-logging",
                "entries": [{"name": "cluster-logging.v6.2.9"}],
            },
            {
                "schema": "olm.channel",
                "name": "stable-6.3",
                "package": "cluster-logging",
                "entries": [{"name": "cluster-logging.v6.3.3"}],
            },
            {
                "schema": "olm.channel",
                "name": "stable-6.4",
                "package": "cluster-logging",
                "entries": [{"name": "cluster-logging.v6.4.3"}],
            },
            {
                "schema": "olm.bundle",
                "name": "cluster-logging.v6.2.9",
                "package": "cluster-logging",
                "properties": [],
                "relatedImages": [],
            },
        ]

        org_catalog_file = StringIO()
        yaml.dump_all(org_catalog_blobs, org_catalog_file)
        org_catalog_file.seek(0)
        result_catalog_file = StringIO()
        images_mirror_set_file = StringIO()
        mock_open.return_value.__enter__.side_effect = [org_catalog_file, result_catalog_file, images_mirror_set_file]

        mock_get_referenced_images.return_value = []
        mock_load_csv_from_bundle.return_value = {
            "metadata": {"name": "cluster-logging-operator", "annotations": {}},
            "spec": {"icon": []},
        }

        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {}
        mock_dfp.labels = {}

        mock_generate_dockerfile.return_value = None

        await rebaser._rebase_dir(metadata, build_repo, bundle_build, "6.3.4", "1", logger)

        result_catalog_file.seek(0)
        result_catalog_blobs = list(yaml.load_all(result_catalog_file))
        result_catalog_blobs = rebaser._catagorize_catalog_blobs(result_catalog_blobs)

        package_blob = result_catalog_blobs["cluster-logging"]["olm.package"]["cluster-logging"]
        self.assertEqual(package_blob["defaultChannel"], "stable-6.4")

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
    async def test_rebase_dir_openshift_group_no_resolution(
        self,
        mock_fetch_olm_bundle_blob,
        mock_fetch_olm_bundle_image_info,
        mock_open,
        mock_is_file,
        mock_mkdir,
        MockDockerfileParser,
        mock_get_referenced_images,
        mock_load_csv_from_bundle,
        mock_path_unlink,
        mock_generate_dockerfile,
    ):
        """For openshift groups, defaultChannel is NOT resolved (remains as-is)"""
        rebaser = KonfluxFbcRebaser(
            base_dir=self.base_dir,
            group="openshift-4.17",
            assembly=self.assembly,
            version="4.17.4",
            release="1",
            commit_message="test",
            push=False,
            fbc_repo="https://example.com/fbc.git",
            upcycle=False,
        )

        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-operator"
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "4"
        metadata.runtime.group_config.vars.MINOR = "17"
        metadata.runtime.konflux_db = MagicMock()
        metadata.config = MagicMock()
        metadata.config.delivery = MagicMock()
        metadata.config.delivery.delivery_repo_names = ["test-repo"]
        metadata.get_olm_bundle_delivery_repo_name.return_value = "test-repo"

        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir
        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="test-operator-bundle-4.17.4-1",
            operator_nvr="test-operator-4.17.4-1",
            operand_nvrs=[],
            image_pullspec="example.com/test-bundle@sha256:abc",
            image_tag="abc123",
            source_repo="https://example.com/test-operator.git",
            commitish="abc123def",
        )
        logger = MagicMock()

        mock_fetch_olm_bundle_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "name": "test-operator",
                        "operators.operatorframework.io.bundle.channels.v1": "stable-6.3",
                        "operators.operatorframework.io.bundle.channel.default.v1": "stable-6.3",
                        "operators.operatorframework.io.bundle.package.v1": "test-package",
                        "operators.operatorframework.io.bundle.manifests.v1": "manifests/",
                    },
                },
            },
        }

        mock_fetch_olm_bundle_blob.return_value = (
            "test-package.v4.17.4",
            "test-package",
            {
                "schema": "olm.bundle",
                "name": "test-package.v4.17.4",
                "package": "test-package",
                "properties": [],
                "relatedImages": [{"name": "", "image": "example.com/test-bundle@sha256:abc"}],
            },
        )

        org_catalog_blobs = [
            {"schema": "olm.package", "name": "test-package", "defaultChannel": "stable-6.3"},
            {
                "schema": "olm.channel",
                "name": "stable-6.2",
                "package": "test-package",
                "entries": [{"name": "test-package.v6.2.9"}],
            },
            {
                "schema": "olm.channel",
                "name": "stable-6.3",
                "package": "test-package",
                "entries": [{"name": "test-package.v6.3.3"}],
            },
            {
                "schema": "olm.channel",
                "name": "stable-6.4",
                "package": "test-package",
                "entries": [{"name": "test-package.v6.4.3"}],
            },
        ]

        org_catalog_file = StringIO()
        yaml.dump_all(org_catalog_blobs, org_catalog_file)
        org_catalog_file.seek(0)
        result_catalog_file = StringIO()
        images_mirror_set_file = StringIO()
        mock_open.return_value.__enter__.side_effect = [org_catalog_file, result_catalog_file, images_mirror_set_file]

        mock_get_referenced_images.return_value = []
        mock_load_csv_from_bundle.return_value = {
            "metadata": {"name": "test-operator", "annotations": {}},
            "spec": {"icon": []},
        }

        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {}
        mock_dfp.labels = {}

        mock_generate_dockerfile.return_value = None

        await rebaser._rebase_dir(metadata, build_repo, bundle_build, "4.17.4", "1", logger)

        result_catalog_file.seek(0)
        result_catalog_blobs = list(yaml.load_all(result_catalog_file))
        result_catalog_blobs = rebaser._catagorize_catalog_blobs(result_catalog_blobs)

        package_blob = result_catalog_blobs["test-package"]["olm.package"]["test-package"]
        self.assertEqual(package_blob["defaultChannel"], "stable-6.3")

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
    async def test_rebase_dir_logs_default_channel_change(
        self,
        mock_fetch_olm_bundle_blob,
        mock_fetch_olm_bundle_image_info,
        mock_open,
        mock_is_file,
        mock_mkdir,
        MockDockerfileParser,
        mock_get_referenced_images,
        mock_load_csv_from_bundle,
        mock_path_unlink,
        mock_generate_dockerfile,
    ):
        """Logging occurs when defaultChannel is set"""
        rebaser = KonfluxFbcRebaser(
            base_dir=self.base_dir,
            group="logging-6.3",
            assembly=self.assembly,
            version="6.3.4",
            release="1",
            commit_message="test",
            push=False,
            fbc_repo="https://example.com/fbc.git",
            upcycle=False,
        )

        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "cluster-logging-operator"
        metadata.runtime = MagicMock()
        metadata.runtime.group_config = MagicMock()
        metadata.runtime.group_config.vars = MagicMock()
        metadata.runtime.group_config.vars.MAJOR = "6"
        metadata.runtime.group_config.vars.MINOR = "3"
        metadata.runtime.konflux_db = MagicMock()
        metadata.config = MagicMock()
        metadata.config.delivery = MagicMock()
        metadata.config.delivery.delivery_repo_names = ["logging-repo"]
        metadata.get_olm_bundle_delivery_repo_name.return_value = "logging-repo"

        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir
        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="cluster-logging-operator-bundle-6.3.4-1",
            operator_nvr="cluster-logging-operator-6.3.4-1",
            operand_nvrs=[],
            image_pullspec="example.com/logging-bundle@sha256:abc",
            image_tag="abc123",
            source_repo="https://example.com/cluster-logging-operator.git",
            commitish="abc123def",
        )
        logger = MagicMock()

        mock_fetch_olm_bundle_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "name": "cluster-logging-operator",
                        "operators.operatorframework.io.bundle.channels.v1": "stable-6.3",
                        "operators.operatorframework.io.bundle.channel.default.v1": "stable-6.3",
                        "operators.operatorframework.io.bundle.package.v1": "cluster-logging",
                        "operators.operatorframework.io.bundle.manifests.v1": "manifests/",
                    },
                },
            },
        }

        mock_fetch_olm_bundle_blob.return_value = (
            "cluster-logging.v6.3.4",
            "cluster-logging",
            {
                "schema": "olm.bundle",
                "name": "cluster-logging.v6.3.4",
                "package": "cluster-logging",
                "properties": [],
                "relatedImages": [{"name": "", "image": "example.com/logging-bundle@sha256:abc"}],
            },
        )

        org_catalog_blobs = [
            {"schema": "olm.package", "name": "cluster-logging", "defaultChannel": "stable-6.3"},
            {
                "schema": "olm.channel",
                "name": "stable-6.2",
                "package": "cluster-logging",
                "entries": [{"name": "cluster-logging.v6.2.9"}],
            },
            {
                "schema": "olm.channel",
                "name": "stable-6.3",
                "package": "cluster-logging",
                "entries": [{"name": "cluster-logging.v6.3.3"}],
            },
            {
                "schema": "olm.channel",
                "name": "stable-6.4",
                "package": "cluster-logging",
                "entries": [{"name": "cluster-logging.v6.4.3"}],
            },
        ]

        org_catalog_file = StringIO()
        yaml.dump_all(org_catalog_blobs, org_catalog_file)
        org_catalog_file.seek(0)
        result_catalog_file = StringIO()
        images_mirror_set_file = StringIO()
        mock_open.return_value.__enter__.side_effect = [org_catalog_file, result_catalog_file, images_mirror_set_file]

        mock_get_referenced_images.return_value = []
        mock_load_csv_from_bundle.return_value = {
            "metadata": {"name": "cluster-logging-operator", "annotations": {}},
            "spec": {"icon": []},
        }

        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {}
        mock_dfp.labels = {}

        mock_generate_dockerfile.return_value = None

        await rebaser._rebase_dir(metadata, build_repo, bundle_build, "6.3.4", "1", logger)

        logger.info.assert_any_call("Setting default channel to %s", "stable-6.4")


class TestFetchCsvFromGit(unittest.IsolatedAsyncioTestCase):
    """Test the _fetch_csv_from_git helper function."""

    @patch("doozerlib.backend.konflux_fbc.shutil.rmtree")
    @patch("doozerlib.backend.konflux_fbc.artlib_util.ensure_github_https_url")
    @patch("artcommonlib.git_helper.run_git_async", new_callable=AsyncMock)
    async def test_fetch_csv_from_git_success(self, mock_run_git, mock_convert_url, mock_rmtree):
        """Test successful CSV extraction from git."""
        with (
            patch("pathlib.Path.exists") as mock_exists,
            patch("pathlib.Path.rglob") as mock_rglob,
            patch(
                "builtins.open",
                unittest.mock.mock_open(
                    read_data="metadata:\n  name: test-csv\n  annotations:\n    olm.skipRange: '>=4.8.0 <4.17.0'"
                ),
            ),
        ):
            mock_exists.side_effect = [
                False,
                True,
                True,
            ]  # repo_dir doesn't exist, manifests exists, repo exists for cleanup
            mock_convert_url.return_value = "https://github.com/openshift-priv/test-operator"

            mock_csv_file = MagicMock()
            mock_csv_file.name = "test.clusterserviceversion.yaml"
            mock_csv_file.open = unittest.mock.mock_open(read_data="metadata:\n  name: test-csv")
            mock_rglob.return_value = [mock_csv_file]

            logger = MagicMock()
            work_dir = Path("/tmp/test")

            mock_run_git.return_value = None

            await _fetch_csv_from_git(
                git_url="https://github.com/openshift-priv/test-operator",
                revision="abc123",
                work_dir=work_dir,
                logger=logger,
            )

            mock_convert_url.assert_called_once_with("https://github.com/openshift-priv/test-operator")

    @patch("doozerlib.backend.konflux_fbc.shutil.rmtree")
    @patch("doozerlib.backend.konflux_fbc.artlib_util.ensure_github_https_url")
    @patch("artcommonlib.git_helper.run_git_async", new_callable=AsyncMock)
    async def test_fetch_csv_from_git_no_manifests_dir(self, mock_run_git, mock_convert_url, mock_rmtree):
        """Test when manifests directory doesn't exist."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.side_effect = [False, False, True]
            mock_convert_url.return_value = "https://github.com/openshift-priv/test-operator"

            logger = MagicMock()
            work_dir = Path("/tmp/test")

            result = await _fetch_csv_from_git(
                git_url="https://github.com/openshift-priv/test-operator",
                revision="abc123",
                work_dir=work_dir,
                logger=logger,
            )

            # Should return None when manifests dir doesn't exist
            self.assertIsNone(result)


class TestKonfluxFbcRebaserAssemblyMethods(unittest.IsolatedAsyncioTestCase):
    """Test the assembly-related methods in KonfluxFbcRebaser."""

    def setUp(self):
        self.base_dir = Path("/tmp/konflux_fbc_rebaser")
        self.group = "openshift-4.20"
        self.assembly = "stream"
        self.version = "1.0.0"
        self.release = "1"
        self.commit_message = "Test rebase commit message"
        self.push = False
        self.fbc_repo = "https://example.com/fbc-repo.git"
        self.upcycle = False

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

    @patch("doozerlib.backend.konflux_fbc.assembly_config_struct")
    @patch("doozerlib.backend.konflux_fbc.assembly_type")
    def test_find_future_release_assembly_found(self, mock_assembly_type, mock_assembly_config_struct):
        """Test finding a future release assembly."""
        mock_assembly_type.return_value = AssemblyTypes.STANDARD
        releases_config = MagicMock()
        releases_config.releases.keys.return_value = ["4.20.10", "4.20.11", "4.20.12"]

        mock_assembly_config_struct.side_effect = lambda rc, name, key, default: {
            "4.20.10": {"release_date": "2024-01-01"},
            "4.20.11": {"release_date": "2099-12-31"},
            "4.20.12": {"release_date": "2099-12-31"},
        }.get(name, default)

        with patch("doozerlib.backend.konflux_fbc.artlib_util.is_future_release_date") as mock_is_future:
            mock_is_future.side_effect = lambda d: d == "2099-12-31"
            result = self.rebaser._find_future_release_assembly(releases_config)

        self.assertEqual(result, "4.20.11")

    @patch("doozerlib.backend.konflux_fbc.assembly_config_struct")
    @patch("doozerlib.backend.konflux_fbc.assembly_type")
    def test_find_future_release_assembly_not_found(self, mock_assembly_type, mock_assembly_config_struct):
        """Test when no future release assembly exists."""
        mock_assembly_type.return_value = AssemblyTypes.STANDARD
        releases_config = MagicMock()
        releases_config.releases.keys.return_value = ["4.20.10", "4.20.11"]

        mock_assembly_config_struct.side_effect = lambda rc, name, key, default: {
            "4.20.10": {"release_date": "2024-01-01"},
            "4.20.11": {"release_date": "2024-06-01"},
        }.get(name, default)

        with patch("doozerlib.backend.konflux_fbc.artlib_util.is_future_release_date") as mock_is_future:
            mock_is_future.return_value = False
            result = self.rebaser._find_future_release_assembly(releases_config)

        self.assertIsNone(result)

    def test_find_future_release_assembly_empty_releases(self):
        """Test when releases config is empty."""
        releases_config = MagicMock()
        releases_config.releases.keys.return_value = []

        result = self.rebaser._find_future_release_assembly(releases_config)
        self.assertIsNone(result)

    @patch("doozerlib.backend.konflux_fbc.assembly_config_struct")
    @patch("doozerlib.backend.konflux_fbc.assembly_type")
    def test_find_future_release_assembly_skips_non_standard(self, mock_assembly_type, mock_assembly_config_struct):
        """Test that non-standard assemblies are skipped."""
        mock_assembly_type.side_effect = lambda rc, name: {
            "artXYZ": AssemblyTypes.CUSTOM,
            "4.20.11": AssemblyTypes.STANDARD,
        }.get(name, AssemblyTypes.STREAM)
        releases_config = MagicMock()
        releases_config.releases.keys.return_value = ["artXYZ", "4.20.11"]

        mock_assembly_config_struct.side_effect = lambda rc, name, key, default: {
            "artXYZ": {"release_date": "2099-12-31"},
            "4.20.11": {"release_date": "2099-12-31"},
        }.get(name, default)

        with patch("doozerlib.backend.konflux_fbc.artlib_util.is_future_release_date") as mock_is_future:
            mock_is_future.return_value = True
            result = self.rebaser._find_future_release_assembly(releases_config)

        self.assertEqual(result, "4.20.11")

    def test_get_shipment_url_for_assembly_found(self):
        """Test getting shipment URL for an assembly."""
        releases_config = MagicMock()
        releases_config.releases.get.return_value = {
            "assembly": {
                "group": {
                    "shipment": {
                        "url": "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/337"
                    }
                }
            }
        }

        result = self.rebaser._get_shipment_url_for_assembly(releases_config, "4.20.13")
        self.assertEqual(
            result, "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data/-/merge_requests/337"
        )

    def test_get_shipment_url_for_assembly_not_found(self):
        """Test when shipment URL doesn't exist."""
        releases_config = MagicMock()
        releases_config.releases.get.return_value = {"assembly": {"group": {}}}

        result = self.rebaser._get_shipment_url_for_assembly(releases_config, "4.20.13")
        self.assertIsNone(result)

    def test_get_shipment_url_for_assembly_missing_assembly(self):
        """Test when assembly doesn't exist in releases."""
        releases_config = MagicMock()
        releases_config.releases.get.return_value = {}

        result = self.rebaser._get_shipment_url_for_assembly(releases_config, "nonexistent")
        self.assertIsNone(result)

    def test_find_component_in_shipment_found(self):
        """Test finding a component in shipment snapshot."""
        shipment = MagicMock()

        component1 = MagicMock()
        component1.name = "other-operator-bundle"
        component2 = MagicMock()
        component2.name = "nfd-operator-bundle"

        shipment.shipment.snapshot.spec.components = [component1, component2]

        result = self.rebaser._find_component_in_shipment(shipment, "nfd-operator")
        self.assertEqual(result, component2)

    def test_find_component_in_shipment_by_operator_name(self):
        """Test finding a component by exact operator name (no -bundle suffix)."""
        shipment = MagicMock()

        component1 = MagicMock()
        component1.name = "other-operator-bundle"
        component2 = MagicMock()
        component2.name = "cluster-nfd-operator"

        shipment.shipment.snapshot.spec.components = [component1, component2]

        result = self.rebaser._find_component_in_shipment(shipment, "cluster-nfd-operator")
        self.assertEqual(result, component2)

    def test_find_component_in_shipment_prefers_bundle_suffix(self):
        """Test that -bundle suffix match is preferred over bare operator name."""
        shipment = MagicMock()

        bare_component = MagicMock()
        bare_component.name = "nfd-operator"
        bundle_component = MagicMock()
        bundle_component.name = "nfd-operator-bundle"

        shipment.shipment.snapshot.spec.components = [bare_component, bundle_component]

        result = self.rebaser._find_component_in_shipment(shipment, "nfd-operator")
        self.assertEqual(result, bundle_component)

    def test_find_component_in_shipment_not_found(self):
        """Test when component is not found in shipment."""
        shipment = MagicMock()

        component1 = MagicMock()
        component1.name = "other-operator-bundle"

        shipment.shipment.snapshot.spec.components = [component1]

        result = self.rebaser._find_component_in_shipment(shipment, "nfd-operator")
        self.assertIsNone(result)

    def test_find_component_in_shipment_empty(self):
        """Test when shipment snapshot is None."""
        shipment = MagicMock()
        shipment.shipment.snapshot = None

        result = self.rebaser._find_component_in_shipment(shipment, "nfd-operator")
        self.assertIsNone(result)

    @patch("doozerlib.backend.konflux_fbc._fetch_csv_from_git", new_callable=AsyncMock)
    async def test_extract_csv_info_from_bundle_success(self, mock_fetch_csv):
        """Test extracting CSV info from a bundle component."""
        bundle_component = MagicMock()
        bundle_component.name = "nfd-operator-bundle"
        bundle_component.source.git.url = "https://github.com/openshift-priv/nfd-operator"
        bundle_component.source.git.revision = "abc123"

        mock_fetch_csv.return_value = {
            "metadata": {
                "name": "nfd.4.20.0-202601280915",
                "annotations": {"olm.skipRange": ">=4.8.0 <4.20.0"},
            }
        }

        result = await self.rebaser._extract_csv_info_from_bundle(bundle_component)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], "nfd.4.20.0-202601280915")
        self.assertEqual(result[1], ">=4.8.0 <4.20.0")

    @patch("doozerlib.backend.konflux_fbc._fetch_csv_from_git", new_callable=AsyncMock)
    async def test_extract_csv_info_from_bundle_no_skip_range(self, mock_fetch_csv):
        """Test extracting CSV info when skipRange is not present."""
        bundle_component = MagicMock()
        bundle_component.name = "nfd-operator-bundle"
        bundle_component.source.git.url = "https://github.com/openshift-priv/nfd-operator"
        bundle_component.source.git.revision = "abc123"

        mock_fetch_csv.return_value = {
            "metadata": {
                "name": "nfd.4.20.0-202601280915",
                "annotations": {},
            }
        }

        result = await self.rebaser._extract_csv_info_from_bundle(bundle_component)

        self.assertIsNotNone(result)
        self.assertEqual(result[0], "nfd.4.20.0-202601280915")
        self.assertIsNone(result[1])

    @patch("doozerlib.backend.konflux_fbc._fetch_csv_from_git", new_callable=AsyncMock)
    async def test_extract_csv_info_from_bundle_fetch_failed(self, mock_fetch_csv):
        """Test when fetching CSV from git fails."""
        bundle_component = MagicMock()
        bundle_component.name = "nfd-operator-bundle"
        bundle_component.source.git.url = "https://github.com/openshift-priv/nfd-operator"
        bundle_component.source.git.revision = "abc123"

        mock_fetch_csv.return_value = None

        result = await self.rebaser._extract_csv_info_from_bundle(bundle_component)
        self.assertIsNone(result)

    @patch("doozerlib.backend.konflux_fbc.opm.render", new_callable=AsyncMock)
    async def test_get_bundle_blob_from_fbc_success(self, mock_render):
        """Test getting bundle blob from FBC image."""
        fbc_component = MagicMock()
        fbc_component.containerImage = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123"

        mock_render.return_value = [
            {"schema": "olm.package", "name": "nfd"},
            {"schema": "olm.channel", "name": "stable", "package": "nfd"},
            {"schema": "olm.bundle", "name": "nfd.4.20.0-202601280915", "package": "nfd"},
            {"schema": "olm.bundle", "name": "nfd.4.19.0", "package": "nfd"},
        ]

        result = await self.rebaser._get_bundle_blob_from_fbc(fbc_component, "nfd.4.20.0-202601280915")

        self.assertIsNotNone(result)
        self.assertEqual(result["schema"], "olm.bundle")
        self.assertEqual(result["name"], "nfd.4.20.0-202601280915")

    @patch("doozerlib.backend.konflux_fbc.opm.render", new_callable=AsyncMock)
    async def test_get_bundle_blob_from_fbc_not_found(self, mock_render):
        """Test when bundle blob is not found in FBC image."""
        fbc_component = MagicMock()
        fbc_component.containerImage = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123"

        mock_render.return_value = [
            {"schema": "olm.package", "name": "nfd"},
            {"schema": "olm.bundle", "name": "nfd.4.19.0", "package": "nfd"},
        ]

        result = await self.rebaser._get_bundle_blob_from_fbc(fbc_component, "nfd.4.20.0-nonexistent")
        self.assertIsNone(result)

    @patch("doozerlib.backend.konflux_fbc.opm.render", new_callable=AsyncMock)
    async def test_get_bundle_blob_from_fbc_empty_blobs(self, mock_render):
        """Test when FBC image returns no blobs."""
        fbc_component = MagicMock()
        fbc_component.containerImage = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abc123"

        mock_render.return_value = []

        result = await self.rebaser._get_bundle_blob_from_fbc(fbc_component, "nfd.4.20.0")
        self.assertIsNone(result)

    @patch("doozerlib.backend.konflux_fbc.get_shipment_config_from_mr")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._get_bundle_blob_from_fbc", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._extract_csv_info_from_bundle", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._find_component_in_shipment")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._get_shipment_url_for_assembly")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._find_future_release_assembly")
    async def test_get_assembly_bundle_csv_info_stream_assembly(
        self,
        mock_find_future,
        mock_get_shipment_url,
        mock_find_component,
        mock_extract_csv,
        mock_get_bundle_blob,
        mock_get_shipment_config,
    ):
        """Test getting assembly bundle CSV info for stream assembly."""
        metadata = MagicMock()
        metadata.distgit_key = "nfd-operator"
        metadata.runtime.get_releases_config.return_value = MagicMock()

        # Setup mocks
        mock_find_future.return_value = "4.20.13"
        mock_get_shipment_url.return_value = "https://gitlab.example.com/mr/123"

        bundle_component = MagicMock()
        bundle_component.name = "nfd-operator-bundle"
        fbc_component = MagicMock()
        fbc_component.containerImage = "quay.io/test@sha256:abc"

        mock_find_component.side_effect = [bundle_component, fbc_component]
        mock_extract_csv.return_value = ("nfd.4.20.0-202601280915", ">=4.8.0 <4.20.0")
        mock_get_bundle_blob.return_value = {"schema": "olm.bundle", "name": "nfd.4.20.0-202601280915"}

        result = await self.rebaser._get_assembly_bundle_csv_info(metadata)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, AssemblyBundleCsvInfo)
        self.assertEqual(result.csv_name, "nfd.4.20.0-202601280915")
        self.assertEqual(result.skip_range, ">=4.8.0 <4.20.0")
        self.assertIsNotNone(result.bundle_blob)

    @patch("doozerlib.backend.konflux_fbc.get_shipment_config_from_mr")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._get_bundle_blob_from_fbc", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._extract_csv_info_from_bundle", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._find_component_in_shipment")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._get_shipment_url_for_assembly")
    async def test_get_assembly_bundle_csv_info_named_assembly(
        self,
        mock_get_shipment_url,
        mock_find_component,
        mock_extract_csv,
        mock_get_bundle_blob,
        mock_get_shipment_config,
    ):
        """Test getting assembly bundle CSV info for named assembly."""
        # Create rebaser with named assembly
        rebaser = KonfluxFbcRebaser(
            base_dir=self.base_dir,
            group=self.group,
            assembly="4.20.13",  # Named assembly
            version=self.version,
            release=self.release,
            commit_message=self.commit_message,
            push=self.push,
            fbc_repo=self.fbc_repo,
            upcycle=self.upcycle,
        )

        metadata = MagicMock()
        metadata.distgit_key = "nfd-operator"
        metadata.runtime.get_releases_config.return_value = MagicMock()

        mock_get_shipment_url.return_value = "https://gitlab.example.com/mr/123"

        bundle_component = MagicMock()
        fbc_component = MagicMock()
        fbc_component.containerImage = "quay.io/test@sha256:abc"

        mock_find_component.side_effect = [bundle_component, fbc_component]
        mock_extract_csv.return_value = ("nfd.4.20.13", None)
        mock_get_bundle_blob.return_value = {"schema": "olm.bundle", "name": "nfd.4.20.13"}

        result = await rebaser._get_assembly_bundle_csv_info(metadata)

        self.assertIsNotNone(result)
        self.assertEqual(result.csv_name, "nfd.4.20.13")
        self.assertIsNone(result.skip_range)

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._find_future_release_assembly")
    async def test_get_assembly_bundle_csv_info_no_future_release(self, mock_find_future):
        """Test when no future release is found for stream assembly."""
        metadata = MagicMock()
        metadata.distgit_key = "nfd-operator"
        metadata.runtime.get_releases_config.return_value = MagicMock()

        mock_find_future.return_value = None

        result = await self.rebaser._get_assembly_bundle_csv_info(metadata)
        self.assertIsNone(result)

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._get_shipment_url_for_assembly")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._find_future_release_assembly")
    async def test_get_assembly_bundle_csv_info_no_shipment_url(self, mock_find_future, mock_get_shipment_url):
        """Test when shipment URL is not found."""
        metadata = MagicMock()
        metadata.distgit_key = "nfd-operator"
        metadata.runtime.get_releases_config.return_value = MagicMock()

        mock_find_future.return_value = "4.20.13"
        mock_get_shipment_url.return_value = None

        result = await self.rebaser._get_assembly_bundle_csv_info(metadata)
        self.assertIsNone(result)


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
        metadata.get_konflux_build_attempts.return_value = 3
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
        metadata.get_konflux_build_attempts.return_value = 3
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

    @patch("doozerlib.util.oc_image_info_for_arch_async", new_callable=AsyncMock)
    @patch("httpx.AsyncClient", autospec=True)
    async def test_merge_idms(self, mock_async_client: Mock, mock_oc_image_info_for_arch_async: AsyncMock):
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
        mock_oc_image_info_for_arch_async.return_value = {
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
