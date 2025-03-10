import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBundleBuildRecord

from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_fbc import KonfluxFbcImporter, KonfluxFbcRebaser
from doozerlib.image import ImageMetadata
from doozerlib.opm import yaml


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
            upcycle=self.upcycle
        )

    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._rebase_dir")
    @patch("doozerlib.backend.konflux_fbc.BuildRepo", spec=BuildRepo)
    @patch("doozerlib.backend.konflux_fbc.opm")
    async def test_rebase(self, mock_opm, mock_build_repo, mock_rebase_dir):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
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

        await self.rebaser.rebase(metadata, bundle_build, version, release)

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=True)
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
        self.rebaser.push = True

        await self.rebaser.rebase(metadata, bundle_build, version, release)

        mock_build_repo.assert_called_once_with(
            url=self.fbc_repo,
            branch="art-test-group-assembly-test-assembly-fbc-test-distgit-key",
            local_dir=self.base_dir.joinpath(metadata.distgit_key),
            logger=ANY,
        )
        build_repo.ensure_source.assert_called_once_with(upcycle=self.upcycle, strict=True)
        mock_rebase_dir.assert_called_once_with(metadata, build_repo, bundle_build, version, release, ANY)
        mock_opm.validate.assert_called_once_with(self.base_dir.joinpath(metadata.distgit_key, "catalog"))
        build_repo.commit.assert_called_once_with(ANY, allow_empty=True)
        build_repo.push.assert_called_once()

    @patch("doozerlib.backend.konflux_fbc.DockerfileParser")
    @patch("pathlib.Path.is_file", return_value=True)
    @patch("pathlib.Path.open")
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._fetch_olm_bundle_image_info", new_callable=AsyncMock)
    @patch("doozerlib.backend.konflux_fbc.KonfluxFbcRebaser._fetch_olm_bundle_blob", new_callable=AsyncMock)
    async def test_rebase_dir(self, mock_fetch_olm_bundle_blob, mock_fetch_olm_bundle_image_info, mock_open, mock_is_file, MockDockerfileParser):
        metadata = MagicMock(spec=ImageMetadata)
        metadata.distgit_key = "test-distgit-key"
        build_repo = MagicMock()
        build_repo.local_dir = self.base_dir
        bundle_build = MagicMock(
            spec=KonfluxBundleBuildRecord,
            nvr="foo-bundle-1.0.0-1",
            image_pullspec="dev.example.com/foo-bundle:1.0.0",
            image_tag="deadbeef",
            source_repo="https://example.com/foo-operator.git",
            commitish="beefdead",
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
                        "operators.operatorframework.io.bundle.package.v1": "test-package"
                    }
                }
            }
        }
        mock_fetch_olm_bundle_blob.return_value = (
            "test-bundle-name.1.2.3", "test-package", {
                "schema": "olm.bundle",
                "name": "test-bundle-name.1.2.3",
                "package": "test-package",
                "properties": [{
                    "type": "olm.csv.metadata",
                    "value": {
                        "annotations": {
                            "olm.skipRange": ">=4.8.0 <4.17.0"
                        }
                    }
                }],
                "relatedImages": [
                    {"name": "", "image": "example.com/test-bundle:1.2.3"},
                    {"name": "test-operator", "image": "example.com/test-operator:1.2.3"}
                ]
            })
        mock_dfp = MockDockerfileParser.return_value
        mock_dfp.envs = {}
        mock_dfp.labels = {}

        org_catalog_blobs = [
            {"schema": "olm.package", "name": "test-package", "defaultChannel": "test-default-channel"},
            {"schema": "olm.channel", "name": "test-channel", "package": "test-package", "entries": [
                {"name": "test-bundle-name.1.0.0", "skipRange": ">=4.8.0 <4.17.0"},
                {"name": "test-bundle-name.1.0.1", "skipRange": ">=4.8.0 <4.17.0"},
                {"name": "test-bundle-name.1.1.0", "skipRange": ">=4.8.0 <4.17.0", "skips": ["test-bundle-name.0.0.9", "test-bundle-name.1.0.0", "test-bundle-name.1.0.1"]}
            ]},
            {"schema": "olm.bundle", "name": "test-bundle-name.1.0.0", "package": "test-package", "properties": [], "relatedImages": [
                {"name": "", "image": "example.com/openshift/test-bundle:1.0.0"},
                {"name": "test-operator", "image": "example.com/test-operator:1.0.0"}
            ]}
        ]
        org_catalog_file = StringIO()
        yaml.dump_all(org_catalog_blobs, org_catalog_file)
        org_catalog_file.seek(0)
        result_catalog_file = StringIO()
        mock_open.return_value.__enter__.side_effect = [org_catalog_file, result_catalog_file]

        await self.rebaser._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)

        mock_fetch_olm_bundle_image_info.assert_called_once_with(bundle_build)
        mock_fetch_olm_bundle_blob.assert_called_once_with(bundle_build)
        self.assertDictContainsSubset({
            "__doozer_group": "test-group",
            "__doozer_key": "test-distgit-key",
            "__doozer_version": "1.0.0",
            "__doozer_release": "1",
            "__doozer_bundle_nvrs": "foo-bundle-1.0.0-1",
        }, mock_dfp.envs)
        self.assertDictContainsSubset({
            "io.openshift.build.source-location": "https://example.com/foo-operator.git",
            "io.openshift.build.commit.id": "beefdead",
        }, mock_dfp.labels)

        result_catalog_file.seek(0)
        result_catalog_blobs = list(yaml.load_all(result_catalog_file))
        result_catalog_blobs = self.rebaser._catagorize_catalog_blobs(result_catalog_blobs)
        self.assertEqual(result_catalog_blobs.keys(), {"test-package"})
        self.assertEqual(result_catalog_blobs["test-package"]["olm.channel"]["test-channel"]["entries"], [
            {"name": "test-bundle-name.1.0.0", "skipRange": ">=4.8.0 <4.17.0"},
            {"name": "test-bundle-name.1.0.1", "skipRange": ">=4.8.0 <4.17.0"},
            {"name": "test-bundle-name.1.1.0", "skipRange": ">=4.8.0 <4.17.0"},
            {"name": "test-bundle-name.1.2.3", "skipRange": ">=4.8.0 <4.17.0", "skips": ["test-bundle-name.0.0.9", "test-bundle-name.1.0.0", "test-bundle-name.1.0.1", "test-bundle-name.1.1.0"]}
        ])
        self.assertEqual(result_catalog_blobs["test-package"]["olm.bundle"].keys(), {"test-bundle-name.1.0.0", "test-bundle-name.1.2.3"})

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
                        "operators.operatorframework.io.bundle.package.v1": "test-package"
                    }
                }
            }
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
        mock_render.return_value = [{
            "schema": "olm.bundle",
            "name": "test-bundle-name",
            "package": "test-package",
            "properties": [{"type": "olm.csv.metadata", "value": {"annotations": {}}}]
        }]
        actual = await self.rebaser._fetch_olm_bundle_blob(bundle_build)
        self.assertEqual(actual, ("test-bundle-name", "test-package", {"schema": "olm.bundle", "name": "test-bundle-name", "package": "test-package", "properties": [{"type": "olm.csv.metadata", "value": {"annotations": {}}}]}))
        mock_render.assert_called_once_with("test-image-pullspec", migrate=True, auth=ANY)

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
        self.assertEqual(actual["test-package"]["olm.package"]["test-package"], {"schema": "olm.package", "name": "test-package"})
        self.assertEqual(actual["test-package"]["olm.channel"]["test-channel"], {"schema": "olm.channel", "name": "test-channel", "package": "test-package"})
        self.assertEqual(actual["test-package"]["olm.bundle"]["test-bundle"], {"schema": "olm.bundle", "name": "test-bundle", "package": "test-package"})
        self.assertEqual(actual["test-package2"].keys(), {"olm.package", "olm.channel", "olm.bundle"})
        self.assertEqual(actual["test-package2"]["olm.package"]["test-package2"], {"schema": "olm.package", "name": "test-package2"})
        self.assertEqual(actual["test-package2"]["olm.channel"]["test-channel2"], {"schema": "olm.channel", "name": "test-channel2", "package": "test-package2"})
        self.assertEqual(actual["test-package2"]["olm.bundle"]["test-bundle2"], {"schema": "olm.bundle", "name": "test-bundle2", "package": "test-package2"})
