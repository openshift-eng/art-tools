import unittest
from io import StringIO
from pathlib import Path
from unittest import mock

from artcommonlib.model import Model

from doozerlib.cli.fbc import FbcImportCli
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolver


class TestFbcImportCli(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group_config.vars = Model({"MAJOR": 4, "MINOR": 17})
        self.runtime.working_dir = "/tmp"
        self.runtime.source_resolver = mock.Mock(spec=SourceResolver)
        self.fbc_import_cli = FbcImportCli(runtime=self.runtime, index_image=None, dest_dir="/tmp/fbc")

    async def test_run(self):
        self.runtime.ordered_image_metas.return_value = [
            mock.Mock(is_olm_operator=True, distgit_key="foo", **{"get_olm_bundle_short_name.return_value": "foo-bundle"}),
        ]
        with mock.patch("doozerlib.opm.verify_opm", return_value=None), \
                mock.patch("doozerlib.opm.render", return_value=[]), \
                mock.patch("doozerlib.opm.generate_basic_template", return_value=None), \
                mock.patch("doozerlib.opm.render_catalog_from_template", return_value=None), \
                mock.patch("doozerlib.opm.generate_dockerfile", return_value=None), \
                mock.patch("doozerlib.cli.fbc.FbcImportCli.load_package_names", return_value=["foo-package", "bar-package"]), \
                mock.patch("shutil.rmtree", return_value=None):
            await self.fbc_import_cli.run()

    @mock.patch("pathlib.Path.open")
    @mock.patch("pathlib.Path.glob")
    async def test_load_package_names(self, mock_glob, mock_open):
        operator_metas = [
            mock.Mock(distgit_key="foo", **{
                "get_olm_bundle_short_name.return_value": "foo-bundle",
                "config": {"update-csv": {"manifests-dir": "manifests/"}},
            }),
            mock.Mock(distgit_key="bar", **{
                "get_olm_bundle_short_name.return_value": "bar-bundle",
                "config": {"update-csv": {"manifests-dir": "manifests/"}},
            }),
        ]
        source_resolver = mock.Mock(spec=SourceResolver)
        source_resolver.get_source_dir.side_effect = [
            Path("/path/to/sources/foo/"),
            Path("/path/to/sources/bar/"),
        ]
        source_resolver.resolve_source.side_effect = [
            {"package.yaml": {"name": "foo-package"}},
            {"package.yaml": {"name": "bar-package"}},
        ]
        mock_glob.side_effect = [
            iter([Path("/path/to/sources/foo/manifests/foo-package.yaml")]),
            iter([Path("/path/to/sources/foo/manifests/bar-package.yaml")]),
        ]
        mock_open.return_value.__enter__.side_effect = [
            StringIO("packageName: foo-package"),
            StringIO("packageName: bar-package"),
        ]
        package_names = await self.fbc_import_cli.load_package_names(operator_metas, source_resolver)
        self.assertListEqual(package_names, ["foo-package", "bar-package"])

    def test_filter_catalog_blobs(self):
        blobs = [
            {"schema": "olm.package", "name": "package1"},
            {"schema": "olm.channel", "package": "package1"},
            {"schema": "olm.bundle", "package": "package2"},
            {"schema": "olm.deprecations", "package": "package3"},
        ]
        allowed_package_names = {"package1", "package2"}
        filtered = self.fbc_import_cli.filter_catalog_blobs(blobs, allowed_package_names)
        self.assertIn("package1", filtered)
        self.assertIn("package2", filtered)
        self.assertNotIn("package3", filtered)
        self.assertEqual(len(filtered["package1"]), 2)
        self.assertEqual(len(filtered["package2"]), 1)

        blobs = [
            {"schema": "olm.package", "name": "package1"},
            {"schema": "unknown.schema"},
        ]
        with self.assertRaises(IOError, msg="Couldn't determine package name for unknown schema: unknown.schema"):
            self.fbc_import_cli.filter_catalog_blobs(blobs, allowed_package_names)
