import unittest
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
        self.runtime.group = "test-group"
        self.runtime.assembly = "test-assembly"
        self.runtime.upcycle = False
        self.runtime.source_resolver = mock.Mock(spec=SourceResolver)
        self.fbc_import_cli = FbcImportCli(
            runtime=self.runtime,
            index_image="example.com/test/test-index-image:latest",
            push=True,
            fbc_repo="https://example.com/test/fbc.git",
            message="Test commit",
            dest_dir="/tmp/fbc",
            registry_auth="/path/to/auth/file.json",
        )

    @mock.patch("doozerlib.cli.fbc.opm.verify_opm")
    @mock.patch("doozerlib.cli.fbc.KonfluxFbcImporter.import_from_index_image")
    async def test_run(self, mock_import_from_index_image: mock.Mock, verify_opm: mock.Mock):
        self.runtime.ordered_image_metas.return_value = [
            mock.MagicMock(
                is_olm_operator=True, distgit_key="foo", **{"get_olm_bundle_short_name.return_value": "foo-bundle"}
            ),
            mock.MagicMock(
                is_olm_operator=True, distgit_key="bar", **{"get_olm_bundle_short_name.return_value": "bar-bundle"}
            ),
        ]
        await self.fbc_import_cli.run()
        mock_import_from_index_image.assert_has_calls(
            [
                mock.call(self.runtime.ordered_image_metas()[0], "example.com/test/test-index-image:latest"),
                mock.call(self.runtime.ordered_image_metas()[1], "example.com/test/test-index-image:latest"),
            ]
        )
