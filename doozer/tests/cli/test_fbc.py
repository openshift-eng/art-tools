import unittest
from unittest import mock

from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.model import Model
from doozerlib.cli.fbc import FbcImportCli, FbcRebaseAndBuildCli
from doozerlib.exceptions import DoozerFatalError
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
            keep_templates=False,
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


class TestFbcRebaseAndBuildCli(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group_config.vars = Model({"MAJOR": 4, "MINOR": 17})
        self.runtime.working_dir = "/tmp"
        self.runtime.group = "test-group"
        self.runtime.assembly = "test-assembly"
        self.runtime.upcycle = False
        self.runtime.source_resolver = mock.Mock(spec=SourceResolver)
        self.runtime.konflux_db = mock.AsyncMock()
        self.runtime.record_logger = mock.Mock()

        self.fbc_cli = FbcRebaseAndBuildCli(
            runtime=self.runtime,
            version="1.0.0",
            release="1",
            commit_message="Test commit",
            fbc_repo="https://example.com/test/fbc.git",
            operator_nvrs=(),
            konflux_kubeconfig="/path/to/kubeconfig",
            konflux_context="test-context",
            konflux_namespace="test-namespace",
            image_repo="test-repo",
            skip_checks=False,
            plr_template="test-template",
            dry_run=False,
            force=False,
            output="json",
        )

    def _create_mock_operator_build(self, name: str, nvr: str) -> KonfluxBuildRecord:
        build = mock.Mock(spec=KonfluxBuildRecord)
        build.name = name
        build.nvr = nvr
        return build

    def _create_mock_bundle_build(self, name: str, nvr: str, operator_nvr: str) -> KonfluxBundleBuildRecord:
        build = mock.Mock(spec=KonfluxBundleBuildRecord)
        build.name = name
        build.nvr = nvr
        build.operator_nvr = operator_nvr
        return build

    @mock.patch("doozerlib.cli.fbc.KonfluxFbcBuilder")
    @mock.patch("doozerlib.cli.fbc.KonfluxFbcRebaser")
    async def test_run_with_operator_nvrs(self, mock_rebaser_class, mock_builder_class):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")
        bundle_build = self._create_mock_bundle_build(
            "test-operator-bundle", "test-operator-bundle-1.0.0-1", "test-operator-1.0.0-1"
        )

        self.runtime.konflux_db.get_build_records_by_nvrs.return_value = [operator_build]
        self.runtime.images = ["test-operator"]
        self.runtime.image_map = {"test-operator": mock.Mock(is_olm_operator=True, distgit_key="test-operator")}

        async def mock_bundle_search(*args, **kwargs):
            yield bundle_build

        async def mock_fbc_search(*args, **kwargs):
            # Empty async generator
            if False:
                yield

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search
        self.fbc_cli._fbc_db.search_builds_by_fields = mock_fbc_search

        mock_rebaser = mock.AsyncMock()
        mock_rebaser.rebase.return_value = "test-fbc-1.0.0-1"
        mock_rebaser_class.return_value = mock_rebaser

        mock_builder = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        self.fbc_cli.operator_nvrs = ("test-operator-1.0.0-1",)

        await self.fbc_cli.run()

        mock_rebaser.rebase.assert_called_once()
        mock_builder.build.assert_called_once()

    @mock.patch("doozerlib.cli.fbc.KonfluxFbcBuilder")
    @mock.patch("doozerlib.cli.fbc.KonfluxFbcRebaser")
    async def test_run_without_operator_nvrs(self, mock_rebaser_class, mock_builder_class):
        operator_meta = mock.Mock(is_olm_operator=True, distgit_key="test-operator")
        operator_meta.get_latest_build = mock.AsyncMock(
            return_value=self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")
        )

        self.runtime.ordered_image_metas.return_value = [operator_meta]
        self.runtime.image_map = {"test-operator": operator_meta}
        bundle_build = self._create_mock_bundle_build(
            "test-operator-bundle", "test-operator-bundle-1.0.0-1", "test-operator-1.0.0-1"
        )

        async def mock_bundle_search(*args, **kwargs):
            yield bundle_build

        async def mock_fbc_search(*args, **kwargs):
            # Empty async generator
            if False:
                yield

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search
        self.fbc_cli._fbc_db.search_builds_by_fields = mock_fbc_search

        mock_rebaser = mock.AsyncMock()
        mock_rebaser.rebase.return_value = "test-fbc-1.0.0-1"
        mock_rebaser_class.return_value = mock_rebaser

        mock_builder = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        await self.fbc_cli.run()

        mock_rebaser.rebase.assert_called_once()
        mock_builder.build.assert_called_once()

    @mock.patch("doozerlib.cli.fbc.KonfluxFbcBuilder")
    @mock.patch("doozerlib.cli.fbc.KonfluxFbcRebaser")
    async def test_run_existing_fbc_build_found_no_force(self, mock_rebaser_class, mock_builder_class):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")
        bundle_build = self._create_mock_bundle_build(
            "test-operator-bundle", "test-operator-bundle-1.0.0-1", "test-operator-1.0.0-1"
        )
        existing_fbc = mock.Mock(spec=KonfluxFbcBuildRecord, nvr="test-fbc-1.0.0-1")

        self.runtime.konflux_db.get_build_records_by_nvrs.return_value = [operator_build]
        self.runtime.images = ["test-operator"]
        self.runtime.image_map = {
            "test-operator": mock.Mock(is_olm_operator=True, distgit_key="test-operator", name="test-operator")
        }

        async def mock_bundle_search(*args, **kwargs):
            yield bundle_build

        async def mock_fbc_search(*args, **kwargs):
            yield existing_fbc

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search
        self.fbc_cli._fbc_db.search_builds_by_fields = mock_fbc_search

        mock_rebaser = mock.AsyncMock()
        mock_rebaser_class.return_value = mock_rebaser

        mock_builder = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        self.fbc_cli.operator_nvrs = ("test-operator-1.0.0-1",)
        self.fbc_cli.force = False

        await self.fbc_cli.run()

        mock_rebaser.rebase.assert_not_called()
        mock_builder.build.assert_not_called()

    @mock.patch("doozerlib.cli.fbc.KonfluxFbcBuilder")
    @mock.patch("doozerlib.cli.fbc.KonfluxFbcRebaser")
    async def test_run_existing_fbc_build_found_with_force(self, mock_rebaser_class, mock_builder_class):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")
        bundle_build = self._create_mock_bundle_build(
            "test-operator-bundle", "test-operator-bundle-1.0.0-1", "test-operator-1.0.0-1"
        )
        existing_fbc = mock.Mock(spec=KonfluxFbcBuildRecord, nvr="test-fbc-1.0.0-1")

        self.runtime.konflux_db.get_build_records_by_nvrs.return_value = [operator_build]
        self.runtime.images = ["test-operator"]
        self.runtime.image_map = {
            "test-operator": mock.Mock(is_olm_operator=True, distgit_key="test-operator", name="test-operator")
        }

        async def mock_bundle_search(*args, **kwargs):
            yield bundle_build

        async def mock_fbc_search(*args, **kwargs):
            yield existing_fbc

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search
        self.fbc_cli._fbc_db.search_builds_by_fields = mock_fbc_search

        mock_rebaser = mock.AsyncMock()
        mock_rebaser.rebase.return_value = "test-fbc-1.0.0-2"
        mock_rebaser_class.return_value = mock_rebaser

        mock_builder = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        self.fbc_cli.operator_nvrs = ("test-operator-1.0.0-1",)
        self.fbc_cli.force = True

        await self.fbc_cli.run()

        mock_rebaser.rebase.assert_called_once()
        mock_builder.build.assert_called_once()

    @mock.patch("doozerlib.cli.fbc.KonfluxFbcBuilder")
    @mock.patch("doozerlib.cli.fbc.KonfluxFbcRebaser")
    async def test_run_two_operators_one_bundle_found(self, mock_rebaser_class, mock_builder_class):
        operator_build1 = self._create_mock_operator_build("test-operator-1", "test-operator-1-1.0.0-1")
        operator_build2 = self._create_mock_operator_build("test-operator-2", "test-operator-2-1.0.0-1")
        bundle_build1 = self._create_mock_bundle_build(
            "test-operator-1-bundle", "test-operator-1-bundle-1.0.0-1", "test-operator-1-1.0.0-1"
        )

        self.runtime.konflux_db.get_build_records_by_nvrs.return_value = [operator_build1, operator_build2]
        self.runtime.images = ["test-operator-1", "test-operator-2"]
        self.runtime.image_map = {
            "test-operator-1": mock.Mock(is_olm_operator=True, distgit_key="test-operator-1", name="test-operator-1"),
            "test-operator-2": mock.Mock(is_olm_operator=True, distgit_key="test-operator-2", name="test-operator-2"),
        }

        async def mock_bundle_search(*args, **kwargs):
            where = kwargs.get('where', {})
            if where.get('name') == 'test-operator-1-bundle':
                yield bundle_build1
            # For other cases, this becomes an empty async generator

        async def mock_fbc_search(*args, **kwargs):
            # Empty async generator
            if False:
                yield

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search
        self.fbc_cli._fbc_db.search_builds_by_fields = mock_fbc_search

        mock_rebaser = mock.AsyncMock()
        mock_rebaser.rebase.return_value = "test-operator-1-fbc-1.0.0-1"
        mock_rebaser_class.return_value = mock_rebaser

        mock_builder = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        self.fbc_cli.operator_nvrs = ("test-operator-1-1.0.0-1", "test-operator-2-1.0.0-1")

        with mock.patch.object(self.fbc_cli._logger, 'warning') as mock_warning:
            await self.fbc_cli.run()

            mock_warning.assert_called_once_with("Bundle build not found for test-operator-2-1.0.0-1. Will skip it.")

        mock_rebaser.rebase.assert_called_once()
        mock_builder.build.assert_called_once()

    async def test_run_no_bundle_builds_found(self):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")

        self.runtime.konflux_db.get_build_records_by_nvrs.return_value = [operator_build]
        self.runtime.images = ["test-operator"]
        self.runtime.image_map = {"test-operator": mock.Mock(is_olm_operator=True, distgit_key="test-operator")}

        async def mock_bundle_search(*args, **kwargs):
            # Empty async generator
            if False:
                yield

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search

        self.fbc_cli.operator_nvrs = ("test-operator-1.0.0-1",)

        with self.assertRaises(DoozerFatalError):
            await self.fbc_cli.run()

    async def test_get_operator_builds_by_nvrs(self):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")
        self.runtime.konflux_db.get_build_records_by_nvrs.return_value = [operator_build]
        self.runtime.images = ["test-operator"]
        self.runtime.image_map = {"test-operator": mock.Mock(is_olm_operator=True)}

        result = await self.fbc_cli.get_operator_builds(("test-operator-1.0.0-1",))

        self.assertEqual(len(result), 1)
        self.assertEqual(result["test-operator"], operator_build)

    async def test_get_bundle_build_for_found(self):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")
        bundle_build = self._create_mock_bundle_build(
            "test-operator-bundle", "test-operator-bundle-1.0.0-1", "test-operator-1.0.0-1"
        )

        async def mock_bundle_search(*args, **kwargs):
            yield bundle_build

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search

        result = await self.fbc_cli.get_bundle_build_for(operator_build)

        self.assertEqual(result, bundle_build)

    async def test_get_bundle_build_for_not_found_strict(self):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")

        async def mock_bundle_search(*args, **kwargs):
            # Empty async generator
            if False:
                yield

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search

        with self.assertRaises(IOError):
            await self.fbc_cli.get_bundle_build_for(operator_build, strict=True)

    async def test_get_bundle_build_for_not_found_non_strict(self):
        operator_build = self._create_mock_operator_build("test-operator", "test-operator-1.0.0-1")

        async def mock_bundle_search(*args, **kwargs):
            # Empty async generator
            if False:
                yield

        self.fbc_cli._db_for_bundles.search_builds_by_fields = mock_bundle_search

        result = await self.fbc_cli.get_bundle_build_for(operator_build, strict=False)

        self.assertIsNone(result)

    @mock.patch("doozerlib.cli.fbc.KonfluxFbcRebaser.get_fbc_name")
    async def test_check_existing_fbc_build_found(self, mock_get_fbc_name):
        mock_get_fbc_name.return_value = "test-fbc"
        operator_meta = mock.Mock(distgit_key="test-operator")
        bundle_build = self._create_mock_bundle_build(
            "test-operator-bundle", "test-operator-bundle-1.0.0-1", "test-operator-1.0.0-1"
        )
        existing_fbc = mock.Mock(spec=KonfluxFbcBuildRecord, nvr="test-fbc-1.0.0-1")

        async def mock_fbc_search(*args, **kwargs):
            yield existing_fbc

        self.fbc_cli._fbc_db.search_builds_by_fields = mock_fbc_search

        result = await self.fbc_cli._check_existing_fbc_build(operator_meta, bundle_build)

        self.assertEqual(result, existing_fbc)

    @mock.patch("doozerlib.cli.fbc.KonfluxFbcRebaser.get_fbc_name")
    async def test_check_existing_fbc_build_not_found(self, mock_get_fbc_name):
        mock_get_fbc_name.return_value = "test-fbc"
        operator_meta = mock.Mock(distgit_key="test-operator")
        bundle_build = self._create_mock_bundle_build(
            "test-operator-bundle", "test-operator-bundle-1.0.0-1", "test-operator-1.0.0-1"
        )

        async def mock_fbc_search(*args, **kwargs):
            # Empty async generator
            if False:
                yield

        self.fbc_cli._fbc_db.search_builds_by_fields = mock_fbc_search

        result = await self.fbc_cli._check_existing_fbc_build(operator_meta, bundle_build)

        self.assertIsNone(result)
