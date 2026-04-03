import json
import unittest
from unittest import mock

from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from artcommonlib.model import Model
from doozerlib.cli.images_konflux import KonfluxBundleCli
from doozerlib.exceptions import DoozerFatalError
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolver


class TestKonfluxBundleCli(unittest.IsolatedAsyncioTestCase):
    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    def setUp(self, mock_konflux_db_class):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.working_dir = "/tmp"
        self.runtime.group = "test-group"
        self.runtime.assembly = "test-assembly"
        self.runtime.images = []
        self.runtime.upcycle = False
        self.runtime.source_resolver = mock.Mock(spec=SourceResolver)
        self.runtime.konflux_db = mock.Mock()
        self.runtime.konflux_db.bind = mock.Mock()
        self.runtime.group_config = Model({})
        self.runtime.record_logger = mock.Mock()

        self.mock_bundle_db = mock.Mock()
        self.mock_bundle_db.bind = mock.Mock()
        mock_konflux_db_class.return_value = self.mock_bundle_db

        self.bundle_cli = KonfluxBundleCli(
            runtime=self.runtime,
            operator_nvrs=(),
            force=False,
            dry_run=False,
            konflux_kubeconfig="/path/to/kubeconfig",
            konflux_context="test-context",
            konflux_namespace="test-namespace",
            image_repo="test-repo",
            skip_checks=False,
            release=None,
            plr_template="test-template",
            output="json",
        )

    def _create_operator_build(self, name: str, nvr: str) -> KonfluxBuildRecord:
        build = mock.Mock(spec=KonfluxBuildRecord)
        build.name = name
        build.nvr = nvr
        return build

    @mock.patch("doozerlib.cli.images_konflux.sys.exit", side_effect=SystemExit(1))
    @mock.patch("doozerlib.cli.images_konflux.click.echo")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxOlmBundleBuilder")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxOlmBundleRebaser")
    async def test_run_outputs_partial_json_before_exiting(
        self, mock_rebaser_class, mock_builder_class, mock_echo, mock_sys_exit
    ):
        operator_build_a = self._create_operator_build("test-operator-a", "test-operator-a-1.0.0-1")
        operator_build_b = self._create_operator_build("test-operator-b", "test-operator-b-1.0.0-1")
        self.runtime.image_map = {
            "test-operator-a": mock.Mock(distgit_key="test-operator-a"),
            "test-operator-b": mock.Mock(distgit_key="test-operator-b"),
        }
        self.bundle_cli.get_operator_builds = mock.AsyncMock(
            return_value={
                "test-operator-a": operator_build_a,
                "test-operator-b": operator_build_b,
            }
        )

        async def rebase_and_build_side_effect(_rebaser, _builder, image_meta, _operator_build, **kwargs):
            if image_meta.distgit_key == "test-operator-b":
                raise RuntimeError("bundle build failed")
            return "test-operator-a-bundle-1.0.0-1"

        self.bundle_cli._rebase_and_build = mock.AsyncMock(side_effect=rebase_and_build_side_effect)
        mock_rebaser_class.return_value = mock.Mock()
        mock_builder = mock.Mock()
        mock_builder._konflux_client.ensure_git_auth_secret = mock.AsyncMock(return_value="test-secret")
        mock_builder._konflux_client.delete_git_auth_secret = mock.AsyncMock()
        mock_builder._konflux_client.cleanup_stale_git_auth_secrets = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        with self.assertRaises(SystemExit):
            await self.bundle_cli.run()

        output = json.loads(mock_echo.call_args.args[0])
        self.assertEqual(output["nvrs"], ["test-operator-a-bundle-1.0.0-1"])
        self.assertEqual(output["failed_count"], 1)
        self.assertEqual(output["success_count"], 1)
        self.assertEqual(output["errors"][0]["operator"], "test-operator-b")
        self.assertEqual(output["errors"][0]["operator_nvr"], "test-operator-b-1.0.0-1")
        self.assertEqual(output["errors"][0]["error"], "bundle build failed")
        mock_sys_exit.assert_called_once_with(1)

    @mock.patch("doozerlib.cli.images_konflux.KonfluxOlmBundleBuilder")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxOlmBundleRebaser")
    async def test_run_raises_fatal_error_for_non_json_callers(self, mock_rebaser_class, mock_builder_class):
        operator_build = self._create_operator_build("test-operator", "test-operator-1.0.0-1")
        self.runtime.image_map = {"test-operator": mock.Mock(distgit_key="test-operator")}
        self.bundle_cli.output = ""
        self.bundle_cli.get_operator_builds = mock.AsyncMock(return_value={"test-operator": operator_build})
        self.bundle_cli._rebase_and_build = mock.AsyncMock(side_effect=RuntimeError("bundle build failed"))
        mock_rebaser_class.return_value = mock.Mock()
        mock_builder = mock.Mock()
        mock_builder._konflux_client.ensure_git_auth_secret = mock.AsyncMock(return_value="test-secret")
        mock_builder._konflux_client.delete_git_auth_secret = mock.AsyncMock()
        mock_builder._konflux_client.cleanup_stale_git_auth_secrets = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        with self.assertRaises(DoozerFatalError):
            await self.bundle_cli.run()
