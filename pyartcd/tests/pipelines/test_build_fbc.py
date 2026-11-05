import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.variants import BuildVariant
from pyartcd.pipelines.build_fbc import BuildFbcPipeline


class TestBuildFbcPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        runtime = MagicMock(dry_run=False, doozer_working="/tmp/doozer-working")
        self.pipeline = BuildFbcPipeline(
            runtime=runtime,
            version="4.18",
            assembly="stream",
            data_path="",
            data_gitref="",
            only=None,
            exclude=None,
            operator_nvrs="",
            fbc_repo="",
            kubeconfig="",
            plr_template="",
            skip_checks=False,
        )

    @patch("pyartcd.pipelines.build_fbc.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_standard_ocp_build_passes_explicit_variant(self, mock_cmd_assert):
        """Standard OCP FBC builds must not rely on Doozer's default variant."""
        with (
            patch("pyartcd.pipelines.build_fbc.load_group_config", new_callable=AsyncMock) as mock_load_group_config,
            patch.object(self.pipeline, "_check_production_index_exists", new=AsyncMock(return_value=True)),
            patch.object(self.pipeline, "_rebase_and_build", new=AsyncMock(return_value=[])) as mock_rebase_and_build,
        ):
            mock_load_group_config.return_value = {}
            await self.pipeline.run()

        mock_rebase_and_build.assert_awaited_once()
        self.assertEqual(mock_rebase_and_build.await_args.kwargs["build_variant"], BuildVariant.OCP)

    @patch("pyartcd.pipelines.build_fbc.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_run_doozer_uses_explicit_variant(self, mock_cmd_assert):
        await self.pipeline._run_doozer([], only=None, exclude=None, build_variant=BuildVariant.OCP)

        self.assertIn("--variant=ocp", mock_cmd_assert.await_args.args[0])

    async def test_run_resolves_variant_from_group_product(self):
        with (
            patch("pyartcd.pipelines.build_fbc.load_group_config", new_callable=AsyncMock) as mock_load_group_config,
            patch.object(self.pipeline, "_check_production_index_exists", new=AsyncMock(return_value=True)),
            patch.object(self.pipeline, "_rebase_and_build", new=AsyncMock(return_value=[])) as mock_rebase_and_build,
        ):
            mock_load_group_config.return_value = {"product": "openshift-logging"}

            await self.pipeline.run()

        mock_rebase_and_build.assert_awaited_once()
        self.assertEqual(mock_rebase_and_build.await_args.kwargs["build_variant"], BuildVariant.LOGGING)

    async def test_skip_rebase_does_not_require_production_index(self):
        self.pipeline.skip_rebase = True
        with (
            patch("pyartcd.pipelines.build_fbc.load_group_config", new=AsyncMock(return_value={})),
            patch.object(self.pipeline, "_check_production_index_exists", new=AsyncMock(return_value=False)) as check,
            patch.object(self.pipeline, "_rebase_and_build", new=AsyncMock(return_value=[])) as build,
        ):
            await self.pipeline.run()
        check.assert_not_awaited()
        build.assert_awaited_once()

    async def test_skip_rebase_is_forwarded_only_when_enabled(self):
        self.pipeline.kubeconfig = "/tmp/kubeconfig"
        self.assertFalse(self.pipeline.skip_rebase)
        for skip_rebase in (False, True):
            with self.subTest(skip_rebase=skip_rebase):
                self.pipeline.skip_rebase = skip_rebase
                with (
                    patch.object(self.pipeline, "_run_doozer", new=AsyncMock()) as run_doozer,
                    patch.object(self.pipeline, "_parse_record_log", new=AsyncMock(return_value=([], []))),
                ):
                    await self.pipeline._rebase_and_build("release", "message", {})
                self.assertEqual("--skip-rebase" in run_doozer.await_args.args[0], skip_rebase)


if __name__ == "__main__":
    unittest.main()
