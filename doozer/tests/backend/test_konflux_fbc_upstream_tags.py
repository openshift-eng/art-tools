import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.backend.konflux_fbc import KonfluxFbcBuilder
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo

SHA = "1995bd170e5c63453b528dd86cf998dac95dc0c6"
DIGEST = "a" * 64


class TestFbcUpstreamTags(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with patch("doozerlib.backend.konflux_fbc.KonfluxClient") as client_cls:
            self.client = client_cls.from_kubeconfig.return_value = AsyncMock(spec=KonfluxClient)
            self.builder = KonfluxFbcBuilder(
                base_dir=MagicMock(),
                group="rhosdt-0.158",
                assembly="stream",
                product="rhosdt",
                db=None,
                fbc_repo="https://example.com/fbc.git",
                konflux_namespace="test",
            )
        self.client.start_pipeline_run_for_image_build.return_value = PipelineRunInfo(
            {"metadata": {"name": "test-build"}}, {}
        )
        self.logger = logging.getLogger(__name__)

    @patch("doozerlib.backend.konflux_fbc.util.oc_image_info_for_arch_async")
    async def test_resolves_upstream_sha_from_staging_bundle_digest_tag(self, inspect):
        inspect.return_value = {"config": {"config": {"Labels": {"io.openshift.build.commit.id": SHA}}}}
        labels = {
            "com.redhat.art.bundle.pullspec": f"registry.redhat.io/rhosdt/otel-bundle@sha256:{DIGEST}",
        }
        result = await self.builder._resolve_upstream_commit(labels, self.logger)
        self.assertEqual(result, SHA)
        self.assertEqual(inspect.await_args.args, (f"registry.stage.redhat.io/rhosdt/otel-bundle:{DIGEST}",))

    async def test_legacy_catalog_uses_upstream_dockerfile_label(self):
        self.assertEqual(
            await self.builder._resolve_upstream_commit({"io.openshift.build.commit.id": SHA}, self.logger), SHA
        )
        self.assertIsNone(await self.builder._resolve_upstream_commit({}, self.logger))

    @patch("doozerlib.backend.konflux_fbc.util.oc_image_info_for_arch_async")
    async def test_nonstream_uses_catalog_upstream_sha_without_stage_release(self, inspect):
        self.builder.assembly = "1.0"
        inspect.side_effect = ChildProcessError("assembly bundle is not stage-released")
        labels = {
            "com.redhat.art.bundle.pullspec": f"registry.redhat.io/rhosdt/otel-bundle@sha256:{DIGEST}",
            "io.openshift.build.commit.id": SHA,
        }
        self.assertEqual(await self.builder._resolve_upstream_commit(labels, self.logger), SHA)
        inspect.assert_not_awaited()

    @patch("doozerlib.backend.konflux_fbc.util.oc_image_info_for_arch_async")
    async def test_missing_or_invalid_upstream_sha_is_rejected(self, inspect):
        for sha in (None, "", "deadbeef", "g" * 40):
            with self.subTest(sha=sha):
                inspect.return_value = {"config": {"config": {"Labels": {"io.openshift.build.commit.id": sha}}}}
                with self.assertRaisesRegex(ValueError, "upstream.*SHA"):
                    await self.builder._resolve_upstream_commit(
                        {"com.redhat.art.bundle.pullspec": f"registry.redhat.io/rhosdt/bundle@sha256:{DIGEST}"},
                        self.logger,
                    )

    @patch("doozerlib.backend.konflux_fbc.util.oc_image_info_for_arch_async")
    async def test_stage_inspection_failure_is_not_silently_ignored(self, inspect):
        inspect.side_effect = ChildProcessError("registry unavailable")
        with self.assertRaises(ChildProcessError):
            await self.builder._resolve_upstream_commit(
                {"com.redhat.art.bundle.pullspec": f"registry.redhat.io/rhosdt/bundle@sha256:{DIGEST}"}, self.logger
            )

    @patch("doozerlib.backend.konflux_fbc.util.oc_image_info_for_arch_async")
    async def test_stream_without_stage_release_can_use_catalog_upstream_sha(self, inspect):
        inspect.side_effect = ChildProcessError("bundle has no stage release plan")
        labels = {
            "com.redhat.art.bundle.pullspec": f"registry.redhat.io/rhosdt/otel-bundle@sha256:{DIGEST}",
            "io.openshift.build.commit.id": SHA,
        }
        self.assertEqual(await self.builder._resolve_upstream_commit(labels, self.logger), SHA)
        inspect.assert_not_awaited()

    async def test_invalid_bundle_reference_is_rejected(self):
        for pullspec in ("registry.redhat.io/rhosdt/bundle:latest", "registry.redhat.io/rhosdt/bundle@sha256:bad"):
            with self.subTest(pullspec=pullspec), self.assertRaisesRegex(ValueError, "bundle pullspec"):
                await self.builder._resolve_upstream_commit({"com.redhat.art.bundle.pullspec": pullspec}, self.logger)

    async def _start(self, group="rhosdt-0.158", assembly="stream", sha=SHA, repo_name="otel-operator", target=(4, 16)):
        self.builder.group = group
        self.builder.assembly = assembly
        self.builder.major_minor_override = target
        metadata = MagicMock()
        metadata.runtime.group = group
        metadata.runtime.assembly = assembly
        metadata.runtime.group_config = {"vars": {"MAJOR": "4", "MINOR": "16"}}
        metadata.config.delivery.delivery_repo_names = [f"rhosdt/{repo_name}"]
        build_repo = MagicMock(commit_hash="build-repo-commit", https_url="https://example.com/fbc.git", branch="test")
        await self.builder._start_build(
            metadata,
            build_repo,
            "quay.io/test/fbc:otel-fbc-1.0-1",
            ["x86_64"],
            self.logger,
            upstream_commit=sha,
        )
        return self.client.start_pipeline_run_for_image_build.await_args.kwargs["build_params"].additional_tags

    async def test_layered_product_has_searchable_sha_tag_and_preserves_existing_tag(self):
        self.assertEqual(
            await self._start(),
            [
                "rhosdt-0.158__v4.16__otel-operator",
                "rhosdt-0.158__v4.16__otel-operator__1995bd1",
                f"sha-{SHA}__v4.16",
            ],
        )

    async def test_ocp_has_upstream_sha_tag(self):
        self.assertEqual(
            await self._start(group="openshift-4.16"),
            [
                "ocp__4.16__otel-operator",
                "ocp__4.16__otel-operator__1995bd1",
                f"sha-{SHA}__v4.16",
            ],
        )

    async def test_named_assembly_has_both_floating_sha_tags(self):
        self.assertEqual(
            await self._start(assembly="1.0"),
            [
                "rhosdt-0.158__v4.16__otel-operator__1995bd1",
                f"sha-{SHA}__v4.16",
            ],
        )

    async def test_named_ocp_assembly_has_both_sha_tags(self):
        self.assertEqual(
            await self._start(group="openshift-4.16", assembly="4.16.1"),
            [
                "ocp__4.16__otel-operator__1995bd1",
                f"sha-{SHA}__v4.16",
            ],
        )

    async def test_layered_product_without_target_override_uses_group_version(self):
        self.assertEqual(
            await self._start(target=None),
            [
                "rhosdt-0.158__v4.16__otel-operator__1995bd1",
                f"sha-{SHA}__v4.16",
            ],
        )

    async def test_rebuild_reuses_the_same_short_and_full_sha_tags(self):
        first = await self._start()
        self.assertEqual(await self._start(), first)

    async def test_ocp_version_distinguishes_both_sha_tag_formats(self):
        self.assertEqual(
            await self._start(target=(4, 21), assembly="1.0"),
            [
                "rhosdt-0.158__v4.21__otel-operator__1995bd1",
                f"sha-{SHA}__v4.21",
            ],
        )

    async def test_sha_does_not_leak_to_next_build(self):
        await self._start()
        self.assertEqual(await self._start(sha=None), ["rhosdt-0.158__v4.16__otel-operator"])

    async def test_oversize_sha_tag_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "128"):
            await self._start(repo_name="operator-" + "a" * 90)
        self.client.ensure_application.assert_not_awaited()

    @patch("doozerlib.backend.konflux_fbc.BuildRepo.from_local_dir")
    @patch("doozerlib.backend.konflux_fbc.util.oc_image_info_for_arch_async")
    async def test_build_passes_staging_upstream_sha_to_pipeline(self, inspect, from_local):
        inspect.return_value = {"config": {"config": {"Labels": {"io.openshift.build.commit.id": SHA}}}}
        metadata = MagicMock()
        metadata.distgit_key = "otel-operator"
        metadata.get_konflux_build_attempts.return_value = 1
        metadata.get_arches.return_value = ["x86_64"]
        metadata.runtime.assembly = "stream"
        metadata.runtime.group = "rhosdt-0.158"
        metadata.runtime.group_config = {}
        metadata.config.delivery.delivery_repo_names = ["rhosdt/otel-operator"]
        self.builder.major_minor_override = (4, 16)
        # Use group arches here; target lookup itself is covered by existing tests.
        self.builder._get_build_arches = AsyncMock(return_value=["x86_64"])
        self.client.wait_for_pipelinerun.return_value = PipelineRunInfo(
            {"metadata": {"name": "test-build"}, "status": {"conditions": [{"type": "Succeeded", "status": "True"}]}},
            {},
        )
        with TemporaryDirectory() as directory:
            self.builder.base_dir = Path(directory)
            repo_dir = Path(directory) / "otel-operator"
            repo_dir.mkdir()
            (repo_dir / "catalog.Dockerfile").write_text(
                'FROM scratch\nENV __doozer_version=1.0 __doozer_release=1\n'
                'LABEL com.redhat.art.name=otel-fbc com.redhat.art.nvr=otel-fbc-1.0-1 '
                f'com.redhat.art.bundle.pullspec=registry.redhat.io/rhosdt/otel-bundle@sha256:{DIGEST}\n'
            )
            from_local.return_value = MagicMock(
                local_dir=repo_dir,
                commit_hash="build-repo-commit",
                https_url="https://example.com/fbc.git",
                branch="test",
            )
            await self.builder.build(metadata)
            params = self.client.start_pipeline_run_for_image_build.await_args.kwargs
            self.assertEqual(params["commit_sha"], "build-repo-commit")
            self.assertEqual(
                params["build_params"].additional_tags,
                [
                    "rhosdt-0.158__v4.16__otel-operator",
                    "rhosdt-0.158__v4.16__otel-operator__1995bd1",
                    f"sha-{SHA}__v4.16",
                ],
            )

            # Named assemblies reuse catalog source metadata, without requiring
            # their bundle to have a stage release.
            self.builder.assembly = metadata.runtime.assembly = "1.0"
            with (repo_dir / "catalog.Dockerfile").open("a") as dockerfile:
                dockerfile.write(f"LABEL io.openshift.build.commit.id={SHA}\n")
            inspect.reset_mock()
            inspect.side_effect = ChildProcessError("assembly has no stage release")
            await self.builder.build(metadata)
            inspect.assert_not_awaited()
        params = self.client.start_pipeline_run_for_image_build.await_args.kwargs
        self.assertEqual(params["commit_sha"], "build-repo-commit")
        self.assertEqual(
            params["build_params"].additional_tags,
            [
                "rhosdt-0.158__v4.16__otel-operator__1995bd1",
                f"sha-{SHA}__v4.16",
            ],
        )
