import tempfile
import unittest
from pathlib import Path
from unittest import mock

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.model import Model
from doozerlib import constants
from doozerlib.cli.release_payload import ReleasePayloadRebaseAndBuildCli
from doozerlib.exceptions import DoozerFatalError
from doozerlib.runtime import Runtime

IMAGE_REFERENCES_YAML = """\
apiVersion: image.openshift.io/1
kind: ImageStream
spec:
  tags:
  - name: cluster-version-operator
    from:
      name: registry.example.com/ocp/release@sha256:cvo-digest
  - name: cli
    from:
      name: registry.example.com/ocp/release@sha256:cli-digest
"""

IMAGE_REFERENCES_NO_CVO_YAML = """\
apiVersion: image.openshift.io/1
kind: ImageStream
spec:
  tags:
  - name: cli
    from:
      name: registry.example.com/ocp/release@sha256:cli-digest
"""


def _make_cli(runtime, **overrides) -> ReleasePayloadRebaseAndBuildCli:
    kwargs = dict(
        runtime=runtime,
        version="4.21.1",
        release="202608011200.p0",
        arch="x86_64",
        payload_repo="https://github.com/openshift-priv/ocp-release-payloads.git",
        image_repo="quay.io/openshift-release-dev/ocp-v4.0-art-dev",
        konflux_kubeconfig="/path/to/kubeconfig",
        konflux_context="test-context",
        konflux_namespace="ocp-art-tenant",
        push=False,
        dry_run=False,
    )
    kwargs.update(overrides)
    return ReleasePayloadRebaseAndBuildCli(**kwargs)


class TestReleasePayloadRebaseAndBuildCliNaming(unittest.TestCase):
    def test_get_application_name(self):
        self.assertEqual(
            ReleasePayloadRebaseAndBuildCli.get_application_name("openshift-4.21"),
            "release-payload-openshift-4-21",
        )

    def test_get_component_name_matches_application_name(self):
        group = "openshift-4.21"
        self.assertEqual(
            ReleasePayloadRebaseAndBuildCli.get_component_name(group),
            ReleasePayloadRebaseAndBuildCli.get_application_name(group),
        )


class TestResolveImagestream(unittest.TestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "stream"
        self.runtime.assembly_type = AssemblyTypes.STREAM
        self.runtime.group_config = Model({"vars": {"MAJOR": 4, "MINOR": 21}})
        self.runtime.get_minor_version = mock.Mock(return_value="4.21")
        self.cli = _make_cli(self.runtime, arch="x86_64")

    def test_resolve_imagestream_for_stream_assembly(self):
        namespace, name = self.cli._resolve_imagestream()
        self.assertEqual(namespace, "ocp")
        self.assertEqual(name, "4.21-art-latest")

    def test_resolve_imagestream_for_named_assembly(self):
        self.runtime.assembly = "4.21.1"
        self.runtime.assembly_type = AssemblyTypes.STANDARD
        namespace, name = self.cli._resolve_imagestream()
        self.assertEqual(namespace, "ocp")
        self.assertEqual(name, "4.21-art-assembly-4.21.1")

    def test_resolve_imagestream_for_non_default_arch(self):
        self.cli.arch = "s390x"
        namespace, name = self.cli._resolve_imagestream()
        self.assertEqual(namespace, "ocp-s390x")
        self.assertEqual(name, "4.21-art-latest-s390x")


class TestGenerateManifests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "stream"
        self.runtime.assembly_type = AssemblyTypes.STREAM
        self.runtime.group_config = Model({"vars": {"MAJOR": 4, "MINOR": 21}})
        self.runtime.get_minor_version = mock.Mock(return_value="4.21")
        self.cli = _make_cli(self.runtime)
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.manifests_dir = Path(self.tmpdir.name) / "release-manifests"

    @mock.patch("doozerlib.cli.release_payload.exectools.cmd_assert_async")
    async def test_generate_manifests_resolves_cvo_pullspec(self, mock_cmd_assert_async):
        async def _write_manifests(cmd, **kwargs):
            (self.manifests_dir / "image-references").write_text(IMAGE_REFERENCES_YAML)
            return 0

        mock_cmd_assert_async.side_effect = _write_manifests

        cvo_pullspec = await self.cli._generate_manifests(self.manifests_dir)

        self.assertEqual(cvo_pullspec, "registry.example.com/ocp/release@sha256:cvo-digest")
        cmd = mock_cmd_assert_async.call_args.args[0]
        self.assertIn(f"--to-dir={self.manifests_dir}", cmd)
        self.assertIn("--name=4.21.1-202608011200.p0", cmd)
        self.assertIn("-n", cmd)
        self.assertIn("ocp", cmd)
        self.assertIn("--from-image-stream=4.21-art-latest", cmd)
        self.assertIn("--reference-mode=source", cmd)

    @mock.patch("doozerlib.cli.release_payload.exectools.cmd_assert_async")
    async def test_generate_manifests_uses_from_release_override(self, mock_cmd_assert_async):
        self.cli.from_release = "registry.example.com/ocp/release:4.21.0"

        async def _write_manifests(cmd, **kwargs):
            (self.manifests_dir / "image-references").write_text(IMAGE_REFERENCES_YAML)
            return 0

        mock_cmd_assert_async.side_effect = _write_manifests

        await self.cli._generate_manifests(self.manifests_dir)

        cmd = mock_cmd_assert_async.call_args.args[0]
        self.assertIn("--from-release=registry.example.com/ocp/release:4.21.0", cmd)
        self.assertNotIn("--from-image-stream=4.21-konflux-art-latest", cmd)

    @mock.patch("doozerlib.cli.release_payload.exectools.cmd_assert_async")
    async def test_generate_manifests_missing_image_references_raises(self, mock_cmd_assert_async):
        mock_cmd_assert_async.return_value = 0  # Does not write the file

        with self.assertRaises(DoozerFatalError):
            await self.cli._generate_manifests(self.manifests_dir)

    @mock.patch("doozerlib.cli.release_payload.exectools.cmd_assert_async")
    async def test_generate_manifests_missing_cvo_tag_raises(self, mock_cmd_assert_async):
        async def _write_manifests(cmd, **kwargs):
            (self.manifests_dir / "image-references").write_text(IMAGE_REFERENCES_NO_CVO_YAML)
            return 0

        mock_cmd_assert_async.side_effect = _write_manifests

        with self.assertRaises(DoozerFatalError):
            await self.cli._generate_manifests(self.manifests_dir)

    @mock.patch("doozerlib.cli.release_payload.exectools.cmd_assert_async")
    async def test_generate_manifests_includes_registry_config(self, mock_cmd_assert_async):
        self.cli.registry_config = "/path/to/auth.json"

        async def _write_manifests(cmd, **kwargs):
            (self.manifests_dir / "image-references").write_text(IMAGE_REFERENCES_YAML)
            return 0

        mock_cmd_assert_async.side_effect = _write_manifests

        await self.cli._generate_manifests(self.manifests_dir)

        cmd = mock_cmd_assert_async.call_args.args[0]
        self.assertIn("--registry-config=/path/to/auth.json", cmd)


class TestRebase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "4.21.1"
        self.runtime.assembly_type = AssemblyTypes.STANDARD
        self.runtime.upcycle = False
        self.runtime.working_dir = self.tmpdir.name
        self.runtime.group_config = Model({"vars": {"MAJOR": 4, "MINOR": 21}})

        self.cli = _make_cli(self.runtime)

        # Pre-create the repo directory that BuildRepo would normally clone into,
        # since BuildRepo itself is mocked out below.
        self.repo_dir = Path(self.tmpdir.name, constants.WORKING_SUBDIR_RELEASE_PAYLOAD_SOURCES, self.runtime.group)
        self.repo_dir.mkdir(parents=True, exist_ok=True)

    @mock.patch("doozerlib.cli.release_payload.BuildRepo")
    async def test_rebase_writes_dockerfile_and_commits(self, mock_build_repo_class):
        mock_build_repo = mock.AsyncMock()
        mock_build_repo.local_dir = self.repo_dir
        mock_build_repo.https_url = "https://github.com/openshift-priv/ocp-release-payloads.git"
        mock_build_repo.branch = "art-openshift-4.21-assembly-4.21.1-dgk-release-payload"
        mock_build_repo.commit_hash = "abc1234"
        mock_build_repo_class.return_value = mock_build_repo

        with mock.patch.object(
            self.cli, "_generate_manifests", mock.AsyncMock(return_value="registry.example.com/cvo@sha256:digest")
        ) as mock_generate_manifests:
            build_repo, cvo_pullspec, branch = await self.cli._rebase()

        mock_build_repo_class.assert_called_once()
        _, kwargs = mock_build_repo_class.call_args
        self.assertEqual(kwargs["url"], self.cli.payload_repo)
        self.assertEqual(kwargs["branch"], "art-openshift-4.21-assembly-4.21.1-dgk-release-payload")

        mock_build_repo.ensure_source.assert_awaited_once()
        mock_build_repo.delete_all_files.assert_awaited_once()
        mock_generate_manifests.assert_awaited_once()
        mock_build_repo.commit.assert_awaited_once()

        self.assertEqual(cvo_pullspec, "registry.example.com/cvo@sha256:digest")
        self.assertEqual(branch, "art-openshift-4.21-assembly-4.21.1-dgk-release-payload")
        self.assertIs(build_repo, mock_build_repo)

        dockerfile_content = (self.repo_dir / "Dockerfile").read_text()
        self.assertIn("FROM registry.example.com/cvo@sha256:digest", dockerfile_content)
        self.assertIn("COPY release-manifests/ /release-manifests/", dockerfile_content)

        commit_message = mock_build_repo.commit.call_args.args[0]
        self.assertIn("openshift-4.21", commit_message)
        self.assertIn("4.21.1", commit_message)

    @mock.patch("doozerlib.cli.release_payload.BuildRepo")
    async def test_rebase_uses_custom_commit_message(self, mock_build_repo_class):
        mock_build_repo = mock.AsyncMock()
        mock_build_repo.local_dir = self.repo_dir
        mock_build_repo.https_url = "https://github.com/openshift-priv/ocp-release-payloads.git"
        mock_build_repo.branch = "some-branch"
        mock_build_repo.commit_hash = "abc1234"
        mock_build_repo_class.return_value = mock_build_repo

        self.cli.commit_message = "Custom commit message"
        with mock.patch.object(
            self.cli, "_generate_manifests", mock.AsyncMock(return_value="registry.example.com/cvo")
        ):
            await self.cli._rebase()

        mock_build_repo.commit.assert_awaited_once_with("Custom commit message", allow_empty=True, force=True)


class TestBuild(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.cli = _make_cli(self.runtime)

        self.build_repo = mock.Mock()
        self.build_repo.commit_hash = "abc1234"
        self.build_repo.branch = "art-openshift-4.21-dgk-release-payload"
        self.build_repo.https_url = "https://github.com/openshift-priv/ocp-release-payloads.git"

    def _mock_konflux_client(self, succeeded: bool = True, reason: str = "Succeeded"):
        konflux_client = mock.AsyncMock()
        konflux_client.ensure_application = mock.AsyncMock()
        konflux_client.ensure_component = mock.AsyncMock()

        pipelinerun_info = mock.Mock()
        pipelinerun_info.name = "release-payload-openshift-4-21-abc123"
        pipelinerun_info.to_dict.return_value = {"metadata": {"name": pipelinerun_info.name}}
        konflux_client.start_pipeline_run_for_image_build = mock.AsyncMock(return_value=pipelinerun_info)
        konflux_client.resource_url = mock.Mock(return_value="https://konflux.example.com/pipelinerun/1")

        completed_pipelinerun_info = mock.Mock()
        completed_pipelinerun_info.name = pipelinerun_info.name
        condition = mock.Mock()
        condition.type = "Succeeded"
        condition.is_status_true.return_value = succeeded
        condition.status = "True" if succeeded else "False"
        condition.reason = reason
        completed_pipelinerun_info.find_condition.return_value = condition
        konflux_client.wait_for_pipelinerun = mock.AsyncMock(return_value=completed_pipelinerun_info)
        return konflux_client

    @mock.patch("doozerlib.cli.release_payload.KonfluxClient")
    async def test_build_success(self, mock_konflux_client_class):
        konflux_client = self._mock_konflux_client(succeeded=True)
        mock_konflux_client_class.from_kubeconfig.return_value = konflux_client

        result = await self.cli._build(self.build_repo, arches=["x86_64", "s390x"])

        konflux_client.ensure_application.assert_awaited_once()
        konflux_client.ensure_component.assert_awaited_once()
        konflux_client.start_pipeline_run_for_image_build.assert_awaited_once()
        _, kwargs = konflux_client.start_pipeline_run_for_image_build.call_args
        self.assertEqual(kwargs["building_arches"], ["x86_64", "s390x"])
        self.assertEqual(kwargs["git_url"], self.build_repo.https_url)
        self.assertEqual(kwargs["commit_sha"], self.build_repo.commit_hash)
        self.assertEqual(kwargs["output_image"], f"{self.cli.image_repo}:{self.cli.version}-{self.cli.release}")

        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.SUCCESS))
        self.assertEqual(result["output_image"], f"{self.cli.image_repo}:{self.cli.version}-{self.cli.release}")
        self.assertEqual(result["pipelinerun_url"], "https://konflux.example.com/pipelinerun/1")

    @mock.patch("doozerlib.cli.release_payload.KonfluxClient")
    async def test_build_failure_outcome(self, mock_konflux_client_class):
        konflux_client = self._mock_konflux_client(succeeded=False, reason="Error")
        mock_konflux_client_class.from_kubeconfig.return_value = konflux_client

        result = await self.cli._build(self.build_repo, arches=["x86_64"])

        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.BUILD_ERROR))

    async def test_build_raises_without_commit(self):
        self.build_repo.commit_hash = None

        with self.assertRaises(IOError):
            await self.cli._build(self.build_repo, arches=["x86_64"])


class TestRun(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "4.21.1"
        self.runtime.initialize = mock.Mock()
        self.runtime.group_config = Model({"vars": {"MAJOR": 4, "MINOR": 21}})
        self.runtime.get_global_konflux_arches = mock.Mock(return_value=["x86_64", "s390x"])

        self.cli = _make_cli(self.runtime, push=False, dry_run=False)

        self.build_repo = mock.Mock()
        self.build_repo.https_url = "https://github.com/openshift-priv/ocp-release-payloads.git"
        self.build_repo.branch = "art-openshift-4.21-assembly-4.21.1-dgk-release-payload"
        self.build_repo.commit_hash = "abc1234"
        self.build_repo.local_dir = "/tmp/release-payload"
        self.build_repo.push = mock.AsyncMock()

    async def test_run_without_push_skips_git_push_and_build(self):
        with (
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock()) as mock_build,
        ):
            result = await self.cli.run()

        mock_build.assert_not_awaited()
        self.build_repo.push.assert_not_awaited()
        self.assertFalse(result["pushed"])
        self.assertIsNone(result["outcome"])
        self.assertEqual(result["building_arches"], ["x86_64", "s390x"])

    async def test_run_raises_if_no_arches_configured(self):
        self.runtime.get_global_konflux_arches.return_value = []

        with self.assertRaises(DoozerFatalError):
            await self.cli.run()

    async def test_run_raises_if_assembly_disabled(self):
        self.runtime.assembly = None

        with self.assertRaises(ValueError):
            await self.cli.run()

    async def test_run_with_push_pushes_and_builds(self):
        self.cli.push = True
        self.cli.dry_run = False

        build_result = {
            "output_image": "quay.io/example/repo:4.21.1-1",
            "pipelinerun_name": "release-payload-abc123",
            "pipelinerun_url": "https://konflux.example.com/pipelinerun/1",
            "outcome": str(KonfluxBuildOutcome.SUCCESS),
        }
        with (
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock(return_value=build_result)) as mock_build,
        ):
            result = await self.cli.run()

        self.build_repo.push.assert_awaited_once_with(force=True)
        mock_build.assert_awaited_once_with(self.build_repo, ["x86_64", "s390x"])
        self.assertTrue(result["pushed"])
        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.SUCCESS))
        self.assertEqual(result["output_image"], build_result["output_image"])

    async def test_run_with_push_and_dry_run_skips_actual_push(self):
        self.cli.push = True
        self.cli.dry_run = True

        build_result = {
            "output_image": "quay.io/example/repo:4.21.1-1",
            "pipelinerun_name": "release-payload-abc123-dry-run",
            "pipelinerun_url": "https://konflux.example.com/pipelinerun/1",
            "outcome": str(KonfluxBuildOutcome.SUCCESS),
        }
        with (
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock(return_value=build_result)) as mock_build,
        ):
            result = await self.cli.run()

        self.build_repo.push.assert_not_awaited()
        mock_build.assert_awaited_once()
        self.assertFalse(result["pushed"])
        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.SUCCESS))

    async def test_run_raises_when_build_does_not_succeed(self):
        self.cli.push = True
        self.cli.dry_run = False

        build_result = {
            "output_image": "quay.io/example/repo:4.21.1-1",
            "pipelinerun_name": "release-payload-abc123",
            "pipelinerun_url": "https://konflux.example.com/pipelinerun/1",
            "outcome": str(KonfluxBuildOutcome.BUILD_ERROR),
        }
        with (
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock(return_value=build_result)),
        ):
            with self.assertRaises(DoozerFatalError):
                await self.cli.run()


if __name__ == "__main__":
    unittest.main()
