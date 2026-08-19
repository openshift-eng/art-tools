import tempfile
import unittest
from pathlib import Path
from unittest import mock

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.model import Model
from doozerlib import constants
from doozerlib.cli.release_payload import RELEASE_PAYLOAD_BUILD_PRIORITY, ReleasePayloadRebaseAndBuildCli
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

IMAGE_REFERENCES_NO_CLI_YAML = """\
apiVersion: image.openshift.io/1
kind: ImageStream
spec:
  tags:
  - name: cluster-version-operator
    from:
      name: registry.example.com/ocp/release@sha256:cvo-digest
"""


def _make_cli(runtime, **overrides) -> ReleasePayloadRebaseAndBuildCli:
    kwargs = dict(
        runtime=runtime,
        version="4.21.1",
        release="202608011200.p2",
        arch="x86_64",
        payload_repo="https://github.com/openshift-priv/ocp-release-payloads.git",
        image_repo="quay.io/openshift-release-dev/ocp-v4.0-art-dev",
        konflux_kubeconfig="/path/to/kubeconfig",
        konflux_context="test-context",
        konflux_namespace="ocp-art-tenant",
        push=False,
        sync=False,
        release_image_repo=constants.RELEASE_PAYLOAD_DEST_REPO,
        dry_run=False,
    )
    kwargs.update(overrides)
    return ReleasePayloadRebaseAndBuildCli(**kwargs)


class TestReleasePayloadRebaseAndBuildCliNaming(unittest.TestCase):
    def test_get_application_name_is_shared_across_groups(self):
        self.assertEqual(ReleasePayloadRebaseAndBuildCli.get_application_name(), "release-payloads")

    def test_get_component_name_is_group_specific(self):
        self.assertEqual(
            ReleasePayloadRebaseAndBuildCli.get_component_name("openshift-4.21"),
            "release-payload-openshift-4-21",
        )
        self.assertEqual(
            ReleasePayloadRebaseAndBuildCli.get_component_name("openshift-5.0"),
            "release-payload-openshift-5-0",
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

        cvo_pullspec, cli_pullspec = await self.cli._generate_manifests(self.manifests_dir)

        self.assertEqual(cvo_pullspec, "registry.example.com/ocp/release@sha256:cvo-digest")
        self.assertEqual(cli_pullspec, "registry.example.com/ocp/release@sha256:cli-digest")
        cmd = mock_cmd_assert_async.call_args.args[0]
        self.assertIn(f"--to-dir={self.manifests_dir}", cmd)
        self.assertIn("--name=4.21.1-202608011200.p2", cmd)
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
    async def test_generate_manifests_missing_cli_tag_raises(self, mock_cmd_assert_async):
        async def _write_manifests(cmd, **kwargs):
            (self.manifests_dir / "image-references").write_text(IMAGE_REFERENCES_NO_CLI_YAML)
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

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly", return_value="4.21.1")
    @mock.patch("doozerlib.cli.release_payload.BuildRepo")
    async def test_rebase_writes_dockerfile_and_commits(self, mock_build_repo_class, mock_get_release_name):
        mock_build_repo = mock.AsyncMock()
        mock_build_repo.local_dir = self.repo_dir
        mock_build_repo.https_url = "https://github.com/openshift-priv/ocp-release-payloads.git"
        mock_build_repo.branch = "art-openshift-4.21-assembly-4.21.1-dgk-release-payload"
        mock_build_repo.commit_hash = "abc1234"
        mock_build_repo_class.return_value = mock_build_repo

        cvo_art_images_pullspec = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:cvo-abc123def456"
        cli_art_images_pullspec = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:cli-def456abc123"
        with (
            mock.patch.object(
                self.cli,
                "_generate_manifests",
                mock.AsyncMock(
                    return_value=(
                        "registry.example.com/cvo@sha256:cvo-digest",
                        "registry.example.com/cli@sha256:cli-digest",
                    )
                ),
            ) as mock_generate_manifests,
            mock.patch.object(
                self.cli,
                "_resolve_art_images_pullspec",
                mock.AsyncMock(side_effect=[cvo_art_images_pullspec, cli_art_images_pullspec]),
            ) as mock_resolve,
        ):
            build_repo, cvo_pullspec, branch = await self.cli._rebase()

        mock_build_repo_class.assert_called_once()
        _, kwargs = mock_build_repo_class.call_args
        self.assertEqual(kwargs["url"], self.cli.payload_repo)
        self.assertEqual(kwargs["branch"], "art-openshift-4.21-assembly-4.21.1-dgk-release-payload")

        mock_build_repo.ensure_source.assert_awaited_once()
        mock_build_repo.delete_all_files.assert_awaited_once()
        mock_generate_manifests.assert_awaited_once()
        self.assertEqual(mock_resolve.await_count, 2)
        mock_resolve.assert_any_await("registry.example.com/cvo@sha256:cvo-digest")
        mock_resolve.assert_any_await("registry.example.com/cli@sha256:cli-digest")
        mock_build_repo.commit.assert_awaited_once()

        self.assertEqual(cvo_pullspec, "registry.example.com/cvo@sha256:cvo-digest")
        self.assertEqual(branch, "art-openshift-4.21-assembly-4.21.1-dgk-release-payload")
        self.assertIs(build_repo, mock_build_repo)

        dockerfile_content = (self.repo_dir / "Dockerfile").read_text()
        self.assertIn(f"FROM {cli_art_images_pullspec} AS cli", dockerfile_content)
        self.assertIn(f"FROM {cvo_art_images_pullspec} AS cvo", dockerfile_content)
        self.assertIn("FROM scratch", dockerfile_content)
        self.assertIn("COPY --from=cvo / /", dockerfile_content)
        self.assertIn("COPY --from=cli /usr/bin/oc /usr/bin/image", dockerfile_content)
        self.assertIn('io.openshift.release="4.21.1"', dockerfile_content)
        self.assertIn('io.openshift.release.base-image-digest="sha256:cvo-abc123def456"', dockerfile_content)
        self.assertIn("COPY release-manifests/ /release-manifests/", dockerfile_content)

        commit_message = mock_build_repo.commit.call_args.args[0]
        self.assertIn("openshift-4.21", commit_message)
        self.assertIn("4.21.1", commit_message)

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly", return_value="4.21.1")
    @mock.patch("doozerlib.cli.release_payload.BuildRepo")
    async def test_rebase_uses_custom_commit_message(self, mock_build_repo_class, mock_get_release_name):
        mock_build_repo = mock.AsyncMock()
        mock_build_repo.local_dir = self.repo_dir
        mock_build_repo.https_url = "https://github.com/openshift-priv/ocp-release-payloads.git"
        mock_build_repo.branch = "some-branch"
        mock_build_repo.commit_hash = "abc1234"
        mock_build_repo_class.return_value = mock_build_repo

        self.cli.commit_message = "Custom commit message"
        with (
            mock.patch.object(
                self.cli,
                "_generate_manifests",
                mock.AsyncMock(
                    return_value=("registry.example.com/cvo@sha256:digest", "registry.example.com/cli@sha256:digest")
                ),
            ),
            mock.patch.object(
                self.cli,
                "_resolve_art_images_pullspec",
                mock.AsyncMock(
                    return_value="quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:abc123def456"
                ),
            ),
        ):
            await self.cli._rebase()

        mock_build_repo.commit.assert_awaited_once_with("Custom commit message", allow_empty=True, force=True)

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly", return_value="4.21.1")
    @mock.patch("doozerlib.cli.release_payload.BuildRepo")
    async def test_rebase_raises_when_art_images_pullspec_has_no_digest(
        self, mock_build_repo_class, mock_get_release_name
    ):
        mock_build_repo = mock.AsyncMock()
        mock_build_repo.local_dir = self.repo_dir
        mock_build_repo_class.return_value = mock_build_repo

        with (
            mock.patch.object(
                self.cli,
                "_generate_manifests",
                mock.AsyncMock(
                    return_value=("registry.example.com/cvo@sha256:digest", "registry.example.com/cli@sha256:digest")
                ),
            ),
            mock.patch.object(
                self.cli,
                "_resolve_art_images_pullspec",
                mock.AsyncMock(return_value="quay.io/redhat-user-workloads/ocp-art-tenant/art-images:tag-only"),
            ),
        ):
            with self.assertRaises(DoozerFatalError):
                await self.cli._rebase()

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly", return_value="5.0.0-ec.6")
    @mock.patch("doozerlib.cli.release_payload.BuildRepo")
    async def test_rebase_preview_assembly_label_includes_prerelease(
        self, mock_build_repo_class, mock_get_release_name
    ):
        self.runtime.group = "openshift-5.0"
        self.runtime.assembly = "ec.6"
        self.runtime.assembly_type = AssemblyTypes.PREVIEW
        self.cli = _make_cli(self.runtime, version="v5.0.0")

        repo_dir = Path(self.tmpdir.name, constants.WORKING_SUBDIR_RELEASE_PAYLOAD_SOURCES, self.runtime.group)
        repo_dir.mkdir(parents=True, exist_ok=True)

        mock_build_repo = mock.AsyncMock()
        mock_build_repo.local_dir = repo_dir
        mock_build_repo.https_url = "https://github.com/openshift-priv/ocp-release-payloads.git"
        mock_build_repo.branch = "art-openshift-5.0-assembly-ec.6-dgk-release-payload"
        mock_build_repo.commit_hash = "def5678"
        mock_build_repo_class.return_value = mock_build_repo

        art_images_pullspec = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images@sha256:def456abc123"
        with (
            mock.patch.object(
                self.cli,
                "_generate_manifests",
                mock.AsyncMock(
                    return_value=("registry.example.com/cvo@sha256:digest", "registry.example.com/cli@sha256:digest")
                ),
            ),
            mock.patch.object(
                self.cli, "_resolve_art_images_pullspec", mock.AsyncMock(return_value=art_images_pullspec)
            ),
        ):
            await self.cli._rebase()

        dockerfile_content = (repo_dir / "Dockerfile").read_text()
        self.assertIn('io.openshift.release="5.0.0-ec.6"', dockerfile_content)
        self.assertNotIn('io.openshift.release="5.0.0"', dockerfile_content)


class TestResolveArtImagesPullspec(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "4.21.1"
        self.runtime.konflux_db = mock.AsyncMock()
        self.runtime.konflux_db.bind = mock.Mock()
        self.cli = _make_cli(self.runtime)

    @mock.patch("doozerlib.cli.release_payload.release_inspector.extract_nvr_from_pullspec")
    async def test_resolves_to_image_pullspec(self, mock_extract_nvr):
        mock_extract_nvr.return_value = ("cluster-version-operator-container", "4.21.1", "202608011200.p2")
        build_record = mock.Mock(spec=KonfluxBuildRecord)
        build_record.image_pullspec = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images:cvo-tag"
        self.runtime.konflux_db.get_build_record_by_nvr = mock.AsyncMock(return_value=build_record)

        result = await self.cli._resolve_art_images_pullspec(
            "quay.io/openshift-release-dev/ocp-v5.0-art-dev@sha256:abc"
        )

        mock_extract_nvr.assert_awaited_once_with(
            "quay.io/openshift-release-dev/ocp-v5.0-art-dev@sha256:abc", registry_config=None
        )
        self.runtime.konflux_db.get_build_record_by_nvr.assert_awaited_once_with(
            nvr="cluster-version-operator-container-4.21.1-202608011200.p2",
            outcome=KonfluxBuildOutcome.SUCCESS,
            exclude_large_columns=True,
        )
        self.assertEqual(result, "quay.io/redhat-user-workloads/ocp-art-tenant/art-images:cvo-tag")

    @mock.patch("doozerlib.cli.release_payload.release_inspector.extract_nvr_from_pullspec")
    async def test_raises_when_no_build_record(self, mock_extract_nvr):
        mock_extract_nvr.return_value = ("cluster-version-operator-container", "4.21.1", "202608011200.p2")
        self.runtime.konflux_db.get_build_record_by_nvr = mock.AsyncMock(return_value=None)

        with self.assertRaises(DoozerFatalError):
            await self.cli._resolve_art_images_pullspec("quay.io/openshift-release-dev/ocp-v5.0-art-dev@sha256:abc")

    @mock.patch("doozerlib.cli.release_payload.release_inspector.extract_nvr_from_pullspec")
    async def test_raises_when_build_record_has_no_pullspec(self, mock_extract_nvr):
        mock_extract_nvr.return_value = ("cluster-version-operator-container", "4.21.1", "202608011200.p2")
        build_record = mock.Mock(spec=KonfluxBuildRecord)
        build_record.image_pullspec = ""
        self.runtime.konflux_db.get_build_record_by_nvr = mock.AsyncMock(return_value=build_record)

        with self.assertRaises(DoozerFatalError):
            await self.cli._resolve_art_images_pullspec("quay.io/openshift-release-dev/ocp-v5.0-art-dev@sha256:abc")

    async def test_raises_when_konflux_db_not_available(self):
        self.runtime.konflux_db = None

        with self.assertRaises(DoozerFatalError):
            await self.cli._resolve_art_images_pullspec("quay.io/openshift-release-dev/ocp-v5.0-art-dev@sha256:abc")


class TestBuild(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "4.21.1"
        self.runtime.konflux_db = mock.Mock()
        self.runtime.konflux_db.bind = mock.Mock()
        self.runtime.konflux_db.add_build = mock.Mock()
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
        self.assertEqual(kwargs["generate_name"], "release-payload-4-21-1-")
        self.assertEqual(kwargs["build_params"].build_priority, RELEASE_PAYLOAD_BUILD_PRIORITY)

        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.SUCCESS))
        self.assertEqual(result["output_image"], f"{self.cli.image_repo}:{self.cli.version}-{self.cli.release}")
        self.assertEqual(result["pipelinerun_url"], "https://konflux.example.com/pipelinerun/1")

        self.runtime.konflux_db.bind.assert_called_once_with(KonfluxBuildRecord)
        self.runtime.konflux_db.add_build.assert_called_once()
        record = self.runtime.konflux_db.add_build.call_args.args[0]
        self.assertEqual(record.name, "release-payload")
        self.assertEqual(record.group, "openshift-4.21")
        self.assertEqual(record.version, self.cli.version)
        self.assertEqual(record.release, self.cli.release)
        self.assertEqual(record.assembly, "4.21.1")
        self.assertEqual(record.arches, ["x86_64", "s390x"])
        self.assertFalse(record.embargoed)
        self.assertEqual(record.outcome, KonfluxBuildOutcome.SUCCESS)
        self.assertEqual(record.image_pullspec, result["output_image"])
        self.assertEqual(record.build_pipeline_url, result["pipelinerun_url"])
        self.assertEqual(record.build_priority, int(RELEASE_PAYLOAD_BUILD_PRIORITY))
        self.assertEqual(record.nvr, f"release-payload-{self.cli.version}-{self.cli.release}")

    @mock.patch("doozerlib.cli.release_payload.KonfluxClient")
    async def test_build_failure_outcome(self, mock_konflux_client_class):
        konflux_client = self._mock_konflux_client(succeeded=False, reason="Error")
        mock_konflux_client_class.from_kubeconfig.return_value = konflux_client

        result = await self.cli._build(self.build_repo, arches=["x86_64"])

        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.BUILD_ERROR))
        record = self.runtime.konflux_db.add_build.call_args.args[0]
        self.assertEqual(record.outcome, KonfluxBuildOutcome.BUILD_ERROR)

    async def test_build_raises_without_commit(self):
        self.build_repo.commit_hash = None

        with self.assertRaises(IOError):
            await self.cli._build(self.build_repo, arches=["x86_64"])

    @mock.patch("doozerlib.cli.release_payload.KonfluxClient")
    async def test_build_skips_recording_in_dry_run(self, mock_konflux_client_class):
        konflux_client = self._mock_konflux_client(succeeded=True)
        mock_konflux_client_class.from_kubeconfig.return_value = konflux_client
        self.cli.dry_run = True

        await self.cli._build(self.build_repo, arches=["x86_64"])

        self.runtime.konflux_db.add_build.assert_not_called()

    @mock.patch("doozerlib.cli.release_payload.KonfluxClient")
    async def test_build_skips_recording_when_no_konflux_db(self, mock_konflux_client_class):
        konflux_client = self._mock_konflux_client(succeeded=True)
        mock_konflux_client_class.from_kubeconfig.return_value = konflux_client
        self.runtime.konflux_db = None

        # Should not raise even though there is nowhere to record the build.
        result = await self.cli._build(self.build_repo, arches=["x86_64"])

        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.SUCCESS))

    @mock.patch("doozerlib.cli.release_payload.KonfluxClient")
    async def test_build_swallows_db_recording_errors(self, mock_konflux_client_class):
        konflux_client = self._mock_konflux_client(succeeded=True)
        mock_konflux_client_class.from_kubeconfig.return_value = konflux_client
        self.runtime.konflux_db.add_build.side_effect = Exception("boom")

        # A DB failure shouldn't fail an otherwise-successful build.
        result = await self.cli._build(self.build_repo, arches=["x86_64"])

        self.assertEqual(result["outcome"], str(KonfluxBuildOutcome.SUCCESS))


class TestSync(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "4.21.1"
        self.cli = _make_cli(self.runtime)
        self.output_image = f"{self.cli.image_repo}:4.21.1-202608011200.p2"
        self.arch_infos = [
            {"digest": "sha256:x8664digest", "architecture": "amd64"},
            {"digest": "sha256:s390xdigest", "architecture": "s390x"},
        ]

    @mock.patch("doozerlib.cli.release_payload.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.oc_image_info_async", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.find_manifest_list_sha", new_callable=mock.AsyncMock)
    async def test_sync_mirrors_list_and_every_arch_digest(
        self, mock_find_sha, mock_oc_image_info_async, mock_sync_to_quay
    ):
        mock_find_sha.return_value = "sha256:deadbeef"
        mock_oc_image_info_async.return_value = self.arch_infos

        result = await self.cli._sync(self.output_image, arches=["x86_64", "s390x"])

        mock_find_sha.assert_awaited_once_with(self.output_image, registry_config=self.cli.registry_config)
        mock_oc_image_info_async.assert_awaited_once_with(
            self.output_image, '--show-multiarch', registry_config=self.cli.registry_config
        )
        expected_list_pullspec = f"{self.cli.image_repo}@sha256:deadbeef"
        expected_arch_pullspecs = [
            f"{self.cli.image_repo}@sha256:x8664digest",
            f"{self.cli.image_repo}@sha256:s390xdigest",
        ]
        mock_sync_to_quay.assert_has_awaits(
            [
                mock.call(expected_list_pullspec, self.cli.release_image_repo),
                mock.call(expected_arch_pullspecs[0], self.cli.release_image_repo),
                mock.call(expected_arch_pullspecs[1], self.cli.release_image_repo),
            ]
        )
        self.assertEqual(mock_sync_to_quay.await_count, 3)
        self.assertTrue(result["synced"])
        self.assertEqual(result["release_repo"], self.cli.release_image_repo)
        self.assertEqual(result["release_pullspec"], expected_list_pullspec)
        self.assertEqual(result["arch_pullspecs"], expected_arch_pullspecs)

    @mock.patch("doozerlib.cli.release_payload.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.oc_image_info_async", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.find_manifest_list_sha", new_callable=mock.AsyncMock)
    async def test_sync_dry_run_skips_mirror(self, mock_find_sha, mock_oc_image_info_async, mock_sync_to_quay):
        mock_find_sha.return_value = "sha256:deadbeef"
        mock_oc_image_info_async.return_value = self.arch_infos
        self.cli.dry_run = True

        result = await self.cli._sync(self.output_image, arches=["x86_64", "s390x"])

        mock_sync_to_quay.assert_not_awaited()
        self.assertFalse(result["synced"])
        self.assertEqual(result["release_pullspec"], f"{self.cli.image_repo}@sha256:deadbeef")
        self.assertEqual(
            result["arch_pullspecs"],
            [f"{self.cli.image_repo}@sha256:x8664digest", f"{self.cli.image_repo}@sha256:s390xdigest"],
        )

    @mock.patch("doozerlib.cli.release_payload.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.oc_image_info_async", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.find_manifest_list_sha", new_callable=mock.AsyncMock)
    async def test_sync_uses_custom_release_image_repo(
        self, mock_find_sha, mock_oc_image_info_async, mock_sync_to_quay
    ):
        mock_find_sha.return_value = "sha256:deadbeef"
        mock_oc_image_info_async.return_value = self.arch_infos
        self.cli.release_image_repo = "quay.io/example/staging-release"

        await self.cli._sync(self.output_image, arches=["x86_64", "s390x"])

        for call_args in mock_sync_to_quay.await_args_list:
            self.assertEqual(call_args.args[1], "quay.io/example/staging-release")
        self.assertEqual(mock_sync_to_quay.await_count, 3)

    @mock.patch("doozerlib.cli.release_payload.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.oc_image_info_async", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.find_manifest_list_sha", new_callable=mock.AsyncMock)
    async def test_sync_accepts_digest_based_source_pullspec(
        self, mock_find_sha, mock_oc_image_info_async, mock_sync_to_quay
    ):
        # Exercises the path used by --nvr, where the source pullspec comes from a build
        # record's image_pullspec (already digest-based) rather than a fresh --version/--release
        # tag. arches also defaults to () here, as _sync_nvr may not always have them.
        mock_find_sha.return_value = "sha256:deadbeef"
        mock_oc_image_info_async.return_value = self.arch_infos
        source_pullspec = f"{self.cli.image_repo}@sha256:originaldigest"

        result = await self.cli._sync(source_pullspec)

        mock_find_sha.assert_awaited_once_with(source_pullspec, registry_config=self.cli.registry_config)
        expected_list_pullspec = f"{self.cli.image_repo}@sha256:deadbeef"
        self.assertEqual(result["release_pullspec"], expected_list_pullspec)
        self.assertEqual(
            result["arch_pullspecs"],
            [f"{self.cli.image_repo}@sha256:x8664digest", f"{self.cli.image_repo}@sha256:s390xdigest"],
        )

    @mock.patch("doozerlib.cli.release_payload.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.oc_image_info_async", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.find_manifest_list_sha", new_callable=mock.AsyncMock)
    async def test_sync_falls_back_to_quay_auth_file_env(
        self, mock_find_sha, mock_oc_image_info_async, mock_sync_to_quay
    ):
        mock_find_sha.return_value = "sha256:deadbeef"
        mock_oc_image_info_async.return_value = self.arch_infos
        self.cli.registry_config = None

        with mock.patch.dict("os.environ", {"QUAY_AUTH_FILE": "/path/to/quay-auth.json"}):
            await self.cli._sync(self.output_image, arches=["x86_64", "s390x"])

        mock_find_sha.assert_awaited_once_with(self.output_image, registry_config="/path/to/quay-auth.json")
        mock_oc_image_info_async.assert_awaited_once_with(
            self.output_image, '--show-multiarch', registry_config="/path/to/quay-auth.json"
        )

    @mock.patch("doozerlib.cli.release_payload.sync_to_quay", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.oc_image_info_async", new_callable=mock.AsyncMock)
    @mock.patch("doozerlib.cli.release_payload.find_manifest_list_sha", new_callable=mock.AsyncMock)
    async def test_sync_explicit_registry_config_overrides_env(
        self, mock_find_sha, mock_oc_image_info_async, mock_sync_to_quay
    ):
        mock_find_sha.return_value = "sha256:deadbeef"
        mock_oc_image_info_async.return_value = self.arch_infos
        self.cli.registry_config = "/explicit/auth.json"

        with mock.patch.dict("os.environ", {"QUAY_AUTH_FILE": "/path/to/quay-auth.json"}):
            await self.cli._sync(self.output_image, arches=["x86_64", "s390x"])

        mock_find_sha.assert_awaited_once_with(self.output_image, registry_config="/explicit/auth.json")
        mock_oc_image_info_async.assert_awaited_once_with(
            self.output_image, '--show-multiarch', registry_config="/explicit/auth.json"
        )


class TestSyncNvr(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "4.21.1"
        self.runtime.konflux_db = mock.AsyncMock()
        self.runtime.konflux_db.bind = mock.Mock()
        self.nvr = "release-payload-4.21.1-202608011200.p2"
        self.cli = _make_cli(self.runtime, nvr=self.nvr)

    async def test_sync_nvr_looks_up_and_syncs(self):
        build_record = mock.Mock(spec=KonfluxBuildRecord)
        build_record.image_pullspec = "quay.io/example/repo@sha256:deadbeef"
        build_record.group = "openshift-4.21"
        build_record.assembly = "4.21.1"
        build_record.version = "4.21.1"
        build_record.release = "202608011200.p2"
        build_record.arches = ["x86_64", "s390x"]
        self.runtime.konflux_db.get_build_record_by_nvr = mock.AsyncMock(return_value=build_record)

        sync_result = {
            "synced": True,
            "release_repo": self.cli.release_image_repo,
            "release_pullspec": "quay.io/example/rel@sha256:deadbeef",
        }
        with mock.patch.object(self.cli, "_sync", mock.AsyncMock(return_value=sync_result)) as mock_sync:
            result = await self.cli._sync_nvr()

        self.runtime.konflux_db.bind.assert_called_once_with(KonfluxBuildRecord)
        self.runtime.konflux_db.get_build_record_by_nvr.assert_awaited_once_with(
            nvr=self.nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            exclude_large_columns=True,
        )
        mock_sync.assert_awaited_once_with(build_record.image_pullspec, build_record.arches)
        self.assertEqual(result["nvr"], self.nvr)
        self.assertEqual(result["group"], "openshift-4.21")
        self.assertTrue(result["synced"])
        self.assertEqual(result["release_pullspec"], sync_result["release_pullspec"])

    async def test_sync_nvr_raises_when_no_record(self):
        self.runtime.konflux_db.get_build_record_by_nvr = mock.AsyncMock(return_value=None)

        with self.assertRaises(DoozerFatalError):
            await self.cli._sync_nvr()

    async def test_sync_nvr_raises_when_no_pullspec(self):
        build_record = mock.Mock(spec=KonfluxBuildRecord)
        build_record.image_pullspec = ""
        self.runtime.konflux_db.get_build_record_by_nvr = mock.AsyncMock(return_value=build_record)

        with self.assertRaises(DoozerFatalError):
            await self.cli._sync_nvr()

    async def test_sync_nvr_raises_when_konflux_db_not_available(self):
        self.runtime.konflux_db = None

        with self.assertRaises(DoozerFatalError):
            await self.cli._sync_nvr()


class TestResolveVersion(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = mock.Mock(spec=Runtime)
        self.runtime.group = "openshift-4.21"
        self.runtime.assembly = "4.21.1"
        self.runtime.get_releases_config = mock.Mock(return_value=Model({}))
        self.cli = _make_cli(self.runtime)

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly")
    async def test_resolve_version_for_standard_assembly(self, mock_get_release_name):
        mock_get_release_name.return_value = "4.21.1"

        result = await self.cli._resolve_version()

        mock_get_release_name.assert_called_once_with(
            self.runtime.group, self.runtime.get_releases_config.return_value, self.runtime.assembly
        )
        self.assertEqual(result, "v4.21.1")

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly")
    async def test_resolve_version_strips_prerelease_suffix(self, mock_get_release_name):
        self.runtime.group = "openshift-5.0"
        self.runtime.assembly = "ec.5"
        mock_get_release_name.return_value = "5.0.0-ec.5"

        result = await self.cli._resolve_version()

        self.assertEqual(result, "v5.0.0")

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly")
    async def test_resolve_version_raises_on_unparseable_release_name(self, mock_get_release_name):
        mock_get_release_name.return_value = "not-a-version"

        with self.assertRaises(DoozerFatalError):
            await self.cli._resolve_version()

    @mock.patch("doozerlib.cli.release_payload.get_release_name_for_assembly")
    async def test_resolve_version_raises_when_assembly_type_unsupported(self, mock_get_release_name):
        mock_get_release_name.side_effect = ValueError("Assembly type AssemblyTypes.STREAM is not supported.")

        with self.assertRaises(DoozerFatalError):
            await self.cli._resolve_version()


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

    async def test_run_derives_version_when_omitted(self):
        self.cli.version = None

        with (
            mock.patch.object(self.cli, "_resolve_version", mock.AsyncMock(return_value="v4.21.1")) as mock_resolve,
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock()),
        ):
            result = await self.cli.run()

        mock_resolve.assert_awaited_once()
        self.assertEqual(self.cli.version, "v4.21.1")
        self.assertEqual(result["version"], "v4.21.1")

    async def test_run_derives_version_when_auto(self):
        self.cli.version = "auto"

        with (
            mock.patch.object(self.cli, "_resolve_version", mock.AsyncMock(return_value="v4.21.1")) as mock_resolve,
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock()),
        ):
            result = await self.cli.run()

        mock_resolve.assert_awaited_once()
        self.assertEqual(result["version"], "v4.21.1")

    async def test_run_with_explicit_version_skips_resolve(self):
        self.cli.version = "4.21.1"

        with (
            mock.patch.object(self.cli, "_resolve_version", mock.AsyncMock()) as mock_resolve,
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock()),
        ):
            result = await self.cli.run()

        mock_resolve.assert_not_awaited()
        self.assertEqual(result["version"], "4.21.1")

    async def test_run_raises_if_no_arches_configured(self):
        self.runtime.get_global_konflux_arches.return_value = []

        with self.assertRaises(DoozerFatalError):
            await self.cli.run()

    async def test_run_raises_if_assembly_disabled(self):
        self.runtime.assembly = None

        with self.assertRaises(ValueError):
            await self.cli.run()

    async def test_run_raises_when_release_missing_and_no_nvr(self):
        self.cli.release = None

        with self.assertRaises(DoozerFatalError):
            await self.cli.run()

    async def test_run_with_nvr_skips_rebase_and_build(self):
        self.cli.nvr = "release-payload-4.21.1-202608011200.p2"
        sync_nvr_result = {"nvr": self.cli.nvr, "synced": True}

        with (
            mock.patch.object(self.cli, "_sync_nvr", mock.AsyncMock(return_value=sync_nvr_result)) as mock_sync_nvr,
            mock.patch.object(self.cli, "_rebase", mock.AsyncMock()) as mock_rebase,
            mock.patch.object(self.cli, "_build", mock.AsyncMock()) as mock_build,
        ):
            result = await self.cli.run()

        mock_sync_nvr.assert_awaited_once()
        mock_rebase.assert_not_awaited()
        mock_build.assert_not_awaited()
        self.assertEqual(result, sync_nvr_result)

    async def test_run_with_nvr_does_not_require_release(self):
        self.cli.nvr = "release-payload-4.21.1-202608011200.p2"
        self.cli.release = None

        with mock.patch.object(self.cli, "_sync_nvr", mock.AsyncMock(return_value={"nvr": self.cli.nvr})):
            result = await self.cli.run()

        self.assertEqual(result["nvr"], self.cli.nvr)

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
        self.assertFalse(result["synced"])

    async def test_run_with_push_and_sync_syncs_after_successful_build(self):
        self.cli.push = True
        self.cli.sync = True
        self.cli.dry_run = False

        build_result = {
            "output_image": "quay.io/example/repo:4.21.1-1",
            "pipelinerun_name": "release-payload-abc123",
            "pipelinerun_url": "https://konflux.example.com/pipelinerun/1",
            "outcome": str(KonfluxBuildOutcome.SUCCESS),
        }
        sync_result = {
            "synced": True,
            "release_repo": self.cli.release_image_repo,
            "release_pullspec": "quay.io/example/repo@sha256:deadbeef",
        }
        with (
            mock.patch.object(
                self.cli,
                "_rebase",
                mock.AsyncMock(return_value=(self.build_repo, "registry.example.com/cvo", self.build_repo.branch)),
            ),
            mock.patch.object(self.cli, "_build", mock.AsyncMock(return_value=build_result)),
            mock.patch.object(self.cli, "_sync", mock.AsyncMock(return_value=sync_result)) as mock_sync,
        ):
            result = await self.cli.run()

        mock_sync.assert_awaited_once_with(build_result["output_image"], ["x86_64", "s390x"])
        self.assertTrue(result["synced"])
        self.assertEqual(result["release_pullspec"], sync_result["release_pullspec"])

    async def test_run_without_sync_flag_does_not_call_sync(self):
        self.cli.push = True
        self.cli.sync = False
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
            mock.patch.object(self.cli, "_build", mock.AsyncMock(return_value=build_result)),
            mock.patch.object(self.cli, "_sync", mock.AsyncMock()) as mock_sync,
        ):
            result = await self.cli.run()

        mock_sync.assert_not_awaited()
        self.assertFalse(result["synced"])

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
