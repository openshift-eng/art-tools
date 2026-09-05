import json
import unittest
from unittest import mock

from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
)
from artcommonlib.model import Model
from doozerlib.cli.images_konflux import BundleStageReleaseRelatedImagesCli, KonfluxBundleCli, KonfluxRebaseCli
from doozerlib.exceptions import DoozerFatalError, ParentRebaseFailedError
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
        self.runtime.assembly_type = None
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

    async def test_get_bundle_build_by_nvr_searches_without_assembly_filter(self):
        existing_build = mock.Mock(spec=KonfluxBundleBuildRecord)

        async def search_builds_by_fields(**_kwargs):
            yield existing_build

        self.mock_bundle_db.search_builds_by_fields = mock.Mock(side_effect=search_builds_by_fields)

        result = await self.bundle_cli._get_bundle_build_by_nvr("test-operator-bundle-1.0.0-1")

        self.assertIs(result, existing_build)
        self.mock_bundle_db.search_builds_by_fields.assert_called_once_with(
            where={
                "nvr": "test-operator-bundle-1.0.0-1",
                "outcome": str(KonfluxBuildOutcome.SUCCESS),
            },
            limit=1,
        )

    async def test_rebase_and_build_rejects_duplicate_nvr_from_another_assembly(self):
        operator_build = self._create_operator_build("test-operator", "test-operator-1.0.0-1.assembly.stream")
        image_meta = mock.Mock()
        rebaser = mock.AsyncMock()
        rebaser.rebase.return_value = "test-operator-bundle-1.0.0-1"
        builder = mock.AsyncMock()

        existing_build = mock.Mock(spec=KonfluxBundleBuildRecord)
        existing_build.assembly = "stream"
        existing_build.image_pullspec = "quay.io/example/bundle@sha256:existing"

        self.bundle_cli._get_bundle_build_for = mock.AsyncMock(return_value=None)
        self.bundle_cli._get_bundle_build_by_nvr = mock.AsyncMock(return_value=existing_build)

        with self.assertRaisesRegex(
            ValueError,
            "Successful bundle NVR test-operator-bundle-1.0.0-1 already exists in DB",
        ):
            await self.bundle_cli._rebase_and_build(rebaser, builder, image_meta, operator_build)

        rebaser.rebase.assert_awaited_once_with(image_meta, operator_build, "1")
        self.bundle_cli._get_bundle_build_by_nvr.assert_awaited_once_with("test-operator-bundle-1.0.0-1")
        builder.build.assert_not_awaited()

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
        mock_builder._konflux_client.token_refresh_loop = mock.AsyncMock()
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
        mock_builder._konflux_client.token_refresh_loop = mock.AsyncMock()
        mock_builder._konflux_client.delete_git_auth_secret = mock.AsyncMock()
        mock_builder._konflux_client.cleanup_stale_git_auth_secrets = mock.AsyncMock()
        mock_builder_class.return_value = mock_builder

        with self.assertRaises(DoozerFatalError):
            await self.bundle_cli.run()


class TestKonfluxRebaseCli(unittest.IsolatedAsyncioTestCase):
    """Tests for beta:images:konflux:rebase state recording."""

    @mock.patch("doozerlib.cli.images_konflux.trace.get_current_span")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxRebaser")
    async def test_run_records_direct_failures_separately_from_skipped_due_to_parent(
        self, mock_rebaser_cls, mock_get_span
    ):
        """Direct rebase failures go to failed-images; cascade failures use skipped-due-to-parent-rebase-failure."""
        mock_get_span.return_value = mock.Mock()

        parent = mock.Mock()
        parent.distgit_key = "parent-img"
        parent.get_lockfile_backend.return_value = "art-internal"
        child = mock.Mock()
        child.distgit_key = "child-img"
        child.get_lockfile_backend.return_value = "art-internal"

        runtime = mock.Mock(spec=Runtime)
        runtime.working_dir = "/tmp"
        runtime.upcycle = False
        runtime.initialize = mock.Mock()
        runtime.source_resolver = mock.Mock(spec=SourceResolver)
        runtime.ordered_image_metas = mock.Mock(return_value=[parent, child])
        runtime.state = {}

        mock_rebaser = mock_rebaser_cls.return_value
        mock_rebaser.rpm_lockfile_generator.ensure_repositories_loaded = mock.AsyncMock()

        async def rebase_to_side_effect(meta, *_args, **_kwargs):
            if meta.distgit_key == "parent-img":
                raise RuntimeError("upstream merge conflict")
            raise ParentRebaseFailedError(meta.distgit_key, ["parent-img"])

        mock_rebaser.rebase_to = mock.AsyncMock(side_effect=rebase_to_side_effect)

        cli = KonfluxRebaseCli(
            runtime=runtime,
            version="4.14.0",
            release="1",
            embargoed=False,
            force_yum_updates=False,
            repo_type="unsigned",
            image_repo="test-repo",
            message="test",
            push=False,
        )

        with self.assertRaises(DoozerFatalError):
            await cli.run()

        rebase_state = runtime.state["images:konflux:rebase"]
        self.assertEqual(rebase_state["failed-images"], ["parent-img"])
        self.assertEqual(
            rebase_state["skipped-due-to-parent-rebase-failure"],
            ["child-img"],
        )
        # Per-image status dict must be present
        self.assertEqual(rebase_state["images"]["parent-img"], {"status": "failure"})
        self.assertEqual(rebase_state["images"]["child-img"], {"status": "skipped"})

    @mock.patch("doozerlib.cli.images_konflux.trace.get_current_span")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxRebaser")
    async def test_ensure_repositories_loaded_skips_prototype_backend(self, mock_rebaser_cls, mock_get_span):
        """ensure_repositories_loaded should only receive art-internal images, not rpm-lockfile-prototype ones."""
        mock_get_span.return_value = mock.Mock()

        art_internal_img = mock.Mock()
        art_internal_img.distgit_key = "art-internal-img"
        art_internal_img.get_lockfile_backend.return_value = "art-internal"

        prototype_img = mock.Mock()
        prototype_img.distgit_key = "prototype-img"
        prototype_img.get_lockfile_backend.return_value = "rpm-lockfile-prototype"

        runtime = mock.Mock(spec=Runtime)
        runtime.working_dir = "/tmp"
        runtime.upcycle = False
        runtime.initialize = mock.Mock()
        runtime.source_resolver = mock.Mock(spec=SourceResolver)
        runtime.ordered_image_metas = mock.Mock(return_value=[art_internal_img, prototype_img])
        runtime.state = {}

        mock_rebaser = mock_rebaser_cls.return_value
        mock_rebaser.rpm_lockfile_generator.ensure_repositories_loaded = mock.AsyncMock()
        mock_rebaser.rebase_to = mock.AsyncMock(return_value=("4.14.0", "1"))

        cli = KonfluxRebaseCli(
            runtime=runtime,
            version="4.14.0",
            release="1",
            embargoed=False,
            force_yum_updates=False,
            repo_type="unsigned",
            image_repo="test-repo",
            message="test",
            push=False,
        )

        await cli.run()

        mock_rebaser.rpm_lockfile_generator.ensure_repositories_loaded.assert_called_once()
        loaded_metas = mock_rebaser.rpm_lockfile_generator.ensure_repositories_loaded.call_args[0][0]
        self.assertEqual([m.distgit_key for m in loaded_metas], ["art-internal-img"])

        self.assertEqual(mock_rebaser.rebase_to.call_count, 2)
        rebased_keys = [call.args[0].distgit_key for call in mock_rebaser.rebase_to.call_args_list]
        self.assertIn("art-internal-img", rebased_keys)
        self.assertIn("prototype-img", rebased_keys)

        # Success path must also write per-image state
        rebase_state = runtime.state["images:konflux:rebase"]
        self.assertEqual(rebase_state["images"]["art-internal-img"], {"status": "success"})
        self.assertEqual(rebase_state["images"]["prototype-img"], {"status": "success"})
        self.assertNotIn("failed-images", rebase_state)
        self.assertNotIn("skipped-due-to-parent-rebase-failure", rebase_state)

    @mock.patch("doozerlib.cli.images_konflux.trace.get_current_span")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxRebaser")
    async def test_run_success_writes_all_images_state(self, mock_rebaser_cls, mock_get_span):
        """On full success, state.yaml records every image with status='success'."""
        mock_get_span.return_value = mock.Mock()

        img_a = mock.Mock()
        img_a.distgit_key = "img-a"
        img_a.get_lockfile_backend.return_value = "art-internal"
        img_b = mock.Mock()
        img_b.distgit_key = "img-b"
        img_b.get_lockfile_backend.return_value = "art-internal"

        runtime = mock.Mock(spec=Runtime)
        runtime.working_dir = "/tmp"
        runtime.upcycle = False
        runtime.initialize = mock.Mock()
        runtime.source_resolver = mock.Mock(spec=SourceResolver)
        runtime.ordered_image_metas = mock.Mock(return_value=[img_a, img_b])
        runtime.state = {}

        mock_rebaser = mock_rebaser_cls.return_value
        mock_rebaser.rpm_lockfile_generator.ensure_repositories_loaded = mock.AsyncMock()
        mock_rebaser.rebase_to = mock.AsyncMock(return_value=("4.14.0", "1"))

        rebase_cli = KonfluxRebaseCli(
            runtime=runtime,
            version="4.14.0",
            release="1",
            embargoed=False,
            force_yum_updates=False,
            repo_type="unsigned",
            image_repo="test-repo",
            message="test",
            push=False,
        )

        await rebase_cli.run()

        rebase_state = runtime.state["images:konflux:rebase"]
        self.assertEqual(
            rebase_state["images"],
            {
                "img-a": {"status": "success"},
                "img-b": {"status": "success"},
            },
        )
        self.assertNotIn("failed-images", rebase_state)
        self.assertNotIn("skipped-due-to-parent-rebase-failure", rebase_state)


class TestBundleStageReleaseRelatedImagesCli(unittest.IsolatedAsyncioTestCase):
    """Cover product major/minor resolution and the per-operator stage release loop.

    Layered products (e.g. ACM) set `version: 2.16.0` in group.yml while using MAJOR/MINOR for
    the OCP brew-branch version (e.g. 4.21).  OCP groups have no `version:` field and their
    MAJOR/MINOR vars ARE the product version.  The code must prefer group_config.version when
    present and fall back to vars.MAJOR/MINOR only when it is absent (MissingModel is falsy).

    Each operator is stage-released on its own so that one failure cannot stop the others from
    reaching their FBC builds; every outcome is written to record.log for the calling pipeline.
    """

    def _make_runtime(self, *, product: str, version_str: str = "", major: int = 0, minor: int = 0):
        runtime = mock.Mock(spec=Runtime)
        runtime.assembly = "stream"
        runtime.product = product
        runtime.konflux_db = mock.Mock()
        runtime.konflux_db.bind = mock.Mock()
        runtime.group = "rhacm2-2.16"
        runtime.record_logger = mock.Mock()
        gc_data = {"vars": {"MAJOR": major, "MINOR": minor}}
        if version_str:
            gc_data["version"] = version_str
        runtime.group_config = Model(gc_data)
        return runtime

    def _make_cli(self, runtime, operator_nvrs=("some-operator-nvr",)):
        return BundleStageReleaseRelatedImagesCli(
            runtime=runtime,
            operator_nvrs=operator_nvrs,
            stage_release_plan=None,
            konflux_kubeconfig="/path/to/kubeconfig",
            konflux_context=None,
            konflux_namespace="test-namespace",
            dry_run=False,
        )

    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    @mock.patch("doozerlib.cli.images_konflux.resolve_konflux_fbc_stage_release_plan")
    async def test_layered_product_uses_group_config_version(self, mock_resolve, mock_db_class):
        """ACM: group_config.version='2.16.0' must be used, not vars.MAJOR=4 / MINOR=21."""
        mock_db_class.return_value = mock.Mock()
        mock_resolve.return_value = None  # no plan → early return after the resolve call

        runtime = self._make_runtime(product="rhacm2", version_str="2.16.0", major=4, minor=21)
        await self._make_cli(runtime).run()

        mock_resolve.assert_called_once_with("rhacm2", 2, 16)

    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    @mock.patch("doozerlib.cli.images_konflux.resolve_konflux_fbc_stage_release_plan")
    async def test_ocp_group_uses_vars_major_minor(self, mock_resolve, mock_db_class):
        """OCP: no version: field in group config; vars.MAJOR/MINOR are the product version."""
        mock_db_class.return_value = mock.Mock()
        mock_resolve.return_value = None

        runtime = self._make_runtime(product="ocp", major=5, minor=0)
        await self._make_cli(runtime).run()

        mock_resolve.assert_called_once_with("ocp", 5, 0)

    @staticmethod
    def _mock_konflux_client(released_condition):
        client = mock.AsyncMock()
        client.resource_url = mock.Mock(return_value="https://konflux/url")
        created = mock.Mock()
        created.metadata.name = "fbc-ri-stage-rhacm2-2-16-operator-a-xyz"
        client._create = mock.AsyncMock(return_value=created)
        client._get = mock.AsyncMock(return_value={})
        client.wait_for_release = mock.AsyncMock(return_value={"status": {"conditions": [released_condition]}})
        return client

    @staticmethod
    def _component(name, pullspec):
        return {
            "name": name,
            "source": {"git": {"url": "https://git/repo", "revision": "deadbeef"}},
            "containerImage": pullspec,
        }

    @staticmethod
    def _operator_record(name):
        record = mock.Mock()
        record.name = name
        record.nvr = f"{name}-1-1"
        return record

    @staticmethod
    def _ref(component_name):
        build = mock.Mock()
        build.get_konflux_component_name.return_value = component_name
        build.rebase_repo_url = "https://git/repo"
        build.rebase_commitish = "deadbeef"
        build.image_pullspec = f"img-{component_name}"
        return build

    def _prepare_run(self, runtime, operator_names, records=None):
        """Wire up a runtime so run() reaches the per-operator stage release loop."""
        operators = records if records is not None else [self._operator_record(n) for n in operator_names]
        runtime.konflux_db.get_build_records_by_nvrs = mock.AsyncMock(return_value=operators)
        meta = mock.Mock()
        meta.get_olm_bundle_short_name.return_value = "some-bundle"
        runtime.image_map = {name: meta for name in operator_names}
        return operators

    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    async def test_stage_release_creates_snapshot_and_release_for_one_operator(self, mock_db_class):
        """A single operator's related images go into one Snapshot and one Release."""
        client = self._mock_konflux_client({"type": "Released", "status": "True", "reason": "Succeeded"})
        cli = self._make_cli(self._make_runtime(product="rhacm2", version_str="2.16.0", major=4, minor=21))

        components = [self._component("comp-a", "img-a"), self._component("comp-b", "img-b")]
        url = await cli._stage_release(
            konflux_client=client,
            components=components,
            release_plan_name="acm-advisory-stage-2-16",
            group="rhacm2-2.16",
            assembly="stream",
            operator_name="operator-a",
            operator_nvr="operator-a-1-1",
        )

        self.assertEqual(url, "https://konflux/url")
        self.assertEqual([c.args[0]["kind"] for c in client._create.call_args_list], ["Snapshot", "Release"])
        snapshot = client._create.call_args_list[0].args[0]
        self.assertEqual([c["name"] for c in snapshot["spec"]["components"]], ["comp-a", "comp-b"])
        self.assertEqual(snapshot["metadata"]["labels"]["release.appstudio.openshift.io/auto-release"], "false")
        # Names are generated server-side so two operators cannot collide on a truncated label
        self.assertNotIn("name", snapshot["metadata"])
        self.assertTrue(snapshot["metadata"]["generateName"].startswith("fbc-ri-stage-rhacm2-2-16-operator-a-"))
        release = client._create.call_args_list[1].args[0]
        self.assertEqual(release["spec"]["releasePlan"], "acm-advisory-stage-2-16")
        self.assertEqual(release["metadata"]["annotations"]["art.redhat.com/operator-nvr"], "operator-a-1-1")
        client.wait_for_release.assert_awaited_once_with("fbc-ri-stage-rhacm2-2-16-operator-a-xyz")

    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    async def test_stage_release_raises_when_release_fails(self, mock_db_class):
        client = self._mock_konflux_client(
            {"type": "Released", "status": "False", "reason": "Failed", "message": "managed pipeline blew up"}
        )
        cli = self._make_cli(self._make_runtime(product="rhacm2", version_str="2.16.0", major=4, minor=21))

        with self.assertRaisesRegex(RuntimeError, "managed pipeline blew up"):
            await cli._stage_release(
                konflux_client=client,
                components=[self._component("comp-a", "img-a")],
                release_plan_name="acm-advisory-stage-2-16",
                group="rhacm2-2.16",
                assembly="stream",
                operator_name="operator-a",
                operator_nvr="operator-a-1-1",
            )

    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    async def test_stage_release_raises_when_release_is_still_progressing(self, mock_db_class):
        client = self._mock_konflux_client(
            {"type": "Released", "status": "False", "reason": "Progressing", "message": "release is running"}
        )
        cli = self._make_cli(self._make_runtime(product="rhacm2", version_str="2.16.0", major=4, minor=21))

        with self.assertRaisesRegex(RuntimeError, "Progressing"):
            await cli._stage_release(
                konflux_client=client,
                components=[self._component("comp-a", "img-a")],
                release_plan_name="acm-advisory-stage-2-16",
                group="rhacm2-2.16",
                assembly="stream",
                operator_name="operator-a",
                operator_nvr="operator-a-1-1",
            )

    @mock.patch("doozerlib.cli.images_konflux.get_referenced_images")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxClient")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    @mock.patch("doozerlib.cli.images_konflux.resolve_konflux_fbc_stage_release_plan")
    async def test_run_creates_one_release_per_operator(
        self, mock_resolve, mock_db_class, mock_client_cls, mock_get_refs
    ):
        """Two operators sharing an operand get one Release each; the shared operand is in both."""
        mock_resolve.return_value = "acm-advisory-stage-2-16"
        runtime = self._make_runtime(product="rhacm2", version_str="2.16.0", major=4, minor=21)
        self._prepare_run(runtime, ["operator-a", "operator-b"])

        mock_get_refs.side_effect = [
            [self._ref("comp-a"), self._ref("comp-shared")],
            [self._ref("comp-b"), self._ref("comp-shared")],
        ]

        async def _bundle_builds(*args, **kwargs):
            yield mock.Mock()

        cli = self._make_cli(runtime, operator_nvrs=("operator-a-1-1", "operator-b-1-1"))
        cli._db_for_bundles.search_builds_by_fields = _bundle_builds
        cli._stage_release = mock.AsyncMock(return_value="https://konflux/url")

        await cli.run()

        self.assertEqual(cli._stage_release.await_count, 2)
        first, second = cli._stage_release.await_args_list
        self.assertEqual([c["name"] for c in first.kwargs["components"]], ["comp-a", "comp-shared"])
        self.assertEqual(first.kwargs["operator_nvr"], "operator-a-1-1")
        self.assertEqual([c["name"] for c in second.kwargs["components"]], ["comp-b", "comp-shared"])
        self.assertEqual(second.kwargs["operator_nvr"], "operator-b-1-1")

        statuses = [call.kwargs["status"] for call in runtime.record_logger.add_record.call_args_list]
        self.assertEqual(statuses, [0, 0])

    @mock.patch("doozerlib.cli.images_konflux.get_referenced_images")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxClient")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    @mock.patch("doozerlib.cli.images_konflux.resolve_konflux_fbc_stage_release_plan")
    async def test_run_continues_after_operator_failure(
        self, mock_resolve, mock_db_class, mock_client_cls, mock_get_refs
    ):
        """A failing operator is recorded and skipped; the next operator is still released."""
        mock_resolve.return_value = "acm-advisory-stage-2-16"
        runtime = self._make_runtime(product="rhacm2", version_str="2.16.0", major=4, minor=21)
        self._prepare_run(runtime, ["operator-a", "operator-b"])

        mock_get_refs.side_effect = [[self._ref("comp-a")], [self._ref("comp-b")]]

        async def _bundle_builds(*args, **kwargs):
            yield mock.Mock()

        cli = self._make_cli(runtime, operator_nvrs=("operator-a-1-1", "operator-b-1-1"))
        cli._db_for_bundles.search_builds_by_fields = _bundle_builds
        cli._stage_release = mock.AsyncMock(side_effect=[RuntimeError("release blew up"), "https://konflux/url"])

        with self.assertRaisesRegex(DoozerFatalError, "operator-a-1-1"):
            await cli.run()

        self.assertEqual(cli._stage_release.await_count, 2)
        records = [call.kwargs for call in runtime.record_logger.add_record.call_args_list]
        self.assertEqual(
            [(r["operator_nvr"], r["status"]) for r in records], [("operator-a-1-1", 1), ("operator-b-1-1", 0)]
        )
        self.assertIn("release blew up", records[0]["message"])

    @mock.patch("doozerlib.cli.images_konflux.get_referenced_images")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxClient")
    @mock.patch("doozerlib.cli.images_konflux.KonfluxDb")
    @mock.patch("doozerlib.cli.images_konflux.resolve_konflux_fbc_stage_release_plan")
    async def test_run_records_operator_with_missing_build_record(
        self, mock_resolve, mock_db_class, mock_client_cls, mock_get_refs
    ):
        """An NVR with no build record fails only itself; the others are still stage-released."""
        mock_resolve.return_value = "acm-advisory-stage-2-16"
        runtime = self._make_runtime(product="rhacm2", version_str="2.16.0", major=4, minor=21)
        self._prepare_run(runtime, ["operator-b"], records=[None, self._operator_record("operator-b")])

        mock_get_refs.side_effect = [[self._ref("comp-b")]]

        async def _bundle_builds(*args, **kwargs):
            yield mock.Mock()

        cli = self._make_cli(runtime, operator_nvrs=("operator-a-1-1", "operator-b-1-1"))
        cli._db_for_bundles.search_builds_by_fields = _bundle_builds
        cli._stage_release = mock.AsyncMock(return_value="https://konflux/url")

        with self.assertRaisesRegex(DoozerFatalError, "operator-a-1-1"):
            await cli.run()

        # strict=False keeps a missing NVR from taking down the whole command
        self.assertFalse(runtime.konflux_db.get_build_records_by_nvrs.await_args.kwargs["strict"])
        cli._stage_release.assert_awaited_once()
        self.assertEqual(cli._stage_release.await_args.kwargs["operator_nvr"], "operator-b-1-1")
        records = [call.kwargs for call in runtime.record_logger.add_record.call_args_list]
        self.assertEqual(
            [(r["operator_nvr"], r["status"]) for r in records], [("operator-a-1-1", 1), ("operator-b-1-1", 0)]
        )
