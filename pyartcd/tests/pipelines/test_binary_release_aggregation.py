import os
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, call, patch

import yaml
from artcommonlib.model import Model
from doozerlib.source_resolver import SourceResolver
from elliottlib.shipment_model import ComponentSource, GitSource, Snapshot, SnapshotComponent, SnapshotSpec
from pyartcd.pipelines import binary_release_konflux as release

SOURCE_NVR = "oc-mirror-container-2.0-202611121200.p2.assembly.stream.el9"
AGGREGATE_NVR = "oc-mirror-aggregate-binaries-container-2.0.0-202611121300.p2.assembly.stream.el9"
IMAGE_REPO = "quay.io/example/oc-mirror-builds"
RECIPE_SOURCE_URL = "git@github.com:openshift-priv/ocp-build-data.git"
ARCHES = ("amd64", "arm64", "ppc64le", "s390x")
PARENTS = {arch: f"{IMAGE_REPO}@sha256:{i:064x}" for i, arch in enumerate(ARCHES, 1)}


def _snapshot(aggregate=False):
    return Snapshot(
        spec=SnapshotSpec(
            application="oc-mirror-2-0",
            components=[
                SnapshotComponent(
                    name="oc-mirror-2-0-oc-mirror" + ("-aggregate-binaries" if aggregate else ""),
                    containerImage=IMAGE_REPO + "@sha256:" + "a" * 64,
                    source=ComponentSource(git=GitSource(url="https://github.com/test/repo", revision="abc123")),
                )
            ],
        ),
        nvrs=[AGGREGATE_NVR if aggregate else SOURCE_NVR],
    )


def _pipeline(directory, **kwargs):
    runtime = MagicMock()
    runtime.working_dir = Path(directory)
    runtime.dry_run = False
    runtime.config = {}
    options = dict(group="oc-mirror-2.0", assembly="stream", nvrs=[SOURCE_NVR], aggregate=True)
    options.update(kwargs)
    return release.BinaryReleaseKonfluxPipeline(runtime=runtime, **options)


class TestAggregationOptions(TestCase):
    def test_combined_aggregation_rejects_overrides_before_starting_work(self):
        for options in (
            {"shipment_kind": "image"},
            {"shipment_kind": "binaries"},
            {"release_plan_key": "oc-mirror-2-0"},
        ):
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, "--aggregate cannot use"):
                _pipeline("/tmp/test", **options)

    def test_build_only_can_ignore_shipment_overrides(self):
        _pipeline("/tmp/test", build_only=True, shipment_kind="image", release_plan_key="oc-mirror-2-0")

    def test_aggregation_requires_one_oc_mirror_source_build(self):
        for options in ({"group": "mirror-gui-1.0"}, {"nvrs": [SOURCE_NVR, SOURCE_NVR]}, {"nvrs": []}):
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, "exactly one source NVR"):
                _pipeline("/tmp/test", **options)

    def test_build_only_requires_aggregation(self):
        with self.assertRaisesRegex(ValueError, "--build-only requires --aggregate"):
            _pipeline("/tmp/test", aggregate=False, build_only=True)

    def test_build_only_needs_no_gitlab_token(self):
        pipeline = _pipeline("/tmp/test", create_mr=True, build_only=True)
        with patch.dict(os.environ, {}, clear=True):
            pipeline.check_env_vars()

    def test_other_products_keep_their_dry_run_shipment_credentials(self):
        pipeline = _pipeline("/tmp/test", group="mirror-gui-1.0", aggregate=False, create_mr=True)
        pipeline.dry_run = True
        with patch.dict(os.environ, {"GITLAB_TOKEN": "unit-test-token"}, clear=True):
            pipeline.check_env_vars()
        self.assertEqual(pipeline.gitlab_token, "unit-test-token")


class TestBuildAggregate(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.pipeline = _pipeline(self.tmp.name)
        self.pipeline.product = "oc-mirror"
        self.pipeline.setup_working_dir()
        self.source = _snapshot()
        self.wrong_parent = False
        self.record_status = "0"
        self.aggregate_nvr = AGGREGATE_NVR
        self.extra_stage = False
        self.record_count = 1
        self.recipe_source_url = RECIPE_SOURCE_URL
        self.commands = self.enterContext(patch.object(release.exectools, "cmd_assert_async", new_callable=AsyncMock))
        self.commands.side_effect = self.command
        self.image_info = self.enterContext(patch.object(release, "oc_image_info_for_arch_async"))
        self.image_info.side_effect = lambda image, go_arch, **kwargs: {
            "digest": PARENTS[go_arch.split("/")[1]].split("@")[1],
            "config": {"os": "linux", "architecture": go_arch.split("/")[1]},
        }
        self.repo_cls = self.enterContext(patch.object(release, "GitRepository"))
        self.repo_cls.side_effect = self.data_repo
        self.enterContext(
            patch.dict(
                os.environ,
                {
                    "DOOZER_DATA_PATH": "https://github.com/openshift-eng/ocp-build-data",
                    "OC_MIRROR_KONFLUX_SA_KUBECONFIG": "/test/oc-mirror-kubeconfig",
                    "QUAY_AUTH_FILE": "/test/registry-auth",
                },
            )
        )

    def data_repo(self, directory):
        self.data_dir = Path(directory)
        repo = MagicMock()
        repo.setup = AsyncMock()
        self.recipe_repo = repo

        async def fetch(branch):
            config = self.data_dir / "binaries/oc-mirror-aggregate.yml"
            config.parent.mkdir(parents=True)
            config.write_text(
                yaml.safe_dump(
                    {
                        "from": {"builder": [{"member": "oc-mirror"}] * 4},
                        "content": {
                            "source": {
                                "git": {"url": self.recipe_source_url},
                                "path": "binaries/oc-mirror-aggregate",
                            }
                        },
                    }
                )
            )

        repo.fetch_switch_branch = AsyncMock(side_effect=fetch)
        return repo

    async def command(self, argv):
        if "beta:binaries:konflux:rebase" in argv:
            self.pinned = yaml.safe_load((self.data_dir / "binaries/oc-mirror-aggregate.yml").read_text())
            source_index = argv.index("--source")
            self.assertEqual(argv[source_index + 1 : source_index + 3], ["oc-mirror-aggregate", str(self.data_dir)])
            dockerfile = (
                self.pipeline.doozer_working_dir / "konflux_build_sources/containers/oc-mirror-aggregate/Dockerfile"
            )
            dockerfile.parent.mkdir(parents=True)
            parents = dict(PARENTS)
            if self.wrong_parent:
                parents["amd64"] = IMAGE_REPO + "@sha256:" + "f" * 64
            dockerfile.write_text(
                "\n".join(f"FROM --platform=linux/{arch} {image} AS {arch}" for arch, image in parents.items())
            )
            if self.extra_stage:
                with dockerfile.open("a") as stream:
                    stream.write(f"\nFROM --platform=linux/amd64 {parents['amd64']} AS amd64\n")
        elif "beta:binaries:konflux:build" in argv:
            self.assertIn("--konflux-namespace=art-oc-mirror-tenant", argv)
            self.assertIn("--konflux-kubeconfig=/test/oc-mirror-kubeconfig", argv)
            self.assertIn(f"--image-repo={IMAGE_REPO}", argv)
            (self.pipeline.doozer_working_dir / "record.log").write_text(
                f"binary_build_konflux|name=oc-mirror-aggregate|status={self.record_status}|nvrs={self.aggregate_nvr}|\n"
                * self.record_count
            )
        else:
            self.fail(f"Unexpected command: {argv}")

    async def test_pins_the_selected_source_before_rebase_and_returns_the_aggregate_nvr(self):
        nvr = await self.pipeline.build_aggregate(self.source)
        self.assertEqual(nvr, AGGREGATE_NVR)
        self.assertEqual(self.pinned["from"]["builder"], [{"image": image} for image in PARENTS.values()])
        self.assertNotIn("git", self.pinned["content"]["source"])
        self.assertEqual(self.pinned["content"]["source"]["alias"], "oc-mirror-aggregate")
        self.assertEqual(
            self.recipe_repo.setup.await_args_list,
            [call("https://github.com/openshift-eng/ocp-build-data"), call(RECIPE_SOURCE_URL)],
        )
        for invocation in self.image_info.await_args_list:
            self.assertEqual(invocation.args[0], self.source.spec.components[0].containerImage)

    async def test_recipe_alias_keeps_the_fetched_checkout_and_private_rebase_destination(self):
        await self.pipeline.build_aggregate(self.source)
        checkout = Path(self.tmp.name, "checkout")
        (checkout / "binaries/oc-mirror-aggregate").mkdir(parents=True)
        resolver = SourceResolver(str(checkout / "sources"), None, Model({"public_upstreams": []}))

        def git_metadata(command, **kwargs):
            if command[-1] == "remote.origin.url":
                return 0, RECIPE_SOURCE_URL, ""
            if command[1] == "rev-parse":
                return 0, "oc-mirror-2.0", ""
            return 1, "", ""

        with (
            patch("doozerlib.source_resolver.exectools.cmd_gather", side_effect=git_metadata),
            patch(
                "doozerlib.source_resolver.exectools.cmd_assert", side_effect=[("a" * 40, ""), ("0", ""), ("aaa", "")]
            ),
            patch("doozerlib.source_resolver.git_clone", side_effect=AssertionError("Unexpected remote clone")),
        ):
            resolver.register_source_alias("oc-mirror-aggregate", str(checkout))
            meta = MagicMock(config=Model(self.pinned))
            meta.has_source.return_value = True
            source = resolver.resolve_source(meta)
        self.assertEqual(SourceResolver.get_source_dir(source, meta), checkout / "binaries/oc-mirror-aggregate")
        self.assertEqual(source.url, RECIPE_SOURCE_URL)
        self.assertIsNone(source.pull_url)

    async def test_rebase_destination_comes_from_the_recipe(self):
        self.recipe_source_url = "git@github.com:example/ocp-build-data.git"
        await self.pipeline.build_aggregate(self.source)
        self.recipe_repo.setup.assert_awaited_with(self.recipe_source_url)

    async def test_missing_recipe_destination_stops_before_rebase(self):
        self.recipe_source_url = None
        with self.assertRaisesRegex(ValueError, "must declare content.source.git.url"):
            await self.pipeline.build_aggregate(self.source)
        self.commands.assert_not_awaited()

    async def test_rebase_version_comes_from_the_group_profile(self):
        self.pipeline.aggregation_profile = replace(self.pipeline.aggregation_profile, version="v2.0.1")
        await self.pipeline.build_aggregate(self.source)
        self.assertIn("--version=v2.0.1", self.commands.await_args_list[0].args[0])

    async def test_local_build_data_is_rejected_before_any_fetch_or_build(self):
        for data_path in (self.tmp.name, "../ocp-build-data", Path(self.tmp.name).as_uri()):
            with (
                self.subTest(data_path=data_path),
                patch.dict(os.environ, {"DOOZER_DATA_PATH": data_path}),
                self.assertRaisesRegex(ValueError, "requires a remote Git URL"),
            ):
                await self.pipeline.build_aggregate(self.source)
        self.image_info.assert_not_awaited()
        self.repo_cls.assert_not_called()
        self.commands.assert_not_awaited()

    async def test_scp_style_git_urls_are_supported(self):
        with patch.dict(os.environ, {"DOOZER_DATA_PATH": "git@github.com:openshift-eng/ocp-build-data.git"}):
            self.assertEqual(await self.pipeline.build_aggregate(self.source), AGGREGATE_NVR)

    async def test_missing_architecture_stops_before_rebase(self):
        valid_lookup = self.image_info.side_effect

        def missing_s390x(image, go_arch, **kwargs):
            if go_arch == "linux/s390x":
                raise ValueError("No s390x image")
            return valid_lookup(image, go_arch, **kwargs)

        self.image_info.side_effect = missing_s390x
        with self.assertRaisesRegex(ValueError, "s390x"):
            await self.pipeline.build_aggregate(self.source)
        self.repo_cls.assert_not_called()
        self.commands.assert_not_awaited()
        self.assertEqual([c.kwargs["go_arch"] for c in self.image_info.await_args_list], [f"linux/{a}" for a in ARCHES])

    async def test_invalid_architecture_digest_stops_before_rebase(self):
        self.image_info.side_effect = None
        self.image_info.return_value = {"digest": ""}
        with self.assertRaisesRegex(ValueError, "No valid amd64 digest"):
            await self.pipeline.build_aggregate(self.source)
        self.commands.assert_not_awaited()

    async def test_single_arch_image_returned_for_every_lookup_stops_before_rebase(self):
        self.image_info.side_effect = None
        self.image_info.return_value = {
            "digest": PARENTS["amd64"].split("@")[1],
            "config": {"os": "linux", "architecture": "amd64"},
        }
        with self.assertRaisesRegex(ValueError, "does not provide a linux/arm64 image"):
            await self.pipeline.build_aggregate(self.source)
        self.assertEqual(
            [invocation.kwargs["go_arch"] for invocation in self.image_info.await_args_list],
            ["linux/amd64", "linux/arm64"],
        )
        self.repo_cls.assert_not_called()
        self.commands.assert_not_awaited()

    async def test_wrong_or_missing_platform_metadata_stops_before_rebase(self):
        self.image_info.side_effect = None
        for image_config in (
            {"os": "windows", "architecture": "amd64"},
            {"os": "linux", "architecture": "arm64"},
            {"os": "linux"},
            {"architecture": "amd64"},
            {},
            None,
        ):
            self.image_info.return_value = {"digest": PARENTS["amd64"].split("@")[1], "config": image_config}
            with self.subTest(config=image_config), self.assertRaisesRegex(ValueError, "linux/amd64 image"):
                await self.pipeline.build_aggregate(self.source)
        self.repo_cls.assert_not_called()
        self.commands.assert_not_awaited()

    async def test_parent_mismatch_stops_before_build(self):
        self.wrong_parent = True
        with self.assertRaisesRegex(ValueError, "parents do not match"):
            await self.pipeline.build_aggregate(self.source)
        self.assertEqual(self.commands.await_count, 1)

    async def test_extra_dockerfile_stage_stops_before_build(self):
        self.extra_stage = True
        with self.assertRaisesRegex(ValueError, "parents do not match"):
            await self.pipeline.build_aggregate(self.source)
        self.assertEqual(self.commands.await_count, 1)

    async def test_rejects_an_existing_aggregate_as_the_source(self):
        with self.assertRaisesRegex(ValueError, "not an existing aggregate"):
            await self.pipeline.build_aggregate(_snapshot(aggregate=True))
        self.commands.assert_not_awaited()

    async def test_rejects_a_mutable_source_image(self):
        self.source.spec.components[0].containerImage = IMAGE_REPO + ":latest"
        with self.assertRaisesRegex(ValueError, "Source NVR must resolve to a digest-pinned image"):
            await self.pipeline.build_aggregate(self.source)
        self.commands.assert_not_awaited()

    async def test_failed_or_incomplete_builds_never_return_an_nvr(self):
        for status, nvr, message in (
            ("1", AGGREGATE_NVR, "No successful aggregate build"),
            ("0", "n/a", "Expected exactly one aggregate NVR"),
            ("0", "", "Expected exactly one aggregate NVR"),
            ("0", f"{AGGREGATE_NVR},{AGGREGATE_NVR}", "Expected exactly one aggregate NVR"),
            ("0", AGGREGATE_NVR.replace(".p2.", ".p3."), "embargoed aggregate build"),
        ):
            with self.subTest(status=status, nvr=nvr), self.assertRaisesRegex(ValueError, message):
                self.record_status = status
                self.aggregate_nvr = nvr
                # Each attempt represents a new CLI invocation with a fresh working directory.
                self.pipeline.setup_working_dir()
                await self.pipeline.build_aggregate(self.source)

    async def test_duplicate_build_records_are_rejected(self):
        self.record_count = 2
        with self.assertRaisesRegex(ValueError, "No successful aggregate build"):
            await self.pipeline.build_aggregate(self.source)


class TestReusedAggregate(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.pipeline = _pipeline(self.tmp.name, aggregate=False, nvrs=[SOURCE_NVR, AGGREGATE_NVR])
        self.pipeline.resolve_aggregate_parents = AsyncMock(return_value=PARENTS)
        self.source = _snapshot()
        self.aggregate = _snapshot(aggregate=True)
        self.revision = "b" * 40
        self.aggregate.spec.components[0].source.git.revision = self.revision
        self.repo = MagicMock()
        self.repo.setup = AsyncMock()
        self.repo.fetch_switch_branch = AsyncMock()
        self.repo.read_file = AsyncMock(
            return_value="\n".join(f"FROM --platform=linux/{a} {p} AS {a}" for a, p in PARENTS.items())
        )
        self.repo_cls = self.enterContext(patch.object(release, "GitRepository", return_value=self.repo))

    async def test_matching_parents_are_checked_at_the_recorded_rebase_commit(self):
        await self.pipeline.verify_reused_aggregate(self.source, self.aggregate)
        self.pipeline.resolve_aggregate_parents.assert_awaited_once_with(self.source)
        self.repo.setup.assert_awaited_once_with(self.aggregate.spec.components[0].source.git.url)
        self.repo.fetch_switch_branch.assert_awaited_once_with("aggregate-verification", upstream_ref=self.revision)
        self.repo.read_file.assert_awaited_once_with("Dockerfile")

    async def test_a_different_source_is_rejected(self):
        self.repo.read_file.return_value = self.repo.read_file.return_value.replace(PARENTS["s390x"], PARENTS["amd64"])
        with self.assertRaisesRegex(ValueError, "parents do not match the selected source NVR"):
            await self.pipeline.verify_reused_aggregate(self.source, self.aggregate)

    async def test_mutable_or_missing_git_provenance_is_rejected_before_fetch(self):
        for url, revision in (
            ("", self.revision),
            ("https://github.com/test/repo", "main"),
            ("https://github.com/test/repo", ""),
        ):
            git = self.aggregate.spec.components[0].source.git
            git.url, git.revision = url, revision
            with (
                self.subTest(url=url, revision=revision),
                self.assertRaisesRegex(ValueError, "recorded Git URL and full rebase commit"),
            ):
                await self.pipeline.verify_reused_aggregate(self.source, self.aggregate)
        self.repo_cls.assert_not_called()

    async def test_mutable_aggregate_image_is_rejected(self):
        self.aggregate.spec.components[0].containerImage = IMAGE_REPO + ":latest"
        with self.assertRaisesRegex(ValueError, "Reused aggregate must resolve to a digest-pinned image"):
            await self.pipeline.verify_reused_aggregate(self.source, self.aggregate)
        self.repo_cls.assert_not_called()

    async def test_unavailable_build_source_is_not_accepted(self):
        self.repo.fetch_switch_branch.side_effect = RuntimeError("commit unavailable")
        with self.assertRaisesRegex(RuntimeError, "commit unavailable"):
            await self.pipeline.verify_reused_aggregate(self.source, self.aggregate)
        self.repo.read_file.assert_not_awaited()

    async def test_missing_dockerfile_is_not_accepted(self):
        self.repo.read_file.side_effect = FileNotFoundError("Dockerfile missing")
        with self.assertRaisesRegex(FileNotFoundError, "Dockerfile missing"):
            await self.pipeline.verify_reused_aggregate(self.source, self.aggregate)


class TestAggregationRun(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.pipeline = _pipeline(self.tmp.name, create_mr=True)
        self.pipeline.check_env_vars = MagicMock()
        self.pipeline._load_product_from_group_config = AsyncMock(return_value="oc-mirror")
        self.pipeline.setup_shipment_repo = AsyncMock()
        self.pipeline.create_snapshot = AsyncMock(side_effect=[_snapshot(), _snapshot(aggregate=True), _snapshot()])
        self.pipeline.build_aggregate = AsyncMock(return_value=AGGREGATE_NVR)
        self.pipeline.verify_reused_aggregate = AsyncMock()
        self.pipeline.create_shipment_config = MagicMock(return_value=MagicMock())
        self.pipeline.create_shipment_mr = AsyncMock(return_value="https://gitlab/shipment/1")
        self.pipeline.set_shipment_mr_ready = AsyncMock()

    async def test_container_and_binaries_are_prepared_in_one_shipment_mr(self):
        await self.pipeline.run()
        self.assertEqual(
            self.pipeline.create_snapshot.await_args_list,
            [call([SOURCE_NVR]), call([AGGREGATE_NVR]), call([SOURCE_NVR])],
        )
        self.pipeline.create_shipment_config.assert_has_calls(
            [
                call(_snapshot(aggregate=True), rhel_suffix=None, shipment_kind="binaries"),
                call(_snapshot(), rhel_suffix=None, shipment_kind="image"),
            ]
        )
        self.pipeline.create_shipment_mr.assert_awaited_once()
        self.assertEqual(set(self.pipeline.create_shipment_mr.await_args.args[0]), {"image", "binaries"})

    async def test_existing_container_and_aggregate_can_be_shipped_without_rebuilding(self):
        self.pipeline.aggregate = False
        self.pipeline.nvrs = [SOURCE_NVR, AGGREGATE_NVR]
        self.pipeline.create_snapshot.side_effect = [_snapshot(aggregate=True), _snapshot()]
        await self.pipeline.run()
        self.pipeline.build_aggregate.assert_not_awaited()
        self.pipeline.verify_reused_aggregate.assert_awaited_once_with(_snapshot(), _snapshot(aggregate=True))
        self.assertEqual(set(self.pipeline.create_shipment_mr.await_args.args[0]), {"image", "binaries"})

    async def test_wrong_components_cannot_be_sent_to_a_container_release_plan(self):
        self.pipeline.aggregate = False
        self.pipeline.shipment_kind = "image"
        self.pipeline.create_snapshot.side_effect = None
        self.pipeline.create_snapshot.return_value = _snapshot(aggregate=True)
        with self.assertRaisesRegex(ValueError, "Unexpected oc-mirror image snapshot"):
            await self.pipeline.run()
        self.pipeline.create_shipment_mr.assert_not_awaited()

    async def test_build_only_stops_before_any_shipment_repository_work(self):
        self.pipeline.build_only = True
        await self.pipeline.run()
        self.pipeline.build_aggregate.assert_awaited_once()
        self.pipeline.setup_shipment_repo.assert_not_awaited()
        self.pipeline.create_shipment_mr.assert_not_awaited()
        self.assertEqual(self.pipeline.create_snapshot.await_count, 1)

    async def test_dry_run_does_not_build_or_create_a_shipment(self):
        self.pipeline.dry_run = True
        with (
            patch.dict(os.environ, {"DOOZER_DATA_PATH": "https://github.com/openshift-eng/ocp-build-data"}),
            patch.object(release, "oc_image_info_for_arch_async", new_callable=AsyncMock) as image_info,
        ):
            image_info.side_effect = lambda image, go_arch, **kwargs: {
                "digest": PARENTS[go_arch.split("/")[1]].split("@")[1],
                "config": {"os": "linux", "architecture": go_arch.split("/")[1]},
            }
            await self.pipeline.run()
        self.pipeline.create_snapshot.assert_awaited_once_with([SOURCE_NVR])
        self.assertEqual([c.kwargs["go_arch"] for c in image_info.await_args_list], [f"linux/{a}" for a in ARCHES])
        self.pipeline.build_aggregate.assert_not_awaited()
        self.pipeline.setup_shipment_repo.assert_not_awaited()
        self.pipeline.create_shipment_mr.assert_not_awaited()

    async def test_dry_run_rejects_invalid_source_metadata(self):
        self.pipeline.dry_run = True
        self.pipeline.create_snapshot.side_effect = None
        self.pipeline.create_snapshot.return_value = _snapshot(aggregate=True)
        with (
            patch.dict(os.environ, {"DOOZER_DATA_PATH": "https://github.com/openshift-eng/ocp-build-data"}),
            self.assertRaisesRegex(ValueError, "not an existing aggregate"),
        ):
            await self.pipeline.run()
        self.pipeline.build_aggregate.assert_not_awaited()
        self.pipeline.setup_shipment_repo.assert_not_awaited()

    async def test_unrelated_aggregate_stops_before_shipment_repository_work(self):
        self.pipeline.aggregate = False
        self.pipeline.nvrs = [SOURCE_NVR, AGGREGATE_NVR]
        self.pipeline.create_snapshot.side_effect = [_snapshot(aggregate=True), _snapshot()]
        self.pipeline.verify_reused_aggregate.side_effect = ValueError("aggregate parents do not match")
        with self.assertRaisesRegex(ValueError, "aggregate parents do not match"):
            await self.pipeline.run()
        self.pipeline.setup_shipment_repo.assert_not_awaited()
        self.pipeline.create_shipment_config.assert_not_called()
        self.pipeline.create_shipment_mr.assert_not_awaited()

    async def test_single_kind_requires_an_explicit_override(self):
        self.pipeline.aggregate = False
        with self.assertRaisesRegex(ValueError, "requires explicit --shipment-kind image"):
            await self.pipeline.run()
        self.pipeline.create_shipment_mr.assert_not_awaited()

    async def test_multiple_source_builds_are_rejected(self):
        self.pipeline.aggregate = False
        self.pipeline.nvrs = [SOURCE_NVR, SOURCE_NVR, AGGREGATE_NVR]
        with self.assertRaisesRegex(ValueError, "exactly one NVR per shipment kind"):
            await self.pipeline.run()
        self.pipeline.create_snapshot.assert_not_awaited()
        self.pipeline.setup_shipment_repo.assert_not_awaited()

    async def test_explicit_image_only_release_is_supported(self):
        self.pipeline.aggregate = False
        self.pipeline.shipment_kind = "image"
        await self.pipeline.run()
        self.assertEqual(set(self.pipeline.create_shipment_mr.await_args.args[0]), {"image"})

    async def test_explicit_binaries_only_release_is_supported(self):
        self.pipeline.aggregate = False
        self.pipeline.shipment_kind = "binaries"
        self.pipeline.nvrs = [AGGREGATE_NVR]
        self.pipeline.create_snapshot.side_effect = [_snapshot(aggregate=True)]
        await self.pipeline.run()
        self.assertEqual(set(self.pipeline.create_shipment_mr.await_args.args[0]), {"binaries"})

    async def test_dry_run_needs_no_gitlab_token(self):
        self.pipeline.dry_run = True
        with patch.dict(os.environ, {}, clear=True):
            release.BinaryReleaseKonfluxPipeline.check_env_vars(self.pipeline)

    async def test_embargoed_source_stops_before_snapshot_or_build(self):
        self.pipeline.nvrs = [SOURCE_NVR.replace(".p2.", ".p3.")]
        with self.assertRaisesRegex(RuntimeError, "embargoed"):
            await self.pipeline.run()
        self.pipeline.create_snapshot.assert_not_awaited()
        self.pipeline.build_aggregate.assert_not_awaited()
        self.pipeline.setup_shipment_repo.assert_not_awaited()

    async def test_failed_build_cannot_fall_back_to_shipping_the_source(self):
        self.pipeline.build_aggregate.side_effect = RuntimeError("build failed")
        with self.assertRaisesRegex(RuntimeError, "build failed"):
            await self.pipeline.run()
        self.pipeline.setup_shipment_repo.assert_not_awaited()
        self.pipeline.create_shipment_mr.assert_not_awaited()

    async def test_missing_source_snapshot_stops_before_build_or_shipment(self):
        self.pipeline.create_snapshot.side_effect = None
        self.pipeline.create_snapshot.return_value = None
        with self.assertRaisesRegex(RuntimeError, "No snapshot"):
            await self.pipeline.run()
        self.pipeline.build_aggregate.assert_not_awaited()
        self.pipeline.setup_shipment_repo.assert_not_awaited()
        self.pipeline.create_shipment_mr.assert_not_awaited()


class TestOcMirrorShipmentKinds(TestCase):
    def test_source_and_aggregate_are_routed_to_different_release_plans(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline = _pipeline(directory, aggregate=False, create_mr=True, nvrs=[SOURCE_NVR, AGGREGATE_NVR])
            pipeline.product = "oc-mirror"
            pipeline.shipment_data_repo._directory = Path(directory)
            applications = {
                "oc-mirror-2-0": {
                    "environments": {env: {"releasePlan": f"oc-mirror-advisory-{env}-2-0"} for env in ("stage", "prod")}
                },
                "oc-mirror-2-0-binaries": {
                    "environments": {env: {"releasePlan": f"oc-mirror-cdn-{env}"} for env in ("stage", "prod")}
                },
            }
            (Path(directory) / "config.yaml").write_text(yaml.safe_dump({"applications": applications}))
            self.assertEqual(
                pipeline._group_nvrs_by_shipment_kind(), {"image": [SOURCE_NVR], "binaries": [AGGREGATE_NVR]}
            )
            for kind, snapshot in (("image", _snapshot()), ("binaries", _snapshot(aggregate=True))):
                with self.subTest(kind=kind):
                    config = pipeline.create_shipment_config(snapshot, shipment_kind=kind)
                    expected = "oc-mirror-advisory-stage-2-0" if kind == "image" else "oc-mirror-cdn-stage"
                    self.assertEqual(config.shipment.environments.stage.releasePlan, expected)
                    self.assertTrue(config.shipment.metadata.release_notes_in_rpa)
                    self.assertIsNone(config.shipment.data)

    def test_binaries_never_fall_back_to_container_release_plans(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline = _pipeline(directory, aggregate=False, create_mr=True, shipment_kind="image")
            pipeline.product = "oc-mirror"
            pipeline.shipment_data_repo._directory = Path(directory)
            (Path(directory) / "config.yaml").write_text(
                "applications:\n  oc-mirror-2-0:\n    environments:\n"
                "      stage:\n        releasePlan: oc-mirror-advisory-stage-2-0\n"
                "      prod:\n        releasePlan: oc-mirror-advisory-prod-2-0\n"
            )
            with self.assertRaisesRegex(ValueError, "oc-mirror-2-0-binaries"):
                pipeline.create_shipment_config(_snapshot(aggregate=True), shipment_kind="binaries")

    def test_unknown_build_names_and_mismatched_kind_overrides_are_rejected(self):
        pipeline = _pipeline("/tmp/test", aggregate=False, nvrs=["unrelated-container-2.0-1.p2.el9"])
        with self.assertRaisesRegex(ValueError, "Unsupported oc-mirror shipment build"):
            pipeline._group_nvrs_by_shipment_kind()
        pipeline.nvrs = [SOURCE_NVR, AGGREGATE_NVR]
        pipeline.shipment_kind = "image"
        with self.assertRaisesRegex(ValueError, "requires shipment kind binaries"):
            pipeline._group_nvrs_by_shipment_kind()
        pipeline.shipment_kind = None
        pipeline.release_plan_key = "oc-mirror-2-0"
        with self.assertRaisesRegex(ValueError, "require separate image and binaries"):
            pipeline._group_nvrs_by_shipment_kind()
