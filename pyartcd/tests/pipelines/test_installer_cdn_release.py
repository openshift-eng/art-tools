import hashlib
import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from gitlab.exceptions import GitlabGetError
from pyartcd.cli import cli
from pyartcd.pipelines import installer_cdn_release as icr
from pyartcd.pipelines.installer_cdn_release import (
    InstallerCdnReleasePipeline,
    cgw_version_file_content,
    parse_delivery_dockerfile_parents,
    rpa_product_version_name,
    set_rpa_product_version_name,
)

ARCHES = {"x86_64": "amd64", "aarch64": "arm64", "ppc64le": "ppc64le", "s390x": "s390x"}
PREFIX_TAG = {"inst": "installer", "art": "installer-artifacts", "bm": "baremetal-installer"}


def _digest(arch: str, tag: str) -> str:
    return "sha256:" + hashlib.sha256(f"{arch}/{tag}".encode()).hexdigest()


EXPECTED_PARENTS = {arch: {tag: _digest(arch, tag) for tag in PREFIX_TAG.values()} for arch in ARCHES}

REBASED_DOCKERFILE = (
    "\n".join(
        f"FROM --platform=linux/{goarch} quay.io/redhat-user-workloads/ocp-art-tenant/art-images@"
        f"{EXPECTED_PARENTS[arch][tag]} AS {prefix}_{goarch}"
        for prefix, tag in PREFIX_TAG.items()
        for arch, goarch in ARCHES.items()
    )
    + "\nFROM art_amd64 AS packer\nRUN bash /work/package.sh\nFROM registry.redhat.io/rhel9-8-els/rhel-minimal:latest\n"
)

RPA_TEMPLATE = """---
kind: ReleasePlanAdmission
metadata:
  name: ocp-art-cdn-prod-4-22
spec:
  data:
    mapping:
      components:
        - name: ose-4-22-openshift-install-delivery
          contentGateway:
            productName: "openshift-install"
            productCode: "openshift-install"
            productVersionName: "{version}"
"""


def _runtime(working_dir: str, dry_run: bool = False):
    runtime = MagicMock()
    runtime.dry_run = dry_run
    runtime.working_dir = Path(working_dir)
    runtime.doozer_working = os.path.join(working_dir, "doozer_working")
    return runtime


def _pipeline(working_dir: str, client_type: str = "ocp", **kwargs) -> InstallerCdnReleasePipeline:
    return InstallerCdnReleasePipeline(
        runtime=_runtime(working_dir, kwargs.pop("dry_run", False)),
        version=kwargs.pop("version", "4.22"),
        assembly="4.22.18",
        release_name=kwargs.pop("release_name", "4.22.18"),
        client_type=client_type,
        poll_seconds=1,
        **kwargs,
    )


class TestHelpers(TestCase):
    def test_cgw_version_file_content_ga(self):
        self.assertEqual(
            cgw_version_file_content("4.22.18", True, "2026-10-20"),
            '---\nversionName: "4.22.18"\nga: true\ntermsAndConditions: "Anonymous Download"\n'
            'hidden: false\ninvisible: false\nreleaseDate: "2026-10-20"\n',
        )

    def test_cgw_version_file_content_dev_preview(self):
        self.assertIn("ga: false\n", cgw_version_file_content("4.23.0-ec.2", False, "2026-10-20"))

    def test_rpa_product_version_name_round_trip(self):
        text = RPA_TEMPLATE.format(version="4.22.17")
        self.assertEqual(rpa_product_version_name(text), "4.22.17")
        updated = set_rpa_product_version_name(text, "4.22.18")
        self.assertEqual(rpa_product_version_name(updated), "4.22.18")
        self.assertEqual(updated, RPA_TEMPLATE.format(version="4.22.18"))

    def test_rpa_product_version_name_requires_exactly_one(self):
        with self.assertRaises(ValueError):
            rpa_product_version_name("spec: {}\n")
        twice = RPA_TEMPLATE.format(version="a") + '            productVersionName: "b"\n'
        with self.assertRaises(ValueError):
            set_rpa_product_version_name(twice, "c")

    def test_parse_delivery_dockerfile_parents_reads_all_twelve_parents(self):
        self.assertEqual(parse_delivery_dockerfile_parents(REBASED_DOCKERFILE), EXPECTED_PARENTS)

    def test_parse_delivery_dockerfile_parents_rejects_platform_stage_mismatch(self):
        bad = f"FROM --platform=linux/arm64 quay.io/x/y@{_digest('x86_64', 'installer')} AS inst_amd64\n"
        with self.assertRaises(ValueError):
            parse_delivery_dockerfile_parents(bad)

    def test_verify_parents_fails_on_mismatch(self):
        wrong = {arch: dict(tags) for arch, tags in EXPECTED_PARENTS.items()}
        wrong["s390x"]["installer"] = "sha256:" + "0" * 64
        InstallerCdnReleasePipeline.verify_parents(REBASED_DOCKERFILE, EXPECTED_PARENTS)
        with self.assertRaises(RuntimeError):
            InstallerCdnReleasePipeline.verify_parents(REBASED_DOCKERFILE, wrong)

    def test_constructor_derives_names_per_client_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            ga = _pipeline(tmp)
            preview = _pipeline(tmp, client_type="ocp-dev-preview", version="5.0", release_name="5.0.0-ec.6")
        self.assertEqual(ga.release_plan_key, "openshift-4-22-cdn")
        self.assertEqual(ga.rpa_names, ["ocp-art-cdn-stage-4-22", "ocp-art-cdn-prod-4-22"])
        self.assertEqual(preview.release_plan_key, "openshift-5-0-cdn-dev-preview")
        self.assertEqual(preview.rpa_names, ["ocp-art-cdn-dev-preview-stage-5-0", "ocp-art-cdn-dev-preview-prod-5-0"])

    def test_constructor_rejects_bad_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                _pipeline(tmp, client_type="ocp-nightly")
            with self.assertRaises(ValueError):
                _pipeline(tmp, version="4.22.18")

    def test_cli_command_registered(self):
        self.assertIn("installer-cdn-release", cli.commands)


class TestCollectAndBuild(IsolatedAsyncioTestCase):
    def _release_info(self, arch):
        return {
            "digest": f"sha256:{arch}release",
            "metadata": {"version": "4.22.18"},
            "references": {
                "spec": {
                    "tags": [
                        {
                            "name": tag,
                            "from": {"name": f"quay.io/openshift-release-dev/ocp-v4.0-art-dev@{digest}"},
                        }
                        for tag, digest in EXPECTED_PARENTS[arch].items()
                    ]
                    + [{"name": "cli", "from": {"name": "quay.io/x@sha256:cli"}}]
                }
            },
        }

    @patch("pyartcd.pipelines.installer_cdn_release.oc.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.installer_cdn_release.oc.get_release_image_info", new_callable=AsyncMock)
    async def test_collect_release_values_all_flavors(self, get_release_image_info, get_image_info):
        get_release_image_info.side_effect = lambda pullspec, **_: self._release_info(pullspec.rsplit("-", 1)[1])
        get_image_info.return_value = [{"listDigest": "sha256:multilist"}]
        with tempfile.TemporaryDirectory() as tmp:
            build_args, parents = await _pipeline(tmp).collect_release_values(list(ARCHES), True)
        self.assertEqual(
            build_args,
            [
                "RELEASE_VERSION=4.22.18",
                "DIGEST_X86_64=sha256:x86_64release",
                "DIGEST_AARCH64=sha256:aarch64release",
                "DIGEST_PPC64LE=sha256:ppc64lerelease",
                "DIGEST_S390X=sha256:s390xrelease",
                "DIGEST_MULTI=sha256:multilist",
            ],
        )
        self.assertEqual(parents, EXPECTED_PARENTS)
        get_image_info.assert_awaited_once()
        self.assertEqual(get_image_info.await_args.args[0], "quay.io/openshift-release-dev/ocp-release:4.22.18-multi")

    @patch("pyartcd.pipelines.installer_cdn_release.oc.get_release_image_info", new_callable=AsyncMock)
    async def test_collect_release_values_rejects_wrong_version(self, get_release_image_info):
        info = self._release_info("x86_64")
        info["metadata"]["version"] = "4.22.17"
        get_release_image_info.return_value = info
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                await _pipeline(tmp).collect_release_values(["x86_64"], False)

    @patch("pyartcd.pipelines.installer_cdn_release.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_delivery_image_configured(self, cmd_gather_async):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = _pipeline(tmp)
            cmd_gather_async.return_value = (0, '{"images": ["openshift-install-delivery"]}\n', "")
            self.assertTrue(await pipeline.delivery_image_configured())
            cmd = cmd_gather_async.await_args.args[0]
            self.assertIn("--load-disabled", cmd)
            self.assertIn("--images=openshift-install-delivery", cmd)
            cmd_gather_async.return_value = (
                1,
                "",
                "The following images or rpms were either missing or filtered out: openshift-install-delivery",
            )
            self.assertFalse(await pipeline.delivery_image_configured())
            cmd_gather_async.return_value = (1, "", "boom")
            with self.assertRaises(RuntimeError):
                await pipeline.delivery_image_configured()

    @patch("pyartcd.pipelines.installer_cdn_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_build_delivery_image_passes_build_args_and_reads_record_log(self, cmd_assert_async):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = _pipeline(tmp)
            os.makedirs(pipeline.runtime.doozer_working)
            Path(pipeline.runtime.doozer_working, "record.log").write_text(
                "image_build_konflux|name=other|status=0|nvrs=other-1-1|\n"
                "image_build_konflux|name=openshift-install-delivery|status=0|"
                "nvrs=openshift-install-delivery-container-v4.22.0-1.p2.assembly.4.22.18.el9|dockerfile=/df|\n"
            )
            with patch.dict(os.environ, {"KONFLUX_SA_KUBECONFIG": "/kc"}):
                entry = await pipeline.build_delivery_image(["RELEASE_VERSION=4.22.18", "DIGEST_MULTI=sha256:m"])
        rebase_cmd, build_cmd = [c.args[0] for c in cmd_assert_async.await_args_list]
        self.assertIn("beta:images:konflux:rebase", rebase_cmd)
        self.assertIn("--push", rebase_cmd)
        self.assertIn("--latest-parent-version", build_cmd)
        self.assertEqual(
            build_cmd[build_cmd.index("beta:images:konflux:build") :],
            [
                "beta:images:konflux:build",
                "--konflux-namespace=ocp-art-tenant",
                "--konflux-kubeconfig",
                "/kc",
                "--build-arg",
                "RELEASE_VERSION=4.22.18",
                "--build-arg",
                "DIGEST_MULTI=sha256:m",
            ],
        )
        self.assertEqual(entry["nvrs"], "openshift-install-delivery-container-v4.22.0-1.p2.assembly.4.22.18.el9")

    @patch("pyartcd.pipelines.installer_cdn_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_build_delivery_image_fails_when_build_failed(self, cmd_assert_async):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = _pipeline(tmp)
            os.makedirs(pipeline.runtime.doozer_working)
            Path(pipeline.runtime.doozer_working, "record.log").write_text(
                "image_build_konflux|name=openshift-install-delivery|status=-1|nvrs=n/a|\n"
            )
            with self.assertRaises(RuntimeError):
                await pipeline.build_delivery_image(["RELEASE_VERSION=4.22.18"])


class TestEnsureCgwVersion(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.enterContext(patch.dict(os.environ, {"GITLAB_TOKEN": "t", "BUILD_URL": "https://jenkins/job/1"}))
        self.enterContext(patch("pyartcd.pipelines.installer_cdn_release.asyncio.sleep", new_callable=AsyncMock))
        self.project = MagicMock()
        self.gitlab = MagicMock()
        self.gitlab.get_project.return_value = self.project
        self.gitlab.list_merge_requests.return_value = []
        self.enterContext(patch.object(InstallerCdnReleasePipeline, "_gitlab", return_value=self.gitlab))
        self.files = {}
        self.project.files.get.side_effect = self._get_file

    def _get_file(self, file_path, ref):
        if file_path not in self.files:
            raise GitlabGetError("404 File Not Found", response_code=404)
        f = MagicMock()
        f.decode.return_value = self.files[file_path].encode()
        return f

    def _set_rpas(self, version):
        for env in ("stage", "prod"):
            self.files[f"{icr.KRD_RPA_DIR}/ocp-art-cdn-{env}-4-22.yaml"] = RPA_TEMPLATE.format(version=version)

    def _merged_mr(self, sync_status="success"):
        mr = MagicMock(iid=7, web_url="https://gitlab/mr/7", state="merged", merge_commit_sha="abc")
        self.project.mergerequests.get.return_value = mr
        job = MagicMock(status=sync_status, web_url="https://gitlab/job/1")
        job.name = "sync_cgw"
        pipeline = MagicMock()
        pipeline.jobs.list.return_value = [job]
        self.project.pipelines.list.return_value = [pipeline]
        return mr

    async def test_noop_when_main_is_already_up_to_date(self):
        self._set_rpas("4.22.18")
        self.files[f"{icr.KRD_CGW_VERSION_DIR}/4.22.18.yaml"] = "x"
        await _pipeline(self.tmp.name).ensure_cgw_version()
        self.project.commits.create.assert_not_called()
        self.project.mergerequests.create.assert_not_called()

    async def test_creates_mr_with_version_file_and_rpa_bumps_then_waits(self):
        self._set_rpas("4.22.17")
        mr = self._merged_mr()
        self.project.mergerequests.create.return_value = mr
        with patch.object(icr, "datetime") as dt:
            dt.now.return_value.strftime.return_value = "2026-10-20"
            await _pipeline(self.tmp.name).ensure_cgw_version()
        commit = self.project.commits.create.call_args.args[0]
        self.assertEqual(commit["branch"], "installer-cdn-4-22-ocp-4.22.18")
        self.assertEqual(commit["start_branch"], "main")
        actions = {a["file_path"]: a for a in commit["actions"]}
        version_action = actions[f"{icr.KRD_CGW_VERSION_DIR}/4.22.18.yaml"]
        self.assertEqual(version_action["action"], "create")
        self.assertEqual(version_action["content"], cgw_version_file_content("4.22.18", True, "2026-10-20"))
        for env in ("stage", "prod"):
            action = actions[f"{icr.KRD_RPA_DIR}/ocp-art-cdn-{env}-4-22.yaml"]
            self.assertEqual(action["action"], "update")
            self.assertEqual(rpa_product_version_name(action["content"]), "4.22.18")
        self.assertEqual(self.project.mergerequests.create.call_args.args[0]["source_branch"], commit["branch"])
        self.project.pipelines.list.assert_called_with(ref="main", sha="abc", get_all=True)

    async def test_refuses_when_another_release_mr_is_open(self):
        self._set_rpas("4.22.17")
        other = MagicMock(source_branch="installer-cdn-4-22-ocp-4.22.17", web_url="https://gitlab/mr/6")
        self.gitlab.list_merge_requests.return_value = [other]
        with self.assertRaises(RuntimeError) as ctx:
            await _pipeline(self.tmp.name).ensure_cgw_version()
        self.assertIn("https://gitlab/mr/6", str(ctx.exception))
        self.project.commits.create.assert_not_called()

    async def test_reuses_open_mr_for_same_release(self):
        self._set_rpas("4.22.17")
        mr = self._merged_mr()
        mr.source_branch = "installer-cdn-4-22-ocp-4.22.18"
        self.gitlab.list_merge_requests.return_value = [mr]
        await _pipeline(self.tmp.name).ensure_cgw_version()
        self.project.commits.create.assert_not_called()

    async def test_fails_when_sync_cgw_fails(self):
        self._set_rpas("4.22.17")
        self.project.mergerequests.create.return_value = self._merged_mr(sync_status="failed")
        with self.assertRaises(RuntimeError):
            await _pipeline(self.tmp.name).ensure_cgw_version()

    async def test_fails_when_mr_closed(self):
        self._set_rpas("4.22.17")
        mr = MagicMock(iid=7, web_url="https://gitlab/mr/7", state="closed")
        self.project.mergerequests.create.return_value = mr
        self.project.mergerequests.get.return_value = mr
        with self.assertRaises(RuntimeError):
            await _pipeline(self.tmp.name).ensure_cgw_version()

    async def test_times_out_when_mr_not_merged(self):
        self._set_rpas("4.22.17")
        mr = MagicMock(iid=7, web_url="https://gitlab/mr/7", state="opened")
        self.project.mergerequests.create.return_value = mr
        self.project.mergerequests.get.return_value = mr
        pipeline = _pipeline(self.tmp.name, cgw_mr_timeout_hours=3 / 3600)
        with self.assertRaises(TimeoutError):
            await pipeline.ensure_cgw_version()

    async def test_fails_when_version_not_onboarded(self):
        with self.assertRaises(RuntimeError) as ctx:
            await _pipeline(self.tmp.name).ensure_cgw_version()
        self.assertIn("not onboarded", str(ctx.exception))

    async def test_dry_run_creates_nothing(self):
        self._set_rpas("4.22.17")
        await _pipeline(self.tmp.name, dry_run=True).ensure_cgw_version()
        self.project.commits.create.assert_not_called()
        self.project.mergerequests.create.assert_not_called()


class TestRun(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.binary = self.enterContext(patch.object(icr, "BinaryReleaseKonfluxPipeline"))
        self.binary.return_value.run = AsyncMock()
        self.load_group_config = self.enterContext(patch.object(icr.util, "load_group_config", new_callable=AsyncMock))
        self.load_group_config.return_value = {"arches": ["x86_64"], "multi_arch": {"enabled": False}}

    async def test_noop_when_image_not_configured(self):
        pipeline = _pipeline(self.tmp.name)
        pipeline.delivery_image_configured = AsyncMock(return_value=False)
        pipeline.build_delivery_image = AsyncMock()
        await pipeline.run()
        pipeline.build_delivery_image.assert_not_awaited()
        self.binary.assert_not_called()

    async def test_builds_verifies_and_releases(self):
        pipeline = _pipeline(self.tmp.name)
        df = Path(self.tmp.name, "Dockerfile")
        df.write_text(REBASED_DOCKERFILE)
        pipeline.delivery_image_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], EXPECTED_PARENTS))
        nvr = "openshift-install-delivery-container-v4.22.0-1.p2.assembly.4.22.18.el9"
        pipeline.build_delivery_image = AsyncMock(return_value={"nvrs": nvr, "dockerfile": str(df)})
        pipeline.ensure_cgw_version = AsyncMock()
        await pipeline.run()
        pipeline.collect_release_values.assert_awaited_once_with(["x86_64"], False)
        pipeline.ensure_cgw_version.assert_awaited_once()
        kwargs = self.binary.call_args.kwargs
        self.assertEqual(kwargs["group"], "openshift-4.22")
        self.assertEqual(kwargs["nvrs"], [nvr])
        self.assertEqual(kwargs["release_plan_key"], "openshift-4-22-cdn")
        self.assertEqual(kwargs["shipment_kind"], "cdn")
        self.assertTrue(kwargs["create_mr"])
        self.binary.return_value.run.assert_awaited_once()

    async def test_build_only_stops_after_verified_build(self):
        pipeline = _pipeline(self.tmp.name, build_only=True)
        df = Path(self.tmp.name, "Dockerfile")
        df.write_text(REBASED_DOCKERFILE)
        pipeline.delivery_image_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], EXPECTED_PARENTS))
        pipeline.build_delivery_image = AsyncMock(return_value={"nvrs": "x-1-1.el9", "dockerfile": str(df)})
        pipeline.ensure_cgw_version = AsyncMock()
        await pipeline.run()
        pipeline.build_delivery_image.assert_awaited_once()
        pipeline.ensure_cgw_version.assert_not_awaited()
        self.binary.assert_not_called()

    async def test_parent_mismatch_stops_before_release(self):
        pipeline = _pipeline(self.tmp.name)
        df = Path(self.tmp.name, "Dockerfile")
        df.write_text(REBASED_DOCKERFILE)
        wrong = {arch: dict(tags) for arch, tags in EXPECTED_PARENTS.items()}
        wrong["x86_64"]["installer"] = "sha256:" + "1" * 64
        pipeline.delivery_image_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], wrong))
        pipeline.build_delivery_image = AsyncMock(return_value={"nvrs": "x-1-1.el9", "dockerfile": str(df)})
        pipeline.ensure_cgw_version = AsyncMock()
        with self.assertRaises(RuntimeError):
            await pipeline.run()
        pipeline.ensure_cgw_version.assert_not_awaited()
        self.binary.assert_not_called()

    async def test_given_nvr_skips_build_and_dev_preview_uses_dev_preview_plans(self):
        pipeline = _pipeline(
            self.tmp.name,
            client_type="ocp-dev-preview",
            version="5.0",
            release_name="5.0.0-ec.6",
            nvr="openshift-install-delivery-container-v5.0.0-1.p2.assembly.ec.6.el9",
            skip_cgw_mr=True,
        )
        pipeline.delivery_image_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=5.0.0-ec.6"], {}))
        pipeline.build_delivery_image = AsyncMock()
        pipeline.ensure_cgw_version = AsyncMock()
        await pipeline.run()
        pipeline.build_delivery_image.assert_not_awaited()
        pipeline.ensure_cgw_version.assert_not_awaited()
        kwargs = self.binary.call_args.kwargs
        self.assertEqual(kwargs["release_plan_key"], "openshift-5-0-cdn-dev-preview")
        self.assertEqual(kwargs["shipment_kind"], "cdn-dev-preview")

    async def test_embargoed_nvr_is_not_released(self):
        pipeline = _pipeline(
            self.tmp.name, nvr="openshift-install-delivery-container-v4.22.0-1.p1.assembly.4.22.18.el9"
        )
        pipeline.delivery_image_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=([], {}))
        pipeline.ensure_cgw_version = AsyncMock()
        await pipeline.run()
        pipeline.ensure_cgw_version.assert_not_awaited()
        self.binary.assert_not_called()
