import hashlib
import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from gitlab.exceptions import GitlabGetError
from pyartcd.cli import cli
from pyartcd.pipelines import binaries_cdn_release as icr
from pyartcd.pipelines.binaries_cdn_release import (
    BINARIES,
    BinariesCdnReleasePipeline,
    binaries_cdn_enabled,
    cgw_version_name,
    parse_binary_dockerfile_parents,
    rpa_product_version_names,
)

from pyartcd import constants

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
  name: ocp-art-binaries-prod-4-22
spec:
  data:
    mapping:
      components:
        - name: ose-4-22-openshift-install-binaries
          contentGateway:
            productName: "RHOSE - OpenShift Installer"
            productCode: "openshift-install"
            productVersionName: "{version}"
"""


def _runtime(working_dir: str, dry_run: bool = False):
    runtime = MagicMock()
    runtime.dry_run = dry_run
    runtime.working_dir = Path(working_dir)
    runtime.doozer_working = os.path.join(working_dir, "doozer_working")
    return runtime


def _pipeline(working_dir: str, client_type: str = "ocp", **kwargs) -> BinariesCdnReleasePipeline:
    return BinariesCdnReleasePipeline(
        runtime=_runtime(working_dir, kwargs.pop("dry_run", False)),
        binary=kwargs.pop("binary", "openshift-install"),
        version=kwargs.pop("version", "4.22"),
        assembly="4.22.18",
        release_name=kwargs.pop("release_name", "4.22.18"),
        client_type=client_type,
        **kwargs,
    )


class TestHelpers(TestCase):
    def test_cgw_version_name_is_major_minor_for_every_client_type(self):
        for client_type in ("ocp", "ocp-dev-preview"):
            with self.subTest(client_type=client_type):
                self.assertEqual(cgw_version_name("4.22", client_type), "4.22")

    def test_rpa_product_version_names(self):
        self.assertEqual(rpa_product_version_names(RPA_TEMPLATE.format(version="4.22")), ["4.22"])
        self.assertEqual(rpa_product_version_names("spec: {}\n"), [])
        self.assertEqual(rpa_product_version_names("productVersionName: 4.22\n"), [])
        twice = RPA_TEMPLATE.format(version="a") + '            productVersionName: "b"\n'
        self.assertEqual(rpa_product_version_names(twice), ["a", "b"])

    def test_parse_binary_dockerfile_parents_reads_all_twelve_parents(self):
        self.assertEqual(parse_binary_dockerfile_parents(REBASED_DOCKERFILE, PREFIX_TAG), EXPECTED_PARENTS)

    def test_parse_binary_dockerfile_parents_ignores_stages_of_other_prefixes(self):
        extra = f"FROM --platform=linux/amd64 quay.io/x/y@{_digest('x86_64', 'cli')} AS cli_amd64\n"
        self.assertEqual(parse_binary_dockerfile_parents(extra + REBASED_DOCKERFILE, PREFIX_TAG), EXPECTED_PARENTS)

    def test_parse_binary_dockerfile_parents_rejects_platform_stage_mismatch(self):
        bad = f"FROM --platform=linux/arm64 quay.io/x/y@{_digest('x86_64', 'installer')} AS inst_amd64\n"
        with self.assertRaises(ValueError):
            parse_binary_dockerfile_parents(bad, PREFIX_TAG)

    def test_verify_parents_fails_on_mismatch(self):
        wrong = {arch: dict(tags) for arch, tags in EXPECTED_PARENTS.items()}
        wrong["s390x"]["installer"] = "sha256:" + "0" * 64
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = _pipeline(tmp)
            pipeline.verify_parents(REBASED_DOCKERFILE, EXPECTED_PARENTS)
            with self.assertRaises(RuntimeError):
                pipeline.verify_parents(REBASED_DOCKERFILE, wrong)

    def test_binaries_profiles(self):
        self.assertEqual(sorted(BINARIES), sorted(constants.CDN_BINARIES))
        self.assertEqual(BINARIES["openshift-install"].payload_parents, PREFIX_TAG)
        self.assertEqual(BINARIES["openshift-install"].cgw_product, "openshift-install")

    def test_binaries_cdn_enabled(self):
        self.assertTrue(binaries_cdn_enabled({"binaries": {"cdn": {"enabled": True}}}))
        for config in ({}, {"binaries": None}, {"binaries": {}}, {"binaries": {"cdn": {"enabled": False}}}):
            with self.subTest(config=config):
                self.assertFalse(binaries_cdn_enabled(config))
        self.assertFalse(binaries_cdn_enabled({"cdn_clients": {"enabled": True}}))

    def test_constructor_derives_names_per_client_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            ga = _pipeline(tmp)
            preview = _pipeline(tmp, client_type="ocp-dev-preview", version="5.0", release_name="5.0.0-ec.6")
        self.assertEqual(ga.release_plan_key, "openshift-4-22-binaries")
        self.assertEqual(ga.rpa_names, ["ocp-art-binaries-stage-4-22", "ocp-art-binaries-prod-4-22"])
        self.assertEqual(ga.cgw_version, "4.22")
        self.assertEqual(preview.release_plan_key, "openshift-5-0-binaries-dev-preview")
        self.assertEqual(
            preview.rpa_names, ["ocp-art-binaries-dev-preview-stage-5-0", "ocp-art-binaries-dev-preview-prod-5-0"]
        )
        self.assertEqual(preview.cgw_version, "5.0")

    def test_constructor_rejects_bad_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                _pipeline(tmp, client_type="ocp-nightly")
            with self.assertRaises(ValueError):
                _pipeline(tmp, version="4.22.18")
            with self.assertRaises(ValueError):
                _pipeline(tmp, binary="opm")

    def test_constructor_rejects_release_names_outside_the_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("4.22", "4.2.18", "4.23.1", "4.22.18.1", "latest"):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    _pipeline(tmp, release_name=name)
            self.assertEqual(_pipeline(tmp, release_name="4.22.0-rc.1").release_name, "4.22.0-rc.1")

    def test_cli_command_registered(self):
        self.assertIn("binaries-cdn-release", cli.commands)
        self.assertNotIn("installer-cdn-release", cli.commands)
        params = [p.name for p in cli.commands["binaries-cdn-release"].params]
        self.assertEqual(params[:2], ["binary", "version"])
        self.assertNotIn("skip_cgw_mr", params)
        self.assertNotIn("cgw_mr_timeout_hours", params)


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

    @patch("pyartcd.pipelines.binaries_cdn_release.oc.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.binaries_cdn_release.oc.get_release_image_info", new_callable=AsyncMock)
    async def test_collect_release_values_all_flavors(self, get_release_image_info, get_image_info):
        get_release_image_info.side_effect = lambda pullspec, **_: self._release_info(pullspec.rsplit("-", 1)[1])
        get_image_info.return_value = [{"listDigest": "sha256:multilist"}]
        with tempfile.TemporaryDirectory() as tmp:
            release_values, parents = await _pipeline(tmp).collect_release_values(list(ARCHES), True)
        self.assertEqual(
            release_values,
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

    @patch("pyartcd.pipelines.binaries_cdn_release.oc.get_release_image_info", new_callable=AsyncMock)
    async def test_collect_release_values_rejects_wrong_version(self, get_release_image_info):
        info = self._release_info("x86_64")
        info["metadata"]["version"] = "4.22.17"
        get_release_image_info.return_value = info
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                await _pipeline(tmp).collect_release_values(["x86_64"], False)

    @patch("pyartcd.pipelines.binaries_cdn_release.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_binary_configured(self, cmd_gather_async):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = _pipeline(tmp)
            cmd_gather_async.return_value = (0, '{"binaries": ["openshift-install"]}\n', "")
            self.assertTrue(await pipeline.binary_configured())
            cmd = cmd_gather_async.await_args.args[0]
            self.assertEqual(cmd[-3:], ["--binaries=openshift-install", "binaries:list", "--json"])
            self.assertNotIn("--load-disabled", cmd)
            self.assertFalse(any(arg.startswith("--images") for arg in cmd))
            cmd_gather_async.return_value = (
                1,
                "",
                "The following binaries were either missing or filtered out: openshift-install",
            )
            self.assertFalse(await pipeline.binary_configured())
            cmd_gather_async.return_value = (1, "", "boom")
            with self.assertRaises(RuntimeError):
                await pipeline.binary_configured()

    @patch("pyartcd.pipelines.binaries_cdn_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_build_binary_rebases_with_release_values_and_reads_record_log(self, cmd_assert_async):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = _pipeline(tmp)
            os.makedirs(pipeline.runtime.doozer_working)
            Path(pipeline.runtime.doozer_working, "record.log").write_text(
                "image_build_konflux|name=openshift-install|status=0|nvrs=other-1-1|\n"
                "binary_build_konflux|name=other|status=0|nvrs=other-1-1|\n"
                "binary_build_konflux|name=openshift-install|status=0|"
                "nvrs=openshift-install-binaries-container-v4.22.0-1.p2.assembly.4.22.18.el9|dockerfile=/df|\n"
            )
            with patch.dict(os.environ, {"KONFLUX_SA_KUBECONFIG": "/kc"}):
                entry = await pipeline.build_binary(["RELEASE_VERSION=4.22.18", "DIGEST_MULTI=sha256:m"])
        rebase_cmd, build_cmd = [c.args[0] for c in cmd_assert_async.await_args_list]
        self.assertIn("beta:binaries:konflux:rebase", rebase_cmd)
        self.assertEqual(
            rebase_cmd[rebase_cmd.index("--build-arg") :],
            ["--build-arg", "RELEASE_VERSION=4.22.18", "--build-arg", "DIGEST_MULTI=sha256:m", "--push"],
        )
        for cmd in (rebase_cmd, build_cmd):
            self.assertIn("--latest-parent-version", cmd)
            self.assertIn("--binaries=openshift-install", cmd)
            self.assertNotIn("--load-disabled", cmd)
            self.assertFalse(any(arg.startswith("--images") for arg in cmd))
        self.assertEqual(
            build_cmd[build_cmd.index("beta:binaries:konflux:build") :],
            [
                "beta:binaries:konflux:build",
                "--konflux-namespace=ocp-art-tenant",
                "--konflux-kubeconfig",
                "/kc",
            ],
        )
        self.assertEqual(entry["nvrs"], "openshift-install-binaries-container-v4.22.0-1.p2.assembly.4.22.18.el9")

    @patch("pyartcd.pipelines.binaries_cdn_release.exectools.cmd_assert_async", new_callable=AsyncMock)
    async def test_build_binary_fails_when_build_failed(self, cmd_assert_async):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = _pipeline(tmp)
            os.makedirs(pipeline.runtime.doozer_working)
            Path(pipeline.runtime.doozer_working, "record.log").write_text(
                "binary_build_konflux|name=openshift-install|status=-1|nvrs=n/a|\n"
            )
            with self.assertRaises(RuntimeError):
                await pipeline.build_binary(["RELEASE_VERSION=4.22.18"])


class TestVerifyCgwOnboarding(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.enterContext(patch.dict(os.environ, {"GITLAB_TOKEN": "t"}))
        self.project = MagicMock()
        self.gitlab = MagicMock()
        self.gitlab.get_project.return_value = self.project
        self.enterContext(patch.object(BinariesCdnReleasePipeline, "_gitlab", return_value=self.gitlab))
        self.files = {}
        self.project.files.get.side_effect = self._get_file

    def _get_file(self, file_path, ref):
        self.assertEqual(ref, "main")
        if file_path not in self.files:
            raise GitlabGetError("404 File Not Found", response_code=404)
        f = MagicMock()
        f.decode.return_value = self.files[file_path].encode()
        return f

    def _onboard(self, rpa_version="4.22", prefix="ocp-art-binaries", version_file="4.22"):
        for env in ("stage", "prod"):
            self.files[f"{icr.KRD_RPA_DIR}/{prefix}-{env}-4-22.yaml"] = RPA_TEMPLATE.format(version=rpa_version)
        if version_file:
            self.files[f"{icr.KRD_CGW_DIR}/openshift-install/{version_file}.yaml"] = "versionName: x\n"

    def _assert_read_only(self):
        self.project.commits.create.assert_not_called()
        self.project.mergerequests.create.assert_not_called()
        self.gitlab.list_merge_requests.assert_not_called()

    def test_passes_when_rpas_and_version_are_on_main(self):
        self._onboard()
        _pipeline(self.tmp.name, release_name="4.22.22").verify_cgw_onboarding()
        self.gitlab.get_project.assert_called_once_with(icr.KRD_PROJECT)
        self._assert_read_only()

    def test_dev_preview_uses_its_rpas_and_the_same_version(self):
        self._onboard(prefix="ocp-art-binaries-dev-preview")
        _pipeline(self.tmp.name, client_type="ocp-dev-preview", release_name="4.22.0-ec.6").verify_cgw_onboarding()
        self._assert_read_only()

    def test_fails_when_an_rpa_is_missing(self):
        self._onboard()
        del self.files[f"{icr.KRD_RPA_DIR}/ocp-art-binaries-prod-4-22.yaml"]
        with self.assertRaises(RuntimeError) as ctx:
            _pipeline(self.tmp.name).verify_cgw_onboarding()
        message = str(ctx.exception)
        self.assertIn("openshift-4.22 is not onboarded", message)
        self.assertIn(f"missing {icr.KRD_RPA_DIR}/ocp-art-binaries-prod-4-22.yaml", message)
        self.assertNotIn("ocp-art-binaries-stage-4-22", message)
        self._assert_read_only()

    def test_fails_when_rpa_still_names_a_release(self):
        self._onboard(rpa_version="4.22.18")
        with self.assertRaises(RuntimeError) as ctx:
            _pipeline(self.tmp.name, release_name="4.22.22").verify_cgw_onboarding()
        message = str(ctx.exception)
        self.assertIn("not onboarded", message)
        for env in ("stage", "prod"):
            self.assertIn(
                f"{icr.KRD_RPA_DIR}/ocp-art-binaries-{env}-4-22.yaml has productVersionName ['4.22.18'], "
                'expected exactly one "4.22"',
                message,
            )
        self._assert_read_only()

    def test_fails_when_rpa_has_two_product_versions(self):
        self._onboard()
        path = f"{icr.KRD_RPA_DIR}/ocp-art-binaries-stage-4-22.yaml"
        self.files[path] += '            productVersionName: "4.22"\n'
        with self.assertRaises(RuntimeError) as ctx:
            _pipeline(self.tmp.name).verify_cgw_onboarding()
        self.assertIn(f"{path} has productVersionName ['4.22', '4.22']", str(ctx.exception))

    def test_fails_when_version_file_is_missing(self):
        self._onboard(version_file="4.22.22")
        with self.assertRaises(RuntimeError) as ctx:
            _pipeline(self.tmp.name, release_name="4.22.22").verify_cgw_onboarding()
        self.assertIn(f"missing {icr.KRD_CGW_DIR}/openshift-install/4.22.yaml", str(ctx.exception))
        self._assert_read_only()

    def test_other_gitlab_errors_propagate(self):
        self.project.files.get.side_effect = GitlabGetError("500", response_code=500)
        with self.assertRaises(GitlabGetError):
            _pipeline(self.tmp.name).verify_cgw_onboarding()

    def test_dry_run_still_checks(self):
        with self.assertRaises(RuntimeError):
            _pipeline(self.tmp.name, dry_run=True).verify_cgw_onboarding()
        self._onboard()
        _pipeline(self.tmp.name, dry_run=True).verify_cgw_onboarding()
        self.assertEqual(self.project.files.get.call_count, 6)
        self._assert_read_only()


class TestRun(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.binary = self.enterContext(patch.object(icr, "BinaryReleaseKonfluxPipeline"))
        self.binary.return_value.run = AsyncMock()
        self.load_group_config = self.enterContext(patch.object(icr.util, "load_group_config", new_callable=AsyncMock))
        self.load_group_config.return_value = {
            "arches": ["x86_64"],
            "multi_arch": {"enabled": False},
            "binaries": {"cdn": {"enabled": True}},
        }

    async def test_noop_when_binary_not_configured(self):
        pipeline = _pipeline(self.tmp.name)
        pipeline.binary_configured = AsyncMock(return_value=False)
        pipeline.build_binary = AsyncMock()
        await pipeline.run()
        pipeline.build_binary.assert_not_awaited()
        self.binary.assert_not_called()

    async def test_builds_verifies_and_releases(self):
        pipeline = _pipeline(self.tmp.name)
        df = Path(self.tmp.name, "Dockerfile")
        df.write_text(REBASED_DOCKERFILE)
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], EXPECTED_PARENTS))
        nvr = "openshift-install-binaries-container-v4.22.0-1.p2.assembly.4.22.18.el9"
        pipeline.build_binary = AsyncMock(return_value={"nvrs": nvr, "dockerfile": str(df)})
        pipeline.verify_cgw_onboarding = MagicMock()
        await pipeline.run()
        pipeline.collect_release_values.assert_awaited_once_with(["x86_64"], False)
        pipeline.verify_cgw_onboarding.assert_called_once_with()
        kwargs = self.binary.call_args.kwargs
        self.assertEqual(kwargs["group"], "openshift-4.22")
        self.assertEqual(kwargs["nvrs"], [nvr])
        self.assertEqual(kwargs["release_plan_key"], "openshift-4-22-binaries")
        self.assertEqual(kwargs["shipment_kind"], "binaries")
        self.assertTrue(kwargs["create_mr"])
        self.binary.return_value.run.assert_awaited_once()

    async def test_noop_when_binaries_cdn_disabled(self):
        for group_config in ({"arches": ["x86_64"]}, {"arches": ["x86_64"], "binaries": {"cdn": {"enabled": False}}}):
            self.load_group_config.return_value = group_config
            pipeline = _pipeline(self.tmp.name)
            pipeline.binary_configured = AsyncMock(return_value=True)
            pipeline.collect_release_values = AsyncMock()
            pipeline.build_binary = AsyncMock()
            await pipeline.run()
            pipeline.collect_release_values.assert_not_awaited()
            pipeline.build_binary.assert_not_awaited()
            self.binary.assert_not_called()

    async def test_build_only_runs_when_binaries_cdn_disabled(self):
        self.load_group_config.return_value = {"arches": ["x86_64"], "binaries": {"cdn": {"enabled": False}}}
        pipeline = _pipeline(self.tmp.name, build_only=True)
        df = Path(self.tmp.name, "Dockerfile")
        df.write_text(REBASED_DOCKERFILE)
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], EXPECTED_PARENTS))
        pipeline.build_binary = AsyncMock(return_value={"nvrs": "x-1-1.el9", "dockerfile": str(df)})
        pipeline.verify_cgw_onboarding = MagicMock()
        await pipeline.run()
        pipeline.build_binary.assert_awaited_once()
        pipeline.verify_cgw_onboarding.assert_not_called()
        self.binary.assert_not_called()

    async def test_build_only_stops_after_verified_build(self):
        pipeline = _pipeline(self.tmp.name, build_only=True)
        df = Path(self.tmp.name, "Dockerfile")
        df.write_text(REBASED_DOCKERFILE)
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], EXPECTED_PARENTS))
        pipeline.build_binary = AsyncMock(return_value={"nvrs": "x-1-1.el9", "dockerfile": str(df)})
        pipeline.verify_cgw_onboarding = MagicMock()
        await pipeline.run()
        pipeline.build_binary.assert_awaited_once()
        pipeline.verify_cgw_onboarding.assert_not_called()
        self.binary.assert_not_called()

    async def test_parent_mismatch_stops_before_release(self):
        pipeline = _pipeline(self.tmp.name)
        df = Path(self.tmp.name, "Dockerfile")
        df.write_text(REBASED_DOCKERFILE)
        wrong = {arch: dict(tags) for arch, tags in EXPECTED_PARENTS.items()}
        wrong["x86_64"]["installer"] = "sha256:" + "1" * 64
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], wrong))
        pipeline.build_binary = AsyncMock(return_value={"nvrs": "x-1-1.el9", "dockerfile": str(df)})
        pipeline.verify_cgw_onboarding = MagicMock()
        with self.assertRaises(RuntimeError):
            await pipeline.run()
        pipeline.verify_cgw_onboarding.assert_not_called()
        self.binary.assert_not_called()

    async def test_given_nvr_skips_build_and_dev_preview_uses_dev_preview_plans(self):
        pipeline = _pipeline(
            self.tmp.name,
            client_type="ocp-dev-preview",
            version="5.0",
            release_name="5.0.0-ec.6",
            nvr="openshift-install-binaries-container-v5.0.0-1.p2.assembly.ec.6.el9",
        )
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=5.0.0-ec.6"], {}))
        pipeline.build_binary = AsyncMock()
        pipeline.verify_cgw_onboarding = MagicMock()
        await pipeline.run()
        pipeline.build_binary.assert_not_awaited()
        pipeline.verify_cgw_onboarding.assert_called_once_with()
        kwargs = self.binary.call_args.kwargs
        self.assertEqual(kwargs["release_plan_key"], "openshift-5-0-binaries-dev-preview")
        self.assertEqual(kwargs["shipment_kind"], "binaries-dev-preview")

    async def test_embargoed_nvr_is_not_released(self):
        pipeline = _pipeline(
            self.tmp.name, nvr="openshift-install-binaries-container-v4.22.0-1.p1.assembly.4.22.18.el9"
        )
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=([], {}))
        pipeline.verify_cgw_onboarding = MagicMock()
        await pipeline.run()
        pipeline.verify_cgw_onboarding.assert_not_called()
        self.binary.assert_not_called()

    async def test_not_onboarded_stops_before_release(self):
        pipeline = _pipeline(
            self.tmp.name, nvr="openshift-install-binaries-container-v4.22.0-1.p2.assembly.4.22.18.el9"
        )
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=([], {}))
        pipeline.verify_cgw_onboarding = MagicMock(side_effect=RuntimeError("openshift-4.22 is not onboarded"))
        with self.assertRaises(RuntimeError):
            await pipeline.run()
        self.binary.assert_not_called()

    async def test_dry_run_without_nvr_checks_onboarding_then_stops(self):
        pipeline = _pipeline(self.tmp.name, dry_run=True)
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=(["RELEASE_VERSION=4.22.18"], EXPECTED_PARENTS))
        pipeline.build_binary = AsyncMock(return_value={"nvrs": "n/a", "dockerfile": "/nonexistent"})
        pipeline.verify_cgw_onboarding = MagicMock()
        await pipeline.run()
        pipeline.build_binary.assert_awaited_once()
        pipeline.verify_cgw_onboarding.assert_called_once_with()
        self.binary.assert_not_called()

    async def test_dry_run_with_nvr_checks_onboarding_and_runs_shipment_dry(self):
        pipeline = _pipeline(
            self.tmp.name,
            dry_run=True,
            nvr="openshift-install-binaries-container-v4.22.0-1.p2.assembly.4.22.18.el9",
        )
        pipeline.binary_configured = AsyncMock(return_value=True)
        pipeline.collect_release_values = AsyncMock(return_value=([], {}))
        pipeline.verify_cgw_onboarding = MagicMock()
        await pipeline.run()
        pipeline.verify_cgw_onboarding.assert_called_once_with()
        self.assertTrue(self.binary.call_args.kwargs["runtime"].dry_run)
        self.binary.return_value.run.assert_awaited_once()
